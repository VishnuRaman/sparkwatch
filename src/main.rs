mod analysis;
mod app;
mod k8s;
mod poller;
mod spark;
mod ui;

use anyhow::Result;
use app::{App, View};
use clap::Parser;
use crossterm::event::{Event, EventStream, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use futures::StreamExt;
use poller::{Message, Request, Source};
use spark::SparkClient;
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc;
use tokio::time::interval;

/// Terminal monitor for Apache Spark applications.
#[derive(Parser, Debug)]
#[command(name = "sparkwatch", version, after_help = "\
Examples:
  sparkwatch                                  driver UI on localhost:4040
  sparkwatch http://history:18080             History Server; pick an app
  sparkwatch http://history:18080 -a app-123  History Server; watch one app
  sparkwatch --k8s -n spark                   pick a running driver pod
  sparkwatch --k8s -n spark my-etl            watch the SparkApplication my-etl")]
struct Cli {
    /// Spark driver UI (http://host:4040) or History Server (http://host:18080).
    /// With --k8s: the SparkApplication name to watch.
    target: Option<String>,

    /// Find Spark driver pods with kubectl and port-forward to them
    #[arg(long)]
    k8s: bool,

    /// Kubernetes namespace (default: the current kubectl context's)
    #[arg(short = 'n', long, requires = "k8s")]
    namespace: Option<String>,

    /// Application id to watch; skips the picker when the endpoint lists several
    #[arg(short, long, conflicts_with = "k8s")]
    app: Option<String>,

    /// Poll interval in seconds
    #[arg(short, long, default_value_t = 2)]
    interval: u64,

    /// HTTP timeout in seconds
    #[arg(short, long, default_value_t = 5)]
    timeout: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let interval = Duration::from_secs(cli.interval.max(1));
    let timeout = Duration::from_secs(cli.timeout.max(1));

    // What the header shows as the endpoint, and which app (if any) to start on.
    let (source, endpoint, watch) = if cli.k8s {
        let endpoint = format!(
            "k8s:{}",
            cli.namespace.as_deref().unwrap_or("<current namespace>")
        );
        let source = Source::Kube {
            namespace: cli.namespace,
            timeout,
            conn: None,
        };
        (source, endpoint, cli.target)
    } else {
        let endpoint = cli
            .target
            .unwrap_or_else(|| "http://localhost:4040".to_string());
        let client = SparkClient::new(endpoint.clone(), timeout)?;
        (Source::Http(client), endpoint, cli.app)
    };

    let poller::Handle {
        req_tx,
        mut msg_rx,
        task,
    } = poller::spawn(source, watch.clone(), interval);

    let mut terminal = ratatui::init();
    let mut app = App::new(endpoint, interval, watch);
    let result = run(&mut terminal, &mut app, &mut msg_rx, &req_tx).await;
    ratatui::restore();

    // Let the poller tear down its port-forward before the process exits;
    // otherwise a stray kubectl can outlive us.
    let _ = req_tx.send(Request::Shutdown).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), task).await;
    result
}

async fn run(
    terminal: &mut ratatui::DefaultTerminal,
    app: &mut App,
    msg_rx: &mut mpsc::Receiver<Message>,
    req_tx: &mpsc::Sender<Request>,
) -> Result<()> {
    let mut events = EventStream::new();
    // Redraw on a timer too, so the "updated Ns ago" clock stays honest.
    let mut tick = interval(Duration::from_millis(500));

    loop {
        terminal.draw(|f| ui::draw(f, app))?;
        if app.should_quit {
            return Ok(());
        }

        select! {
            maybe_event = events.next() => {
                match maybe_event {
                    Some(Ok(Event::Key(key))) if key.kind == KeyEventKind::Press => {
                        handle_key(app, key, req_tx).await;
                    }
                    Some(Ok(_)) => {}          // resize, mouse, focus: just redraw
                    Some(Err(e)) => return Err(e.into()),
                    None => return Ok(()),     // stdin closed
                }
            }
            Some(msg) = msg_rx.recv() => match msg {
                Message::Apps(result) => {
                    if let Some(id) = app.apply_apps(result) {
                        let _ = req_tx.send(Request::WatchApp(id)).await;
                    }
                }
                Message::Snapshot { app_id, result } => {
                    if !app.paused {
                        app.apply_snapshot(&app_id, result);
                    }
                }
                Message::Detail { detail, result } => {
                    if !app.paused {
                        app.apply_detail(detail, result);
                    }
                }
            },
            _ = tick.tick() => {}
        }
    }
}

async fn handle_key(app: &mut App, key: KeyEvent, req_tx: &mpsc::Sender<Request>) {
    // Keys that mean the same thing on every screen.
    match key.code {
        KeyCode::Char('q') => return app.should_quit = true,
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            return app.should_quit = true;
        }
        KeyCode::Down | KeyCode::Char('j') => return app.move_selection(1),
        KeyCode::Up | KeyCode::Char('k') => return app.move_selection(-1),
        KeyCode::PageDown => return app.move_selection(10),
        KeyCode::PageUp => return app.move_selection(-10),
        KeyCode::Char('g') | KeyCode::Home => return app.select_edge(false),
        KeyCode::Char('G') | KeyCode::End => return app.select_edge(true),
        // Waking the poller early: it is parked in a select! on this channel.
        KeyCode::Char('r') => return send(req_tx, Request::RefreshNow).await,
        _ => {}
    }

    match app.view {
        View::Picker => match key.code {
            KeyCode::Enter => {
                if let Some(id) = app.picked_app() {
                    app.watch(id.clone());
                    send(req_tx, Request::WatchApp(id)).await;
                }
            }
            // Esc goes back to the app we were watching, if any.
            KeyCode::Esc if app.watching.is_some() => app.view = View::Main,
            _ => {}
        },
        View::Stage => match key.code {
            KeyCode::Esc | KeyCode::Backspace => {
                app.close_detail();
                send(req_tx, Request::SetDetail(None)).await;
            }
            KeyCode::Char('f') => app.toggle_failed_tasks(),
            KeyCode::Char('p') => app.paused = !app.paused,
            _ => {}
        },
        View::Main => match key.code {
            // Esc peels back one layer: a stage filter first, then the app.
            KeyCode::Esc => {
                if !app.clear_stage_filter() {
                    app.should_quit = true;
                }
            }
            KeyCode::Enter => match app.tab {
                app::Tab::Stages => {
                    if let Some(target) = app.open_stage_detail() {
                        send(req_tx, Request::SetDetail(Some(target))).await;
                        send(req_tx, Request::RefreshNow).await;
                    }
                }
                app::Tab::Jobs => app.filter_stages_by_selected_job(),
                _ => {}
            },
            KeyCode::Tab | KeyCode::Right | KeyCode::Char('l') => app.tab = app.tab.next(),
            KeyCode::BackTab | KeyCode::Left | KeyCode::Char('h') => app.tab = app.tab.prev(),
            KeyCode::Char(c @ '1'..='4') => {
                app.tab = app::Tab::ALL[c as usize - '1' as usize];
            }
            KeyCode::Char('a') => {
                app.open_picker();
                send(req_tx, Request::ListApps).await;
            }
            KeyCode::Char('p') => app.paused = !app.paused,
            KeyCode::Char('+') | KeyCode::Char('=') => {
                app.bump_interval(true);
                send(req_tx, Request::SetInterval(app.interval)).await;
            }
            KeyCode::Char('-') => {
                app.bump_interval(false);
                send(req_tx, Request::SetInterval(app.interval)).await;
            }
            _ => {}
        },
    }
}

async fn send(req_tx: &mpsc::Sender<Request>, req: Request) {
    // The only way this fails is the poller having exited, and then we are
    // shutting down anyway.
    let _ = req_tx.send(req).await;
}
