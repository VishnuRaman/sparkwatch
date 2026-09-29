mod alerts;
mod analysis;
mod app;
mod config;
mod k8s;
mod logview;
mod poller;
mod spark;
mod streaming;
mod threads;
mod ui;

use anyhow::Result;
use app::{App, View};
use clap::Parser;
use crossterm::event::{Event, EventStream, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use futures::StreamExt;
use logview::LogTarget;
use poller::{Message, Request, Source};
use spark::SparkClient;
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc;
use tokio::time::interval;

/// Terminal monitor for Apache Spark applications.
#[derive(Parser, Debug)]
#[command(
    name = "sparkwatch",
    version,
    after_help = "\
Examples:
  sparkwatch                                  driver UI on localhost:4040
  sparkwatch http://history:18080             History Server; pick an app
  sparkwatch http://history:18080 -a app-123  History Server; watch one app
  sparkwatch --k8s -n spark                   pick a running driver pod
  sparkwatch --k8s -n spark my-etl            watch the SparkApplication my-etl
  sparkwatch --k8s --context prod -n spark    another cluster from your kubeconfig
  sparkwatch prod                             a target from ~/.config/sparkwatch.toml

Config file (~/.config/sparkwatch.toml or $XDG_CONFIG_HOME/sparkwatch.toml):
  [defaults]
  interval = 2
  timeout = 5
  [targets.prod]
  k8s = true
  context = \"prod\"       # optional kubeconfig context
  namespace = \"spark\"
  app = \"my-etl\"          # optional
  [targets.history]
  url = \"http://history:18080\""
)]
struct Cli {
    /// Spark driver UI (http://host:4040), History Server (http://host:18080),
    /// or a target name from the config file. With --k8s: the SparkApplication name.
    target: Option<String>,

    /// Find Spark driver pods with kubectl and port-forward to them
    #[arg(long)]
    k8s: bool,

    /// Kubernetes namespace (default: the current kubectl context's)
    #[arg(short = 'n', long)]
    namespace: Option<String>,

    /// kubeconfig context to use with --k8s (default: the current one)
    #[arg(long, value_name = "NAME")]
    context: Option<String>,

    /// Application id to watch; skips the picker when the endpoint lists several
    #[arg(short, long, conflicts_with = "k8s")]
    app: Option<String>,

    /// Poll interval in seconds [default: 2, or [defaults].interval]
    #[arg(short, long)]
    interval: Option<u64>,

    /// HTTP timeout in seconds [default: 5, or [defaults].timeout]
    #[arg(short, long)]
    timeout: Option<u64>,

    /// Config file [default: ~/.config/sparkwatch.toml]
    #[arg(long, value_name = "PATH")]
    config: Option<std::path::PathBuf>,

    /// List the targets in the config file and exit
    #[arg(long)]
    targets: bool,
}

/// Fold a named config target into the CLI: the target supplies what the
/// command line left unset.
fn resolve_target(mut cli: Cli, cfg: &config::Config) -> Cli {
    if cli.k8s {
        return cli; // positional is an app name here, never a target name
    }
    let Some(t) = cli
        .target
        .as_ref()
        .and_then(|n| cfg.targets.get(n))
        .cloned()
    else {
        return cli;
    };
    if t.k8s {
        cli.k8s = true;
        cli.namespace = cli.namespace.or(t.namespace);
        cli.context = cli.context.or(t.context);
        cli.target = t.app;
        cli.app = None;
    } else {
        cli.target = t.url;
        cli.app = cli.app.or(t.app);
    }
    cli
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let cfg = config::load(cli.config.clone())?;
    if cli.targets {
        if cfg.targets.is_empty() {
            println!(
                "no targets configured ({})",
                config::default_path()
                    .map(|p| p.display().to_string())
                    .unwrap_or_default()
            );
        }
        for (name, t) in &cfg.targets {
            let how = if t.k8s {
                format!(
                    "k8s{}",
                    t.namespace
                        .as_ref()
                        .map(|n| format!(" -n {n}"))
                        .unwrap_or_default()
                )
            } else {
                t.url.clone().unwrap_or_default()
            };
            println!(
                "{name:<16} {how}{}",
                t.app
                    .as_ref()
                    .map(|a| format!("  app {a}"))
                    .unwrap_or_default()
            );
        }
        return Ok(());
    }
    let cli = resolve_target(cli, &cfg);
    let interval = Duration::from_secs(cli.interval.or(cfg.defaults.interval).unwrap_or(2).max(1));
    let timeout = Duration::from_secs(cli.timeout.or(cfg.defaults.timeout).unwrap_or(5).max(1));

    // What the header shows as the endpoint, and which app (if any) to start on.
    let (source, endpoint, watch) = if cli.k8s {
        let kube = k8s::Kube {
            namespace: cli.namespace,
            context: cli.context,
        };
        let endpoint = kube.describe();
        let source = Source::Kube {
            kube,
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
                        keylog(&key);
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
                Message::Log(event) => app.apply_log(event),
                Message::Threads { executor_id, result } => app.apply_threads(&executor_id, result),
                Message::Progress(p) => app.apply_progress(p),
                Message::ProgressStatus(s) => app.streaming_status = Some(s),
            },
            _ = tick.tick() => {}
        }
    }
}

async fn handle_key(app: &mut App, key: KeyEvent, req_tx: &mpsc::Sender<Request>) {
    // Typing a filter: every key is text until Enter/Esc.
    if app.filter_input_key(key.code) {
        return;
    }

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
        KeyCode::Char('r') => {
            if app.view == View::Threads {
                app.threads.error = None;
                send(
                    req_tx,
                    Request::FetchThreads(app.threads.executor_id.clone()),
                )
                .await;
            }
            return send(req_tx, Request::RefreshNow).await;
        }
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
            KeyCode::Char('L') => open_logs(app, req_tx, App::open_logs_selected_task).await,
            KeyCode::Char('p') => app.paused = !app.paused,
            _ => {}
        },
        View::Sql => match key.code {
            KeyCode::Esc | KeyCode::Backspace => {
                app.close_detail();
                send(req_tx, Request::SetDetail(None)).await;
            }
            KeyCode::Tab | KeyCode::BackTab => app.toggle_sql_focus(),
            KeyCode::Char('p') => app.toggle_plan_only(),
            _ => {}
        },
        View::Rdd => match key.code {
            KeyCode::Esc | KeyCode::Backspace => {
                app.close_detail();
                send(req_tx, Request::SetDetail(None)).await;
            }
            _ => {}
        },
        View::Alert => match key.code {
            KeyCode::Esc | KeyCode::Backspace => app.close_alert(),
            KeyCode::Char('s') => open_alert_stage(app, req_tx).await,
            KeyCode::Char('L') => open_logs(app, req_tx, App::open_logs_selected_alert).await,
            KeyCode::Char('x') => app.alerts.acknowledge(),
            _ => {}
        },
        View::Logs => match key.code {
            KeyCode::Esc | KeyCode::Backspace => {
                app.close_logs();
                send(req_tx, Request::CloseLogs).await;
            }
            KeyCode::Char('/') => app.logs.start_filter(),
            KeyCode::Char('c') => app.logs.clear_filter(),
            KeyCode::Char('w') => app.logs.wrap = !app.logs.wrap,
            KeyCode::Char('F') => app.logs.scroll_to_end(),
            KeyCode::Char('P') => open_logs(app, req_tx, App::logs_toggle_previous).await,
            KeyCode::Char('o') => open_logs(app, req_tx, App::logs_toggle_stream).await,
            KeyCode::Char('t') => {
                if let Some(t) = app.logs.target.clone() {
                    send(req_tx, Request::CloseLogs).await;
                    app.logs.close();
                    let id = app.open_threads(t.executor_id);
                    send(req_tx, Request::FetchThreads(id)).await;
                }
            }
            _ => {}
        },
        View::Threads => match key.code {
            KeyCode::Esc | KeyCode::Backspace => app.close_threads(),
            KeyCode::Char('/') => {
                app.threads.filter_input = Some(app.threads.filter.clone().unwrap_or_default())
            }
            KeyCode::Char('c') => app.threads.filter = None,
            KeyCode::Char('e') => app.threads.expanded = !app.threads.expanded,
            KeyCode::Char('L') => {
                let id = app.threads.executor_id.clone();
                app.threads = app::ThreadsView::default();
                let target = app.open_logs(id, None);
                send(req_tx, Request::OpenLogs(target)).await;
            }
            _ => {}
        },
        View::Main => match key.code {
            // Esc peels back one layer: the text filter, the job's stage
            // filter, then the app.
            KeyCode::Esc => {
                if !app.clear_table_filter() && !app.clear_stage_filter() {
                    app.should_quit = true;
                }
            }
            KeyCode::Char('/') => app.start_table_filter(),
            KeyCode::Char('c') => {
                app.clear_table_filter();
            }
            KeyCode::Enter => match app.tab {
                app::Tab::Stages => {
                    if let Some(target) = app.open_stage_detail() {
                        send(req_tx, Request::SetDetail(Some(target))).await;
                        send(req_tx, Request::RefreshNow).await;
                    }
                }
                app::Tab::Sql => {
                    if let Some(target) = app.open_sql_detail() {
                        send(req_tx, Request::SetDetail(Some(target))).await;
                        send(req_tx, Request::RefreshNow).await;
                    }
                }
                app::Tab::Jobs => app.filter_stages_by_selected_job(),
                app::Tab::Failures => app.open_alert(),
                app::Tab::Storage => {
                    if let Some(target) = app.open_rdd_detail() {
                        send(req_tx, Request::SetDetail(Some(target))).await;
                        send(req_tx, Request::RefreshNow).await;
                    }
                }
                _ => {}
            },
            KeyCode::Char('x') => app.alerts.acknowledge(),
            KeyCode::Char('s') if app.tab == app::Tab::Failures => {
                open_alert_stage(app, req_tx).await;
            }
            KeyCode::Char('L') if app.tab == app::Tab::Failures => {
                open_logs(app, req_tx, App::open_logs_selected_alert).await;
            }
            KeyCode::Char('L') if app.tab == app::Tab::Executors => {
                open_logs(app, req_tx, App::open_logs_selected_executor).await;
            }
            KeyCode::Char('t') if app.tab == app::Tab::Executors => {
                if let Some(id) = app.open_threads_selected_executor() {
                    send(req_tx, Request::FetchThreads(id)).await;
                }
            }
            KeyCode::Tab | KeyCode::Right | KeyCode::Char('l') => {
                app.tab = app.tab.next();
                tap_if_streaming(app, req_tx).await;
            }
            KeyCode::BackTab | KeyCode::Left | KeyCode::Char('h') => {
                app.tab = app.tab.prev();
                tap_if_streaming(app, req_tx).await;
            }
            KeyCode::Char(c @ '1'..='8') => {
                app.tab = app::Tab::ALL[c as usize - '1' as usize];
                tap_if_streaming(app, req_tx).await;
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

/// The first visit to the Streaming tab starts the driver log tap, which
/// then runs for as long as this app is watched.
async fn tap_if_streaming(app: &mut App, req_tx: &mpsc::Sender<Request>) {
    if app.tab == app::Tab::Streaming && app.want_tap() {
        send(req_tx, Request::TapProgress).await;
    }
}

/// Open the log view via one of `App`'s `open_logs_*` helpers and start the
/// stream for whatever target it chose.
async fn open_logs(
    app: &mut App,
    req_tx: &mpsc::Sender<Request>,
    pick: fn(&mut App) -> Option<LogTarget>,
) {
    if let Some(target) = pick(app) {
        send(req_tx, Request::OpenLogs(target)).await;
    }
}

async fn open_alert_stage(app: &mut App, req_tx: &mpsc::Sender<Request>) {
    if let Some(target) = app.open_alert_stage() {
        send(req_tx, Request::SetDetail(Some(target))).await;
        send(req_tx, Request::RefreshNow).await;
    }
}

/// Debug aid for headless runs: `SPARKWATCH_KEYLOG=/path` appends every key
/// press received, so "key ignored" and "key never arrived" can be told apart.
fn keylog(key: &KeyEvent) {
    static START: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    let t0 = START.get_or_init(std::time::Instant::now);
    if let Ok(path) = std::env::var("SPARKWATCH_KEYLOG") {
        use std::io::Write;
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
        {
            let _ = writeln!(
                f,
                "+{}ms {:?} {:?}",
                t0.elapsed().as_millis(),
                key.code,
                key.modifiers
            );
        }
    }
}

async fn send(req_tx: &mpsc::Sender<Request>, req: Request) {
    // The only way this fails is the poller having exited, and then we are
    // shutting down anyway.
    let _ = req_tx.send(req).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cli(args: &[&str]) -> Cli {
        Cli::parse_from(std::iter::once("sparkwatch").chain(args.iter().copied()))
    }

    fn cfg() -> config::Config {
        config::parse(
            "[targets.prod]\nk8s = true\ncontext = \"gke-prod\"\nnamespace = \"spark\"\napp = \"my-etl\"\n[targets.history]\nurl = \"http://h:18080\"\napp = \"app-1\"",
        )
        .unwrap()
    }

    #[test]
    fn target_name_expands_to_its_settings() {
        let c = resolve_target(cli(&["prod"]), &cfg());
        assert!(c.k8s);
        assert_eq!(c.namespace.as_deref(), Some("spark"));
        assert_eq!(c.context.as_deref(), Some("gke-prod"));
        assert_eq!(c.target.as_deref(), Some("my-etl"));

        let c = resolve_target(cli(&["history"]), &cfg());
        assert!(!c.k8s);
        assert_eq!(c.target.as_deref(), Some("http://h:18080"));
        assert_eq!(c.app.as_deref(), Some("app-1"));
    }

    #[test]
    fn explicit_flags_win_and_unknown_names_pass_through() {
        let c = resolve_target(
            cli(&["prod", "-n", "other", "--context", "staging"]),
            &cfg(),
        );
        assert_eq!(c.namespace.as_deref(), Some("other"));
        assert_eq!(c.context.as_deref(), Some("staging"));
        let c = resolve_target(cli(&["http://x:4040"]), &cfg());
        assert_eq!(c.target.as_deref(), Some("http://x:4040"));
        // With --k8s the positional is an app name, even if a target shares it.
        let c = resolve_target(cli(&["--k8s", "prod"]), &cfg());
        assert_eq!(c.target.as_deref(), Some("prod"));
        assert_eq!(c.namespace, None);
    }
}
