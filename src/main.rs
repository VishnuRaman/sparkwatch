mod app;
mod spark;
mod ui;

use anyhow::Result;
use app::App;
use crossterm::event::{Event, EventStream, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use futures::StreamExt;
use spark::{Snapshot, SparkClient};
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc;
use tokio::time::interval;

/// What the poller task sends back to the UI.
type PollResult = Result<Snapshot, String>;

#[tokio::main]
async fn main() -> Result<()> {
    // Minimal arg parsing; swap in `clap` when you want flags and help text.
    let mut args = std::env::args().skip(1);
    let endpoint = args
        .next()
        .unwrap_or_else(|| "http://localhost:4040".to_string());
    let app_id = args.next();
    let interval = Duration::from_secs(2);

    let client = SparkClient::new(endpoint.clone(), app_id, Duration::from_secs(5))?;

    let (tx, mut rx) = mpsc::channel::<PollResult>(8);
    let (refresh_tx, mut refresh_rx) = mpsc::channel::<Duration>(8);

    // Poller: owns all network I/O so the render loop never blocks on it.
    tokio::spawn({
        let client = client.clone();
        async move {
            let mut period = interval;
            loop {
                let result = client.poll().await.map_err(|e| format!("{e:#}"));
                if tx.send(result).await.is_err() {
                    break; // UI is gone
                }
                tokio::select! {
                    _ = tokio::time::sleep(period) => {}
                    Some(next) = refresh_rx.recv() => { period = next; }
                }
            }
        }
    });

    let mut terminal = ratatui::init();
    let mut app = App::new(endpoint, interval);
    let result = run(&mut terminal, &mut app, &mut rx, &refresh_tx).await;
    ratatui::restore();
    result
}

async fn run(
    terminal: &mut ratatui::DefaultTerminal,
    app: &mut App,
    rx: &mut mpsc::Receiver<PollResult>,
    refresh_tx: &mpsc::Sender<Duration>,
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
                        handle_key(app, key, refresh_tx).await;
                    }
                    Some(Ok(_)) => {}          // resize, mouse, focus: just redraw
                    Some(Err(e)) => return Err(e.into()),
                    None => return Ok(()),     // stdin closed
                }
            }
            Some(result) = rx.recv() => {
                if !app.paused {
                    app.apply(result);
                }
            }
            _ = tick.tick() => {}
        }
    }
}

async fn handle_key(app: &mut App, key: KeyEvent, refresh_tx: &mpsc::Sender<Duration>) {
    match key.code {
        KeyCode::Char('q') | KeyCode::Esc => app.should_quit = true,
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.should_quit = true
        }

        KeyCode::Tab | KeyCode::Right | KeyCode::Char('l') => app.tab = app.tab.next(),
        KeyCode::BackTab | KeyCode::Left | KeyCode::Char('h') => app.tab = app.tab.prev(),
        KeyCode::Char(c @ '1'..='4') => {
            app.tab = app::Tab::ALL[c as usize - '1' as usize];
        }

        KeyCode::Down | KeyCode::Char('j') => app.move_selection(1),
        KeyCode::Up | KeyCode::Char('k') => app.move_selection(-1),
        KeyCode::PageDown => app.move_selection(10),
        KeyCode::PageUp => app.move_selection(-10),
        KeyCode::Char('g') | KeyCode::Home => app.select_edge(false),
        KeyCode::Char('G') | KeyCode::End => app.select_edge(true),

        KeyCode::Char('p') => app.paused = !app.paused,
        // Waking the poller early: it is parked in a select! on this channel.
        KeyCode::Char('r') => {
            let _ = refresh_tx.send(app.interval).await;
        }
        KeyCode::Char('+') | KeyCode::Char('=') => {
            app.bump_interval(true);
            let _ = refresh_tx.send(app.interval).await;
        }
        KeyCode::Char('-') => {
            app.bump_interval(false);
            let _ = refresh_tx.send(app.interval).await;
        }
        _ => {}
    }
}
