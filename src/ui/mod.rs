//! Rendering. `draw` is the single entry point; each view lives in its own
//! submodule and shares the formatting helpers defined here.

mod overview;
mod tables;

use crate::app::{App, Tab};
use ratatui::{
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Paragraph, Row, Tabs},
    Frame,
};

// ---------------------------------------------------------------- formatting

pub fn fmt_bytes(b: i64) -> String {
    const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    if b <= 0 {
        return "0 B".into();
    }
    let mut v = b as f64;
    let mut i = 0;
    while v >= 1024.0 && i < UNITS.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{} {}", b, UNITS[i])
    } else {
        format!("{v:.1} {}", UNITS[i])
    }
}

pub fn fmt_millis(ms: i64) -> String {
    if ms <= 0 {
        return "-".into();
    }
    let s = ms / 1000;
    let (h, m, sec) = (s / 3600, (s % 3600) / 60, s % 60);
    if h > 0 {
        format!("{h}h{m:02}m")
    } else if m > 0 {
        format!("{m}m{sec:02}s")
    } else if s > 0 {
        format!("{s}.{:01}s", (ms % 1000) / 100)
    } else {
        format!("{ms}ms")
    }
}

/// Spark hands back timestamps like `2024-05-01T09:14:02.331GMT`, which is not
/// valid RFC 3339. Rather than pull in a date library just to reformat it, take
/// the useful part.
pub fn short_time(ts: &Option<String>) -> String {
    match ts {
        None => "-".into(),
        Some(t) => t
            .split('T')
            .nth(1)
            .map(|s| s.chars().take(8).collect())
            .unwrap_or_else(|| t.clone()),
    }
}

pub fn status_style(status: &str) -> Style {
    match status {
        "RUNNING" | "ACTIVE" => Style::default().fg(Color::Yellow),
        "SUCCEEDED" | "COMPLETE" => Style::default().fg(Color::Green),
        "FAILED" => Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        "PENDING" => Style::default().fg(Color::Blue),
        "SKIPPED" => Style::default().fg(Color::DarkGray),
        _ => Style::default().fg(Color::Gray),
    }
}

/// Unicode bar, useful inside a table cell where a Gauge cannot go.
pub fn mini_bar(ratio: f64, width: usize) -> String {
    let filled = ((ratio.clamp(0.0, 1.0)) * width as f64).round() as usize;
    format!("{}{}", "█".repeat(filled), "░".repeat(width - filled))
}

/// Red when the value is non-zero, dim otherwise. For failure counters.
pub fn warn_if(nonzero: i64) -> Style {
    if nonzero > 0 {
        Style::default().fg(Color::Red)
    } else {
        Style::default().fg(Color::DarkGray)
    }
}

pub fn table_block(title: String) -> Block<'static> {
    Block::default().borders(Borders::ALL).title(title)
}

pub fn selected_style() -> Style {
    Style::default().bg(Color::Rgb(40, 44, 60)).add_modifier(Modifier::BOLD)
}

pub fn header_row(cols: &[&'static str]) -> Row<'static> {
    Row::new(cols.iter().map(|c| Cell::from(*c)).collect::<Vec<_>>())
        .style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD))
        .height(1)
}

// -------------------------------------------------------------------- layout

pub fn draw(f: &mut Frame, app: &mut App) {
    let [header, body, footer] = Layout::vertical([
        Constraint::Length(3),
        Constraint::Min(0),
        Constraint::Length(1),
    ])
    .areas(f.area());

    draw_header(f, header, app);
    draw_footer(f, footer, app);

    // Destructure so the table states and the snapshot borrow disjointly.
    let App {
        tab,
        snapshot,
        jobs,
        stages,
        executors,
        ..
    } = app;

    let Some(snap) = snapshot.as_ref() else {
        let msg = Paragraph::new("Connecting to Spark…")
            .style(Style::default().fg(Color::DarkGray))
            .block(Block::default().borders(Borders::ALL));
        f.render_widget(msg, body);
        return;
    };

    match *tab {
        Tab::Overview => overview::draw(f, body, snap),
        Tab::Jobs => tables::draw_jobs(f, body, snap, &mut jobs.state),
        Tab::Stages => tables::draw_stages(f, body, snap, &mut stages.state),
        Tab::Executors => tables::draw_executors(f, body, snap, &mut executors.state),
    }
}

fn draw_header(f: &mut Frame, area: Rect, app: &App) {
    let titles: Vec<Line> = Tab::ALL.iter().map(|t| Line::from(t.title())).collect();

    let (name, id) = match &app.snapshot {
        Some(s) => (s.app.name.clone(), s.app.id.clone()),
        None => ("—".into(), "—".into()),
    };

    let age = app
        .last_update
        .map(|t| format!("{}s ago", t.elapsed().as_secs()))
        .unwrap_or_else(|| "never".into());

    let state = if app.paused {
        Span::styled(" PAUSED ", Style::default().fg(Color::Black).bg(Color::Yellow))
    } else if app.last_error.is_some() {
        Span::styled(" ERROR ", Style::default().fg(Color::White).bg(Color::Red))
    } else {
        Span::styled(" LIVE ", Style::default().fg(Color::Black).bg(Color::Green))
    };

    let title = Line::from(vec![
        Span::raw(" "),
        state,
        Span::raw(format!(
            " {name} [{id}] @ {} · every {}s · updated {age} ",
            app.endpoint,
            app.interval.as_secs()
        )),
    ]);

    let tabs = Tabs::new(titles)
        .select(app.tab.index())
        .highlight_style(
            Style::default()
                .fg(Color::Black)
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )
        .divider("│")
        .block(Block::default().borders(Borders::ALL).title(title));

    f.render_widget(tabs, area);
}

fn draw_footer(f: &mut Frame, area: Rect, app: &App) {
    let text = match &app.last_error {
        Some(e) => Line::from(Span::styled(
            format!(" {} ", e.lines().next().unwrap_or(e)),
            Style::default().fg(Color::Red),
        )),
        None => Line::from(Span::styled(
            " q quit · tab/←→ switch · j/k move · g/G top/bottom · r refresh · p pause · +/- interval ",
            Style::default().fg(Color::DarkGray),
        )),
    };
    f.render_widget(Paragraph::new(text), area);
}
