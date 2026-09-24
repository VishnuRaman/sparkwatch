//! The log viewer.

use crate::logview::{severity, LogView, Severity};
use ratatui::{
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Wrap},
    Frame,
};

/// Draws the viewer and reports how many log rows fit, so scrolling can
/// page by the real viewport height.
pub fn draw(f: &mut Frame, area: Rect, logs: &LogView, viewport_rows: &mut usize) {
    let Some(t) = &logs.target else { return };

    let (win, start) = logs.window(area.height.saturating_sub(3) as usize);
    let shown = logs.visible().len();
    let mut title = format!(" Logs · executor {} ", t.executor_id);
    if t.previous {
        title.push_str("· previous container ");
    }
    if t.http_url.is_some() {
        title.push_str(&format!("· {} ", t.stream.name()));
    }
    title.push_str(&format!("· {}-{} of {} lines", start + 1, start + win.len(), shown));
    if logs.filter.is_some() {
        title.push_str(&format!(" ({} unfiltered)", logs.total()));
    }
    if logs.follow {
        title.push_str(" · following ");
    } else {
        title.push_str(" · F to follow ");
    }

    let block = Block::default().borders(Borders::ALL).title(title);
    let inner = block.inner(area);
    f.render_widget(block, area);
    let [body, bar] = Layout::vertical([Constraint::Min(1), Constraint::Length(1)]).areas(inner);
    *viewport_rows = body.height as usize;

    let lines: Vec<Line> = win
        .iter()
        .map(|l| {
            let style = match severity(l) {
                Severity::Error => Style::default().fg(Color::Red),
                Severity::Warn => Style::default().fg(Color::Yellow),
                Severity::Plain => Style::default(),
            };
            Line::from(Span::styled(l.to_string(), style))
        })
        .collect();
    let mut para = Paragraph::new(lines);
    if logs.wrap {
        para = para.wrap(Wrap { trim: false });
    }
    f.render_widget(para, body);

    // Bottom bar: the filter being typed, else the active filter + status.
    let bar_line = match &logs.filter_input {
        Some(input) => Line::from(vec![
            Span::styled(" /", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
            Span::raw(input.clone()),
            Span::styled("█", Style::default().fg(Color::Cyan)),
            "  Enter apply · Esc cancel".dark_gray(),
        ]),
        None => {
            let mut spans = vec![Span::raw(" ")];
            if let Some(fl) = &logs.filter {
                spans.push(Span::styled(
                    format!("filter: {fl} "),
                    Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
                ));
                spans.push("(c clears) ".dark_gray());
            }
            if let Some(s) = &logs.status {
                let style = if s.starts_with("pod gone") || s.contains("failed") || s.contains("ended") || s.starts_with("no log") {
                    Style::default().fg(Color::Red)
                } else {
                    Style::default().fg(Color::DarkGray)
                };
                spans.push(Span::styled(s.clone(), style));
            } else if logs.total() == 0 {
                spans.push("waiting for lines…".dark_gray());
            }
            Line::from(spans)
        }
    };
    f.render_widget(Paragraph::new(bar_line), bar);
}
