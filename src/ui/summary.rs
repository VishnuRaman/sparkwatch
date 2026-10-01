//! The "since you were away" summary screen.

use crate::report::Section;
use ratatui::{
    Frame,
    layout::Rect,
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Wrap},
};

/// Renders the sections; reports the line count for the scroll bound.
pub fn draw(f: &mut Frame, area: Rect, sections: &[Section], scroll: u16, total_lines: &mut usize) {
    let mut lines: Vec<Line> = Vec::new();
    for s in sections {
        lines.push(Line::from(Span::styled(
            format!("── {} ──", s.title),
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )));
        for l in &s.lines {
            let style = if l.contains("FAILED")
                || l.contains(" lost ")
                || l.contains("FALLING BEHIND")
                || l.contains("SKEW")
                || l.starts_with("GC:")
                || l.contains("% of spark.executor.memory")
            {
                Style::default().fg(Color::Red)
            } else if l.contains("none seen") || l.contains("all executors alive") {
                Style::default().fg(Color::Green)
            } else {
                Style::default()
            };
            lines.push(Line::from(vec![
                "  • ".dark_gray(),
                Span::styled(l.clone(), style),
            ]));
        }
        lines.push(Line::from(""));
    }
    lines.push(Line::from(
        "D writes this summary with the snapshot, failures, streaming history, environment and log tails as a bundle · Esc back"
            .dark_gray(),
    ));
    *total_lines = lines.len();
    f.render_widget(
        Paragraph::new(lines)
            .wrap(Wrap { trim: false })
            .scroll((scroll, 0))
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" Summary · what happened since the app started "),
            ),
        area,
    );
}
