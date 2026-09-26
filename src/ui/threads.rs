//! Thread dump view: contention first, Spark frames highlighted.

use crate::app::ThreadsView;
use crate::threads::{Group, ordered};
use ratatui::{
    Frame,
    layout::Rect,
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
};

const COLLAPSED_FRAMES: usize = 8;

fn group_style(g: Group) -> Style {
    match g {
        Group::Blocked => Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        Group::Waiting => Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD),
        Group::Runnable => Style::default()
            .fg(Color::Green)
            .add_modifier(Modifier::BOLD),
        Group::Idle => Style::default()
            .fg(Color::DarkGray)
            .add_modifier(Modifier::BOLD),
    }
}

/// Renders the dump and reports the total line count for scroll bounds.
pub fn draw(f: &mut Frame, area: Rect, tv: &ThreadsView, total_lines: &mut usize) {
    let mut title = format!(" Threads · executor {} ", tv.executor_id);
    let mut lines: Vec<Line> = Vec::new();

    match (&tv.threads, &tv.error) {
        (None, Some(e)) => {
            title.push_str("· unavailable ");
            lines.push(Line::from(Span::styled(
                e.clone(),
                Style::default().fg(Color::Red),
            )));
        }
        (None, None) => lines.push(Line::from("fetching thread dump…".dark_gray())),
        (Some(threads), _) => {
            let rows = ordered(threads, tv.filter.as_deref());
            let blocked = rows.iter().filter(|(g, _)| *g == Group::Blocked).count();
            title.push_str(&format!(
                "· {} threads{} · {} blocked · e {} · / filter · r refresh ",
                threads.len(),
                match &tv.filter {
                    Some(fl) => format!(" ({} match '{}')", rows.len(), fl),
                    None => String::new(),
                },
                blocked,
                if tv.expanded { "collapse" } else { "expand" }
            ));
            if let Some(e) = &tv.error {
                lines.push(Line::from(Span::styled(
                    format!("refresh failed: {e}"),
                    Style::default().fg(Color::Red),
                )));
            }

            let mut current: Option<Group> = None;
            for (g, t) in rows {
                if current != Some(g) {
                    current = Some(g);
                    lines.push(Line::from(""));
                    lines.push(Line::from(Span::styled(
                        format!("── {} ──", g.title()),
                        group_style(g),
                    )));
                }
                let mut head = vec![
                    Span::styled(
                        format!("#{} ", t.thread_id),
                        Style::default().fg(Color::DarkGray),
                    ),
                    Span::styled(
                        t.thread_name.clone(),
                        Style::default().add_modifier(Modifier::BOLD),
                    ),
                    Span::styled(format!("  [{}]", t.thread_state), group_style(g)),
                ];
                if let Some(b) = t.blocked_by_thread_id {
                    head.push(Span::styled(
                        format!(
                            "  blocked by #{b}{}",
                            t.lock_owner_name
                                .as_ref()
                                .map(|o| format!(" ({o})"))
                                .unwrap_or_default()
                        ),
                        Style::default().fg(Color::Red),
                    ));
                } else if let Some(l) = &t.lock_name {
                    head.push(Span::styled(
                        format!("  on {l}"),
                        Style::default().fg(Color::Yellow),
                    ));
                }
                if !t.holding_locks.is_empty() {
                    head.push(Span::styled(
                        format!("  holds {}", t.holding_locks.join(", ")),
                        Style::default().fg(Color::Magenta),
                    ));
                }
                lines.push(Line::from(head));

                let frames = t.frames();
                let shown = if tv.expanded {
                    frames.len()
                } else {
                    frames.len().min(COLLAPSED_FRAMES)
                };
                for fr in &frames[..shown] {
                    let style = if fr.starts_with("org.apache.spark") {
                        Style::default().fg(Color::Cyan)
                    } else {
                        Style::default().fg(Color::DarkGray)
                    };
                    lines.push(Line::from(Span::styled(format!("      at {fr}"), style)));
                }
                if shown < frames.len() {
                    lines.push(Line::from(Span::styled(
                        format!("      … {} more", frames.len() - shown),
                        Style::default().fg(Color::DarkGray),
                    )));
                }
            }
        }
    }

    if let Some(input) = &tv.filter_input {
        lines.insert(
            0,
            Line::from(vec![
                Span::styled(
                    "/",
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw(input.clone()),
                Span::styled("█", Style::default().fg(Color::Cyan)),
                "  Enter apply · Esc cancel".dark_gray(),
            ]),
        );
    }

    *total_lines = lines.len();
    f.render_widget(
        Paragraph::new(lines)
            .scroll((tv.scroll, 0))
            .block(Block::default().borders(Borders::ALL).title(title)),
        area,
    );
}
