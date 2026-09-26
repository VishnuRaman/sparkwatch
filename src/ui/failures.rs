//! The alert strip, the Failures tab, and the full-text alert view.

use super::{header_row, selected_style, table_block};
use crate::alerts::{Alert, AlertLog, Kind};
use ratatui::{
    Frame,
    layout::{Constraint, Rect},
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table, TableState, Wrap},
};
use std::time::Instant;

pub fn age(since: Instant) -> String {
    let s = since.elapsed().as_secs();
    if s < 60 {
        format!("{s}s ago")
    } else if s < 3600 {
        format!("{}m ago", s / 60)
    } else {
        format!("{}h{:02}m ago", s / 3600, (s % 3600) / 60)
    }
}

fn kind_style(kind: Kind) -> Style {
    match kind {
        Kind::Stage | Kind::Job | Kind::Sql => Style::default().fg(Color::Red),
        Kind::Task => Style::default().fg(Color::Yellow),
        Kind::Executor => Style::default().fg(Color::Magenta),
    }
}

/// One red line under the header while there are unacknowledged failures.
pub fn draw_strip(f: &mut Frame, area: Rect, log: &AlertLog) {
    let Some(latest) = log.latest_unacked() else {
        return;
    };
    let n = log.unacked();
    let text = format!(
        " ▲ {n} new failure{} · {} — {} · x acknowledge · 6 for all ",
        if n == 1 { "" } else { "s" },
        latest.title,
        latest.detail_line()
    );
    f.render_widget(
        Paragraph::new(Line::from(Span::styled(
            text,
            Style::default()
                .fg(Color::White)
                .bg(Color::Red)
                .add_modifier(Modifier::BOLD),
        ))),
        area,
    );
}

pub fn draw_table(
    f: &mut Frame,
    area: Rect,
    log: &AlertLog,
    rows_in: &[&Alert],
    title: String,
    state: &mut TableState,
) {
    if log.len() == 0 {
        f.render_widget(
            Paragraph::new(vec![
                Line::from(""),
                Line::from("  No failures seen since sparkwatch started.".green()),
                Line::from(Span::styled(
                    "  Failed stages, jobs, tasks, queries and lost/excluded executors collect here and stay, even after Spark forgets them.",
                    Style::default().fg(Color::DarkGray),
                )),
            ])
            .block(table_block(" Failures ".into())),
            area,
        );
        return;
    }

    let rows: Vec<Row> = rows_in
        .iter()
        .map(|a| {
            let row = Row::new(vec![
                Cell::from(age(a.first_seen)),
                Cell::from(a.kind.label()).style(kind_style(a.kind)),
                Cell::from(a.title.clone()),
                Cell::from(a.detail_line().chars().take(120).collect::<String>())
                    .style(Style::default().fg(Color::Gray)),
            ]);
            if log.is_new(a) {
                row.style(Style::default().add_modifier(Modifier::BOLD))
            } else {
                row.style(Style::default().fg(Color::DarkGray))
            }
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Length(10),
            Constraint::Length(9),
            Constraint::Percentage(45),
            Constraint::Min(20),
        ],
    )
    .header(header_row(&["WHEN", "KIND", "WHAT", "DETAIL"]))
    .block(table_block(title))
    .row_highlight_style(selected_style())
    .highlight_symbol("▌");

    f.render_stateful_widget(table, area, state);
}

/// The whole error text, wrapped and scrollable.
pub fn draw_detail(f: &mut Frame, area: Rect, alert: &Alert, scroll: u16) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(format!(" {} ", alert.title));
    let inner = block.inner(area);
    f.render_widget(block, area);

    let mut lines = vec![Line::from(vec![
        Span::styled(
            alert.kind.label(),
            kind_style(alert.kind).add_modifier(Modifier::BOLD),
        ),
        format!("  first seen {}", age(alert.first_seen)).dark_gray(),
        match alert.stage {
            Some((id, att)) => format!("  stage {id}.{att} (s to open)").dark_gray(),
            None => Span::raw(""),
        },
        match &alert.executor_id {
            Some(e) => format!("  executor {e}").dark_gray(),
            None => Span::raw(""),
        },
    ])];
    lines.push(Line::from(""));
    match &alert.detail {
        Some(d) => lines.extend(d.lines().map(|l| {
            // Stack frames fade back so the exception line stands out.
            if l.trim_start().starts_with("at ") {
                Line::from(Span::styled(
                    l.to_string(),
                    Style::default().fg(Color::DarkGray),
                ))
            } else {
                Line::from(l.to_string())
            }
        })),
        None => lines.push(Line::from(Span::styled(
            "(no further detail reported by Spark)",
            Style::default().fg(Color::DarkGray),
        ))),
    }
    f.render_widget(
        Paragraph::new(lines)
            .wrap(Wrap { trim: false })
            .scroll((scroll, 0)),
        inner,
    );
}
