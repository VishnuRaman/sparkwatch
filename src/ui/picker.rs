//! Application picker, shown when the endpoint lists more than one app.

use super::{fmt_millis, header_row, selected_style, table_block};
use crate::spark::ApplicationInfo;
use ratatui::{
    Frame,
    layout::{Constraint, Rect},
    style::{Color, Style},
    widgets::{Cell, Paragraph, Row, Table, TableState},
};

pub fn draw(f: &mut Frame, area: Rect, apps: &[ApplicationInfo], state: &mut TableState) {
    if apps.is_empty() {
        f.render_widget(
            Paragraph::new("Listing applications…")
                .style(Style::default().fg(Color::DarkGray))
                .block(table_block(" Applications ".into())),
            area,
        );
        return;
    }

    let rows: Vec<Row> = apps
        .iter()
        .map(|a| {
            // The History Server lists attempts newest first.
            let at = a.attempts.first().cloned().unwrap_or_default();
            let (state, style) = if at.completed {
                ("completed", Style::default().fg(Color::Green))
            } else {
                ("running", Style::default().fg(Color::Yellow))
            };
            Row::new(vec![
                Cell::from(a.id.clone()),
                Cell::from(a.name.chars().take(40).collect::<String>()),
                Cell::from(at.spark_user),
                Cell::from(at.start_time.chars().take(19).collect::<String>()),
                Cell::from(fmt_millis(at.duration)),
                Cell::from(state).style(style),
                Cell::from(at.app_spark_version),
            ])
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Length(32),
            Constraint::Min(20),
            Constraint::Length(12),
            Constraint::Length(20),
            Constraint::Length(9),
            Constraint::Length(10),
            Constraint::Length(8),
        ],
    )
    .header(header_row(&[
        "ID", "NAME", "USER", "STARTED", "DURATION", "STATE", "SPARK",
    ]))
    .block(table_block(format!(
        " Applications ({}) · Enter to watch ",
        apps.len()
    )))
    .row_highlight_style(selected_style())
    .highlight_symbol("▌");

    f.render_stateful_widget(table, area, state);
}
