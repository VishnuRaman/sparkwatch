//! Storage tab: cached RDDs / DataFrames, and one RDD's per-executor spread.

use super::{fmt_bytes, header_row, mini_bar, selected_style, table_block};
use crate::spark::{RddStorageInfo, Snapshot};
use ratatui::{
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table, TableState},
    Frame,
};
use std::collections::BTreeMap;

pub fn draw_list(f: &mut Frame, area: Rect, snap: &Snapshot, rdds: &[&RddStorageInfo], title: String, state: &mut TableState) {
    let [summary, list] = Layout::vertical([Constraint::Length(3), Constraint::Min(0)]).areas(area);

    // Storage memory is a slice of each executor's heap; this is the "is it
    // full" number the tab exists for.
    let used: i64 = snap.executors.iter().map(|e| e.memory_used).sum();
    let max: i64 = snap.executors.iter().map(|e| e.max_memory).sum();
    let disk: i64 = snap.executors.iter().map(|e| e.disk_used).sum();
    let pct = if max > 0 { 100.0 * used as f64 / max as f64 } else { 0.0 };
    let fullest = snap
        .executors
        .iter()
        .filter(|e| e.max_memory > 0)
        .max_by(|a, b| a.memory_ratio().partial_cmp(&b.memory_ratio()).unwrap())
        .map(|e| format!("fullest executor {} at {:.0}%", e.id, 100.0 * e.memory_ratio()))
        .unwrap_or_default();
    f.render_widget(
        Paragraph::new(Line::from(vec![
            "storage memory ".dark_gray(),
            Span::styled(
                format!("{} / {} ({pct:.0}%)", fmt_bytes(used), fmt_bytes(max)),
                if pct > 90.0 { Style::default().fg(Color::Red) } else { Style::default() },
            ),
            "  disk ".dark_gray(),
            fmt_bytes(disk).into(),
            "  ".into(),
            fullest.dark_gray(),
            "  cached ".dark_gray(),
            format!("{} RDDs", snap.rdds.len()).into(),
        ]))
        .block(Block::default().borders(Borders::ALL).title(" Storage ")),
        summary,
    );

    if snap.rdds.is_empty() {
        f.render_widget(
            Paragraph::new(vec![
                Line::from(""),
                Line::from("  Nothing cached.".dark_gray()),
                Line::from(Span::styled(
                    "  RDDs and DataFrames show up here after cache()/persist() once a job has materialised them. The History Server never has storage data.",
                    Style::default().fg(Color::DarkGray),
                )),
            ])
            .block(table_block(title)),
            list,
        );
        return;
    }

    let rows: Vec<Row> = rdds
        .iter()
        .map(|r| {
            let partial = r.num_cached_partitions < r.num_partitions;
            Row::new(vec![
                Cell::from(r.id.to_string()),
                Cell::from(r.name.chars().take(60).collect::<String>()),
                Cell::from(r.storage_level.clone()).style(Style::default().fg(Color::DarkGray)),
                Cell::from(format!("{}/{}", r.num_cached_partitions, r.num_partitions)).style(if partial {
                    Style::default().fg(Color::Yellow)
                } else {
                    Style::default()
                }),
                Cell::from(mini_bar(r.cached_ratio(), 10)),
                Cell::from(fmt_bytes(r.memory_used)),
                Cell::from(fmt_bytes(r.disk_used)).style(if r.disk_used > 0 {
                    Style::default().fg(Color::Magenta)
                } else {
                    Style::default().fg(Color::DarkGray)
                }),
            ])
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Length(5),
            Constraint::Min(30),
            Constraint::Length(36),
            Constraint::Length(11),
            Constraint::Length(11),
            Constraint::Length(10),
            Constraint::Length(10),
        ],
    )
    .header(header_row(&["ID", "NAME", "LEVEL", "PARTITIONS", "CACHED", "MEMORY", "DISK"]))
    .block(table_block(title))
    .row_highlight_style(selected_style())
    .highlight_symbol("▌");
    f.render_stateful_widget(table, area_of(list), state);
}

fn area_of(r: Rect) -> Rect {
    r
}

pub fn draw_detail(f: &mut Frame, area: Rect, rdd: Option<&RddStorageInfo>, error: Option<&str>) {
    let Some(r) = rdd else {
        let text = match error {
            Some(e) => Line::from(Span::styled(format!("Error: {e}"), Style::default().fg(Color::Red))),
            None => Line::from(Span::styled("Loading RDD…", Style::default().fg(Color::DarkGray))),
        };
        f.render_widget(Paragraph::new(text).block(Block::default().borders(Borders::ALL)), area);
        return;
    };

    let block = Block::default()
        .borders(Borders::ALL)
        .title(format!(" RDD {} · {} ", r.id, r.name));
    let inner = block.inner(area);
    f.render_widget(block, area);
    let [head, dist, parts] = Layout::vertical([
        Constraint::Length(2),
        Constraint::Percentage(55),
        Constraint::Min(3),
    ])
    .areas(inner);

    let partial = r.num_cached_partitions < r.num_partitions;
    f.render_widget(
        Paragraph::new(vec![
            Line::from(vec![
                "level ".dark_gray(),
                r.storage_level.clone().into(),
                "  partitions ".dark_gray(),
                Span::styled(
                    format!("{} of {} cached", r.num_cached_partitions, r.num_partitions),
                    if partial { Style::default().fg(Color::Yellow) } else { Style::default() },
                ),
                if partial {
                    "  (the rest will be recomputed when read)".yellow()
                } else {
                    Span::raw("")
                },
            ]),
            Line::from(vec![
                "memory ".dark_gray(),
                fmt_bytes(r.memory_used).into(),
                "  disk ".dark_gray(),
                fmt_bytes(r.disk_used).into(),
            ]),
        ]),
        head,
    );

    // Partitions per executor, so a lopsided cache is visible next to bytes.
    let mut per_exec: BTreeMap<&str, usize> = BTreeMap::new();
    for p in &r.partitions {
        for e in &p.executors {
            *per_exec.entry(e.as_str()).or_default() += 1;
        }
    }
    let total_mem: i64 = r.data_distribution.iter().map(|d| d.memory_used).sum::<i64>().max(1);
    let rows: Vec<Row> = r
        .data_distribution
        .iter()
        .map(|d| {
            let share = d.memory_used as f64 / total_mem as f64;
            let cap = d.memory_used + d.memory_remaining;
            let fill = if cap > 0 { d.memory_used as f64 / cap as f64 } else { 0.0 };
            Row::new(vec![
                Cell::from(d.address.clone()),
                Cell::from(per_exec.get(d.address.as_str()).map_or("-".into(), |n| n.to_string())),
                Cell::from(fmt_bytes(d.memory_used)),
                Cell::from(format!("{:.0}%", 100.0 * share)),
                Cell::from(mini_bar(share, 10)),
                Cell::from(fmt_bytes(d.memory_remaining)),
                Cell::from(format!("{:.0}%", 100.0 * fill)).style(if fill > 0.9 {
                    Style::default().fg(Color::Red)
                } else {
                    Style::default()
                }),
                Cell::from(fmt_bytes(d.disk_used)),
            ])
        })
        .collect();
    let table = Table::new(
        rows,
        [
            Constraint::Length(20),
            Constraint::Length(6),
            Constraint::Length(10),
            Constraint::Length(6),
            Constraint::Length(11),
            Constraint::Length(11),
            Constraint::Length(9),
            Constraint::Length(10),
        ],
    )
    .header(header_row(&[
        "EXECUTOR", "PARTS", "MEMORY", "SHARE", "", "REMAINING", "EXEC FULL", "DISK",
    ]))
    .block(table_block(format!(" Distribution across {} executors ", r.data_distribution.len())));
    f.render_widget(table, dist);

    let on_disk = r.partitions.iter().filter(|p| p.disk_used > 0).count();
    let biggest = r.partitions.iter().max_by_key(|p| p.memory_used + p.disk_used);
    let mut lines = vec![Line::from(vec![
        Span::styled(format!("{} partitions listed", r.partitions.len()), Style::default().add_modifier(Modifier::BOLD)),
        format!("  {} on disk", on_disk).dark_gray(),
    ])];
    if let Some(b) = biggest {
        lines.push(Line::from(vec![
            "largest ".dark_gray(),
            b.block_name.clone().into(),
            format!("  {} mem · {} disk · on {}", fmt_bytes(b.memory_used), fmt_bytes(b.disk_used), b.executors.join(", ")).dark_gray(),
        ]));
    }
    f.render_widget(Paragraph::new(lines), parts);
}
