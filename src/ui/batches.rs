//! Per-batch drill-down for a streaming query: the batch list, and one
//! batch's numbers, stages, failures.

use super::{
    display_name, fmt_bytes, fmt_millis, header_row, selected_style, status_style, table_block,
};
use crate::alerts::Alert;
use crate::spark::StageData;
use crate::streaming::{Batch, QueryHistory, QueryStats, fmt_clock};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table, TableState, Wrap},
};

fn fmt_num(n: i64) -> String {
    let s = n.abs().to_string();
    let mut out = String::new();
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    if n < 0 { format!("-{out}") } else { out }
}

fn fmt_rate(r: f64) -> String {
    if r >= 1000.0 {
        format!("{:.1}k", r / 1000.0)
    } else {
        format!("{r:.0}")
    }
}

fn time_of(b: &Batch) -> String {
    let ts = b
        .progress
        .as_ref()
        .map(|p| p.timestamp.as_str())
        .filter(|t| !t.is_empty())
        .unwrap_or(b.submitted.as_str());
    ts.chars().skip(11).take(8).collect()
}

/// All batches of one query, newest first, with a cursor.
pub fn draw_list(
    f: &mut Frame,
    area: Rect,
    q: &QueryHistory,
    batches: &[&Batch],
    st: &QueryStats,
    state: &mut TableState,
    log_start: Option<i64>,
) {
    let rows: Vec<Row> = batches
        .iter()
        .map(|b| {
            let p = b.progress.as_ref();
            let status = if b.status.is_empty() {
                "-"
            } else {
                b.status.as_str()
            };
            let slow = st.p95_ms > 0 && b.duration_ms > st.p95_ms;
            let behind = p.is_some_and(|p| {
                p.num_input_rows > 0 && p.processed_rows_per_second < p.input_rows_per_second
            });
            let row = Row::new(vec![
                Cell::from(b.batch_id.to_string()),
                Cell::from(status).style(status_style(status)),
                Cell::from(fmt_millis(b.duration_ms)).style(if slow {
                    Style::default().fg(Color::Yellow)
                } else {
                    Style::default()
                }),
                Cell::from(p.map_or("-".into(), |p| fmt_num(p.num_input_rows))),
                Cell::from(p.map_or("-".into(), |p| fmt_rate(p.input_rows_per_second))),
                Cell::from(p.map_or("-".into(), |p| fmt_rate(p.processed_rows_per_second))).style(
                    if behind {
                        Style::default().fg(Color::Red)
                    } else {
                        Style::default()
                    },
                ),
                Cell::from(p.map_or("-".into(), |p| fmt_num(p.state_rows()))),
                Cell::from(p.map_or("-".into(), |p| {
                    p.duration_ms
                        .get("addBatch")
                        .map(|v| fmt_millis(*v))
                        .unwrap_or_else(|| "-".into())
                })),
                Cell::from(p.map_or("-".into(), |p| {
                    p.watermark_lag_ms()
                        .map(fmt_millis)
                        .unwrap_or_else(|| "-".into())
                })),
                Cell::from(time_of(b)),
            ]);
            if status == "FAILED" {
                row.style(Style::default().fg(Color::Red))
            } else {
                row
            }
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Length(8),
            Constraint::Length(10),
            Constraint::Length(9),
            Constraint::Length(12),
            Constraint::Length(9),
            Constraint::Length(11),
            Constraint::Length(12),
            Constraint::Length(9),
            Constraint::Length(9),
            Constraint::Min(8),
        ],
    )
    .header(header_row(&[
        "BATCH",
        "STATUS",
        "TRIGGER",
        "INPUT ROWS",
        "IN/S",
        "PROCESSED/S",
        "STATE ROWS",
        "ADDBATCH",
        "WM LAG",
        "AT",
    ]));
    // Rows with `-` for rows/rates are batches the driver log didn't cover;
    // say where the log starts so the dashes read as a fact, not a bug.
    let missing = batches.iter().filter(|b| b.progress.is_none()).count();
    let coverage = match (missing, log_start) {
        (0, _) => String::new(),
        (n, Some(t)) => format!(
            "· {n} before the driver log's start at {} have durations only ",
            fmt_clock(t)
        ),
        (n, None) => format!("· {n} without progress from the driver log "),
    };
    let table = table
        .block(table_block(format!(
            " Batches of {} · {} kept · mean {} · p95 {} {coverage}· Enter for detail ",
            q.label(),
            batches.len(),
            fmt_millis(st.mean_ms),
            fmt_millis(st.p95_ms)
        )))
        .row_highlight_style(selected_style())
        .highlight_symbol("▌");
    f.render_stateful_widget(table, area, state);
}

pub struct DetailProps<'a> {
    pub query: &'a QueryHistory,
    pub batch: &'a Batch,
    pub stats: &'a QueryStats<'a>,
    pub stages: &'a [&'a StageData],
    pub failures: &'a [&'a Alert],
    pub stage_state: &'a mut TableState,
    /// First driver log line the tap could read, when known.
    pub log_start: Option<i64>,
}

pub fn draw_detail(f: &mut Frame, area: Rect, p: DetailProps) {
    let b = p.batch;
    let block = Block::default().borders(Borders::ALL).title(format!(
        " {} · run {} · batch {} ",
        p.query.label(),
        p.query.run_id.chars().take(8).collect::<String>(),
        b.batch_id
    ));
    let inner = block.inner(area);
    f.render_widget(block, area);
    let [head, stages, failures] = Layout::vertical([
        Constraint::Length(7),
        Constraint::Percentage(50),
        Constraint::Min(4),
    ])
    .areas(inner);

    // ---- numbers
    let status = if b.status.is_empty() {
        "-"
    } else {
        b.status.as_str()
    };
    let vs_mean = if p.stats.mean_ms > 0 {
        b.duration_ms as f64 / p.stats.mean_ms as f64
    } else {
        0.0
    };
    let mut lines = vec![Line::from(vec![
        Span::styled(status, status_style(status).add_modifier(Modifier::BOLD)),
        "  trigger ".dark_gray(),
        Span::styled(
            fmt_millis(b.duration_ms),
            if p.stats.p95_ms > 0 && b.duration_ms > p.stats.p95_ms {
                Style::default().fg(Color::Yellow)
            } else {
                Style::default()
            },
        ),
        format!(
            "  ({vs_mean:.1}× the query's mean of {}, p95 {})",
            fmt_millis(p.stats.mean_ms),
            fmt_millis(p.stats.p95_ms)
        )
        .dark_gray(),
        "  at ".dark_gray(),
        time_of(b).into(),
    ])];
    match &b.progress {
        Some(pr) => {
            let behind =
                pr.num_input_rows > 0 && pr.processed_rows_per_second < pr.input_rows_per_second;
            let mut l = vec![
                "input ".dark_gray(),
                format!("{} rows", fmt_num(pr.num_input_rows)).into(),
                "  ".into(),
                Span::styled(
                    format!("{} rows/s in", fmt_rate(pr.input_rows_per_second)),
                    Style::default().fg(Color::Yellow),
                ),
                "  vs  ".dark_gray(),
                Span::styled(
                    format!(
                        "{} rows/s processed",
                        fmt_rate(pr.processed_rows_per_second)
                    ),
                    Style::default().fg(if behind { Color::Red } else { Color::Green }),
                ),
            ];
            if behind {
                l.push(Span::styled(
                    "  ▲ behind",
                    Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
                ));
            }
            if let Some(lag) = pr.watermark_lag_ms() {
                l.push("  watermark lag ".dark_gray());
                l.push(fmt_millis(lag).into());
            }
            if pr.state_rows() > 0 {
                l.push("  state ".dark_gray());
                l.push(
                    format!(
                        "{} rows / {}",
                        fmt_num(pr.state_rows()),
                        fmt_bytes(pr.state_bytes())
                    )
                    .into(),
                );
            }
            lines.push(Line::from(l));
            let parts: Vec<String> = [
                "addBatch",
                "getBatch",
                "latestOffset",
                "queryPlanning",
                "walCommit",
                "commitOffsets",
            ]
            .iter()
            .filter_map(|k| {
                pr.duration_ms
                    .get(*k)
                    .map(|v| format!("{k} {}", fmt_millis(*v)))
            })
            .collect();
            lines.push(Line::from(vec![
                "durations ".dark_gray(),
                parts.join(" · ").into(),
            ]));
            for src in &pr.sources {
                lines.push(Line::from(vec![
                    "source ".dark_gray(),
                    src.description.clone().into(),
                    format!(
                        "  {} rows · {}/s in · {}/s processed",
                        fmt_num(src.num_input_rows),
                        fmt_rate(src.input_rows_per_second),
                        fmt_rate(src.processed_rows_per_second)
                    )
                    .dark_gray(),
                ]));
            }
            if !pr.sink.description.is_empty() {
                lines.push(Line::from(vec![
                    "sink ".dark_gray(),
                    pr.sink.description.clone().into(),
                    format!("  {} rows out", fmt_num(pr.sink.num_output_rows)).dark_gray(),
                ]));
            }
        }
        None => {
            let why = match p.log_start {
                Some(t) if b.window_ms().is_some_and(|(_, end)| end < t) => format!(
                    "the driver log sparkwatch can read starts at {} (rotated by the kubelet, or a tail), after this batch",
                    fmt_clock(t)
                ),
                _ => "driver log not available, not at INFO, or not yet read this far".into(),
            };
            lines.push(Line::from(
                format!("no progress event for this batch — {why}; status and duration are from its SQL execution")
                    .dark_gray(),
            ));
        }
    }
    lines.push(Line::from(
        "Enter: stage drill-down · L: driver log for this batch's time window · Esc: back"
            .dark_gray(),
    ));
    f.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), head);

    // ---- stages of this batch
    let rows: Vec<Row> = p
        .stages
        .iter()
        .map(|st| {
            Row::new(vec![
                Cell::from(format!(
                    "{}.{}{}",
                    st.stage_id,
                    st.attempt_id,
                    if st.failure_reason.is_some() {
                        " ✗"
                    } else {
                        ""
                    }
                )),
                Cell::from(st.status.clone()).style(status_style(&st.status)),
                Cell::from(display_name(&st.name, st.description.as_deref(), None)),
                Cell::from(format!("{}/{}", st.num_complete_tasks, st.num_tasks)),
                Cell::from(st.num_failed_tasks.to_string())
                    .style(super::warn_if(st.num_failed_tasks)),
                Cell::from(fmt_millis(st.executor_run_time)),
                Cell::from(fmt_bytes(st.input_bytes)),
                Cell::from(fmt_bytes(st.shuffle_read_bytes)),
                Cell::from(fmt_bytes(st.shuffle_write_bytes)),
                Cell::from(fmt_bytes(st.memory_bytes_spilled)).style(
                    if st.memory_bytes_spilled > 0 {
                        Style::default().fg(Color::Magenta)
                    } else {
                        Style::default().fg(Color::DarkGray)
                    },
                ),
            ])
        })
        .collect();
    let table = Table::new(
        rows,
        [
            Constraint::Length(9),
            Constraint::Length(9),
            Constraint::Min(18),
            Constraint::Length(10),
            Constraint::Length(7),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(10),
        ],
    )
    .header(header_row(&[
        "STAGE",
        "STATUS",
        "NAME",
        "TASKS",
        "FAILED",
        "TASK TIME",
        "INPUT",
        "SHUF R",
        "SHUF W",
        "SPILL",
    ]))
    .block(table_block(if p.stages.is_empty() {
        " Stages of this batch · none still retained by the driver ".into()
    } else {
        format!(
            " Stages of this batch ({}) · Enter opens the drill-down ",
            p.stages.len()
        )
    }))
    .row_highlight_style(selected_style())
    .highlight_symbol("▌");
    f.render_stateful_widget(table, stages, p.stage_state);

    // ---- failures of this batch
    let lines: Vec<Line> = if p.failures.is_empty() {
        vec![Line::from(Span::styled(
            "none",
            Style::default().fg(Color::Green),
        ))]
    } else {
        p.failures
            .iter()
            .map(|a| {
                Line::from(vec![
                    Span::styled(
                        format!("{:<9}", a.kind.label()),
                        Style::default().fg(Color::Red),
                    ),
                    a.title.clone().into(),
                    format!("  {}", a.detail_line()).dark_gray(),
                ])
            })
            .collect()
    };
    f.render_widget(
        Paragraph::new(lines)
            .wrap(Wrap { trim: false })
            .block(table_block(format!(
                " Failures of this batch ({}) · full list on 6 ",
                p.failures.len()
            ))),
        failures,
    );
}
