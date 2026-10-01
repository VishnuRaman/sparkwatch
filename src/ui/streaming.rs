//! Streaming tab: per query, the numbers the Structured Streaming UI page
//! shows, built from progress events and micro-batch SQL executions.

use super::{fmt_bytes, fmt_millis, selected_style};
use crate::streaming::{QueryHistory, QueryStats, Streaming, tail};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Sparkline, Wrap},
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

pub fn draw(f: &mut Frame, area: Rect, s: &Streaming, status: Option<&str>, selected: usize) {
    if s.is_empty() {
        let mut lines = vec![
            Line::from(""),
            Line::from("  No streaming queries seen yet.".dark_gray()),
            Line::from(""),
            Line::from("  This tab needs one of:"),
            Line::from(
                "   · the driver log at INFO for org.apache.spark.sql.execution.streaming (--k8s, or YARN/standalone log URLs)",
            ),
            Line::from(
                "   · micro-batch SQL executions on the SQL tab (any endpoint, incl. the History Server) — durations only",
            ),
            Line::from(""),
        ];
        if let Some(st) = status {
            lines.push(Line::from(vec!["  tap: ".dark_gray(), st.into()]));
        }
        f.render_widget(
            Paragraph::new(lines)
                .wrap(Wrap { trim: false })
                .block(Block::default().borders(Borders::ALL).title(" Streaming ")),
            area,
        );
        return;
    }

    // By name, so j/k and the selected index don't shift when a restarted
    // query comes back under a new (random) id.
    let mut queries: Vec<&QueryHistory> = s.queries.values().collect();
    queries.sort_by_key(|q| q.label());
    let sel = selected.min(queries.len() - 1);
    let others = queries.len().saturating_sub(1);

    // Selected query gets the panel; the rest one summary row each.
    let [panel, list, foot] = Layout::vertical([
        Constraint::Min(14),
        Constraint::Length(if others > 0 {
            queries.len() as u16 + 2
        } else {
            0
        }),
        Constraint::Length(1),
    ])
    .areas(area);

    draw_query(f, panel, queries[sel]);

    if others > 0 {
        let lines: Vec<Line> = queries
            .iter()
            .enumerate()
            .map(|(i, q)| {
                let st = QueryStats::of(q);
                let mut line = Line::from(vec![
                    Span::raw(if i == sel { "▌ " } else { "  " }),
                    Span::styled(q.label(), Style::default().add_modifier(Modifier::BOLD)),
                    format!(
                        "  batch {}  trigger {}  in {}/s  processed {}/s",
                        st.latest.map_or(0, |b| b.batch_id),
                        fmt_millis(st.latest.map_or(0, |b| b.duration_ms)),
                        fmt_rate(st.input_rps),
                        fmt_rate(st.processed_rps)
                    )
                    .into(),
                ]);
                if st.behind {
                    line.push_span(Span::styled(
                        "  FALLING BEHIND",
                        Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
                    ));
                }
                if i == sel {
                    line = line.style(selected_style());
                }
                line
            })
            .collect();
        f.render_widget(
            Paragraph::new(lines).block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(format!(" Queries ({}) · j/k select ", queries.len())),
            ),
            list,
        );
    }

    let src = format!(
        " {} progress events from the driver log · {} batches from SQL executions{} ",
        s.progress_events,
        s.sql_batches,
        status.map(|st| format!(" · tap: {st}")).unwrap_or_default()
    );
    f.render_widget(Paragraph::new(Line::from(src.dark_gray())), foot);
}

fn draw_query(f: &mut Frame, area: Rect, q: &QueryHistory) {
    let st = QueryStats::of(q);
    let block = Block::default().borders(Borders::ALL).title(format!(
        " {} · run {} ",
        q.label(),
        q.run_id.chars().take(8).collect::<String>()
    ));
    let inner = block.inner(area);
    f.render_widget(block, area);

    let [head, charts, detail, recent] = Layout::vertical([
        Constraint::Length(2),
        Constraint::Length(7),
        Constraint::Length(6),
        Constraint::Min(4),
    ])
    .areas(inner);

    // ---- header: latest batch and the verdict
    let latest = st.latest;
    let latest_p = latest.and_then(|b| b.progress.as_ref());
    let mut l1 = vec![
        "batch ".dark_gray(),
        Span::styled(
            latest.map_or("-".into(), |b| b.batch_id.to_string()),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        "  trigger ".dark_gray(),
        fmt_millis(latest.map_or(0, |b| b.duration_ms)).into(),
        "  mean ".dark_gray(),
        fmt_millis(st.mean_ms).into(),
        "  p95 ".dark_gray(),
        fmt_millis(st.p95_ms).into(),
        "  max ".dark_gray(),
        fmt_millis(st.max_ms).into(),
    ];
    if st.batches_per_min > 0.0 {
        l1.push("  rate ".dark_gray());
        l1.push(format!("{:.1} batches/min", st.batches_per_min).into());
    }
    if let Some(b) = latest
        && !b.status.is_empty()
        && b.status != "COMPLETED"
    {
        l1.push("  ".into());
        l1.push(Span::styled(
            b.status.clone(),
            super::status_style(&b.status),
        ));
    }
    let mut l2 = match latest_p {
        Some(p) => vec![
            "input ".dark_gray(),
            format!("{} rows", fmt_num(p.num_input_rows)).into(),
            "  ".into(),
            Span::styled(
                format!("{} rows/s in", fmt_rate(st.input_rps)),
                Style::default().fg(Color::Yellow),
            ),
            "  vs  ".dark_gray(),
            Span::styled(
                format!("{} rows/s processed", fmt_rate(st.processed_rps)),
                Style::default().fg(if st.behind { Color::Red } else { Color::Green }),
            ),
        ],
        None => vec![
            "rates need the driver log (progress events); showing SQL batch durations only"
                .dark_gray(),
        ],
    };
    if st.behind {
        l2.push(Span::styled(
            "  ▲ FALLING BEHIND",
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        ));
    }
    if let Some(lag) = st.watermark_lag_ms {
        l2.push("  watermark lag ".dark_gray());
        l2.push(fmt_millis(lag).into());
    }
    if st.state_rows > 0 {
        l2.push("  state ".dark_gray());
        l2.push(
            format!(
                "{} rows / {}",
                fmt_num(st.state_rows),
                fmt_bytes(st.state_bytes)
            )
            .into(),
        );
    }
    f.render_widget(Paragraph::new(vec![Line::from(l1), Line::from(l2)]), head);

    // ---- sparklines
    let [c1, c2, c3] = Layout::horizontal([
        Constraint::Percentage(34),
        Constraint::Percentage(33),
        Constraint::Percentage(33),
    ])
    .areas(charts);
    let w = |r: Rect| r.width.saturating_sub(2) as usize;
    f.render_widget(
        Sparkline::default()
            .data(tail(&st.durations, w(c1)))
            .style(Style::default().fg(Color::Cyan))
            .block(Block::default().borders(Borders::ALL).title(format!(
                " Trigger duration · last {} batches ",
                st.durations.len().min(w(c1))
            ))),
        c1,
    );
    // Input over processed: two half-height sparklines so both trends read.
    let rate_block = Block::default().borders(Borders::ALL).title(format!(
        " Rows/s · in {} (yellow) · processed {} (green) ",
        fmt_rate(st.input_rps),
        fmt_rate(st.processed_rps)
    ));
    let rate_inner = rate_block.inner(c2);
    f.render_widget(rate_block, c2);
    let [top, bottom] = Layout::vertical([Constraint::Percentage(50), Constraint::Percentage(50)])
        .areas(rate_inner);
    let scale = st
        .input_series
        .iter()
        .chain(st.processed_series.iter())
        .max()
        .copied()
        .unwrap_or(1)
        .max(1);
    f.render_widget(
        Sparkline::default()
            .data(tail(&st.input_series, rate_inner.width as usize))
            .max(scale)
            .style(Style::default().fg(Color::Yellow)),
        top,
    );
    f.render_widget(
        Sparkline::default()
            .data(tail(&st.processed_series, rate_inner.width as usize))
            .max(scale)
            .style(Style::default().fg(if st.behind { Color::Red } else { Color::Green })),
        bottom,
    );
    f.render_widget(
        Sparkline::default()
            .data(tail(&st.state_series, w(c3)))
            .style(Style::default().fg(Color::Magenta))
            .block(Block::default().borders(Borders::ALL).title(format!(
                " State rows · {} · {} ",
                fmt_num(st.state_rows),
                fmt_bytes(st.state_bytes)
            ))),
        c3,
    );

    // ---- latest batch breakdown
    let mut lines = Vec::new();
    if let Some(p) = latest_p {
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
            p.duration_ms
                .get(*k)
                .map(|v| format!("{k} {}", fmt_millis(*v)))
        })
        .collect();
        lines.push(Line::from(vec![
            "durations ".dark_gray(),
            parts.join(" · ").into(),
        ]));
        for src in &p.sources {
            lines.push(Line::from(vec![
                "source ".dark_gray(),
                src.description.chars().take(60).collect::<String>().into(),
                format!(
                    "  {} rows · {}/s in · {}/s processed",
                    fmt_num(src.num_input_rows),
                    fmt_rate(src.input_rows_per_second),
                    fmt_rate(src.processed_rows_per_second)
                )
                .dark_gray(),
            ]));
        }
        if !p.sink.description.is_empty() {
            lines.push(Line::from(vec![
                "sink ".dark_gray(),
                p.sink
                    .description
                    .chars()
                    .take(60)
                    .collect::<String>()
                    .into(),
                format!("  {} rows out", fmt_num(p.sink.num_output_rows)).dark_gray(),
            ]));
        }
        for op in &p.state_operators {
            lines.push(Line::from(vec![
                "state ".dark_gray(),
                op.operator_name.clone().into(),
                format!(
                    "  {} rows ({} updated, {} dropped by watermark) · {}",
                    fmt_num(op.num_rows_total),
                    fmt_num(op.num_rows_updated),
                    fmt_num(op.num_rows_dropped_by_watermark),
                    fmt_bytes(op.memory_used_bytes)
                )
                .dark_gray(),
            ]));
        }
        lines.push(Line::from(vec![
            "at ".dark_gray(),
            p.timestamp.clone().into(),
        ]));
    } else {
        lines.push(Line::from(
            "no progress event for the latest batch — rates and breakdown need the driver log"
                .dark_gray(),
        ));
    }
    f.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), detail);

    draw_recent_batches(f, recent, q, &st);
}

/// One row per batch, newest first: the per-batch history that the
/// Structured Streaming UI page has and the REST API does not.
fn draw_recent_batches(f: &mut Frame, area: Rect, q: &QueryHistory, st: &QueryStats) {
    use ratatui::widgets::{Cell, Row, Table};

    let rows: Vec<Row> =
        q.batches
            .values()
            .rev()
            .take(area.height.saturating_sub(3) as usize)
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
                    Cell::from(status).style(super::status_style(status)),
                    Cell::from(fmt_millis(b.duration_ms)).style(if slow {
                        Style::default().fg(Color::Yellow)
                    } else {
                        Style::default()
                    }),
                    Cell::from(p.map_or("-".into(), |p| fmt_num(p.num_input_rows))),
                    Cell::from(p.map_or("-".into(), |p| fmt_rate(p.input_rows_per_second))),
                    Cell::from(p.map_or("-".into(), |p| fmt_rate(p.processed_rows_per_second)))
                        .style(if behind {
                            Style::default().fg(Color::Red)
                        } else {
                            Style::default()
                        }),
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
                    Cell::from(p.map_or(String::new(), |p| {
                        p.timestamp.chars().skip(11).take(8).collect::<String>()
                    })),
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
    .header(super::header_row(&[
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
    ]))
    .block(Block::default().borders(Borders::ALL).title(format!(
        " Recent batches ({} kept) · yellow trigger = above p95 · red rate = behind ",
        q.batches.len()
    )));
    f.render_widget(table, area);
}
