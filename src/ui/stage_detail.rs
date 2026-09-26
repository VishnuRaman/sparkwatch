//! Stage drill-down: task metric distributions with skew flags, per-executor
//! breakdown, and the slowest (or failed) tasks.

use super::{fmt_bytes, fmt_millis, header_row, selected_style, status_style, table_block};
use crate::analysis::{self, Unit};
use crate::spark::{StageDetail, TaskData};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Gauge, Paragraph, Row, Table, TableState, Wrap},
};

pub struct Props<'a> {
    pub detail: Option<&'a StageDetail>,
    pub error: Option<&'a str>,
    pub tasks: &'a [TaskData],
    pub show_failed: bool,
    pub tasks_state: &'a mut TableState,
}

pub fn draw(f: &mut Frame, area: Rect, p: Props) {
    let Some(d) = p.detail else {
        let text = match p.error {
            Some(e) => Line::from(Span::styled(
                format!("Error: {e}"),
                Style::default().fg(Color::Red),
            )),
            None => Line::from(Span::styled(
                "Loading stage…",
                Style::default().fg(Color::DarkGray),
            )),
        };
        f.render_widget(
            Paragraph::new(text).block(Block::default().borders(Borders::ALL)),
            area,
        );
        return;
    };

    let has_failure = d.stage.failure_reason.is_some();
    let [head, middle, bottom] = Layout::vertical([
        Constraint::Length(if has_failure { 6 } else { 5 }),
        Constraint::Percentage(45),
        Constraint::Min(6),
    ])
    .areas(area);

    draw_head(f, head, d);

    // Side by side on a wide terminal, stacked on a narrow one so neither
    // table loses its rightmost columns.
    let [left, right] = if middle.width >= 160 {
        Layout::horizontal([Constraint::Percentage(55), Constraint::Percentage(45)]).areas(middle)
    } else {
        Layout::vertical([Constraint::Percentage(60), Constraint::Percentage(40)]).areas(middle)
    };
    draw_distributions(f, left, d);
    draw_executors(f, right, d);

    draw_tasks(f, bottom, d, p.tasks, p.show_failed, p.tasks_state);
}

fn draw_head(f: &mut Frame, area: Rect, d: &StageDetail) {
    let st = &d.stage;
    let block = Block::default().borders(Borders::ALL).title(format!(
        " Stage {}.{} · {} ",
        st.stage_id, st.attempt_id, st.name
    ));
    let inner = block.inner(area);
    f.render_widget(block, area);

    let [info, gauge, reason] = Layout::vertical([
        Constraint::Length(2),
        Constraint::Length(1),
        Constraint::Min(0),
    ])
    .areas(inner);

    let gc_pct = if st.executor_run_time > 0 {
        100.0 * st.jvm_gc_time as f64 / st.executor_run_time as f64
    } else {
        0.0
    };
    let lines = vec![
        Line::from(vec![
            Span::styled(
                st.status.clone(),
                status_style(&st.status).add_modifier(Modifier::BOLD),
            ),
            "  pool ".dark_gray(),
            st.scheduling_pool.clone().into(),
            "  tasks ".dark_gray(),
            format!(
                "{} done · {} running · {} failed · {} killed / {}",
                st.num_complete_tasks,
                st.num_active_tasks,
                st.num_failed_tasks,
                st.num_killed_tasks,
                st.num_tasks
            )
            .into(),
        ]),
        Line::from(vec![
            "input ".dark_gray(),
            fmt_bytes(st.input_bytes).into(),
            "  shuffle r ".dark_gray(),
            fmt_bytes(st.shuffle_read_bytes).into(),
            "  shuffle w ".dark_gray(),
            fmt_bytes(st.shuffle_write_bytes).into(),
            "  spill ".dark_gray(),
            Span::styled(
                format!(
                    "{} mem / {} disk",
                    fmt_bytes(st.memory_bytes_spilled),
                    fmt_bytes(st.disk_bytes_spilled)
                ),
                if st.memory_bytes_spilled > 0 {
                    Style::default().fg(Color::Magenta)
                } else {
                    Style::default()
                },
            ),
            "  task time ".dark_gray(),
            fmt_millis(st.executor_run_time).into(),
            "  gc ".dark_gray(),
            Span::styled(
                format!("{gc_pct:.1}%"),
                if gc_pct > 10.0 {
                    Style::default().fg(Color::Red)
                } else {
                    Style::default()
                },
            ),
        ]),
    ];
    f.render_widget(Paragraph::new(lines), info);

    f.render_widget(
        Gauge::default()
            .gauge_style(status_style(&st.status))
            .ratio(st.progress())
            .label(format!("{:.0}%", st.progress() * 100.0)),
        gauge,
    );

    if let Some(r) = &st.failure_reason {
        f.render_widget(
            Paragraph::new(Line::from(vec![
                "✗ ".red().bold(),
                Span::styled(
                    r.lines().next().unwrap_or(r).to_string(),
                    Style::default().fg(Color::Red),
                ),
            ]))
            .wrap(Wrap { trim: true }),
            reason,
        );
    }
}

fn fmt_value(v: f64, unit: Unit) -> String {
    match unit {
        Unit::Millis => fmt_millis(v.round() as i64),
        Unit::Bytes => fmt_bytes(v.round() as i64),
    }
}

fn draw_distributions(f: &mut Frame, area: Rect, d: &StageDetail) {
    let Some(summary) = &d.summary else {
        f.render_widget(
            Paragraph::new("no completed tasks yet")
                .style(Style::default().fg(Color::DarkGray))
                .block(table_block(" Task metrics ".into())),
            area,
        );
        return;
    };

    let rows: Vec<Row> = analysis::metric_rows(summary)
        .into_iter()
        .map(|m| {
            let skewed = m.is_skewed();
            let skew_cell = match m.skew {
                Some(r) if skewed => Cell::from(format!("⚠ ×{}", fmt_ratio(r))).style(
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
                Some(r) => Cell::from(format!("×{}", fmt_ratio(r)))
                    .style(Style::default().fg(Color::DarkGray)),
                None => Cell::from(""),
            };
            let mut cells = vec![Cell::from(m.name)];
            cells.extend(m.values.iter().map(|v| Cell::from(fmt_value(*v, m.unit))));
            // Pad if Spark returned fewer quantiles than we asked for.
            while cells.len() < 7 {
                cells.push(Cell::from("-"));
            }
            cells.push(skew_cell);
            let row = Row::new(cells);
            if skewed {
                row.style(Style::default().fg(Color::Yellow))
            } else {
                row
            }
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Length(14),
            Constraint::Length(9),
            Constraint::Length(9),
            Constraint::Length(9),
            Constraint::Length(9),
            Constraint::Length(9),
            Constraint::Length(9),
            Constraint::Min(8),
        ],
    )
    .header(header_row(&[
        "METRIC", "P5", "P25", "P50", "P75", "P95", "MAX", "SKEW",
    ]))
    .block(table_block(format!(
        " Task metrics · skew = max/median, flagged ≥ ×{} ",
        analysis::SKEW_RATIO
    )));
    f.render_widget(table, area);
}

fn fmt_ratio(r: f64) -> String {
    if r.is_infinite() {
        "∞".into()
    } else if r >= 100.0 {
        format!("{r:.0}")
    } else {
        format!("{r:.1}")
    }
}

fn draw_executors(f: &mut Frame, area: Rect, d: &StageDetail) {
    let rows_data = analysis::executor_rows(d);
    if rows_data.is_empty() {
        f.render_widget(
            Paragraph::new("no executor data")
                .style(Style::default().fg(Color::DarkGray))
                .block(table_block(" Executors ".into())),
            area,
        );
        return;
    }

    let rows: Vec<Row> = rows_data
        .iter()
        .map(|r| {
            let e = &r.summary;
            let row = Row::new(vec![
                Cell::from(r.id.clone()),
                Cell::from(format!("{}✓ {}✗", e.succeeded_tasks, e.failed_tasks)),
                Cell::from(fmt_millis(e.task_time)),
                Cell::from(fmt_millis(r.mean_task_ms.round() as i64)),
                Cell::from(fmt_bytes(e.shuffle_read)),
                Cell::from(fmt_bytes(e.memory_bytes_spilled + e.disk_bytes_spilled)),
                Cell::from(r.reason.clone()),
            ]);
            if r.flagged {
                row.style(Style::default().fg(Color::Red))
            } else {
                row
            }
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Length(6),
            Constraint::Length(9),
            Constraint::Length(7),
            Constraint::Length(7),
            Constraint::Length(9),
            Constraint::Length(9),
            Constraint::Min(16),
        ],
    )
    .header(header_row(&[
        "EXEC", "TASKS", "TIME", "AVG", "SHUF R", "SPILL", "WHY",
    ]))
    .block(table_block(format!(" Executors ({}) ", rows_data.len())));
    f.render_widget(table, area);
}

fn draw_tasks(
    f: &mut Frame,
    area: Rect,
    d: &StageDetail,
    tasks: &[TaskData],
    show_failed: bool,
    state: &mut TableState,
) {
    let median = analysis::median_duration(d);
    let rows: Vec<Row> = tasks
        .iter()
        .map(|t| {
            let m = t.metrics();
            let straggler = !show_failed && analysis::is_straggler(t, median);
            let note = if show_failed {
                t.error_message
                    .as_deref()
                    .and_then(|e| e.lines().next())
                    .unwrap_or("")
                    .to_string()
            } else if straggler {
                format!("straggler ×{:.1}", t.duration_ms() as f64 / median.max(1.0))
            } else if t.speculative {
                "speculative".into()
            } else {
                String::new()
            };
            let row = Row::new(vec![
                Cell::from(t.task_id.to_string()),
                Cell::from(t.index.to_string()),
                Cell::from(t.attempt.to_string()),
                Cell::from(t.executor_id.clone()),
                Cell::from(t.host.clone()),
                Cell::from(t.status.clone()).style(status_style(match t.status.as_str() {
                    "SUCCESS" => "SUCCEEDED",
                    s => s,
                })),
                Cell::from(fmt_millis(t.duration_ms())),
                Cell::from(fmt_millis(m.jvm_gc_time)),
                Cell::from(fmt_bytes(m.shuffle_read_metrics.bytes())),
                Cell::from(fmt_bytes(m.memory_bytes_spilled + m.disk_bytes_spilled)),
                Cell::from(note),
            ]);
            if show_failed {
                row.style(Style::default().fg(Color::Red))
            } else if straggler {
                row.style(Style::default().fg(Color::Yellow))
            } else {
                row
            }
        })
        .collect();

    let title = if show_failed {
        format!(" Failed tasks ({}) · f: slowest ", tasks.len())
    } else {
        format!(
            " Slowest tasks ({}) · median {} · f: failed ({}) ",
            tasks.len(),
            fmt_millis(median.round() as i64),
            d.failed.len()
        )
    };

    let table = Table::new(
        rows,
        [
            Constraint::Length(7),
            Constraint::Length(6),
            Constraint::Length(4),
            Constraint::Length(6),
            Constraint::Length(16),
            Constraint::Length(8),
            Constraint::Length(9),
            Constraint::Length(7),
            Constraint::Length(9),
            Constraint::Length(9),
            Constraint::Min(12),
        ],
    )
    .header(header_row(&[
        "TASK", "IDX", "ATT", "EXEC", "HOST", "STATUS", "DURATION", "GC", "SHUF R", "SPILL", "NOTE",
    ]))
    .block(table_block(title))
    .row_highlight_style(selected_style())
    .highlight_symbol("▌");
    f.render_stateful_widget(table, area, state);
}
