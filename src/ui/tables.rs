//! Jobs / Stages / Executors tables.

use super::fmt_millis;
use super::{
    display_name, fmt_bytes, header_row, mini_bar, selected_style, short_time, status_style,
    table_block, warn_if,
};
use crate::spark::{ExecutionData, ExecutorSummary, JobData, StageData};
use ratatui::{
    Frame,
    layout::{Constraint, Rect},
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Cell, Paragraph, Row, Table, TableState},
};

pub fn draw_jobs(
    f: &mut Frame,
    area: Rect,
    jobs: &[&JobData],
    title: String,
    state: &mut TableState,
) {
    let rows: Vec<Row> = jobs
        .iter()
        .map(|j| {
            Row::new(vec![
                Cell::from(j.job_id.to_string()),
                Cell::from(j.status.clone()).style(status_style(&j.status)),
                Cell::from(
                    display_name(&j.name, j.description.as_deref())
                        .chars()
                        .take(48)
                        .collect::<String>(),
                ),
                Cell::from(short_time(&j.submission_time)),
                Cell::from(format!("{}/{}", j.num_completed_tasks, j.num_tasks)),
                Cell::from(j.num_failed_tasks.to_string()).style(warn_if(j.num_failed_tasks)),
                Cell::from(format!("{}/{}", j.num_completed_stages, j.stage_ids.len())),
                Cell::from(mini_bar(j.progress(), 12)),
            ])
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Length(5),
            Constraint::Length(10),
            Constraint::Min(20),
            Constraint::Length(9),
            Constraint::Length(13),
            Constraint::Length(7),
            Constraint::Length(9),
            Constraint::Length(13),
        ],
    )
    .header(header_row(&[
        "ID",
        "STATUS",
        "NAME",
        "SUBMITTED",
        "TASKS",
        "FAILED",
        "STAGES",
        "PROGRESS",
    ]))
    .block(table_block(title))
    .row_highlight_style(selected_style())
    .highlight_symbol("▌");

    f.render_stateful_widget(table, area, state);
}

pub fn draw_stages(
    f: &mut Frame,
    area: Rect,
    stages: &[&StageData],
    title: String,
    state: &mut TableState,
) {
    let rows: Vec<Row> = stages
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
                Cell::from(
                    display_name(&st.name, st.description.as_deref())
                        .chars()
                        .take(40)
                        .collect::<String>(),
                ),
                Cell::from(format!("{}/{}", st.num_complete_tasks, st.num_tasks)),
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
                Cell::from(mini_bar(st.progress(), 12)),
            ])
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Length(9),
            Constraint::Length(9),
            Constraint::Min(18),
            Constraint::Length(12),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(13),
        ],
    )
    .header(header_row(&[
        "STAGE", "STATUS", "NAME", "TASKS", "INPUT", "SHUF R", "SHUF W", "SPILL", "PROGRESS",
    ]))
    .block(table_block(title))
    .row_highlight_style(selected_style())
    .highlight_symbol("▌");

    f.render_stateful_widget(table, area, state);
}

pub fn draw_executors(
    f: &mut Frame,
    area: Rect,
    execs: &[&ExecutorSummary],
    title: String,
    state: &mut TableState,
) {
    let rows: Vec<Row> = execs
        .iter()
        .map(|e| {
            let gc_pct = if e.total_duration > 0 {
                100.0 * e.total_gc_time as f64 / e.total_duration as f64
            } else {
                0.0
            };
            let (state, state_style) = if !e.is_active {
                ("dead", Style::default().fg(Color::Red))
            } else if e.excluded() {
                ("excl", Style::default().fg(Color::Magenta))
            } else {
                ("up", Style::default().fg(Color::Green))
            };
            // A dead executor's row says why, right where you're looking.
            let host = match (&e.remove_reason, e.is_active) {
                (Some(r), false) => format!(
                    "{} · {}",
                    e.host_port,
                    r.lines()
                        .next()
                        .unwrap_or(r)
                        .chars()
                        .take(48)
                        .collect::<String>()
                ),
                _ => e.host_port.clone(),
            };
            Row::new(vec![
                Cell::from(e.id.clone()),
                Cell::from(state).style(state_style),
                Cell::from(host).style(if e.is_active {
                    Style::default()
                } else {
                    Style::default().fg(Color::Red)
                }),
                Cell::from(format!("{}/{}", e.active_tasks, e.total_cores)),
                Cell::from(e.failed_tasks.to_string()).style(warn_if(e.failed_tasks)),
                Cell::from(format!(
                    "{} / {}",
                    fmt_bytes(e.memory_used),
                    fmt_bytes(e.max_memory)
                )),
                Cell::from(mini_bar(e.memory_ratio(), 10)),
                Cell::from(format!("{gc_pct:.1}%")).style(if gc_pct > 10.0 {
                    Style::default().fg(Color::Red)
                } else {
                    Style::default()
                }),
                Cell::from(fmt_bytes(e.total_shuffle_read)),
            ])
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Length(8),
            Constraint::Length(6),
            Constraint::Min(18),
            Constraint::Length(10),
            Constraint::Length(7),
            Constraint::Length(22),
            Constraint::Length(11),
            Constraint::Length(7),
            Constraint::Length(10),
        ],
    )
    .header(header_row(&[
        "EXEC", "STATE", "HOST", "TASKS", "FAILED", "STORAGE", "MEM", "GC", "SHUF R",
    ]))
    .block(table_block(title))
    .row_highlight_style(selected_style())
    .highlight_symbol("▌");

    f.render_stateful_widget(table, area, state);
}

pub fn draw_sql(
    f: &mut Frame,
    area: Rect,
    sql: Option<&[&ExecutionData]>,
    title: String,
    state: &mut TableState,
) {
    let Some(execs) = sql else {
        f.render_widget(
            Paragraph::new(vec![
                Line::from(""),
                Line::from("  This endpoint has no /sql."),
                Line::from(Span::styled(
                    "  Not a Spark SQL / DataFrame application, or Spark older than 3.0.",
                    Style::default().fg(Color::DarkGray),
                )),
            ])
            .block(table_block(" SQL ".into())),
            area,
        );
        return;
    };

    let rows: Vec<Row> = execs
        .iter()
        .map(|e| {
            let jobs = format!(
                "▶{} ✓{} ✗{}",
                e.running_job_ids.len(),
                e.success_job_ids.len(),
                e.failed_job_ids.len()
            );
            let error = e
                .error_message
                .as_deref()
                .and_then(|m| m.lines().next())
                .unwrap_or("")
                .to_string();
            Row::new(vec![
                Cell::from(e.id.to_string()),
                Cell::from(e.status.clone()).style(status_style(&e.status)),
                Cell::from(e.title().chars().take(70).collect::<String>()),
                Cell::from(short_time(&Some(e.submission_time.clone()))),
                Cell::from(fmt_millis(e.duration)),
                Cell::from(jobs).style(if e.failed_job_ids.is_empty() {
                    Style::default()
                } else {
                    Style::default().fg(Color::Red)
                }),
                Cell::from(error).style(Style::default().fg(Color::Red)),
            ])
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Length(6),
            Constraint::Length(10),
            Constraint::Min(30),
            Constraint::Length(9),
            Constraint::Length(9),
            Constraint::Length(12),
            Constraint::Min(16),
        ],
    )
    .header(header_row(&[
        "ID",
        "STATUS",
        "QUERY",
        "SUBMITTED",
        "DURATION",
        "JOBS",
        "ERROR",
    ]))
    .block(table_block(title))
    .row_highlight_style(selected_style())
    .highlight_symbol("▌");

    f.render_stateful_widget(table, area, state);
}
