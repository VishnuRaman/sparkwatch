//! Overview tab: application info, cluster totals, one gauge per running job.

use super::{fmt_bytes, fmt_millis};
use crate::spark::Snapshot;
use ratatui::{
    layout::{Constraint, Layout, Rect},
    style::{Color, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Borders, Gauge, Paragraph, Wrap},
    Frame,
};

pub fn draw(f: &mut Frame, area: Rect, s: &Snapshot) {
    let [top, bottom] = Layout::vertical([Constraint::Length(9), Constraint::Min(0)]).areas(area);
    let [left, right] = Layout::horizontal([Constraint::Percentage(50); 2]).areas(top);

    let attempt = s.app.attempts.first().cloned().unwrap_or_default();
    let info = vec![
        Line::from(vec!["user      ".dark_gray(), attempt.spark_user.clone().into()]),
        Line::from(vec!["version   ".dark_gray(), attempt.app_spark_version.clone().into()]),
        Line::from(vec!["started   ".dark_gray(), attempt.start_time.clone().into()]),
        Line::from(vec!["uptime    ".dark_gray(), fmt_millis(attempt.duration).into()]),
        Line::from(vec![
            "state     ".dark_gray(),
            if attempt.completed {
                Span::styled("completed", Style::default().fg(Color::Green))
            } else {
                Span::styled("running", Style::default().fg(Color::Yellow))
            },
        ]),
    ];
    f.render_widget(
        Paragraph::new(info)
            .wrap(Wrap { trim: true })
            .block(Block::default().borders(Borders::ALL).title(" Application ")),
        left,
    );

    let running = s.jobs.iter().filter(|j| j.status == "RUNNING").count();
    let failed = s.jobs.iter().filter(|j| j.status == "FAILED").count();
    let active_tasks: i64 = s.executors.iter().map(|e| e.active_tasks).sum();
    let cores: i64 = s.executors.iter().filter(|e| e.is_active).map(|e| e.total_cores).sum();
    let mem_used: i64 = s.executors.iter().map(|e| e.memory_used).sum();
    let mem_max: i64 = s.executors.iter().map(|e| e.max_memory).sum();
    let shuffle_read: i64 = s.executors.iter().map(|e| e.total_shuffle_read).sum();
    let gc: i64 = s.executors.iter().map(|e| e.total_gc_time).sum();
    let run: i64 = s.executors.iter().map(|e| e.total_duration).sum();
    let gc_pct = if run > 0 { 100.0 * gc as f64 / run as f64 } else { 0.0 };

    let stats = vec![
        Line::from(vec![
            "jobs      ".dark_gray(),
            format!("{running} running").yellow(),
            " · ".dark_gray(),
            format!("{failed} failed").red(),
            format!(" · {} total", s.jobs.len()).into(),
        ]),
        Line::from(vec![
            "executors ".dark_gray(),
            format!(
                "{} active / {} total",
                s.executors.iter().filter(|e| e.is_active).count(),
                s.executors.len()
            )
            .into(),
        ]),
        Line::from(vec![
            "cores     ".dark_gray(),
            format!("{cores} ({active_tasks} tasks running)").into(),
        ]),
        Line::from(vec![
            "storage   ".dark_gray(),
            format!("{} / {}", fmt_bytes(mem_used), fmt_bytes(mem_max)).into(),
        ]),
        Line::from(vec!["shuffle r ".dark_gray(), fmt_bytes(shuffle_read).into()]),
        Line::from(vec![
            "gc time   ".dark_gray(),
            Span::styled(
                format!("{} ({gc_pct:.1}% of task time)", fmt_millis(gc)),
                if gc_pct > 10.0 {
                    Style::default().fg(Color::Red)
                } else {
                    Style::default()
                },
            ),
        ]),
    ];
    f.render_widget(
        Paragraph::new(stats).block(Block::default().borders(Borders::ALL).title(" Cluster ")),
        right,
    );

    draw_active_gauges(f, bottom, s);
}

/// One gauge per running job, showing task completion.
fn draw_active_gauges(f: &mut Frame, area: Rect, s: &Snapshot) {
    let block = Block::default().borders(Borders::ALL).title(" Running jobs ");
    let inner = block.inner(area);
    f.render_widget(block, area);

    let running: Vec<_> = s.jobs.iter().filter(|j| j.status == "RUNNING").collect();
    if running.is_empty() {
        f.render_widget(
            Paragraph::new("no jobs running").style(Style::default().fg(Color::DarkGray)),
            inner,
        );
        return;
    }

    // Two rows per job (label + bar); don't overflow the pane.
    let capacity = (inner.height as usize / 2).max(1);
    let shown = running.len().min(capacity);
    let rows = Layout::vertical(vec![Constraint::Length(2); shown]).split(inner);

    for (job, cell) in running.iter().take(shown).zip(rows.iter()) {
        let [label, bar] = Layout::vertical([Constraint::Length(1); 2]).areas(*cell);
        f.render_widget(
            Paragraph::new(Line::from(vec![
                format!("#{} ", job.job_id).cyan(),
                job.name.chars().take(60).collect::<String>().into(),
                format!("  {}/{} tasks", job.num_completed_tasks, job.num_tasks).dark_gray(),
            ])),
            label,
        );
        f.render_widget(
            Gauge::default()
                .gauge_style(Style::default().fg(Color::Yellow))
                .ratio(job.progress())
                .label(format!("{:.0}%", job.progress() * 100.0)),
            bar,
        );
    }
}
