use crate::app::{App, Tab};
use crate::spark::Snapshot;
use ratatui::{
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Gauge, Paragraph, Row, Table, TableState, Tabs, Wrap},
    Frame,
};

// ---------------------------------------------------------------- formatting

pub fn fmt_bytes(b: i64) -> String {
    const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    if b <= 0 {
        return "0 B".into();
    }
    let mut v = b as f64;
    let mut i = 0;
    while v >= 1024.0 && i < UNITS.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{} {}", b, UNITS[i])
    } else {
        format!("{v:.1} {}", UNITS[i])
    }
}

pub fn fmt_millis(ms: i64) -> String {
    if ms <= 0 {
        return "-".into();
    }
    let s = ms / 1000;
    let (h, m, sec) = (s / 3600, (s % 3600) / 60, s % 60);
    if h > 0 {
        format!("{h}h{m:02}m")
    } else if m > 0 {
        format!("{m}m{sec:02}s")
    } else if s > 0 {
        format!("{s}.{:01}s", (ms % 1000) / 100)
    } else {
        format!("{ms}ms")
    }
}

/// Spark hands back timestamps like `2024-05-01T09:14:02.331GMT`, which is not
/// valid RFC 3339. Rather than pull in a date library just to reformat it, take
/// the useful part.
fn short_time(ts: &Option<String>) -> String {
    match ts {
        None => "-".into(),
        Some(t) => t
            .split('T')
            .nth(1)
            .map(|s| s.chars().take(8).collect())
            .unwrap_or_else(|| t.clone()),
    }
}

fn status_style(status: &str) -> Style {
    match status {
        "RUNNING" | "ACTIVE" => Style::default().fg(Color::Yellow),
        "SUCCEEDED" | "COMPLETE" => Style::default().fg(Color::Green),
        "FAILED" => Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        "PENDING" => Style::default().fg(Color::Blue),
        "SKIPPED" => Style::default().fg(Color::DarkGray),
        _ => Style::default().fg(Color::Gray),
    }
}

/// Unicode bar, useful inside a table cell where a Gauge cannot go.
fn mini_bar(ratio: f64, width: usize) -> String {
    let filled = ((ratio.clamp(0.0, 1.0)) * width as f64).round() as usize;
    format!("{}{}", "█".repeat(filled), "░".repeat(width - filled))
}

// -------------------------------------------------------------------- layout

pub fn draw(f: &mut Frame, app: &mut App) {
    let [header, body, footer] = Layout::vertical([
        Constraint::Length(3),
        Constraint::Min(0),
        Constraint::Length(1),
    ])
    .areas(f.area());

    draw_header(f, header, app);
    draw_footer(f, footer, app);

    // Destructure so the table states and the snapshot borrow disjointly.
    let App {
        tab,
        snapshot,
        jobs_state,
        stages_state,
        executors_state,
        ..
    } = app;

    let Some(snap) = snapshot.as_ref() else {
        let msg = Paragraph::new("Connecting to Spark…")
            .style(Style::default().fg(Color::DarkGray))
            .block(Block::default().borders(Borders::ALL));
        f.render_widget(msg, body);
        return;
    };

    match *tab {
        Tab::Overview => draw_overview(f, body, snap),
        Tab::Jobs => draw_jobs(f, body, snap, jobs_state),
        Tab::Stages => draw_stages(f, body, snap, stages_state),
        Tab::Executors => draw_executors(f, body, snap, executors_state),
    }
}

fn draw_header(f: &mut Frame, area: Rect, app: &App) {
    let titles: Vec<Line> = Tab::ALL.iter().map(|t| Line::from(t.title())).collect();

    let (name, id) = match &app.snapshot {
        Some(s) => (s.app.name.clone(), s.app.id.clone()),
        None => ("—".into(), "—".into()),
    };

    let age = app
        .last_update
        .map(|t| format!("{}s ago", t.elapsed().as_secs()))
        .unwrap_or_else(|| "never".into());

    let state = if app.paused {
        Span::styled(" PAUSED ", Style::default().fg(Color::Black).bg(Color::Yellow))
    } else if app.last_error.is_some() {
        Span::styled(" ERROR ", Style::default().fg(Color::White).bg(Color::Red))
    } else {
        Span::styled(" LIVE ", Style::default().fg(Color::Black).bg(Color::Green))
    };

    let title = Line::from(vec![
        Span::raw(" "),
        state,
        Span::raw(format!(" {name} [{id}] @ {} · every {}s · updated {age} ",
            app.endpoint, app.interval.as_secs())),
    ]);

    let tabs = Tabs::new(titles)
        .select(app.tab.index())
        .highlight_style(
            Style::default()
                .fg(Color::Black)
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )
        .divider("│")
        .block(Block::default().borders(Borders::ALL).title(title));

    f.render_widget(tabs, area);
}

fn draw_footer(f: &mut Frame, area: Rect, app: &App) {
    let text = match &app.last_error {
        Some(e) => Line::from(Span::styled(
            format!(" {} ", e.lines().next().unwrap_or(e)),
            Style::default().fg(Color::Red),
        )),
        None => Line::from(Span::styled(
            " q quit · tab/←→ switch · j/k move · g/G top/bottom · r refresh · p pause · +/- interval ",
            Style::default().fg(Color::DarkGray),
        )),
    };
    f.render_widget(Paragraph::new(text), area);
}

// ------------------------------------------------------------------ overview

fn draw_overview(f: &mut Frame, area: Rect, s: &Snapshot) {
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
            format!("{} active / {} total", s.executors.iter().filter(|e| e.is_active).count(), s.executors.len()).into(),
        ]),
        Line::from(vec!["cores     ".dark_gray(), format!("{cores} ({active_tasks} tasks running)").into()]),
        Line::from(vec!["storage   ".dark_gray(), format!("{} / {}", fmt_bytes(mem_used), fmt_bytes(mem_max)).into()]),
        Line::from(vec!["shuffle r ".dark_gray(), fmt_bytes(shuffle_read).into()]),
        Line::from(vec![
            "gc time   ".dark_gray(),
            Span::styled(
                format!("{} ({gc_pct:.1}% of task time)", fmt_millis(gc)),
                if gc_pct > 10.0 { Style::default().fg(Color::Red) } else { Style::default() },
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

// -------------------------------------------------------------------- tables

fn table_block(title: String) -> Block<'static> {
    Block::default().borders(Borders::ALL).title(title)
}

fn selected_style() -> Style {
    Style::default().bg(Color::Rgb(40, 44, 60)).add_modifier(Modifier::BOLD)
}

fn header_row(cols: &[&'static str]) -> Row<'static> {
    Row::new(cols.iter().map(|c| Cell::from(*c)).collect::<Vec<_>>())
        .style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD))
        .height(1)
}

fn draw_jobs(f: &mut Frame, area: Rect, s: &Snapshot, state: &mut TableState) {
    let rows: Vec<Row> = s
        .jobs
        .iter()
        .map(|j| {
            Row::new(vec![
                Cell::from(j.job_id.to_string()),
                Cell::from(j.status.clone()).style(status_style(&j.status)),
                Cell::from(j.name.chars().take(48).collect::<String>()),
                Cell::from(short_time(&j.submission_time)),
                Cell::from(format!("{}/{}", j.num_completed_tasks, j.num_tasks)),
                Cell::from(j.num_failed_tasks.to_string()).style(if j.num_failed_tasks > 0 {
                    Style::default().fg(Color::Red)
                } else {
                    Style::default().fg(Color::DarkGray)
                }),
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
    .header(header_row(&["ID", "STATUS", "NAME", "SUBMITTED", "TASKS", "FAILED", "STAGES", "PROGRESS"]))
    .block(table_block(format!(" Jobs ({}) ", s.jobs.len())))
    .row_highlight_style(selected_style())
    .highlight_symbol("▌");

    f.render_stateful_widget(table, area, state);
}

fn draw_stages(f: &mut Frame, area: Rect, s: &Snapshot, state: &mut TableState) {
    let rows: Vec<Row> = s
        .stages
        .iter()
        .map(|st| {
            Row::new(vec![
                Cell::from(format!("{}.{}", st.stage_id, st.attempt_id)),
                Cell::from(st.status.clone()).style(status_style(&st.status)),
                Cell::from(st.name.chars().take(40).collect::<String>()),
                Cell::from(format!("{}/{}", st.num_complete_tasks, st.num_tasks)),
                Cell::from(fmt_bytes(st.input_bytes)),
                Cell::from(fmt_bytes(st.shuffle_read_bytes)),
                Cell::from(fmt_bytes(st.shuffle_write_bytes)),
                Cell::from(fmt_bytes(st.memory_bytes_spilled)).style(if st.memory_bytes_spilled > 0 {
                    Style::default().fg(Color::Magenta)
                } else {
                    Style::default().fg(Color::DarkGray)
                }),
                Cell::from(mini_bar(st.progress(), 12)),
            ])
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Length(7),
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
    .block(table_block(format!(" Stages ({}) ", s.stages.len())))
    .row_highlight_style(selected_style())
    .highlight_symbol("▌");

    f.render_stateful_widget(table, area, state);
}

fn draw_executors(f: &mut Frame, area: Rect, s: &Snapshot, state: &mut TableState) {
    let rows: Vec<Row> = s
        .executors
        .iter()
        .map(|e| {
            let gc_pct = if e.total_duration > 0 {
                100.0 * e.total_gc_time as f64 / e.total_duration as f64
            } else {
                0.0
            };
            Row::new(vec![
                Cell::from(e.id.clone()),
                Cell::from(if e.is_active { "up" } else { "dead" }).style(if e.is_active {
                    Style::default().fg(Color::Green)
                } else {
                    Style::default().fg(Color::Red)
                }),
                Cell::from(e.host_port.clone()),
                Cell::from(format!("{}/{}", e.active_tasks, e.total_cores)),
                Cell::from(e.failed_tasks.to_string()).style(if e.failed_tasks > 0 {
                    Style::default().fg(Color::Red)
                } else {
                    Style::default().fg(Color::DarkGray)
                }),
                Cell::from(format!("{} / {}", fmt_bytes(e.memory_used), fmt_bytes(e.max_memory))),
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
    .block(table_block(format!(" Executors ({}) ", s.executors.len())))
    .row_highlight_style(selected_style())
    .highlight_symbol("▌");

    f.render_stateful_widget(table, area, state);
}
