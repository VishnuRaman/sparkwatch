//! Rendering. `draw` is the single entry point; each view lives in its own
//! submodule and shares the formatting helpers defined here.

mod batches;
pub mod environment;
mod exec_memory;
mod failures;
mod logs;
mod metrics;
mod overview;
mod picker;
pub mod sql_detail;
mod stage_detail;
mod storage;
mod streaming;
mod summary;
mod tables;
mod threads;

use crate::alerts::Alert;
use crate::app::{
    App, Tab, View, batch_failures_of, batch_stages_of, batches_newest_first, filtered_title,
    visible_alerts, visible_executors, visible_jobs, visible_rdds, visible_sql, visible_stages,
};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Paragraph, Row, Tabs},
};
use std::collections::HashMap;

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
pub fn short_time(ts: &Option<String>) -> String {
    match ts {
        None => "-".into(),
        Some(t) => t
            .split('T')
            .nth(1)
            .map(|s| s.chars().take(8).collect())
            .unwrap_or_else(|| t.clone()),
    }
}

pub fn status_style(status: &str) -> Style {
    match status {
        "RUNNING" | "ACTIVE" => Style::default().fg(Color::Yellow),
        "SUCCEEDED" | "COMPLETE" | "COMPLETED" | "SUCCESS" => Style::default().fg(Color::Green),
        "FAILED" => Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        "PENDING" => Style::default().fg(Color::Blue),
        "SKIPPED" => Style::default().fg(Color::DarkGray),
        _ => Style::default().fg(Color::Gray),
    }
}

/// What to call a job or stage. Spark's `name` is the user-code call site,
/// which is `run at <unknown>:0` whenever there is no user code on the
/// driver's stack (Spark Connect, streaming micro-batches). Better sources,
/// in order: the job description (streaming sets it to
/// `<query>\nid = …\nrunId = …\nbatch = N`; `setJobDescription` users put
/// their own text there), then the SQL execution the job belongs to.
pub fn display_name(name: &str, description: Option<&str>, sql_hint: Option<&str>) -> String {
    if let Some(d) = description.map(str::trim).filter(|d| !d.is_empty()) {
        let first = d.lines().next().unwrap_or(name).trim();
        let batch = d
            .lines()
            .find_map(|l| l.trim().strip_prefix("batch = "))
            .map(|b| format!(" · batch {b}"))
            .unwrap_or_default();
        // A streaming description whose first line is `id = …` means the
        // query had no name; use the SQL hint or call site plus the batch.
        if !first.starts_with("id = ") {
            return format!("{first}{batch}");
        }
        let base = sql_hint.unwrap_or(name);
        return format!("{base}{batch}");
    }
    match sql_hint {
        Some(h) if name.contains("<unknown>") => h.to_string(),
        _ => name.to_string(),
    }
}

/// Per-job and per-stage labels derived from the SQL executions of a
/// snapshot (`sql #12 · SELECT …`), for jobs and stages whose own name is
/// just `run at <unknown>:0`.
#[derive(Default)]
pub struct Labels {
    pub jobs: HashMap<i64, String>,
    pub stages: HashMap<i64, String>,
    /// Streaming query names that appear with more than one run id in this
    /// snapshot (restarts): their rows get a `· run xxxx` suffix so that two
    /// "batch 4" rows from different runs can be told apart.
    pub multi_run: std::collections::HashSet<String>,
}

/// `(name, runId)` from a streaming job/stage description.
fn streaming_run(description: &str) -> Option<(String, String)> {
    let d = crate::streaming::parse_description(description)?;
    Some((d.name?, d.run_id))
}

impl Labels {
    pub fn from_snapshot(s: &crate::spark::Snapshot) -> Self {
        let mut l = Labels::default();
        let mut runs: HashMap<String, std::collections::HashSet<String>> = HashMap::new();
        for d in s
            .jobs
            .iter()
            .filter_map(|j| j.description.as_deref())
            .chain(s.stages.iter().filter_map(|st| st.description.as_deref()))
        {
            if let Some((name, run)) = streaming_run(d) {
                runs.entry(name).or_default().insert(run);
            }
        }
        l.multi_run = runs
            .into_iter()
            .filter(|(_, r)| r.len() > 1)
            .map(|(n, _)| n)
            .collect();
        let Some(sql) = &s.sql else { return l };
        for e in sql {
            let hint = format!(
                "sql #{} · {}",
                e.id,
                display_name(e.title(), Some(&e.description), None)
            );
            for job_id in e
                .running_job_ids
                .iter()
                .chain(&e.success_job_ids)
                .chain(&e.failed_job_ids)
            {
                l.jobs.insert(*job_id, hint.clone());
            }
        }
        for j in &s.jobs {
            if let Some(h) = l.jobs.get(&j.job_id) {
                for st in &j.stage_ids {
                    l.stages.entry(*st).or_insert_with(|| h.clone());
                }
            }
        }
        l
    }

    pub fn job(&self, id: i64) -> Option<&str> {
        self.jobs.get(&id).map(String::as_str)
    }

    pub fn stage(&self, id: i64) -> Option<&str> {
        self.stages.get(&id).map(String::as_str)
    }

    /// Append `· run xxxx` when this description's query has restarted.
    pub fn with_run(&self, label: String, description: Option<&str>) -> String {
        match description.and_then(streaming_run) {
            Some((name, run)) if self.multi_run.contains(&name) => {
                format!("{label} · run {}", run.chars().take(8).collect::<String>())
            }
            _ => label,
        }
    }
}

/// Unicode bar, useful inside a table cell where a Gauge cannot go.
pub fn mini_bar(ratio: f64, width: usize) -> String {
    let filled = ((ratio.clamp(0.0, 1.0)) * width as f64).round() as usize;
    format!("{}{}", "█".repeat(filled), "░".repeat(width - filled))
}

/// Red when the value is non-zero, dim otherwise. For failure counters.
pub fn warn_if(nonzero: i64) -> Style {
    if nonzero > 0 {
        Style::default().fg(Color::Red)
    } else {
        Style::default().fg(Color::DarkGray)
    }
}

pub fn table_block(title: String) -> Block<'static> {
    Block::default().borders(Borders::ALL).title(title)
}

pub fn selected_style() -> Style {
    Style::default()
        .bg(Color::Rgb(40, 44, 60))
        .add_modifier(Modifier::BOLD)
}

pub fn header_row(cols: &[&'static str]) -> Row<'static> {
    Row::new(cols.iter().map(|c| Cell::from(*c)).collect::<Vec<_>>())
        .style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )
        .height(1)
}

// -------------------------------------------------------------------- layout

pub fn draw(f: &mut Frame, app: &mut App) {
    // The alert strip only takes a line while there is something new.
    let strip_h = if app.view != View::Picker && app.alerts.unacked() > 0 {
        1
    } else {
        0
    };
    let [header, strip, body, footer] = Layout::vertical([
        Constraint::Length(3),
        Constraint::Length(strip_h),
        Constraint::Min(0),
        Constraint::Length(1),
    ])
    .areas(f.area());

    // The summary borrows all of `app`; build it before the destructure below.
    let sections_for_draw = if app.view == View::Summary {
        app.summary_sections()
    } else {
        Vec::new()
    };
    draw_header(f, header, app);
    if strip_h > 0 {
        failures::draw_strip(f, strip, &app.alerts);
    }
    draw_footer(f, footer, app);

    // Destructure so the table states and the snapshot borrow disjointly.
    let App {
        view,
        tab,
        apps,
        picker,
        snapshot,
        history,
        jobs,
        stages,
        stage_filter,
        executors,
        sql,
        rdds,
        filters,
        rdd_detail,
        stage_detail,
        detail_error,
        tasks,
        show_failed,
        sql_detail: sql_exec,
        plan_scroll,
        nodes_scroll,
        sql_focus,
        plan_only,
        alerts,
        alerts_cursor,
        alert_scroll,
        logs,
        logs_rows,
        threads,
        threads_lines,
        streaming: streaming_state,
        streaming_status,
        progress_log_start,
        streaming_sel,
        batch_query,
        batches_cursor,
        batch_id,
        batch_stages,
        environment,
        env_error,
        env_scroll,
        env_lines,
        metrics_sel,
        metrics_rows,
        metric_pins,
        metric_series,
        metrics_prev,
        summary_scroll,
        summary_lines,
        ..
    } = app;

    match *view {
        View::Picker => {
            picker::draw(f, body, apps, &mut picker.state);
            return;
        }
        View::Stage => {
            let detail = stage_detail.as_ref();
            let rows: &[_] = match detail {
                Some(d) if *show_failed => &d.failed,
                Some(d) => &d.slowest,
                None => &[],
            };
            stage_detail::draw(
                f,
                body,
                stage_detail::Props {
                    detail,
                    error: detail_error.as_deref(),
                    tasks: rows,
                    show_failed: *show_failed,
                    tasks_state: &mut tasks.state,
                },
            );
            return;
        }
        View::Sql => {
            sql_detail::draw(
                f,
                body,
                sql_detail::Props {
                    exec: sql_exec.as_ref(),
                    error: detail_error.as_deref(),
                    plan_scroll: *plan_scroll,
                    nodes_scroll: *nodes_scroll,
                    focus: *sql_focus,
                    plan_only: *plan_only,
                },
            );
            return;
        }
        View::Alert => {
            if let Some(a) = alerts.get_newest(alerts_cursor.state.selected().unwrap_or(0)) {
                failures::draw_detail(f, body, a, *alert_scroll);
            }
            return;
        }
        View::Logs => {
            logs::draw(f, body, logs, logs_rows);
            return;
        }
        View::Threads => {
            threads::draw(f, body, threads, threads_lines);
            return;
        }
        View::Rdd => {
            storage::draw_detail(f, body, rdd_detail.as_ref(), detail_error.as_deref());
            return;
        }
        View::Summary => {
            let sections = sections_for_draw;
            summary::draw(f, body, &sections, *summary_scroll, summary_lines);
            return;
        }
        View::ExecutorMemory => {
            let empty = crate::spark::Snapshot::default();
            let snap = snapshot.as_ref().unwrap_or(&empty);
            let rows = visible_executors(snap, filters.get(&Tab::Executors).map(String::as_str));
            exec_memory::draw(f, body, &rows, environment.as_ref(), &mut executors.state);
            return;
        }
        View::Batches | View::Batch => {
            let q = batch_query
                .as_ref()
                .and_then(|id| streaming_state.queries.get(id));
            let Some(q) = q else {
                f.render_widget(
                    Paragraph::new("query no longer known")
                        .block(Block::default().borders(Borders::ALL)),
                    body,
                );
                return;
            };
            let st = crate::streaming::QueryStats::of(q);
            if *view == View::Batches {
                batches::draw_list(
                    f,
                    body,
                    q,
                    &batches_newest_first(q),
                    &st,
                    &mut batches_cursor.state,
                    *progress_log_start,
                );
                return;
            }
            let Some(b) = batch_id.and_then(|id| q.batches.get(&id)) else {
                f.render_widget(
                    Paragraph::new("batch no longer in the window")
                        .block(Block::default().borders(Borders::ALL)),
                    body,
                );
                return;
            };
            let empty = crate::spark::Snapshot::default();
            let snap = snapshot.as_ref().unwrap_or(&empty);
            let stages = batch_stages_of(snap, q, b.batch_id);
            let failures = batch_failures_of(alerts, snap, q, b.batch_id);
            batches::draw_detail(
                f,
                body,
                batches::DetailProps {
                    query: q,
                    batch: b,
                    stats: &st,
                    stages: &stages,
                    failures: &failures,
                    stage_state: &mut batch_stages.state,
                    log_start: *progress_log_start,
                },
            );
            return;
        }
        View::Main => {}
    }

    // Failures come from the alert log, not the snapshot, so they show even
    // while the endpoint is unreachable.
    let filter = |t: Tab| filters.get(&t).map(String::as_str);
    if *tab == Tab::Failures {
        let rows: Vec<&Alert> = visible_alerts(alerts, filter(Tab::Failures));
        let title = format!(
            "{}· {} new · Enter full text · s open stage · x acknowledge ",
            filtered_title("Failures", rows.len(), alerts.len(), filter(Tab::Failures)),
            alerts.unacked()
        );
        failures::draw_table(f, body, alerts, &rows, title, &mut alerts_cursor.state);
        return;
    }
    if *tab == Tab::Streaming {
        streaming::draw(
            f,
            body,
            streaming_state,
            streaming_status.as_deref(),
            *streaming_sel,
        );
        return;
    }
    if *tab == Tab::Metrics {
        let fl = filters.get(&Tab::Metrics).map(String::as_str);
        let ms = snapshot.as_ref().and_then(|s| s.metrics.as_deref());
        let prev = metrics_prev.as_ref().map(|(at, m)| crate::metrics::Prev {
            at: *at,
            metrics: m,
        });
        *metrics_rows = metrics::draw(
            f,
            body,
            metrics::Props {
                metrics: ms,
                prev: prev.as_ref(),
                filter: fl,
                selected: *metrics_sel,
                pins: metric_pins,
                series: metric_series,
                on_history_server: snapshot
                    .as_ref()
                    .is_some_and(|s| s.app.attempts.iter().all(|a| a.completed)),
            },
        );
        return;
    }
    if *tab == Tab::Environment {
        let fl = filters.get(&Tab::Environment).map(String::as_str);
        *env_lines = environment
            .as_ref()
            .map_or(0, |e| environment::lines(e, fl).len());
        environment::draw(
            f,
            body,
            environment.as_ref(),
            env_error.as_deref(),
            fl,
            *env_scroll,
        );
        return;
    }

    let Some(snap) = snapshot.as_ref() else {
        let msg = Paragraph::new("Connecting to Spark…")
            .style(Style::default().fg(Color::DarkGray))
            .block(Block::default().borders(Borders::ALL));
        f.render_widget(msg, body);
        return;
    };

    let labels = Labels::from_snapshot(snap);
    match *tab {
        Tab::Overview => overview::draw(f, body, snap, history, &labels),
        Tab::Jobs => {
            let rows = visible_jobs(snap, filter(Tab::Jobs));
            let title = filtered_title("Jobs", rows.len(), snap.jobs.len(), filter(Tab::Jobs));
            tables::draw_jobs(f, body, &rows, title, &labels, &mut jobs.state)
        }
        Tab::Stages => {
            let rows = visible_stages(snap, stage_filter, filter(Tab::Stages));
            let mut title =
                filtered_title("Stages", rows.len(), snap.stages.len(), filter(Tab::Stages));
            if let Some(fl) = stage_filter {
                title = format!("{title}· job #{} · Esc to clear ", fl.job_id);
            }
            tables::draw_stages(f, body, &rows, title, &labels, &mut stages.state)
        }
        Tab::Executors => {
            let rows = visible_executors(snap, filter(Tab::Executors));
            let title = filtered_title(
                "Executors",
                rows.len(),
                snap.executors.len(),
                filter(Tab::Executors),
            );
            tables::draw_executors(f, body, &rows, title, &mut executors.state)
        }
        Tab::Sql => {
            let rows = visible_sql(snap, filter(Tab::Sql));
            let title = format!(
                "{}· Enter for plan ",
                filtered_title(
                    "SQL executions",
                    rows.as_ref().map_or(0, Vec::len),
                    snap.sql.as_ref().map_or(0, Vec::len),
                    filter(Tab::Sql)
                )
            );
            tables::draw_sql(f, body, rows.as_deref(), title, &mut sql.state)
        }
        Tab::Storage => {
            let rows = visible_rdds(snap, filter(Tab::Storage));
            let title = format!(
                "{}· Enter for distribution ",
                filtered_title(
                    "Cached RDDs",
                    rows.len(),
                    snap.rdds.len(),
                    filter(Tab::Storage)
                )
            );
            storage::draw_list(f, body, snap, &rows, title, &mut rdds.state)
        }
        Tab::Failures | Tab::Streaming | Tab::Environment | Tab::Metrics => {
            unreachable!("handled above")
        }
    }
}

fn draw_header(f: &mut Frame, area: Rect, app: &App) {
    let age = app
        .last_update
        .map(|t| format!("{}s ago", t.elapsed().as_secs()))
        .unwrap_or_else(|| "never".into());

    let state = if app.paused {
        Span::styled(
            " PAUSED ",
            Style::default().fg(Color::Black).bg(Color::Yellow),
        )
    } else if app.last_error.is_some() {
        Span::styled(" ERROR ", Style::default().fg(Color::White).bg(Color::Red))
    } else {
        Span::styled(" LIVE ", Style::default().fg(Color::Black).bg(Color::Green))
    };

    let what = match (&app.view, &app.snapshot) {
        (View::Picker, _) => format!("{} · updated {age}", app.endpoint),
        (
            View::Main
            | View::Stage
            | View::Sql
            | View::Alert
            | View::Logs
            | View::Threads
            | View::Rdd
            | View::Batches
            | View::Batch
            | View::ExecutorMemory
            | View::Summary,
            Some(s),
        ) => format!(
            "{} [{}] @ {} · every {}s · updated {age}",
            s.app.name,
            s.app.id,
            app.endpoint,
            app.interval.as_secs()
        ),
        (
            View::Main
            | View::Stage
            | View::Sql
            | View::Alert
            | View::Logs
            | View::Threads
            | View::Rdd
            | View::Batches
            | View::Batch
            | View::ExecutorMemory
            | View::Summary,
            None,
        ) => format!(
            "[{}] @ {} · every {}s · updated {age}",
            app.watching.as_deref().unwrap_or("—"),
            app.endpoint,
            app.interval.as_secs()
        ),
    };
    let title = Line::from(vec![Span::raw(" "), state, Span::raw(format!(" {what} "))]);
    let block = Block::default().borders(Borders::ALL).title(title);

    if app.view == View::Picker {
        f.render_widget(
            Paragraph::new(" choose an application")
                .style(Style::default().fg(Color::DarkGray))
                .block(block),
            area,
        );
        return;
    }

    let titles: Vec<Line> = Tab::ALL
        .iter()
        .map(|t| match t {
            // The failures tab carries its count, red while any are new.
            Tab::Failures if app.alerts.len() > 0 => Line::from(Span::styled(
                format!("Failures ({})", app.alerts.len()),
                if app.alerts.unacked() > 0 {
                    Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                },
            )),
            // Red while any query is falling behind.
            Tab::Streaming
                if app
                    .streaming
                    .queries
                    .values()
                    .any(|q| crate::streaming::QueryStats::of(q).behind) =>
            {
                Line::from(Span::styled(
                    "Streaming ▲",
                    Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
                ))
            }
            t => Line::from(t.title()),
        })
        .collect();
    let tabs = Tabs::new(titles)
        .select(app.tab.index())
        .highlight_style(
            Style::default()
                .fg(Color::Black)
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )
        .divider("│")
        .block(block);
    f.render_widget(tabs, area);
}

fn draw_footer(f: &mut Frame, area: Rect, app: &App) {
    // Typing a table filter takes over the footer.
    if let (View::Main, Some(input)) = (app.view, &app.filter_input) {
        f.render_widget(
            Paragraph::new(Line::from(vec![
                Span::styled(
                    " /",
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw(input.clone()),
                Span::styled("█", Style::default().fg(Color::Cyan)),
                Span::styled(
                    format!(
                        "  filter {} · Enter apply · Esc cancel",
                        app.tab.title().to_lowercase()
                    ),
                    Style::default().fg(Color::DarkGray),
                ),
            ])),
            area,
        );
        return;
    }
    if let Some(n) = &app.notice {
        f.render_widget(
            Paragraph::new(Line::from(Span::styled(
                format!(" {n} "),
                Style::default().fg(Color::Black).bg(Color::Cyan),
            ))),
            area,
        );
        return;
    }
    let help = match app.view {
        View::Picker if app.watching.is_some() => {
            " q quit · j/k move · Enter watch · Esc back · r refresh "
        }
        View::Picker => " q quit · j/k move · Enter watch · r refresh ",
        View::Main if app.tab == Tab::Failures => {
            " q quit · tab/←→ switch · j/k move · Enter full text · s open stage · L logs · x acknowledge · a apps "
        }
        View::Main if app.tab == Tab::Streaming => {
            " q quit · tab/←→ switch · j/k select query · Enter batches · x ack failures · a apps · r refresh · p pause "
        }
        View::Main if app.tab == Tab::Executors => {
            " q quit · tab/←→ switch · j/k move · / filter · L logs · t threads · m memory · x ack failures · a apps · r refresh "
        }
        View::Main if app.tab == Tab::Environment => {
            " q quit · tab/←→ switch · j/k PgUp/PgDn scroll · / search · c clear · a apps · r refresh "
        }
        View::Main if app.tab == Tab::Metrics => {
            " q quit · tab/←→ switch · j/k move · / filter · c clear · Enter pin/unpin sparkline · a apps · r refresh "
        }
        View::Main if app.tab == Tab::Storage => {
            " q quit · tab/←→ switch · j/k move · / filter · Enter distribution · a apps · r refresh · p pause "
        }
        View::Main => {
            " q quit · tab/←→ switch · j/k move · / filter · Enter open · S summary · D dump · x ack · a apps · r refresh · p pause "
        }
        View::Stage => {
            " Esc back · j/k tasks · f failed/slowest · L logs of task's executor · r refresh · p pause · q quit "
        }
        View::Logs => {
            " Esc back · j/k PgUp/PgDn scroll · g/G · F follow · / filter · c clear filter · w wrap · P previous · o stdout/stderr · t threads · q quit "
        }
        View::Threads => {
            " Esc back · j/k scroll · e expand · / filter · r refresh · L logs · q quit "
        }
        View::Sql => {
            " Esc back · j/k scroll · Tab plan/nodes · p plan only · g/G top/bottom · q quit "
        }
        View::Alert => " Esc back · j/k scroll · s open stage · L logs · x acknowledge · q quit ",
        View::Rdd => " Esc back · r refresh · q quit ",
        View::Batches => " Esc back · j/k · Enter batch detail · g/G · q quit ",
        View::ExecutorMemory => " Esc back · j/k executor · L logs · t threads · q quit ",
        View::Summary => " Esc back · j/k scroll · D dump bundle · r refresh · q quit ",
        View::Batch => {
            " Esc back · j/k stages · Enter stage drill-down · L driver log for this batch · q quit "
        }
    };
    let error = match app.view {
        View::Stage | View::Sql => app.detail_error.as_ref().or(app.last_error.as_ref()),
        View::Rdd => app.detail_error.as_ref().or(app.last_error.as_ref()),
        View::Batches | View::Batch | View::ExecutorMemory | View::Summary => {
            app.last_error.as_ref()
        }
        View::Alert | View::Logs | View::Threads => None,
        _ => app.last_error.as_ref(),
    };
    let text = match error {
        Some(e) => Line::from(Span::styled(
            format!(" {} ", e.lines().next().unwrap_or(e)),
            Style::default().fg(Color::Red),
        )),
        None => Line::from(Span::styled(help, Style::default().fg(Color::DarkGray))),
    };
    f.render_widget(Paragraph::new(text), area);
}

#[cfg(test)]
mod tests {
    use super::{Labels, display_name};
    use crate::spark::{ExecutionData, JobData, Snapshot};

    #[test]
    fn job_names_prefer_description_then_sql_then_call_site() {
        assert_eq!(
            display_name("run at <unknown>:0", None, None),
            "run at <unknown>:0"
        );
        assert_eq!(
            display_name(
                "run at <unknown>:0",
                Some("orders-agg\nid = q\nrunId = r\nbatch = 4123"),
                None
            ),
            "orders-agg · batch 4123"
        );
        assert_eq!(
            display_name(
                "run at <unknown>:0",
                Some("id = q\nrunId = r\nbatch = 7"),
                Some("sql #3 · x")
            ),
            "sql #3 · x · batch 7"
        );
        assert_eq!(
            display_name(
                "run at <unknown>:0",
                None,
                Some("sql #12 · SELECT a FROM t")
            ),
            "sql #12 · SELECT a FROM t"
        );
        // An informative call site is kept even when a SQL hint exists.
        assert_eq!(
            display_name(
                "count at Main.scala:40",
                None,
                Some("sql #12 · count at Main.scala:40")
            ),
            "count at Main.scala:40"
        );
        assert_eq!(
            display_name("count at Main.scala:40", Some("nightly load"), None),
            "nightly load"
        );
        assert_eq!(
            display_name("count at Main.scala:40", Some("  "), None),
            "count at Main.scala:40"
        );
    }

    #[test]
    fn labels_follow_execution_to_jobs_to_stages() {
        let snap = Snapshot {
            sql: Some(vec![ExecutionData {
                id: 22,
                description: "orders-poison\nid = q\nrunId = r\nbatch = 3".into(),
                failed_job_ids: vec![9],
                ..Default::default()
            }]),
            jobs: vec![JobData {
                job_id: 9,
                stage_ids: vec![30, 31],
                ..Default::default()
            }],
            ..Default::default()
        };
        let l = Labels::from_snapshot(&snap);
        assert_eq!(l.job(9), Some("sql #22 · orders-poison · batch 3"));
        assert_eq!(l.stage(31), Some("sql #22 · orders-poison · batch 3"));
        assert_eq!(l.stage(99), None);
    }

    #[test]
    fn restarted_queries_get_a_run_suffix() {
        let desc = |run: &str| Some(format!("orders-poison\nid = q\nrunId = {run}\nbatch = 4"));
        let snap = Snapshot {
            jobs: vec![
                JobData {
                    job_id: 1,
                    description: desc("aaaaaaaa-1"),
                    ..Default::default()
                },
                JobData {
                    job_id: 2,
                    description: desc("bbbbbbbb-2"),
                    ..Default::default()
                },
                JobData {
                    job_id: 3,
                    description: Some("orders-raw\nid = x\nrunId = cccccccc\nbatch = 9".into()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let l = Labels::from_snapshot(&snap);
        assert_eq!(
            l.with_run(
                "orders-poison · batch 4".into(),
                desc("bbbbbbbb-2").as_deref()
            ),
            "orders-poison · batch 4 · run bbbbbbbb"
        );
        // A single-run query is left alone.
        assert_eq!(
            l.with_run(
                "orders-raw · batch 9".into(),
                snap.jobs[2].description.as_deref()
            ),
            "orders-raw · batch 9"
        );
    }
}
