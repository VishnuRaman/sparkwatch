use crate::alerts::{Alert, AlertLog, Kind};
use crate::logview::{filter_from_error, LogTarget, LogView, Stream};
use crate::poller::{Detail, DetailData, LogEvent};
use crate::spark::{ApplicationInfo, ExecutionData, Snapshot, StageData, StageDetail, TaskData, ThreadStackTrace};
use crate::ui::sql_detail::Pane;
use ratatui::widgets::TableState;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tab {
    Overview,
    Jobs,
    Stages,
    Executors,
    Sql,
    Failures,
}

impl Tab {
    pub const ALL: [Tab; 6] = [
        Tab::Overview,
        Tab::Jobs,
        Tab::Stages,
        Tab::Executors,
        Tab::Sql,
        Tab::Failures,
    ];

    pub fn title(&self) -> &'static str {
        match self {
            Tab::Overview => "Overview",
            Tab::Jobs => "Jobs",
            Tab::Stages => "Stages",
            Tab::Executors => "Executors",
            Tab::Sql => "SQL",
            Tab::Failures => "Failures",
        }
    }

    pub fn index(&self) -> usize {
        Self::ALL.iter().position(|t| t == self).unwrap_or(0)
    }

    pub fn next(&self) -> Tab {
        Self::ALL[(self.index() + 1) % Self::ALL.len()]
    }

    pub fn prev(&self) -> Tab {
        Self::ALL[(self.index() + Self::ALL.len() - 1) % Self::ALL.len()]
    }
}

/// Which screen is showing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum View {
    /// Choosing an application (History Server with several, or `a` pressed).
    Picker,
    /// The tabbed monitor for one application.
    Main,
    /// Drill-down into one stage.
    Stage,
    /// Drill-down into one SQL execution.
    Sql,
    /// Full text of one alert.
    Alert,
    /// An executor's log stream.
    Logs,
    /// An executor's thread dump.
    Threads,
}

#[derive(Default)]
pub struct ThreadsView {
    pub executor_id: String,
    pub threads: Option<Vec<ThreadStackTrace>>,
    pub error: Option<String>,
    pub scroll: u16,
    pub expanded: bool,
    pub filter: Option<String>,
    pub filter_input: Option<String>,
}

/// A table cursor that survives the rows being re-sorted underneath it.
///
/// `TableState` only knows an index; every poll re-sorts the rows, so we also
/// remember the *key* of the selected row and re-find it after each update.
pub struct Cursor<K> {
    pub state: TableState,
    key: Option<K>,
}

impl<K: PartialEq + Clone> Cursor<K> {
    fn new() -> Self {
        Self {
            state: TableState::default().with_selected(Some(0)),
            key: None,
        }
    }

    fn selected(&self) -> usize {
        self.state.selected().unwrap_or(0)
    }

    /// Point at `index` and remember which row that is.
    fn select<T>(&mut self, index: usize, rows: &[T], key_of: impl Fn(&T) -> K) {
        self.state.select(Some(index));
        self.key = rows.get(index).map(key_of);
    }

    /// Rows changed: follow the remembered key, else clamp the index.
    fn resync<T>(&mut self, rows: &[T], key_of: impl Fn(&T) -> K) {
        if rows.is_empty() {
            self.state.select(None);
            self.key = None;
            return;
        }
        let by_key = self
            .key
            .as_ref()
            .and_then(|k| rows.iter().position(|r| key_of(r) == *k));
        let index = by_key.unwrap_or_else(|| self.selected().min(rows.len() - 1));
        self.select(index, rows, key_of);
    }
}

/// One point on the Overview sparklines, taken from executor totals each poll.
#[derive(Debug, Clone, Copy)]
pub struct Sample {
    pub at: Instant,
    pub completed_tasks: i64,
    pub active_tasks: i64,
}

/// How many samples the sparklines keep. At the default 2 s poll that is
/// four minutes of history, which is about what fits on a wide terminal.
pub const HISTORY_LEN: usize = 120;

/// Stages tab narrowed to one job's stages (Enter on the Jobs tab).
#[derive(Debug, Clone)]
pub struct StageFilter {
    pub job_id: i64,
    pub stage_ids: Vec<i64>,
}

/// The stages the Stages tab shows, honouring the job filter.
pub fn visible_stages<'a>(snapshot: &'a Snapshot, filter: &Option<StageFilter>) -> Vec<&'a StageData> {
    snapshot
        .stages
        .iter()
        .filter(|s| filter.as_ref().is_none_or(|f| f.stage_ids.contains(&s.stage_id)))
        .collect()
}

pub struct App {
    pub view: View,
    pub tab: Tab,
    pub endpoint: String,
    pub interval: Duration,
    pub paused: bool,
    pub should_quit: bool,

    /// Application currently being polled; `None` while in the picker.
    pub watching: Option<String>,
    pub apps: Vec<ApplicationInfo>,
    pub picker: Cursor<String>,

    pub snapshot: Option<Snapshot>,
    pub history: VecDeque<Sample>,
    pub last_error: Option<String>,
    pub last_update: Option<Instant>,

    pub jobs: Cursor<i64>,
    pub stages: Cursor<(i64, i64)>,
    pub stage_filter: Option<StageFilter>,
    pub executors: Cursor<String>,
    pub sql: Cursor<i64>,

    /// The drill-down that is open (and being refreshed by the poller).
    pub detail_target: Option<Detail>,
    pub detail_error: Option<String>,

    pub stage_detail: Option<StageDetail>,
    pub tasks: Cursor<i64>,
    /// Task table shows failed tasks instead of the slowest ones.
    pub show_failed: bool,

    pub sql_detail: Option<ExecutionData>,
    pub plan_scroll: u16,
    pub nodes_scroll: u16,
    pub sql_focus: Pane,
    pub plan_only: bool,

    pub alerts: AlertLog,
    pub alerts_cursor: Cursor<String>,
    pub alert_scroll: u16,

    pub logs: LogView,
    /// Rows the log viewport had at the last draw; paging uses it.
    pub logs_rows: usize,
    pub threads: ThreadsView,
    /// Lines the thread view rendered at the last draw; scroll bound.
    pub threads_lines: usize,
    /// Where Esc from the logs / threads view goes back to.
    pub return_view: View,
}

impl App {
    pub fn new(endpoint: String, interval: Duration, watching: Option<String>) -> Self {
        Self {
            view: if watching.is_some() { View::Main } else { View::Picker },
            tab: Tab::Overview,
            endpoint,
            interval,
            paused: false,
            should_quit: false,
            watching,
            apps: Vec::new(),
            picker: Cursor::new(),
            snapshot: None,
            history: VecDeque::with_capacity(HISTORY_LEN),
            last_error: None,
            last_update: None,
            jobs: Cursor::new(),
            stages: Cursor::new(),
            stage_filter: None,
            executors: Cursor::new(),
            sql: Cursor::new(),
            detail_target: None,
            detail_error: None,
            stage_detail: None,
            tasks: Cursor::new(),
            show_failed: false,
            sql_detail: None,
            plan_scroll: 0,
            nodes_scroll: 0,
            sql_focus: Pane::Plan,
            plan_only: false,
            alerts: AlertLog::default(),
            alerts_cursor: Cursor::new(),
            alert_scroll: 0,
            logs: LogView::default(),
            logs_rows: 20,
            threads: ThreadsView::default(),
            threads_lines: 0,
            return_view: View::Main,
        }
    }

    // ------------------------------------------------------------ poll results

    /// Returns the id to auto-watch when the endpoint only reports one app
    /// (a live driver) so the user is not asked to pick from a list of one.
    pub fn apply_apps(&mut self, result: Result<Vec<ApplicationInfo>, String>) -> Option<String> {
        match result {
            Ok(apps) => {
                self.picker.resync(&apps, |a| a.id.clone());
                self.apps = apps;
                self.last_error = None;
                self.last_update = Some(Instant::now());
                if self.watching.is_none() && self.apps.len() == 1 {
                    let id = self.apps[0].id.clone();
                    self.watch(id.clone());
                    return Some(id);
                }
                None
            }
            Err(e) => {
                self.last_error = Some(e);
                None
            }
        }
    }

    pub fn apply_snapshot(&mut self, app_id: &str, result: Result<Snapshot, String>) {
        // Late result for an app we have already switched away from.
        if self.watching.as_deref() != Some(app_id) {
            return;
        }
        match result {
            Ok(s) => {
                self.jobs.resync(&s.jobs, |j| j.job_id);
                self.stages
                    .resync(&visible_stages(&s, &self.stage_filter), |st| st.key());
                self.executors.resync(&s.executors, |e| e.id.clone());
                self.sql.resync(s.sql.as_deref().unwrap_or(&[]), |e| e.id);
                self.alerts.ingest(&s);
                let keys: Vec<&Alert> = self.alerts.newest_first().collect();
                self.alerts_cursor.resync(&keys, |a| a.key.clone());
                self.record_sample(&s);
                self.snapshot = Some(s);
                self.last_error = None;
                self.last_update = Some(Instant::now());
            }
            // Keep showing the last good snapshot; surface the error in the header.
            Err(e) => self.last_error = Some(e),
        }
    }

    pub fn apply_detail(&mut self, detail: Detail, result: Result<DetailData, String>) {
        // Late result for a detail view that has since been closed or changed.
        if self.detail_target != Some(detail) {
            return;
        }
        match result {
            Ok(DetailData::Stage(d)) => {
                self.stage_detail = Some(d);
                self.detail_error = None;
                self.resync_tasks();
            }
            Ok(DetailData::Sql(Some(e))) => {
                self.sql_detail = Some(e);
                self.detail_error = None;
            }
            Ok(DetailData::Sql(None)) => {
                self.detail_error = Some("this execution is no longer retained by the driver".into());
            }
            Err(e) => self.detail_error = Some(e),
        }
    }

    fn record_sample(&mut self, s: &Snapshot) {
        let sum = |f: fn(&crate::spark::ExecutorSummary) -> i64| s.executors.iter().map(f).sum();
        if self.history.len() == HISTORY_LEN {
            self.history.pop_front();
        }
        self.history.push_back(Sample {
            at: Instant::now(),
            completed_tasks: sum(|e| e.completed_tasks),
            active_tasks: sum(|e| e.active_tasks),
        });
    }

    // ------------------------------------------------------------- navigation

    /// Switch to watching `id`; all per-app state is dropped.
    pub fn watch(&mut self, id: String) {
        if self.watching.as_deref() != Some(id.as_str()) {
            self.snapshot = None;
            self.history.clear();
            self.last_error = None;
            self.jobs = Cursor::new();
            self.stages = Cursor::new();
            self.stage_filter = None;
            self.executors = Cursor::new();
            self.sql = Cursor::new();
            self.alerts.clear();
            self.alerts_cursor = Cursor::new();
            self.logs.close();
            self.threads = ThreadsView::default();
            self.close_detail();
        }
        self.watching = Some(id);
        self.view = View::Main;
    }

    pub fn open_picker(&mut self) {
        self.close_detail();
        self.view = View::Picker;
    }

    /// The id under the cursor in the picker.
    pub fn picked_app(&self) -> Option<String> {
        self.apps.get(self.picker.selected()).map(|a| a.id.clone())
    }

    /// The stage under the cursor on the Stages tab.
    pub fn selected_stage(&self) -> Option<&StageData> {
        let s = self.snapshot.as_ref()?;
        visible_stages(s, &self.stage_filter)
            .get(self.stages.selected())
            .copied()
    }

    /// Enter on the Stages tab: open the drill-down for the selected stage.
    /// Returns the detail the poller should start fetching.
    pub fn open_stage_detail(&mut self) -> Option<Detail> {
        let st = self.selected_stage()?;
        let (id, attempt) = st.key();
        // A failed stage is opened on its failures; a live one on its stragglers.
        let start_on_failed = st.num_failed_tasks > 0 && st.status == "FAILED";
        Some(self.open_stage(id, attempt, start_on_failed))
    }

    fn open_stage(&mut self, id: i64, attempt: i64, start_on_failed: bool) -> Detail {
        let target = Detail::Stage { id, attempt };
        if self.detail_target != Some(target) {
            self.stage_detail = None;
            self.detail_error = None;
            self.tasks = Cursor::new();
            self.show_failed = start_on_failed;
        }
        self.detail_target = Some(target);
        self.view = View::Stage;
        target
    }

    // ---------------------------------------------------------------- alerts

    pub fn selected_alert(&self) -> Option<&Alert> {
        self.alerts.get_newest(self.alerts_cursor.selected())
    }

    pub fn open_alert(&mut self) {
        if self.selected_alert().is_some() {
            self.alert_scroll = 0;
            self.view = View::Alert;
        }
    }

    pub fn close_alert(&mut self) {
        if self.view == View::Alert {
            self.view = View::Main;
        }
    }

    /// `s` on an alert: jump into the stage it concerns, on its failures.
    pub fn open_alert_stage(&mut self) -> Option<Detail> {
        let (id, attempt) = self.selected_alert()?.stage?;
        Some(self.open_stage(id, attempt, true))
    }

    // ------------------------------------------------------- logs & threads

    fn executor(&self, id: &str) -> Option<&crate::spark::ExecutorSummary> {
        self.snapshot.as_ref()?.executors.iter().find(|e| e.id == id)
    }

    /// Open the log viewer on an executor. Returns the target to stream.
    pub fn open_logs(&mut self, executor_id: String, filter: Option<String>) -> LogTarget {
        let exec = self.executor(&executor_id);
        let target = LogTarget {
            // A dead executor's container has restarted or gone; on k8s the
            // previous container is the one with the reason.
            previous: exec.is_some_and(|e| !e.is_active),
            stream: Stream::Stderr,
            http_url: exec.and_then(|e| e.log_url("stderr")),
            executor_id,
        };
        if !matches!(self.view, View::Logs | View::Threads) {
            self.return_view = self.view;
        }
        self.logs.open(target.clone(), filter);
        self.view = View::Logs;
        target
    }

    /// `L` on the Executors tab.
    pub fn open_logs_selected_executor(&mut self) -> Option<LogTarget> {
        let id = self.snapshot.as_ref()?.executors.get(self.executors.selected())?.id.clone();
        Some(self.open_logs(id, None))
    }

    /// `L` on a task in the stage drill-down: that executor's logs, filtered
    /// to the error when the task failed.
    pub fn open_logs_selected_task(&mut self) -> Option<LogTarget> {
        let t = self.visible_tasks().get(self.tasks.selected())?;
        let id = t.executor_id.clone();
        let filter = t.error_message.as_deref().and_then(filter_from_error);
        Some(self.open_logs(id, filter))
    }

    /// `L` on an alert: the executor it concerns.
    pub fn open_logs_selected_alert(&mut self) -> Option<LogTarget> {
        let a = self.selected_alert()?;
        let id = a.executor_id.clone()?;
        let filter = if a.kind == Kind::Task {
            a.detail.as_deref().and_then(filter_from_error)
        } else {
            None
        };
        Some(self.open_logs(id, filter))
    }

    /// Re-open the same executor with `previous` flipped (`P`).
    pub fn logs_toggle_previous(&mut self) -> Option<LogTarget> {
        let mut t = self.logs.target.clone()?;
        t.previous = !t.previous;
        let filter = self.logs.filter.clone();
        self.logs.open(t.clone(), filter);
        Some(t)
    }

    /// Re-open on the other stream (`o`, HTTP sources).
    pub fn logs_toggle_stream(&mut self) -> Option<LogTarget> {
        let mut t = self.logs.target.clone()?;
        t.stream = t.stream.other();
        t.http_url = self.executor(&t.executor_id).and_then(|e| e.log_url(t.stream.name()));
        let filter = self.logs.filter.clone();
        self.logs.open(t.clone(), filter);
        Some(t)
    }

    pub fn close_logs(&mut self) {
        self.logs.close();
        self.view = self.return_view;
    }

    pub fn apply_log(&mut self, event: LogEvent) {
        if !self.logs.is_open() {
            return;
        }
        match event {
            LogEvent::Lines(l) => self.logs.append(l),
            LogEvent::Replace(l) => self.logs.replace(l),
            LogEvent::Status(s) => self.logs.status = Some(s),
        }
    }

    /// Open the thread dump view. Returns the executor to fetch.
    pub fn open_threads(&mut self, executor_id: String) -> String {
        if !matches!(self.view, View::Logs | View::Threads) {
            self.return_view = self.view;
        }
        self.threads = ThreadsView {
            executor_id: executor_id.clone(),
            ..Default::default()
        };
        self.view = View::Threads;
        executor_id
    }

    pub fn open_threads_selected_executor(&mut self) -> Option<String> {
        let id = self.snapshot.as_ref()?.executors.get(self.executors.selected())?.id.clone();
        Some(self.open_threads(id))
    }

    pub fn close_threads(&mut self) {
        self.threads = ThreadsView::default();
        self.view = self.return_view;
    }

    pub fn apply_threads(&mut self, executor_id: &str, result: Result<Option<Vec<ThreadStackTrace>>, String>) {
        if self.view != View::Threads || self.threads.executor_id != executor_id {
            return;
        }
        match result {
            Ok(Some(t)) => {
                self.threads.threads = Some(t);
                self.threads.error = None;
            }
            Ok(None) => {
                self.threads.error = Some(
                    "no thread dump served for this executor (History Server, or the executor is gone)".into(),
                );
            }
            Err(e) => self.threads.error = Some(e),
        }
    }

    /// Text input for the `/` filter in the logs or threads view. Returns
    /// true if the key was consumed as text.
    pub fn filter_input_key(&mut self, key: crossterm::event::KeyCode) -> bool {
        use crossterm::event::KeyCode::*;
        match self.view {
            View::Logs if self.logs.filter_input.is_some() => {
                match key {
                    Char(c) => self.logs.filter_push(c),
                    Backspace => self.logs.filter_pop(),
                    Enter => self.logs.filter_commit(),
                    Esc => self.logs.filter_cancel(),
                    _ => {}
                }
                true
            }
            View::Threads if self.threads.filter_input.is_some() => {
                match key {
                    Char(c) => self.threads.filter_input.as_mut().unwrap().push(c),
                    Backspace => {
                        self.threads.filter_input.as_mut().unwrap().pop();
                    }
                    Enter => {
                        let f = self.threads.filter_input.take().unwrap().trim().to_lowercase();
                        self.threads.filter = if f.is_empty() { None } else { Some(f) };
                        self.threads.scroll = 0;
                    }
                    Esc => self.threads.filter_input = None,
                    _ => {}
                }
                true
            }
            _ => false,
        }
    }

    fn alert_scroll_max(&self) -> u16 {
        let lines = self
            .selected_alert()
            .and_then(|a| a.detail.as_ref())
            .map_or(0, |d| d.lines().count() + 2);
        lines.saturating_sub(1).min(u16::MAX as usize) as u16
    }

    /// The SQL execution under the cursor on the SQL tab.
    pub fn selected_sql(&self) -> Option<&ExecutionData> {
        self.snapshot.as_ref()?.sql.as_ref()?.get(self.sql.selected())
    }

    /// Enter on the SQL tab: open the plan/metrics view for the selection.
    pub fn open_sql_detail(&mut self) -> Option<Detail> {
        let target = Detail::Sql(self.selected_sql()?.id);
        if self.detail_target != Some(target) {
            self.sql_detail = None;
            self.detail_error = None;
            self.plan_scroll = 0;
            self.nodes_scroll = 0;
            self.sql_focus = Pane::Plan;
        }
        self.detail_target = Some(target);
        self.view = View::Sql;
        Some(target)
    }

    pub fn toggle_sql_focus(&mut self) {
        self.sql_focus = match self.sql_focus {
            Pane::Plan => Pane::Nodes,
            Pane::Nodes => Pane::Plan,
        };
    }

    pub fn toggle_plan_only(&mut self) {
        self.plan_only = !self.plan_only;
        if self.plan_only {
            self.sql_focus = Pane::Plan;
        }
    }

    /// Scroll range of the focused SQL pane (last line index).
    fn sql_scroll_max(&self) -> u16 {
        let Some(e) = &self.sql_detail else { return 0 };
        let lines = match self.sql_focus {
            Pane::Plan => e.plan_description.lines().count(),
            Pane::Nodes => e.nodes.iter().map(|n| crate::ui::sql_detail::node_lines(n).len()).sum(),
        };
        lines.saturating_sub(1).min(u16::MAX as usize) as u16
    }

    fn sql_scroll_by(&mut self, delta: isize) {
        let max = self.sql_scroll_max();
        let cur = match self.sql_focus {
            Pane::Plan => &mut self.plan_scroll,
            Pane::Nodes => &mut self.nodes_scroll,
        };
        *cur = (*cur as isize + delta).clamp(0, max as isize) as u16;
    }

    pub fn close_detail(&mut self) {
        self.detail_target = None;
        self.stage_detail = None;
        self.sql_detail = None;
        self.detail_error = None;
        if matches!(self.view, View::Stage | View::Sql | View::Alert) {
            self.view = View::Main;
        }
    }

    pub fn toggle_failed_tasks(&mut self) {
        self.show_failed = !self.show_failed;
        self.tasks = Cursor::new();
        self.resync_tasks();
    }

    /// Tasks the drill-down's bottom table lists.
    pub fn visible_tasks(&self) -> &[TaskData] {
        match &self.stage_detail {
            Some(d) if self.show_failed => &d.failed,
            Some(d) => &d.slowest,
            None => &[],
        }
    }

    fn resync_tasks(&mut self) {
        let Self {
            stage_detail,
            show_failed,
            tasks,
            ..
        } = self;
        let rows: &[TaskData] = match stage_detail {
            Some(d) if *show_failed => &d.failed,
            Some(d) => &d.slowest,
            None => &[],
        };
        tasks.resync(rows, |t| t.task_id);
    }

    /// Enter on the Jobs tab: show only that job's stages.
    pub fn filter_stages_by_selected_job(&mut self) {
        let Some(s) = &self.snapshot else { return };
        let Some(job) = s.jobs.get(self.jobs.selected()) else { return };
        self.stage_filter = Some(StageFilter {
            job_id: job.job_id,
            stage_ids: job.stage_ids.clone(),
        });
        self.stages = Cursor::new();
        self.stages
            .resync(&visible_stages(s, &self.stage_filter), |st| st.key());
        self.tab = Tab::Stages;
    }

    /// Returns true if there was a filter to clear.
    pub fn clear_stage_filter(&mut self) -> bool {
        if self.stage_filter.take().is_none() {
            return false;
        }
        if let Some(s) = &self.snapshot {
            self.stages.resync(&visible_stages(s, &None), |st| st.key());
        }
        true
    }

    /// Row count of the table currently in focus.
    fn current_len(&self) -> usize {
        match self.view {
            View::Picker => self.apps.len(),
            View::Stage => self.visible_tasks().len(),
            View::Sql => self.sql_scroll_max() as usize + 1,
            View::Alert => self.alert_scroll_max() as usize + 1,
            View::Logs => self.logs.visible().len(),
            View::Threads => self.threads_lines,
            View::Main => {
                if self.tab == Tab::Failures {
                    return self.alerts.len();
                }
                let Some(s) = &self.snapshot else { return 0 };
                match self.tab {
                    Tab::Jobs => s.jobs.len(),
                    Tab::Stages => visible_stages(s, &self.stage_filter).len(),
                    Tab::Executors => s.executors.len(),
                    Tab::Sql => s.sql.as_ref().map_or(0, Vec::len),
                    Tab::Overview | Tab::Failures => 0,
                }
            }
        }
    }

    fn current_index(&self) -> usize {
        match self.view {
            View::Picker => self.picker.selected(),
            View::Stage => self.tasks.selected(),
            View::Sql => match self.sql_focus {
                Pane::Plan => self.plan_scroll as usize,
                Pane::Nodes => self.nodes_scroll as usize,
            },
            View::Alert => self.alert_scroll as usize,
            View::Logs => self.logs.scroll,
            View::Threads => self.threads.scroll as usize,
            View::Main => match self.tab {
                Tab::Jobs => self.jobs.selected(),
                Tab::Stages => self.stages.selected(),
                Tab::Executors => self.executors.selected(),
                Tab::Sql => self.sql.selected(),
                Tab::Failures => self.alerts_cursor.selected(),
                Tab::Overview => 0,
            },
        }
    }

    fn select_index(&mut self, index: usize) {
        match self.view {
            View::Picker => self.picker.select(index, &self.apps, |a| a.id.clone()),
            View::Sql => {
                let max = self.sql_scroll_max();
                let v = (index.min(max as usize)) as u16;
                match self.sql_focus {
                    Pane::Plan => self.plan_scroll = v,
                    Pane::Nodes => self.nodes_scroll = v,
                }
            }
            View::Alert => {
                self.alert_scroll = index.min(self.alert_scroll_max() as usize) as u16;
            }
            View::Logs => {
                if index == 0 {
                    self.logs.scroll_to_start();
                } else {
                    self.logs.scroll_to_end();
                }
            }
            View::Threads => {
                self.threads.scroll = index.min(self.threads_lines.saturating_sub(1)) as u16;
            }
            View::Stage => {
                let Self {
                    stage_detail,
                    show_failed,
                    tasks,
                    ..
                } = self;
                let rows: &[TaskData] = match stage_detail {
                    Some(d) if *show_failed => &d.failed,
                    Some(d) => &d.slowest,
                    None => &[],
                };
                tasks.select(index, rows, |t| t.task_id);
            }
            View::Main => {
                if self.tab == Tab::Failures {
                    let keys: Vec<&Alert> = self.alerts.newest_first().collect();
                    return self.alerts_cursor.select(index, &keys, |a| a.key.clone());
                }
                let Self {
                    snapshot,
                    tab,
                    jobs,
                    stages,
                    stage_filter,
                    executors,
                    sql,
                    ..
                } = self;
                let Some(s) = snapshot else { return };
                match tab {
                    Tab::Jobs => jobs.select(index, &s.jobs, |j| j.job_id),
                    Tab::Stages => stages.select(index, &visible_stages(s, stage_filter), |st| st.key()),
                    Tab::Executors => executors.select(index, &s.executors, |e| e.id.clone()),
                    Tab::Sql => sql.select(index, s.sql.as_deref().unwrap_or(&[]), |e| e.id),
                    Tab::Overview | Tab::Failures => {}
                }
            }
        }
    }

    pub fn move_selection(&mut self, delta: isize) {
        // Scrolling text doesn't wrap around like a table cursor does.
        if self.view == View::Sql {
            return self.sql_scroll_by(delta);
        }
        if self.view == View::Alert {
            let max = self.alert_scroll_max() as isize;
            self.alert_scroll = (self.alert_scroll as isize + delta).clamp(0, max) as u16;
            return;
        }
        if self.view == View::Logs {
            return self.logs.scroll_by(delta, self.logs_rows.max(1));
        }
        if self.view == View::Threads {
            let max = self.threads_lines.saturating_sub(1) as isize;
            self.threads.scroll = (self.threads.scroll as isize + delta).clamp(0, max) as u16;
            return;
        }
        let len = self.current_len();
        if len == 0 {
            return;
        }
        let cur = self.current_index() as isize;
        self.select_index((cur + delta).rem_euclid(len as isize) as usize);
    }

    pub fn select_edge(&mut self, last: bool) {
        let len = self.current_len();
        if len == 0 {
            return;
        }
        self.select_index(if last { len - 1 } else { 0 });
    }

    pub fn bump_interval(&mut self, up: bool) {
        let secs = self.interval.as_secs();
        let next = if up {
            (secs + 1).min(60)
        } else {
            secs.saturating_sub(1).max(1)
        };
        self.interval = Duration::from_secs(next);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spark::JobData;

    #[test]
    fn cursor_follows_row_after_resort() {
        let mut c: Cursor<i64> = Cursor::new();
        let rows = vec![10, 20, 30];
        c.select(1, &rows, |r| *r); // on 20
        let resorted = vec![30, 20, 10];
        c.resync(&resorted, |r| *r);
        assert_eq!(c.state.selected(), Some(1));
        assert_eq!(c.key, Some(20));
    }

    #[test]
    fn cursor_clamps_when_row_disappears() {
        let mut c: Cursor<i64> = Cursor::new();
        c.select(2, &[1, 2, 3], |r| *r);
        c.resync(&[1, 2], |r| *r);
        assert_eq!(c.state.selected(), Some(1));
        assert_eq!(c.key, Some(2));
        c.resync(&[], |r: &i64| *r);
        assert_eq!(c.state.selected(), None);
    }

    fn app_info(id: &str) -> ApplicationInfo {
        ApplicationInfo {
            id: id.into(),
            ..Default::default()
        }
    }

    #[test]
    fn single_app_is_watched_without_picker() {
        let mut app = App::new("x".into(), Duration::from_secs(2), None);
        assert_eq!(app.view, View::Picker);
        let auto = app.apply_apps(Ok(vec![app_info("app-1")]));
        assert_eq!(auto.as_deref(), Some("app-1"));
        assert_eq!(app.view, View::Main);
    }

    #[test]
    fn several_apps_stay_in_picker() {
        let mut app = App::new("x".into(), Duration::from_secs(2), None);
        let auto = app.apply_apps(Ok(vec![app_info("a"), app_info("b")]));
        assert!(auto.is_none());
        assert_eq!(app.view, View::Picker);
        app.move_selection(1);
        assert_eq!(app.picked_app().as_deref(), Some("b"));
    }

    #[test]
    fn stale_snapshot_for_other_app_is_ignored() {
        let mut app = App::new("x".into(), Duration::from_secs(2), Some("a".into()));
        app.apply_snapshot("b", Ok(Snapshot::default()));
        assert!(app.snapshot.is_none());
        app.apply_snapshot("a", Ok(Snapshot::default()));
        assert!(app.snapshot.is_some());
        assert_eq!(app.history.len(), 1);
    }

    fn stage(id: i64, status: &str) -> StageData {
        StageData {
            stage_id: id,
            status: status.into(),
            ..Default::default()
        }
    }

    fn watched_app() -> App {
        let mut app = App::new("x".into(), Duration::from_secs(2), Some("a".into()));
        let snap = Snapshot {
            jobs: vec![JobData {
                job_id: 3,
                stage_ids: vec![7, 9],
                ..Default::default()
            }],
            stages: vec![stage(9, "ACTIVE"), stage(8, "COMPLETE"), stage(7, "COMPLETE")],
            ..Default::default()
        };
        app.apply_snapshot("a", Ok(snap));
        app
    }

    #[test]
    fn job_filter_narrows_stages_and_esc_clears_it() {
        let mut app = watched_app();
        app.tab = Tab::Jobs;
        app.filter_stages_by_selected_job();
        assert_eq!(app.tab, Tab::Stages);
        let ids: Vec<i64> = visible_stages(app.snapshot.as_ref().unwrap(), &app.stage_filter)
            .iter()
            .map(|s| s.stage_id)
            .collect();
        assert_eq!(ids, [9, 7]);
        app.move_selection(1);
        assert_eq!(app.selected_stage().unwrap().stage_id, 7);
        assert!(app.clear_stage_filter());
        assert!(!app.clear_stage_filter());
        // Cursor followed stage 7 into the unfiltered list.
        assert_eq!(app.selected_stage().unwrap().stage_id, 7);
    }

    #[test]
    fn failures_tab_lists_alerts_and_jumps_to_stage() {
        let mut app = watched_app();
        let snap = Snapshot {
            stages: vec![StageData {
                stage_id: 4,
                status: "FAILED".into(),
                failure_reason: Some("boom\n\tat X".into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        app.apply_snapshot("a", Ok(snap));
        assert_eq!(app.alerts.unacked(), 1);
        app.tab = Tab::Failures;
        assert_eq!(app.selected_alert().unwrap().stage, Some((4, 0)));
        app.open_alert();
        assert_eq!(app.view, View::Alert);
        app.move_selection(5);
        assert!(app.alert_scroll > 0);
        let target = app.open_alert_stage().unwrap();
        assert_eq!(target, Detail::Stage { id: 4, attempt: 0 });
        assert_eq!(app.view, View::Stage);
        assert!(app.show_failed);
        app.alerts.acknowledge();
        assert_eq!(app.alerts.unacked(), 0);
    }

    #[test]
    fn logs_open_from_failed_task_with_error_filter_and_return() {
        let mut app = watched_app();
        app.apply_snapshot(
            "a",
            Ok(Snapshot {
                executors: vec![crate::spark::ExecutorSummary {
                    id: "2".into(),
                    is_active: false,
                    ..Default::default()
                }],
                stages: vec![stage(9, "ACTIVE")],
                ..Default::default()
            }),
        );
        app.tab = Tab::Stages;
        let target = app.open_stage_detail().unwrap();
        app.apply_detail(
            target,
            Ok(DetailData::Stage(StageDetail {
                failed: vec![TaskData {
                    task_id: 4050,
                    executor_id: "2".into(),
                    error_message: Some("ExecutorLostFailure (executor 2 exited)".into()),
                    ..Default::default()
                }],
                ..Default::default()
            })),
        );
        app.toggle_failed_tasks();
        let lt = app.open_logs_selected_task().unwrap();
        assert_eq!(lt.executor_id, "2");
        assert!(lt.previous, "dead executor → previous container");
        assert_eq!(app.logs.filter.as_deref(), Some("executorlostfailure"));
        assert_eq!(app.view, View::Logs);

        app.apply_log(LogEvent::Lines(vec!["INFO ok".into(), "ERROR ExecutorLostFailure boom".into()]));
        assert_eq!(app.logs.visible(), ["ERROR ExecutorLostFailure boom"]);
        app.close_logs();
        assert_eq!(app.view, View::Stage, "Esc returns to where L was pressed");
    }

    #[test]
    fn sql_detail_scrolls_focused_pane_within_bounds() {
        let mut app = watched_app();
        let snap = Snapshot {
            sql: Some(vec![ExecutionData { id: 12, ..Default::default() }]),
            ..Default::default()
        };
        app.apply_snapshot("a", Ok(snap));
        app.tab = Tab::Sql;
        let target = app.open_sql_detail().unwrap();
        assert_eq!(target, Detail::Sql(12));
        assert_eq!(app.view, View::Sql);

        let exec = ExecutionData {
            id: 12,
            plan_description: "a\nb\nc\nd".into(),
            ..Default::default()
        };
        app.apply_detail(target, Ok(DetailData::Sql(Some(exec))));
        app.move_selection(10);
        assert_eq!(app.plan_scroll, 3); // clamped to last line
        app.move_selection(-10);
        assert_eq!(app.plan_scroll, 0);
        app.select_edge(true);
        assert_eq!(app.plan_scroll, 3);

        app.apply_detail(target, Ok(DetailData::Sql(None)));
        assert!(app.detail_error.as_deref().unwrap().contains("no longer retained"));
        assert!(app.sql_detail.is_some()); // last good data stays
    }

    #[test]
    fn stage_detail_opens_for_selected_stage_and_ignores_stale_results() {
        let mut app = watched_app();
        app.tab = Tab::Stages;
        app.move_selection(1); // stage 8
        let target = app.open_stage_detail().unwrap();
        assert_eq!(target, Detail::Stage { id: 8, attempt: 0 });
        assert_eq!(app.view, View::Stage);

        let other = Detail::Stage { id: 9, attempt: 0 };
        app.apply_detail(other, Ok(DetailData::Stage(StageDetail::default())));
        assert!(app.stage_detail.is_none());

        let detail = StageDetail {
            slowest: vec![TaskData { task_id: 1, ..Default::default() }],
            ..Default::default()
        };
        app.apply_detail(target, Ok(DetailData::Stage(detail)));
        assert_eq!(app.visible_tasks().len(), 1);

        app.close_detail();
        assert_eq!(app.view, View::Main);
        assert!(app.detail_target.is_none());
    }
}
