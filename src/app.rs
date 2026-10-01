use crate::alerts::{Alert, AlertLog, Kind};
use crate::logview::{LogTarget, LogView, Stream, filter_from_error};
use crate::poller::{Detail, DetailData, LogEvent};
use crate::report;
use crate::spark::TaskMetricDistributions;
use crate::spark::{
    ApplicationEnvironmentInfo, ApplicationInfo, ExecutionData, ExecutorSummary, JobData,
    RddStorageInfo, Snapshot, StageData, StageDetail, TaskData, ThreadStackTrace,
};
use crate::streaming::{Batch, Progress, QueryHistory, Streaming, parse_description};
use crate::ui::sql_detail::Pane;
use ratatui::widgets::TableState;
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Tab {
    Overview,
    Jobs,
    Stages,
    Executors,
    Sql,
    Failures,
    Streaming,
    Storage,
    Environment,
}

impl Tab {
    pub const ALL: [Tab; 9] = [
        Tab::Overview,
        Tab::Jobs,
        Tab::Stages,
        Tab::Executors,
        Tab::Sql,
        Tab::Failures,
        Tab::Streaming,
        Tab::Storage,
        Tab::Environment,
    ];

    /// Tabs whose table `/` can narrow.
    pub fn filterable(&self) -> bool {
        matches!(
            self,
            Tab::Jobs
                | Tab::Stages
                | Tab::Executors
                | Tab::Sql
                | Tab::Failures
                | Tab::Storage
                | Tab::Environment
        )
    }

    pub fn title(&self) -> &'static str {
        match self {
            Tab::Overview => "Overview",
            Tab::Jobs => "Jobs",
            Tab::Stages => "Stages",
            Tab::Executors => "Executors",
            Tab::Sql => "SQL",
            Tab::Failures => "Failures",
            Tab::Streaming => "Streaming",
            Tab::Storage => "Storage",
            Tab::Environment => "Env",
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
    /// One cached RDD's distribution.
    Rdd,
    /// All batches of one streaming query.
    Batches,
    /// One batch: numbers, stages, failures.
    Batch,
    /// Executor peak memory against its budget.
    ExecutorMemory,
    /// The "since you were away" summary.
    Summary,
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
    /// Executors alive at this poll (the executor timeline).
    pub alive_executors: i64,
    /// Executors that have gone away so far.
    pub removed_executors: i64,
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

/// Case-insensitive substring match of a `/` filter against a row's fields.
fn hit(filter: Option<&str>, fields: &[&str]) -> bool {
    match filter {
        None => true,
        Some(f) => fields.iter().any(|x| x.to_lowercase().contains(f)),
    }
}

pub fn visible_jobs<'a>(s: &'a Snapshot, f: Option<&str>) -> Vec<&'a JobData> {
    s.jobs
        .iter()
        .filter(|j| hit(f, &[&j.job_id.to_string(), &j.status, &j.name]))
        .collect()
}

/// The stages the Stages tab shows: the job filter (Enter on a job) and the
/// text filter both apply.
pub fn visible_stages<'a>(
    s: &'a Snapshot,
    job: &Option<StageFilter>,
    f: Option<&str>,
) -> Vec<&'a StageData> {
    s.stages
        .iter()
        .filter(|st| {
            job.as_ref()
                .is_none_or(|j| j.stage_ids.contains(&st.stage_id))
        })
        .filter(|st| {
            hit(
                f,
                &[
                    &format!("{}.{}", st.stage_id, st.attempt_id),
                    &st.status,
                    &st.name,
                ],
            )
        })
        .collect()
}

pub fn visible_executors<'a>(s: &'a Snapshot, f: Option<&str>) -> Vec<&'a ExecutorSummary> {
    s.executors
        .iter()
        .filter(|e| {
            let state = if !e.is_active {
                "dead"
            } else if e.excluded() {
                "excluded"
            } else {
                "up"
            };
            hit(
                f,
                &[
                    &e.id,
                    &e.host_port,
                    state,
                    e.remove_reason.as_deref().unwrap_or(""),
                ],
            )
        })
        .collect()
}

pub fn visible_sql<'a>(s: &'a Snapshot, f: Option<&str>) -> Option<Vec<&'a ExecutionData>> {
    Some(
        s.sql
            .as_ref()?
            .iter()
            .filter(|e| {
                hit(
                    f,
                    &[
                        &e.id.to_string(),
                        &e.status,
                        &e.description,
                        e.error_message.as_deref().unwrap_or(""),
                    ],
                )
            })
            .collect(),
    )
}

pub fn visible_alerts<'a>(log: &'a AlertLog, f: Option<&str>) -> Vec<&'a Alert> {
    log.newest_first()
        .filter(|a| {
            hit(
                f,
                &[a.kind.label(), &a.title, a.detail.as_deref().unwrap_or("")],
            )
        })
        .collect()
}

pub fn visible_rdds<'a>(s: &'a Snapshot, f: Option<&str>) -> Vec<&'a RddStorageInfo> {
    s.rdds
        .iter()
        .filter(|r| hit(f, &[&r.id.to_string(), &r.name, &r.storage_level]))
        .collect()
}

pub fn batches_newest_first(q: &QueryHistory) -> Vec<&Batch> {
    q.batches.values().rev().collect()
}

/// The stages that ran for one micro-batch: Spark tags every job of a batch
/// with `runId = … / batch = N` in its description, and jobs list their
/// stages. Stages still being retained by the driver, that is.
pub fn batch_stages_of<'a>(s: &'a Snapshot, q: &QueryHistory, batch_id: i64) -> Vec<&'a StageData> {
    let mut stage_ids: Vec<i64> = s
        .jobs
        .iter()
        .filter(|j| {
            j.description
                .as_deref()
                .and_then(parse_description)
                .is_some_and(|d| d.run_id == q.run_id && d.batch_id == batch_id)
        })
        .flat_map(|j| j.stage_ids.iter().copied())
        .collect();
    stage_ids.sort_unstable();
    stage_ids.dedup();
    let mut stages: Vec<&StageData> = s
        .stages
        .iter()
        .filter(|st| stage_ids.contains(&st.stage_id))
        .collect();
    stages.sort_by_key(|st| st.sort_key());
    stages
}

/// Alerts belonging to one micro-batch: its stages' and tasks' failures,
/// its jobs, and its SQL execution.
pub fn batch_failures_of<'a>(
    log: &'a AlertLog,
    s: &Snapshot,
    q: &QueryHistory,
    batch_id: i64,
) -> Vec<&'a Alert> {
    let is_batch = |d: Option<&str>| {
        d.and_then(parse_description)
            .is_some_and(|d| d.run_id == q.run_id && d.batch_id == batch_id)
    };
    let job_ids: Vec<i64> = s
        .jobs
        .iter()
        .filter(|j| is_batch(j.description.as_deref()))
        .map(|j| j.job_id)
        .collect();
    let stage_ids: Vec<i64> = batch_stages_of(s, q, batch_id)
        .iter()
        .map(|st| st.stage_id)
        .collect();
    let sql_ids: Vec<i64> = s
        .sql
        .iter()
        .flatten()
        .filter(|e| is_batch(Some(&e.description)))
        .map(|e| e.id)
        .collect();
    log.newest_first()
        .filter(|a| {
            a.stage.is_some_and(|(id, _)| stage_ids.contains(&id))
                || job_ids.iter().any(|j| a.key == format!("job:{j}"))
                || sql_ids.iter().any(|e| a.key == format!("sql:{e}"))
        })
        .collect()
}

/// `" Jobs (3 of 12) · filter: foo "` or `" Jobs (12) "`.
pub fn filtered_title(base: &str, shown: usize, total: usize, f: Option<&str>) -> String {
    match f {
        Some(f) if shown != total => format!(" {base} ({shown} of {total}) · filter: {f} "),
        Some(f) => format!(" {base} ({total}) · filter: {f} "),
        None => format!(" {base} ({total}) "),
    }
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
    pub rdds: Cursor<i64>,
    /// `/` text filter per tab, lower-cased.
    pub filters: HashMap<Tab, String>,
    /// `Some` while a filter is being typed for the current tab.
    pub filter_input: Option<String>,
    pub rdd_detail: Option<RddStorageInfo>,

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

    pub streaming: Streaming,
    pub streaming_status: Option<String>,
    /// First driver log line the tap could read (epoch ms); batches before
    /// it show durations only, and the batch views say so.
    pub progress_log_start: Option<i64>,
    /// Selected query on the Streaming tab.
    pub streaming_sel: usize,
    /// Whether the driver log tap has been requested for this app.
    pub tap_requested: bool,

    /// The query whose batches are open (its query id).
    pub batch_query: Option<String>,
    pub batches_cursor: Cursor<i64>,
    /// The batch open in the detail view.
    pub batch_id: Option<i64>,
    pub batch_stages: Cursor<(i64, i64)>,
    /// Where closing a stage drill-down returns to (Main, or a batch).
    pub stage_return: View,

    pub environment: Option<ApplicationEnvironmentInfo>,
    pub env_error: Option<String>,
    pub env_scroll: u16,
    /// Lines the Environment tab rendered at the last draw; scroll bound.
    pub env_lines: usize,

    /// `taskSummary` of the slowest stages, for the summary's skew flags.
    pub stage_summaries: HashMap<(i64, i64), TaskMetricDistributions>,
    pub summary_scroll: u16,
    pub summary_lines: usize,
    /// The summary was auto-opened once for a completed app.
    pub summary_shown: bool,
    /// Where `D` / `--dump` write bundles.
    pub dump_dir: PathBuf,
    /// Log lines per executor to include in a bundle (0 = none).
    pub dump_logs: usize,
    /// A one-line notice for the footer ("wrote …"), cleared on the next key.
    pub notice: Option<String>,
}

impl App {
    pub fn new(endpoint: String, interval: Duration, watching: Option<String>) -> Self {
        Self {
            view: if watching.is_some() {
                View::Main
            } else {
                View::Picker
            },
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
            rdds: Cursor::new(),
            filters: HashMap::new(),
            filter_input: None,
            rdd_detail: None,
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
            streaming: Streaming::default(),
            streaming_status: None,
            progress_log_start: None,
            streaming_sel: 0,
            tap_requested: false,
            batch_query: None,
            batches_cursor: Cursor::new(),
            batch_id: None,
            batch_stages: Cursor::new(),
            stage_return: View::Main,
            environment: None,
            env_error: None,
            env_scroll: 0,
            env_lines: 0,
            stage_summaries: HashMap::new(),
            summary_scroll: 0,
            summary_lines: 0,
            summary_shown: false,
            dump_dir: PathBuf::from("."),
            dump_logs: 2000,
            notice: None,
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
                self.alerts.ingest(&s);
                if let Some(sql) = &s.sql {
                    self.streaming.ingest_sql(sql);
                }
                self.record_sample(&s);
                self.snapshot = Some(s);
                self.resync_all();
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
                self.detail_error =
                    Some("this execution is no longer retained by the driver".into());
            }
            Ok(DetailData::Rdd(Some(r))) => {
                self.rdd_detail = Some(r);
                self.detail_error = None;
            }
            Ok(DetailData::Rdd(None)) => {
                self.detail_error = Some("this RDD has been unpersisted".into());
            }
            Err(e) => self.detail_error = Some(e),
        }
    }

    /// Re-point every table cursor at its row after the rows changed
    /// (new snapshot, filter edited).
    fn resync_all(&mut self) {
        let Self {
            snapshot,
            filters,
            stage_filter,
            jobs,
            stages,
            executors,
            sql,
            rdds,
            alerts,
            alerts_cursor,
            ..
        } = self;
        let f = |t: Tab| filters.get(&t).map(String::as_str);
        alerts_cursor.resync(&visible_alerts(alerts, f(Tab::Failures)), |a| a.key.clone());
        let Some(s) = snapshot else { return };
        jobs.resync(&visible_jobs(s, f(Tab::Jobs)), |j| j.job_id);
        stages.resync(&visible_stages(s, stage_filter, f(Tab::Stages)), |st| {
            st.key()
        });
        executors.resync(&visible_executors(s, f(Tab::Executors)), |e| e.id.clone());
        sql.resync(&visible_sql(s, f(Tab::Sql)).unwrap_or_default(), |e| e.id);
        rdds.resync(&visible_rdds(s, f(Tab::Storage)), |r| r.id);
        self.resync_batches();
    }

    /// The `/` filter for a tab.
    pub fn filter_for(&self, tab: Tab) -> Option<&str> {
        self.filters.get(&tab).map(String::as_str)
    }

    pub fn start_table_filter(&mut self) {
        if self.tab.filterable() {
            self.filter_input = Some(self.filters.get(&self.tab).cloned().unwrap_or_default());
        }
    }

    /// Returns true if there was a filter to clear.
    pub fn clear_table_filter(&mut self) -> bool {
        if self.filters.remove(&self.tab).is_none() {
            return false;
        }
        self.resync_all();
        true
    }

    fn commit_table_filter(&mut self) {
        if let Some(text) = self.filter_input.take() {
            let text = text.trim().to_lowercase();
            if text.is_empty() {
                self.filters.remove(&self.tab);
            } else {
                self.filters.insert(self.tab, text);
            }
            self.env_scroll = 0;
            self.resync_all();
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
            alive_executors: s
                .executors
                .iter()
                .filter(|e| e.is_active && e.id != "driver")
                .count() as i64,
            removed_executors: s.executors.iter().filter(|e| !e.is_active).count() as i64,
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
            self.rdds = Cursor::new();
            self.filters.clear();
            self.filter_input = None;
            self.alerts.clear();
            self.alerts_cursor = Cursor::new();
            self.logs.close();
            self.threads = ThreadsView::default();
            self.streaming = Streaming::default();
            self.streaming_status = None;
            self.progress_log_start = None;
            self.streaming_sel = 0;
            self.tap_requested = false;
            self.batch_query = None;
            self.batch_id = None;
            self.batches_cursor = Cursor::new();
            self.batch_stages = Cursor::new();
            self.stage_return = View::Main;
            self.environment = None;
            self.env_error = None;
            self.env_scroll = 0;
            self.stage_summaries.clear();
            self.summary_shown = false;
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
        visible_stages(s, &self.stage_filter, self.filter_for(Tab::Stages))
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
        visible_alerts(&self.alerts, self.filter_for(Tab::Failures))
            .get(self.alerts_cursor.selected())
            .copied()
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
        self.snapshot
            .as_ref()?
            .executors
            .iter()
            .find(|e| e.id == id)
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
            since_ms: None,
        };
        if !matches!(self.view, View::Logs | View::Threads) {
            self.return_view = self.view;
        }
        self.logs.open(target.clone(), filter);
        self.view = View::Logs;
        target
    }

    /// `L` on the Executors tab.
    fn selected_executor_id(&self) -> Option<String> {
        let s = self.snapshot.as_ref()?;
        visible_executors(s, self.filter_for(Tab::Executors))
            .get(self.executors.selected())
            .map(|e| e.id.clone())
    }

    pub fn open_logs_selected_executor(&mut self) -> Option<LogTarget> {
        let id = self.selected_executor_id()?;
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
        t.http_url = self
            .executor(&t.executor_id)
            .and_then(|e| e.log_url(t.stream.name()));
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
        let id = self.selected_executor_id()?;
        Some(self.open_threads(id))
    }

    pub fn close_threads(&mut self) {
        self.threads = ThreadsView::default();
        self.view = self.return_view;
    }

    pub fn apply_threads(
        &mut self,
        executor_id: &str,
        result: Result<Option<Vec<ThreadStackTrace>>, String>,
    ) {
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

    pub fn apply_environment(&mut self, result: Result<ApplicationEnvironmentInfo, String>) {
        match result {
            Ok(e) => {
                self.environment = Some(e);
                self.env_error = None;
            }
            Err(e) => self.env_error = Some(e),
        }
    }

    // ------------------------------------------------------ summary & dump

    pub fn summary_sections(&self) -> Vec<report::Section> {
        report::summarize(&report::Inputs {
            endpoint: &self.endpoint,
            snapshot: self.snapshot.as_ref(),
            alerts: &self.alerts,
            streaming: &self.streaming,
            environment: self.environment.as_ref(),
            stage_summaries: &self.stage_summaries,
        })
    }

    /// The slowest stages whose quantiles the summary still lacks.
    pub fn stages_needing_summaries(&self) -> Vec<(i64, i64)> {
        let Some(s) = &self.snapshot else {
            return Vec::new();
        };
        let mut stages: Vec<&StageData> = s
            .stages
            .iter()
            .filter(|st| st.executor_run_time > 0)
            .collect();
        stages.sort_by_key(|st| -st.executor_run_time);
        stages
            .iter()
            .take(5)
            .map(|st| st.key())
            .filter(|k| !self.stage_summaries.contains_key(k))
            .collect()
    }

    /// `S`: open the summary. Returns the stage summaries to fetch.
    pub fn open_summary(&mut self) -> Vec<(i64, i64)> {
        if !matches!(self.view, View::Summary) {
            self.return_view = self.view;
        }
        self.summary_scroll = 0;
        self.view = View::Summary;
        self.stages_needing_summaries()
    }

    pub fn close_summary(&mut self) {
        self.view = self.return_view;
    }

    /// A finished application (History Server) opens on its summary once.
    pub fn maybe_auto_summary(&mut self) -> Option<Vec<(i64, i64)>> {
        if self.summary_shown || self.view != View::Main {
            return None;
        }
        let completed = self
            .snapshot
            .as_ref()?
            .app
            .attempts
            .first()
            .is_some_and(|a| a.completed);
        if !completed {
            return None;
        }
        self.summary_shown = true;
        Some(self.open_summary())
    }

    pub fn apply_stage_summaries(&mut self, v: Vec<((i64, i64), Option<TaskMetricDistributions>)>) {
        for (k, d) in v {
            if let Some(d) = d {
                self.stage_summaries.insert(k, d);
            }
        }
    }

    /// Executor ids whose logs a dump should include.
    pub fn dump_executor_ids(&self) -> Vec<String> {
        self.snapshot
            .as_ref()
            .map(|s| {
                s.executors
                    .iter()
                    .filter(|e| e.id != "driver")
                    .map(|e| e.id.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Write the bundle with these log tails (plus whatever log view is open).
    pub fn write_dump(&mut self, mut logs: Vec<(String, Vec<String>)>) {
        if let Some(t) = &self.logs.target
            && self.logs.total() > 0
        {
            let name = format!("viewer-executor-{}.log", t.executor_id);
            logs.push((
                name,
                self.logs.visible().iter().map(|l| l.to_string()).collect(),
            ));
        }
        let sections = self.summary_sections();
        let inputs = report::Inputs {
            endpoint: &self.endpoint,
            snapshot: self.snapshot.as_ref(),
            alerts: &self.alerts,
            streaming: &self.streaming,
            environment: self.environment.as_ref(),
            stage_summaries: &self.stage_summaries,
        };
        self.notice = Some(
            match report::write_bundle(&self.dump_dir, &inputs, &sections, &logs) {
                Ok(path) => format!("wrote {}", path.display()),
                Err(e) => format!("dump failed: {e:#}"),
            },
        );
    }

    /// `m` on the Executors tab.
    pub fn open_executor_memory(&mut self) {
        if self
            .snapshot
            .as_ref()
            .is_some_and(|s| !s.executors.is_empty())
        {
            self.return_view = View::Main;
            self.view = View::ExecutorMemory;
        }
    }

    pub fn close_executor_memory(&mut self) {
        self.view = View::Main;
    }

    // -------------------------------------------------------------- streaming

    pub fn apply_progress(&mut self, p: Progress) {
        self.streaming.ingest_progress(p);
        self.resync_batches();
    }

    // ---------------------------------------------------------- batch views

    /// The query selected on the Streaming tab (display order).
    fn selected_query(&self) -> Option<&QueryHistory> {
        self.streaming.sorted().get(self.streaming_sel).copied()
    }

    pub fn batch_query_history(&self) -> Option<&QueryHistory> {
        self.streaming.queries.get(self.batch_query.as_ref()?)
    }

    /// Enter on the Streaming tab.
    pub fn open_batches(&mut self) -> bool {
        let Some(q) = self.selected_query() else {
            return false;
        };
        let id = q.query_id.clone();
        if self.batch_query.as_deref() != Some(id.as_str()) {
            self.batches_cursor = Cursor::new();
        }
        self.batch_query = Some(id);
        self.view = View::Batches;
        self.resync_batches();
        true
    }

    pub fn close_batches(&mut self) {
        self.view = View::Main;
    }

    /// Enter on a batch row.
    pub fn open_batch(&mut self) -> bool {
        let Some(b) = self.selected_batch() else {
            return false;
        };
        let id = b.batch_id;
        if self.batch_id != Some(id) {
            self.batch_stages = Cursor::new();
        }
        self.batch_id = Some(id);
        self.view = View::Batch;
        self.resync_batches();
        true
    }

    pub fn close_batch(&mut self) {
        self.view = View::Batches;
    }

    pub fn selected_batch(&self) -> Option<&Batch> {
        let q = self.batch_query_history()?;
        batches_newest_first(q)
            .get(self.batches_cursor.selected())
            .copied()
    }

    pub fn open_batch_detail_data(&self) -> Option<(&QueryHistory, &Batch)> {
        let q = self.batch_query_history()?;
        let b = q.batches.get(&self.batch_id?)?;
        Some((q, b))
    }

    /// Enter on a stage row of the batch view: the stage drill-down, with
    /// Esc returning here.
    pub fn open_batch_stage(&mut self) -> Option<Detail> {
        let (q, b) = self.open_batch_detail_data()?;
        let st = *batch_stages_of(self.snapshot.as_ref()?, q, b.batch_id)
            .get(self.batch_stages.selected())?;
        let (id, attempt) = st.key();
        let failed = st.num_failed_tasks > 0 && st.status == "FAILED";
        let target = self.open_stage(id, attempt, failed);
        self.stage_return = View::Batch;
        Some(target)
    }

    /// `L` in the batch view: the driver log, sliced to the batch's window.
    pub fn open_batch_logs(&mut self) -> Option<LogTarget> {
        let (_, b) = self.open_batch_detail_data()?;
        let window = b.window_ms();
        let batch_id = b.batch_id;
        let mut target = self.open_logs("driver".into(), None);
        // Re-open with the window, and ask the stream to start there: an
        // early batch is long gone from the last 2000 lines. Fall back to a
        // text hint when the batch has no timestamps to slice by.
        target.since_ms = window.map(|(start, _)| start);
        let filter = if window.is_none() {
            Some(format!("batch {batch_id}"))
        } else {
            None
        };
        self.logs.open_window(target.clone(), filter, window);
        Some(target)
    }

    /// `c` in the log view: clears the filter; when a batch window was
    /// set, re-opens the stream from the tail so the whole log is back.
    pub fn logs_widen(&mut self) -> Option<LogTarget> {
        let windowed = self.logs.window.is_some();
        self.logs.clear_filter();
        if !windowed {
            return None;
        }
        let mut t = self.logs.target.clone()?;
        t.since_ms = None;
        self.logs.open(t.clone(), None);
        Some(t)
    }

    fn resync_batches(&mut self) {
        let Self {
            streaming,
            batch_query,
            batch_id,
            batches_cursor,
            batch_stages,
            snapshot,
            ..
        } = self;
        let Some(q) = batch_query
            .as_ref()
            .and_then(|id| streaming.queries.get(id))
        else {
            return;
        };
        batches_cursor.resync(&batches_newest_first(q), |b| b.batch_id);
        if let (Some(s), Some(bid)) = (snapshot.as_ref(), *batch_id) {
            batch_stages.resync(&batch_stages_of(s, q, bid), |st| st.key());
        }
    }

    /// Entering the Streaming tab: returns true the first time, when the
    /// driver log tap should be started.
    pub fn want_tap(&mut self) -> bool {
        if self.tap_requested {
            return false;
        }
        self.tap_requested = true;
        true
    }

    /// Text input for the `/` filter in the logs or threads view. Returns
    /// true if the key was consumed as text.
    pub fn filter_input_key(&mut self, key: crossterm::event::KeyCode) -> bool {
        use crossterm::event::KeyCode::*;
        match self.view {
            View::Main if self.filter_input.is_some() => {
                match key {
                    Char(c) => self.filter_input.as_mut().unwrap().push(c),
                    Backspace => {
                        self.filter_input.as_mut().unwrap().pop();
                    }
                    Enter => self.commit_table_filter(),
                    Esc => self.filter_input = None,
                    _ => {}
                }
                true
            }
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
                        let f = self
                            .threads
                            .filter_input
                            .take()
                            .unwrap()
                            .trim()
                            .to_lowercase();
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
        visible_sql(self.snapshot.as_ref()?, self.filter_for(Tab::Sql))?
            .get(self.sql.selected())
            .copied()
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
            Pane::Nodes => e
                .nodes
                .iter()
                .map(|n| crate::ui::sql_detail::node_lines(n).len())
                .sum(),
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
        self.rdd_detail = None;
        self.detail_error = None;
        if self.view == View::Stage {
            self.view = std::mem::replace(&mut self.stage_return, View::Main);
        } else if matches!(self.view, View::Sql | View::Alert | View::Rdd) {
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
        let Some(job) = visible_jobs(s, self.filter_for(Tab::Jobs))
            .get(self.jobs.selected())
            .copied()
        else {
            return;
        };
        self.stage_filter = Some(StageFilter {
            job_id: job.job_id,
            stage_ids: job.stage_ids.clone(),
        });
        self.stages = Cursor::new();
        self.tab = Tab::Stages;
        self.resync_all();
    }

    /// Returns true if there was a filter to clear.
    pub fn clear_stage_filter(&mut self) -> bool {
        if self.stage_filter.take().is_none() {
            return false;
        }
        self.resync_all();
        true
    }

    // --------------------------------------------------------------- storage

    pub fn selected_rdd(&self) -> Option<&RddStorageInfo> {
        let s = self.snapshot.as_ref()?;
        visible_rdds(s, self.filter_for(Tab::Storage))
            .get(self.rdds.selected())
            .copied()
    }

    pub fn open_rdd_detail(&mut self) -> Option<Detail> {
        let target = Detail::Rdd(self.selected_rdd()?.id);
        if self.detail_target != Some(target) {
            self.rdd_detail = None;
            self.detail_error = None;
        }
        self.detail_target = Some(target);
        self.view = View::Rdd;
        Some(target)
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
            View::Rdd => 0,
            View::ExecutorMemory => self.snapshot.as_ref().map_or(0, |s| {
                visible_executors(s, self.filter_for(Tab::Executors)).len()
            }),
            View::Summary => self.summary_lines,
            View::Batches => self.batch_query_history().map_or(0, |q| q.batches.len()),
            View::Batch => match (self.snapshot.as_ref(), self.open_batch_detail_data()) {
                (Some(s), Some((q, b))) => batch_stages_of(s, q, b.batch_id).len(),
                _ => 0,
            },
            View::Main => {
                if self.tab == Tab::Failures {
                    return visible_alerts(&self.alerts, self.filter_for(Tab::Failures)).len();
                }
                if self.tab == Tab::Streaming {
                    return self.streaming.queries.len();
                }
                if self.tab == Tab::Environment {
                    return self.env_lines;
                }
                let Some(s) = &self.snapshot else { return 0 };
                let f = self.filter_for(self.tab);
                match self.tab {
                    Tab::Jobs => visible_jobs(s, f).len(),
                    Tab::Stages => visible_stages(s, &self.stage_filter, f).len(),
                    Tab::Executors => visible_executors(s, f).len(),
                    Tab::Sql => visible_sql(s, f).map_or(0, |v| v.len()),
                    Tab::Storage => visible_rdds(s, f).len(),
                    Tab::Overview | Tab::Failures | Tab::Streaming | Tab::Environment => 0,
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
            View::Rdd => 0,
            View::Batches => self.batches_cursor.selected(),
            View::Batch => self.batch_stages.selected(),
            View::ExecutorMemory => self.executors.selected(),
            View::Summary => self.summary_scroll as usize,
            View::Main => match self.tab {
                Tab::Jobs => self.jobs.selected(),
                Tab::Stages => self.stages.selected(),
                Tab::Executors => self.executors.selected(),
                Tab::Sql => self.sql.selected(),
                Tab::Failures => self.alerts_cursor.selected(),
                Tab::Streaming => self.streaming_sel,
                Tab::Storage => self.rdds.selected(),
                Tab::Environment => self.env_scroll as usize,
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
            View::Rdd => {}
            View::Summary => {
                self.summary_scroll = index.min(self.summary_lines.saturating_sub(1)) as u16;
            }
            View::ExecutorMemory => {
                let Self {
                    snapshot,
                    filters,
                    executors,
                    ..
                } = self;
                if let Some(s) = snapshot {
                    let f = filters.get(&Tab::Executors).map(String::as_str);
                    executors.select(index, &visible_executors(s, f), |e| e.id.clone());
                }
            }
            View::Batches => {
                let Self {
                    streaming,
                    batch_query,
                    batches_cursor,
                    ..
                } = self;
                if let Some(q) = batch_query
                    .as_ref()
                    .and_then(|id| streaming.queries.get(id))
                {
                    batches_cursor.select(index, &batches_newest_first(q), |b| b.batch_id);
                }
            }
            View::Batch => {
                let Self {
                    streaming,
                    batch_query,
                    batch_id,
                    batch_stages,
                    snapshot,
                    ..
                } = self;
                if let (Some(s), Some(q), Some(bid)) = (
                    snapshot.as_ref(),
                    batch_query
                        .as_ref()
                        .and_then(|id| streaming.queries.get(id)),
                    *batch_id,
                ) {
                    batch_stages.select(index, &batch_stages_of(s, q, bid), |st| st.key());
                }
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
                    let rows = visible_alerts(&self.alerts, self.filter_for(Tab::Failures));
                    return self.alerts_cursor.select(index, &rows, |a| a.key.clone());
                }
                if self.tab == Tab::Streaming {
                    self.streaming_sel = index;
                    return;
                }
                if self.tab == Tab::Environment {
                    self.env_scroll = index.min(self.env_lines.saturating_sub(1)) as u16;
                    return;
                }
                let Self {
                    snapshot,
                    tab,
                    filters,
                    jobs,
                    stages,
                    stage_filter,
                    executors,
                    sql,
                    rdds,
                    ..
                } = self;
                let Some(s) = snapshot else { return };
                let f = filters.get(tab).map(String::as_str);
                match tab {
                    Tab::Jobs => jobs.select(index, &visible_jobs(s, f), |j| j.job_id),
                    Tab::Stages => {
                        stages.select(index, &visible_stages(s, stage_filter, f), |st| st.key())
                    }
                    Tab::Executors => {
                        executors.select(index, &visible_executors(s, f), |e| e.id.clone())
                    }
                    Tab::Sql => sql.select(index, &visible_sql(s, f).unwrap_or_default(), |e| e.id),
                    Tab::Storage => rdds.select(index, &visible_rdds(s, f), |r| r.id),
                    Tab::Overview | Tab::Failures | Tab::Streaming | Tab::Environment => {}
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
        if self.view == View::Summary {
            let max = self.summary_lines.saturating_sub(1) as isize;
            self.summary_scroll = (self.summary_scroll as isize + delta).clamp(0, max) as u16;
            return;
        }
        if self.view == View::Main && self.tab == Tab::Environment {
            let max = self.env_lines.saturating_sub(1) as isize;
            self.env_scroll = (self.env_scroll as isize + delta).clamp(0, max) as u16;
            return;
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
            stages: vec![
                stage(9, "ACTIVE"),
                stage(8, "COMPLETE"),
                stage(7, "COMPLETE"),
            ],
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
        let ids: Vec<i64> = visible_stages(app.snapshot.as_ref().unwrap(), &app.stage_filter, None)
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
    fn table_filter_narrows_rows_and_keeps_cursor_on_a_matching_row() {
        let mut app = watched_app();
        let snap = Snapshot {
            stages: vec![
                StageData {
                    stage_id: 9,
                    status: "ACTIVE".into(),
                    name: "mapPartitions at Writer.scala:88".into(),
                    ..Default::default()
                },
                StageData {
                    stage_id: 8,
                    status: "COMPLETE".into(),
                    name: "exchange at Writer.scala:70".into(),
                    ..Default::default()
                },
                StageData {
                    stage_id: 4,
                    status: "FAILED".into(),
                    name: "collect at Main.scala:22".into(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        app.apply_snapshot("a", Ok(snap));
        app.tab = Tab::Stages;
        app.move_selection(2); // stage 4
        app.start_table_filter();
        for c in "WRITER".chars() {
            assert!(app.filter_input_key(crossterm::event::KeyCode::Char(c)));
        }
        assert!(app.filter_input_key(crossterm::event::KeyCode::Enter));
        assert_eq!(app.filter_for(Tab::Stages), Some("writer"));
        let ids: Vec<i64> = visible_stages(
            app.snapshot.as_ref().unwrap(),
            &None,
            app.filter_for(Tab::Stages),
        )
        .iter()
        .map(|s| s.stage_id)
        .collect();
        assert_eq!(ids, [9, 8]);
        // Stage 4 vanished from view; the cursor lands on a visible row.
        assert!(app.selected_stage().is_some());
        assert_eq!(
            filtered_title("Stages", 2, 3, app.filter_for(Tab::Stages)),
            " Stages (2 of 3) · filter: writer "
        );
        assert!(app.clear_table_filter());
        assert!(!app.clear_table_filter());
        assert_eq!(app.filter_for(Tab::Stages), None);
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

        app.apply_log(LogEvent::Lines(vec![
            "INFO ok".into(),
            "ERROR ExecutorLostFailure boom".into(),
        ]));
        assert_eq!(app.logs.visible(), ["ERROR ExecutorLostFailure boom"]);
        app.close_logs();
        assert_eq!(app.view, View::Stage, "Esc returns to where L was pressed");
    }

    #[test]
    fn batch_views_find_the_batch_stages_and_failures() {
        use crate::streaming::Progress;
        let desc = |batch: i64| Some(format!("orders-agg\nid = q\nrunId = r\nbatch = {batch}"));
        let mut app = watched_app();
        let snap = Snapshot {
            jobs: vec![
                JobData {
                    job_id: 1,
                    description: desc(4),
                    stage_ids: vec![31, 30],
                    ..Default::default()
                },
                JobData {
                    job_id: 2,
                    description: desc(5),
                    stage_ids: vec![32],
                    ..Default::default()
                },
            ],
            stages: vec![
                StageData {
                    stage_id: 30,
                    status: "COMPLETE".into(),
                    ..Default::default()
                },
                StageData {
                    stage_id: 31,
                    status: "FAILED".into(),
                    failure_reason: Some("boom".into()),
                    ..Default::default()
                },
                StageData {
                    stage_id: 32,
                    status: "COMPLETE".into(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        app.apply_snapshot("a", Ok(snap));
        for b in [3, 4, 5] {
            app.apply_progress(Progress {
                id: "q".into(),
                run_id: "r".into(),
                name: Some("orders-agg".into()),
                batch_id: b,
                timestamp: "2026-10-01T18:03:15.000Z".into(),
                ..Default::default()
            });
        }
        app.tab = Tab::Streaming;
        assert!(app.open_batches());
        assert_eq!(app.view, View::Batches);
        assert_eq!(app.selected_batch().unwrap().batch_id, 5, "newest first");
        app.move_selection(1);
        assert!(app.open_batch());
        assert_eq!(app.batch_id, Some(4));
        let (q, b) = app.open_batch_detail_data().unwrap();
        let s = app.snapshot.as_ref().unwrap();
        let ids: Vec<i64> = batch_stages_of(s, q, b.batch_id)
            .iter()
            .map(|st| st.stage_id)
            .collect();
        assert_eq!(ids, [31, 30], "failed first, then by id");
        let fails = batch_failures_of(&app.alerts, s, q, b.batch_id);
        assert_eq!(fails.len(), 1);
        assert_eq!(fails[0].stage, Some((31, 0)));
        // Enter on the first stage opens the drill-down; Esc comes back here.
        let target = app.open_batch_stage().unwrap();
        assert_eq!(target, Detail::Stage { id: 31, attempt: 0 });
        assert_eq!(app.view, View::Stage);
        app.close_detail();
        assert_eq!(app.view, View::Batch);
        // L opens the driver log sliced to the batch's window.
        let lt = app.open_batch_logs().unwrap();
        assert_eq!(lt.executor_id, "driver");
        assert!(app.logs.window.is_some());
        assert_eq!(app.view, View::Logs);
        app.close_logs();
        assert_eq!(app.view, View::Batch);
    }

    #[test]
    fn sql_detail_scrolls_focused_pane_within_bounds() {
        let mut app = watched_app();
        let snap = Snapshot {
            sql: Some(vec![ExecutionData {
                id: 12,
                ..Default::default()
            }]),
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
        assert!(
            app.detail_error
                .as_deref()
                .unwrap()
                .contains("no longer retained")
        );
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
            slowest: vec![TaskData {
                task_id: 1,
                ..Default::default()
            }],
            ..Default::default()
        };
        app.apply_detail(target, Ok(DetailData::Stage(detail)));
        assert_eq!(app.visible_tasks().len(), 1);

        app.close_detail();
        assert_eq!(app.view, View::Main);
        assert!(app.detail_target.is_none());
    }
}
