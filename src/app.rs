use crate::poller::{Detail, DetailData};
use crate::spark::{ApplicationInfo, Snapshot, StageData, StageDetail, TaskData};
use ratatui::widgets::TableState;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tab {
    Overview,
    Jobs,
    Stages,
    Executors,
}

impl Tab {
    pub const ALL: [Tab; 4] = [Tab::Overview, Tab::Jobs, Tab::Stages, Tab::Executors];

    pub fn title(&self) -> &'static str {
        match self {
            Tab::Overview => "Overview",
            Tab::Jobs => "Jobs",
            Tab::Stages => "Stages",
            Tab::Executors => "Executors",
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

    /// The drill-down that is open (and being refreshed by the poller).
    pub detail_target: Option<Detail>,
    pub stage_detail: Option<StageDetail>,
    pub detail_error: Option<String>,
    pub tasks: Cursor<i64>,
    /// Task table shows failed tasks instead of the slowest ones.
    pub show_failed: bool,
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
            detail_target: None,
            stage_detail: None,
            detail_error: None,
            tasks: Cursor::new(),
            show_failed: false,
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
        let target = Detail::Stage {
            id: st.stage_id,
            attempt: st.attempt_id,
        };
        // A failed stage is opened on its failures; a live one on its stragglers.
        let start_on_failed = st.num_failed_tasks > 0 && st.status == "FAILED";
        if self.detail_target != Some(target) {
            self.stage_detail = None;
            self.detail_error = None;
            self.tasks = Cursor::new();
            self.show_failed = start_on_failed;
        }
        self.detail_target = Some(target);
        self.view = View::Stage;
        Some(target)
    }

    pub fn close_detail(&mut self) {
        self.detail_target = None;
        self.stage_detail = None;
        self.detail_error = None;
        if self.view == View::Stage {
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
            View::Main => {
                let Some(s) = &self.snapshot else { return 0 };
                match self.tab {
                    Tab::Jobs => s.jobs.len(),
                    Tab::Stages => visible_stages(s, &self.stage_filter).len(),
                    Tab::Executors => s.executors.len(),
                    Tab::Overview => 0,
                }
            }
        }
    }

    fn current_index(&self) -> usize {
        match self.view {
            View::Picker => self.picker.selected(),
            View::Stage => self.tasks.selected(),
            View::Main => match self.tab {
                Tab::Jobs => self.jobs.selected(),
                Tab::Stages => self.stages.selected(),
                Tab::Executors => self.executors.selected(),
                Tab::Overview => 0,
            },
        }
    }

    fn select_index(&mut self, index: usize) {
        match self.view {
            View::Picker => self.picker.select(index, &self.apps, |a| a.id.clone()),
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
                let Self {
                    snapshot,
                    tab,
                    jobs,
                    stages,
                    stage_filter,
                    executors,
                    ..
                } = self;
                let Some(s) = snapshot else { return };
                match tab {
                    Tab::Jobs => jobs.select(index, &s.jobs, |j| j.job_id),
                    Tab::Stages => stages.select(index, &visible_stages(s, stage_filter), |st| st.key()),
                    Tab::Executors => executors.select(index, &s.executors, |e| e.id.clone()),
                    Tab::Overview => {}
                }
            }
        }
    }

    pub fn move_selection(&mut self, delta: isize) {
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
