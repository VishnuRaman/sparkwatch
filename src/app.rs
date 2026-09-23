use crate::spark::Snapshot;
use ratatui::widgets::TableState;
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

pub struct App {
    pub tab: Tab,
    pub snapshot: Option<Snapshot>,
    pub last_error: Option<String>,
    pub last_update: Option<Instant>,
    pub interval: Duration,
    pub paused: bool,
    pub should_quit: bool,
    pub jobs_state: TableState,
    pub stages_state: TableState,
    pub executors_state: TableState,
    pub endpoint: String,
}

impl App {
    pub fn new(endpoint: String, interval: Duration) -> Self {
        Self {
            tab: Tab::Overview,
            snapshot: None,
            last_error: None,
            last_update: None,
            interval,
            paused: false,
            should_quit: false,
            jobs_state: TableState::default().with_selected(Some(0)),
            stages_state: TableState::default().with_selected(Some(0)),
            executors_state: TableState::default().with_selected(Some(0)),
            endpoint,
        }
    }

    pub fn apply(&mut self, result: Result<Snapshot, String>) {
        match result {
            Ok(s) => {
                self.snapshot = Some(s);
                self.last_error = None;
                self.last_update = Some(Instant::now());
                self.clamp_selection();
            }
            // Keep showing the last good snapshot; surface the error in the header.
            Err(e) => self.last_error = Some(e),
        }
    }

    /// Row count of the table currently in focus.
    fn current_len(&self) -> usize {
        let Some(s) = &self.snapshot else { return 0 };
        match self.tab {
            Tab::Jobs => s.jobs.len(),
            Tab::Stages => s.stages.len(),
            Tab::Executors => s.executors.len(),
            Tab::Overview => 0,
        }
    }

    fn current_state(&mut self) -> Option<&mut TableState> {
        match self.tab {
            Tab::Jobs => Some(&mut self.jobs_state),
            Tab::Stages => Some(&mut self.stages_state),
            Tab::Executors => Some(&mut self.executors_state),
            Tab::Overview => None,
        }
    }

    fn clamp_selection(&mut self) {
        let len = self.current_len();
        if let Some(state) = self.current_state() {
            let sel = state.selected().unwrap_or(0);
            state.select(if len == 0 {
                None
            } else {
                Some(sel.min(len - 1))
            });
        }
    }

    pub fn move_selection(&mut self, delta: isize) {
        let len = self.current_len();
        if len == 0 {
            return;
        }
        if let Some(state) = self.current_state() {
            let cur = state.selected().unwrap_or(0) as isize;
            let next = (cur + delta).rem_euclid(len as isize) as usize;
            state.select(Some(next));
        }
    }

    pub fn select_edge(&mut self, last: bool) {
        let len = self.current_len();
        if len == 0 {
            return;
        }
        if let Some(state) = self.current_state() {
            state.select(Some(if last { len - 1 } else { 0 }));
        }
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
