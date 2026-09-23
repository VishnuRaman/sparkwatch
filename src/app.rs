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

pub struct App {
    pub tab: Tab,
    pub snapshot: Option<Snapshot>,
    pub last_error: Option<String>,
    pub last_update: Option<Instant>,
    pub interval: Duration,
    pub paused: bool,
    pub should_quit: bool,
    pub jobs: Cursor<i64>,
    pub stages: Cursor<(i64, i64)>,
    pub executors: Cursor<String>,
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
            jobs: Cursor::new(),
            stages: Cursor::new(),
            executors: Cursor::new(),
            endpoint,
        }
    }

    pub fn apply(&mut self, result: Result<Snapshot, String>) {
        match result {
            Ok(s) => {
                self.jobs.resync(&s.jobs, |j| j.job_id);
                self.stages.resync(&s.stages, |st| st.key());
                self.executors.resync(&s.executors, |e| e.id.clone());
                self.snapshot = Some(s);
                self.last_error = None;
                self.last_update = Some(Instant::now());
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

    fn current_index(&self) -> usize {
        match self.tab {
            Tab::Jobs => self.jobs.selected(),
            Tab::Stages => self.stages.selected(),
            Tab::Executors => self.executors.selected(),
            Tab::Overview => 0,
        }
    }

    fn select_index(&mut self, index: usize) {
        let Some(s) = &self.snapshot else { return };
        match self.tab {
            Tab::Jobs => self.jobs.select(index, &s.jobs, |j| j.job_id),
            Tab::Stages => self.stages.select(index, &s.stages, |st| st.key()),
            Tab::Executors => self.executors.select(index, &s.executors, |e| e.id.clone()),
            Tab::Overview => {}
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
}
