//! substring filter and a scroll position that survives new lines arriving.

use std::collections::VecDeque;

/// Enough for a chatty executor's recent past; a long-running streaming
/// driver would otherwise eat memory line by line.
pub const MAX_LINES: usize = 20_000;

/// Which stream of an executor to show (HTTP sources only; kubectl merges).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stream {
    Stderr,
    Stdout,
}

impl Stream {
    pub fn name(&self) -> &'static str {
        match self {
            Stream::Stderr => "stderr",
            Stream::Stdout => "stdout",
        }
    }

    pub fn other(&self) -> Stream {
        match self {
            Stream::Stderr => Stream::Stdout,
            Stream::Stdout => Stream::Stderr,
        }
    }
}

/// What the user asked to see. Sent to the poller, which picks the source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogTarget {
    pub executor_id: String,
    /// `kubectl logs --previous`: the container before the last restart.
    pub previous: bool,
    pub stream: Stream,
    /// The `executorLogs` URL for `stream`, when the app reports one.
    pub http_url: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Severity {
    Error,
    Warn,
    Plain,
}

pub fn severity(line: &str) -> Severity {
    if line.contains(" ERROR ")
        || line.contains("Exception")
        || line.contains("Error:")
        || line.contains("FATAL")
    {
        Severity::Error
    } else if line.contains(" WARN ") {
        Severity::Warn
    } else {
        Severity::Plain
    }
}

#[derive(Default)]
pub struct LogView {
    pub target: Option<LogTarget>,
    lines: VecDeque<String>,
    /// Lower-cased substring; lines not containing it are hidden.
    pub filter: Option<String>,
    /// `Some` while the user is typing a filter after `/`.
    pub filter_input: Option<String>,
    /// Stick to the tail as lines arrive. Scrolling up turns it off.
    pub follow: bool,
    /// First visible line, as an index into the *filtered* lines.
    pub scroll: usize,
    pub wrap: bool,
    /// Source's last word: "streaming pod/x", "pod gone — …", "kubectl exited".
    pub status: Option<String>,
    /// Lines the source has delivered in total (for the title).
    pub received: usize,
}

impl LogView {
    pub fn open(&mut self, target: LogTarget, filter: Option<String>) {
        *self = LogView {
            target: Some(target),
            filter: filter.map(|f| f.to_lowercase()),
            follow: true,
            ..Default::default()
        };
    }

    pub fn close(&mut self) {
        *self = LogView::default();
    }

    pub fn is_open(&self) -> bool {
        self.target.is_some()
    }

    pub fn append(&mut self, new: Vec<String>) {
        self.received += new.len();
        for l in new {
            if self.lines.len() == MAX_LINES {
                self.lines.pop_front();
                // The window slid under us; keep the same lines on screen.
                self.scroll = self.scroll.saturating_sub(1);
            }
            self.lines.push_back(l);
        }
    }

    /// An HTTP source hands us a fresh tail each time rather than a stream.
    pub fn replace(&mut self, lines: Vec<String>) {
        self.lines.clear();
        self.received = lines.len();
        self.lines.extend(lines);
    }

    pub fn total(&self) -> usize {
        self.lines.len()
    }

    /// Lines that pass the filter, in order.
    pub fn visible(&self) -> Vec<&str> {
        match &self.filter {
            None => self.lines.iter().map(String::as_str).collect(),
            Some(f) => self
                .lines
                .iter()
                .filter(|l| l.to_lowercase().contains(f.as_str()))
                .map(String::as_str)
                .collect(),
        }
    }

    /// The slice to draw for a viewport `height` rows tall, and the index
    /// of its first line (for the title).
    pub fn window(&self, height: usize) -> (Vec<&str>, usize) {
        let vis = self.visible();
        let start = if self.follow {
            vis.len().saturating_sub(height)
        } else {
            self.scroll.min(vis.len().saturating_sub(1))
        };
        let end = (start + height).min(vis.len());
        (vis[start..end].to_vec(), start)
    }

    pub fn scroll_by(&mut self, delta: isize, height: usize) {
        let n = self.visible().len();
        let max_start = n.saturating_sub(height);
        // Leaving follow mode: start from where the tail currently is.
        let cur = if self.follow { max_start } else { self.scroll };
        let next = (cur as isize + delta).clamp(0, max_start as isize) as usize;
        self.scroll = next;
        // Scrolling back down to the end re-engages follow.
        self.follow = next >= max_start && delta > 0;
    }

    pub fn scroll_to_end(&mut self) {
        self.follow = true;
    }

    pub fn scroll_to_start(&mut self) {
        self.follow = false;
        self.scroll = 0;
    }

    // --------------------------------------------------------- filter input

    pub fn start_filter(&mut self) {
        self.filter_input = Some(self.filter.clone().unwrap_or_default());
    }

    pub fn filter_push(&mut self, c: char) {
        if let Some(f) = &mut self.filter_input {
            f.push(c);
        }
    }

    pub fn filter_pop(&mut self) {
        if let Some(f) = &mut self.filter_input {
            f.pop();
        }
    }

    pub fn filter_commit(&mut self) {
        if let Some(f) = self.filter_input.take() {
            let f = f.trim().to_lowercase();
            self.filter = if f.is_empty() { None } else { Some(f) };
            self.follow = true;
        }
    }

    pub fn filter_cancel(&mut self) {
        self.filter_input = None;
    }

    pub fn clear_filter(&mut self) {
        self.filter = None;
        self.follow = true;
    }
}

/// The first "word" of an error message, for pre-setting the log filter when
/// jumping in from a failed task: `java.lang.OutOfMemoryError: Java heap
/// space` → `java.lang.OutOfMemoryError`; `ExecutorLostFailure (executor 2
/// …` → `ExecutorLostFailure`.
pub fn filter_from_error(error: &str) -> Option<String> {
    let first = error.lines().find(|l| !l.trim().is_empty())?.trim();
    let token: String = first
        .chars()
        .take_while(|c| !c.is_whitespace() && *c != ':' && *c != '(')
        .collect();
    if token.len() >= 4 { Some(token) } else { None }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn view_with(n: usize) -> LogView {
        let mut v = LogView::default();
        v.open(
            LogTarget {
                executor_id: "1".into(),
                previous: false,
                stream: Stream::Stderr,
                http_url: None,
            },
            None,
        );
        v.append((0..n).map(|i| format!("line {i}")).collect());
        v
    }

    #[test]
    fn follow_shows_tail_and_scrolling_up_leaves_it() {
        let mut v = view_with(100);
        let (win, start) = v.window(10);
        assert_eq!(start, 90);
        assert_eq!(win.last(), Some(&"line 99"));

        v.scroll_by(-5, 10);
        assert!(!v.follow);
        assert_eq!(v.window(10).1, 85);
        // New lines don't move the view while not following.
        v.append(vec!["line 100".into()]);
        assert_eq!(v.window(10).1, 85);
        // Scrolling to the end re-engages follow.
        v.scroll_by(100, 10);
        assert!(v.follow);
        assert_eq!(v.window(10).0.last(), Some(&"line 100"));
    }

    #[test]
    fn filter_is_case_insensitive_and_applies_to_window() {
        let mut v = view_with(0);
        v.append(vec![
            "INFO fine".into(),
            "ERROR bad thing".into(),
            "WARN meh".into(),
            "error again".into(),
        ]);
        v.start_filter();
        for c in "ERR".chars() {
            v.filter_push(c);
        }
        v.filter_commit();
        assert_eq!(v.visible(), ["ERROR bad thing", "error again"]);
        assert_eq!(v.window(10).0.len(), 2);
        v.clear_filter();
        assert_eq!(v.visible().len(), 4);
    }

    #[test]
    fn buffer_is_bounded_and_scroll_tracks_the_slide() {
        let mut v = view_with(MAX_LINES);
        v.scroll_by(-10, 10);
        let before = v.scroll;
        v.append(vec!["x".into(); 3]);
        assert_eq!(v.total(), MAX_LINES);
        assert_eq!(v.scroll, before - 3);
    }

    #[test]
    fn error_first_token_becomes_filter() {
        assert_eq!(
            filter_from_error("java.lang.OutOfMemoryError: Java heap space\n\tat x").as_deref(),
            Some("java.lang.OutOfMemoryError")
        );
        assert_eq!(
            filter_from_error("ExecutorLostFailure (executor 2 exited)").as_deref(),
            Some("ExecutorLostFailure")
        );
        assert_eq!(filter_from_error("at x"), None);
    }

    #[test]
    fn severity_classification() {
        assert_eq!(
            severity("26/09/24 10:00:00 ERROR Executor: boom"),
            Severity::Error
        );
        assert_eq!(
            severity("26/09/24 10:00:00 WARN TaskSetManager: lost"),
            Severity::Warn
        );
        assert_eq!(
            severity("26/09/24 10:00:00 INFO Executor: ok"),
            Severity::Plain
        );
    }
}
