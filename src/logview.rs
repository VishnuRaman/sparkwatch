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
    /// Start the stream at this time (epoch ms) instead of at the tail —
    /// `kubectl logs --since-time`. A batch's window may be far behind the
    /// last 2000 lines.
    pub since_ms: Option<i64>,
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
    /// Only lines whose timestamp falls in `[start, end]` (epoch ms) are
    /// shown; lines without a timestamp follow the line before them.
    pub window: Option<(i64, i64)>,
    /// The stream has delivered a line dated after the window's end.
    pub past_window: bool,
    /// Timestamp of the first dated line the source delivered, kept or not:
    /// when a window drops everything, this says where the log starts.
    pub first_seen_ms: Option<i64>,
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

    pub fn open_window(
        &mut self,
        target: LogTarget,
        filter: Option<String>,
        window: Option<(i64, i64)>,
    ) {
        self.open(target, filter);
        self.window = window;
        // A window is a slice of the past: start at its beginning, not the tail.
        if window.is_some() {
            self.follow = false;
            self.scroll = 0;
        }
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
            if self.first_seen_ms.is_none() {
                self.first_seen_ms = line_time_ms(&l);
            }
            // A window is a slice of the past: once the stream is past its
            // end, stop, so a chatty driver can't push the slice out of the
            // buffer (`c` re-opens the whole log).
            if let Some((_, end)) = self.window {
                if self.past_window {
                    continue;
                }
                if line_time_ms(&l).is_some_and(|t| t > end) {
                    self.past_window = true;
                    continue;
                }
            }
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

    /// Timestamps of the oldest and newest dated lines in the buffer, to
    /// explain an empty window ("the log we have starts after the batch").
    pub fn time_span(&self) -> Option<(i64, i64)> {
        let first = self.lines.iter().find_map(|l| line_time_ms(l));
        let last = self.lines.iter().rev().find_map(|l| line_time_ms(l));
        match (first, last) {
            (Some(f), Some(l)) => Some((f, l)),
            // Everything delivered was outside the window and dropped; the
            // first line's time is still the answer to "where does it start".
            _ => self.first_seen_ms.map(|t| (t, t)),
        }
    }

    /// Lines that pass the time window and the text filter, in order.
    pub fn visible(&self) -> Vec<&str> {
        let mut in_window = self.window.is_none();
        self.lines
            .iter()
            .filter(|l| {
                if let Some((start, end)) = self.window
                    && let Some(t) = line_time_ms(l)
                {
                    in_window = (start..=end).contains(&t);
                }
                in_window
            })
            .filter(|l| {
                self.filter
                    .as_ref()
                    .is_none_or(|f| l.to_lowercase().contains(f.as_str()))
            })
            .map(String::as_str)
            .collect()
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
        self.window = None;
        self.follow = true;
    }
}

/// The timestamp at the start of a Spark log line, as epoch millis (UTC).
/// Spark's default log4j pattern is `yy/MM/dd HH:mm:ss`; ISO-8601
/// (`yyyy-MM-dd HH:mm:ss,SSS` / `T`) is the other common one.
pub fn line_time_ms(line: &str) -> Option<i64> {
    if line.starts_with('{') {
        return crate::streaming::structured_ts_ms(line);
    }
    let b = line.as_bytes();
    let digits =
        |r: std::ops::Range<usize>| b.get(r).is_some_and(|x| x.iter().all(u8::is_ascii_digit));
    if b.len() >= 17
        && digits(0..2)
        && b[2] == b'/'
        && digits(3..5)
        && b[5] == b'/'
        && digits(6..8)
        && b[8] == b' '
    {
        let iso = format!(
            "20{}-{}-{}T{}",
            &line[0..2],
            &line[3..5],
            &line[6..8],
            &line[9..17]
        );
        return crate::streaming::parse_iso_ms(&iso);
    }
    if b.len() >= 19
        && digits(0..4)
        && b[4] == b'-'
        && digits(5..7)
        && b[7] == b'-'
        && digits(8..10)
        && (b[10] == b' ' || b[10] == b'T')
    {
        let iso = format!("{}T{}", &line[0..10], &line[11..19]);
        return crate::streaming::parse_iso_ms(&iso);
    }
    None
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
                since_ms: None,
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
    fn window_stops_ingesting_past_its_end_so_the_slice_survives() {
        let mut v = view_with(0);
        let t = |s: &str| line_time_ms(s).unwrap();
        v.window = Some((t("26/10/01 18:03:14 x"), t("26/10/01 18:03:17 x")));
        v.append(vec![
            "26/10/01 18:03:15 INFO in window".into(),
            "  continuation".into(),
            "26/10/01 18:03:30 INFO after".into(),
            "  continuation of after".into(),
        ]);
        v.append(vec!["x".into(); MAX_LINES]);
        assert!(v.past_window);
        assert_eq!(v.total(), 2);
        assert_eq!(v.received, 4 + MAX_LINES);
        assert_eq!(
            v.time_span(),
            Some((t("26/10/01 18:03:15 x"), t("26/10/01 18:03:15 x")))
        );
    }

    #[test]
    fn window_entirely_before_the_log_still_knows_where_the_log_starts() {
        let mut v = view_with(0);
        let t = |s: &str| line_time_ms(s).unwrap();
        v.window = Some((t("26/10/01 18:03:14 x"), t("26/10/01 18:03:17 x")));
        v.append(vec![
            "26/10/01 18:10:00 INFO rotated log starts here".into(),
            "26/10/01 18:10:01 INFO more".into(),
        ]);
        assert_eq!(v.total(), 0);
        assert!(v.received > 0);
        assert_eq!(
            v.time_span(),
            Some((t("26/10/01 18:10:00 x"), t("26/10/01 18:10:00 x")))
        );
    }

    #[test]
    fn time_window_keeps_lines_in_range_and_their_continuations() {
        let mut v = view_with(0);
        v.append(vec![
            "26/10/01 18:03:13 INFO before".into(),
            "26/10/01 18:03:15 INFO Starting batch 4".into(),
            "  continuation of the line above".into(),
            "26/10/01 18:03:16 ERROR Exception in task 3.0".into(),
            "\tat org.apache.spark.x(A.scala:1)".into(),
            "26/10/01 18:03:20 INFO after".into(),
        ]);
        let t = |s: &str| line_time_ms(s).unwrap();
        v.window = Some((t("26/10/01 18:03:14 x"), t("26/10/01 18:03:17 x")));
        v.follow = false;
        assert_eq!(
            v.visible(),
            [
                "26/10/01 18:03:15 INFO Starting batch 4",
                "  continuation of the line above",
                "26/10/01 18:03:16 ERROR Exception in task 3.0",
                "\tat org.apache.spark.x(A.scala:1)",
            ]
        );
        assert_eq!(
            line_time_ms("2026-10-01 18:03:15,123 INFO x"),
            line_time_ms("26/10/01 18:03:15 INFO x")
        );
        assert_eq!(
            line_time_ms("2026-10-01T18:03:15.000Z x"),
            line_time_ms("26/10/01 18:03:15 x")
        );
        assert_eq!(line_time_ms("no timestamp here"), None);
        assert_eq!(
            line_time_ms(r#"{"ts":"2026-10-01T18:03:15.000Z","level":"INFO","msg":"x"}"#),
            line_time_ms("26/10/01 18:03:15 INFO x")
        );
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
