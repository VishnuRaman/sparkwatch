//! Structured Streaming progress, reconstructed from the driver log and the
//! micro-batch SQL executions, since the REST API does not expose it.

use crate::spark::ExecutionData;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

/// One `StreamingQueryProgress`, as logged by `ProgressReporter`.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Progress {
    pub id: String,
    pub run_id: String,
    pub name: Option<String>,
    /// `2026-09-25T10:00:00.000Z`
    pub timestamp: String,
    pub batch_id: i64,
    pub batch_duration: i64,
    pub num_input_rows: i64,
    pub input_rows_per_second: f64,
    pub processed_rows_per_second: f64,
    /// addBatch, getBatch, latestOffset, queryPlanning, triggerExecution, walCommit, commitOffsets
    pub duration_ms: BTreeMap<String, i64>,
    /// avg / max / min / watermark
    pub event_time: HashMap<String, String>,
    pub state_operators: Vec<StateOperator>,
    pub sources: Vec<SourceProgress>,
    pub sink: SinkProgress,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", default)]
pub struct StateOperator {
    pub operator_name: String,
    pub num_rows_total: i64,
    pub num_rows_updated: i64,
    pub memory_used_bytes: i64,
    pub num_rows_dropped_by_watermark: i64,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SourceProgress {
    pub description: String,
    pub num_input_rows: i64,
    pub input_rows_per_second: f64,
    pub processed_rows_per_second: f64,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SinkProgress {
    pub description: String,
    pub num_output_rows: i64,
}

impl Progress {
    pub fn watermark(&self) -> Option<&str> {
        self.event_time
            .get("watermark")
            .map(String::as_str)
            .filter(|w| !w.is_empty())
    }

    pub fn trigger_ms(&self) -> i64 {
        self.duration_ms
            .get("triggerExecution")
            .copied()
            .unwrap_or(self.batch_duration)
    }

    pub fn state_rows(&self) -> i64 {
        self.state_operators.iter().map(|s| s.num_rows_total).sum()
    }

    pub fn state_bytes(&self) -> i64 {
        self.state_operators
            .iter()
            .map(|s| s.memory_used_bytes)
            .sum()
    }

    /// `timestamp − watermark`, when both parse.
    pub fn watermark_lag_ms(&self) -> Option<i64> {
        Some(parse_iso_ms(&self.timestamp)? - parse_iso_ms(self.watermark()?)?)
    }
}

/// `2026-09-25T10:00:00.000Z` (or `.000GMT`, or no fraction) → epoch millis.
/// Hand-rolled so we don't pull a date crate in for one field.
pub fn parse_iso_ms(s: &str) -> Option<i64> {
    let s = s
        .trim()
        .trim_end_matches('Z')
        .trim_end_matches("GMT")
        .trim_end_matches("UTC");
    let (date, time) = s.split_once('T')?;
    // A numeric offset (`+02:00`, `+0000`, `-05:00`), as log4j's JSON
    // layout writes: strip it and shift the result back to UTC.
    let (time, offset_ms) = match time.rfind(['+', '-']) {
        Some(i) => {
            let (t, off) = time.split_at(i);
            let digits: String = off[1..].chars().filter(char::is_ascii_digit).collect();
            let (h, m) = (
                digits.get(0..2)?.parse::<i64>().ok()?,
                digits.get(2..4)?.parse::<i64>().ok()?,
            );
            let sign = if off.starts_with('-') { -1 } else { 1 };
            (t, sign * (h * 60 + m) * 60 * 1000)
        }
        None => (time, 0),
    };
    let mut d = date.split('-').map(|p| p.parse::<i64>());
    let (y, m, day) = (d.next()?.ok()?, d.next()?.ok()?, d.next()?.ok()?);
    let (hms, frac) = time.split_once('.').unwrap_or((time, "0"));
    let mut t = hms.split(':').map(|p| p.parse::<i64>());
    let (h, mi, sec) = (t.next()?.ok()?, t.next()?.ok()?, t.next()?.ok()?);
    let millis: i64 = format!("{:0<3}", frac.chars().take(3).collect::<String>())
        .parse()
        .ok()?;
    // Days from civil (Howard Hinnant).
    let (y, m) = if m <= 2 { (y - 1, m + 9) } else { (y, m - 3) };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * m + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe - 719468;
    Some(((days * 24 + h) * 60 + mi) * 60 * 1000 + sec * 1000 + millis - offset_ms)
}

/// Epoch millis → `2026-09-25T10:00:00Z` (the inverse of [`parse_iso_ms`],
/// to the second), for `kubectl logs --since-time`.
pub fn fmt_iso_s(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    // Civil from days (Howard Hinnant).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        sod / 3600,
        sod % 3600 / 60,
        sod % 60
    )
}

/// `HH:MM:SS` (UTC) of an epoch-millis instant, for titles.
pub fn fmt_clock(ms: i64) -> String {
    let sod = ms.div_euclid(1000).rem_euclid(86_400);
    format!("{:02}:{:02}:{:02}", sod / 3600, sod % 3600 / 60, sod % 60)
}

// ---------------------------------------------------------------- parsing

const MARKER: &str = "Streaming query made progress:";

/// A structured-logging line (`{"ts":…,"level":…,"msg":…}`) → its `msg`.
/// Cheap rejection first: nearly every line isn't one.
pub fn structured_msg(line: &str) -> Option<String> {
    let t = line.trim_start();
    if !t.starts_with('{') || !t.contains("\"msg\"") {
        return None;
    }
    let v: serde_json::Value = serde_json::from_str(t).ok()?;
    v.get("msg")?.as_str().map(str::to_string)
}

/// A structured-logging line's `ts` as epoch millis.
pub fn structured_ts_ms(line: &str) -> Option<i64> {
    let t = line.trim_start();
    if !t.starts_with('{') || !t.contains("\"ts\"") {
        return None;
    }
    let v: serde_json::Value = serde_json::from_str(t).ok()?;
    parse_iso_ms(v.get("ts")?.as_str()?)
}

/// Finds progress blocks in a stream of log lines. Spark pretty-prints the
/// JSON over many lines, so lines are buffered until the braces balance.
#[derive(Default)]
pub struct ProgressParser {
    buf: String,
    depth: i32,
    active: bool,
}

impl ProgressParser {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn feed(&mut self, line: &str) -> Option<Progress> {
        // Spark 4 structured logging (`log4j2-json-layout`): one JSON object
        // per line with the message, newlines and all, in `msg`. Unwrap it
        // and read the message as if it had been logged as text.
        if let Some(msg) = structured_msg(line) {
            if !msg.contains(MARKER) {
                return None;
            }
            let mut out = None;
            for l in msg.lines() {
                out = self.feed(l).or(out);
            }
            return out;
        }
        if let Some(i) = line.find(MARKER) {
            // A new block; whatever was buffered was truncated.
            self.buf.clear();
            self.depth = 0;
            self.active = true;
            return self.push(&line[i + MARKER.len()..]);
        }
        if self.active {
            return self.push(line);
        }
        None
    }

    fn push(&mut self, text: &str) -> Option<Progress> {
        for c in text.chars() {
            match c {
                '{' => self.depth += 1,
                '}' => self.depth -= 1,
                _ => {}
            }
        }
        self.buf.push_str(text);
        self.buf.push('\n');
        if self.depth > 0 || !self.buf.contains('{') {
            // Still inside the object (or the marker line ended before `{`).
            if self.buf.len() > 256 * 1024 {
                self.active = false; // runaway; give up on this block
            }
            return None;
        }
        self.active = false;
        let start = self.buf.find('{')?;
        let end = self.buf.rfind('}')?;
        let json = &self.buf[start..=end];
        let parsed = serde_json::from_str::<Progress>(json).ok();
        self.buf.clear();
        parsed
    }
}

/// A micro-batch's SQL execution description:
/// `"<name>\nid = <queryId>\nrunId = <runId>\nbatch = <n>"`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlBatch {
    pub query_id: String,
    pub run_id: String,
    pub batch_id: i64,
    pub name: Option<String>,
}

pub fn batch_from_sql(e: &ExecutionData) -> Option<SqlBatch> {
    parse_description(&e.description)
}

/// The same, from any job/stage/execution description.
pub fn parse_description(description: &str) -> Option<SqlBatch> {
    let mut query_id = None;
    let mut run_id = None;
    let mut batch_id = None;
    let mut name = None;
    for line in description.lines().map(str::trim) {
        if let Some(v) = line.strip_prefix("id = ") {
            query_id = Some(v.to_string());
        } else if let Some(v) = line.strip_prefix("runId = ") {
            run_id = Some(v.to_string());
        } else if let Some(v) = line.strip_prefix("batch = ") {
            batch_id = v.parse().ok();
        } else if !line.is_empty() && query_id.is_none() && !line.contains(" = ") {
            name = Some(line.to_string());
        }
    }
    Some(SqlBatch {
        query_id: query_id?,
        run_id: run_id?,
        batch_id: batch_id?,
        name,
    })
}

// ------------------------------------------------------------------ model

#[derive(Debug, Clone, Default, Serialize)]
pub struct Batch {
    pub batch_id: i64,
    pub duration_ms: i64,
    /// From the SQL execution: RUNNING | COMPLETED | FAILED; empty if only
    /// the log reported it.
    pub status: String,
    /// The SQL execution's submission time, when known.
    pub submitted: String,
    pub progress: Option<Progress>,
}

impl Batch {
    /// When the batch ran, with a second of slack either side, for slicing
    /// the driver log. From the progress event when there is one, else from
    /// the SQL execution.
    pub fn window_ms(&self) -> Option<(i64, i64)> {
        let (start, len) = match &self.progress {
            Some(p) => (
                parse_iso_ms(&p.timestamp)?,
                p.trigger_ms().max(self.duration_ms),
            ),
            None => (parse_iso_ms(&self.submitted)?, self.duration_ms),
        };
        Some((start - 1000, start + len.max(0) + 1000))
    }
}

/// Batches kept per query; a day of 5 s micro-batches would be 17k, so
/// this is a window, not a history.
pub const MAX_BATCHES: usize = 300;

#[derive(Debug, Clone, Default, Serialize)]
pub struct QueryHistory {
    pub query_id: String,
    pub run_id: String,
    pub name: Option<String>,
    pub batches: BTreeMap<i64, Batch>,
}

impl QueryHistory {
    pub fn label(&self) -> String {
        self.name
            .clone()
            .unwrap_or_else(|| self.query_id.chars().take(8).collect())
    }

    /// A restarted query gets a new run id; its old batches are history.
    fn on_run(&mut self, run_id: &str) {
        if self.run_id != run_id {
            if !self.run_id.is_empty() {
                self.batches.clear();
            }
            self.run_id = run_id.to_string();
        }
    }

    fn trim(&mut self) {
        while self.batches.len() > MAX_BATCHES {
            self.batches.pop_first();
        }
    }
}

impl Streaming {
    /// When the oldest batch still listed for any live query ran: where the
    /// log tap should start so every listed batch gets its progress.
    pub fn oldest_batch_ms(&self) -> Option<i64> {
        self.queries
            .values()
            .filter(|q| !self.retired.contains(&q.query_id))
            .filter_map(|q| {
                let b = q.batches.values().next()?;
                b.progress
                    .as_ref()
                    .and_then(|p| parse_iso_ms(&p.timestamp))
                    .or_else(|| parse_iso_ms(&b.submitted))
            })
            .min()
    }
}

#[derive(Default)]
pub struct Streaming {
    pub queries: BTreeMap<String, QueryHistory>,
    /// Query name → the query id currently carrying it.
    current: HashMap<String, String>,
    /// Query ids superseded by a restart; their (cached) data is stale.
    retired: std::collections::HashSet<String>,
    /// How many progress events the log tap has delivered.
    pub progress_events: usize,
    /// Batches seen only through SQL executions (no log tap).
    pub sql_batches: usize,
}

impl Streaming {
    pub fn is_empty(&self) -> bool {
        self.queries.is_empty()
    }

    /// Queries in display order (by name), the order the tab's list and
    /// its selection index use.
    pub fn sorted(&self) -> Vec<&QueryHistory> {
        let mut v: Vec<&QueryHistory> = self.queries.values().collect();
        v.sort_by_key(|q| q.label());
        v
    }

    /// A query restarted from a fresh checkpoint gets a new query id but
    /// keeps its name, and Spark allows only one active query per name, so
    /// a same-named query under a different id is the same logical query.
    /// Returns whether data for `id` should be ingested: the current id
    /// always; a never-seen id takes over (batches start again, as with a
    /// new run); an id that was superseded is stale and is ignored — the SQL
    /// cache and the HTTP log tail keep re-presenting old incarnations.
    fn admit(&mut self, id: &str, name: Option<&str>) -> bool {
        let Some(name) = name.filter(|n| !n.is_empty()) else {
            return !self.retired.contains(id);
        };
        match self.current.get(name) {
            Some(cur) if cur == id => true,
            _ if self.retired.contains(id) => false,
            Some(cur) => {
                let old = cur.clone();
                self.retired.insert(old.clone());
                if let Some(mut q) = self.queries.remove(&old) {
                    q.query_id = id.to_string();
                    q.run_id.clear();
                    q.batches.clear();
                    self.queries.insert(id.to_string(), q);
                }
                self.current.insert(name.to_string(), id.to_string());
                true
            }
            None => {
                self.current.insert(name.to_string(), id.to_string());
                true
            }
        }
    }

    /// Returns false for a duplicate (same run and batch already known
    /// with progress), which the HTTP re-fetch produces constantly.
    pub fn ingest_progress(&mut self, p: Progress) -> bool {
        if !self.admit(&p.id, p.name.as_deref()) {
            return false;
        }
        let q = self
            .queries
            .entry(p.id.clone())
            .or_insert_with(|| QueryHistory {
                query_id: p.id.clone(),
                ..Default::default()
            });
        q.on_run(&p.run_id);
        if p.name.is_some() {
            q.name = p.name.clone();
        }
        let b = q.batches.entry(p.batch_id).or_insert_with(|| Batch {
            batch_id: p.batch_id,
            ..Default::default()
        });
        if b.progress.is_some() {
            return false;
        }
        b.duration_ms = p.trigger_ms();
        b.progress = Some(p);
        q.trim();
        self.progress_events += 1;
        true
    }

    pub fn ingest_sql(&mut self, execs: &[ExecutionData]) {
        // Oldest first, so the newest incarnation of a restarted query is the
        // one that ends up current.
        let mut ordered: Vec<&ExecutionData> = execs.iter().collect();
        ordered.sort_by_key(|e| e.id);
        for e in ordered {
            let Some(sb) = batch_from_sql(e) else {
                continue;
            };
            if !self.admit(&sb.query_id, sb.name.as_deref()) {
                continue;
            }
            let q = self
                .queries
                .entry(sb.query_id.clone())
                .or_insert_with(|| QueryHistory {
                    query_id: sb.query_id.clone(),
                    ..Default::default()
                });
            q.on_run(&sb.run_id);
            if q.name.is_none() {
                q.name = sb.name;
            }
            let b = q.batches.entry(sb.batch_id).or_insert_with(|| {
                self.sql_batches += 1;
                Batch {
                    batch_id: sb.batch_id,
                    ..Default::default()
                }
            });
            b.status = e.status.clone();
            if !e.submission_time.is_empty() {
                b.submitted = e.submission_time.clone();
            }
            if b.progress.is_none() {
                b.duration_ms = e.duration;
            }
            q.trim();
        }
    }
}

/// Everything the tab shows for one query, computed from its window.
pub struct QueryStats<'a> {
    pub latest: Option<&'a Batch>,
    pub mean_ms: i64,
    pub p95_ms: i64,
    pub max_ms: i64,
    pub input_rps: f64,
    pub processed_rps: f64,
    /// Processing slower than input for most of the recent batches.
    pub behind: bool,
    pub state_rows: i64,
    pub state_bytes: i64,
    pub watermark_lag_ms: Option<i64>,
    pub batches_per_min: f64,
    /// Sparkline series, oldest first.
    pub durations: Vec<u64>,
    pub input_series: Vec<u64>,
    pub processed_series: Vec<u64>,
    pub state_series: Vec<u64>,
    /// `addBatch` ms per batch: the actual work.
    pub add_batch_series: Vec<u64>,
    /// Everything else in `durationMs` (planning, offsets, commits): overhead.
    pub overhead_series: Vec<u64>,
    /// Watermark lag ms per batch (0 when unknown).
    pub lag_series: Vec<u64>,
    /// Rows dropped by the watermark per batch.
    pub dropped_series: Vec<u64>,
}

/// `behind` needs this many of the last `BEHIND_WINDOW` batches slow.
const BEHIND_WINDOW: usize = 5;
const BEHIND_MIN: usize = 3;

impl<'a> QueryStats<'a> {
    pub fn of(q: &'a QueryHistory) -> Self {
        let batches: Vec<&Batch> = q.batches.values().collect();
        let mut sorted: Vec<i64> = batches
            .iter()
            .map(|b| b.duration_ms)
            .filter(|d| *d > 0)
            .collect();
        sorted.sort_unstable();
        let pct = |p: f64| -> i64 {
            if sorted.is_empty() {
                0
            } else {
                sorted[(((sorted.len() - 1) as f64) * p).round() as usize]
            }
        };
        let mean_ms = if sorted.is_empty() {
            0
        } else {
            sorted.iter().sum::<i64>() / sorted.len() as i64
        };

        let with_progress: Vec<&Progress> =
            batches.iter().filter_map(|b| b.progress.as_ref()).collect();
        let latest_p = with_progress.last().copied();
        let recent: Vec<&&Progress> = with_progress
            .iter()
            .rev()
            .take(BEHIND_WINDOW)
            .filter(|p| p.num_input_rows > 0)
            .collect();
        let slow = recent
            .iter()
            .filter(|p| p.processed_rows_per_second < p.input_rows_per_second)
            .count();
        let behind = recent.len() >= BEHIND_MIN && slow >= BEHIND_MIN;

        // Batches per minute over the window, from progress timestamps.
        let times: Vec<i64> = with_progress
            .iter()
            .filter_map(|p| parse_iso_ms(&p.timestamp))
            .collect();
        let batches_per_min = match (times.first(), times.last()) {
            (Some(a), Some(b)) if b > a && times.len() > 1 => {
                (times.len() - 1) as f64 * 60_000.0 / (b - a) as f64
            }
            _ => 0.0,
        };

        let series = |f: &dyn Fn(&Progress) -> u64| -> Vec<u64> {
            with_progress.iter().map(|p| f(p)).collect()
        };
        QueryStats {
            latest: batches.last().copied(),
            mean_ms,
            p95_ms: pct(0.95),
            max_ms: sorted.last().copied().unwrap_or(0),
            input_rps: latest_p.map_or(0.0, |p| p.input_rows_per_second),
            processed_rps: latest_p.map_or(0.0, |p| p.processed_rows_per_second),
            behind,
            state_rows: latest_p.map_or(0, Progress::state_rows),
            state_bytes: latest_p.map_or(0, Progress::state_bytes),
            watermark_lag_ms: latest_p.and_then(Progress::watermark_lag_ms),
            batches_per_min,
            durations: batches
                .iter()
                .map(|b| b.duration_ms.max(0) as u64)
                .collect(),
            input_series: series(&|p| p.input_rows_per_second.max(0.0).round() as u64),
            processed_series: series(&|p| p.processed_rows_per_second.max(0.0).round() as u64),
            state_series: series(&|p| p.state_rows().max(0) as u64),
            add_batch_series: series(&|p| {
                p.duration_ms.get("addBatch").copied().unwrap_or(0).max(0) as u64
            }),
            overhead_series: series(&|p| {
                p.duration_ms
                    .iter()
                    .filter(|(k, _)| k.as_str() != "addBatch" && k.as_str() != "triggerExecution")
                    .map(|(_, v)| v.max(&0))
                    .sum::<i64>() as u64
            }),
            lag_series: series(&|p| p.watermark_lag_ms().unwrap_or(0).max(0) as u64),
            dropped_series: series(&|p| {
                p.state_operators
                    .iter()
                    .map(|o| o.num_rows_dropped_by_watermark.max(0))
                    .sum::<i64>() as u64
            }),
        }
    }
}

/// Keep the newest `n` entries of a series (sparklines draw from the left).
pub fn tail(series: &[u64], n: usize) -> Vec<u64> {
    series[series.len().saturating_sub(n)..].to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    const PRETTY: &str = r#"26/09/25 10:00:05 INFO MicroBatchExecution: Streaming query made progress: {
  "id" : "3b8f1e2c-0000-4000-8000-000000000001",
  "runId" : "9c0d2a11-0000-4000-8000-000000000002",
  "name" : "orders-agg",
  "timestamp" : "2026-09-25T10:00:05.000Z",
  "batchId" : 4123,
  "batchDuration" : 1200,
  "numInputRows" : 12400,
  "inputRowsPerSecond" : 10300.0,
  "processedRowsPerSecond" : 9000.0,
  "durationMs" : {
    "addBatch" : 900,
    "getBatch" : 10,
    "queryPlanning" : 30,
    "triggerExecution" : 1200,
    "walCommit" : 15
  },
  "eventTime" : {
    "watermark" : "2026-09-25T09:56:53.000Z"
  },
  "stateOperators" : [ {
    "operatorName" : "stateStoreSave",
    "numRowsTotal" : 1500000,
    "numRowsUpdated" : 12000,
    "memoryUsedBytes" : 209715200
  } ],
  "sources" : [ {
    "description" : "KafkaV2[Subscribe[orders]]",
    "numInputRows" : 12400,
    "inputRowsPerSecond" : 10300.0,
    "processedRowsPerSecond" : 9000.0
  } ],
  "sink" : {
    "description" : "DeltaSink[s3://bucket/orders_agg]",
    "numOutputRows" : 3100
  }
}"#;

    #[test]
    fn parses_pretty_printed_block_across_lines() {
        let mut p = ProgressParser::new();
        let mut got = None;
        for line in PRETTY.lines() {
            if let Some(pr) = p.feed(line) {
                got = Some(pr);
            }
        }
        let pr = got.expect("parsed");
        assert_eq!(pr.batch_id, 4123);
        assert_eq!(pr.name.as_deref(), Some("orders-agg"));
        assert_eq!(pr.trigger_ms(), 1200);
        assert_eq!(pr.state_rows(), 1_500_000);
        assert_eq!(pr.watermark_lag_ms(), Some(192_000));
        assert_eq!(pr.sources[0].description, "KafkaV2[Subscribe[orders]]");
    }

    #[test]
    fn parses_single_line_and_ignores_noise_between() {
        let mut p = ProgressParser::new();
        assert!(p.feed("26/09/25 INFO Executor: unrelated").is_none());
        let one = r#"INFO MicroBatchExecution: Streaming query made progress: {"id":"q","runId":"r","batchId":7,"batchDuration":500,"durationMs":{"triggerExecution":480}}"#;
        let pr = p.feed(one).expect("single line");
        assert_eq!(pr.batch_id, 7);
        assert_eq!(pr.trigger_ms(), 480);
        assert!(
            p.feed("}").is_none(),
            "stray brace after a finished block is ignored"
        );
    }

    #[test]
    fn iso_timestamps() {
        assert_eq!(parse_iso_ms("1970-01-01T00:00:00.000Z"), Some(0));
        assert_eq!(
            parse_iso_ms("2026-09-25T10:00:05.000Z"),
            Some(1_790_330_405_000)
        );
        assert_eq!(
            parse_iso_ms("2026-09-25T10:00:05.5GMT"),
            Some(1_790_330_405_500)
        );
        assert_eq!(parse_iso_ms("nope"), None);
    }

    #[test]
    fn iso_offsets_shift_to_utc() {
        let utc = parse_iso_ms("2026-09-25T10:00:05.000Z").unwrap();
        assert_eq!(parse_iso_ms("2026-09-25T10:00:05.000+0000"), Some(utc));
        assert_eq!(parse_iso_ms("2026-09-25T12:00:05.000+02:00"), Some(utc));
        assert_eq!(parse_iso_ms("2026-09-25T05:00:05-05:00"), Some(utc));
    }

    #[test]
    fn structured_log_lines_are_unwrapped() {
        let progress = r#"{\n  \"id\" : \"q1\",\n  \"runId\" : \"r1\",\n  \"name\" : \"orders\",\n  \"timestamp\" : \"2026-09-25T10:00:05.000Z\",\n  \"batchId\" : 7,\n  \"numInputRows\" : 20,\n  \"inputRowsPerSecond\" : 2.0,\n  \"processedRowsPerSecond\" : 4.0,\n  \"durationMs\" : { \"addBatch\" : 300, \"triggerExecution\" : 400 },\n  \"stateOperators\" : [ ],\n  \"sources\" : [ ],\n  \"sink\" : { \"description\" : \"x\", \"numOutputRows\" : 20 }\n}"#;
        let line = format!(
            r#"{{"ts":"2026-09-25T10:00:05.123+0000","level":"INFO","msg":"Streaming query made progress: {progress}","logger":"MicroBatchExecution"}}"#
        );
        let mut p = ProgressParser::new();
        assert!(p.feed(r#"{"ts":"2026-09-25T10:00:04.000Z","level":"INFO","msg":"Committed offsets for batch 7","logger":"MicroBatchExecution"}"#).is_none());
        let got = p
            .feed(&line)
            .expect("progress parsed from a structured line");
        assert_eq!(got.batch_id, 7);
        assert_eq!(got.num_input_rows, 20);
        assert_eq!(
            structured_ts_ms(&line),
            parse_iso_ms("2026-09-25T10:00:05.123Z")
        );
        assert_eq!(structured_msg("26/10/01 18:03:15 INFO plain line"), None);
    }

    #[test]
    fn iso_round_trips() {
        for s in [
            "1970-01-01T00:00:00Z",
            "2026-09-25T10:00:05Z",
            "2026-02-28T23:59:59Z",
            "2024-02-29T12:00:00Z",
            "2026-12-31T00:00:00Z",
        ] {
            assert_eq!(fmt_iso_s(parse_iso_ms(s).unwrap()), s);
        }
        assert_eq!(fmt_iso_s(1_790_330_405_900), "2026-09-25T10:00:05Z");
        assert_eq!(fmt_clock(1_790_330_405_900), "10:00:05");
    }

    #[test]
    fn sql_description_yields_batch() {
        let e = ExecutionData {
            description: "orders-agg\nid = q1\nrunId = r1\nbatch = 42".into(),
            ..Default::default()
        };
        assert_eq!(
            batch_from_sql(&e),
            Some(SqlBatch {
                query_id: "q1".into(),
                run_id: "r1".into(),
                batch_id: 42,
                name: Some("orders-agg".into())
            })
        );
        assert!(
            batch_from_sql(&ExecutionData {
                description: "count at Main.scala:40".into(),
                ..Default::default()
            })
            .is_none()
        );
    }

    fn progress(batch: i64, input: f64, processed: f64) -> Progress {
        Progress {
            id: "q".into(),
            run_id: "r".into(),
            batch_id: batch,
            num_input_rows: 100,
            input_rows_per_second: input,
            processed_rows_per_second: processed,
            timestamp: format!("2026-09-25T10:00:{:02}.000Z", batch % 60),
            duration_ms: BTreeMap::from([("triggerExecution".to_string(), 1000 + batch * 10)]),
            ..Default::default()
        }
    }

    #[test]
    fn dedups_and_detects_falling_behind() {
        let mut s = Streaming::default();
        for b in 0..10 {
            assert!(s.ingest_progress(progress(b, 100.0, 120.0)));
        }
        assert!(!s.ingest_progress(progress(9, 100.0, 120.0)), "duplicate");
        let q = &s.queries["q"];
        assert!(!QueryStats::of(q).behind);

        for b in 10..14 {
            s.ingest_progress(progress(b, 100.0, 80.0));
        }
        let st = QueryStats::of(&s.queries["q"]);
        assert!(st.behind);
        assert_eq!(st.durations.len(), 14);
        assert_eq!(st.max_ms, 1130);
        assert!(st.batches_per_min > 0.0);
    }

    #[test]
    fn sql_batches_merge_with_progress_and_restart_resets() {
        let mut s = Streaming::default();
        let exec = |batch: i64, ms: i64| ExecutionData {
            description: format!("agg\nid = q\nrunId = r\nbatch = {batch}"),
            status: "COMPLETED".into(),
            duration: ms,
            ..Default::default()
        };
        s.ingest_sql(&[exec(1, 900), exec(2, 950)]);
        assert_eq!(s.queries["q"].batches.len(), 2);
        assert_eq!(s.queries["q"].label(), "agg");
        // Progress for batch 2 upgrades it; the SQL duration is replaced.
        s.ingest_progress(progress(2, 10.0, 10.0));
        let b2 = &s.queries["q"].batches[&2];
        assert!(b2.progress.is_some());
        assert_eq!(b2.duration_ms, 1020);
        assert_eq!(b2.status, "COMPLETED");
        // New run id: old batches gone.
        let mut p = progress(3, 10.0, 10.0);
        p.run_id = "r2".into();
        s.ingest_progress(p);
        assert_eq!(s.queries["q"].batches.len(), 1);
    }
}

#[cfg(test)]
mod restart_tests {
    use super::*;

    fn p(id: &str, run: &str, name: &str, batch: i64) -> Progress {
        Progress {
            id: id.into(),
            run_id: run.into(),
            name: Some(name.into()),
            batch_id: batch,
            ..Default::default()
        }
    }

    #[test]
    fn stale_data_for_a_superseded_id_is_ignored() {
        let mut s = Streaming::default();
        s.ingest_progress(p("q1", "r1", "orders-poison", 4));
        s.ingest_progress(p("q2", "r2", "orders-poison", 0));
        // The HTTP tail / SQL cache re-present the old incarnation every poll.
        assert!(!s.ingest_progress(p("q1", "r1", "orders-poison", 4)));
        assert_eq!(s.queries.len(), 1);
        assert!(s.queries.contains_key("q2"));
        assert_eq!(
            s.queries["q2"].batches.len(),
            1,
            "q2's batches survive the stale replay"
        );

        // SQL executions arrive newest-first; the newest id must still win.
        let exec = |id: i64, qid: &str, batch: i64| ExecutionData {
            id,
            description: format!("orders-poison\nid = {qid}\nrunId = r\nbatch = {batch}"),
            status: "COMPLETED".into(),
            ..Default::default()
        };
        let mut s2 = Streaming::default();
        s2.ingest_sql(&[exec(30, "new", 1), exec(29, "new", 0), exec(12, "old", 4)]);
        assert_eq!(s2.queries.len(), 1);
        assert!(s2.queries.contains_key("new"));
        assert_eq!(
            s2.queries["new"]
                .batches
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            [0, 1]
        );
        // and the same list again changes nothing
        s2.ingest_sql(&[exec(30, "new", 1), exec(29, "new", 0), exec(12, "old", 4)]);
        assert_eq!(s2.queries["new"].batches.len(), 2);
    }

    #[test]
    fn same_name_new_id_is_one_query_not_two() {
        let mut s = Streaming::default();
        s.ingest_progress(p("q1", "r1", "orders-poison", 4));
        s.ingest_progress(p("q2", "r2", "orders-poison", 0)); // fresh checkpoint restart
        s.ingest_progress(p("q2", "r2", "orders-poison", 1));
        assert_eq!(s.queries.len(), 1, "one logical query");
        let q = &s.queries["q2"];
        assert_eq!(q.batches.keys().copied().collect::<Vec<_>>(), [0, 1]);
        // Differently named queries stay separate.
        s.ingest_progress(p("q3", "r3", "orders-raw", 9));
        assert_eq!(s.queries.len(), 2);
    }
}
