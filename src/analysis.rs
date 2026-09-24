//! Pure functions that turn stage detail into "what is wrong with this
//! stage": skewed metrics, straggler tasks, and misbehaving executors.
//!
//! Skew is judged from the quantile distributions Spark computes server-side,
//! so it is right even for a stage with 100k tasks of which we only fetched
//! the slowest hundred.

use crate::spark::{ExecutorStageSummary, StageDetail, TaskData, TaskMetricDistributions};

/// A task whose value is this many times the median counts as skewed.
pub const SKEW_RATIO: f64 = 3.0;
/// An executor whose mean task time is this many times the stage mean is slow.
pub const SLOW_EXECUTOR_RATIO: f64 = 2.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unit {
    Millis,
    Bytes,
}

/// One row of the distribution table.
#[derive(Debug, Clone)]
pub struct MetricRow {
    pub name: &'static str,
    pub unit: Unit,
    /// Values at each requested quantile; last is the max (quantile 1.0).
    pub values: Vec<f64>,
    /// `max / median`, or `None` when the metric is too small to matter.
    pub skew: Option<f64>,
}

impl MetricRow {
    pub fn median(&self) -> f64 {
        // Quantiles are 0.05, 0.25, 0.5, 0.75, 0.95, 1.0 → index 2 is p50.
        self.values.get(2).copied().unwrap_or(0.0)
    }

    pub fn max(&self) -> f64 {
        self.values.last().copied().unwrap_or(0.0)
    }

    pub fn is_skewed(&self) -> bool {
        self.skew.is_some_and(|r| r >= SKEW_RATIO)
    }
}

/// Below these the metric is noise, not skew (a 5 ms task next to a 20 ms
/// one is not a problem worth flagging).
fn floor(unit: Unit) -> f64 {
    match unit {
        Unit::Millis => 1_000.0,
        Unit::Bytes => 1024.0 * 1024.0,
    }
}

fn row(name: &'static str, unit: Unit, values: &[f64]) -> Option<MetricRow> {
    if values.is_empty() {
        return None;
    }
    let mut r = MetricRow {
        name,
        unit,
        values: values.to_vec(),
        skew: None,
    };
    let (median, max) = (r.median(), r.max());
    if max >= floor(unit) {
        // A zero median with a real max is the worst skew there is.
        r.skew = Some(if median > 0.0 { max / median } else { f64::INFINITY });
    }
    Some(r)
}

/// Distribution rows in display order; metrics Spark did not report are omitted.
pub fn metric_rows(d: &TaskMetricDistributions) -> Vec<MetricRow> {
    [
        row("duration", Unit::Millis, &d.duration),
        row("gc time", Unit::Millis, &d.jvm_gc_time),
        row("sched delay", Unit::Millis, &d.scheduler_delay),
        row("input", Unit::Bytes, &d.input_metrics.bytes_read),
        row("shuffle read", Unit::Bytes, &d.shuffle_read_metrics.read_bytes),
        row("fetch wait", Unit::Millis, &d.shuffle_read_metrics.fetch_wait_time),
        row("shuffle write", Unit::Bytes, &d.shuffle_write_metrics.write_bytes),
        row("mem spill", Unit::Bytes, &d.memory_bytes_spilled),
        row("disk spill", Unit::Bytes, &d.disk_bytes_spilled),
        row("peak exec mem", Unit::Bytes, &d.peak_execution_memory),
    ]
    .into_iter()
    .flatten()
    .collect()
}

/// Median task duration, from the distribution when Spark gave us one,
/// otherwise from the tasks we have.
pub fn median_duration(detail: &StageDetail) -> f64 {
    if let Some(d) = &detail.summary {
        if let Some(m) = d.duration.get(2) {
            return *m;
        }
    }
    let mut ds: Vec<i64> = detail
        .slowest
        .iter()
        .filter(|t| t.status == "SUCCESS")
        .map(TaskData::duration_ms)
        .collect();
    if ds.is_empty() {
        return 0.0;
    }
    ds.sort_unstable();
    ds[ds.len() / 2] as f64
}

/// A task is a straggler when it ran `SKEW_RATIO`× longer than the median.
pub fn is_straggler(task: &TaskData, median_ms: f64) -> bool {
    median_ms >= floor(Unit::Millis) / 10.0
        && task.duration_ms() as f64 >= SKEW_RATIO * median_ms
}

/// Per-executor view of a stage, worst first.
#[derive(Debug, Clone)]
pub struct ExecutorRow {
    pub id: String,
    pub summary: ExecutorStageSummary,
    pub mean_task_ms: f64,
    pub flagged: bool,
    /// Why it is flagged, for the table ("2 failed · 3.1× stage avg").
    pub reason: String,
}

pub fn executor_rows(detail: &StageDetail) -> Vec<ExecutorRow> {
    let execs = &detail.stage.executor_summary;
    let total_tasks: i64 = execs.values().map(ExecutorStageSummary::tasks).sum();
    let total_time: i64 = execs.values().map(|e| e.task_time).sum();
    let stage_mean = if total_tasks > 0 {
        total_time as f64 / total_tasks as f64
    } else {
        0.0
    };

    let mut rows: Vec<ExecutorRow> = execs
        .iter()
        .map(|(id, e)| {
            let mean = if e.tasks() > 0 {
                e.task_time as f64 / e.tasks() as f64
            } else {
                0.0
            };
            let slow = stage_mean >= floor(Unit::Millis) / 10.0 && mean >= SLOW_EXECUTOR_RATIO * stage_mean;
            let mut reasons = Vec::new();
            if e.failed_tasks > 0 {
                reasons.push(format!("{} failed", e.failed_tasks));
            }
            if e.excluded() {
                reasons.push("excluded".to_string());
            }
            if slow {
                reasons.push(format!("{:.1}× stage avg", mean / stage_mean));
            }
            ExecutorRow {
                id: id.clone(),
                summary: e.clone(),
                mean_task_ms: mean,
                flagged: !reasons.is_empty(),
                reason: reasons.join(" · "),
            }
        })
        .collect();
    // Flagged first, then by time spent, so the culprit is at the top.
    rows.sort_by(|a, b| {
        b.flagged
            .cmp(&a.flagged)
            .then(b.summary.task_time.cmp(&a.summary.task_time))
            .then(a.id.cmp(&b.id))
    });
    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spark::{StageData, TaskMetricDistributions};
    use std::collections::HashMap;

    fn dist(duration: &[f64], read: &[f64]) -> TaskMetricDistributions {
        let mut d = TaskMetricDistributions {
            quantiles: vec![0.05, 0.25, 0.5, 0.75, 0.95, 1.0],
            duration: duration.to_vec(),
            ..Default::default()
        };
        d.shuffle_read_metrics.read_bytes = read.to_vec();
        d
    }

    #[test]
    fn flags_skew_from_quantiles() {
        let mib = 1024.0 * 1024.0;
        let d = dist(
            &[1000.0, 2500.0, 8000.0, 9500.0, 30000.0, 91000.0],
            &[mib, 2.0 * mib, 4.0 * mib, 8.0 * mib, 32.0 * mib, 1025.0 * mib],
        );
        let rows = metric_rows(&d);
        let dur = rows.iter().find(|r| r.name == "duration").unwrap();
        assert!(dur.is_skewed());
        assert!((dur.skew.unwrap() - 91000.0 / 8000.0).abs() < 1e-9);
        let read = rows.iter().find(|r| r.name == "shuffle read").unwrap();
        assert!(read.is_skewed());
        // Metrics Spark did not report are simply absent.
        assert!(rows.iter().all(|r| r.name != "mem spill"));
    }

    #[test]
    fn tiny_values_are_not_skew() {
        let d = dist(&[5.0, 8.0, 10.0, 12.0, 20.0, 40.0], &[]);
        let dur = &metric_rows(&d)[0];
        assert!(dur.skew.is_none());
        assert!(!dur.is_skewed());
    }

    #[test]
    fn zero_median_with_real_max_is_infinite_skew() {
        let mut d = dist(&[], &[]);
        d.memory_bytes_spilled = vec![0.0, 0.0, 0.0, 0.0, 0.0, 512.0 * 1024.0 * 1024.0];
        let spill = metric_rows(&d).into_iter().find(|r| r.name == "mem spill").unwrap();
        assert!(spill.is_skewed());
        assert!(spill.skew.unwrap().is_infinite());
    }

    #[test]
    fn stragglers_use_median_from_distribution() {
        let detail = StageDetail {
            summary: Some(dist(&[1000.0, 2500.0, 8000.0, 9500.0, 30000.0, 91000.0], &[])),
            ..Default::default()
        };
        let m = median_duration(&detail);
        assert_eq!(m, 8000.0);
        let slow = TaskData {
            duration: Some(91_000),
            ..Default::default()
        };
        let ok = TaskData {
            duration: Some(9_000),
            ..Default::default()
        };
        assert!(is_straggler(&slow, m));
        assert!(!is_straggler(&ok, m));
    }

    #[test]
    fn median_falls_back_to_task_list() {
        let task = |ms: i64| TaskData {
            duration: Some(ms),
            status: "SUCCESS".into(),
            ..Default::default()
        };
        let detail = StageDetail {
            slowest: vec![task(100), task(300), task(200)],
            ..Default::default()
        };
        assert_eq!(median_duration(&detail), 200.0);
    }

    #[test]
    fn executor_with_failures_or_slow_tasks_sorts_first() {
        let exec = |time: i64, ok: i64, failed: i64| ExecutorStageSummary {
            task_time: time,
            succeeded_tasks: ok,
            failed_tasks: failed,
            ..Default::default()
        };
        let mut execs = HashMap::new();
        execs.insert("1".to_string(), exec(100_000, 100, 0)); // 1 s/task
        execs.insert("2".to_string(), exec(50_000, 50, 0)); // 1 s/task
        execs.insert("3".to_string(), exec(30_000, 10, 0)); // 3 s/task: slow
        execs.insert("4".to_string(), exec(10_000, 8, 2)); // failures
        let detail = StageDetail {
            stage: StageData {
                executor_summary: execs,
                ..Default::default()
            },
            ..Default::default()
        };
        let rows = executor_rows(&detail);
        let ids: Vec<&str> = rows.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(ids, ["3", "4", "1", "2"]);
        assert!(rows[0].flagged && rows[1].flagged);
        assert!(!rows[2].flagged && !rows[3].flagged);
    }
}
