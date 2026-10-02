//! Spark's metrics registry, as served by the driver's `MetricsServlet`
//! (`/metrics/json/`, on by default): the application's own sources —
//! whatever it registered with `SparkEnv.get.metricsSystem` — next to
//! Spark's built-ins (`DAGScheduler`, `LiveListenerBus`, `BlockManager`…).
//!
//! Keys are `<namespace>.driver.<source>.<name>`; the namespace is the app
//! id unless `spark.metrics.namespace` says otherwise, so everything before
//! `.driver.` is dropped. Executor-side registries never reach the driver
//! servlet (they only leave through a sink), so this is the driver's view.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::time::Instant;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum Kind {
    #[default]
    Gauge,
    Counter,
    Meter,
    Histogram,
    Timer,
}

impl Kind {
    pub fn label(&self) -> &'static str {
        match self {
            Kind::Gauge => "gauge",
            Kind::Counter => "counter",
            Kind::Meter => "meter",
            Kind::Histogram => "histogram",
            Kind::Timer => "timer",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Metric {
    /// The registry key with the namespace and `driver.` stripped.
    pub key: String,
    pub source: String,
    pub name: String,
    pub kind: Kind,
    /// A gauge's number, or a counter / meter / histogram / timer `count`.
    pub value: Option<f64>,
    /// A gauge whose value isn't a number.
    pub text: Option<String>,
    /// The other numeric fields Dropwizard reports (rates, quantiles).
    pub fields: Vec<(String, f64)>,
    /// A timer's `duration_units`.
    pub units: Option<String>,
}

impl Metric {
    pub fn field(&self, name: &str) -> Option<f64> {
        self.fields.iter().find(|(k, _)| k == name).map(|(_, v)| *v)
    }
}

/// Spark's own sources, in the order they're worth reading. Anything else
/// is the application's and is listed first.
const BUILTIN: [&str; 12] = [
    "LiveListenerBus",
    "DAGScheduler",
    "BlockManager",
    "CodeGenerator",
    "JVMCPUTime",
    "ExecutorMetrics",
    "ExecutorAllocationManager",
    "HiveExternalCatalog",
    "appStatus",
    "jvm",
    "spark.streaming",
    "AccumulatorSource",
];

pub fn is_builtin(source: &str) -> bool {
    BUILTIN.contains(&source)
}

fn source_rank(source: &str) -> usize {
    BUILTIN
        .iter()
        .position(|b| *b == source)
        .unwrap_or(usize::MAX)
}

/// `/metrics/json/` → the driver's metrics, application sources first.
/// Keys without `.driver.` (a History Server's own registry, executor
/// namespaces) are left out.
pub fn parse(body: &str) -> Result<Vec<Metric>, serde_json::Error> {
    let root: Value = serde_json::from_str(body)?;
    let mut out = Vec::new();
    for (section, kind) in [
        ("gauges", Kind::Gauge),
        ("counters", Kind::Counter),
        ("meters", Kind::Meter),
        ("histograms", Kind::Histogram),
        ("timers", Kind::Timer),
    ] {
        let Some(map) = root.get(section).and_then(Value::as_object) else {
            continue;
        };
        for (full, v) in map {
            let Some((_, rest)) = full.split_once(".driver.") else {
                continue;
            };
            // `spark.streaming.<query>.<metric>` keeps its three-part prefix
            // as the source so one query's metrics stay together.
            let (source, name) = if let Some(r) = rest.strip_prefix("spark.streaming.") {
                match r.split_once('.') {
                    Some((q, n)) => (format!("spark.streaming.{q}"), n.to_string()),
                    None => ("spark.streaming".into(), r.to_string()),
                }
            } else {
                match rest.split_once('.') {
                    Some((s, n)) => (s.to_string(), n.to_string()),
                    None => (rest.to_string(), String::new()),
                }
            };
            let obj = v.as_object();
            let mut m = Metric {
                key: rest.to_string(),
                source,
                name,
                kind,
                ..Default::default()
            };
            match kind {
                Kind::Gauge => match obj.and_then(|o| o.get("value")) {
                    Some(Value::Number(n)) => m.value = n.as_f64(),
                    Some(Value::Bool(b)) => m.value = Some(if *b { 1.0 } else { 0.0 }),
                    Some(Value::Null) | None => {}
                    Some(other) => {
                        m.text = Some(
                            other
                                .as_str()
                                .map(str::to_string)
                                .unwrap_or_else(|| other.to_string()),
                        )
                    }
                },
                _ => {
                    if let Some(o) = obj {
                        m.value = o.get("count").and_then(Value::as_f64);
                        for (k, fv) in o {
                            if k == "count" {
                                continue;
                            }
                            match fv {
                                Value::Number(n) => {
                                    if let Some(x) = n.as_f64() {
                                        m.fields.push((k.clone(), x));
                                    }
                                }
                                Value::String(s) if k == "duration_units" => {
                                    m.units = Some(s.clone())
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
            out.push(m);
        }
    }
    out.sort_by(|a, b| {
        (
            is_builtin(&a.source),
            source_rank(&a.source),
            &a.source,
            &a.name,
        )
            .cmp(&(
                is_builtin(&b.source),
                source_rank(&b.source),
                &b.source,
                &b.name,
            ))
    });
    Ok(out)
}

/// The previous poll's metrics, so counters can be shown as rates.
pub struct Prev<'a> {
    pub at: Instant,
    pub metrics: &'a [Metric],
}

impl Prev<'_> {
    fn get(&self, key: &str) -> Option<&Metric> {
        self.metrics.iter().find(|m| m.key == key)
    }
}

/// A counter's change per second since the previous poll.
pub fn rate(m: &Metric, prev: Option<&Prev>, now: Instant) -> Option<f64> {
    let p = prev?;
    let before = p.get(&m.key)?.value?;
    let dt = now.duration_since(p.at).as_secs_f64();
    if dt <= 0.0 {
        return None;
    }
    Some((m.value? - before) / dt)
}

pub fn fmt_num(x: f64) -> String {
    if !x.is_finite() {
        return "-".into();
    }
    let a = x.abs();
    if a >= 1e12 {
        format!("{:.2}T", x / 1e12)
    } else if a >= 1e9 {
        format!("{:.2}G", x / 1e9)
    } else if a >= 1e6 {
        format!("{:.2}M", x / 1e6)
    } else if a >= 1e4 {
        format!("{:.1}k", x / 1e3)
    } else if x.fract() == 0.0 {
        format!("{x:.0}")
    } else if a >= 10.0 {
        format!("{x:.1}")
    } else {
        format!("{x:.2}")
    }
}

/// The one number worth charting: a gauge's value, a counter's rate, a
/// meter's one-minute rate, a histogram's or timer's mean.
pub fn series_value(m: &Metric, prev: Option<&Prev>, now: Instant) -> Option<f64> {
    match m.kind {
        Kind::Gauge => m.value,
        Kind::Counter => rate(m, prev, now),
        Kind::Meter => m.field("m1_rate"),
        Kind::Histogram | Kind::Timer => m.field("mean"),
    }
}

/// What the row shows: the headline value and the detail string.
pub fn display(m: &Metric, prev: Option<&Prev>, now: Instant) -> (String, String) {
    match m.kind {
        Kind::Gauge => match (&m.text, m.value) {
            (Some(t), _) => (t.clone(), String::new()),
            (None, Some(v)) => (fmt_num(v), String::new()),
            (None, None) => ("-".into(), String::new()),
        },
        Kind::Counter => {
            let v = m.value.map(fmt_num).unwrap_or_else(|| "-".into());
            let r = rate(m, prev, now)
                .map(|r| format!("{}/s since last poll", fmt_num(r)))
                .unwrap_or_default();
            (v, r)
        }
        Kind::Meter => (
            m.value.map(fmt_num).unwrap_or_else(|| "-".into()),
            format!(
                "{}/s (1m) · {}/s (5m) · {}/s mean",
                fmt_num(m.field("m1_rate").unwrap_or(0.0)),
                fmt_num(m.field("m5_rate").unwrap_or(0.0)),
                fmt_num(m.field("mean_rate").unwrap_or(0.0))
            ),
        ),
        Kind::Histogram | Kind::Timer => {
            let u = m.units.as_deref().map(unit_suffix).unwrap_or("");
            (
                m.value.map(fmt_num).unwrap_or_else(|| "-".into()),
                format!(
                    "mean {}{u} · p50 {}{u} · p95 {}{u} · max {}{u}{}",
                    fmt_num(m.field("mean").unwrap_or(0.0)),
                    fmt_num(m.field("p50").unwrap_or(0.0)),
                    fmt_num(m.field("p95").unwrap_or(0.0)),
                    fmt_num(m.field("max").unwrap_or(0.0)),
                    m.field("m1_rate")
                        .map(|r| format!(" · {}/s", fmt_num(r)))
                        .unwrap_or_default()
                ),
            )
        }
    }
}

fn unit_suffix(units: &str) -> &'static str {
    match units {
        "milliseconds" => "ms",
        "microseconds" => "µs",
        "nanoseconds" => "ns",
        "seconds" => "s",
        _ => "",
    }
}

/// The handful of built-ins that answer "is the driver itself in trouble",
/// pinned above the list: `(label, value, warn)`.
pub fn headline(
    metrics: &[Metric],
    prev: Option<&Prev>,
    now: Instant,
) -> Vec<(String, String, bool)> {
    let mut out = Vec::new();
    let get = |key: &str| metrics.iter().find(|m| m.key == key);

    let dropped: f64 = metrics
        .iter()
        .filter(|m| m.source == "LiveListenerBus" && m.name.ends_with("numDroppedEvents"))
        .filter_map(|m| m.value)
        .sum();
    if metrics.iter().any(|m| m.source == "LiveListenerBus") {
        out.push((
            "listener events dropped".into(),
            if dropped > 0.0 {
                format!(
                    "{} — the UI/REST API has missed events; raise spark.scheduler.listenerbus.eventqueue.capacity",
                    fmt_num(dropped)
                )
            } else {
                "0".into()
            },
            dropped > 0.0,
        ));
    }
    if let Some(m) = get("DAGScheduler.messageProcessingTime") {
        let p95 = m.field("p95").unwrap_or(0.0);
        out.push((
            "scheduler message time p95".into(),
            format!("{}ms", fmt_num(p95)),
            p95 > 1000.0,
        ));
    }
    let stage = |n: &str| {
        get(&format!("DAGScheduler.stage.{n}"))
            .and_then(|m| m.value)
            .unwrap_or(0.0)
    };
    if get("DAGScheduler.stage.runningStages").is_some() {
        out.push((
            "stages running / waiting / failed".into(),
            format!(
                "{} / {} / {}",
                fmt_num(stage("runningStages")),
                fmt_num(stage("waitingStages")),
                fmt_num(stage("failedStages"))
            ),
            stage("failedStages") > 0.0,
        ));
    }
    if let (Some(used), Some(rem)) = (
        get("BlockManager.memory.memUsed_MB").and_then(|m| m.value),
        get("BlockManager.memory.remainingMem_MB").and_then(|m| m.value),
    ) {
        let total = used + rem;
        let pct = if total > 0.0 {
            100.0 * used / total
        } else {
            0.0
        };
        out.push((
            "block manager memory".into(),
            format!("{} of {} MiB ({pct:.0}%)", fmt_num(used), fmt_num(total)),
            pct >= 90.0,
        ));
    }
    if let Some(m) = get("CodeGenerator.compilationTime") {
        out.push((
            "codegen compile".into(),
            format!(
                "{} compilations · mean {}ms · max {}ms",
                fmt_num(m.value.unwrap_or(0.0)),
                fmt_num(m.field("mean").unwrap_or(0.0)),
                fmt_num(m.field("max").unwrap_or(0.0))
            ),
            false,
        ));
    }
    if let Some(m) = get("JVMCPUTime.jvmCpuTime")
        && let Some(r) = rate(m, prev, now)
    {
        // Nanoseconds of CPU per second of wall clock = cores busy.
        out.push(("driver CPU".into(), format!("{:.2} cores", r / 1e9), false));
    }
    out
}

/// A row of the tab: a source header or one metric (by index into the
/// filtered list). Selection moves over metric rows only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Row {
    Source(String),
    Metric(usize),
}

/// The metrics that pass the filter (matched against source, name and key,
/// case-insensitively), with a header row before each source.
pub fn rows<'a>(metrics: &'a [Metric], filter: Option<&str>) -> (Vec<&'a Metric>, Vec<Row>) {
    let keep: Vec<&Metric> = metrics
        .iter()
        .filter(|m| {
            filter.is_none_or(|f| {
                let f = f.to_lowercase();
                m.key.to_lowercase().contains(&f) || m.source.to_lowercase().contains(&f)
            })
        })
        .collect();
    let mut rows = Vec::new();
    let mut last: Option<&str> = None;
    for (i, m) in keep.iter().enumerate() {
        if last != Some(m.source.as_str()) {
            rows.push(Row::Source(m.source.clone()));
            last = Some(m.source.as_str());
        }
        rows.push(Row::Metric(i));
    }
    (keep, rows)
}

/// Sources grouped with their metric counts, for titles and the summary.
pub fn sources(metrics: &[Metric]) -> BTreeMap<&str, usize> {
    let mut out = BTreeMap::new();
    for m in metrics {
        *out.entry(m.source.as_str()).or_insert(0) += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    const SAMPLE: &str = r#"{
      "version": "4.0.0",
      "gauges": {
        "app-1.driver.DAGScheduler.stage.runningStages": {"value": 2},
        "app-1.driver.DAGScheduler.stage.failedStages": {"value": 0},
        "app-1.driver.BlockManager.memory.memUsed_MB": {"value": 512},
        "app-1.driver.BlockManager.memory.remainingMem_MB": {"value": 1536},
        "app-1.driver.OrdersPipeline.lag-seconds": {"value": 42.5},
        "app-1.driver.OrdersPipeline.source-topic": {"value": "orders-v2"},
        "app-1.driver.spark.streaming.orders-agg.inputRate-total": {"value": 2000.0},
        "app-1.driver.spark.streaming.orders-agg.latency": {"value": 410},
        "app-1.1.executor.threadpool.activeTasks": {"value": 3},
        "history.server.something": {"value": 1}
      },
      "counters": {
        "app-1.driver.OrdersPipeline.records-rejected": {"count": 120},
        "app-1.driver.LiveListenerBus.queue.appStatus.numDroppedEvents": {"count": 7},
        "app-1.driver.JVMCPUTime.jvmCpuTime": {"count": 4000000000}
      },
      "meters": {
        "app-1.driver.OrdersPipeline.orders-seen": {"count": 90000, "m1_rate": 1990.0, "m5_rate": 1950.0, "m15_rate": 1900.0, "mean_rate": 1980.0, "units": "events/second"}
      },
      "histograms": {
        "app-1.driver.CodeGenerator.compilationTime": {"count": 31, "max": 900, "mean": 120.5, "min": 10, "p50": 100, "p75": 150, "p95": 400, "p98": 500, "p99": 800, "p999": 900, "stddev": 90}
      },
      "timers": {
        "app-1.driver.DAGScheduler.messageProcessingTime": {"count": 5000, "max": 1500.0, "mean": 2.3, "min": 0.1, "p50": 1.0, "p75": 2.0, "p95": 9.0, "p98": 20.0, "p99": 50.0, "p999": 1500.0, "stddev": 8.0, "m15_rate": 3.0, "m1_rate": 3.2, "m5_rate": 3.1, "mean_rate": 3.0, "duration_units": "milliseconds", "rate_units": "calls/second"}
      }
    }"#;

    #[test]
    fn parses_driver_metrics_app_sources_first() {
        let ms = parse(SAMPLE).unwrap();
        // Executor and History Server keys are left out.
        assert!(ms.iter().all(|m| !m.key.contains("executor")));
        assert_eq!(ms.len(), 14);
        assert_eq!(ms[0].source, "OrdersPipeline");
        assert!(!is_builtin("OrdersPipeline"));
        let rejected = ms.iter().find(|m| m.name == "records-rejected").unwrap();
        assert_eq!(rejected.kind, Kind::Counter);
        assert_eq!(rejected.value, Some(120.0));
        let topic = ms.iter().find(|m| m.name == "source-topic").unwrap();
        assert_eq!(topic.text.as_deref(), Some("orders-v2"));
        let timer = ms
            .iter()
            .find(|m| m.key == "DAGScheduler.messageProcessingTime")
            .unwrap();
        assert_eq!(timer.kind, Kind::Timer);
        assert_eq!(timer.field("p95"), Some(9.0));
        assert_eq!(timer.units.as_deref(), Some("milliseconds"));
        let sq = ms
            .iter()
            .find(|m| m.source == "spark.streaming.orders-agg")
            .unwrap();
        assert_eq!(sq.name, "inputRate-total");
        // Built-ins follow in their fixed order.
        let builtin: Vec<&str> = ms
            .iter()
            .filter(|m| is_builtin(&m.source))
            .map(|m| m.source.as_str())
            .collect();
        assert_eq!(builtin[0], "LiveListenerBus");
        assert_eq!(builtin[1], "DAGScheduler");
    }

    #[test]
    fn counter_rates_and_headline() {
        let now = Instant::now();
        let mut before = parse(SAMPLE).unwrap();
        for m in &mut before {
            if m.name == "records-rejected" {
                m.value = Some(100.0);
            }
            if m.name == "jvmCpuTime" {
                m.value = Some(0.0);
            }
        }
        let after = parse(SAMPLE).unwrap();
        let prev = Prev {
            at: now - Duration::from_secs(2),
            metrics: &before,
        };
        let rejected = after.iter().find(|m| m.name == "records-rejected").unwrap();
        assert_eq!(rate(rejected, Some(&prev), now), Some(10.0));
        let (v, d) = display(rejected, Some(&prev), now);
        assert_eq!(v, "120");
        assert!(d.starts_with("10/s"));
        assert_eq!(display(rejected, None, now).1, "");

        let h = headline(&after, Some(&prev), now);
        let dropped = h
            .iter()
            .find(|(l, _, _)| l.starts_with("listener"))
            .unwrap();
        assert!(dropped.2, "7 dropped events must warn");
        assert!(dropped.1.starts_with("7 "));
        let cpu = h.iter().find(|(l, _, _)| l == "driver CPU").unwrap();
        assert_eq!(cpu.1, "2.00 cores");
        let mem = h.iter().find(|(l, _, _)| l.starts_with("block")).unwrap();
        assert_eq!(mem.1, "512 of 2048 MiB (25%)");
    }

    #[test]
    fn rows_group_by_source_and_filter() {
        let ms = parse(SAMPLE).unwrap();
        let (keep, rows) = super::rows(&ms, None);
        assert_eq!(keep.len(), ms.len());
        assert_eq!(rows[0], Row::Source("OrdersPipeline".into()));
        let (keep, rows) = super::rows(&ms, Some("REJECTED"));
        assert_eq!(keep.len(), 1);
        assert_eq!(rows.len(), 2);
        let (keep, _) = super::rows(&ms, Some("dagscheduler"));
        assert_eq!(keep.len(), 3);
    }

    #[test]
    fn numbers_read_well() {
        assert_eq!(fmt_num(0.0), "0");
        assert_eq!(fmt_num(42.5), "42.5");
        assert_eq!(fmt_num(2.345), "2.35");
        assert_eq!(fmt_num(12_345.0), "12.3k");
        assert_eq!(fmt_num(4e9), "4.00G");
    }
}
