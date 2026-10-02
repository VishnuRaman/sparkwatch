//! The "since you were away" summary and the dump bundle.
//!
//! One generator produces plain sections of text; the Summary view renders
//! them on screen and `write_bundle` writes the same thing as `summary.md`,
//! so what you read is what you send.

use crate::alerts::AlertLog;
use crate::analysis;
use crate::spark::{ApplicationEnvironmentInfo, Snapshot, TaskMetricDistributions};
use crate::streaming::{QueryStats, Streaming};
use crate::ui::{fmt_bytes, fmt_millis};
use anyhow::{Context, Result};
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

/// One block of the summary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Section {
    pub title: String,
    pub lines: Vec<String>,
}

/// Everything the summary looks at.
pub struct Inputs<'a> {
    pub endpoint: &'a str,
    pub snapshot: Option<&'a Snapshot>,
    pub alerts: &'a AlertLog,
    pub streaming: &'a Streaming,
    pub environment: Option<&'a ApplicationEnvironmentInfo>,
    /// `taskSummary` quantiles for the slowest stages, when fetched.
    pub stage_summaries: &'a HashMap<(i64, i64), TaskMetricDistributions>,
}

/// How many "top N" entries each section lists.
const TOP: usize = 5;

pub fn summarize(inp: &Inputs) -> Vec<Section> {
    let mut out = Vec::new();

    // ---- application
    let mut app_lines = Vec::new();
    if let Some(s) = inp.snapshot {
        let a = s.app.attempts.first().cloned().unwrap_or_default();
        app_lines.push(format!("{} [{}] @ {}", s.app.name, s.app.id, inp.endpoint));
        app_lines.push(format!(
            "user {} · Spark {} · started {} · {} {}",
            a.spark_user,
            a.app_spark_version,
            a.start_time,
            if a.completed { "ran for" } else { "up for" },
            fmt_millis(a.duration)
        ));
        let active = s.executors.iter().filter(|e| e.is_active).count();
        let cores: i64 = s
            .executors
            .iter()
            .filter(|e| e.is_active)
            .map(|e| e.total_cores)
            .sum();
        app_lines.push(format!(
            "{} jobs ({} failed) · {} stages ({} failed) · {} executors alive of {} seen · {} cores",
            s.jobs.len(),
            s.jobs.iter().filter(|j| j.status == "FAILED").count(),
            s.stages.len(),
            s.stages.iter().filter(|st| st.status == "FAILED").count(),
            active,
            s.executors.len(),
            cores
        ));
    } else {
        app_lines.push(format!("{} · no snapshot yet", inp.endpoint));
    }
    out.push(Section {
        title: "Application".into(),
        lines: app_lines,
    });

    // ---- failures: counts by kind, then the most common reasons
    let mut by_kind: BTreeMap<&str, usize> = BTreeMap::new();
    let mut reasons: HashMap<String, (usize, String)> = HashMap::new();
    for a in inp.alerts.newest_first() {
        *by_kind.entry(a.kind.label()).or_default() += 1;
        let key = a.detail_line().chars().take(120).collect::<String>();
        if !key.is_empty() {
            let e = reasons.entry(key).or_insert((0, a.title.clone()));
            e.0 += 1;
        }
    }
    let mut lines = Vec::new();
    if inp.alerts.len() == 0 {
        lines.push("none seen".into());
    } else {
        lines.push(
            by_kind
                .iter()
                .map(|(k, n)| format!("{n} {k}"))
                .collect::<Vec<_>>()
                .join(" · "),
        );
        let mut top: Vec<(&String, &(usize, String))> = reasons.iter().collect();
        top.sort_by(|a, b| b.1.0.cmp(&a.1.0).then(a.0.cmp(b.0)));
        for (reason, (n, first_title)) in top.into_iter().take(TOP) {
            lines.push(format!("{n}× {reason}  (e.g. {first_title})"));
        }
    }
    out.push(Section {
        title: "Failures".into(),
        lines,
    });

    // ---- slowest stages, with skew when we have the quantiles
    if let Some(s) = inp.snapshot {
        let mut stages: Vec<_> = s
            .stages
            .iter()
            .filter(|st| st.executor_run_time > 0)
            .collect();
        stages.sort_by_key(|st| -st.executor_run_time);
        let mut lines = Vec::new();
        for st in stages.iter().take(TOP) {
            let mut line = format!(
                "{}.{} {} · {} · task time {} · {} tasks · shuffle r {} w {} · spill {}",
                st.stage_id,
                st.attempt_id,
                st.status,
                crate::ui::display_name(&st.name, st.description.as_deref(), None),
                fmt_millis(st.executor_run_time),
                st.num_tasks,
                fmt_bytes(st.shuffle_read_bytes),
                fmt_bytes(st.shuffle_write_bytes),
                fmt_bytes(st.memory_bytes_spilled + st.disk_bytes_spilled)
            );
            if let Some(d) = inp.stage_summaries.get(&st.key()) {
                let skewed: Vec<String> = analysis::metric_rows(d)
                    .into_iter()
                    .filter(|m| m.is_skewed())
                    .map(|m| {
                        format!(
                            "{} ×{}",
                            m.name,
                            m.skew
                                .map(|r| if r.is_infinite() {
                                    "∞".to_string()
                                } else {
                                    format!("{r:.1}")
                                })
                                .unwrap_or_default()
                        )
                    })
                    .collect();
                if !skewed.is_empty() {
                    line.push_str(&format!(" · SKEW {}", skewed.join(", ")));
                }
            }
            lines.push(line);
        }
        if lines.is_empty() {
            lines.push("no stage has run yet".into());
        }
        out.push(Section {
            title: "Slowest stages".into(),
            lines,
        });

        // ---- executors
        let mut lines = Vec::new();
        for e in s.executors.iter().filter(|e| !e.is_active) {
            // Kubernetes removal reasons span many lines; the first
            // non-empty one carries the exit code and the gist.
            let reason = e
                .remove_reason
                .as_deref()
                .and_then(|r| r.lines().map(str::trim).find(|l| !l.is_empty()))
                .unwrap_or("no reason reported");
            lines.push(format!(
                "executor {} lost ({}) — {reason}",
                e.id, e.host_port
            ));
        }
        for e in s.executors.iter().filter(|e| e.excluded()) {
            lines.push(format!(
                "executor {} excluded after {} failed tasks",
                e.id, e.failed_tasks
            ));
        }
        let mut gc: Vec<(&str, f64)> = s
            .executors
            .iter()
            .filter(|e| e.total_duration > 0)
            .map(|e| {
                (
                    e.id.as_str(),
                    100.0 * e.total_gc_time as f64 / e.total_duration as f64,
                )
            })
            .collect();
        gc.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        if let Some((id, pct)) = gc.first()
            && *pct > 10.0
        {
            lines.push(format!(
                "GC: executor {id} spent {pct:.1}% of task time in GC (worst of {})",
                gc.len()
            ));
        }
        if let Some(env) = inp.environment {
            for e in &s.executors {
                let budget = env
                    .resource_profiles
                    .iter()
                    .find(|p| p.id == e.resource_profile_id)
                    .and_then(|p| p.executor_resources.get("memory"))
                    .map(|r| r.amount * 1024 * 1024);
                if let (Some(peak), Some(max)) = (e.peak("JVMHeapMemory"), budget)
                    && max > 0
                    && peak as f64 / max as f64 >= 0.9
                {
                    lines.push(format!(
                        "executor {}: peak heap {} is {:.0}% of spark.executor.memory",
                        e.id,
                        fmt_bytes(peak),
                        100.0 * peak as f64 / max as f64
                    ));
                }
            }
        }
        if lines.is_empty() {
            lines.push("all executors alive, no exclusions, GC under 10%".into());
        }
        out.push(Section {
            title: "Executors".into(),
            lines,
        });

        // ---- storage
        let partial = s
            .rdds
            .iter()
            .filter(|r| r.num_cached_partitions < r.num_partitions)
            .count();
        if !s.rdds.is_empty() {
            let mem: i64 = s.rdds.iter().map(|r| r.memory_used).sum();
            let disk: i64 = s.rdds.iter().map(|r| r.disk_used).sum();
            out.push(Section {
                title: "Storage".into(),
                lines: vec![format!(
                    "{} cached RDDs · {} in memory · {} on disk{}",
                    s.rdds.len(),
                    fmt_bytes(mem),
                    fmt_bytes(disk),
                    if partial > 0 {
                        format!(" · {partial} only partially cached (recomputed on read)")
                    } else {
                        String::new()
                    }
                )],
            });
        }
    }

    // ---- streaming
    if !inp.streaming.is_empty() {
        let mut lines = Vec::new();
        for q in inp.streaming.sorted() {
            let st = QueryStats::of(q);
            let failed = q.batches.values().filter(|b| b.status == "FAILED").count();
            let mut line = format!(
                "{}: {} batches · trigger mean {} p95 {} max {}",
                q.label(),
                q.batches.len(),
                fmt_millis(st.mean_ms),
                fmt_millis(st.p95_ms),
                fmt_millis(st.max_ms)
            );
            if st.input_rps > 0.0 || st.processed_rps > 0.0 {
                line.push_str(&format!(
                    " · in {:.0}/s vs processed {:.0}/s",
                    st.input_rps, st.processed_rps
                ));
            }
            if st.behind {
                line.push_str(" · FALLING BEHIND");
            }
            if let Some(lag) = st.watermark_lag_ms {
                line.push_str(&format!(" · watermark lag {}", fmt_millis(lag)));
            }
            if failed > 0 {
                line.push_str(&format!(" · {failed} failed batches"));
            }
            lines.push(line);
            let mut slow: Vec<_> = q.batches.values().collect();
            slow.sort_by_key(|b| -b.duration_ms);
            let slowest: Vec<String> = slow
                .iter()
                .take(3)
                .map(|b| format!("batch {} {}", b.batch_id, fmt_millis(b.duration_ms)))
                .collect();
            if !slowest.is_empty() {
                lines.push(format!("  slowest: {}", slowest.join(", ")));
            }
        }
        out.push(Section {
            title: "Streaming".into(),
            lines,
        });
    }

    // ---- driver metrics: the health headline, then the app's own sources
    if let Some(ms) = inp.snapshot.and_then(|s| s.metrics.as_deref()) {
        let now = std::time::Instant::now();
        let mut lines: Vec<String> = crate::metrics::headline(ms, None, now)
            .into_iter()
            .map(|(label, value, warn)| {
                format!("{label}: {value}{}", if warn { "  ⚠" } else { "" })
            })
            .collect();
        for (source, n) in crate::metrics::sources(ms) {
            if crate::metrics::is_builtin(source) {
                continue;
            }
            lines.push(format!("app source {source} ({n} metrics):"));
            for m in ms.iter().filter(|m| m.source == source).take(12) {
                let (v, d) = crate::metrics::display(m, None, now);
                lines.push(format!(
                    "  {} = {v}{}",
                    m.name,
                    if d.is_empty() {
                        String::new()
                    } else {
                        format!("  ({d})")
                    }
                ));
            }
        }
        if !lines.is_empty() {
            out.push(Section {
                title: "Metrics".into(),
                lines,
            });
        }
    }

    // ---- key settings
    if let Some(env) = inp.environment {
        let lines: Vec<String> = crate::ui::environment::KEY_SETTINGS
            .iter()
            .filter_map(|k| env.spark(k).map(|v| format!("{k} = {v}")))
            .collect();
        if !lines.is_empty() {
            out.push(Section {
                title: "Key settings".into(),
                lines,
            });
        }
    }
    out
}

/// The summary as Markdown.
pub fn to_markdown(sections: &[Section]) -> String {
    let mut md = String::from("# sparkwatch summary\n\n");
    for s in sections {
        md.push_str(&format!("## {}\n\n", s.title));
        for l in &s.lines {
            md.push_str(&format!("- {l}\n"));
        }
        md.push('\n');
    }
    md
}

/// Alerts as the dump sees them (`Instant` isn't serialisable, so an age).
#[derive(Serialize)]
pub struct AlertOut<'a> {
    pub key: &'a str,
    pub kind: &'a str,
    pub age_secs: u64,
    pub title: &'a str,
    pub detail: Option<&'a str>,
    pub stage: Option<(i64, i64)>,
    pub executor_id: Option<&'a str>,
}

/// Write the bundle. `logs` is `(file name, lines)`.
pub fn write_bundle(
    dir: &Path,
    inp: &Inputs,
    sections: &[Section],
    logs: &[(String, Vec<String>)],
) -> Result<PathBuf> {
    let app_id = inp
        .snapshot
        .map(|s| s.app.id.clone())
        .unwrap_or_else(|| "no-app".into());
    let stamp = {
        let secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        secs.to_string()
    };
    let bundle = dir.join(format!("sparkwatch-{app_id}-{stamp}"));
    std::fs::create_dir_all(bundle.join("logs"))
        .with_context(|| format!("creating {}", bundle.display()))?;

    let write = |name: &str, body: String| -> Result<()> {
        std::fs::write(bundle.join(name), body).with_context(|| format!("writing {name}"))
    };
    write("summary.md", to_markdown(sections))?;
    if let Some(s) = inp.snapshot {
        write("snapshot.json", serde_json::to_string_pretty(s)?)?;
    }
    let alerts: Vec<AlertOut> = inp
        .alerts
        .newest_first()
        .map(|a| AlertOut {
            key: &a.key,
            kind: a.kind.label(),
            age_secs: a.first_seen.elapsed().as_secs(),
            title: &a.title,
            detail: a.detail.as_deref(),
            stage: a.stage,
            executor_id: a.executor_id.as_deref(),
        })
        .collect();
    write("failures.json", serde_json::to_string_pretty(&alerts)?)?;
    write(
        "streaming.json",
        serde_json::to_string_pretty(&inp.streaming.queries)?,
    )?;
    if let Some(env) = inp.environment {
        write("environment.json", serde_json::to_string_pretty(env)?)?;
    }
    if let Some(ms) = inp.snapshot.and_then(|s| s.metrics.as_ref()) {
        write("metrics.json", serde_json::to_string_pretty(ms)?)?;
    }
    for (name, lines) in logs {
        std::fs::write(bundle.join("logs").join(name), lines.join("\n") + "\n")
            .with_context(|| format!("writing logs/{name}"))?;
    }
    Ok(bundle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spark::{ExecutorSummary, JobData, StageData};

    #[test]
    fn summary_covers_failures_stages_and_executors() {
        let mut alerts = AlertLog::default();
        let snap = Snapshot {
            stages: vec![
                StageData {
                    stage_id: 4,
                    status: "FAILED".into(),
                    name: "collect at Main.scala:22".into(),
                    executor_run_time: 30_000,
                    failure_reason: Some("Job aborted due to stage failure: poison".into()),
                    ..Default::default()
                },
                StageData {
                    stage_id: 9,
                    status: "ACTIVE".into(),
                    name: "mapPartitions at Writer.scala:88".into(),
                    executor_run_time: 900_000,
                    memory_bytes_spilled: 512 << 20,
                    ..Default::default()
                },
            ],
            jobs: vec![JobData {
                job_id: 1,
                status: "FAILED".into(),
                stage_ids: vec![4],
                ..Default::default()
            }],
            executors: vec![ExecutorSummary {
                id: "2".into(),
                host_port: "10.0.1.10:7079".into(),
                is_active: false,
                remove_reason: Some("OOMKilled".into()),
                total_duration: 100,
                total_gc_time: 50,
                ..Default::default()
            }],
            ..Default::default()
        };
        alerts.ingest(&snap);
        let mut summaries = HashMap::new();
        let d = TaskMetricDistributions {
            duration: vec![1000.0, 2500.0, 8000.0, 9500.0, 30000.0, 91000.0],
            ..Default::default()
        };
        summaries.insert((9, 0), d);
        let inp = Inputs {
            endpoint: "http://x:4040",
            snapshot: Some(&snap),
            alerts: &alerts,
            streaming: &Streaming::default(),
            environment: None,
            stage_summaries: &summaries,
        };
        let sections = summarize(&inp);
        let titles: Vec<&str> = sections.iter().map(|s| s.title.as_str()).collect();
        assert_eq!(
            titles,
            ["Application", "Failures", "Slowest stages", "Executors"]
        );
        let failures = &sections[1].lines;
        assert!(
            failures[0].contains("1 executor")
                && failures[0].contains("1 job")
                && failures[0].contains("1 stage")
        );
        assert!(
            failures
                .iter()
                .any(|l| l.contains("Job aborted due to stage failure: poison"))
        );
        let stages = &sections[2].lines;
        assert!(
            stages[0].starts_with("9.0 ACTIVE"),
            "slowest first: {}",
            stages[0]
        );
        assert!(stages[0].contains("SKEW duration ×11.4"));
        let execs = &sections[3].lines;
        assert!(
            execs
                .iter()
                .any(|l| l.contains("executor 2 lost") && l.contains("OOMKilled"))
        );
        assert!(
            execs
                .iter()
                .any(|l| l.contains("GC: executor 2 spent 50.0%"))
        );

        let md = to_markdown(&sections);
        assert!(md.starts_with("# sparkwatch summary"));
        assert!(md.contains("## Slowest stages"));
    }

    #[test]
    fn bundle_is_written_with_every_part() {
        let dir = std::env::temp_dir().join(format!("sparkwatch-test-{}", std::process::id()));
        let snap = Snapshot::default();
        let alerts = AlertLog::default();
        let inp = Inputs {
            endpoint: "x",
            snapshot: Some(&snap),
            alerts: &alerts,
            streaming: &Streaming::default(),
            environment: Some(&ApplicationEnvironmentInfo::default()),
            stage_summaries: &HashMap::new(),
        };
        let sections = summarize(&inp);
        let bundle = write_bundle(
            &dir,
            &inp,
            &sections,
            &[("driver.log".into(), vec!["a".into(), "b".into()])],
        )
        .unwrap();
        for f in [
            "summary.md",
            "snapshot.json",
            "failures.json",
            "streaming.json",
            "environment.json",
            "logs/driver.log",
        ] {
            assert!(bundle.join(f).exists(), "{f} missing");
        }
        assert_eq!(
            std::fs::read_to_string(bundle.join("logs/driver.log")).unwrap(),
            "a\nb\n"
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
