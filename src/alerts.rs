//! A persistent log of things that went wrong.
//!
//! The Spark UI shows failures only where they happened, and only while the
//! API still retains them; a failed task's error scrolls off the stage page
//! and a lost executor is a grey row on the executors page. This log is
//! derived from every snapshot, deduplicated by key, and kept for the life
//! of the process, so nothing is missed because you were on another tab.

use crate::spark::{Snapshot, TaskData};
use std::collections::HashSet;
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Job,
    Stage,
    Task,
    Executor,
    Sql,
}

impl Kind {
    pub fn label(&self) -> &'static str {
        match self {
            Kind::Job => "job",
            Kind::Stage => "stage",
            Kind::Task => "task",
            Kind::Executor => "executor",
            Kind::Sql => "sql",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Alert {
    /// Dedup key, e.g. `stage:9.0`, `task:4021.0`, `exec:2:removed`.
    pub key: String,
    pub kind: Kind,
    pub first_seen: Instant,
    pub title: String,
    /// Full error / reason text; may be many lines.
    pub detail: Option<String>,
    /// The stage this concerns, so the Failures tab can jump into it.
    pub stage: Option<(i64, i64)>,
    pub executor_id: Option<String>,
}

impl Alert {
    pub fn detail_line(&self) -> &str {
        self.detail
            .as_deref()
            .and_then(|d| d.lines().find(|l| !l.trim().is_empty()))
            .unwrap_or("")
            .trim()
    }
}

/// Keeps memory bounded on a long-running streaming app that fails a task
/// every few minutes for a week.
const MAX_ALERTS: usize = 1000;

#[derive(Default)]
pub struct AlertLog {
    /// Oldest first.
    alerts: Vec<Alert>,
    keys: HashSet<String>,
    /// Everything before this index has been acknowledged with `x`.
    acked: usize,
}

impl AlertLog {
    /// Newest first.
    pub fn newest_first(&self) -> impl Iterator<Item = &Alert> {
        self.alerts.iter().rev()
    }

    pub fn len(&self) -> usize {
        self.alerts.len()
    }

    pub fn unacked(&self) -> usize {
        self.alerts.len() - self.acked
    }

    /// The most recent alert nobody has looked at yet.
    pub fn latest_unacked(&self) -> Option<&Alert> {
        if self.unacked() == 0 {
            None
        } else {
            self.alerts.last()
        }
    }

    pub fn acknowledge(&mut self) {
        self.acked = self.alerts.len();
    }

    /// Not yet acknowledged with `x`.
    pub fn is_new(&self, a: &Alert) -> bool {
        self.alerts
            .iter()
            .position(|x| x.key == a.key)
            .is_some_and(|i| i >= self.acked)
    }

    pub fn clear(&mut self) {
        *self = Self::default();
    }

    /// Alert at a position in newest-first order.
    pub fn get_newest(&self, index: usize) -> Option<&Alert> {
        self.alerts.iter().rev().nth(index)
    }

    fn push(&mut self, mut alert: Alert) {
        if self.keys.contains(&alert.key) {
            // Already known; the only thing worth updating is a reason that
            // arrived later (Spark fills failureReason after the FAILED status).
            if let Some(existing) = self.alerts.iter_mut().find(|a| a.key == alert.key)
                && existing.detail.is_none()
                && alert.detail.is_some()
            {
                existing.detail = alert.detail.take();
            }
            return;
        }
        alert.first_seen = Instant::now();
        self.keys.insert(alert.key.clone());
        self.alerts.push(alert);
        if self.alerts.len() > MAX_ALERTS {
            let dropped = self.alerts.remove(0);
            self.keys.remove(&dropped.key);
            self.acked = self.acked.saturating_sub(1);
        }
    }

    /// Derive alerts from a fresh snapshot. Returns how many are new.
    pub fn ingest(&mut self, s: &Snapshot) -> usize {
        let before = self.alerts.len();

        for st in s.stages.iter().filter(|st| st.status == "FAILED") {
            self.push(Alert {
                key: format!("stage:{}.{}", st.stage_id, st.attempt_id),
                kind: Kind::Stage,
                first_seen: Instant::now(),
                title: format!(
                    "Stage {}.{} failed · {}",
                    st.stage_id, st.attempt_id, st.name
                ),
                detail: st.failure_reason.clone(),
                stage: Some(st.key()),
                executor_id: None,
            });
        }

        for j in s.jobs.iter().filter(|j| j.status == "FAILED") {
            // The failed stage(s) carry the reason; point at the newest one.
            let stage = s
                .stages
                .iter()
                .filter(|st| st.status == "FAILED" && j.stage_ids.contains(&st.stage_id))
                .max_by_key(|st| st.stage_id)
                .map(|st| st.key());
            self.push(Alert {
                key: format!("job:{}", j.job_id),
                kind: Kind::Job,
                first_seen: Instant::now(),
                title: format!("Job #{} failed · {}", j.job_id, j.name),
                detail: Some(format!(
                    "{} of {} stages failed, {} tasks failed",
                    j.num_failed_stages,
                    j.stage_ids.len(),
                    j.num_failed_tasks
                )),
                stage,
                executor_id: None,
            });
        }

        for e in &s.executors {
            if !e.is_active && e.remove_reason.is_some() {
                self.push(Alert {
                    key: format!("exec:{}:removed", e.id),
                    kind: Kind::Executor,
                    first_seen: Instant::now(),
                    title: format!("Executor {} lost · {}", e.id, e.host_port),
                    detail: e.remove_reason.clone(),
                    stage: None,
                    executor_id: Some(e.id.clone()),
                });
            }
            if e.excluded() {
                self.push(Alert {
                    key: format!("exec:{}:excluded", e.id),
                    kind: Kind::Executor,
                    first_seen: Instant::now(),
                    title: format!("Executor {} excluded · {}", e.id, e.host_port),
                    detail: Some(format!(
                        "Excluded by the scheduler after {} failed tasks; no new tasks will be scheduled on it.",
                        e.failed_tasks
                    )),
                    stage: None,
                    executor_id: Some(e.id.clone()),
                });
            }
        }

        for q in s.sql.iter().flatten().filter(|q| q.status == "FAILED") {
            self.push(Alert {
                key: format!("sql:{}", q.id),
                kind: Kind::Sql,
                first_seen: Instant::now(),
                title: format!("Query #{} failed · {}", q.id, q.title()),
                detail: q.error_message.clone(),
                stage: None,
                executor_id: None,
            });
        }

        for (stage, tasks) in &s.failed_tasks {
            for t in tasks {
                self.push_task(*stage, t);
            }
        }

        self.alerts.len() - before
    }

    fn push_task(&mut self, stage: (i64, i64), t: &TaskData) {
        self.push(Alert {
            key: format!("task:{}.{}", t.task_id, t.attempt),
            kind: Kind::Task,
            first_seen: Instant::now(),
            title: format!(
                "Task {}.{} failed · stage {}.{} · executor {} ({})",
                t.task_id, t.attempt, stage.0, stage.1, t.executor_id, t.host
            ),
            detail: t.error_message.clone(),
            stage: Some(stage),
            executor_id: Some(t.executor_id.clone()),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spark::{ExecutionData, ExecutorSummary, JobData, StageData};

    fn failed_stage(id: i64, reason: Option<&str>) -> StageData {
        StageData {
            stage_id: id,
            status: "FAILED".into(),
            name: "collect at Main.scala:22".into(),
            failure_reason: reason.map(str::to_string),
            ..Default::default()
        }
    }

    #[test]
    fn derives_one_alert_per_failure_and_dedups_across_polls() {
        let mut log = AlertLog::default();
        let snap = Snapshot {
            stages: vec![failed_stage(4, Some("Job aborted"))],
            jobs: vec![JobData {
                job_id: 1,
                status: "FAILED".into(),
                stage_ids: vec![4],
                ..Default::default()
            }],
            executors: vec![
                ExecutorSummary {
                    id: "2".into(),
                    is_active: false,
                    remove_reason: Some("OOMKilled".into()),
                    ..Default::default()
                },
                ExecutorSummary {
                    id: "3".into(),
                    is_active: true,
                    is_excluded: true,
                    ..Default::default()
                },
            ],
            sql: Some(vec![ExecutionData {
                id: 11,
                status: "FAILED".into(),
                error_message: Some("SparkException".into()),
                ..Default::default()
            }]),
            failed_tasks: vec![(
                (4, 0),
                vec![TaskData {
                    task_id: 412,
                    attempt: 3,
                    error_message: Some("OutOfMemoryError".into()),
                    ..Default::default()
                }],
            )],
            ..Default::default()
        };
        assert_eq!(log.ingest(&snap), 6);
        assert_eq!(log.unacked(), 6);
        // Same snapshot again: nothing new.
        assert_eq!(log.ingest(&snap), 0);
        assert_eq!(log.len(), 6);

        let kinds: Vec<Kind> = log.newest_first().map(|a| a.kind).collect();
        assert_eq!(kinds[0], Kind::Task); // ingested last → newest
        // Job alert points at its failed stage so the tab can jump there.
        let job = log.newest_first().find(|a| a.kind == Kind::Job).unwrap();
        assert_eq!(job.stage, Some((4, 0)));

        log.acknowledge();
        assert_eq!(log.unacked(), 0);
        assert!(log.latest_unacked().is_none());
    }

    #[test]
    fn late_failure_reason_fills_in_without_a_new_alert() {
        let mut log = AlertLog::default();
        let mut snap = Snapshot {
            stages: vec![failed_stage(4, None)],
            ..Default::default()
        };
        assert_eq!(log.ingest(&snap), 1);
        assert_eq!(log.newest_first().next().unwrap().detail_line(), "");
        snap.stages[0].failure_reason = Some("Job aborted due to stage failure".into());
        assert_eq!(log.ingest(&snap), 0);
        assert_eq!(
            log.newest_first().next().unwrap().detail_line(),
            "Job aborted due to stage failure"
        );
    }

    #[test]
    fn log_is_bounded() {
        let mut log = AlertLog::default();
        for i in 0..(MAX_ALERTS as i64 + 50) {
            let snap = Snapshot {
                stages: vec![failed_stage(i, None)],
                ..Default::default()
            };
            log.ingest(&snap);
        }
        assert_eq!(log.len(), MAX_ALERTS);
        assert_eq!(
            log.newest_first().next().unwrap().stage,
            Some((MAX_ALERTS as i64 + 49, 0))
        );
    }
}
