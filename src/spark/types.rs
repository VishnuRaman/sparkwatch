//! Data types mirrored from the Spark REST API (`/api/v1`).
//!
//! Every field is `#[serde(default)]` so that a missing field in an
//! older/newer Spark release degrades to a zero value instead of failing the
//! whole deserialization.

use serde::Deserialize;

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ApplicationInfo {
    pub id: String,
    pub name: String,
    pub attempts: Vec<Attempt>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Attempt {
    pub attempt_id: Option<String>,
    pub start_time: String,
    pub end_time: String,
    pub last_updated: String,
    pub duration: i64,
    pub spark_user: String,
    pub completed: bool,
    pub app_spark_version: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct JobData {
    pub job_id: i64,
    pub name: String,
    pub description: Option<String>,
    pub submission_time: Option<String>,
    pub completion_time: Option<String>,
    /// RUNNING | SUCCEEDED | FAILED | UNKNOWN
    pub status: String,
    pub num_tasks: i64,
    pub num_active_tasks: i64,
    pub num_completed_tasks: i64,
    pub num_skipped_tasks: i64,
    pub num_failed_tasks: i64,
    pub num_active_stages: i64,
    pub num_completed_stages: i64,
    pub num_failed_stages: i64,
    pub stage_ids: Vec<i64>,
}

impl JobData {
    pub fn progress(&self) -> f64 {
        if self.num_tasks <= 0 {
            return 0.0;
        }
        let done = (self.num_completed_tasks + self.num_skipped_tasks) as f64;
        (done / self.num_tasks as f64).clamp(0.0, 1.0)
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct StageData {
    /// ACTIVE | COMPLETE | FAILED | PENDING | SKIPPED
    pub status: String,
    pub stage_id: i64,
    pub attempt_id: i64,
    pub name: String,
    pub num_tasks: i64,
    pub num_active_tasks: i64,
    // NOTE: the stage endpoint spells this `numCompleteTasks`, not
    // `numCompletedTasks` like the jobs endpoint does.
    pub num_complete_tasks: i64,
    pub num_failed_tasks: i64,
    pub num_killed_tasks: i64,
    pub executor_run_time: i64,
    pub submission_time: Option<String>,
    pub completion_time: Option<String>,
    pub input_bytes: i64,
    pub output_bytes: i64,
    pub shuffle_read_bytes: i64,
    pub shuffle_write_bytes: i64,
    pub memory_bytes_spilled: i64,
    pub disk_bytes_spilled: i64,
}

impl StageData {
    pub fn key(&self) -> (i64, i64) {
        (self.stage_id, self.attempt_id)
    }

    pub fn progress(&self) -> f64 {
        if self.num_tasks <= 0 {
            return 0.0;
        }
        (self.num_complete_tasks as f64 / self.num_tasks as f64).clamp(0.0, 1.0)
    }

    /// Active stages sort first, then most recent stage id.
    pub fn sort_key(&self) -> (u8, i64) {
        let rank = match self.status.as_str() {
            "ACTIVE" => 0,
            "PENDING" => 1,
            "FAILED" => 2,
            _ => 3,
        };
        (rank, -self.stage_id)
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ExecutorSummary {
    pub id: String,
    pub host_port: String,
    pub is_active: bool,
    pub rdd_blocks: i64,
    pub memory_used: i64,
    pub disk_used: i64,
    pub total_cores: i64,
    pub max_tasks: i64,
    pub active_tasks: i64,
    pub failed_tasks: i64,
    pub completed_tasks: i64,
    pub total_tasks: i64,
    pub total_duration: i64,
    // serde's camelCase would produce `totalGcTime`; Spark emits `totalGCTime`.
    #[serde(rename = "totalGCTime")]
    pub total_gc_time: i64,
    pub total_input_bytes: i64,
    pub total_shuffle_read: i64,
    pub total_shuffle_write: i64,
    pub max_memory: i64,
    pub add_time: String,
}

impl ExecutorSummary {
    pub fn memory_ratio(&self) -> f64 {
        if self.max_memory <= 0 {
            return 0.0;
        }
        (self.memory_used as f64 / self.max_memory as f64).clamp(0.0, 1.0)
    }
}

/// One consistent poll of everything the UI displays.
#[derive(Debug, Clone, Default)]
pub struct Snapshot {
    pub app: ApplicationInfo,
    pub jobs: Vec<JobData>,
    pub stages: Vec<StageData>,
    pub executors: Vec<ExecutorSummary>,
}

#[cfg(test)]
mod tests {
    use super::*;

    // Trimmed real responses. These guard the two field names that are easy to
    // get wrong: `numCompleteTasks` on stages and `totalGCTime` on executors.
    const STAGES_JSON: &str = r#"[{
        "status":"ACTIVE","stageId":13,"attemptId":0,
        "name":"mapPartitions at Writer.scala:88",
        "numTasks":200,"numActiveTasks":12,"numCompleteTasks":97,"numFailedTasks":3,
        "shuffleReadBytes":2147483648,"memoryBytesSpilled":536870912,
        "details":"org.apache.spark.rdd.RDD.mapPartitions(RDD.scala:863)"
    }]"#;

    const EXECS_JSON: &str = r#"[{
        "id":"1","hostPort":"10.0.1.9:7079","isActive":true,
        "totalCores":4,"activeTasks":4,"failedTasks":2,"completedTasks":610,
        "totalDuration":1820000,"totalGCTime":240000,
        "memoryUsed":1610612736,"maxMemory":4294967296,
        "executorLogs":{"stdout":"http://host:8042/logs/stdout"}
    }]"#;

    #[test]
    fn parses_stage_task_counts() {
        let s: Vec<StageData> = serde_json::from_str(STAGES_JSON).unwrap();
        assert_eq!(s[0].num_complete_tasks, 97);
        assert!((s[0].progress() - 0.485).abs() < 1e-6);
    }

    #[test]
    fn parses_executor_gc_time() {
        let e: Vec<ExecutorSummary> = serde_json::from_str(EXECS_JSON).unwrap();
        assert_eq!(e[0].total_gc_time, 240_000);
        assert!((e[0].memory_ratio() - 0.375).abs() < 1e-6);
    }

    #[test]
    fn unknown_and_missing_fields_are_tolerated() {
        // Forward compatibility: a future Spark adding fields must not break us,
        // and an older Spark omitting them must not either.
        let j: Vec<JobData> =
            serde_json::from_str(r#"[{"jobId":1,"status":"RUNNING","someNewField":42}]"#).unwrap();
        assert_eq!(j[0].job_id, 1);
        assert_eq!(j[0].num_tasks, 0);
    }
}
