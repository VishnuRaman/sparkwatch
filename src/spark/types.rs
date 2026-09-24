//! Data types mirrored from the Spark REST API (`/api/v1`).
//!
//! Every field is `#[serde(default)]` so that a missing field in an
//! older/newer Spark release degrades to a zero value instead of failing the
//! whole deserialization.

use serde::Deserialize;
use std::collections::HashMap;

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
    pub jvm_gc_time: i64,
    pub peak_execution_memory: i64,
    pub description: Option<String>,
    pub scheduling_pool: String,
    pub failure_reason: Option<String>,
    /// Only populated by the single-stage endpoint, keyed by executor id.
    pub executor_summary: HashMap<String, ExecutorStageSummary>,
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

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ExecutorStageSummary {
    pub task_time: i64,
    pub failed_tasks: i64,
    pub succeeded_tasks: i64,
    pub killed_tasks: i64,
    pub input_bytes: i64,
    pub output_bytes: i64,
    pub shuffle_read: i64,
    pub shuffle_write: i64,
    pub memory_bytes_spilled: i64,
    pub disk_bytes_spilled: i64,
    // Renamed in Spark 3.1; accept both spellings.
    pub is_blacklisted_for_stage: bool,
    pub is_excluded_for_stage: bool,
}

impl ExecutorStageSummary {
    pub fn tasks(&self) -> i64 {
        self.succeeded_tasks + self.failed_tasks + self.killed_tasks
    }

    pub fn excluded(&self) -> bool {
        self.is_excluded_for_stage || self.is_blacklisted_for_stage
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct TaskData {
    pub task_id: i64,
    pub index: i64,
    pub attempt: i64,
    pub launch_time: String,
    pub duration: Option<i64>,
    pub executor_id: String,
    pub host: String,
    /// RUNNING | SUCCESS | FAILED | KILLED | PENDING
    pub status: String,
    pub task_locality: String,
    pub speculative: bool,
    pub error_message: Option<String>,
    pub task_metrics: Option<TaskMetrics>,
    pub scheduler_delay: i64,
    pub getting_result_time: i64,
}

impl TaskData {
    pub fn duration_ms(&self) -> i64 {
        self.duration.unwrap_or(0)
    }

    pub fn metrics(&self) -> TaskMetrics {
        self.task_metrics.clone().unwrap_or_default()
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct TaskMetrics {
    pub executor_deserialize_time: i64,
    pub executor_run_time: i64,
    pub executor_cpu_time: i64,
    pub result_size: i64,
    pub jvm_gc_time: i64,
    pub result_serialization_time: i64,
    pub memory_bytes_spilled: i64,
    pub disk_bytes_spilled: i64,
    pub peak_execution_memory: i64,
    pub input_metrics: InputMetrics,
    pub output_metrics: OutputMetrics,
    pub shuffle_read_metrics: ShuffleReadMetrics,
    pub shuffle_write_metrics: ShuffleWriteMetrics,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct InputMetrics {
    pub bytes_read: i64,
    pub records_read: i64,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct OutputMetrics {
    pub bytes_written: i64,
    pub records_written: i64,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ShuffleReadMetrics {
    pub remote_blocks_fetched: i64,
    pub local_blocks_fetched: i64,
    pub fetch_wait_time: i64,
    pub remote_bytes_read: i64,
    pub remote_bytes_read_to_disk: i64,
    pub local_bytes_read: i64,
    pub records_read: i64,
}

impl ShuffleReadMetrics {
    pub fn bytes(&self) -> i64 {
        self.remote_bytes_read + self.local_bytes_read
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ShuffleWriteMetrics {
    pub bytes_written: i64,
    pub write_time: i64,
    pub records_written: i64,
}

/// Per-metric quantiles from `taskSummary`. Every vector is parallel to
/// `quantiles`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct TaskMetricDistributions {
    pub quantiles: Vec<f64>,
    pub duration: Vec<f64>,
    pub executor_run_time: Vec<f64>,
    pub executor_cpu_time: Vec<f64>,
    pub jvm_gc_time: Vec<f64>,
    pub scheduler_delay: Vec<f64>,
    pub peak_execution_memory: Vec<f64>,
    pub memory_bytes_spilled: Vec<f64>,
    pub disk_bytes_spilled: Vec<f64>,
    pub input_metrics: InputMetricDistributions,
    pub output_metrics: OutputMetricDistributions,
    pub shuffle_read_metrics: ShuffleReadMetricDistributions,
    pub shuffle_write_metrics: ShuffleWriteMetricDistributions,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct InputMetricDistributions {
    pub bytes_read: Vec<f64>,
    pub records_read: Vec<f64>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct OutputMetricDistributions {
    pub bytes_written: Vec<f64>,
    pub records_written: Vec<f64>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ShuffleReadMetricDistributions {
    pub read_bytes: Vec<f64>,
    pub read_records: Vec<f64>,
    pub fetch_wait_time: Vec<f64>,
    pub remote_bytes_read: Vec<f64>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ShuffleWriteMetricDistributions {
    pub write_bytes: Vec<f64>,
    pub write_records: Vec<f64>,
    pub write_time: Vec<f64>,
}

/// One Spark SQL execution (a query, or a micro-batch of a streaming query).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ExecutionData {
    pub id: i64,
    /// RUNNING | COMPLETED | FAILED
    pub status: String,
    pub description: String,
    /// The physical plan, only when asked for with `planDescription=true`.
    pub plan_description: String,
    pub submission_time: String,
    pub duration: i64,
    pub running_job_ids: Vec<i64>,
    pub success_job_ids: Vec<i64>,
    pub failed_job_ids: Vec<i64>,
    /// Plan nodes with their metrics, only with `details=true`.
    pub nodes: Vec<SqlNode>,
    pub error_message: Option<String>,
}

impl ExecutionData {
    /// First line of the description, which for DataFrame code is the call
    /// site and for SQL the start of the statement.
    pub fn title(&self) -> &str {
        self.description.lines().next().unwrap_or("").trim()
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SqlNode {
    pub node_id: i64,
    pub node_name: String,
    pub whole_stage_codegen_id: Option<i64>,
    pub metrics: Vec<SqlMetric>,
}

/// Values arrive pre-formatted by Spark ("1,234,567", "2.1 GiB", "12.3 s").
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SqlMetric {
    pub name: String,
    pub value: String,
}

/// Everything the stage drill-down shows, fetched together.
#[derive(Debug, Clone, Default)]
pub struct StageDetail {
    pub stage: StageData,
    /// `None` until at least one task has completed (Spark 404s before that).
    pub summary: Option<TaskMetricDistributions>,
    /// Longest-running tasks first.
    pub slowest: Vec<TaskData>,
    pub failed: Vec<TaskData>,
}

/// One consistent poll of everything the UI displays.
#[derive(Debug, Clone, Default)]
pub struct Snapshot {
    pub app: ApplicationInfo,
    pub jobs: Vec<JobData>,
    pub stages: Vec<StageData>,
    pub executors: Vec<ExecutorSummary>,
    /// Newest first. `None` when the endpoint has no `/sql` (not a SQL app,
    /// or a Spark too old to serve it).
    pub sql: Option<Vec<ExecutionData>>,
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

    const TASKS_JSON: &str = r#"[{
        "taskId":4021,"index":17,"attempt":0,"launchTime":"2026-09-23T10:05:12.000GMT",
        "duration":91000,"executorId":"1","host":"10.0.1.9","status":"SUCCESS",
        "taskLocality":"PROCESS_LOCAL","speculative":false,"accumulatorUpdates":[],
        "taskMetrics":{"executorRunTime":90200,"jvmGcTime":12000,"memoryBytesSpilled":536870912,
            "shuffleReadMetrics":{"remoteBytesRead":1073741824,"localBytesRead":1048576,"fetchWaitTime":300}}
    },{
        "taskId":4022,"index":18,"attempt":1,"executorId":"2","host":"10.0.1.10","status":"FAILED",
        "errorMessage":"ExecutorLostFailure (executor 2 exited caused by one of the running tasks)\nReason: Container killed by YARN for exceeding memory limits."
    }]"#;

    const SUMMARY_JSON: &str = r#"{
        "quantiles":[0.05,0.25,0.5,0.75,0.95,1.0],
        "duration":[1000.0,2500.0,8000.0,9500.0,30000.0,91000.0],
        "jvmGcTime":[0.0,10.0,50.0,100.0,900.0,12000.0],
        "shuffleReadMetrics":{"readBytes":[1048576.0,2097152.0,4194304.0,8388608.0,33554432.0,1074790400.0],
            "readRecords":[1.0,2.0,3.0,4.0,5.0,6.0],"fetchWaitTime":[0,0,0,0,0,300]},
        "shuffleWriteMetrics":{"writeBytes":[0,0,0,0,0,0]}
    }"#;

    #[test]
    fn parses_task_list_including_failed_task() {
        let t: Vec<TaskData> = serde_json::from_str(TASKS_JSON).unwrap();
        assert_eq!(t[0].duration_ms(), 91_000);
        assert_eq!(t[0].metrics().shuffle_read_metrics.bytes(), 1073741824 + 1048576);
        assert_eq!(t[1].duration_ms(), 0);
        assert!(t[1].error_message.as_deref().unwrap().starts_with("ExecutorLostFailure"));
    }

    #[test]
    fn parses_task_summary_quantiles() {
        let d: TaskMetricDistributions = serde_json::from_str(SUMMARY_JSON).unwrap();
        assert_eq!(d.quantiles.len(), 6);
        assert_eq!(d.duration[2], 8000.0);
        assert_eq!(d.shuffle_read_metrics.read_bytes[5], 1074790400.0);
        assert!(d.memory_bytes_spilled.is_empty()); // absent -> empty, not an error
    }

    #[test]
    fn parses_stage_executor_summary() {
        let s: StageData = serde_json::from_str(r#"{"stageId":9,"attemptId":0,
            "failureReason":"Job aborted due to stage failure",
            "executorSummary":{"1":{"taskTime":1820000,"failedTasks":2,"succeededTasks":98,"isExcludedForStage":true}}}"#).unwrap();
        assert_eq!(s.failure_reason.as_deref(), Some("Job aborted due to stage failure"));
        let e = &s.executor_summary["1"];
        assert_eq!(e.tasks(), 100);
        assert!(e.excluded());
    }

    #[test]
    fn parses_sql_execution_with_nodes() {
        let e: ExecutionData = serde_json::from_str(r#"{
            "id":12,"status":"COMPLETED","description":"save at Writer.scala:88\n== Physical Plan ==",
            "planDescription":"*(2) HashAggregate(keys=[k#1], functions=[count(1)])\n+- Exchange hashpartitioning(k#1, 200)",
            "submissionTime":"2026-09-23T10:05:11.000GMT","duration":41200,
            "runningJobIds":[],"successJobIds":[3,4],"failedJobIds":[],
            "nodes":[{"nodeId":2,"nodeName":"HashAggregate","wholeStageCodegenId":2,
                      "metrics":[{"name":"number of output rows","value":"1,204"},
                                 {"name":"spill size","value":"0.0 B"}]}],
            "edges":[{"fromId":3,"toId":2}]
        }"#).unwrap();
        assert_eq!(e.title(), "save at Writer.scala:88");
        assert_eq!(e.success_job_ids, [3, 4]);
        assert_eq!(e.nodes[0].metrics[0].value, "1,204");
        assert_eq!(e.nodes[0].whole_stage_codegen_id, Some(2));
        assert!(e.error_message.is_none());
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
