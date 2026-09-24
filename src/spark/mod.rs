//! Thin client over the Spark REST API (`/api/v1`).
//!
//! Works against both a live driver (`http://<driver>:4040`) and the
//! History Server (`http://<host>:18080`).

mod types;

pub use types::*;

use anyhow::{Context, Result};
use serde::Deserialize;
use std::time::Duration;

#[derive(Clone)]
pub struct SparkClient {
    http: reqwest::Client,
    /// Base URL without trailing slash, e.g. `http://localhost:4040`.
    base: String,
}

impl SparkClient {
    pub fn new(base: impl Into<String>, timeout: Duration) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .context("building HTTP client")?;
        Ok(Self {
            http,
            base: base.into().trim_end_matches('/').to_string(),
        })
    }

    async fn get<T: for<'de> Deserialize<'de>>(&self, path: &str) -> Result<T> {
        self.get_opt(path)
            .await?
            .with_context(|| format!("GET {}/api/v1{path} -> 404", self.base))
    }

    /// Like `get`, but a 404 is `Ok(None)`. Spark answers 404 for things that
    /// legitimately don't exist yet (task summary before any task finished,
    /// `/sql` on a non-SQL app) and those must not read as errors.
    async fn get_opt<T: for<'de> Deserialize<'de>>(&self, path: &str) -> Result<Option<T>> {
        let url = format!("{}/api/v1{}", self.base, path);
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        let status = resp.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("GET {url} -> {status}: {}", body.chars().take(200).collect::<String>());
        }
        resp.json::<T>()
            .await
            .map(Some)
            .with_context(|| format!("decoding response from {url}"))
    }

    pub async fn applications(&self) -> Result<Vec<ApplicationInfo>> {
        self.get("/applications").await
    }

    /// Everything the main view shows for one application, fetched concurrently.
    pub async fn poll(&self, id: &str) -> Result<Snapshot> {
        let (app_path, jobs_path, stages_path, execs_path) = (
            format!("/applications/{id}"),
            format!("/applications/{id}/jobs"),
            format!("/applications/{id}/stages"),
            format!("/applications/{id}/allexecutors"),
        );

        let (app, jobs, stages, executors) = tokio::try_join!(
            self.get::<ApplicationInfo>(&app_path),
            self.get::<Vec<JobData>>(&jobs_path),
            self.get::<Vec<StageData>>(&stages_path),
            self.get::<Vec<ExecutorSummary>>(&execs_path),
        )?;

        let mut jobs = jobs;
        jobs.sort_by_key(|j| -j.job_id);
        let mut stages = stages;
        stages.sort_by_key(|s| s.sort_key());

        Ok(Snapshot {
            app,
            jobs,
            stages,
            executors,
        })
    }

    /// The stage drill-down: stage with per-executor summary, task metric
    /// quantiles, the slowest tasks and (if any) the failed ones.
    pub async fn stage_detail(&self, app_id: &str, stage_id: i64, attempt: i64) -> Result<StageDetail> {
        let base = format!("/applications/{app_id}/stages/{stage_id}/{attempt}");
        let summary_path = format!("{base}/taskSummary?quantiles={SUMMARY_QUANTILES}");
        let slowest_path = format!("{base}/taskList?sortBy=-runtime&length={SLOWEST_TASKS}");
        let (stage, summary, slowest) = tokio::try_join!(
            self.get::<StageData>(&base),
            self.get_opt::<TaskMetricDistributions>(&summary_path),
            self.get::<Vec<TaskData>>(&slowest_path),
        )?;
        let failed = if stage.num_failed_tasks > 0 {
            self.get::<Vec<TaskData>>(&format!("{base}/taskList?status=failed&length={FAILED_TASKS}"))
                .await?
        } else {
            Vec::new()
        };
        Ok(StageDetail {
            stage,
            summary,
            slowest,
            failed,
        })
    }
}

/// p5 … p95 for the distribution table, plus 1.0 so we also get the max.
pub const SUMMARY_QUANTILES: &str = "0.05,0.25,0.5,0.75,0.95,1.0";
const SLOWEST_TASKS: usize = 100;
const FAILED_TASKS: usize = 50;
