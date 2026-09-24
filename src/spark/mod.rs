//! Thin client over the Spark REST API (`/api/v1`).
//!
//! Works against both a live driver (`http://<driver>:4040`) and the
//! History Server (`http://<host>:18080`).

pub mod logs;
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
            sql: None,               // filled in by the poller from its SQL cache
            failed_tasks: Vec::new(), // likewise, once it knows which stages grew
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
            self.failed_tasks(app_id, stage_id, attempt, FAILED_TASKS).await?
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

/// The `/sql` list is oldest-first, has no sort parameter, and a streaming
/// app retains up to 1000 micro-batches with multi-KB descriptions — so we
/// never fetch it whole on every poll. Instead:
///
/// * execution ids are dense and the list is id-ordered, so `index = id -
///   min_id` where `min_id` is the first retained execution (one tiny GET);
/// * `after` is the highest id already known: fetch from a little before it,
///   which refreshes the status of recent executions and picks up new ones.
///
/// The first call (`after == None`) pages through everything once.
impl SparkClient {
    /// `Ok(None)` when the endpoint has no `/sql` at all.
    pub async fn sql_tail(&self, app_id: &str, after: Option<i64>) -> Result<Option<Vec<ExecutionData>>> {
        let list = |offset: usize, length: usize| {
            let path = format!(
                "/applications/{app_id}/sql?details=false&planDescription=false&offset={offset}&length={length}"
            );
            async move { self.get_opt::<Vec<ExecutionData>>(&path).await }
        };

        let Some(first) = list(0, 1).await? else {
            return Ok(None);
        };
        let Some(min_id) = first.first().map(|e| e.id) else {
            return Ok(Some(Vec::new()));
        };

        let mut out = Vec::new();
        let mut offset = match after {
            Some(max) => (max - min_id + 1 - SQL_REFRESH_WINDOW as i64).max(0) as usize,
            None => 0,
        };
        // Bounded: the driver retains at most spark.sql.ui.retainedExecutions
        // (default 1000), so this is a handful of pages at worst, once.
        for _ in 0..SQL_MAX_PAGES {
            let page = list(offset, SQL_PAGE).await?.unwrap_or_default();
            let n = page.len();
            out.extend(page);
            if n < SQL_PAGE {
                break;
            }
            offset += n;
        }
        Ok(Some(out))
    }

    /// Full execution with plan and node metrics. `None` if it has been
    /// evicted from the driver's retained set since we listed it.
    pub async fn sql_detail(&self, app_id: &str, id: i64) -> Result<Option<ExecutionData>> {
        self.get_opt(&format!(
            "/applications/{app_id}/sql/{id}?details=true&planDescription=true"
        ))
        .await
    }
}

/// How many already-known executions to re-fetch each poll so their status
/// (RUNNING → COMPLETED/FAILED) stays current.
const SQL_REFRESH_WINDOW: usize = 100;
const SQL_PAGE: usize = 500;
const SQL_MAX_PAGES: usize = 10;

impl SparkClient {
    /// Live thread dump of an executor. `None` where it is not served (the
    /// History Server, or an executor that is gone).
    pub async fn threads(&self, app_id: &str, executor_id: &str) -> Result<Option<Vec<ThreadStackTrace>>> {
        self.get_opt(&format!("/applications/{app_id}/executors/{executor_id}/threads"))
            .await
    }

    /// A raw GET of any URL (executor log pages live outside `/api/v1`).
    pub async fn fetch_text(&self, url: &str) -> Result<String> {
        let resp = self
            .http
            .get(url)
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        let status = resp.status();
        if !status.is_success() {
            anyhow::bail!("GET {url} -> {status}");
        }
        resp.text().await.with_context(|| format!("reading {url}"))
    }

    /// The failed tasks of a stage attempt, with their error messages.
    pub async fn failed_tasks(&self, app_id: &str, stage_id: i64, attempt: i64, length: usize) -> Result<Vec<TaskData>> {
        self.get(&format!(
            "/applications/{app_id}/stages/{stage_id}/{attempt}/taskList?status=failed&length={length}"
        ))
        .await
    }
}

/// p5 … p95 for the distribution table, plus 1.0 so we also get the max.
pub const SUMMARY_QUANTILES: &str = "0.05,0.25,0.5,0.75,0.95,1.0";
const SLOWEST_TASKS: usize = 100;
const FAILED_TASKS: usize = 50;
