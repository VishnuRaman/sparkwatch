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
    /// Pinned application id, or `None` to auto-pick the first one listed.
    app_id: Option<String>,
}

impl SparkClient {
    pub fn new(base: impl Into<String>, app_id: Option<String>, timeout: Duration) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .context("building HTTP client")?;
        Ok(Self {
            http,
            base: base.into().trim_end_matches('/').to_string(),
            app_id,
        })
    }

    async fn get<T: for<'de> Deserialize<'de>>(&self, path: &str) -> Result<T> {
        let url = format!("{}/api/v1{}", self.base, path);
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("GET {url} -> {status}: {}", body.chars().take(200).collect::<String>());
        }
        resp.json::<T>()
            .await
            .with_context(|| format!("decoding response from {url}"))
    }

    pub async fn applications(&self) -> Result<Vec<ApplicationInfo>> {
        self.get("/applications").await
    }

    /// Resolve which application to watch. On a live driver there is exactly
    /// one; on the History Server we take the pinned id or the newest entry.
    async fn resolve_app(&self) -> Result<ApplicationInfo> {
        let apps = self.applications().await?;
        match &self.app_id {
            Some(want) => apps
                .into_iter()
                .find(|a| &a.id == want)
                .with_context(|| format!("application {want} not found at {}", self.base)),
            None => apps
                .into_iter()
                .next()
                .context("no applications reported by this endpoint"),
        }
    }

    pub async fn poll(&self) -> Result<Snapshot> {
        let app = self.resolve_app().await?;
        let id = &app.id;

        let (jobs_path, stages_path, execs_path) = (
            format!("/applications/{id}/jobs"),
            format!("/applications/{id}/stages"),
            format!("/applications/{id}/allexecutors"),
        );

        // One round trip each, issued concurrently.
        let (jobs, stages, executors) = tokio::try_join!(
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
}
