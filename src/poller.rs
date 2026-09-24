//! Background task that owns all network I/O, so the render loop never blocks
//! on a slow History Server or a `kubectl port-forward` handshake.
//!
//! The UI talks to it with [`Request`]s and gets [`Message`]s back. The poller
//! is sequential: one fetch per cycle, then sleep until the interval elapses
//! or a request wakes it early.

use crate::k8s::{self, LogStream, PortForward};
use crate::logview::LogTarget;
use crate::streaming::{Progress, ProgressParser};
use crate::spark::{logs, ApplicationInfo, ExecutionData, Snapshot, SparkClient, StageDetail, ThreadStackTrace};
use anyhow::{Context, Result};
use std::collections::{BTreeMap, HashMap};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// UI → poller.
#[derive(Debug)]
pub enum Request {
    SetInterval(Duration),
    /// Wake up and fetch now.
    RefreshNow,
    /// Start polling this application (a Spark app id, or with `--k8s` the
    /// SparkApplication name).
    WatchApp(String),
    /// Stop polling an application and list applications instead.
    ListApps,
    /// Also fetch this detail view on every cycle (`None` to stop).
    SetDetail(Option<Detail>),
    /// Start streaming an executor's logs (replaces any open stream).
    OpenLogs(LogTarget),
    CloseLogs,
    /// One-shot thread dump.
    FetchThreads(String),
    /// Start following the driver log for streaming progress (idempotent).
    TapProgress,
    /// Tear down (kills any port-forward) and exit.
    Shutdown,
}

/// A drill-down the UI has open, refreshed at the poll interval alongside
/// the snapshot so it stays live.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Detail {
    Stage { id: i64, attempt: i64 },
    Sql(i64),
}

/// Poller → UI.
#[derive(Debug)]
pub enum Message {
    Apps(Result<Vec<ApplicationInfo>, String>),
    /// Tagged with the app id so the UI can drop a snapshot that arrives after
    /// the user has already switched away.
    Snapshot {
        app_id: String,
        result: Result<Snapshot, String>,
    },
    Detail {
        detail: Detail,
        result: Result<DetailData, String>,
    },
    Log(LogEvent),
    Threads {
        executor_id: String,
        result: Result<Option<Vec<ThreadStackTrace>>, String>,
    },
    /// A micro-batch's progress from the driver log tap.
    Progress(Progress),
    /// What the tap is doing, or why it can't.
    ProgressStatus(String),
}

#[derive(Debug)]
pub enum LogEvent {
    /// New lines from a stream (kubectl).
    Lines(Vec<String>),
    /// A fresh tail from a page fetch (HTTP): replaces the buffer.
    Replace(Vec<String>),
    /// What the source is doing, or why it stopped.
    Status(String),
}

#[derive(Debug)]
pub enum DetailData {
    Stage(StageDetail),
    /// `None`: the driver no longer retains this execution.
    Sql(Option<ExecutionData>),
}

/// Per-app poller state that survives between cycles.
#[derive(Default)]
struct AppState {
    sql: SqlCache,
    /// The driver's stderr `executorLogs` URL (YARN/standalone), for the tap.
    driver_log_url: Option<String>,
    /// Failed-task count already fetched per stage attempt, so the error
    /// messages are pulled only when a stage gains new failures.
    failed_seen: HashMap<(i64, i64), i64>,
}

/// Stages whose failure count grew, capped so a mass failure doesn't turn
/// one poll into a hundred requests.
const FAILED_STAGES_PER_CYCLE: usize = 5;
const FAILED_TASKS_PER_STAGE: usize = 20;

/// Pull error messages for stages with new failures. Errors are skipped:
/// the count in the memo stays put and the stage is retried next cycle.
async fn fetch_failed_tasks(
    client: &SparkClient,
    spark_id: &str,
    snapshot: &Snapshot,
    state: &mut AppState,
) -> Vec<((i64, i64), Vec<crate::spark::TaskData>)> {
    let mut candidates: Vec<_> = snapshot
        .stages
        .iter()
        .filter(|st| st.num_failed_tasks > state.failed_seen.get(&st.key()).copied().unwrap_or(0))
        .collect();
    candidates.sort_by_key(|st| -st.stage_id);
    candidates.truncate(FAILED_STAGES_PER_CYCLE);

    let fetches = candidates.iter().map(|st| async move {
        let tasks = client
            .failed_tasks(spark_id, st.stage_id, st.attempt_id, FAILED_TASKS_PER_STAGE)
            .await;
        (st.key(), st.num_failed_tasks, tasks)
    });
    let mut out = Vec::new();
    for (key, count, tasks) in futures::future::join_all(fetches).await {
        if let Ok(tasks) = tasks {
            state.failed_seen.insert(key, count);
            out.push((key, tasks));
        }
    }
    out
}

/// SQL executions seen so far for the watched app. `sql_tail` only fetches
/// what is new or recent; this holds the rest so the tab shows a stable list.
#[derive(Default)]
struct SqlCache {
    by_id: BTreeMap<i64, ExecutionData>,
    /// `None` until the first fetch; `Some(false)` when there is no `/sql`.
    available: Option<bool>,
}

/// More than this and the tab is a scroll of ancient micro-batches anyway.
const SQL_KEEP: usize = 500;

impl SqlCache {
    fn max_id(&self) -> Option<i64> {
        self.by_id.keys().next_back().copied()
    }

    fn merge(&mut self, tail: Option<Vec<ExecutionData>>) {
        match tail {
            None => {
                self.available = Some(false);
                self.by_id.clear();
            }
            Some(execs) => {
                self.available = Some(true);
                for e in execs {
                    self.by_id.insert(e.id, e);
                }
                while self.by_id.len() > SQL_KEEP {
                    self.by_id.pop_first();
                }
            }
        }
    }

    /// Newest first, or `None` when the endpoint has no `/sql`.
    fn view(&self) -> Option<Vec<ExecutionData>> {
        match self.available {
            Some(true) => Some(self.by_id.values().rev().cloned().collect()),
            _ => None,
        }
    }
}

/// Where applications come from and how to reach one.
pub enum Source {
    /// A URL: live driver UI or History Server.
    Http(SparkClient),
    /// Driver pods in a namespace, reached via `kubectl port-forward`.
    Kube {
        namespace: Option<String>,
        timeout: Duration,
        /// The forward + client for the driver currently being watched.
        conn: Option<KubeConn>,
    },
}

pub struct KubeConn {
    app_name: String,
    driver_pod: String,
    _forward: PortForward,
    client: SparkClient,
    /// The driver reports its own Spark application id; that is what the
    /// REST paths need, not the SparkApplication name.
    spark_app_id: String,
}

/// What a log stream needs to know about where the app runs.
enum LogSource {
    Kube {
        namespace: Option<String>,
        spark_app_id: String,
        driver_pod: String,
    },
    Http(SparkClient),
}

impl Source {
    async fn list(&self) -> Result<Vec<ApplicationInfo>> {
        match self {
            Source::Http(client) => client.applications().await,
            Source::Kube { namespace, .. } => Ok(k8s::list_drivers(namespace.as_deref())
                .await?
                .iter()
                .map(k8s::Driver::as_application)
                .collect()),
        }
    }

    /// A client that can reach the watched application, and the Spark app id
    /// to use in REST paths. Cheap to call: `SparkClient` is an `Arc` inside.
    async fn resolve(&mut self, key: &str) -> Result<(SparkClient, String)> {
        match self {
            Source::Http(client) => Ok((client.clone(), key.to_string())),
            Source::Kube {
                namespace,
                timeout,
                conn,
            } => {
                if conn.as_ref().is_none_or(|c| c.app_name != key) {
                    *conn = None; // drop the old forward before opening another
                    *conn = Some(connect(namespace.as_deref(), key, *timeout).await?);
                }
                let c = conn.as_ref().expect("connected above");
                Ok((c.client.clone(), c.spark_app_id.clone()))
            }
        }
    }

    fn log_source(&self) -> Result<LogSource> {
        match self {
            Source::Http(client) => Ok(LogSource::Http(client.clone())),
            Source::Kube { namespace, conn, .. } => {
                let c = conn.as_ref().context("not connected to a driver yet")?;
                Ok(LogSource::Kube {
                    namespace: namespace.clone(),
                    spark_app_id: c.spark_app_id.clone(),
                    driver_pod: c.driver_pod.clone(),
                })
            }
        }
    }

    /// A dead forward (pod gone, kubectl exited) shows up as a connection
    /// error; drop it so the next cycle reconnects.
    fn poll_failed(&mut self) {
        self.unwatch();
    }

    fn unwatch(&mut self) {
        if let Source::Kube { conn, .. } = self {
            *conn = None;
        }
    }
}

async fn connect(namespace: Option<&str>, app_name: &str, timeout: Duration) -> Result<KubeConn> {
    let driver = k8s::find_driver(namespace, app_name).await?;
    let forward = PortForward::to_driver(namespace, &driver.pod).await?;
    let client = SparkClient::new(forward.base_url(), timeout)?;
    let spark_app_id = client
        .applications()
        .await
        .with_context(|| format!("talking to the Spark UI in {}", driver.pod))?
        .into_iter()
        .next()
        .with_context(|| format!("{} reports no applications yet", driver.pod))?
        .id;
    Ok(KubeConn {
        app_name: app_name.to_string(),
        driver_pod: driver.pod,
        _forward: forward,
        client,
        spark_app_id,
    })
}

/// Listing applications is comparatively heavy (History Server scan or a
/// kubectl round trip) and the list changes slowly, so never hammer it.
const LIST_APPS_MIN_INTERVAL: Duration = Duration::from_secs(10);

pub struct Handle {
    pub req_tx: mpsc::Sender<Request>,
    pub msg_rx: mpsc::Receiver<Message>,
    pub task: JoinHandle<()>,
}

pub fn spawn(source: Source, watch: Option<String>, interval: Duration) -> Handle {
    let (req_tx, mut req_rx) = mpsc::channel::<Request>(16);
    let (msg_tx, msg_rx) = mpsc::channel::<Message>(64);

    let task = tokio::spawn(async move {
        let mut p = Poller {
            source,
            period: interval,
            watch,
            detail: None,
            state: AppState::default(),
            logs: None,
            tap: None,
            msg_tx,
        };

        loop {
            match p.watch.clone() {
                Some(key) => {
                    let msgs = fetch(&mut p.source, &key, p.detail, &mut p.state).await;
                    for msg in msgs {
                        if p.msg_tx.send(msg).await.is_err() {
                            return; // UI is gone
                        }
                    }
                }
                None => {
                    let msg = Message::Apps(p.source.list().await.map_err(|e| format!("{e:#}")));
                    if p.msg_tx.send(msg).await.is_err() {
                        return;
                    }
                }
            }

            let sleep_for = if p.watch.is_none() {
                p.period.max(LIST_APPS_MIN_INTERVAL)
            } else {
                p.period
            };

            tokio::select! {
                _ = tokio::time::sleep(sleep_for) => {}
                first = req_rx.recv() => {
                    let Some(first) = first else { break };
                    let mut stop = p.apply(first).await;
                    // Requests often arrive in bursts (interval change + refresh);
                    // apply them all before the next fetch.
                    while let Ok(more) = req_rx.try_recv() {
                        stop |= p.apply(more).await;
                    }
                    if stop {
                        break;
                    }
                }
            }
        }
        // `p` drops here: the port-forward and any log stream are killed.
    });

    Handle {
        req_tx,
        msg_rx,
        task,
    }
}

struct Poller {
    source: Source,
    period: Duration,
    watch: Option<String>,
    detail: Option<Detail>,
    state: AppState,
    /// The running log stream task, if a log view is open.
    logs: Option<JoinHandle<()>>,
    /// The driver log tap feeding the Streaming tab, for the app's lifetime.
    tap: Option<JoinHandle<()>>,
    msg_tx: mpsc::Sender<Message>,
}

impl Drop for Poller {
    fn drop(&mut self) {
        self.close_logs();
        self.close_tap();
    }
}

impl Poller {
    fn close_logs(&mut self) {
        if let Some(h) = self.logs.take() {
            h.abort(); // drops the kubectl child (kill_on_drop)
        }
    }

    fn close_tap(&mut self) {
        if let Some(h) = self.tap.take() {
            h.abort();
        }
    }

    /// Returns true when the poller should exit.
    async fn apply(&mut self, req: Request) -> bool {
        match req {
            Request::SetInterval(d) => self.period = d,
            Request::RefreshNow => {}
            Request::WatchApp(id) => {
                if self.watch.as_deref() != Some(id.as_str()) {
                    self.detail = None;
                    self.state = AppState::default();
                    self.close_logs();
                    self.close_tap();
                }
                self.watch = Some(id);
            }
            Request::ListApps => {
                self.watch = None;
                self.detail = None;
                self.state = AppState::default();
                self.close_logs();
                self.close_tap();
                self.source.unwatch();
            }
            Request::SetDetail(d) => self.detail = d,
            Request::OpenLogs(target) => {
                self.close_logs();
                let tx = self.msg_tx.clone();
                match self.source.log_source() {
                    Ok(src) => self.logs = Some(tokio::spawn(run_logs(src, target, tx))),
                    Err(e) => {
                        let _ = tx.send(Message::Log(LogEvent::Status(format!("{e:#}")))).await;
                    }
                }
            }
            Request::CloseLogs => self.close_logs(),
            Request::TapProgress => {
                if self.tap.as_ref().is_some_and(|h| !h.is_finished()) {
                    return false; // already tapping
                }
                let tx = self.msg_tx.clone();
                // The driver's log URL comes from the snapshot's `driver` row
                // in HTTP mode; the UI passes it along in the target it built.
                match self.source.log_source() {
                    Ok(src) => {
                        let driver_url = self.state.driver_log_url.clone();
                        self.tap = Some(tokio::spawn(run_tap(src, driver_url, tx)));
                    }
                    Err(e) => {
                        let _ = tx.send(Message::ProgressStatus(format!("{e:#}"))).await;
                    }
                }
            }
            Request::FetchThreads(executor_id) => {
                let result = match &self.watch {
                    Some(key) => match self.source.resolve(key).await {
                        Ok((client, spark_id)) => client
                            .threads(&spark_id, &executor_id)
                            .await
                            .map_err(|e| format!("{e:#}")),
                        Err(e) => Err(format!("{e:#}")),
                    },
                    None => Err("no application is being watched".into()),
                };
                let _ = self.msg_tx.send(Message::Threads { executor_id, result }).await;
            }
            Request::Shutdown => return true,
        }
        false
    }
}

// -------------------------------------------------------------- log streams

/// Lines a kubectl stream keeps before the viewer gets its first batch.
const LOG_TAIL: usize = 2000;
/// Batch lines so the UI wakes ~10×/s at most, not once per line.
const LOG_BATCH_EVERY: Duration = Duration::from_millis(100);
const LOG_BATCH_MAX: usize = 500;
/// HTTP sources have no follow; re-fetch the tail this often.
const HTTP_LOG_REFRESH: Duration = Duration::from_secs(3);

async fn run_logs(src: LogSource, target: LogTarget, tx: mpsc::Sender<Message>) {
    let status = |s: String| {
        let tx = tx.clone();
        async move {
            let _ = tx.send(Message::Log(LogEvent::Status(s))).await;
        }
    };
    match src {
        LogSource::Kube {
            namespace,
            spark_app_id,
            driver_pod,
        } => {
            let ns = namespace.as_deref();
            let (pod, default_container) = if target.executor_id == "driver" {
                (driver_pod, k8s::DRIVER_CONTAINER)
            } else {
                match k8s::find_executor_pod(ns, &spark_app_id, &target.executor_id).await {
                    Ok(Some(p)) => (p, k8s::EXECUTOR_CONTAINER),
                    Ok(None) => {
                        return status(
                            "pod gone — set spark.kubernetes.executor.deleteOnTermination=false to keep executor logs"
                                .into(),
                        )
                        .await;
                    }
                    Err(e) => return status(format!("{e:#}")).await,
                }
            };
            // First without -c; a pod with sidecars makes kubectl refuse,
            // and then we name Spark's container.
            let mut container: Option<&str> = None;
            loop {
                let mut stream = match LogStream::start(ns, &pod, container, target.previous, LOG_TAIL).await {
                    Ok(s) => s,
                    Err(e) => return status(format!("{e:#}")).await,
                };
                status(format!(
                    "streaming pod/{pod}{}{}",
                    container.map(|c| format!(" -c {c}")).unwrap_or_default(),
                    if target.previous { " --previous" } else { "" }
                ))
                .await;
                let ended = pump(&mut stream, &tx).await;
                let reason = stream.exit_reason().await;
                if ended && container.is_none() && k8s::needs_container_name(&reason) {
                    container = Some(default_container);
                    continue;
                }
                let why = if reason.is_empty() { "stream ended".to_string() } else { reason };
                return status(format!("kubectl logs ended: {why}")).await;
            }
        }
        LogSource::Http(client) => {
            let Some(url) = &target.http_url else {
                return status(
                    "no log URLs reported by this executor (on Kubernetes, run with --k8s to stream pod logs)".into(),
                )
                .await;
            };
            let url = logs::with_tail(url);
            loop {
                match client.fetch_text(&url).await {
                    Ok(body) => {
                        let lines: Vec<String> = logs::extract_log_text(&body).lines().map(str::to_string).collect();
                        if tx.send(Message::Log(LogEvent::Replace(lines))).await.is_err() {
                            return;
                        }
                        status(format!("tail of {url} · refreshed every {}s", HTTP_LOG_REFRESH.as_secs())).await;
                    }
                    Err(e) => status(format!("fetch failed: {e:#}")).await,
                }
                tokio::time::sleep(HTTP_LOG_REFRESH).await;
            }
        }
    }
}

/// How far back the tap reads on start: enough for a few hundred batches
/// of pretty-printed progress.
const TAP_TAIL: usize = 10_000;
const TAP_HTTP_REFRESH: Duration = Duration::from_secs(5);

/// Follow the driver log and forward only parsed progress events.
async fn run_tap(src: LogSource, driver_url: Option<String>, tx: mpsc::Sender<Message>) {
    let status = |s: String| {
        let tx = tx.clone();
        async move {
            let _ = tx.send(Message::ProgressStatus(s)).await;
        }
    };
    let mut parser = ProgressParser::new();
    match src {
        LogSource::Kube {
            namespace,
            driver_pod,
            ..
        } => {
            let ns = namespace.as_deref();
            let mut container: Option<&str> = None;
            loop {
                let mut stream = match LogStream::start(ns, &driver_pod, container, false, TAP_TAIL).await {
                    Ok(s) => s,
                    Err(e) => return status(format!("{e:#}")).await,
                };
                status(format!("following pod/{driver_pod}")).await;
                loop {
                    match stream.lines.next_line().await {
                        Ok(Some(line)) => {
                            if let Some(p) = parser.feed(&line) {
                                if tx.send(Message::Progress(p)).await.is_err() {
                                    return;
                                }
                            }
                        }
                        _ => break,
                    }
                }
                let reason = stream.exit_reason().await;
                if container.is_none() && k8s::needs_container_name(&reason) {
                    container = Some(k8s::DRIVER_CONTAINER);
                    continue;
                }
                return status(format!("driver log ended: {reason}")).await;
            }
        }
        LogSource::Http(client) => {
            let Some(url) = driver_url else {
                return status("no driver log URL (History Server?) — batch durations from SQL executions only".into()).await;
            };
            let url = logs::with_tail(&url);
            loop {
                match client.fetch_text(&url).await {
                    Ok(body) => {
                        let mut n = 0;
                        for line in logs::extract_log_text(&body).lines() {
                            if let Some(p) = parser.feed(line) {
                                n += 1;
                                if tx.send(Message::Progress(p)).await.is_err() {
                                    return;
                                }
                            }
                        }
                        status(format!(
                            "driver stderr page · {n} progress events in the tail · refreshed every {}s",
                            TAP_HTTP_REFRESH.as_secs()
                        ))
                        .await;
                    }
                    Err(e) => status(format!("fetch failed: {e:#}")).await,
                }
                tokio::time::sleep(TAP_HTTP_REFRESH).await;
            }
        }
    }
}

/// Forward lines in batches until the stream ends. Returns true on a clean
/// EOF (as opposed to the UI having gone away).
async fn pump(stream: &mut LogStream, tx: &mpsc::Sender<Message>) -> bool {
    let mut batch: Vec<String> = Vec::new();
    loop {
        let (eof, timer) = match tokio::time::timeout(LOG_BATCH_EVERY, stream.lines.next_line()).await {
            Ok(Ok(Some(line))) => {
                batch.push(line);
                (false, false)
            }
            Ok(Ok(None)) | Ok(Err(_)) => (true, false),
            Err(_) => (false, true), // batch timer fired
        };
        if !batch.is_empty() && (eof || timer || batch.len() >= LOG_BATCH_MAX) {
            if tx.send(Message::Log(LogEvent::Lines(std::mem::take(&mut batch)))).await.is_err() {
                return false;
            }
        }
        if eof {
            return true;
        }
    }
}

/// One cycle for a watched app: the snapshot, plus the open detail view if
/// any, fetched concurrently.
async fn fetch(source: &mut Source, key: &str, detail: Option<Detail>, state: &mut AppState) -> Vec<Message> {
    let err = |e: anyhow::Error| format!("{e:#}");
    let (client, spark_id) = match source.resolve(key).await {
        Ok(c) => c,
        Err(e) => {
            source.poll_failed();
            return vec![Message::Snapshot {
                app_id: key.to_string(),
                result: Err(err(e)),
            }];
        }
    };

    let snapshot = client.poll(&spark_id);
    let sql_tail = client.sql_tail(&spark_id, state.sql.max_id());
    let detail_fut = async {
        match detail {
            Some(d @ Detail::Stage { id, attempt }) => Some((
                d,
                client
                    .stage_detail(&spark_id, id, attempt)
                    .await
                    .map(DetailData::Stage)
                    .map_err(err),
            )),
            Some(d @ Detail::Sql(id)) => Some((
                d,
                client.sql_detail(&spark_id, id).await.map(DetailData::Sql).map_err(err),
            )),
            None => None,
        }
    };
    let (snapshot, sql_tail, detail) = tokio::join!(snapshot, sql_tail, detail_fut);

    if snapshot.is_err() {
        source.poll_failed();
    }
    // A failed SQL fetch keeps the cached list rather than failing the whole
    // snapshot: jobs/stages/executors are still good and more important.
    if let Ok(tail) = sql_tail {
        state.sql.merge(tail);
    }
    let snapshot = match snapshot {
        Ok(mut s) => {
            s.sql = state.sql.view();
            state.driver_log_url = s
                .executors
                .iter()
                .find(|e| e.id == "driver")
                .and_then(|e| e.log_url("stderr"));
            // Needs the stage list, so it runs after the snapshot, not with it.
            s.failed_tasks = fetch_failed_tasks(&client, &spark_id, &s, state).await;
            Ok(s)
        }
        Err(e) => Err(e),
    };
    let mut msgs = vec![Message::Snapshot {
        app_id: key.to_string(),
        result: snapshot.map_err(err),
    }];
    if let Some((detail, result)) = detail {
        msgs.push(Message::Detail { detail, result });
    }
    msgs
}
