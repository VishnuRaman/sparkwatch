//! Background task that owns all network I/O, so the render loop never blocks
//! on a slow History Server or a `kubectl port-forward` handshake.
//!
//! The UI talks to it with [`Request`]s and gets [`Message`]s back. The poller
//! is sequential: one fetch per cycle, then sleep until the interval elapses
//! or a request wakes it early.

use crate::k8s::{self, PortForward};
use crate::spark::{ApplicationInfo, Snapshot, SparkClient, StageDetail};
use anyhow::{Context, Result};
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
    /// Tear down (kills any port-forward) and exit.
    Shutdown,
}

/// A drill-down the UI has open, refreshed at the poll interval alongside
/// the snapshot so it stays live.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Detail {
    Stage { id: i64, attempt: i64 },
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
}

#[derive(Debug)]
pub enum DetailData {
    Stage(StageDetail),
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
    _forward: PortForward,
    client: SparkClient,
    /// The driver reports its own Spark application id; that is what the
    /// REST paths need, not the SparkApplication name.
    spark_app_id: String,
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

pub fn spawn(mut source: Source, watch: Option<String>, interval: Duration) -> Handle {
    let (req_tx, mut req_rx) = mpsc::channel::<Request>(16);
    let (msg_tx, msg_rx) = mpsc::channel::<Message>(16);

    let task = tokio::spawn(async move {
        let mut period = interval;
        let mut watch = watch;
        let mut detail: Option<Detail> = None;

        loop {
            match &watch {
                Some(key) => {
                    let msgs = fetch(&mut source, key, detail).await;
                    for msg in msgs {
                        if msg_tx.send(msg).await.is_err() {
                            return; // UI is gone
                        }
                    }
                }
                None => {
                    let msg = Message::Apps(source.list().await.map_err(|e| format!("{e:#}")));
                    if msg_tx.send(msg).await.is_err() {
                        return;
                    }
                }
            }

            let sleep_for = if watch.is_none() {
                period.max(LIST_APPS_MIN_INTERVAL)
            } else {
                period
            };

            tokio::select! {
                _ = tokio::time::sleep(sleep_for) => {}
                first = req_rx.recv() => {
                    let Some(first) = first else { break };
                    let mut stop = apply(first, &mut source, &mut period, &mut watch, &mut detail);
                    // Requests often arrive in bursts (interval change + refresh);
                    // apply them all before the next fetch.
                    while let Ok(more) = req_rx.try_recv() {
                        stop |= apply(more, &mut source, &mut period, &mut watch, &mut detail);
                    }
                    if stop {
                        break;
                    }
                }
            }
        }
        // `source` drops here, which kills any kubectl port-forward.
    });

    Handle {
        req_tx,
        msg_rx,
        task,
    }
}

/// One cycle for a watched app: the snapshot, plus the open detail view if
/// any, fetched concurrently.
async fn fetch(source: &mut Source, key: &str, detail: Option<Detail>) -> Vec<Message> {
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
            None => None,
        }
    };
    let (snapshot, detail) = tokio::join!(snapshot, detail_fut);

    if snapshot.is_err() {
        source.poll_failed();
    }
    let mut msgs = vec![Message::Snapshot {
        app_id: key.to_string(),
        result: snapshot.map_err(err),
    }];
    if let Some((detail, result)) = detail {
        msgs.push(Message::Detail { detail, result });
    }
    msgs
}

/// Returns true when the poller should exit.
fn apply(
    req: Request,
    source: &mut Source,
    period: &mut Duration,
    watch: &mut Option<String>,
    detail: &mut Option<Detail>,
) -> bool {
    match req {
        Request::SetInterval(d) => *period = d,
        Request::RefreshNow => {}
        Request::WatchApp(id) => {
            if watch.as_deref() != Some(id.as_str()) {
                *detail = None;
            }
            *watch = Some(id);
        }
        Request::ListApps => {
            *watch = None;
            *detail = None;
            source.unwatch();
        }
        Request::SetDetail(d) => *detail = d,
        Request::Shutdown => return true,
    }
    false
}
