//! Kubernetes support: find Spark driver pods and reach their UI through
//! `kubectl port-forward`.
//!
//! Shelling out to `kubectl` rather than speaking the Kubernetes API keeps
//! the binary small and means whatever auth the user's kubeconfig uses
//! (OIDC, exec plugins, cloud CLIs) just works.

use crate::spark::{ApplicationInfo, Attempt};
use anyhow::{Context, Result};
use serde_json::Value;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::{Child, Command};

/// Port the Spark UI listens on inside the driver pod.
const SPARK_UI_PORT: u16 = 4040;

/// A running Spark driver pod.
#[derive(Debug, Clone)]
pub struct Driver {
    pub pod: String,
    /// The `SparkApplication` name (operator) or `spark-app-name` label.
    pub app_name: String,
    pub spark_version: String,
    pub started: String,
}

impl Driver {
    /// All the names a user might reasonably type to mean this driver.
    pub fn matches(&self, name: &str) -> bool {
        self.app_name == name
            || self.pod == name
            || self.pod.trim_end_matches("-driver") == name
            || self.pod.trim_end_matches("-server") == name
    }

    /// Present a driver like a History Server entry so the picker can show it.
    pub fn as_application(&self) -> ApplicationInfo {
        ApplicationInfo {
            id: self.app_name.clone(),
            name: self.pod.clone(),
            attempts: vec![Attempt {
                start_time: self.started.clone(),
                completed: false,
                app_spark_version: self.spark_version.clone(),
                ..Default::default()
            }],
        }
    }
}

fn kubectl(namespace: Option<&str>) -> Command {
    let mut cmd = Command::new("kubectl");
    if let Some(ns) = namespace {
        cmd.args(["-n", ns]);
    }
    cmd.stdin(Stdio::null());
    cmd
}

/// Running driver pods in the namespace, newest first.
pub async fn list_drivers(namespace: Option<&str>) -> Result<Vec<Driver>> {
    let out = kubectl(namespace)
        // A Spark Connect server run by the operator's SparkConnect resource
        // is a driver too, just labelled differently.
        .args([
            "get",
            "pods",
            "-l",
            "spark-role in (driver,connect-server)",
            "-o",
            "json",
        ])
        .output()
        .await
        .context("running kubectl (is it on PATH?)")?;
    if !out.status.success() {
        anyhow::bail!(
            "kubectl get pods: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    let json: Value = serde_json::from_slice(&out.stdout).context("parsing kubectl output")?;

    let mut drivers: Vec<Driver> = json["items"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|pod| pod["status"]["phase"] == "Running")
        .map(|pod| {
            let meta = &pod["metadata"];
            let labels = &meta["labels"];
            let pod_name = meta["name"].as_str().unwrap_or_default().to_string();
            let label = |k: &str| labels[k].as_str().map(str::to_string);
            Driver {
                // The operator's labels first (SparkApplication, then SparkConnect);
                // plain spark-submit only sets `spark-app-name`.
                app_name: label("sparkoperator.k8s.io/app-name")
                    .or_else(|| label("sparkoperator.k8s.io/connect-name"))
                    .or_else(|| label("spark-app-name"))
                    .unwrap_or_else(|| {
                        pod_name
                            .trim_end_matches("-driver")
                            .trim_end_matches("-server")
                            .to_string()
                    }),
                spark_version: label("spark-version").unwrap_or_default(),
                started: pod["status"]["startTime"]
                    .as_str()
                    .or(meta["creationTimestamp"].as_str())
                    .unwrap_or_default()
                    .to_string(),
                pod: pod_name,
            }
        })
        .collect();
    drivers.sort_by(|a, b| b.started.cmp(&a.started));
    Ok(drivers)
}

pub async fn find_driver(namespace: Option<&str>, name: &str) -> Result<Driver> {
    let drivers = list_drivers(namespace).await?;
    drivers
        .iter()
        .find(|d| d.matches(name))
        .cloned()
        .with_context(|| {
            let known: Vec<_> = drivers.iter().map(|d| d.app_name.as_str()).collect();
            format!(
                "no running driver for '{name}'{}",
                if known.is_empty() {
                    String::new()
                } else {
                    format!(" (running: {})", known.join(", "))
                }
            )
        })
}

/// The pod running executor `exec_id` of Spark application `spark_app_id`,
/// if it still exists. Spark labels executor pods with `spark-exec-id` and
/// `spark-app-selector` (= the Spark app id); the operator keeps those.
pub async fn find_executor_pod(
    namespace: Option<&str>,
    spark_app_id: &str,
    exec_id: &str,
) -> Result<Option<String>> {
    let selector =
        format!("spark-role=executor,spark-exec-id={exec_id},spark-app-selector={spark_app_id}");
    let out = kubectl(namespace)
        .args(["get", "pods", "-l", &selector, "-o", "json"])
        .output()
        .await
        .context("running kubectl (is it on PATH?)")?;
    if !out.status.success() {
        anyhow::bail!(
            "kubectl get pods: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    let json: Value = serde_json::from_slice(&out.stdout).context("parsing kubectl output")?;
    Ok(json["items"]
        .as_array()
        .and_then(|items| items.first())
        .and_then(|pod| pod["metadata"]["name"].as_str())
        .map(str::to_string))
}

/// Container names spark-submit gives the driver and executor containers,
/// used when a pod has sidecars and kubectl insists on `-c`.
pub const DRIVER_CONTAINER: &str = "spark-kubernetes-driver";
pub const EXECUTOR_CONTAINER: &str = "spark-kubernetes-executor";

/// A `kubectl logs -f` child process with its stdout as a line stream.
/// Killed when dropped.
pub struct LogStream {
    _child: Child,
    pub lines: tokio::io::Lines<BufReader<tokio::process::ChildStdout>>,
    stderr: Option<tokio::process::ChildStderr>,
}

impl LogStream {
    pub async fn start(
        namespace: Option<&str>,
        pod: &str,
        container: Option<&str>,
        previous: bool,
        tail: usize,
    ) -> Result<Self> {
        let mut cmd = kubectl(namespace);
        cmd.args(["logs", "-f", &format!("--tail={tail}")]);
        if previous {
            cmd.arg("--previous");
        }
        if let Some(c) = container {
            cmd.args(["-c", c]);
        }
        let mut child = cmd
            .arg(format!("pod/{pod}"))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .context("spawning kubectl logs (is kubectl on PATH?)")?;
        let stdout = child.stdout.take().expect("stdout piped");
        let stderr = child.stderr.take();
        Ok(Self {
            _child: child,
            lines: BufReader::new(stdout).lines(),
            stderr,
        })
    }

    /// After stdout closes: whatever kubectl complained about.
    pub async fn exit_reason(&mut self) -> String {
        let mut err = String::new();
        if let Some(mut e) = self.stderr.take() {
            e.read_to_string(&mut err).await.ok();
        }
        err.trim().to_string()
    }
}

/// kubectl's message when a pod has several containers and none was named.
pub fn needs_container_name(err: &str) -> bool {
    err.contains("a container name must be specified")
}

/// A `kubectl port-forward` child process. Killed when dropped.
pub struct PortForward {
    _child: Child,
    pub local_port: u16,
}

impl PortForward {
    /// Forward a free local port to the driver's UI and wait until kubectl
    /// reports it is listening.
    pub async fn to_driver(namespace: Option<&str>, pod: &str) -> Result<Self> {
        let mut child = kubectl(namespace)
            .args([
                "port-forward",
                &format!("pod/{pod}"),
                // Local port 0: let kubectl pick a free one and tell us.
                &format!("0:{SPARK_UI_PORT}"),
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .context("spawning kubectl port-forward (is kubectl on PATH?)")?;

        let stdout = child.stdout.take().expect("stdout piped");
        let mut stderr = child.stderr.take().expect("stderr piped");
        let mut lines = BufReader::new(stdout).lines();

        let wait_for_port = async {
            while let Some(line) = lines.next_line().await? {
                if let Some(port) = parse_forwarding_line(&line) {
                    return Ok(port);
                }
            }
            // stdout closed without the banner: kubectl gave up. Say why.
            let mut err = String::new();
            stderr.read_to_string(&mut err).await.ok();
            anyhow::bail!("kubectl port-forward to {pod} failed: {}", err.trim())
        };
        let local_port = tokio::time::timeout(Duration::from_secs(20), wait_for_port)
            .await
            .with_context(|| format!("timed out waiting for kubectl port-forward to {pod}"))??;

        // kubectl keeps chatting ("Handling connection for ...") for every
        // request we make; if nobody reads, its pipe fills and it stalls.
        tokio::spawn(async move { while let Ok(Some(_)) = lines.next_line().await {} });
        tokio::spawn(async move {
            let mut sink = Vec::new();
            let _ = stderr.read_to_end(&mut sink).await;
        });

        Ok(Self {
            _child: child,
            local_port,
        })
    }

    pub fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.local_port)
    }
}

/// `Forwarding from 127.0.0.1:54321 -> 4040` (kubectl prints one per address family).
fn parse_forwarding_line(line: &str) -> Option<u16> {
    let rest = line.strip_prefix("Forwarding from ")?;
    let (addr, _) = rest.split_once(" -> ")?;
    addr.rsplit_once(':')?.1.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_kubectl_forwarding_banner() {
        assert_eq!(
            parse_forwarding_line("Forwarding from 127.0.0.1:54321 -> 4040"),
            Some(54321)
        );
        assert_eq!(
            parse_forwarding_line("Forwarding from [::1]:54321 -> 4040"),
            Some(54321)
        );
        assert_eq!(parse_forwarding_line("Handling connection for 54321"), None);
    }

    #[test]
    fn driver_matches_the_names_people_type() {
        let d = Driver {
            pod: "my-etl-driver".into(),
            app_name: "my-etl".into(),
            spark_version: String::new(),
            started: String::new(),
        };
        assert!(d.matches("my-etl"));
        assert!(d.matches("my-etl-driver"));
        assert!(!d.matches("my-other-etl"));
        let server = Driver {
            pod: "spark-connect-server".into(),
            app_name: "spark-connect".into(),
            spark_version: String::new(),
            started: String::new(),
        };
        assert!(server.matches("spark-connect"));
        assert!(server.matches("spark-connect-server"));
    }
}
