# sparkwatch

A terminal UI for monitoring Apache Spark applications. Same data as the Spark
web UI, without the clicking: live tables of jobs, stages and executors, task
throughput over time, and a picker for hopping between applications.

Talks to the Spark REST API (`/api/v1`), so it works against a running driver,
the History Server, or — via `kubectl port-forward` — driver pods on
Kubernetes (Spark Operator or plain `spark-submit`).

## Install

Requires a Rust toolchain (1.85+, edition 2024).

```bash
cargo install --path .
```

That puts `sparkwatch` in `~/.cargo/bin`. For `--k8s` mode, `kubectl` must be
on your `PATH` and pointed at the right cluster.

## Usage

```
sparkwatch [OPTIONS] [TARGET]
```

| Option | Meaning |
|---|---|
| `TARGET` | Spark UI or History Server URL. With `--k8s`: the SparkApplication name. |
| `--k8s` | Find Spark driver pods with `kubectl` and port-forward to them. |
| `-n, --namespace <NS>` | Kubernetes namespace (default: the current kubectl context's). Requires `--k8s`. |
| `-a, --app <ID>` | Spark application id to watch; skips the picker on a History Server. |
| `-i, --interval <SECS>` | Poll interval (default `2`). |
| `-t, --timeout <SECS>` | HTTP timeout (default `5`). |

### Live driver

```bash
sparkwatch                          # http://localhost:4040
sparkwatch http://driver-host:4040
```

A driver reports exactly one application, so you land straight on the monitor.

### History Server

```bash
sparkwatch http://history-host:18080              # pick from the list
sparkwatch http://history-host:18080 -a app-20260923-0001
```

With several applications listed you get a picker; `Enter` watches the
highlighted one, `a` brings the picker back at any time.

### Kubernetes

```bash
sparkwatch --k8s -n spark             # pick a running driver pod
sparkwatch --k8s -n spark my-etl      # watch the SparkApplication "my-etl"
```

sparkwatch runs `kubectl get pods -l spark-role=driver` in the namespace,
lists the running drivers, and when you pick one spawns
`kubectl port-forward pod/<driver> 0:4040` on a free local port. The forward is
torn down when you switch apps or quit. No separate port-forward terminal
needed.

The name matches the operator's `sparkoperator.k8s.io/app-name` label, the
`spark-app-name` label set by `spark-submit`, or the driver pod name
(`my-etl-driver`), so any of those work as `TARGET`.

## Keys

| Key | Action |
|---|---|
| `Tab` `→` `l` / `Shift-Tab` `←` `h` | Next / previous tab |
| `1`–`4` | Jump to Overview / Jobs / Stages / Executors |
| `j` `k` `↓` `↑` | Move selection |
| `PgUp` `PgDn` | Move selection by 10 |
| `g` `G` `Home` `End` | First / last row |
| `Enter` | Picker: watch the application · Jobs: show only that job's stages · Stages: open the stage drill-down |
| `f` | Stage drill-down: switch between slowest and failed tasks |
| `a` | Open the application picker |
| `Esc` | Back: closes the drill-down, then the job filter, then the picker |
| `r` | Refresh now |
| `p` | Pause / resume polling |
| `+` `-` | Poll interval up / down (1–60 s) |
| `q` `Ctrl-C` | Quit |

## Tabs

- **Overview** — application info, cluster totals (cores, storage, shuffle,
  GC share flagged when over 10 %), sparklines of tasks/s and active tasks over
  the last few minutes, a progress gauge per running job.
- **Jobs** — status, task and stage counts, failures, progress.
- **Stages** — active stages first; task counts, input, shuffle read/write,
  spill (highlighted), progress.
- **Executors** — up/dead, host, running tasks vs cores, failed tasks, storage
  memory, GC share, shuffle read.

The header shows `LIVE`, `PAUSED` or `ERROR`; on an error the last good data
stays on screen and the message appears in the footer.

## Stage drill-down

`Enter` on a stage opens it. It refreshes at the poll interval like everything
else, so it is fine to leave open on a running stage.

- **Header** — status, pool, task counts, input/shuffle/spill totals, GC share,
  a progress gauge, and the `failureReason` if the stage failed.
- **Task metrics** — p5/p25/p50/p75/p95/max for duration, GC, scheduler
  delay, input, shuffle read/write, fetch wait, spill and peak memory, from
  Spark's own `taskSummary` quantiles (so it is exact even for 100k-task
  stages). A metric whose max is 3× or more its median is flagged `⚠ ×N` —
  that is your skewed partition.
- **Executors** — per-executor tasks, time, mean task time, shuffle read,
  spill. An executor is flagged red, with the reason, when it has failed
  tasks, was excluded for the stage, or its mean task time is 2× the stage
  mean — that is your bad node.
- **Tasks** — the 100 slowest tasks with stragglers (3× median) in yellow, or
  with `f` the failed tasks with their error message. A failed stage opens on
  its failures directly.

`Enter` on a job narrows the Stages tab to that job's stages; `Esc` clears it.

## Development

```bash
python3 dev/mock_spark.py &          # fake /api/v1 with two apps (picker)
python3 dev/mock_spark.py --single & # one app, straight to the monitor
cargo run
cargo test
```

The mock serves on `localhost:4040` and never needs a cluster.
