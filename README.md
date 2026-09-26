# sparkwatch

A terminal UI for monitoring Apache Spark applications. Same data as the Spark
web UI, without the clicking: live tables of jobs, stages and executors, task
throughput over time, and a picker for hopping between applications.

Talks to the Spark REST API (`/api/v1`), so it works against a running driver,
the History Server, or — via `kubectl port-forward` — driver pods on
Kubernetes (Spark Operator or plain `spark-submit`).

## Install

Prebuilt binaries (macOS arm64/x86_64, Linux x86_64/arm64 as static musl,
Windows x86_64) are attached to every
[release](https://github.com/VishnuRaman/sparkwatch/releases). On macOS or
Linux:

```bash
curl -fsSL https://raw.githubusercontent.com/VishnuRaman/sparkwatch/main/install.sh | sh
```

That verifies the SHA256 and installs to `~/.local/bin` (or `/usr/local/bin`
as root); `SPARKWATCH_VERSION=v0.1.0` pins a version,
`SPARKWATCH_INSTALL_DIR` changes the location. On Windows, unzip the
`x86_64-pc-windows-msvc` asset somewhere on your `PATH`.

From source, with a Rust toolchain (1.88+):

```bash
cargo install --path .
```

For `--k8s` mode, `kubectl` must be on your `PATH` and pointed at the right
cluster. The binary is a single file with no other dependencies (TLS via
rustls, no OpenSSL).

### Releasing

CI (`.github/workflows/ci.yml`) runs `cargo fmt --check`, `clippy -D
warnings` and the tests on Linux, macOS and Windows for every push. To cut a
release, bump `version` in `Cargo.toml`, then tag it:

```bash
git tag v0.2.0 && git push origin v0.2.0
```

`.github/workflows/release.yml` refuses a tag that doesn't match
`Cargo.toml`, builds the five targets, and publishes a GitHub release with
the archives, `SHA256SUMS` and generated notes.

## Usage

```
sparkwatch [OPTIONS] [TARGET]
```

| Option | Meaning |
|---|---|
| `TARGET` | Spark UI or History Server URL, or a target name from the config file. With `--k8s`: the SparkApplication name. |
| `--k8s` | Find Spark driver pods with `kubectl` and port-forward to them. |
| `-n, --namespace <NS>` | Kubernetes namespace (default: the current kubectl context's). Requires `--k8s`. |
| `-a, --app <ID>` | Spark application id to watch; skips the picker on a History Server. |
| `-i, --interval <SECS>` | Poll interval (default `2`, or `[defaults].interval`). |
| `-t, --timeout <SECS>` | HTTP timeout (default `5`, or `[defaults].timeout`). |
| `--config <PATH>` | Config file (default `~/.config/sparkwatch.toml`). |
| `--targets` | List configured targets and exit. |

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

### Config file

Name your clusters once in `~/.config/sparkwatch.toml` (or
`$XDG_CONFIG_HOME/sparkwatch.toml`):

```toml
[defaults]
interval = 2
timeout = 5

[targets.prod]
k8s = true
namespace = "spark"
app = "my-etl"            # optional: skip the picker

[targets.history]
url = "http://history:18080"
app = "app-20260923-0001" # optional
```

Then `sparkwatch prod` or `sparkwatch history`. Flags on the command line
still win (`sparkwatch prod -n staging`), a URL that isn't a target name is
used as-is, and with `--k8s` the positional is always an app name. A
malformed config is an error, not silently ignored; `--targets` lists what
it found.

## Keys

| Key | Action |
|---|---|
| `Tab` `→` `l` / `Shift-Tab` `←` `h` | Next / previous tab |
| `1`–`8` | Jump to Overview / Jobs / Stages / Executors / SQL / Failures / Streaming / Storage |
| `/` | Filter the table on Jobs / Stages / Executors / SQL / Failures / Storage (typed in the footer; `Enter` applies) |
| `c` | Clear the table filter |
| `j` `k` `↓` `↑` | Move selection |
| `PgUp` `PgDn` | Move selection by 10 |
| `g` `G` `Home` `End` | First / last row |
| `Enter` | Picker: watch the application · Jobs: show only that job's stages · Stages: open the stage drill-down · SQL: open the query's plan · Failures: full error text |
| `x` | Acknowledge new failures (clears the red strip; the log is kept) |
| `s` | Failures: open the stage the selected failure belongs to, on its failed tasks |
| `L` | Logs of the selected executor (Executors tab), of the executor a failed task ran on (stage drill-down, filtered to the error), or of the executor an alert concerns (Failures) |
| `t` | Thread dump of the selected executor (Executors tab, or from the log view) |
| `f` | Stage drill-down: switch between slowest and failed tasks |
| `Tab` / `p` | SQL drill-down: switch scrolling between plan and nodes / plan-only view |
| `a` | Open the application picker |
| `Esc` | Back: closes a drill-down, then the table filter, then the job filter, then quits |
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
- **SQL** — one row per Spark SQL execution (a query, or a micro-batch of a
  streaming query), newest first: status, query text / call site, submitted,
  duration, job counts (`▶` running `✓` succeeded `✗` failed), error. On an app
  without `/sql` (RDD-only, or Spark < 3.0) the tab says so instead of erroring.
- **Failures** — everything that went wrong since sparkwatch started, newest
  first, and it stays even after Spark forgets it. See below.
- **Streaming** — Structured Streaming queries: batch durations, input vs
  processing rate, watermark lag, state size, with a `FALLING BEHIND` flag.
  See below.
- **Storage** — storage memory used vs available across executors (and which
  one is fullest), then every cached RDD / DataFrame: partitions cached vs
  total (yellow when partial — the rest gets recomputed), storage level,
  memory and disk. `Enter` shows how one RDD is spread across executors:
  partitions, bytes, share, and how full that executor's storage is. The
  History Server has no storage data.

### Filtering

`/` on any table tab opens a filter in the footer; type a substring and
`Enter`. It matches what you'd expect for the tab — job/stage name and
status, executor id/host/state/removal reason, query text and error, alert
title and detail, RDD name and level — case-insensitively, and the title
shows `Stages (12 of 340) · filter: writer`. Filters are per tab and stay
until `c` or `Esc`. On a streaming app with thousands of micro-batches, `/`
then `FAILED` on the SQL tab is the quickest way to the ones that matter.

Dead executors show their `removeReason` next to the host on the Executors
tab, executors the scheduler has excluded show `excl`, and failed stages carry
a `✗` on the Stages tab.

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

## Failures

The Spark UI only shows a failure where it happened, and only while the API
still retains it. sparkwatch derives an alert from every poll for:

- a stage in `FAILED` (with its `failureReason`),
- a job in `FAILED`,
- an executor that is gone with a `removeReason` (`Container killed…`,
  `OOMKilled…`, decommission),
- an executor the scheduler has excluded,
- a SQL execution in `FAILED` (with its error),
- every failed task, with its full error message — pulled from
  `taskList?status=failed` whenever a stage's failure count grows (at most 5
  stages × 20 tasks per poll, so a mass failure can't flood the driver).

Alerts are deduplicated and kept for the life of the process (last 1000).
While there are new ones a red strip sits under the header with the count
and the latest one; `x` acknowledges. The **Failures** tab lists them all;
`Enter` shows the full text (stack frames dimmed), `s` opens the stage the
failure belongs to, straight on its failed tasks.

## Streaming

Structured Streaming has no REST API (the `/streaming/*` endpoints are the
old DStreams), so the tab rebuilds what the web UI's Structured Streaming
page shows from two sources:

- **The driver log.** Every micro-batch is logged at INFO as
  `Streaming query made progress: {…}` with the full `StreamingQueryProgress`
  JSON. On the first visit to the tab sparkwatch starts following the driver
  log (a second `kubectl logs -f` under `--k8s`; the driver's `executorLogs`
  stderr page on YARN/standalone, re-read every 5 s) and keeps following for
  as long as the app is watched, so batches accumulate while you look at
  other tabs. The driver must log at INFO for
  `org.apache.spark.sql.execution.streaming`.
- **SQL executions.** Each micro-batch is also a SQL execution whose
  description carries the query id, run id and batch number. That gives
  batch ids, status and duration everywhere — including the History Server —
  with no log access, just not the rates or the watermark.

Per query: latest batch, trigger duration (with mean/p95/max over the last
300 batches and batches/min), input rows/s vs processed rows/s, watermark lag,
state rows and memory; sparklines of trigger duration, input vs processed
rate, and state size; the latest batch's `durationMs` breakdown (`addBatch`,
`getBatch`, `queryPlanning`, `walCommit`…), sources and sink. When
processing has been slower than input for most of the last five batches the
query is marked **FALLING BEHIND** and the tab title turns red. `j`/`k`
select between queries when there are several.

## Logs

`L` on an executor (the `driver` row too) opens its log, following the tail
as lines arrive. Scrolling up stops following; `F` (or scrolling back to the
end) resumes. `/` types a case-insensitive filter — `ERROR`, `OutOfMemory`,
a task id — applied to the whole buffer (last 20 000 lines); `c` clears it,
`w` toggles wrapping, `Esc` goes back to where you were. ERROR/exception
lines are red, WARN yellow.

Where the lines come from depends on how sparkwatch reached the app:

- **`--k8s`** — `kubectl logs -f --tail=2000` on the driver pod, or on the
  executor's pod (found by its `spark-exec-id` / `spark-app-selector`
  labels). Pods with sidecars are handled (it retries naming Spark's
  container). `P` switches to `--previous`, the container before the last
  restart — which is where the reason for a crash is; a dead executor opens
  on `--previous` straight away. If the executor's pod is gone the view says
  so: executor pods are deleted on exit unless the app sets
  `spark.kubernetes.executor.deleteOnTermination=false`, which is worth
  doing for anything long-running.
- **YARN / standalone** — the `executorLogs` URLs Spark reports (NodeManager
  or worker log pages), re-fetched every 3 s as a 256 KiB tail. `o` switches
  between `stderr` (Spark's own logging) and `stdout`.
- **History Server** — no logs are available; the view says so.

From a **failed task** in the stage drill-down, `L` opens the executor it
ran on with the filter pre-set to the exception name, so the OOM you just saw
in the task table is one key away from its context in the log. Same from a
task or executor alert on the Failures tab.

## Thread dump

`t` on an executor fetches a live thread dump (`/executors/{id}/threads`)
and groups it: **BLOCKED** threads first, with which thread holds the lock
they want, then threads **waiting on a lock**, then **RUNNABLE**, with the
ones inside Spark code (cyan frames) ahead of idle pool threads. Eight
frames per thread; `e` expands to all. `/` filters by thread name or frame
(`HashAggregate`, `BlockManager`…), `r` refreshes, `L` jumps to that
executor's log. Not available through the History Server.

## SQL drill-down

`Enter` on a SQL execution opens it: status, submission time, duration, the
job ids it ran (running / succeeded / failed) and the error if it failed.
Below, side by side (stacked on a narrow terminal):

- **Physical plan** — the `planDescription` Spark reports, operators
  highlighted, scrollable with `j`/`k`/`PgUp`/`PgDn`/`g`/`G`. `p` gives it the
  whole screen.
- **Nodes** — every plan node with its three most telling metrics (output
  rows, spill, peak memory, time, bytes…), spill in magenta when non-zero.
  `Tab` moves scrolling focus between the two panes.

The list is fetched incrementally: Spark's `/sql` endpoint is oldest-first
with no sort, and a streaming app retains up to 1000 micro-batches, so
sparkwatch asks only for what is new or recent each poll and keeps the last
500 in memory. A query evicted by the driver since you listed it says so when
opened.

## Development

```bash
python3 dev/mock_spark.py &          # fake /api/v1 with two apps (picker)
python3 dev/mock_spark.py --single & # one app, straight to the monitor
cargo run
cargo test
```

The mock serves on `localhost:4040` and never needs a cluster.

Headless smoke test (no tmux needed): give the binary a pty with `script`,
feed it keystrokes, then replay the capture through a terminal emulator and
assert on the reconstructed screens:

```bash
pip install pyte
(sleep 3; printf '6'; sleep 1; printf 'q') | script -q out.txt sh -c 'stty cols 170 rows 50; ./target/debug/sparkwatch'
python3 dev/screens.py out.txt 170 50 'Failures (11, 11 new)' '!ERROR'   # '!' = must be absent
```

Give the app a few seconds before the first key — until the first poll lands
it is on the picker / "Connecting" screen, where tab keys do nothing.
`SPARKWATCH_KEYLOG=/path` appends every key press it receives, with a
timestamp, which tells "key ignored" from "key never arrived" apart.
