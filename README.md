# sparkwatch

A terminal UI for monitoring Apache Spark applications — live driver,
History Server, or driver pods on Kubernetes — with the things the Spark web
UI makes you dig for brought to the front:

- **Failures that don't scroll away.** Every failed task, stage, job, SQL
  execution and lost executor is kept for the life of the process, with its
  full error, and one key opens the executor's log filtered to it.
- **Skew and stragglers flagged for you.** The stage drill-down marks a task
  metric whose max is 3× its median, the executor that is 2× slower than the
  rest, and the tasks that dragged.
- **Structured Streaming without clicking per batch.** Trigger duration,
  input vs processed rate, watermark lag and state size per query, every
  batch in a table, a `FALLING BEHIND` flag, and a drill-down from a batch to
  its stages, failures and log window.
- **A summary for an app you weren't watching**, and a bundle (`summary.md`
  + JSON + log tails) to attach to a ticket.

<!-- screenshot: docs/sparkwatch.png (Overview or Streaming tab) -->

Everything comes from the Spark REST API (`/api/v1`) and the driver log;
nothing is installed on the cluster.

## Quick start

```bash
curl -fsSL https://raw.githubusercontent.com/VishnuRaman/sparkwatch/main/install.sh | sh
sparkwatch http://localhost:4040        # a running driver
sparkwatch --k8s -n spark               # Kubernetes: pick a driver pod
```

Then `1`–`9` switch tabs, `Enter` drills in, `Esc` backs out, `S` shows the
summary, `q` quits. [CHEATSHEET.md](CHEATSHEET.md) is the one-page version of
everything below.

## Compatibility

Any Spark 3.0 or newer, on YARN, standalone or Kubernetes; Spark 4.x
included. Version differences are handled (`isBlacklisted`/`isExcluded`,
thread-dump encodings, the missing SQL error text on Spark ≤ 4.0 — the
failed stage's reason is used instead). What a source can't provide:

| | Live driver | History Server |
|---|---|---|
| Jobs, stages, executors, SQL, environment, summary, bundle | ✓ | ✓ |
| Streaming batch ids, status, durations | ✓ | ✓ |
| Streaming rates, watermark, state (from the driver log) | ✓ | – |
| Logs, thread dumps, storage | ✓ | – |

The demo job needs a Spark Connect server (3.4+), but that's only the demo.

## Install

Prebuilt binaries (macOS arm64/x86_64, Linux x86_64/arm64 as static musl,
Windows x86_64) are attached to every
[release](https://github.com/VishnuRaman/sparkwatch/releases). The install
script above verifies the SHA256 and installs to `~/.local/bin` (or
`/usr/local/bin` as root); `SPARKWATCH_VERSION=v0.1.0` pins a version,
`SPARKWATCH_INSTALL_DIR` changes the location. On Windows, unzip the
`x86_64-pc-windows-msvc` asset somewhere on your `PATH`.

From source, with a Rust toolchain (1.88+):

```bash
cargo install --path .
```

The binary is a single file with no other dependencies (TLS via rustls, no
OpenSSL). `--k8s` mode shells out to `kubectl`, which must be on your `PATH`.

## Usage

```
sparkwatch [OPTIONS] [TARGET]
```

| Option | Meaning |
|---|---|
| `TARGET` | Spark UI or History Server URL, or a target name from the config file. With `--k8s`: the SparkApplication name. |
| `--k8s` | Find Spark driver pods with `kubectl` and port-forward to them. |
| `-n, --namespace <NS>` | Kubernetes namespace (default: the current kubectl context's). |
| `--context <NAME>` | kubeconfig context to use with `--k8s` (default: the current one). |
| `-a, --app <ID>` | Spark application id to watch; skips the picker on a History Server. |
| `-i, --interval <SECS>` | Poll interval (default `2`, or `[defaults].interval`). |
| `-t, --timeout <SECS>` | HTTP timeout (default `5`, or `[defaults].timeout`). |
| `--config <PATH>` | Config file (default `~/.config/sparkwatch.toml`). |
| `--targets` | List configured targets and exit. |
| `--dump` | Headless: connect, collect one snapshot (and streaming progress for ~10 s), write a bundle into `--out`, exit. |
| `--out <DIR>` | Where bundles go (default `.`). |
| `--dump-logs <N>` | Log lines per executor in a bundle (default 2000; 0 = none). |

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
highlighted one, `a` brings the picker back at any time. For a finished
application the summary opens by itself.

### Kubernetes

```bash
sparkwatch --k8s -n spark                       # pick a running driver pod
sparkwatch --k8s -n spark my-etl                # watch the SparkApplication "my-etl"
sparkwatch --k8s -n spark --context prod-gke    # a specific kubeconfig context
```

sparkwatch lists the running driver pods in the namespace (`spark-role`
`driver` or `connect-server`, so Spark Operator `SparkApplication`s,
`SparkConnect` servers and plain `spark-submit` all show up), and when you
pick one spawns `kubectl port-forward pod/<driver> 0:4040` on a free local
port. The forward is torn down when you switch apps or quit; if it dies it is
re-established on the next poll. No separate port-forward terminal, and
nothing else to set up — the same command works against GKE, EKS or kind as
long as `kubectl` does.

`TARGET` can be the app name (the operator's `sparkoperator.k8s.io/app-name`
or `connect-name` label, or `spark-submit`'s `spark-app-name`), the driver
pod's name, or the pod name without its `-driver` / `-server` suffix. Not
sure which? Leave it off: the picker's `ID` column is the app name and
`NAME` is the pod, and either works. A name that doesn't match fails with the
list of drivers that are running.

It uses your kubeconfig and needs, in the namespace: `get`/`list` on `pods`,
`create` on `pods/portforward`, and `get` on `pods/log` (for the Logs view and
the Streaming tab). Nothing is written to the cluster.

### Config file

Name your clusters once in `~/.config/sparkwatch.toml` (or
`$XDG_CONFIG_HOME/sparkwatch.toml`); it is read whenever it exists:

```toml
[defaults]
interval = 2
timeout = 5

[targets.prod]
k8s = true
context = "gke_my-project_europe-west1_prod"   # optional kubeconfig context
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
| `1`–`9` | Jump to Overview / Jobs / Stages / Executors / SQL / Failures / Streaming / Storage / Env |
| `j` `k` `↓` `↑` · `PgUp` `PgDn` · `g` `G` | Move selection · by 10 · first / last |
| `Enter` | Picker: watch · Jobs: show that job's stages · Stages / SQL / Streaming query / batch: open the drill-down · Failures: full error text |
| `Esc` | Back: closes a drill-down, then the table filter, then the job filter, then quits |
| `/` · `c` | Filter the table (typed in the footer, `Enter` applies) · clear it |
| `S` | Summary — what happened since the app started |
| `D` | Write a bundle (summary, snapshot, failures, streaming history, environment, log tails) into `--out` |
| `m` | Executors: peak memory per region against the executor's budget |
| `L` | Logs of the selected executor, of the executor a failed task ran on, or of the one an alert concerns |
| `t` | Thread dump of the selected executor |
| `x` | Acknowledge new failures (clears the red strip; the log is kept) |
| `s` | Failures: open the stage the selected failure belongs to, on its failed tasks |
| `f` | Stage drill-down: switch between slowest and failed tasks |
| `a` | Open the application picker |
| `r` · `p` · `+` `-` | Refresh now · pause / resume polling · poll interval up / down (1–60 s) |
| `q` `Ctrl-C` | Quit |

The header shows `LIVE`, `PAUSED` or `ERROR`; on an error the last good data
stays on screen and the message appears in the footer.

## Tabs

- **Overview** — application info, cluster totals (cores, storage, shuffle,
  GC share flagged over 10 %), sparklines of tasks/s, active tasks and
  executors alive (red when any were removed in the window), a progress gauge
  per running job.
- **Jobs** — status, task and stage counts, failures, progress. Names come
  from the job description, the SQL execution the job belongs to, or the call
  site, whichever Spark provides; streaming batches read `orders-agg · batch
  4123`. `Enter` narrows the Stages tab to the job.
- **Stages** — active first; tasks, input, shuffle read/write, spill
  (highlighted), progress, `✗` on failed stages. `Enter` opens the
  drill-down.
- **Executors** — up/dead (with the `removeReason`), host, running tasks vs
  cores, failed tasks, storage memory, GC share, shuffle read, age. `L` logs,
  `t` threads, `m` memory detail.
- **SQL** — one row per execution (a query, or a micro-batch), newest first:
  status, text, submitted, duration, job counts, error. `Enter` opens the
  plan.
- **Failures** — everything that went wrong since sparkwatch started, newest
  first; it stays even after Spark forgets it.
- **Streaming** — Structured Streaming queries with charts, the recent
  batches, and `Enter` into any batch.
- **Storage** — storage memory across executors, every cached RDD /
  DataFrame with partitions cached vs total (yellow when partial), `Enter`
  for its spread across executors.
- **Env** — the performance-relevant settings pinned at the top (executor
  memory / overhead / cores, shuffle partitions, AQE, dynamic allocation,
  checkpoint location…), then every Spark / Hadoop / system property. `/`
  searches keys and values.

`/` on any table filters by what you'd expect for the tab (name and status,
executor id/host/reason, query text and error…), case-insensitively; the
title shows `Stages (12 of 340) · filter: writer`. Filters are per tab.

## Drill-downs

**Stage** (`Enter` on a stage) — header with totals, GC share, locality
summary and the `failureReason`; p5/p25/p50/p75/p95/max for duration, GC,
scheduler delay, input, shuffle, fetch wait, spill and peak memory from
Spark's own `taskSummary` (exact even for 100k-task stages), with a metric
whose max is 3× its median flagged `⚠ ×N`; per-executor tasks, time, shuffle
and spill with a bad executor flagged red and the reason; the 100 slowest
tasks with stragglers in yellow, or with `f` the failed tasks and their
errors. `L` on a failed task opens its executor's log filtered to the
exception. Refreshes at the poll interval, so it is fine to leave open.

**Failures** — an alert is derived on every poll for a `FAILED` stage, job
or SQL execution, a lost or excluded executor, and every failed task with
its full error (pulled from `taskList?status=failed` when a stage's failure
count grows, at most 5 stages × 20 tasks per poll). Deduplicated, kept for
the life of the process (last 1000). A red strip under the header counts the
new ones until `x`; `Enter` shows the full text, `s` opens the stage on its
failed tasks, `L` the executor's log.

**Streaming** — there is no REST API for Structured Streaming, so the tab
combines the micro-batch SQL executions (batch ids, status, duration; works
on the History Server) with the driver log's `Streaming query made progress`
events (rates, watermark, state; the driver must log at INFO for
`org.apache.spark.sql.execution.streaming`). The log is followed from the
first visit for as long as the app is watched. Per query: trigger duration
with mean/p95/max, input vs processed rows/s, watermark lag, state rows and
memory, `addBatch` vs planning/offsets/commit overhead, rows dropped by the
watermark, sparklines of each, the latest `durationMs` breakdown, and a
**Recent batches** table. A query processing slower than its input for most
of the last five batches is marked **FALLING BEHIND**. A restart from a fresh
checkpoint is treated as the same query. `Enter` on a query lists every batch
kept; `Enter` on a batch shows its numbers against the query's mean and p95,
the **stages that ran for it** (`Enter` → stage drill-down), the **failures
of this batch**, and `L` for the **driver log sliced to the batch's time
window**.

**Executor memory** (`m`) — peak heap (and its share of
`spark.executor.memory`, red from 90 %), off-heap, execution, storage,
direct buffers, process RSS for the JVM and Python workers, GC time; the
selected executor's peaks drawn against its budget from the resource profile
with a verdict — *heap near its maximum* (raise `spark.executor.memory`, or
more partitions) versus *RSS near the container limit while the heap is not*
(raise `spark.executor.memoryOverhead`). That's the difference between the
two causes of exit code 137. RSS needs
`spark.executor.processTreeMetrics.enabled`.

**SQL** (`Enter` on an execution) — status, duration, job ids, error; the
physical plan with operators highlighted (`p` full-screen) next to every
plan node with its three most telling metrics (output rows, spill, peak
memory, time…), `Tab` switching scroll focus.

**Logs** (`L`) — follows the tail; scrolling up stops following, `F`
resumes; `/` filters the whole buffer (last 20 000 lines), `w` wraps.
Under `--k8s` it is `kubectl logs -f` on the driver or executor pod, `P`
switches to `--previous` (where a crash's reason is; a dead executor opens
there directly), and a pod that is gone says so — executor pods are deleted
on exit unless `spark.kubernetes.executor.deleteOnTermination=false`, worth
setting for anything long-running. On YARN / standalone it is the
`executorLogs` page Spark reports, re-fetched as a tail, `o` switching
stderr / stdout.

**Thread dump** (`t`) — live, grouped: **BLOCKED** threads first with the
lock holder, then waiting, then **RUNNABLE** with Spark frames ahead of idle
pool threads; `e` expands, `/` filters by thread or frame, `r` refreshes.

## Summary and bundles

`S` opens a one-screen **summary**: failures by kind with the most common
reasons, the slowest stages with skew flagged, executors lost and why, the
worst GC, executors whose peak heap is near `spark.executor.memory`,
storage, each streaming query's batch timing / lag / failed batches, and the
key settings. For a finished application it opens by itself — the "what
happened while I was away" view.

`D` writes the same summary as `summary.md` into
`sparkwatch-<app>-<timestamp>/` under `--out`, with `snapshot.json`,
`failures.json`, `streaming.json`, `environment.json` and
`logs/driver.log` + `logs/executor-N.log` tails (`--dump-logs`). That's what
to attach to a ticket. Headless, for cron or CI:

```bash
sparkwatch --k8s -n spark my-etl --dump --out /var/tmp/spark-dumps
```

## Troubleshooting

| Symptom | Meaning / fix |
|---|---|
| Sits on `Connecting` | the driver UI isn't answering on that port (`spark.ui.enabled`, `spark.ui.port`); under `--k8s`, the pod isn't `Running` |
| `ERROR` badge, `connection refused` in the footer | driver gone or port-forward died; under `--k8s` it reconnects on the next poll |
| `no running driver for 'x' (running: …)` | the name matches no running driver; use one listed, or omit it for the picker |
| Streaming tab shows durations but no rates / watermark | the driver isn't logging at INFO for `org.apache.spark.sql.execution.streaming` |
| Logs say `pod gone` | the executor's pod was deleted on exit; set `spark.kubernetes.executor.deleteOnTermination=false` |
| Empty Storage / Logs / Threads | History Server — those aren't in the event log |
| Keys do nothing right after start | still on the picker / "Connecting" screen; wait for `LIVE` |

More in [CHEATSHEET.md § 8](CHEATSHEET.md).

## Demo workload

[`streaming-job/`](streaming-job/) is a long-running Structured Streaming
job in Rust, submitted over Spark Connect, built to give sparkwatch something
worth watching: a synthetic order stream written raw to parquet, a windowed
per-customer aggregation with a deliberately hot key (skew), and an optional
query that fails on purpose (failures). Its README covers running it against
a local Spark Connect server or one on Kubernetes.

## Development

```bash
python3 dev/mock_spark.py &          # fake /api/v1 with two apps (picker)
python3 dev/mock_spark.py --single & # one app, straight to the monitor
cargo run
cargo test
```

The mock serves on `localhost:4040` and never needs a cluster. The headless
smoke-test harness (pty + terminal-emulator replay) is in
[CHEATSHEET.md § 10](CHEATSHEET.md).

CI (`.github/workflows/ci.yml`) runs `cargo fmt --check`, `clippy -D
warnings` and the tests on Linux, macOS and Windows for every push. To cut a
release, bump `version` in `Cargo.toml`, then tag it:

```bash
git tag v0.2.0 && git push origin v0.2.0
```

`release.yml` refuses a tag that doesn't match `Cargo.toml`, builds the five
targets, and publishes a GitHub release with the archives, `SHA256SUMS` and
generated notes.

## License

[Apache-2.0](LICENSE).
