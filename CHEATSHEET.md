# sparkwatch cheatsheet

Everything on one page: how to connect, every key, what each screen shows
and how to read it, where to look for a given problem, and what the tool
can't see. The narrative version is [README.md](README.md).

---

## 1. Connecting

| You have | Run |
|---|---|
| A driver UI on this machine | `sparkwatch` (defaults to `http://localhost:4040`) |
| A driver UI elsewhere | `sparkwatch http://driver-host:4040` |
| A History Server | `sparkwatch http://history:18080` → pick from the list, or `-a <app-id>` |
| Spark on Kubernetes (operator or spark-submit) | `sparkwatch --k8s -n <ns>` → pick a driver, or `sparkwatch --k8s -n <ns> <app>` |
| A Spark Connect server (operator `SparkConnect`) | `sparkwatch --k8s -n <ns> <name>` (pod `<name>-server`) |
| A named target in `~/.config/sparkwatch.toml` | `sparkwatch <name>` |

```
-a, --app <ID>         Spark app id (History Server) — skips the picker
-n, --namespace <NS>   Kubernetes namespace (default: current kubectl context's)
    --context <NAME>   kubeconfig context to use with --k8s (default: current)
-i, --interval <S>     poll interval, default 2 (or [defaults].interval)
-t, --timeout <S>      HTTP timeout, default 5
    --config <PATH>    config file (default ~/.config/sparkwatch.toml)
    --targets          list configured targets and exit
```

### How `--k8s` finds things
- Lists pods with `spark-role in (driver, connect-server)` in `Running` phase — a `SparkApplication`
  driver (`my-etl-driver`), a `spark-submit --master k8s://` driver, or a `SparkConnect` server
  (`spark-connect-server`). `Pending`/`Completed` drivers are not listed.
- `<app>` matches the app name (`sparkoperator.k8s.io/app-name`, `sparkoperator.k8s.io/connect-name`
  or `spark-app-name` label), the pod name, or the pod name minus `-driver`/`-server`.
- On pick: `kubectl port-forward pod/<driver> 0:4040` on a free local port, then the REST API through
  it. The forward is killed on `a` (switch app) and on quit, and re-made if a poll fails.
- Executor pods (for logs) are found by `spark-role=executor`, `spark-exec-id=<id>`,
  `spark-app-selector=<spark app id>`.
- Sidecars: `kubectl logs` refuses pods with several containers; sparkwatch retries with
  `-c spark-kubernetes-driver` / `-c spark-kubernetes-executor`.
- Auth is whatever your kubeconfig does (OIDC, exec plugins, cloud CLIs) — it just runs `kubectl`,
  with `--context <NAME>` if given, so several clusters in one kubeconfig need no switching.

### Config file `~/.config/sparkwatch.toml`
```toml
[defaults]
interval = 2
timeout = 5

[targets.kind]                  # sparkwatch kind
k8s = true
context = "kind-spark"          # optional; any kubeconfig context
namespace = "default"
app = "spark-connect"           # optional: skip the picker

[targets.history]               # sparkwatch history
url = "http://history:18080"
app = "app-20260923-0001"       # optional
```
Command-line flags win over the target; under `--k8s` the positional is always an app name; a URL that
isn't a target name is used as-is; a typo in the file is a hard error naming the field.

---

## 2. Keys

### Everywhere
| Key | |
|---|---|
| `q` / `Ctrl-C` | quit (kills its port-forward and log streams) |
| `j` `k` `↓` `↑` | move / scroll |
| `PgUp` `PgDn` | by 10 rows (log viewer: by a page) |
| `g` `G` `Home` `End` | first / last |
| `r` | refresh now (thread view: re-fetch the dump) |
| `p` | pause / resume polling (data freezes; `PAUSED` badge) |
| `+` `-` | poll interval up / down, 1–60 s |
| `Esc` | back one layer: filter being typed → drill-down → table filter → job's stage filter → quit |

### Tabs
| Key | Tab | `Enter` does |
|---|---|---|
| `1` | Overview | — |
| `2` | Jobs | narrow the Stages tab to this job's stages |
| `3` | Stages | stage drill-down |
| `4` | Executors | — (`L` logs, `t` thread dump) |
| `5` | SQL | physical plan + node metrics |
| `6` | Failures | full error text (`s` open its stage, `L` its executor's log) |
| `7` | Streaming | — (`j`/`k` pick the query) |
| `8` | Storage | one RDD's distribution across executors |
| `Tab` `Shift-Tab` `←` `→` `h` `l` | next / previous tab | |

### On any table
| Key | |
|---|---|
| `/` … `Enter` | filter rows, case-insensitive substring; per tab, kept until cleared |
| `c` | clear the filter |
| `a` | application picker (History Server / k8s: switch app) |
| `x` | acknowledge new failures (clears the red strip; the log is kept) |
| `L` | logs — Executors tab: selected executor; Failures tab: the alert's executor |
| `t` | thread dump of the selected executor (Executors tab) |

What `/` matches: Jobs id/status/name · Stages id/status/name · Executors id/host/state (`up`,
`dead`, `excluded`)/removal reason · SQL id/status/query text/error · Failures kind/title/detail ·
Storage id/name/level. Title shows `Stages (12 of 340) · filter: writer`.

### Stage drill-down
| Key | |
|---|---|
| `f` | slowest tasks ↔ failed tasks |
| `j` `k` | move in the task table |
| `L` | logs of the selected task's executor, pre-filtered to its exception |
| `Esc` | back |

### SQL drill-down
| Key | |
|---|---|
| `Tab` | which pane `j`/`k` scroll: plan ↔ nodes |
| `p` | plan-only full screen |
| `Esc` | back |

### Log viewer
| Key | |
|---|---|
| `/` … `Enter` | filter lines (whole buffer, case-insensitive); `c` clears |
| `j` `k` `PgUp` `PgDn` | scroll — stops following the tail |
| `F` | follow the tail again (scrolling to the end also re-enables it) |
| `w` | wrap long lines |
| `P` | previous container (`kubectl logs --previous`) — k8s only; a dead executor opens on it |
| `o` | stderr ↔ stdout — YARN/standalone only |
| `t` | thread dump of the same executor |
| `Esc` | back to where `L` was pressed (stage drill-down, Failures, Executors) |

### Thread dump
| Key | |
|---|---|
| `e` | expand all frames / back to 8 per thread |
| `/` … `Enter` | filter by thread name or frame text; `c` clears |
| `r` | re-fetch |
| `L` | logs of the same executor |
| `Esc` | back |

---

## 3. Screens

### Header and strip
- Badge: `LIVE` (last poll ok) · `PAUSED` (`p`) · `ERROR` (last poll failed; the last good data stays on
  screen and the message is in the footer — the tab still works).
- `name [app-id] @ endpoint · every Ns · updated Ns ago` — if "updated" keeps growing, the poll is stuck.
- Red strip: `▲ N new failures · <latest> — <reason> · x acknowledge · 6 for all`.
- Tab titles: `Failures (N)` red = unacknowledged; `Streaming ▲` red = a query is falling behind.

### `1` Overview
- **Application**: user, Spark version, start, uptime, state.
- **Cluster**: jobs running/failed/total · executors active/total · cores and tasks running ·
  storage memory used/max · shuffle read · **GC time and its share of task time — red above 10 %**
  (a GC-bound cluster; check executor memory or the aggregation's spill).
- **Sparklines**: tasks/s (peak) and active tasks over the last ~4 min. Tasks/s dropping to zero
  while active tasks stay high = stuck tasks; active tasks far below cores = idle cluster
  (too few partitions, or the driver is the bottleneck).
- **Running jobs**: one gauge per RUNNING job. Empty between micro-batches on a streaming app.

### `2` Jobs
Newest first: id, status (RUNNING yellow / SUCCEEDED green / FAILED red), name (call site), submitted,
tasks done/total, failed tasks (red), stages done/total, progress bar. `Enter` narrows the Stages tab to
that job (`Stages (3 of 340) · job #12`; `Esc` clears).

### `3` Stages
Active first, then pending, failed, complete. `9.0` = stage 9 attempt 0; `✗` = has a failure reason.
Tasks done/total, input, shuffle read, shuffle write, **spill (magenta when non-zero — memory pressure
in that stage)**, progress bar.

### Stage drill-down (`Enter` on a stage)
- **Header**: status, scheduling pool, done/running/failed/killed tasks, input, shuffle r/w, spill
  mem/disk, total task time, GC %, progress gauge, `✗ failureReason` in red if failed.
- **Task metrics**: p5 / p25 / p50 / p75 / p95 / max for duration, GC, scheduler delay, input, shuffle
  read, fetch wait, shuffle write, mem spill, disk spill, peak memory — from Spark's own `taskSummary`
  quantiles, so exact even for 100k-task stages. `SKEW` column = max ÷ median; **`⚠ ×N` in yellow
  when ≥ 3** (and the max is at least 1 s / 1 MiB, so noise isn't flagged). `×∞` = median 0, max
  large — e.g. one partition spills, the rest don't. Duration skew with shuffle-read skew = a hot key.
  Duration skew without = a slow node (look at Executors below).
- **Executors**: per executor tasks ✓/✗, task time, mean per task, shuffle read, spill, and **WHY**
  when flagged red: `N failed` · `excluded` · `2.4× stage avg` (mean task time ≥ 2× the stage mean).
  Flagged rows sort first.
- **Tasks**: the 100 slowest — task id, index, attempt, executor, host, status, duration, GC,
  shuffle read, spill; **stragglers (≥ 3× median) in yellow with `straggler ×N`**, speculative
  copies marked. `f` switches to the failed tasks with the first line of each error. A FAILED stage
  opens on its failures directly. `L` jumps to the selected task's executor log, filtered to the
  exception name.
- Refreshes with every poll; fine to leave open on a running stage.

### `4` Executors
id, state (`up` green / `dead` red / `excl` magenta = scheduler excluded it after failures), host —
**a dead executor shows its `removeReason` next to the host** (`Container killed on request. Exit
code is 137…` = OOM-killed by Kubernetes/YARN) — tasks running/cores, failed tasks (red), storage
memory used/max with bar, **GC % (red above 10 %)**, shuffle read. `L` logs, `t` thread dump.

### `5` SQL
One row per SQL execution (a query, or a micro-batch of a streaming query), newest first: id, status
(RUNNING / COMPLETED / FAILED), query text or call site, submitted, duration, jobs `▶running
✓succeeded ✗failed` (red when any failed), first line of the error. Lists are fetched incrementally
(only new/recent executions each poll; last 500 kept), so it works on a streaming app with thousands
of batches. An app with no `/sql` (RDD-only, Spark < 3) says so.

### SQL drill-down (`Enter` on a query)
Header: status, submitted, duration, job ids per state, error. **Physical plan** (operators in cyan,
tree glyphs faded; scrollable) beside the **nodes** list: each plan node with its three most telling
metrics — output rows, **spill size (magenta when non-zero)**, peak memory, time, bytes. Use it to
find *which operator* in a slow batch is the expensive one (an Exchange with huge spill, a
HashAggregate with peak memory at the limit, a scan reading far more files than expected).

### `6` Failures
A persistent log — kept for the whole session even after Spark forgets — of: FAILED stages (with
`failureReason`), FAILED jobs, lost executors (with `removeReason`), excluded executors, FAILED SQL
executions (with error), and **every failed task with its full error message** (fetched when a
stage's failure count grows; ≤ 5 stages × 20 tasks per poll). Columns: when (age), kind
(stage/job/sql red, task yellow, executor magenta), what, first line of detail; new ones bold.
`Enter` = full text with stack frames dimmed; `s` = open the stage on its failed tasks; `L` = the
executor's log; `x` = acknowledge. Works even while the endpoint is unreachable.

### `7` Streaming
Structured Streaming has **no REST API**, so this is rebuilt from two sources, merged per
`(runId, batchId)`:
1. the **driver log** — every micro-batch's `Streaming query made progress: {…}` (started on the first
   visit and kept for the app's lifetime; `kubectl logs -f` under `--k8s`, the driver's stderr page on
   YARN/standalone). Needs the driver logging at INFO for `org.apache.spark.sql.execution.streaming`.
2. the **SQL executions** — each micro-batch's description carries query id / run id / batch number:
   batch ids, status and durations everywhere, History Server included, but no rates or watermark.

Per query: latest batch, trigger duration with **mean / p95 / max over the last 300 batches** and
batches/min, `input rows/s` vs `processed rows/s` — **`▲ FALLING BEHIND` when processing was slower
than input in ≥ 3 of the last 5 batches** — watermark lag, state rows and memory. Sparklines:
trigger duration, input vs processed rate (stacked, same scale), state size. Then the latest batch's
`durationMs` breakdown — `addBatch` (the actual work), `getBatch`, `latestOffset`, `queryPlanning`,
`walCommit`, `commitOffsets` — its sources (rows, rates) and sink; and a **Recent batches** table,
newest first: status, trigger (yellow above p95), input rows, in/s, processed/s (red when behind),
state rows, `addBatch`, watermark lag, time; failed batches red. A restarted query (new runId, or a
fresh checkpoint = new query id with the same name) keeps one entry and starts its window over;
stale data from the old id is ignored, and its jobs/stages get a `· run xxxxxxxx` suffix. Several
queries: a name-sorted list under the panel (≤ ⅓ of the screen, scrolls with the selection), `j`/`k`.

Reading it: trigger ≈ trigger interval and growing = the batch can't finish in time; `addBatch`
dominating = executor-side work (go to `3`); `queryPlanning`/`walCommit` dominating = driver-side
overhead (too many small batches, slow checkpoint store); state rows climbing without bound =
watermark not evicting (missing/too-long watermark); watermark lag growing = falling behind on
event time.

### `8` Storage
Summary: storage memory used / max across executors (red above 90 %), disk, the fullest executor,
number of cached RDDs. Then each cached RDD / DataFrame: id, name (the `cache()` call site), storage
level, **partitions cached/total — yellow when partial: the uncached ones are recomputed on every
read**, cached bar, memory, disk (magenta when spilled to disk). `Enter`: per-executor spread —
partitions held, bytes, share of the RDD, how full that executor's storage is (red above 90 %), disk
— plus a partition summary. Empty on the History Server (no storage data is retained).

### Log viewer (`L`)
Title: executor, `previous container` / stream, lines shown of total (and unfiltered count), `following`.
Lines: **ERROR / exception red, WARN yellow**. Bottom bar: the filter being typed, the active filter,
and the source status — `streaming pod/x`, `tail of http://… refreshed every 3s`, or why it can't:
`pod gone — set spark.kubernetes.executor.deleteOnTermination=false to keep executor logs`,
`no log URLs reported by this executor`, `kubectl logs ended: …`. Buffer is the last 20 000 lines.
Sources: `--k8s` → `kubectl logs -f --tail=2000` (driver pod, or the executor's pod); YARN/standalone →
the `executorLogs` page, 256 KiB tail, re-fetched every 3 s; History Server → none.

### Thread dump (`t`)
`GET /executors/{id}/threads`, grouped: **BLOCKED** first (with `blocked by #id (owner)` and what each
thread holds — the contention chain), **WAITING ON LOCK**, **RUNNABLE** (Spark frames in cyan, threads
doing task work ahead of idle pool threads), **IDLE / OTHER**. 8 frames per thread, `e` for all. Use it
when an executor has active tasks but tasks/s is zero: a BLOCKED `Executor task launch worker` tells
you what it's waiting for. Not available through the History Server or for a dead executor.

---

## 4. Where to look

| Question | Go to |
|---|---|
| Is the app healthy right now? | `1`: GC share, tasks/s vs active tasks, running-job gauges; the strip |
| A job is slow — which stage? | `2`, `Enter` on the job → its stages, active first; look at spill and task counts |
| A stage is slow — why? | `3`, `Enter`: `⚠ ×N` skew rows → hot key; a red executor with `×N stage avg` → bad node; stragglers in the task list |
| A task failed — with what error? | `6` (persistent), or `3` `Enter` `f`; `L` for the executor's log at that moment |
| An executor died — why? | `4`: `removeReason` next to the host (137 = OOM-killed); `6` has it too; `L` then `P` for the previous container's log |
| An executor is stuck? | `4` `t`: BLOCKED threads and who holds the lock |
| How long do micro-batches take? Keeping up? | `7`: trigger mean/p95/max, in vs processed rows/s, `FALLING BEHIND`, watermark lag |
| Where does a batch's time go? | `7`: `durationMs` breakdown; `5` `Enter`: per-operator metrics |
| Which operator spills / is heavy? | `5` `Enter`: node metrics (spill, peak memory, output rows) |
| Storage memory full? | `8`: used vs max, fullest executor, partial caches, disk spill |
| Something failed while I wasn't looking? | `6` — nothing is dropped; the strip counts what's new |

Typical drill: `7` (batches slow) → `5` `Enter` (which operator) → `3` `Enter` (which partition /
executor) → `L` (its log, pre-filtered) → `t` if it's hung.

### Walk-through: from a failure to its log
1. `6` — Failures tab. Newest at the top, bold until acknowledged.
2. `j`/`k` onto the failure. `/` `poison` `Enter` narrows the list if there are many.
3. `Enter` — full error: exception, message, stack (frames dimmed). `j`/`k` scroll, `Esc` back.
4. `L` — the executor's log, filter pre-set to the exception name, so the matching lines show first.
   `c` clears the filter to read the context around them; `F` follows the tail; `Esc` returns here.
   (Works on task and executor alerts. A job / stage / SQL alert has no executor of its own: use `s`.)
5. `s` — the stage the failure belongs to, opened on its failed tasks. `j`/`k` to a task, `L` for the
   log of the executor that ran *that attempt*; `f` flips to the slowest tasks.
6. `x` when you're done — clears the red strip, keeps the log.

Same thing from the other end: `3`, `Enter` on a `✗` stage (opens on its failed tasks), `j`/`k`, `L`.

## 5. Glossary (the Spark words on these screens)
| Term | Meaning | Where it shows |
|---|---|---|
| **Job** | one action (`count`, `write`, a micro-batch); made of stages. Named from its description (`orders-agg · batch 4123`), else its SQL execution (`sql #12 · SELECT …`), else the call site (`run at <unknown>:0` = no user code on the driver's stack, e.g. Spark Connect) | `2` |
| **Stage** | a set of identical tasks between two shuffles; `9.0` = stage 9, attempt 0 (retries make new attempts) | `3` |
| **Task** | one stage's work on one partition, run on one executor core | drill-down task table |
| **Shuffle** | data moved between executors when a stage needs rows regrouped by key (joins, groupBy, repartition); "shuffle write" leaves a stage, "shuffle read" enters the next | Stages, drill-down, SQL `Exchange` nodes |
| **Spill** | a task ran out of execution memory and wrote its working set (sort/aggregation/shuffle buffers) to local disk, then read it back — slow, and the usual step before an OOM. Causes: too few partitions, a skewed key, undersized executors | magenta `SPILL` on Stages; `mem spill` / `disk spill` rows in the drill-down; `spill size` on SQL nodes |
| **Skew** | a few partitions carry far more data or time than the rest (a hot key): one task runs on while the others sit idle | drill-down `⚠ ×N` |
| **Straggler** | a single task much slower than its siblings (≥ 3× median) — skew, a slow node, or GC | yellow task rows |
| **Speculative task** | Spark's own copy of a suspected straggler launched elsewhere; first to finish wins | task table note |
| **GC** | JVM garbage collection; time the executor spent not running your code. Above ~10 % = memory pressure | Overview, Executors, stage header |
| **Executor excluded** (`excl`) | the scheduler stopped giving an executor tasks after repeated failures on it (formerly "blacklisted") | Executors, Failures |
| **removeReason** | why an executor went away — `exit code 137` = killed for memory by Kubernetes/YARN | Executors host column, Failures |
| **Storage memory** | the part of executor memory holding cached RDD/DataFrame blocks (vs execution memory for shuffles/sorts) | `8`, Overview |
| **Micro-batch / trigger** | Structured Streaming processes input in batches; the trigger interval is how often; trigger duration is how long one took | `7`, SQL rows |
| **Watermark** | how late an event may arrive and still be counted; the lag is how far event time trails processing | `7` |
| **State** | rows a streaming aggregation/join keeps between batches (state store); grows until the watermark evicts them | `7` |
| **Physical plan** | the operator tree Spark actually runs (`Exchange`, `HashAggregate`, `Scan`…) | `5` `Enter` |

## 6. Thresholds and colours
| Signal | Rule |
|---|---|
| GC red | GC time > 10 % of task time (Overview, Executors, stage header) |
| `⚠ ×N` skew | metric max ≥ 3 × median, and max ≥ 1 s (times) / 1 MiB (bytes) |
| Straggler (yellow task) | task duration ≥ 3 × median task duration |
| Flagged executor (red) | failed tasks > 0, or excluded for the stage, or mean task time ≥ 2 × stage mean |
| `FALLING BEHIND` | processed rows/s < input rows/s in ≥ 3 of the last 5 batches with input |
| Storage red | used ≥ 90 % of max (cluster or one executor) |
| Spill magenta | any bytes spilled (stage row, SQL node, RDD on disk) |
| Partial cache yellow | cached partitions < total |
| `excl` | executor excluded by the scheduler (`isExcluded` / `isBlacklisted`) |

## 7. Compatibility and what it can't see
Works with any Spark ≥ 3.0 (driver or History Server; YARN, standalone, Kubernetes; 4.x included).

- **History Server**: no logs, no thread dumps, no storage; SQL and stages are fine; the Streaming tab
  shows batch durations only (from SQL executions).
- **Streaming rates / watermark** need the driver log at INFO; with log4j at WARN the tab keeps the
  SQL-derived durations and says so.
- **Logs on YARN/standalone** are a re-fetched tail, not a stream; **on Kubernetes** a dead executor's
  pod is deleted unless `spark.kubernetes.executor.deleteOnTermination=false`.
- **Thread dumps** only for live executors reachable from the driver.
- `/sql` absent on RDD-only apps and Spark < 3.0; `taskSummary` is empty until a task has completed.
- Spark ≤ 4.0 reports no error text for a failed SQL execution; the alert borrows the failed stage's
  `failureReason` instead (and `s` opens that stage).
- Polling: each cycle is 4–5 GETs plus ≤ 4 for an open drill-down plus ≤ 5 for new failures; a History
  Server is slow, so raise `-i` there.

## 8. Troubleshooting sparkwatch itself
| Symptom | Meaning / fix |
|---|---|
| `ERROR` badge, footer `GET … connection refused` | driver gone or port-forward died; under `--k8s` it reconnects on the next poll |
| Job: `connecting to Spark Connect … transport error` | nothing on `localhost:15002` — the 15002 port-forward isn't running (it dies whenever the server pod is replaced; use the self-restarting loop) |
| `no running driver for 'x' (running: …)` | name doesn't match any `Running` driver pod; use one listed, or omit it for the picker |
| Picker shows the app but `Enter` sits on `Connecting` | the driver UI isn't answering on 4040 inside the pod (`spark.ui.enabled`, or a different port) |
| `a container name must be specified` | handled automatically (retries with Spark's container name) |
| `pod gone — set …deleteOnTermination=false` | the executor's pod was deleted; only its driver-side `removeReason` survives |
| Keys ignored right after start | still on the picker / "Connecting" screen; wait for `LIVE` |
| Anything odd with input | `SPARKWATCH_KEYLOG=/tmp/keys.log sparkwatch …` logs every key press with a timestamp |

## 9. Spark Connect demo on Kubernetes (kind + Spark Operator)
```bash
# once: create the server and wait for it
kubectl apply -f streaming-job/k8s/spark-connect.yaml
kubectl wait sparkconnect/spark-connect --for=jsonpath='{.status.state}'=Ready --timeout=10m

# terminal A: ONE self-restarting forward (a second copy fails with "address already in use")
while true; do kubectl port-forward svc/spark-connect-server 15002:15002; sleep 1; done

# terminal B: the job
./streaming-job/target/debug/sparkwatch-streaming-job --remote sc://localhost:15002 --output /data/streaming-job

# terminal C: watch
sparkwatch --k8s spark-connect
```
Job options: `--rows-per-second 2000` `--partitions 8` `--hot-share 0.4` (skew) `--poison-every 50000`
(deliberate failures) `--duration-min 30`. It prints one line per query every 10 s from `lastProgress`.

### Seeing failures on purpose
```bash
./streaming-job/target/debug/sparkwatch-streaming-job --remote sc://localhost:15002 --output /data/streaming-job --poison-every 20000
```
A third query, `orders-poison`, raises `poison order 20000` on the 20 000th order (≈ 10 s at the
default 2 000 rows/s). Spark retries the task 4 times, then fails the stage, the job and the query;
the job notices within 10 s (`orders-poison died (as intended); restarting it`) and starts it again
with a fresh checkpoint, so it happens every ~20 s for as long as it runs. In sparkwatch:

| Where | What you see |
|---|---|
| red strip | `▲ N new failures · Task … failed · stage … · executor …` within one poll |
| `6` Failures | task alerts (4 attempts, each with `poison order 20000` and the stack), then the stage, the job, the SQL execution — `Enter` for the full text, `s` opens the stage on its failed tasks, `L` the executor log with `poison` pre-filtered |
| `2` Jobs / `3` Stages | the `FAILED` rows, `✗` on the stage; `Enter` on it opens on the failed-task list |
| `4` Executors | failed-task counts climbing on whichever executors ran the attempts |
| `5` SQL | the batch's execution `FAILED` with the error, `✗1` in the jobs column |
| `7` Streaming | `orders-poison` restarting: a new run id, batch ids from 0 again |

Make the failure rarer with a bigger `--poison-every`; make executors actually die (exit code 137,
`removeReason`, `L` then `P` for the previous container) by giving them less memory than the
aggregation needs — edit `memory: 1g` down in `k8s/spark-connect.yaml` and re-apply.

Stopping:
```bash
# just the queries: Ctrl-C in the job's terminal (clean stop; checkpoints resume next run)
kubectl delete pod spark-connect-server                    # orphaned queries: recreate the server
kubectl delete -f streaming-job/k8s/spark-connect.yaml     # everything: server, executors, service, PVC
pkill -f "port-forward svc/spark-connect-server"           # the 15002 forward (also fixes "address already in use")
```

Same logs without sparkwatch:
```bash
kubectl logs spark-connect-server -c spark-kubernetes-driver -f
kubectl logs spark-connect-exec-1 -f
kubectl get pods -l sparkoperator.k8s.io/connect-name=spark-connect -o wide
kubectl describe sparkconnect spark-connect | tail -30
```

## 10. Development
```bash
cargo test                            # unit tests
python3 dev/mock_spark.py --single &  # fake Spark API on :4040 (also --no-sql, --many-queries; no flag = two apps → picker)
cargo run
# headless smoke test: pty + terminal-emulator replay (pip install pyte)
(sleep 5; printf '6'; sleep 1; printf 'q') | script -q out.txt sh -c 'stty cols 170 rows 50; ./target/debug/sparkwatch'
python3 dev/screens.py out.txt 170 50 'Failures (11, 11 new)' '!ERROR'
```
CI runs `cargo fmt --check`, `clippy -D warnings`, tests on Linux/macOS/Windows, and builds the demo job.
Release: bump `version` in `Cargo.toml`, then `git tag vX.Y.Z && git push origin vX.Y.Z` — binaries
for macOS (arm64/x86_64), Linux (x86_64/arm64, static), Windows, with `SHA256SUMS`; `install.sh` fetches them.
