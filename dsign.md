# sparkwatch v2 — plan

## Context

`sparkwatch` is a ratatui TUI that replaces the Spark web UI for day-to-day monitoring. The v1 in the repo works (verified against a mock `/api/v1`): four tabs (Overview / Jobs / Stages / Executors), background poller, vim keys. It is untracked in git.

What the Spark UI does badly and v1 doesn't fix yet: finding *why* a stage is slow (task skew, stragglers, one bad executor), seeing SQL queries rather than raw jobs, finding failure messages before they scroll away, and driving it from the History Server with many apps. All four are wanted. This plan does them in four phases, each independently shippable and committed on its own.

Spark REST facts the design relies on (verified against `monitoring.html` and `status/api/v1/api.scala`):

- `stages/{id}/{attempt}/taskSummary?quantiles=0.05,0.25,0.5,0.75,0.95` → `TaskMetricDistributions` (`quantiles`, `duration[]`, `jvmGcTime[]`, `memoryBytesSpilled[]`, `diskBytesSpilled[]`, `shuffleReadMetrics.readBytes[]`, `shuffleWriteMetrics.writeBytes[]`, `inputMetrics.bytesRead[]`). 404s when no task has completed yet.
- `stages/{id}/{attempt}/taskList?sortBy=-runtime&length=N&status=…` → `TaskData[]` (`taskId`, `index`, `attempt`, `duration`, `executorId`, `host`, `status`, `speculative`, `errorMessage`, `taskMetrics{executorRunTime, jvmGcTime, memoryBytesSpilled, diskBytesSpilled, shuffleReadMetrics{remoteBytesRead, localBytesRead, fetchWaitTime}, shuffleWriteMetrics{bytesWritten}, inputMetrics{bytesRead}}`).
- `stages/{id}/{attempt}` → `StageData` incl. `failureReason`, `executorSummary: Map<execId, {taskTime, failedTasks, succeededTasks, killedTasks, inputBytes, shuffleRead, shuffleWrite, memoryBytesSpilled, diskBytesSpilled, isExcludedForStage}>`.
- `sql?details=false&planDescription=false&offset&length` → `ExecutionData[]` (`id`, `status`, `description`, `submissionTime`, `duration`, `runningJobIds`, `successJobIds`, `failedJobIds`, `errorMessage`); `sql/{id}?details=true&planDescription=true` adds `planDescription` and `nodes[{nodeId, nodeName, wholeStageCodegenId, metrics[{name,value}]}]`. 404 on non-SQL apps → treat as "no SQL".
- `ExecutorSummary` has `removeTime`, `removeReason`, `isExcluded` (3.1+) / `isBlacklisted` (older).
- `applications?status=&limit=` for the picker.

All new structs keep the existing `#[serde(rename_all = "camelCase", default)]` convention from `src/spark.rs` so version drift never breaks decoding.

## Phase 0 — baseline (small) ✅ done

1. `git add` + commit the v1 as-is ("Initial sparkwatch v1").
2. Split for growth, no behaviour change:
    - `src/spark.rs` → `src/spark/mod.rs` (client) + `src/spark/types.rs` (all `Deserialize` structs + tests).
    - `src/ui.rs` → `src/ui/mod.rs` (`draw`, header, footer, shared helpers `fmt_bytes`, `fmt_millis`, `short_time`, `status_style`, `mini_bar`, `header_row`, `selected_style`) + `src/ui/overview.rs` + `src/ui/tables.rs` (jobs/stages/executors).
3. **Stable selection by key.** Tables re-sort every poll so an index-based `TableState` drifts under the cursor. In `App`, alongside each `TableState`, remember the selected row's key (`job_id`, `(stage_id, attempt_id)`, executor `id`); after `apply()` re-find the index by key, falling back to the clamped index. Needed before drill-down so `Enter` opens the row the user is looking at.
4. Add `dev/mock_spark.py` (the fixture used to verify v1, extended in each phase with the new endpoints) so anyone can run `python3 dev/mock_spark.py & cargo run` without a cluster.

## Phase 1 — CLI + app picker + sparklines ✅ done

**Why first:** the picker changes the startup flow (`App` gains a `View` enum) and every later phase builds on that state model.

- `Cargo.toml`: add `clap = { version = "4", features = ["derive"] }`.
- `src/main.rs`: `#[derive(Parser)] struct Cli { endpoint: Option<String> (default http://localhost:4040), --app <ID>, --interval <SECS> (2), --timeout <SECS> (5) }`. Replace the hand-rolled `std::env::args`.
- **Request/response protocol between UI and poller** (replaces the bare `mpsc<Duration>` refresh channel):

  ```rust
  enum Request { SetInterval(Duration), RefreshNow, Watch(Focus) }   // UI → poller
  enum Focus  { App(String), Stage { id: i64, attempt: i64 }, Sql(i64), None }
  enum Message { Apps(Vec<ApplicationInfo>), Snapshot(Result<Snapshot,String>), Detail(Result<Detail,String>) }
  ```
  The poller keeps the current `Focus` and, on every cycle, fetches the snapshot **and** the focused detail concurrently (`tokio::join!`), so open detail views stay live at the poll interval. This one mechanism serves phases 2 and 3.

  *As built:* `Request::{SetInterval, RefreshNow, WatchApp(String), ListApps, SetDetail(Option<Detail>), Shutdown}` and `Message::{Apps, Snapshot{app_id, result}, Detail{detail, result}}`. Snapshots are tagged with the app id so a late result for an app the user has switched away from is dropped; requests that arrive in a burst are drained before the next fetch; listing apps is throttled to ≥10 s.
- **App picker:** on startup, if `--app` not given and `/applications` returns >1 entries, `App.view = View::Picker` with a table (id, name, user, start, duration, state); `j/k`, `Enter` selects → `Request::Watch(Focus::App(id))`; `a` from the main view returns to the picker. Exactly one app (live driver) → skip straight to `View::Main`.
- **Sparklines (Overview):** `App.history: VecDeque<Sample>` (cap 120) where `Sample { at: Instant, completed_tasks: i64, active_tasks: i64, failed_tasks: i64 }` is derived from executor totals on each `apply()`. Two `ratatui::widgets::Sparkline`s in a new row of the Overview: tasks completed/sec (delta between samples) and active tasks. Reset history when the watched app id changes.
- Files: `src/main.rs`, `src/app.rs` (`View`, `history`, picker state), `src/spark/mod.rs` (`applications()` with `limit`), `src/ui/picker.rs`, `src/ui/overview.rs`.

## Phase 1b — Kubernetes mode ✅ done (added after Phase 1)

Added because the real deployment is Spark Operator on Kubernetes, where "run `kubectl port-forward` in another terminal, then sparkwatch" is the clunky part.

- `sparkwatch --k8s [-n NS] [APP]`: `APP` is the `SparkApplication` name (also accepts the driver pod name or the `spark-app-name` label). Without `APP` the picker lists running driver pods.
- `src/k8s.rs`: shells out to `kubectl` (so kubeconfig auth — OIDC, exec plugins, cloud CLIs — just works, and the binary stays small).
  - `list_drivers`: `kubectl get pods -l spark-role=driver -o json`, `Running` only, newest first; app name from `sparkoperator.k8s.io/app-name` → `spark-app-name` → pod name minus `-driver`.
  - `PortForward::to_driver`: `kubectl port-forward pod/<driver> 0:4040`; local port 0 lets kubectl pick a free one, parsed from its `Forwarding from 127.0.0.1:<port>` banner. stdout/stderr are drained so kubectl's per-connection chatter can't fill the pipe and stall it. `kill_on_drop`.
- `src/poller.rs`: `Source::{Http(SparkClient), Kube{namespace, timeout, conn}}`. `resolve(key)` hands back a client + the Spark app id (a driver reports its own `spark-…` id; REST paths need that, not the k8s name). A poll error drops the forward so the next cycle reconnects. `Request::Shutdown` tears it down before exit so no kubectl outlives the process.
- CLI: `--k8s`, `-n/--namespace` (requires `--k8s`), `-a/--app` conflicts with `--k8s`; the positional is a URL in HTTP mode and the app name in k8s mode.
- Verified with a fake `kubectl` on `PATH` (two Running drivers + one Succeeded that must be filtered; fakes the forward banner) against the mock Spark server.

## Phase 2 — stage drill-down + skew detection ✅ done

- Types (`src/spark/types.rs`): `TaskData`, `TaskMetrics`, `ShuffleReadMetrics`, `ShuffleWriteMetrics`, `InputMetrics`, `OutputMetrics`, `TaskMetricDistributions` (each metric a `Vec<f64>` parallel to `quantiles`), `ExecutorStageSummary`; extend `StageData` with `failure_reason: Option<String>`, `executor_summary: HashMap<String, ExecutorStageSummary>`, `description`, `scheduling_pool`, `jvm_gc_time`, `executor_run_time`.
- Client (`src/spark/mod.rs`): `stage_detail(id, attempt) -> StageDetail` doing three concurrent GETs: `stages/{id}/{attempt}`, `taskSummary?quantiles=0.05,0.25,0.5,0.75,0.95` (404 → `None`), `taskList?sortBy=-runtime&length=100`. Plus `taskList?status=failed&length=50` when `num_failed_tasks > 0`.
- **Skew analysis** (`src/analysis.rs`, pure functions, unit-tested):
    - `Skew { median, p95, max, ratio: max/median }` per metric (duration, shuffle read, spill, GC); flag `ratio >= 3.0` as skewed. Computed from the distributions, not the task list, so it is correct even for stages with 100k tasks.
    - Stragglers: tasks from the top-N list whose `duration > 3 × median`.
    - Per-executor: from `executor_summary`, sort by `task_time` desc; flag any executor whose `failed_tasks > 0` or whose mean task time is > 2× the stage mean.
- UI (`src/ui/stage_detail.rs`), opened with `Enter` on the Stages tab, closed with `Esc`/`Backspace`:
    - Top: stage id/name/status/pool, `failure_reason` in red if set, progress gauge.
    - Left: distribution table — rows: duration, GC, shuffle read, shuffle write, spill (mem/disk), input; columns p5/p25/p50/p75/p95 + max; skewed rows highlighted with a `⚠ skew ×N` tag.
    - Right: per-executor table (exec, host, tasks ok/failed, task time, shuffle read, spill), worst first, flagged rows red.
    - Bottom: slowest tasks table (task, index, executor, duration, GC, shuffle r, spill, status) with stragglers in yellow; `f` toggles to the failed-tasks list showing `errorMessage` first line. `j/k` scroll this table.
- Enter on the **Jobs** tab jumps to the Stages tab with the job's `stage_ids` filtered (small: `App.stage_filter: Option<Vec<i64>>`, cleared with `Esc`). Cheap and makes the jobs table useful.

## Phase 3 — SQL tab ✅ done

- Types: `ExecutionData`, `SqlNode`, `SqlMetric` (`metrics: Vec<{name, value: String}>` — values are pre-formatted strings from Spark).
- Client: `sql_list(offset, length)` (`details=false&planDescription=false`; 404 → empty list and a `sql_available: bool` flag so the tab shows "no SQL executions / not a SQL app" instead of an error); `sql_detail(id)`. `sql_list` is part of the periodic snapshot; `sql_detail` is a `Focus::Sql(id)` detail fetch.
- `Tab::Sql` added to `Tab::ALL` (key `5`). Table: id, status (RUNNING/COMPLETED/FAILED styled via existing `status_style`), description (truncated), submitted, duration, jobs (`✓n ✗n ▶n`), error (first line, red).
- Detail (`src/ui/sql_detail.rs`, `Enter`/`Esc`): header (description, status, duration, job ids, `errorMessage`); body split: left = scrollable `planDescription` `Paragraph` (`j/k`, `PgUp/PgDn`); right = node list (`nodeName` + up to 3 most useful metrics: prefer names containing `output rows`, `spill`, `peak memory`, `time`, `bytes`). `p` toggles plan-only fullscreen.

*As built — the list fetch is not "part of the snapshot" as planned:* `/sql` is oldest-first, has no sort parameter, defaults to 20 rows, and a streaming app retains up to `spark.sql.ui.retainedExecutions` (1000) micro-batches with multi-KB descriptions, so fetching it whole every 2 s over a port-forward is not on. `SparkClient::sql_tail(app, after)` uses two facts — execution ids are dense and the list is id-ordered — so `index = id − min_id`, where `min_id` comes from one `offset=0&length=1` GET. It then fetches from `after − 100` (refreshing recent statuses and picking up new executions); the first call pages through everything once (bounded by 10 × 500). The poller holds a `SqlCache` (`BTreeMap<id, ExecutionData>`, last 500, reset on app switch) and fills `Snapshot.sql` from it; a failed SQL fetch keeps the cached list rather than failing the whole snapshot. `sql_detail(id)` is a `Detail::Sql(id)` fetch, `None` (404) → "no longer retained by the driver" while the last good plan stays on screen. `Tab` switches which pane `j/k` scroll; `Esc` closes.

## Phase 4 — failures surfaced ✅ done

- Types: `ExecutorSummary` gains `remove_time: Option<String>`, `remove_reason: Option<String>`, `is_excluded: bool`, `is_blacklisted: bool` (either spelling → "excluded").
- **Alert log** (`src/alerts.rs`, unit-tested): `Alert { key: String, kind: Stage|Task|Executor|Sql, first_seen: Instant, severity, title, detail: Option<String> }` stored in `App.alerts: Vec<Alert>` keyed for dedup. Derived on every `apply()` from: stages with `status == FAILED` (title = stage name, detail = `failure_reason`), jobs `FAILED`, executors with `!is_active && remove_reason.is_some()` or `is_excluded`, SQL executions `FAILED` with `errorMessage`, and per-task failures pulled by the poller for every stage with `num_failed_tasks > 0` that is ACTIVE or FAILED (`taskList?status=failed&length=20`, capped at the 5 most recent such stages per cycle to bound request count). Alerts persist for the process lifetime even after the source drops off the API — the point is that they don't scroll away.
- **Alert strip:** when `alerts` has entries newer than the last acknowledge, a 1-line red bar under the header: `▲ 3 new failures · latest: stage 9 "mapPartitions at Writer.scala:88" — ExecutorLostFailure …`. `x` acknowledges (clears the "new" state, keeps the log).
- **Failures tab** (`Tab::Failures`, key `6`): list of alerts newest-first (time, kind, title, first line of detail); `Enter` opens a wrapped, scrollable detail paragraph with the full error/stack; `Enter` on a stage alert opens that stage's drill-down (reuses Phase 2).
- Existing rows get failure context too: Executors tab shows `remove_reason` (first 40 chars) in the HOST column for dead executors; Stages tab shows a `✗` marker on rows with `failure_reason`.

*As built:* `src/alerts.rs` (`AlertLog`, keyed dedup, bounded at 1000, `acknowledge()` marks everything so far as seen; a `failureReason` that Spark fills in after the FAILED status updates the existing alert rather than adding one). Failed-task fetching lives in the poller's per-app `AppState.failed_seen` memo: only stages whose `numFailedTasks` grew since the last fetch are queried, newest 5 per cycle, 20 tasks each; results ride on `Snapshot.failed_tasks` as a delta and the log dedups by `task:{id}.{attempt}`. The Failures tab renders from the log, not the snapshot, so it works while the endpoint is down. `s` on an alert (tab or full-text view) opens its stage on the failed-tasks list via the Phase 2 drill-down. Excluded executors show `excl` in the STATE column.

## Phase 5 — logs and thread dumps ✅ done

The Spark UI's other weak spot: executor logs are links to a NodeManager page, and on Kubernetes they are not linked at all. `executorLogs` in the REST API is a `Map<String, String>` of `stdout`/`stderr` URLs per executor (the `driver` row too, on YARN); it is empty on Kubernetes unless `spark.executorEnv.SPARK_LOG_URL_*` is configured. The History Server has no stdout/stderr at all (only event logs as a zip via `/logs`).

| Deployment | Driver | Executors |
|---|---|---|
| Kubernetes | `kubectl logs <app>-driver` | `kubectl logs` on pods with `spark-role=executor`, `spark-exec-id=<N>`, `spark-app-selector=<spark app id>` |
| YARN / standalone | `executorLogs` URLs (`driver` entry) | `executorLogs` URLs, served by the NodeManager / worker |
| History Server | — | — |

Also: `GET /applications/{app}/executors/{id}/threads` (live driver only, not History Server) returns a thread dump — `[{threadId, threadName, threadState, stackTrace{elems[]}, blockedByThreadId, blockedByLock, holdingLocks[], lockName, lockOwnerName}]`. For "why is executor 7 doing nothing" that beats the logs, and the web UI hides it behind a per-executor link.

### 5.1 Log viewer (`L` on the Executors tab, incl. the `driver` row)

- `View::Logs { executor_id }`, `src/ui/logs.rs`: a scrollable pane, **follow** by default (sticks to the tail until the user scrolls up; `F` re-enables follow), `/` filter (substring, case-insensitive — type `ERROR` or `OutOfMemory` to see only matching lines; filter applies to the buffer, not the stream), `w` wrap toggle, `g/G`, `PgUp/PgDn`, `Esc` back. Ring buffer of ~20k lines so a chatty executor can't eat memory.
- **Kubernetes source** (`src/k8s.rs::LogStream`): `kubectl logs -f --tail=<N> <pod>` as a child process (same lifecycle rules as `PortForward`: killed on view close, on app switch, on shutdown). Executor pod found by `spark-role=executor,spark-exec-id=<id>,spark-app-selector=<spark app id>` in the namespace. `P` switches to `--previous` for a container that restarted, which is how you read why it crashed.
  - Caveat surfaced in the view, not as a bare error: executor pods are deleted when the executor dies unless `spark.kubernetes.executor.deleteOnTermination=false`, so a dead executor's logs are readable only if the pods are kept. Message: `pod gone — set spark.kubernetes.executor.deleteOnTermination=false to keep executor logs`.
- **HTTP source** (`src/spark/logs.rs`): best-effort over the `executorLogs` URLs. YARN NodeManager supports `?start=-<bytes>` for tailing and returns HTML around a `<pre>`; standalone's worker `logPage` is also HTML. Fetch, strip tags, show; re-fetch on the poll interval to approximate follow. When the map is empty the view says `no log URLs reported by this executor` instead of erroring.
- Poller: `Request::SetLogs(Option<LogTarget>)`, `Message::LogLines(Vec<String>)` (batched per 100 ms so the UI isn't woken per line). The stream is *not* tied to the poll cycle — it is its own task feeding the channel.

### 5.2 Thread dump (`t` on an executor)

- `View::Threads { executor_id }`, fetched once on open and on `r`. Grouped by state, `BLOCKED` first then `WAITING`/`TIMED_WAITING` on a lock (with `blockedByThreadId` / `lockOwnerName` shown so the chain is visible), then `RUNNABLE`; the top 8 frames of each, `Enter` expands one thread's full stack, `/` filters by thread name or frame text (e.g. `HashAggregate`).
- Not available on the History Server or through an ingress that blocks it → the view says so.

### Streaming apps

Executors of a streaming app live for days, so live tailing is the common case and works as-is. The `deleteOnTermination` caveat matters more here, not less: the executor that OOMs and gets replaced is exactly the one whose log you want. Recommend `spark.kubernetes.executor.deleteOnTermination=false` for streaming apps in the README.

The driver log is the important one for Structured Streaming: each micro-batch's `StreamingQueryProgress` JSON (batch id, `batchDuration`, `numInputRows`, `inputRowsPerSecond`, `processedRowsPerSecond`, watermark, state-store rows/bytes) is written there and **nowhere in the REST API** (`/streaming/*` is legacy DStreams only). Each micro-batch also appears as a SQL execution on the Phase 3 tab.

### 5.3 Jump-in from a failure

- In the stage drill-down's failed-tasks table, `L` opens the logs of the executor the task ran on with the filter pre-set to the first token of the error (e.g. `OutOfMemoryError`); on a dead executor in k8s mode it tries `--previous` first. This is the workflow the whole phase exists for: see the OOM in the task table → one key → that executor's stderr at that moment.
- Same key on the Failures tab (Phase 4) for task and executor alerts.

### As built

- `src/logview.rs` (model: bounded buffer, follow, filter with `/` input mode, `filter_from_error`), `src/threads.rs` (grouping/ordering), `src/spark/logs.rs` (NodeManager/worker HTML → text, `?start=-N` tail), `src/ui/logs.rs`, `src/ui/threads.rs`. All unit-tested.
- Poller: became a `Poller` struct; `Request::{OpenLogs(LogTarget), CloseLogs, FetchThreads(id)}`, `Message::{Log(LogEvent::{Lines, Replace, Status}), Threads}`. The log stream is its own tokio task (`run_logs`), aborted on close / app switch / shutdown; kubectl lines are batched every 100 ms (max 500) so the UI isn't woken per line. `Source::log_source()` yields `LogSource::{Kube{namespace, spark_app_id, driver_pod}, Http(client)}`.
- k8s: `find_executor_pod` by `spark-role=executor,spark-exec-id,spark-app-selector`; `LogStream` (`kubectl logs -f --tail=2000 [--previous] [-c …] pod/…`, `kill_on_drop`). First attempt names no container; if kubectl answers "a container name must be specified" (sidecars) it retries with `spark-kubernetes-driver` / `spark-kubernetes-executor`.
- `App.return_view` remembers where `L`/`t` was pressed so `Esc` goes back there (stage drill-down, Failures tab…). `open_logs` marks `previous: true` when the executor is dead in the snapshot. `t` from the log view and `L` from the thread view swap between the two for the same executor.
- HTTP mode: `stderr` by default (`o` toggles), page re-fetched every 3 s and the buffer replaced; no URL → "no log URLs reported … run with --k8s".

### Tests / verification

- Unit: log line ring buffer + filter; HTML stripping for NodeManager/worker pages; thread dump parsing and grouping/ordering; executor-pod label matching.
- Fake `kubectl` gains `logs` (streams a few lines then keeps printing) and `get pods -l spark-role=executor`; mock Spark gains `executorLogs` URLs pointing at itself with a fake NodeManager HTML page, and `/executors/1/threads` with one BLOCKED chain.
- Smoke: `4` Executors → `L` → lines appear and follow → `/ERROR` narrows → `Esc`; `t` → BLOCKED thread at top; stage drill-down failed task → `L` opens pre-filtered.

## Cross-cutting

- **Footer help** in `src/ui/mod.rs::draw_footer` becomes per-view (main vs detail vs picker) so keys are discoverable.
- **Fetch failures for details** never replace the last good snapshot (same rule as today's `apply()`); shown in the footer.
- **Request budget per cycle**: snapshot (4 GETs incl. sql list) + focused detail (≤4 GETs) + failed-task lists (≤5). ~13 GETs worst case at a 2 s interval against a driver — fine; the History Server is slower, so the picker view raises the default interval to 10 s.

## Verification

- `cargo test`: parsing fixtures for `taskList`, `taskSummary`, `sql`, `sql/{id}`, executor `removeReason`; `analysis` skew/straggler math; `alerts` dedup + acknowledge.
- `dev/mock_spark.py` extended with: two apps (to trigger the picker), a skewed `taskSummary` (p50 = 8 s, max = 90 s), a failed task with `errorMessage`, a dead executor with `removeReason`, two SQL executions (one FAILED) with a `planDescription`.
- Smoke run per phase, as done for v1: `script -q out.txt sh -c 'stty cols 150 rows 45; ./target/debug/sparkwatch'` with scripted keys, then grep the de-ANSI'd frames for expected strings (picker rows → `Enter` → `5` SQL tab → `3` Stages → `Enter` drill-down shows `⚠ skew` → `6` Failures shows the `removeReason`).
- Final: run against a real driver/History Server if one is reachable; otherwise the mock is the acceptance fixture.

## Phase 6 — Streaming tab ✅ done

Structured Streaming has **no REST API** (`/streaming/*` is legacy DStreams; the web UI's Structured Streaming page reads the listener bus directly). Two sources are available and both are used:

1. **Driver log tap** (primary). `ProgressReporter` logs every micro-batch at INFO: `… INFO MicroBatchExecution: Streaming query made progress: {` followed by the `StreamingQueryProgress` as *pretty-printed multi-line JSON* (`prettyJson`), closing when braces balance. Fields used: `id`, `runId`, `name`, `timestamp`, `batchId`, `batchDuration`, `numInputRows`, `inputRowsPerSecond`, `processedRowsPerSecond`, `durationMs{addBatch, getBatch, latestOffset, queryPlanning, triggerExecution, walCommit, commitOffsets}`, `eventTime.watermark`, `stateOperators[{operatorName, numRowsTotal, numRowsUpdated, memoryUsedBytes, numRowsDroppedByWatermark}]`, `sources[{description, numInputRows, inputRowsPerSecond, processedRowsPerSecond}]`, `sink{description, numOutputRows}`. All optional under `#[serde(default)]`. Requires the driver to log at INFO for `org.apache.spark.sql.execution.streaming` — the tab says so when it has seen the log but no progress lines.
   - `--k8s`: a second `kubectl logs -f --tail=10000` on the driver pod, independent of the user's log view, started when the Streaming tab is first opened and kept for the life of the watched app (batches keep accumulating while you look elsewhere).
   - YARN/standalone: the driver's `executorLogs` stderr page, re-fetched every 5 s and re-parsed; duplicates dropped by `(runId, batchId)`.
   - History Server: no log → source 2 only.
2. **SQL executions** (fallback, everywhere incl. History Server). A micro-batch's execution description is `"<name>\nid = <queryId>\nrunId = <runId>\nbatch = <n>"`. That yields batch id, status and duration per query with zero extra requests — enough for the batch-duration sparkline and "batches/min", not for rates or watermark.

### Model — `src/streaming.rs` (pure, unit-tested)

- `Progress` (the JSON above), `ProgressParser::feed(line) -> Option<Progress>`: detects the marker, buffers following lines while tracking brace depth, parses on balance; tolerates the single-line form and garbage in between (resets on a new marker).
- `batch_from_sql(&ExecutionData) -> Option<(query_id, run_id, batch_id)>` from the description.
- `QueryHistory { query_id, run_id, name, batches: BTreeMap<batch_id, Batch> }` capped at 300 batches; `Batch { batch_id, duration_ms, status, progress: Option<Progress> }`; a progress event upgrades an existing SQL-derived batch. `Streaming { queries: BTreeMap<query_id, QueryHistory> }` with `ingest_progress`, `ingest_sql(&[ExecutionData])`.
- `QueryStats::of(&QueryHistory)`: latest batch, mean/p95/max trigger duration over the retained window, current input vs processed rows/s, `behind: bool` = processed < input in ≥ 3 of the last 5 batches with input > 0, total state rows / memory, watermark lag = `timestamp − watermark` when both parse, batches per minute.

### Plumbing

- Poller: `Request::TapProgress` (idempotent start of the driver tap), `Message::Progress(Progress)`, `Message::ProgressStatus(String)` ("tapping pod/x", "no driver log URL", "History Server: batch durations only"). Tap task = `run_logs` variant that feeds lines into `ProgressParser` and only sends parsed events (the UI never sees raw log lines). Closed on app switch / ListApps / shutdown like the log stream.
- App: `Tab::Streaming` (key `7`), `streaming: Streaming`, `streaming_status`, `streaming_cursor: Cursor<String>` over queries (j/k), `apply_snapshot` calls `streaming.ingest_sql`, `apply_progress`. Opening the tab sends `TapProgress` once.

### UI — `src/ui/streaming.rs`

Per query, stacked (selected query expanded, others one summary line when there are several):

- Header: name (or short id) · run id · `batch 4123` · `2s ago` · trigger `1.2s` · input `12,400 rows` · `10,300 rows/s in` vs `12,800 rows/s processed` (red + `FALLING BEHIND` when `behind`) · watermark lag `3m12s`.
- Three sparklines side by side: **trigger duration** (mean/p95/max in the title), **rows/s** input (yellow) vs processed (green) overlaid as two sparklines split vertically, **state rows** (with memory in the title). Uses the existing `fit()` helper pattern from the Overview.
- Latest batch breakdown line: `addBatch 900ms · getBatch 10ms · queryPlanning 30ms · walCommit 15ms · commitOffsets 20ms` and per-source rows.
- Empty state explains the sources: "no streaming queries seen — this tab needs the driver log at INFO (`--k8s` or YARN log URLs) or micro-batch SQL executions".

### As built

`src/streaming.rs` (types, `ProgressParser`, `batch_from_sql`, `Streaming`/`QueryHistory`/`Batch`, `QueryStats`, hand-rolled ISO-8601 → epoch so no date crate), `src/ui/streaming.rs`. Poller: `Request::TapProgress` is idempotent (re-sent on every tab visit, ignored while the tap task runs), `run_tap` shares `LogStream`/`extract_log_text` with Phase 5 and forwards only parsed `Message::Progress`; the driver's stderr URL for HTTP mode is remembered from each snapshot's `driver` executor row. The tap is closed on app switch / ListApps / shutdown. A restarted query (new `runId`) drops its old batches; `(runId, batchId)` dedup absorbs the HTTP re-fetch. Verified against the fake `kubectl` driver log (pretty-printed blocks every ~1 s, falling behind from batch 4121) and the mock's driver stderr page + micro-batch SQL executions (`6 progress events · 3 batches from SQL executions`, batch 4124 `RUNNING` known only from SQL).

### Verification

- Unit: multi-line and single-line progress parsing, marker mid-line with log4j prefix, interleaved unrelated lines, dedup by `(runId, batchId)`, SQL description parsing, `behind` detection, p95.
- Mock: fake `kubectl logs` for the driver emits a pretty-printed progress block every second with input > processed for the last few batches; mock Spark's driver stderr page carries the same; SQL executions gain three micro-batch executions with the `runId = … batch = n` description.
- Smoke: `7` → header with batch id, `FALLING BEHIND`, three sparkline titles, breakdown line; History-Server-style run (`--no-sql` off, no log URL) shows durations only with the status line explaining why.

## Phase 9 — Spark UI parity + reports ✅ done

What the web UI still had over sparkwatch, plus two things it doesn't.

- **Environment tab** (`9`). `GET /environment` → `runtime` (Java/Scala), `sparkProperties`, `hadoopProperties`, `systemProperties`, `metricsProperties`, `classpathEntries` (all `[key, value]` pairs), `resourceProfiles`. Grouped sections, `/` searches keys and values, a short list of performance-relevant keys (`spark.executor.memory/cores/memoryOverhead`, `spark.sql.shuffle.partitions`, `spark.dynamicAllocation.*`, `spark.sql.adaptive.*`, `spark.sql.streaming.checkpointLocation`, `spark.eventLog.*`, `spark.kubernetes.executor.deleteOnTermination`) is pinned at the top. Fetched once per app (it doesn't change) and on `r`.
- **Executor memory detail** (`m` on the Executors tab, `View::ExecutorMemory`). From `ExecutorSummary.memoryMetrics` (`usedOnHeapStorageMemory`, `usedOffHeapStorageMemory`, `totalOn/OffHeapStorageMemory`) and `peakMemoryMetrics` (`JVMHeapMemory`, `JVMOffHeapMemory`, `OnHeap/OffHeapExecutionMemory`, `OnHeap/OffHeapStorageMemory`, `OnHeap/OffHeapUnifiedMemory`, `DirectPoolMemory`, `MappedPoolMemory`, `ProcessTreeJVM/Python/OtherRSSMemory`, `Minor/Major/ConcurrentGCCount/Time`, `TotalGCTime`) shown against the budget from the resource profile (`memory`, `memoryOverhead`, `offHeap`) and `spark.memory.fraction`, so "heap full vs overhead full" is one screen. Table across executors + the selected one expanded.
- **Stage locality summary** in the drill-down header: counts of `taskLocality` over the fetched tasks (`PROCESS_LOCAL · NODE_LOCAL · RACK_LOCAL · ANY`) plus input/output records from the stage.
- **Executor timeline** on the Overview: executors-alive count over the sparkline window, derived from `addTime`/`removeTime`, with removals marked; the Executors tab gets an age column.
- **Streaming charts**: sparklines per `durationMs` component (`addBatch`, `getBatch`, `queryPlanning`, `walCommit`, `commitOffsets`) and for state rows, rows dropped by watermark, watermark lag, over the batch window. Rendering only — the data is already in `QueryHistory`.
- **`--dump DIR` / `D`**: a bundle `sparkwatch-<app>-<timestamp>/` with `snapshot.json`, `failures.json`, `streaming.json`, `environment.json`, `summary.md`, and `logs/driver.log` + `logs/executor-<id>.log` tails (`--dump-logs N`, default 2000 lines, 0 = none). `D` in the app writes it from current state (plus any open log buffer); `--dump` on the command line is headless: connect, poll once (and wait ~10 s for the streaming tap when the app has queries), write, exit — cron-able. Serialisation via `serde::Serialize` on the existing types.
- **Summary view** (`S`; shown automatically at startup when the app is completed, i.e. History Server): failures by kind with top reasons; slowest stages by task time (with `taskSummary` fetched for the top 5 so skew is flagged); executors lost and why; worst GC executors; for streaming: slowest and failed batches, `FALLING BEHIND` queries, watermark lag. Generated by the same code that writes `summary.md`, so the report read on screen is the one shipped in the dump.

Order: Environment → memory detail → locality (quick wins) → `--dump` + Summary (shared generator) → executor timeline → streaming charts.

*As built so far:* Environment tab (`src/ui/environment.rs`, `KEY_SETTINGS` pinned; fetched once per app in the poller's `fetch` after the first snapshot), executor memory view (`src/ui/exec_memory.rs`; budget from the resource profile, verdict line), locality + records line in the stage header, `src/report.rs` (`summarize` → `Section`s, `to_markdown`, `write_bundle`), Summary view (`S`, auto for completed apps; `FetchStageSummaries` for the five slowest stages), `D` / `--dump` (`DumpLogs` request → `collect_log_tails`: kubectl `logs --tail` per pod with the sidecar retry, or the driver's HTTP page; `run_headless` in main.rs). All `Snapshot`/streaming types now derive `Serialize`. Executor timeline: `Sample` carries alive/removed executor counts, third sparkline on the Overview; Executors tab has an AGE column from `addTime`/`removeTime`. Streaming charts: `QueryStats` gained `add_batch_series`, `overhead_series`, `lag_series`, `dropped_series`; a second chart row on the Streaming tab (`draw_charts2`). Each verified against the live Connect server plus the mock (which gains `/environment`, memory metrics and `--completed` for the auto-summary).

## Out of scope (later)

(nothing left from the original list)

## Demo workload — `streaming-job/` ✅ done

A standalone crate (own `[workspace]`, not part of the sparkwatch package) using `spark-connect-rs 0.0.2` — chosen over the official `apache-spark-connect 4.2` because the latter requires a Spark 4.2+ server, while the 3.5 protocol is accepted by 3.5 and 4.x. Both need `protoc` at build time. Three queries off the `rate` source: `orders-raw` (parquet, partitioned by region), `orders-agg` (1-min window × customer, 30 s watermark, 40 % of orders on one hot key → shuffle skew), `orders-poison` (opt-in, `raise_error` every Nth order; restarted with a fresh checkpoint when it dies). Progress printed from `lastProgress`. Compiles and lints clean; **not yet run against a real Spark Connect server** (none available here — Java 25 is too new for Spark 3.5/4.x, and no distribution is installed), so the first real run may need small API adjustments. CI builds it with `protobuf-compiler` installed.

## Phase 8 — CI and releases ✅ done

- `.github/workflows/ci.yml`: `cargo fmt --check` + `clippy --all-targets -D warnings` on Linux, then `cargo test --locked`, a release build and `--help` on Linux/macOS/Windows. The tree was `cargo fmt`-ed and clippy-cleaned once to make that enforceable.
- `.github/workflows/release.yml` on `v*` tags: a job that refuses a tag not matching `Cargo.toml`'s version; a matrix of `x86_64`/`aarch64-unknown-linux-musl` (static, runs in any image), `x86_64`/`aarch64-apple-darwin`, `x86_64-pc-windows-msvc`; archives `sparkwatch-<ver>-<target>.tar.gz|zip` with the README; `SHA256SUMS`; `softprops/action-gh-release` with generated notes. Linux arm64 builds natively on `ubuntu-24.04-arm` rather than cross-compiling.
- `install.sh`: detects OS/arch, resolves the latest tag from the `/releases/latest` redirect (no API token), downloads the archive + `SHA256SUMS`, verifies, installs to `~/.local/bin` or `/usr/local/bin`; `SPARKWATCH_VERSION` / `SPARKWATCH_INSTALL_DIR` overrides.
- `Cargo.toml`: crate metadata and a `[profile.release]` with LTO, one codegen unit, `strip`, `panic = "abort"` for small assets. No license field yet — pick one before the first public release.

## Phase 7 — table filtering, Storage tab, config file ✅ done

- **Table filtering.** `App.filters: HashMap<Tab, String>` (lower-cased) and `filter_input` for the one being typed; `visible_jobs/stages/executors/sql/alerts/rdds(snapshot, filter)` are the single source of rows for both drawing and cursors, so a cursor can never point at a hidden row (`resync_all` after every snapshot and filter edit). `/` starts input (footer takes over, like the log view), `Enter` applies, `c` clears, `Esc` peels: text filter → job stage-filter → quit. Titles via `filtered_title`. Fields matched per tab are listed in the README.
- **Storage tab** (`8`). `/storage/rdd` joins the snapshot (404 → empty: the History Server has none); sorted by bytes. Summary line: storage memory used/max across executors, fullest executor, disk. `Enter` → `Detail::Rdd(id)` fetches `/storage/rdd/{id}` for `dataDistribution` (per executor: partitions held, bytes, share, how full that executor's storage is) and a partition summary; unpersisted → "has been unpersisted".
- **Config file.** `src/config.rs`: `[defaults] interval/timeout`, `[targets.<name>]` with `url` xor `k8s` (+ `namespace`, `app`); `deny_unknown_fields` so a typo is an error; missing file = empty config, malformed = error. `resolve_target` folds a named target into the parsed CLI, CLI flags winning; skipped under `--k8s` (positional is an app name). `--config PATH`, `--targets`. `interval`/`timeout` became `Option` on the CLI so the config's defaults can apply.
