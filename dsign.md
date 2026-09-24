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

## Phase 6 (future) — Streaming tab

Built on the Phase 5 driver log stream: recognise `StreamingQueryProgress` lines (JSON after `Streaming query made progress:`), keep a ring of the last ~200 batches per query, and render a **Streaming** tab: per query, batch duration / input rate / processing rate / watermark lag as sparklines, latest batch's numbers, state-store size trend, and a red flag when `processedRowsPerSecond < inputRowsPerSecond` over the last N batches (falling behind). Terminal equivalent of the web UI's Structured Streaming page, which the REST API does not expose.

## Out of scope (later)

Table filtering (`/`) on the main tabs, storage/RDD tab, streaming tab, config file for endpoints, GitHub release workflow with prebuilt binaries (after the phases above).
