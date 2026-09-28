# sparkwatch cheatsheet

## Starting it

| You have | Run |
|---|---|
| A driver UI on this machine | `sparkwatch` (defaults to `http://localhost:4040`) |
| A driver UI elsewhere | `sparkwatch http://driver-host:4040` |
| A History Server | `sparkwatch http://history:18080` → pick from the list, or `-a <app-id>` |
| Spark on Kubernetes (operator or spark-submit) | `sparkwatch --k8s -n <ns>` → pick a driver, or `sparkwatch --k8s -n <ns> <app>` |
| A Spark Connect server from the operator's `SparkConnect` resource | `sparkwatch --k8s -n <ns> <name>` (the `<name>-server` pod) |
| A named target in `~/.config/sparkwatch.toml` | `sparkwatch <name>` |

`<app>` under `--k8s` is the SparkApplication name, the driver pod name, or the
pod name minus `-driver`/`-server`. Only `Running` driver pods are listed.

```
-a, --app <ID>         Spark app id (History Server) — skips the picker
-n, --namespace <NS>   Kubernetes namespace (default: current context's)
-i, --interval <S>     poll interval, default 2
-t, --timeout <S>      HTTP timeout, default 5
    --config <PATH>    config file (default ~/.config/sparkwatch.toml)
    --targets          list configured targets and exit
```

`~/.config/sparkwatch.toml`:

```toml
[defaults]
interval = 2

[targets.kind]
k8s = true
namespace = "default"
app = "spark-connect"

[targets.history]
url = "http://history:18080"
```

## Keys

### Everywhere
| Key | |
|---|---|
| `q` / `Ctrl-C` | quit |
| `j` `k` `↓` `↑` | move / scroll |
| `PgUp` `PgDn` | by 10 (logs: by a page) |
| `g` `G` `Home` `End` | first / last |
| `r` | refresh now (thread view: re-fetch) |
| `p` | pause / resume polling |
| `+` `-` | poll interval up / down |
| `Esc` | back one layer: filter input → drill-down → table filter → job filter → quit |

### Tabs
| Key | Tab | Enter does |
|---|---|---|
| `1` | Overview | — |
| `2` | Jobs | narrow the Stages tab to this job's stages |
| `3` | Stages | stage drill-down (skew, executors, slowest/failed tasks) |
| `4` | Executors | — (`L` logs, `t` thread dump) |
| `5` | SQL | physical plan + node metrics |
| `6` | Failures | full error text (`s` open its stage, `L` its executor's log) |
| `7` | Streaming | — (`j`/`k` pick the query) |
| `8` | Storage | RDD distribution across executors |
| `Tab` / `Shift-Tab`, `←` `→`, `h` `l` | next / previous tab | |

### Tables
| Key | |
|---|---|
| `/` | filter the table — type, `Enter`; matches name / status / host / query / error |
| `c` | clear the filter |
| `a` | back to the application picker |
| `x` | acknowledge new failures (clears the red strip, keeps the log) |
| `L` | logs: Executors tab (selected executor), Failures tab (the alert's executor) |
| `t` | thread dump of the selected executor (Executors tab) |

### Stage drill-down (`Enter` on a stage)
| Key | |
|---|---|
| `f` | slowest tasks ↔ failed tasks |
| `j` `k` | move in the task table |
| `L` | logs of the selected task's executor, filtered to its exception |
| `Esc` | back |

### SQL drill-down (`Enter` on a query)
| Key | |
|---|---|
| `Tab` | scroll focus: plan ↔ nodes |
| `p` | plan-only full screen |
| `Esc` | back |

### Log viewer (`L`)
| Key | |
|---|---|
| `/` … `Enter` | filter lines (case-insensitive substring); `c` clears |
| `j` `k` `PgUp` `PgDn` | scroll (stops following) |
| `F` | follow the tail again |
| `w` | wrap long lines |
| `P` | previous container (`kubectl logs --previous`), k8s only |
| `o` | stderr ↔ stdout, YARN/standalone only |
| `t` | thread dump of the same executor |
| `Esc` | back to where you were |

### Thread dump (`t`)
| Key | |
|---|---|
| `e` | expand / collapse frames |
| `/` … `Enter` | filter by thread name or frame; `c` clears |
| `r` | re-fetch |
| `L` | logs of the same executor |
| `Esc` | back |

## Reading the screen

- Header badge: `LIVE` / `PAUSED` / `ERROR` (last good data stays; error in the footer).
- Red strip under the header = unacknowledged failures; `6` for the list, `x` to clear.
- `Failures (N)` tab title red = new failures; `Streaming ▲` red = a query is falling behind.
- Stage drill-down: `⚠ ×N` = metric's max is N× its median (skew); red executor rows say why; yellow tasks are stragglers.
- Overview "Running jobs" is empty between micro-batches — for streaming apps use `5` and `7`.

## Spark Connect demo on Kubernetes (kind + Spark Operator)

```bash
kubectl apply -f streaming-job/k8s/spark-connect.yaml
kubectl wait sparkconnect/spark-connect --for=jsonpath='{.status.state}'=Ready --timeout=10m
kubectl port-forward svc/spark-connect-server 15002:15002          # terminal A, keep open
./streaming-job/target/debug/sparkwatch-streaming-job --remote sc://localhost:15002 --output /data/streaming-job   # terminal B
sparkwatch --k8s spark-connect                                       # terminal C
```

Stopping:

```bash
# just the queries: Ctrl-C in the job's terminal (stops them cleanly; checkpoints resume next run)
kubectl delete pod spark-connect-server                    # queries orphaned on the server? recreate it
kubectl delete -f streaming-job/k8s/spark-connect.yaml     # everything: server, executors, service, PVC
pkill -f "port-forward svc/spark-connect-server"           # the 15002 forward
```

Job options: `--rows-per-second 2000` `--partitions 8` `--hot-share 0.4`
`--poison-every 50000` (deliberate failures) `--duration-min 30`.

Same logs without sparkwatch:

```bash
kubectl logs spark-connect-server -c spark-kubernetes-driver -f
kubectl logs spark-connect-exec-1 -f
kubectl get pods -l sparkoperator.k8s.io/connect-name=spark-connect -o wide
kubectl describe sparkconnect spark-connect | tail -30
```

## Development

```bash
cargo test                            # unit tests
python3 dev/mock_spark.py --single &  # fake Spark API on :4040 (also: --no-sql, or no flag for two apps)
cargo run
# headless smoke test: pty + terminal-emulator replay
(sleep 5; printf '6'; sleep 1; printf 'q') | script -q out.txt sh -c 'stty cols 170 rows 50; ./target/debug/sparkwatch'
python3 dev/screens.py out.txt 170 50 'Failures (11, 11 new)' '!ERROR'
SPARKWATCH_KEYLOG=/tmp/keys.log sparkwatch   # log every key press received
```

Release: bump `version` in `Cargo.toml`, then `git tag vX.Y.Z && git push origin vX.Y.Z`.
