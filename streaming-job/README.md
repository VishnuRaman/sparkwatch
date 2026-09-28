# streaming-job

A long-running Structured Streaming workload, written in Rust and submitted
over [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html),
that gives sparkwatch something realistic to watch. No Kafka or data needed:
the built-in `rate` source generates synthetic orders.

| Query | What it does | What it exercises in sparkwatch |
|---|---|---|
| `orders-raw` | every order appended to parquet, partitioned by region | steady micro-batches, executors, storage |
| `orders-agg` | 1-minute windowed revenue per customer, 30 s watermark | stateful streaming (state rows / memory, watermark lag); one hot customer gets 40 % of orders, so the shuffle is **skewed** — the stage drill-down flags it |
| `orders-poison` (opt-in) | a copy that calls `raise_error` on every Nth order, and is restarted when it dies | failed tasks → failed stage → failed query on the Failures tab, `L` into the executor log |

## Build

Needs a Rust toolchain and `protoc` (the Spark Connect client compiles the
gRPC protos at build time):

```bash
brew install protobuf        # macOS; apt install protobuf-compiler on Debian/Ubuntu
cd streaming-job
cargo build --release
```

## Run

Against a local Spark Connect server (Spark 3.5+; the client speaks the 3.5
protocol, which newer servers accept):

```bash
$SPARK_HOME/sbin/start-connect-server.sh --packages org.apache.spark:spark-connect_2.12:3.5.3   # Spark 3.5
# $SPARK_HOME/sbin/start-connect-server.sh                                                     # Spark 4.x
./target/release/sparkwatch-streaming-job --remote sc://localhost:15002 --rows-per-second 2000
```

Then, in another terminal, point sparkwatch at the server's UI — with Spark
Connect the *server* is the driver, so that is the application you monitor:

```bash
sparkwatch http://localhost:4040
```

The `7` (Streaming) tab needs the driver log; locally, tail it with the log
URLs sparkwatch shows, or run the server under `--k8s` (below).

On Kubernetes with the Spark Operator, run the Connect server as a
`SparkApplication` whose main class is
`org.apache.spark.sql.connect.service.SparkConnectServer`, expose port
`15002` with a Service, `kubectl port-forward svc/<name> 15002:15002`, and
run the job against `sc://localhost:15002`. Then
`sparkwatch --k8s -n <ns> <name>` watches it — logs, thread dumps and the
Streaming tab all work through `kubectl`.

Output and checkpoints go under `--output` (default
`/tmp/sparkwatch-streaming-job`); on a cluster give a path the executors can
reach (`s3a://…`, a PVC mount).

## Options

```
--remote <URL>           Spark Connect endpoint [env: SPARK_REMOTE] [default: sc://localhost:15002]
--rows-per-second <N>    orders per second [default: 2000]
--partitions <N>         rate-source partitions [default: 8]
--hot-share <0..1>       share of orders for the hot customer [default: 0.4]
--output <PATH>          parquet + checkpoints [env: JOB_OUTPUT]
--raw-trigger <DUR>      micro-batch interval of orders-raw [default: "5 seconds"]
--agg-trigger <DUR>      micro-batch interval of orders-agg [default: "10 seconds"]
--poison-every <N>       also run orders-poison failing every Nth order (0 = off)
--duration-min <MIN>     stop after this long (default: until Ctrl-C)
--report-secs <S>        progress print interval [default: 10]
```

Every `--report-secs` it prints one line per query from `lastProgress` —
batch id, trigger duration, rows, input vs processed rate with a `▲ behind`
marker — the same numbers sparkwatch's Streaming tab reconstructs from the
driver log. Ctrl-C stops the queries cleanly.

Make it fall behind on purpose with a rate the cluster can't keep up with
(`--rows-per-second 200000 --partitions 2`), or make it fail with
`--poison-every 50000`.
