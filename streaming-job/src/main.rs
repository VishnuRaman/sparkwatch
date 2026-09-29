//! A long-running Structured Streaming workload, submitted over Spark Connect.
//!
//! It exists to give sparkwatch something realistic to watch: a synthetic
//! order stream (the built-in `rate` source, so no Kafka is needed) fanned
//! out into
//!
//! * `orders-raw` — every order appended to parquet (steady, I/O-bound),
//! * `orders-agg` — 1-minute windowed revenue per customer with a watermark
//!   (stateful; one hot customer makes the shuffle skewed, which the stage
//!   drill-down should flag),
//! * `orders-poison` (opt-in) — a copy that raises an error every N rows, so
//!   tasks fail, the query dies and gets restarted, and the Failures tab has
//!   something to show.
//!
//! Progress is printed every few seconds from `lastProgress`, and Ctrl-C
//! stops the queries cleanly.

use anyhow::{Context, Result};
use clap::Parser;
use spark_connect_rs::functions::{col, count, expr, sum, window};
use spark_connect_rs::spark::write_stream_operation_start::Trigger;
use spark_connect_rs::streaming::{OutputMode, StreamingQuery};
use spark_connect_rs::{DataFrame, SparkSession, SparkSessionBuilder};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Parser, Debug)]
#[command(name = "streaming-job", version, about)]
struct Cli {
    /// Spark Connect endpoint
    #[arg(long, env = "SPARK_REMOTE", default_value = "sc://localhost:15002")]
    remote: String,

    /// Synthetic orders per second across all partitions
    #[arg(long, default_value_t = 2000)]
    rows_per_second: u32,

    /// Partitions of the rate source (≈ parallelism of the input)
    #[arg(long, default_value_t = 8)]
    partitions: u32,

    /// Share of orders that go to one hot customer (0 = no skew)
    #[arg(long, default_value_t = 0.4)]
    hot_share: f64,

    /// Where parquet output and checkpoints go (a path the executors can
    /// reach: local for a local server, s3a://… or a PVC on a cluster)
    #[arg(
        long,
        env = "JOB_OUTPUT",
        default_value = "/tmp/sparkwatch-streaming-job"
    )]
    output: String,

    /// Micro-batch interval for the raw sink
    #[arg(long, default_value = "5 seconds")]
    raw_trigger: String,

    /// Micro-batch interval for the aggregation
    #[arg(long, default_value = "10 seconds")]
    agg_trigger: String,

    /// Also run a query that fails on every Nth order (0 = off), and keep
    /// restarting it, so failures keep appearing
    #[arg(long, default_value_t = 0)]
    poison_every: u64,

    /// Stop after this many minutes (default: run until Ctrl-C)
    #[arg(long)]
    duration_min: Option<u64>,

    /// How often to print progress
    #[arg(long, default_value_t = 10)]
    report_secs: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    println!("connecting to {}", cli.remote);
    let spark = SparkSessionBuilder::remote(&cli.remote)
        .app_name("sparkwatch-streaming-job")
        .build()
        .await
        .with_context(|| format!("connecting to Spark Connect at {}", cli.remote))?;
    println!("connected · session {}", spark.session_id());

    let mut queries: Vec<StreamingQuery> = vec![
        start_raw(&spark, &cli).await?,
        start_agg(&spark, &cli).await?,
    ];
    if cli.poison_every > 0 {
        queries.push(start_poison(&spark, &cli).await?);
    }
    for q in &queries {
        println!(
            "started {:<14} id {}  run {}",
            q.name().unwrap_or_default(),
            q.id(),
            q.run_id()
        );
    }

    let started = Instant::now();
    let deadline = cli.duration_min.map(|m| Duration::from_secs(m * 60));
    let mut tick = tokio::time::interval(Duration::from_secs(cli.report_secs.max(1)));
    tick.tick().await; // the first tick fires immediately; skip it

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("\nCtrl-C: stopping queries…");
                break;
            }
            _ = tick.tick() => {
                report(&queries).await;
                // The poison query is meant to die; bring it back so the
                // failures keep coming.
                if cli.poison_every > 0 {
                    restart_dead_poison(&spark, &cli, &mut queries).await;
                }
                if deadline.is_some_and(|d| started.elapsed() >= d) {
                    println!("duration reached: stopping queries…");
                    break;
                }
            }
        }
    }

    for q in &queries {
        if let Err(e) = q.stop().await {
            eprintln!("stopping {}: {e}", q.name().unwrap_or_default());
        }
    }
    println!("done after {}", fmt_dur(started.elapsed()));
    Ok(())
}

/// The synthetic order stream. `rate` emits (timestamp, value) rows; the
/// rest is derived so the data has a shape worth aggregating.
fn orders(spark: &SparkSession, cli: &Cli) -> Result<DataFrame> {
    let raw = spark
        .read_stream()
        .format("rate")
        .option("rowsPerSecond", &cli.rows_per_second.to_string())
        .option("numPartitions", &cli.partitions.to_string())
        .option("rampUpTime", "10s")
        .load(None)
        .context("creating the rate source")?;

    // A hot customer takes `hot_share` of all orders: the per-customer
    // aggregation then has one partition doing most of the work, which is
    // exactly the skew a stage drill-down should surface.
    let customer = format!(
        "CASE WHEN rand() < {} THEN 42 ELSE CAST(value % 1000 AS INT) END",
        cli.hot_share
    );
    Ok(raw.select([
        col("timestamp").alias("event_time"),
        col("value").alias("order_id"),
        expr(&customer).alias("customer_id"),
        expr("ROUND(rand() * 250 + 1, 2)").alias("amount"),
        expr("element_at(array('eu', 'us', 'apac'), CAST(value % 3 AS INT) + 1)").alias("region"),
    ]))
}

async fn start_raw(spark: &SparkSession, cli: &Cli) -> Result<StreamingQuery> {
    orders(spark, cli)?
        .write_stream()
        .format("parquet")
        .option("path", &format!("{}/orders", cli.output))
        .option(
            "checkpointLocation",
            &format!("{}/checkpoints/orders-raw", cli.output),
        )
        .partition_by(["region"])
        .output_mode(OutputMode::Append)
        .trigger(Trigger::ProcessingTimeInterval(cli.raw_trigger.clone()))
        .query_name("orders-raw")
        .start(None)
        .await
        .context("starting orders-raw")
}

async fn start_agg(spark: &SparkSession, cli: &Cli) -> Result<StreamingQuery> {
    let df = orders(spark, cli)?
        // Late orders up to 30 s old still land in their window; older
        // ones are dropped and counted under numRowsDroppedByWatermark.
        .with_watermark("event_time", "30 seconds")
        .group_by(Some([
            window(col("event_time"), "1 minute", None, None),
            col("customer_id"),
        ]))
        .agg([
            sum(col("amount")).alias("revenue"),
            count(col("order_id")).alias("orders"),
        ]);

    df.write_stream()
        .format("parquet")
        .option("path", &format!("{}/revenue_by_customer", cli.output))
        .option(
            "checkpointLocation",
            &format!("{}/checkpoints/orders-agg", cli.output),
        )
        .output_mode(OutputMode::Append)
        .trigger(Trigger::ProcessingTimeInterval(cli.agg_trigger.clone()))
        .query_name("orders-agg")
        .start(None)
        .await
        .context("starting orders-agg")
}

/// A copy of the raw stream that raises on every `poison_every`-th order.
/// Spark retries the task four times, then fails the stage and the query.
async fn start_poison(spark: &SparkSession, cli: &Cli) -> Result<StreamingQuery> {
    let guard = format!(
        "CASE WHEN order_id > 0 AND order_id % {} = 0 THEN raise_error(concat('poison order ', order_id)) ELSE order_id END",
        cli.poison_every
    );
    orders(spark, cli)?
        .with_column("order_id", expr(&guard))
        .write_stream()
        .format("parquet")
        .option("path", &format!("{}/orders_poison", cli.output))
        .option(
            "checkpointLocation",
            &format!("{}/checkpoints/orders-poison", cli.output),
        )
        .output_mode(OutputMode::Append)
        .trigger(Trigger::ProcessingTimeInterval(cli.raw_trigger.clone()))
        .query_name("orders-poison")
        .start(None)
        .await
        .context("starting orders-poison")
}

async fn restart_dead_poison(spark: &SparkSession, cli: &Cli, queries: &mut [StreamingQuery]) {
    let Some(i) = queries
        .iter()
        .position(|q| q.name().as_deref() == Some("orders-poison"))
    else {
        return;
    };
    let alive = queries[i].is_active().await.unwrap_or(false);
    if alive {
        return;
    }
    println!("orders-poison died (as intended); restarting it");
    // The checkpoint remembers the poisoned offset, so a restart would hit
    // the same row again. Fresh checkpoint each time.
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let cli_fresh = Cli {
        output: format!("{}/poison-run-{stamp}", cli.output),
        ..clone_cli(cli)
    };
    match start_poison(spark, &cli_fresh).await {
        Ok(q) => queries[i] = q,
        Err(e) => eprintln!("restart failed: {e:#}"),
    }
}

fn clone_cli(c: &Cli) -> Cli {
    Cli {
        remote: c.remote.clone(),
        rows_per_second: c.rows_per_second,
        partitions: c.partitions,
        hot_share: c.hot_share,
        output: c.output.clone(),
        raw_trigger: c.raw_trigger.clone(),
        agg_trigger: c.agg_trigger.clone(),
        poison_every: c.poison_every,
        duration_min: c.duration_min,
        report_secs: c.report_secs,
    }
}

/// One line per query from `lastProgress`: the same numbers sparkwatch's
/// Streaming tab derives from the driver log.
async fn report(queries: &[StreamingQuery]) {
    for q in queries {
        let name = q.name().unwrap_or_else(|| q.id());
        let active = q.is_active().await.unwrap_or(false);
        match q.last_progress().await {
            Ok(p) if !p.is_null() => {
                let n = |k: &str| p.get(k).and_then(|v| v.as_i64()).unwrap_or(0);
                let f = |k: &str| p.get(k).and_then(|v| v.as_f64()).unwrap_or(0.0);
                let trigger = p
                    .pointer("/durationMs/triggerExecution")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);
                let state_rows: i64 = p
                    .get("stateOperators")
                    .and_then(|v| v.as_array())
                    .map(|ops| {
                        ops.iter()
                            .filter_map(|o| o.get("numRowsTotal")?.as_i64())
                            .sum()
                    })
                    .unwrap_or(0);
                let behind =
                    f("processedRowsPerSecond") < f("inputRowsPerSecond") && n("numInputRows") > 0;
                println!(
                    "{name:<14} batch {:<6} trigger {:>6} ms  in {:>7} rows  {:>8.0}/s in  {:>8.0}/s processed{}  state {state_rows}{}",
                    n("batchId"),
                    trigger,
                    n("numInputRows"),
                    f("inputRowsPerSecond"),
                    f("processedRowsPerSecond"),
                    if behind { "  ▲ behind" } else { "" },
                    if active { "" } else { "  [stopped]" },
                );
            }
            Ok(_) => println!(
                "{name:<14} no progress yet{}",
                if active { "" } else { "  [stopped]" }
            ),
            Err(e) => println!("{name:<14} progress unavailable: {e}"),
        }
    }
}

fn fmt_dur(d: Duration) -> String {
    let s = d.as_secs();
    format!("{}h{:02}m{:02}s", s / 3600, (s % 3600) / 60, s % 60)
}
