"""Tiny fake of the Spark REST API for running sparkwatch without a cluster.

    python3 dev/mock_spark.py &          # two apps -> picker
    python3 dev/mock_spark.py --single & # one app  -> straight to the monitor
    python3 dev/mock_spark.py --no-sql & # no /sql endpoint (non-SQL app / old Spark)
    cargo run
"""
import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

ETL = {"id": "app-20260923-0001", "name": "etl-nightly",
       "attempts": [{"startTime": "2026-09-23T09:00:00.000GMT", "endTime": "",
                     "lastUpdated": "", "duration": 4200000, "sparkUser": "vishnu",
                     "completed": False, "appSparkVersion": "3.5.1"}]}
REPORT = {"id": "app-20260922-0007", "name": "daily-report",
          "attempts": [{"startTime": "2026-09-22T02:00:00.000GMT", "endTime": "2026-09-22T02:41:00.000GMT",
                        "lastUpdated": "", "duration": 2460000, "sparkUser": "airflow",
                        "completed": True, "appSparkVersion": "3.4.2"}]}
APPS = [ETL] if "--single" in sys.argv else [ETL, REPORT]

JOBS = [{"jobId": 3, "name": "save at Writer.scala:88", "status": "RUNNING",
         "numTasks": 400, "numActiveTasks": 16, "numCompletedTasks": 150,
         "numSkippedTasks": 0, "numFailedTasks": 2, "stageIds": [7, 8, 9],
         "numCompletedStages": 1, "submissionTime": "2026-09-23T10:05:11.000GMT"},
        {"jobId": 2, "name": "count at Main.scala:40", "status": "SUCCEEDED",
         "numTasks": 200, "numCompletedTasks": 200, "stageIds": [5, 6],
         "numCompletedStages": 2, "submissionTime": "2026-09-23T09:50:00.000GMT"},
        {"jobId": 1, "name": "collect at Main.scala:22", "status": "FAILED",
         "numTasks": 50, "numCompletedTasks": 12, "numFailedTasks": 4, "stageIds": [4],
         "submissionTime": "2026-09-23T09:10:00.000GMT"}]
MiB = 1024 * 1024
STAGES = [{"status": "ACTIVE", "stageId": 9, "attemptId": 0, "name": "mapPartitions at Writer.scala:88",
           "numTasks": 200, "numActiveTasks": 16, "numCompleteTasks": 97, "numFailedTasks": 2,
           "inputBytes": 5368709120, "shuffleReadBytes": 2147483648, "shuffleWriteBytes": 0,
           "memoryBytesSpilled": 536870912, "executorRunTime": 900000, "jvmGcTime": 120000,
           "schedulingPool": "default",
           "executorSummary": {
               "1": {"taskTime": 700000, "succeededTasks": 60, "failedTasks": 0, "shuffleRead": 1500 * MiB},
               "2": {"taskTime": 150000, "succeededTasks": 30, "failedTasks": 2, "shuffleRead": 400 * MiB,
                     "memoryBytesSpilled": 512 * MiB, "isExcludedForStage": True},
               # Executor 3 is a slow node: 7 tasks in as long as exec 2 took for 32.
               "3": {"taskTime": 250000, "succeededTasks": 7, "failedTasks": 0, "shuffleRead": 148 * MiB}}},
          {"status": "COMPLETE", "stageId": 8, "attemptId": 0, "name": "exchange at Writer.scala:70",
           "numTasks": 200, "numCompleteTasks": 200, "shuffleWriteBytes": 2147483648, "executorRunTime": 400000,
           "schedulingPool": "default",
           "executorSummary": {"1": {"taskTime": 200000, "succeededTasks": 100},
                               "2": {"taskTime": 200000, "succeededTasks": 100}}},
          {"status": "FAILED", "stageId": 4, "attemptId": 0, "name": "collect at Main.scala:22",
           "numTasks": 50, "numCompleteTasks": 12, "numFailedTasks": 4, "executorRunTime": 30000,
           "schedulingPool": "default",
           "failureReason": "Job aborted due to stage failure: Task 3 in stage 4.0 failed 4 times, most recent failure: "
                            "Lost task 3.3 in stage 4.0 (TID 412) (10.0.1.10 executor 2): java.lang.OutOfMemoryError: Java heap space",
           "executorSummary": {"2": {"taskTime": 30000, "succeededTasks": 12, "failedTasks": 4}}}]

# Task metric quantiles for p5,p25,p50,p75,p95,max. Stage 9 has a badly skewed
# partition (max 91 s vs median 8 s, and 1 GiB shuffle read vs 4 MiB).
SUMMARIES = {
    9: {"quantiles": [0.05, 0.25, 0.5, 0.75, 0.95, 1.0],
        "duration": [1000, 2500, 8000, 9500, 30000, 91000],
        "jvmGcTime": [0, 10, 50, 100, 900, 12000],
        "schedulerDelay": [2, 3, 4, 5, 8, 20],
        "memoryBytesSpilled": [0, 0, 0, 0, 0, 512 * MiB],
        "inputMetrics": {"bytesRead": [20 * MiB, 24 * MiB, 26 * MiB, 28 * MiB, 32 * MiB, 40 * MiB]},
        "shuffleReadMetrics": {"readBytes": [1 * MiB, 2 * MiB, 4 * MiB, 8 * MiB, 32 * MiB, 1025 * MiB],
                               "fetchWaitTime": [0, 0, 0, 0, 0, 300]},
        "shuffleWriteMetrics": {"writeBytes": [0, 0, 0, 0, 0, 0]}},
    8: {"quantiles": [0.05, 0.25, 0.5, 0.75, 0.95, 1.0],
        "duration": [1800, 1900, 2000, 2100, 2300, 2600],
        "jvmGcTime": [0, 5, 10, 15, 30, 60],
        "shuffleWriteMetrics": {"writeBytes": [9 * MiB, 10 * MiB, 10 * MiB, 11 * MiB, 12 * MiB, 13 * MiB]}},
}


def task(tid, idx, exec_id, host, ms, status="SUCCESS", **kw):
    t = {"taskId": tid, "index": idx, "attempt": kw.pop("attempt", 0), "executorId": exec_id, "host": host,
         "status": status, "taskLocality": "PROCESS_LOCAL", "speculative": kw.pop("speculative", False),
         "launchTime": "2026-09-23T10:05:12.000GMT"}
    if ms is not None:
        t["duration"] = ms
        t["taskMetrics"] = {"executorRunTime": ms - 100, "jvmGcTime": kw.pop("gc", ms // 100),
                            "memoryBytesSpilled": kw.pop("spill", 0),
                            "shuffleReadMetrics": {"remoteBytesRead": kw.pop("read", 4 * MiB), "localBytesRead": 0}}
    t.update(kw)
    return t


OOM = ("java.lang.OutOfMemoryError: Java heap space\n\tat java.util.Arrays.copyOf(Arrays.java:3236)\n"
       "\tat org.apache.spark.sql.execution.aggregate.HashAggregateExec...")
LOST = "ExecutorLostFailure (executor 2 exited caused by one of the running tasks) Reason: Container killed on request. Exit code is 137"
TASKS = {
    9: {"slowest": [task(4021, 17, "1", "10.0.1.9", 91000, read=1025 * MiB, spill=512 * MiB, gc=12000),
                    task(4088, 84, "2", "10.0.1.10", 31000),
                    task(4090, 86, "2", "10.0.1.10", 30000, speculative=True),
                    task(4011, 7, "1", "10.0.1.9", 9800),
                    task(4012, 8, "3", "10.0.1.11", 9500),
                    task(4013, 9, "1", "10.0.1.9", 9100),
                    task(4014, 10, "3", "10.0.1.11", 8200),
                    task(4015, 11, "1", "10.0.1.9", 8000)],
        "failed": [task(4050, 46, "2", "10.0.1.10", 4200, "FAILED", errorMessage=LOST),
                   task(4051, 46, "2", "10.0.1.10", 3900, "FAILED", attempt=1, errorMessage=LOST)]},
    8: {"slowest": [task(3000 + i, i, str(1 + i % 2), "10.0.1.%d" % (9 + i % 2), 2600 - i * 40) for i in range(10)],
        "failed": []},
    4: {"slowest": [task(400 + i, i, "2", "10.0.1.10", 1000 + i * 10) for i in range(5)],
        "failed": [task(412, 3, "2", "10.0.1.10", None, "FAILED", attempt=a, errorMessage=OOM) for a in range(4)]},
}
PLAN = """*(3) HashAggregate(keys=[customer_id#12L], functions=[sum(amount#15)])
+- Exchange hashpartitioning(customer_id#12L, 200), ENSURE_REQUIREMENTS, [plan_id=88]
   +- *(2) HashAggregate(keys=[customer_id#12L], functions=[partial_sum(amount#15)])
      +- *(2) Project [customer_id#12L, amount#15]
         +- *(2) BroadcastHashJoin [customer_id#12L], [id#40L], Inner, BuildRight, false
            :- *(2) Filter isnotnull(customer_id#12L)
            :  +- *(2) ColumnarToRow
            :     +- FileScan parquet events[customer_id#12L,amount#15] Batched: true, DataFilters: [isnotnull(customer_id#12L)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[s3://bucket/events/dt=2026-09-23], PartitionFilters: [], PushedFilters: [IsNotNull(customer_id)], ReadSchema: struct<customer_id:bigint,amount:double>
            +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, false]),false), [plan_id=84]
               +- *(1) Filter isnotnull(id#40L)
                  +- *(1) ColumnarToRow
                     +- FileScan parquet customers[id#40L] Batched: true, Format: Parquet, Location: InMemoryFileIndex(1 paths)[s3://bucket/customers]"""


def node(nid, name, codegen, **metrics):
    return {"nodeId": nid, "nodeName": name, "wholeStageCodegenId": codegen,
            "metrics": [{"name": k.replace("_", " "), "value": v} for k, v in metrics.items()]}


SQL_NODES = [
    node(0, "HashAggregate", 3, number_of_output_rows="1,204", spill_size="0.0 B", peak_memory="16.0 MiB",
         time_in_aggregation_build="2.1 s"),
    node(1, "Exchange", None, number_of_partitions="200", shuffle_bytes_written="2.0 GiB",
         shuffle_records_written="41,000,000", fetch_wait_time="18.3 s", spill_size="512.0 MiB"),
    node(2, "HashAggregate", 2, number_of_output_rows="41,000,000", spill_size="512.0 MiB", peak_memory="2.0 GiB"),
    node(3, "Project", 2),
    node(4, "BroadcastHashJoin", 2, number_of_output_rows="98,000,000"),
    node(5, "Filter", 2, number_of_output_rows="98,000,000"),
    node(6, "Scan parquet events", None, number_of_output_rows="98,000,000", number_of_files_read="1,200",
         size_of_files_read="5.0 GiB", scan_time="41.0 s"),
    node(7, "BroadcastExchange", None, number_of_output_rows="50,000", data_size="1.2 MiB", time_to_build="0.4 s"),
    node(8, "Scan parquet customers", None, number_of_output_rows="50,000", size_of_files_read="1.1 MiB"),
]


def execution(eid, status, desc, ms, running=(), ok=(), failed=(), error=None, submitted="2026-09-23T10:05:11.000GMT"):
    e = {"id": eid, "status": status, "description": desc, "planDescription": "", "submissionTime": submitted,
         "duration": ms, "runningJobIds": list(running), "successJobIds": list(ok), "failedJobIds": list(failed),
         "nodes": [], "edges": []}
    if error:
        e["errorMessage"] = error
    return e


SQL_ERROR = ("org.apache.spark.SparkException: Job aborted due to stage failure: Task 3 in stage 4.0 failed 4 times\n"
             "\tat org.apache.spark.scheduler.DAGScheduler.failJobAndIndependentStages(DAGScheduler.scala:2856)")
SQL = [
    execution(10, "COMPLETED", "count at Main.scala:40", 12300, ok=[2], submitted="2026-09-23T09:50:00.000GMT"),
    execution(11, "FAILED", "collect at Main.scala:22\n== Physical Plan ==\n...", 8100, failed=[1], error=SQL_ERROR,
              submitted="2026-09-23T09:10:00.000GMT"),
    execution(12, "COMPLETED", "SELECT customer_id, sum(amount) FROM events e JOIN customers c ON e.customer_id = c.id GROUP BY customer_id",
              41200, ok=[3, 4], submitted="2026-09-23T10:00:00.000GMT"),
    execution(13, "RUNNING", "save at Writer.scala:88", 95000, running=[3], ok=[]),
    # Micro-batches of a streaming query: the description carries ids.
    execution(14, "COMPLETED", "orders-agg\nid = 3b8f1e2c-0000-4000-8000-000000000001\nrunId = 9c0d2a11-0000-4000-8000-000000000002\nbatch = 4121",
              1650, ok=[5], submitted="2026-09-25T10:08:41.000GMT"),
    execution(15, "COMPLETED", "orders-agg\nid = 3b8f1e2c-0000-4000-8000-000000000001\nrunId = 9c0d2a11-0000-4000-8000-000000000002\nbatch = 4122",
              1500, ok=[6], submitted="2026-09-25T10:08:42.000GMT"),
    execution(16, "RUNNING", "orders-agg\nid = 3b8f1e2c-0000-4000-8000-000000000001\nrunId = 9c0d2a11-0000-4000-8000-000000000002\nbatch = 4124",
              400, running=[7], submitted="2026-09-25T10:08:44.000GMT"),
]
SQL_ENABLED = "--no-sql" not in sys.argv
if "--many-queries" in sys.argv:
    # Twenty streaming queries, one micro-batch each, to exercise the Streaming tab's query list.
    for i in range(20):
        SQL.append(execution(100 + i, "COMPLETED",
                             "stream-%02d\nid = q%02d\nrunId = r%02d\nbatch = %d" % (i, i, i, 10 + i),
                             500 + i * 10, ok=[200 + i], submitted="2026-09-25T10:09:%02d.000GMT" % i))

def logs_for(eid):
    return {"stdout": "http://127.0.0.1:4040/node/containerlogs/container_%s/vishnu/stdout?start=-4096" % eid,
            "stderr": "http://127.0.0.1:4040/node/containerlogs/container_%s/vishnu/stderr?start=-4096" % eid}


EXECS = [{"id": "driver", "hostPort": "10.0.1.1:7078", "isActive": True, "totalCores": 0, "executorLogs": logs_for("driver")},
         {"id": "1", "hostPort": "10.0.1.9:7079", "isActive": True, "totalCores": 4, "executorLogs": logs_for("1"),
          "activeTasks": 4, "failedTasks": 2, "completedTasks": 610, "totalDuration": 1820000,
          "totalGCTime": 240000, "memoryUsed": 1610612736, "maxMemory": 4294967296,
          "totalShuffleRead": 1073741824},
         {"id": "2", "hostPort": "10.0.1.10:7079", "isActive": False, "totalCores": 4,
          "completedTasks": 300, "failedTasks": 6, "totalDuration": 900000, "totalGCTime": 30000,
          "maxMemory": 4294967296, "removeTime": "2026-09-23T10:20:00.000GMT",
          "removeReason": "Container killed on request. Exit code is 137 (OOMKilled by the kubelet: memory limit exceeded)"},
         {"id": "3", "hostPort": "10.0.1.11:7079", "isActive": True, "totalCores": 4,
          "activeTasks": 1, "completedTasks": 40, "totalDuration": 250000, "totalGCTime": 90000,
          "memoryUsed": 536870912, "maxMemory": 4294967296, "isExcluded": True}]

THREADS = [
    {"threadId": 45, "threadName": "Executor task launch worker for task 3.0 in stage 9.0 (TID 4088)",
     "threadState": "BLOCKED", "blockedByThreadId": 12, "blockedByLock": "java.lang.Object@1a2b3c",
     "lockName": "java.lang.Object@1a2b3c", "lockOwnerName": "shuffle-client-2", "holdingLocks": [],
     "stackTrace": {"elems": ["org.apache.spark.storage.BlockManager.getRemoteBytes(BlockManager.scala:1032)",
                              "org.apache.spark.shuffle.BlockStoreShuffleReader.read(BlockStoreShuffleReader.scala:88)",
                              "org.apache.spark.sql.execution.ShuffledRowRDD.compute(ShuffledRowRDD.scala:210)",
                              "org.apache.spark.scheduler.Task.run(Task.scala:141)",
                              "java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)",
                              "java.lang.Thread.run(Thread.java:750)"]}},
    {"threadId": 12, "threadName": "shuffle-client-2", "threadState": "RUNNABLE", "holdingLocks": ["java.lang.Object@1a2b3c"],
     "stackTrace": {"elems": ["sun.nio.ch.EPoll.wait(Native Method)", "io.netty.channel.epoll.EpollEventLoop.run(EpollEventLoop.java:364)"]}},
    {"threadId": 46, "threadName": "Executor task launch worker for task 8.0 in stage 9.0 (TID 4090)",
     "threadState": "RUNNABLE",
     "stackTrace": {"elems": ["org.apache.spark.sql.execution.aggregate.HashAggregateExec.doExecute(HashAggregateExec.scala:120)",
                              "org.apache.spark.scheduler.Task.run(Task.scala:141)"]}},
    {"threadId": 7, "threadName": "dispatcher-Executor", "threadState": "WAITING", "lockName": "java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject@77",
     "stackTrace": {"elems": ["sun.misc.Unsafe.park(Native Method)", "java.util.concurrent.LinkedBlockingQueue.take(LinkedBlockingQueue.java:442)"]}},
    {"threadId": 2, "threadName": "Reference Handler", "threadState": "TIMED_WAITING",
     "stackTrace": {"elems": ["java.lang.Object.wait(Native Method)"]}},
]


PROGRESS = """26/09/25 10:00:05 INFO MicroBatchExecution: Streaming query made progress: {
  "id" : "3b8f1e2c-0000-4000-8000-000000000001",
  "runId" : "9c0d2a11-0000-4000-8000-000000000002",
  "name" : "orders-agg",
  "timestamp" : "2026-09-25T10:%(m)02d:%(s)02d.000Z",
  "batchId" : %(batch)d,
  "batchDuration" : %(trig)d,
  "numInputRows" : 12400,
  "inputRowsPerSecond" : 10300.0,
  "processedRowsPerSecond" : %(out).1f,
  "durationMs" : {
    "addBatch" : %(trig)d,
    "getBatch" : 10,
    "queryPlanning" : 30,
    "triggerExecution" : %(trig)d,
    "walCommit" : 15
  },
  "eventTime" : {
    "watermark" : "2026-09-25T09:56:53.000Z"
  },
  "stateOperators" : [ {
    "operatorName" : "stateStoreSave",
    "numRowsTotal" : 1500000,
    "numRowsUpdated" : 12000,
    "memoryUsedBytes" : 209715200
  } ],
  "sources" : [ {
    "description" : "KafkaV2[Subscribe[orders]]",
    "numInputRows" : 12400,
    "inputRowsPerSecond" : 10300.0,
    "processedRowsPerSecond" : %(out).1f
  } ],
  "sink" : {
    "description" : "DeltaSink[s3://bucket/orders_agg]",
    "numOutputRows" : 3100
  }
}"""


def log_page(eid, stream):
    lines = ["26/09/24 10:05:%02d %s Executor: %s line %d on executor %s" % (i, "ERROR" if i == 5 else "INFO",
             stream, i, eid) for i in range(40)]
    if stream == "stderr":
        lines[5] = "26/09/24 10:05:05 ERROR Executor: Exception in task 3.0: java.lang.OutOfMemoryError: Java heap space"
    if eid == "driver" and stream == "stderr":
        # Six micro-batches of progress; the last three fall behind.
        for b in range(4118, 4124):
            lines.append(PROGRESS % {"batch": b, "m": b // 60 % 60, "s": b % 60,
                                     "trig": 1200 + (b % 5) * 150, "out": 9000.0 if b >= 4121 else 12800.0})
    body = "\n".join(lines).replace("<", "&lt;")
    return "<html><body><h1>Logs for container_%s</h1><pre>%s</pre></body></html>" % (eid, body)


GiB = 1024 * MiB
RDDS = [
    {"id": 40, "name": "*(2) Project [customer_id#12L, amount#15] MapPartitionsRDD[40] at cache at Main.scala:31",
     "numPartitions": 200, "numCachedPartitions": 150, "storageLevel": "Memory Deserialized 1x Replicated",
     "memoryUsed": 3 * GiB, "diskUsed": 0,
     "dataDistribution": [{"address": "10.0.1.9:7079", "memoryUsed": 2 * GiB, "memoryRemaining": 1 * GiB, "diskUsed": 0},
                          {"address": "10.0.1.11:7079", "memoryUsed": 1 * GiB, "memoryRemaining": 2 * GiB, "diskUsed": 0}],
     "partitions": [{"blockName": "rdd_40_%d" % i, "storageLevel": "Memory Deserialized 1x Replicated",
                     "memoryUsed": 20 * MiB, "diskUsed": 0, "executors": ["10.0.1.9:7079" if i % 3 else "10.0.1.11:7079"]}
                    for i in range(150)]},
    {"id": 12, "name": "customers MapPartitionsRDD[12] at persist at Main.scala:18",
     "numPartitions": 8, "numCachedPartitions": 8, "storageLevel": "Disk Memory Serialized 1x Replicated",
     "memoryUsed": 400 * MiB, "diskUsed": 1200 * MiB,
     "dataDistribution": [{"address": "10.0.1.9:7079", "memoryUsed": 400 * MiB, "memoryRemaining": 600 * MiB, "diskUsed": 1200 * MiB}],
     "partitions": []},
]

ENVIRONMENT = {
    "runtime": {"javaVersion": "17.0.16 (Eclipse Adoptium)", "javaHome": "/opt/java/openjdk", "scalaVersion": "version 2.13.16"},
    "sparkProperties": [
        ["spark.app.id", "app-20260923-0001"], ["spark.app.name", "etl-nightly"],
        ["spark.executor.memory", "4g"], ["spark.executor.cores", "4"], ["spark.executor.memoryOverhead", "512m"],
        ["spark.executor.instances", "2"], ["spark.driver.memory", "2g"],
        ["spark.sql.shuffle.partitions", "200"], ["spark.sql.adaptive.enabled", "true"],
        ["spark.dynamicAllocation.enabled", "false"], ["spark.memory.fraction", "0.6"],
        ["spark.sql.streaming.checkpointLocation", "s3a://bucket/checkpoints"],
        ["spark.eventLog.enabled", "true"], ["spark.eventLog.dir", "s3a://bucket/spark-events"],
        ["spark.kubernetes.executor.deleteOnTermination", "false"],
        ["spark.serializer", "org.apache.spark.serializer.KryoSerializer"],
        ["spark.master", "k8s://https://10.96.0.1:443"], ["spark.submit.deployMode", "cluster"],
    ],
    "hadoopProperties": [["fs.s3a.connection.maximum", "96"], ["fs.s3a.endpoint", "s3.eu-west-1.amazonaws.com"]],
    "systemProperties": [["java.version", "17.0.16"], ["user.timezone", "UTC"], ["SPARK_SUBMIT", "true"]],
    "metricsProperties": [["*.sink.servlet.class", "org.apache.spark.metrics.sink.MetricsServlet"]],
    "classpathEntries": [["/opt/spark/conf", "System Classpath"], ["/opt/spark/jars/spark-core_2.13-4.0.1.jar", "System Classpath"]],
    "resourceProfiles": [{"id": 0,
                          "executorResources": {"memory": {"resourceName": "memory", "amount": 4096},
                                                "memoryOverhead": {"resourceName": "memoryOverhead", "amount": 512},
                                                "offHeap": {"resourceName": "offHeap", "amount": 0},
                                                "cores": {"resourceName": "cores", "amount": 4}},
                          "taskResources": {"cpus": {"resourceName": "cpus", "amount": 1.0}}}],
}

PEAK = {"JVMHeapMemory": 3 * GiB, "JVMOffHeapMemory": 180 * MiB, "OnHeapExecutionMemory": 900 * MiB,
        "OffHeapExecutionMemory": 0, "OnHeapStorageMemory": 1536 * MiB, "OffHeapStorageMemory": 0,
        "OnHeapUnifiedMemory": 2300 * MiB, "OffHeapUnifiedMemory": 0, "DirectPoolMemory": 64 * MiB,
        "MappedPoolMemory": 0, "ProcessTreeJVMRSSMemory": 3700 * MiB, "ProcessTreeJVMVMemory": 6 * GiB,
        "ProcessTreePythonRSSMemory": 0, "ProcessTreePythonVMemory": 0, "ProcessTreeOtherRSSMemory": 0,
        "ProcessTreeOtherVMemory": 0, "MinorGCCount": 420, "MinorGCTime": 180000, "MajorGCCount": 6,
        "MajorGCTime": 60000, "ConcurrentGCCount": 0, "ConcurrentGCTime": 0, "TotalGCTime": 240000}
MEMORY = {"usedOnHeapStorageMemory": 1536 * MiB, "usedOffHeapStorageMemory": 0,
          "totalOnHeapStorageMemory": 2300 * MiB, "totalOffHeapStorageMemory": 0}

# Completed-task counters tick up on every poll so the sparklines move.
TICK = {"n": 0}


class H(BaseHTTPRequestHandler):
    def route(self, path):
        query = self.query
        if path == "/api/v1/applications":
            return APPS
        prefix = "/api/v1/applications/"
        if not path.startswith(prefix):
            return None
        app_id, _, rest = path[len(prefix):].partition("/")
        app = next((a for a in APPS if a["id"] == app_id), None)
        if app is None:
            return None
        if rest == "":
            return app
        if rest == "jobs":
            return JOBS
        if rest == "stages":
            return STAGES
        if rest.startswith("stages/"):
            return self.stage_route(rest.split("/")[1:], query)
        if rest == "storage/rdd":
            return [{k: v for k, v in r.items() if k not in ("dataDistribution", "partitions")} for r in RDDS]
        if rest.startswith("storage/rdd/"):
            rid = int(rest.split("/")[2])
            return next((r for r in RDDS if r["id"] == rid), None)
        if rest == "sql" and SQL_ENABLED:
            offset, length = int(query.get("offset", 0)), int(query.get("length", 20))
            return [e for e in SQL[offset:offset + length]]
        if rest.startswith("sql/") and SQL_ENABLED:
            eid = int(rest.split("/")[1])
            e = next((e for e in SQL if e["id"] == eid), None)
            if e is None:
                return None
            e = dict(e)
            if e["id"] in (12, 13):
                e["planDescription"] = PLAN
                e["nodes"] = SQL_NODES
            return e
        if rest.startswith("executors/") and rest.endswith("/threads"):
            eid = rest.split("/")[1]
            return THREADS if eid in ("1", "3", "driver") else None
        if rest == "environment":
            return ENVIRONMENT
        if rest == "allexecutors":
            TICK["n"] += 1
            execs = json.loads(json.dumps(EXECS))
            execs[1]["completedTasks"] += TICK["n"] * 7
            for e in execs:
                if e["id"] != "driver":
                    e["memoryMetrics"] = MEMORY
                    e["peakMemoryMetrics"] = PEAK
            return execs
        return None

    def stage_route(self, parts, query):
        # parts: [stage_id, attempt, (taskSummary | taskList)?]
        if len(parts) < 2:
            return None
        sid = int(parts[0])
        stage = next((s for s in STAGES if s["stageId"] == sid), None)
        if stage is None:
            return None
        if len(parts) == 2:
            return stage
        if parts[2] == "taskSummary":
            return SUMMARIES.get(sid)  # 404 when no completed tasks
        if parts[2] == "taskList":
            tasks = TASKS.get(sid, {"slowest": [], "failed": []})
            if query.get("status") == "failed":
                return tasks["failed"]
            return tasks["slowest"]
        return None

    def do_GET(self):
        path, _, qs = self.path.partition("?")
        query = dict(kv.split("=", 1) for kv in qs.split("&") if "=" in kv)
        self.query = query
        if path.startswith("/node/containerlogs/"):
            # Fake YARN NodeManager log page.
            parts = path.split("/")
            eid, stream = parts[3].replace("container_", ""), parts[5]
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(log_page(eid, stream).encode())
            return
        body = self.route(path)
        self.send_response(200 if body is not None else 404)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(body if body is not None else {"error": "nope"}).encode())

    def log_message(self, *a):
        pass


HTTPServer(("127.0.0.1", 4040), H).serve_forever()
