"""Tiny fake of the Spark REST API for smoke-testing sparkwatch."""
import json
from http.server import BaseHTTPRequestHandler, HTTPServer

APP = [{"id": "app-20260923-0001", "name": "etl-nightly",
        "attempts": [{"startTime": "2026-09-23T09:00:00.000GMT", "endTime": "",
                      "lastUpdated": "", "duration": 4200000, "sparkUser": "vishnu",
                      "completed": False, "appSparkVersion": "3.5.1"}]}]
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
STAGES = [{"status": "ACTIVE", "stageId": 9, "attemptId": 0, "name": "mapPartitions at Writer.scala:88",
           "numTasks": 200, "numActiveTasks": 16, "numCompleteTasks": 97, "numFailedTasks": 2,
           "inputBytes": 5368709120, "shuffleReadBytes": 2147483648, "shuffleWriteBytes": 0,
           "memoryBytesSpilled": 536870912, "executorRunTime": 900000},
          {"status": "COMPLETE", "stageId": 8, "attemptId": 0, "name": "exchange at Writer.scala:70",
           "numTasks": 200, "numCompleteTasks": 200, "shuffleWriteBytes": 2147483648}]
EXECS = [{"id": "driver", "hostPort": "10.0.1.1:7078", "isActive": True, "totalCores": 0},
         {"id": "1", "hostPort": "10.0.1.9:7079", "isActive": True, "totalCores": 4,
          "activeTasks": 4, "failedTasks": 2, "completedTasks": 610, "totalDuration": 1820000,
          "totalGCTime": 240000, "memoryUsed": 1610612736, "maxMemory": 4294967296,
          "totalShuffleRead": 1073741824},
         {"id": "2", "hostPort": "10.0.1.10:7079", "isActive": False, "totalCores": 4,
          "completedTasks": 300, "totalDuration": 900000, "totalGCTime": 30000,
          "maxMemory": 4294967296}]

ROUTES = {"/api/v1/applications": APP,
          "/api/v1/applications/app-20260923-0001/jobs": JOBS,
          "/api/v1/applications/app-20260923-0001/stages": STAGES,
          "/api/v1/applications/app-20260923-0001/allexecutors": EXECS}


class H(BaseHTTPRequestHandler):
    def do_GET(self):
        body = ROUTES.get(self.path)
        self.send_response(200 if body is not None else 404)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(body if body is not None else {"error": "nope"}).encode())

    def log_message(self, *a):
        pass


HTTPServer(("127.0.0.1", 4040), H).serve_forever()
