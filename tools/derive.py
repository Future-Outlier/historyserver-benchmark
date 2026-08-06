#!/usr/bin/env python3
"""Flatten every bench-report.json under results/ into one derived.csv.

The docs quote numbers from this file; regenerate it after adding runs:

    python3 tools/derive.py results > results/derived.csv
"""
import csv
import glob
import json
import pathlib
import sys

MIB = 1024 * 1024
NS = 1e-9

FIELDS = [
    "run", "tasks", "gzip", "num_cpus", "driver_tps", "hs_cpu_limit",
    "hs_env", "gc_percent", "gomaxprocs", "events_per_task", "bytes_per_event", "raw_mib", "stored_mib", "gzip_ratio",
    "raw_b_per_task", "stored_b_per_task", "logs_mib",
    "task_ids_seen", "task_ids_expected",
    "job_wall_s", "node_peak_events_per_s", "collector_us_per_event",
    "collector_worker_peak_mib", "collector_head_peak_mib",
    "collector_worker_peak_cores", "collector_worker_avg_cores",
    "collector_head_peak_cores", "collector_head_avg_cores",
    "collector_503s", "collector_upload_failures", "collector_log_capture",
    "flush_s", "hs_cold_s", "hs_measured", "hs_status", "hs_peak_mib", "hs_b_per_task",
    "ray_head_peak_mib", "ray_worker_peak_mib", "hs_ms_per_task",
]


def cg(report, phase, field, *substrs, default=0):
    # Container keys look like "<pod>/<container>", so "worker" alone would also
    # match the ray-worker container; every caller passes both parts.
    vals = [
        row[field]
        for row in report.get("cgroups") or []
        if row["phase"] == phase and all(s in row["container"] for s in substrs)
    ]
    return max(vals, default=default)


def row_for(path, root):
    report = json.load(open(path))
    name = pathlib.Path(path).parent.name
    cfg, ev, hs = report["config"], report["storage"]["events"], report["historyServer"]
    n = cfg["TaskCount"]
    per_node = ev.get("perNode") or []
    logs = report["collectorLogs"] or []
    # The matrix runs predate the log-follower fix, so their collector counters
    # are structurally zero; mark them so nobody reads 0 as "measured zero".
    captured = any(c["uploads"] for c in logs)
    hs_mib = cg(report, "historyserver", "peakAnonMiB", "/historyserver")
    wall = report["job"]["wallClock"] * NS
    # CPU per event on the worker node: its collector only sees that node's events,
    # which is the smaller of the two per-node counts (the head also carries
    # owner-side lifecycle events).
    worker_events = min((x["events"] for x in per_node), default=0)
    worker_cores = cg(report, "job", "avgCores", "worker", "/collector")
    # enterColdLatency is only a latency when the client actually got a 200;
    # otherwise it is the probe budget, which says nothing about the server.
    measured = hs.get("enterMeasured", hs["enterStatus"] == 200)
    cold_s = hs["enterColdLatency"] * NS
    ids = ev.get("benchJobTaskIDs") or ev["distinctTaskDefIDs"]
    return {
        "run": name,
        "tasks": n,
        "gzip": "on" if cfg["Compression"] else "off",
        "num_cpus": cfg["TaskNumCPUs"],
        "driver_tps": round(report["job"]["driverRateTPS"], 1),
        # empty config value means the run used the sample manifest's 500m
        "hs_cpu_limit": cfg.get("HSCPULimit") or "500m",
        "hs_env": cfg.get("HSEnv", ""),
        "gc_percent": (hs.get("gc") or {}).get("finalPercent", ""),
        "gomaxprocs": (hs.get("gc") or {}).get("gomaxprocs", ""),
        "events_per_task": round(ev["eventsPerTask"], 3),
        "bytes_per_event": round(ev["avgRawBytesPerEvent"], 1),
        "raw_mib": round(ev["rawJSONLBytes"] / MIB, 2),
        "stored_mib": round(ev["storedEventBytes"] / MIB, 2),
        "gzip_ratio": round(ev["compressionRatio"], 4),
        "raw_b_per_task": round(ev["rawJSONLBytes"] / n, 1),
        "stored_b_per_task": round(ev["storedEventBytes"] / n, 1),
        "logs_mib": round((report["storage"].get("categories") or {}).get("logs", 0) / MIB, 3),
        "task_ids_seen": ids,
        "task_ids_expected": ev["expectedTasks"],
        "job_wall_s": round(wall, 1),
        "node_peak_events_per_s": round(max((x["peak10sEventsPerSec"] for x in per_node), default=0), 1),
        "collector_us_per_event": round(worker_cores * wall / worker_events * 1e6, 1) if worker_events else "",
        "collector_worker_peak_mib": round(cg(report, "job", "peakAnonMiB", "worker", "/collector"), 1),
        "collector_head_peak_mib": round(cg(report, "job", "peakAnonMiB", "head", "/collector"), 1),
        "collector_worker_peak_cores": round(cg(report, "job", "peakCores", "worker", "/collector"), 3),
        "collector_worker_avg_cores": round(cg(report, "job", "avgCores", "worker", "/collector"), 3),
        "collector_head_peak_cores": round(cg(report, "job", "peakCores", "head", "/collector"), 3),
        "collector_head_avg_cores": round(cg(report, "job", "avgCores", "head", "/collector"), 3),
        "collector_503s": sum(c["diskPressure503s"] for c in logs),
        "collector_upload_failures": sum(c["uploadFailures"] for c in logs),
        "collector_log_capture": "yes" if captured else "no",
        "flush_s": round(report["flushDuration"] * NS, 1),
        "hs_cold_s": round(cold_s, 2) if measured else "",
        "hs_measured": "yes" if measured else "no",
        "hs_status": hs["enterStatus"],
        "hs_peak_mib": round(hs_mib, 1),
        "hs_b_per_task": round(hs_mib * MIB / n, 1),
        "hs_ms_per_task": round(cold_s * 1000 / n, 3) if measured else "",
        "ray_head_peak_mib": round(cg(report, "job", "peakAnonMiB", "/ray-head"), 1),
        "ray_worker_peak_mib": round(cg(report, "job", "peakAnonMiB", "/ray-worker"), 1),
    }


def main():
    root = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else "results")
    paths = sorted(glob.glob(str(root / "**" / "bench-report.json"), recursive=True))
    if not paths:
        print(f"no bench-report.json under {root}", file=sys.stderr)
        return 1
    writer = csv.DictWriter(sys.stdout, fieldnames=FIELDS)
    writer.writeheader()
    for p in paths:
        writer.writerow(row_for(p, root))
    return 0


if __name__ == "__main__":
    sys.exit(main())
