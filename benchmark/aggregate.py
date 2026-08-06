#!/usr/bin/env python3
"""Aggregate bench-report.json files from a matrix run into one Markdown summary.

Usage: aggregate.py <matrix-out-dir>   (writes Markdown to stdout)

Stdlib only, so it runs on a stock macOS/Linux python3.
"""
import json
import pathlib
import sys

NS = 1e-9
MIB = 1024 * 1024


def fmt_bytes(n):
    if n >= 1 << 30:
        return f"{n / (1 << 30):.2f} GiB"
    if n >= 1 << 20:
        return f"{n / (1 << 20):.2f} MiB"
    if n >= 1 << 10:
        return f"{n / (1 << 10):.2f} KiB"
    return f"{n} B"


def resource_row(report, class_name, phase):
    for row in report.get("resources") or []:
        if row["class"] == class_name and row["phase"] == phase:
            return row
    return {}


def cgroup_rows(report, contains, phase):
    return [
        row
        for row in report.get("cgroups") or []
        if contains in row["container"] and row["phase"] == phase
    ]


def load_reports(root):
    reports = []
    for path in sorted(root.rglob("bench-report.json")):
        with open(path) as f:
            report = json.load(f)
        # Run name is the directory the matrix runner created, e.g. "A-n50000".
        rel = path.relative_to(root)
        report["_name"] = rel.parts[0] if len(rel.parts) > 1 else path.parent.name
        reports.append(report)
    return reports


def main():
    root = pathlib.Path(sys.argv[1])
    reports = load_reports(root)
    if not reports:
        print(f"no bench-report.json found under {root}", file=sys.stderr)
        return 1

    print("# Benchmark Matrix Summary\n")
    print(f"- Runs aggregated: {len(reports)}\n")

    print("## Storage & History Server vs total tasks (A/C axes)\n")
    print(
        "| run | N | gzip | driver tasks/s | k | B/event | events raw | events stored "
        "| ratio | logs | flush | taskIds ok | HS cold load | HS status | HS peak anon | ray-head peak anon |"
    )
    print("|---" * 15 + "|")
    for r in sorted(reports, key=lambda r: (r["_name"][0], r["config"]["TaskCount"])):
        cfg = r["config"]
        ev = r["storage"]["events"]
        hs = r["historyServer"]
        hs_anon = max((row["peakAnonMiB"] for row in cgroup_rows(r, "/historyserver", "historyserver")), default=0)
        head_anon = max((row["peakAnonMiB"] for row in cgroup_rows(r, "/ray-head", "job")), default=0)
        bench_ids = ev.get("benchJobTaskIDs", 0)
        if bench_ids:
            ids = f'{bench_ids}/{ev["expectedTasks"]} (bench job)'
        else:
            ids = f'{ev["distinctTaskDefIDs"]}/{ev["expectedTasks"]}'
        print(
            f'| {r["_name"]} | {cfg["TaskCount"]} | {"on" if cfg["Compression"] else "off"} '
            f'| {r["job"]["driverRateTPS"]:.0f} | {ev["eventsPerTask"]:.2f} | {ev["avgRawBytesPerEvent"]:.0f} '
            f'| {fmt_bytes(ev["rawJSONLBytes"])} | {fmt_bytes(ev["storedEventBytes"])} | {ev["compressionRatio"]:.3f} '
            f'| {fmt_bytes((r["storage"].get("categories") or {}).get("logs", 0))} | {r["flushDuration"] * NS:.1f}s '
            f'| {ids} | {hs["enterColdLatency"] * NS:.2f}s | {hs["enterStatus"]} '
            f'| {hs_anon:.0f} MiB | {head_anon:.0f} MiB |'
        )

    print("\n## Collector vs per-node event rate (B axis)\n")
    print(
        "| run | num_cpus | driver tasks/s | peak node events/s (10s) "
        "| worker collector peak anon | worker collector avg/peak cores | disk-pressure 503s | upload failures |"
    )
    print("|---" * 8 + "|")
    for r in sorted(reports, key=lambda r: -float(r["config"]["TaskNumCPUs"])):
        if not r["_name"].startswith("B-"):
            continue
        cfg = r["config"]
        per_node = r["storage"]["events"].get("perNode") or []
        peak_rate = max((n["peak10sEventsPerSec"] for n in per_node), default=0)
        col = {}
        for row in cgroup_rows(r, "/collector", "job"):
            if "worker" in row["container"]:
                col = row
        p503 = sum(c["diskPressure503s"] for c in r.get("collectorLogs") or [])
        fails = sum(c["uploadFailures"] for c in r.get("collectorLogs") or [])
        print(
            f'| {r["_name"]} | {cfg["TaskNumCPUs"]} | {r["job"]["driverRateTPS"]:.0f} | {peak_rate:.0f} '
            f'| {col.get("peakAnonMiB", 0):.0f} MiB | {col.get("avgCores", 0):.3f} / {col.get("peakCores", 0):.3f} '
            f'| {p503} | {fails} |'
        )

    print("\n## Collector flatness check across N (A axis, fixed rate)\n")
    print("| run | N | worker collector peak anon | head collector peak anon | worker peak cores | head peak cores |")
    print("|---" * 6 + "|")
    for r in sorted(reports, key=lambda r: r["config"]["TaskCount"]):
        if not r["_name"].startswith("A-"):
            continue
        worker = head = {}
        for row in cgroup_rows(r, "/collector", "job"):
            if "head" in row["container"]:
                head = row
            else:
                worker = row
        print(
            f'| {r["_name"]} | {r["config"]["TaskCount"]} | {worker.get("peakAnonMiB", 0):.0f} MiB '
            f'| {head.get("peakAnonMiB", 0):.0f} MiB | {worker.get("peakCores", 0):.3f} | {head.get("peakCores", 0):.3f} |'
        )

    print("\n(Working-set variants of every number are in each run's bench-report.md; "
          "raw series in samples.csv / cgroup_samples.csv / node_rate.csv.)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
