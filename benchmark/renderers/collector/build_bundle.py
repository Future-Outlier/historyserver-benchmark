#!/usr/bin/env python3

import bisect
import csv
import hashlib
import json
import math
import statistics
from pathlib import Path


SOURCE = Path("/private/tmp/kuberay-collector-memory-formal-20260813-r7")
OUT = Path("/private/tmp/ray-summit-collector-results-r7")
MIB = 1024 * 1024
A_RATES = (1000, 2000, 3000, 5000)
B_RATES = (2000, 5000)
C_LIMITS = (192, 256, 512)
REPEATS = (1, 2, 3)


def q(values, p):
    values = sorted(v for v in values if v is not None)
    if not values:
        return None
    x = (len(values) - 1) * p
    lo, hi = math.floor(x), math.ceil(x)
    if lo == hi:
        return values[lo]
    return values[lo] * (hi - x) + values[hi] * (x - lo)


def rounded(value, digits=6):
    return None if value is None else round(value, digits)


def summary(values):
    values = [v for v in values if v is not None]
    return {
        "n": len(values),
        "min": rounded(min(values)) if values else None,
        "median": rounded(statistics.median(values)) if values else None,
        "max": rounded(max(values)) if values else None,
    }


def cross_quantiles(values):
    return {
        "n": len([v for v in values if v is not None]),
        "q0": rounded(q(values, 0.00)),
        "q25": rounded(q(values, 0.25)),
        "q50": rounded(q(values, 0.50)),
        "q75": rounded(q(values, 0.75)),
        "q100": rounded(q(values, 1.00)),
    }


def weighted_q(value_weights, p):
    points = sorted((v, w) for v, w in value_weights if w > 0)
    if not points:
        return None
    target = p * sum(w for _, w in points)
    cumulative = 0.0
    for value, weight in points:
        cumulative += weight
        if cumulative >= target:
            return value
    return points[-1][0]


def weighted_summary(value_weights):
    return {
        "p50": rounded(weighted_q(value_weights, 0.50)),
        "p95": rounded(weighted_q(value_weights, 0.95)),
        "p99": rounded(weighted_q(value_weights, 0.99)),
    }


def interpolate(points, target):
    if not points or target < points[0][0] or target > points[-1][0]:
        return None
    times = [x[0] for x in points]
    at = bisect.bisect_left(times, target)
    if at == 0:
        return points[0][1]
    if at == len(points):
        return points[-1][1]
    t0, v0 = points[at - 1]
    t1, v1 = points[at]
    if t1 == t0:
        return v1
    fraction = (target - t0) / (t1 - t0)
    return v0 + fraction * (v1 - v0)


def phase_value_weights(points, start, end):
    result = []
    for index, (timestamp, value) in enumerate(points[:-1]):
        next_timestamp = points[index + 1][0]
        lo = max(timestamp, start)
        hi = min(next_timestamp, end)
        if hi > lo:
            result.append((value, hi - lo))
    return result


def arm_run_dir(arm):
    candidates = sorted((SOURCE / arm).glob("*/collector-memory-report.json"))
    if len(candidates) != 1:
        raise RuntimeError(f"{arm}: expected one report, found {len(candidates)}")
    return candidates[0].parent


def estimate_host_time(report, node_timestamp_ns):
    # Interpolate the measured node-minus-host midpoint skew between the
    # bracketing clock probes, then convert the node log timestamp to host time.
    before = report["clockSkew"]["before"]
    after = report["clockSkew"]["after"]
    node0, node1 = before["nodeUnixNano"], after["nodeUnixNano"]
    skew0, skew1 = before["skewNano"], after["skewNano"]
    if node1 == node0:
        skew = skew0
    else:
        fraction = (node_timestamp_ns - node0) / (node1 - node0)
        skew = skew0 + fraction * (skew1 - skew0)
    return node_timestamp_ns - skew


def load_arm(arm):
    run_dir = arm_run_dir(arm)
    report = json.loads((run_dir / "collector-memory-report.json").read_text())
    phases_ns = {point["name"]: point["timeNano"] for point in report["phases"]}
    phases = {name: timestamp / 1e9 for name, timestamp in phases_ns.items()}
    collector = report["collectorLogs"][0]
    container_id = collector["containerID"]

    memory = []
    with (run_dir / "collector_memory_samples.csv").open(newline="") as handle:
        for row in csv.DictReader(handle):
            if row["container_id"] != container_id:
                continue
            memory.append({
                "t": int(row["time_nano"]) / 1e9,
                "current_mib": int(row["current_bytes"]) / MIB,
                "peak_mib": int(row["peak_bytes"]) / MIB,
                "anon_mib": int(row["anon_bytes"]) / MIB,
                "file_mib": int(row["file_bytes"]) / MIB,
                "file_dirty_mib": int(row["file_dirty_bytes"]) / MIB,
                "file_writeback_mib": int(row["file_writeback_bytes"]) / MIB,
                "kernel_mib": int(row["kernel_bytes"]) / MIB,
                "slab_mib": int(row["slab_bytes"]) / MIB,
                "events_max": int(row["memory_events_max"]),
                "events_oom": int(row["memory_events_oom"]),
                "events_oom_kill": int(row["memory_events_oom_kill"]),
                "psi_full_total_usec": int(row["psi_full_total_usec"]),
            })
    memory.sort(key=lambda row: row["t"])

    spool = []
    with (run_dir / "event_spool_samples.csv").open(newline="") as handle:
        for row in csv.DictReader(handle):
            if row["pod_uid"] != report["runtime"]["podUID"] or row["valid"] != "true":
                continue
            spool.append({
                "t": int(row["time_nano"]) / 1e9,
                "raw_mib": int(row["raw_jsonl_bytes"]) / MIB,
                "gzip_mib": int(row["gzip_bytes"]) / MIB,
                "tmp_mib": int(row["tmp_bytes"]) / MIB,
                "other_mib": int(row["other_bytes"]) / MIB,
                "total_mib": int(row["total_bytes"]) / MIB,
            })
    spool.sort(key=lambda row: row["t"])
    for row in spool:
        row["event_files_mib"] = row["raw_mib"] + row["gzip_mib"] + row["tmp_mib"]

    uploads = []
    for index, upload in enumerate(collector["uploadTimeline"], start=1):
        host_t = estimate_host_time(report, upload["unixNano"]) / 1e9
        uploads.append({
            "ordinal": index,
            "node_t": upload["unixNano"] / 1e9,
            "host_t": host_t,
            "raw_mib": upload["bytes"] / MIB,
            "kind": "runtime" if host_t < phases["shutdown"] else "shutdown",
        })

    final = collector["finalCgroupMemory"]
    return {
        "arm": arm,
        "run_dir": str(run_dir),
        "report": report,
        "phases": phases,
        "phases_ns": phases_ns,
        "memory": memory,
        "spool": spool,
        "uploads": uploads,
        "final": {
            "host_t": estimate_host_time(report, final["unixNano"]) / 1e9,
            "current_mib": final["currentBytes"] / MIB,
            "peak_mib": final["peakBytes"] / MIB,
            "events_max": final["eventsMax"],
            "events_oom": final["eventsOOM"],
            "events_oom_kill": final["eventsOOMKill"],
            "psi_full_total_usec": final["psiFullTotalUsec"],
        },
    }


def memory_points(run, metric, relative_to_ingest=False, include_final=False):
    origin = run["phases"]["ingest"] if relative_to_ingest else 0.0
    points = [(row["t"] - origin, row[metric]) for row in run["memory"]]
    if include_final and metric in {"current_mib", "peak_mib"}:
        points.append((run["final"]["host_t"] - origin, run["final"][metric]))
    return sorted(points)


def spool_points(run, metric, relative_to_ingest=False):
    origin = run["phases"]["ingest"] if relative_to_ingest else 0.0
    return sorted((row["t"] - origin, row[metric]) for row in run["spool"])


def write_csv(path, rows, fieldnames):
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def column_definition(filename, column):
    cross = "cross-run q0/q25/q50/q75/q100 at the aligned grid point after linear interpolation; n=3"
    time_weighted = "piecewise-constant interval weighting clipped to the named phase and pooled across n=3"
    exact = "exact value from the named arm/report"
    common = {
        "target_events_per_second": ("Configured synthetic event-ingress target.", "events/s", "group key"),
        "n_runs": ("Number of independent fresh-Pod arms in the group.", "runs", "count"),
        "seconds_relative_ingest": ("Raw dense-sample node unixNano minus each run's measured host ingest phase timestamp; no phase warping. Clock probes measured about 20ms node-host skew, below the 250ms sample cadence.", "seconds", "fixed 0.5-second grid"),
        "arm": ("Formal expected-matrix arm name.", "identifier", exact),
        "repeat": ("Independent repeat ordinal within the group.", "ordinal", exact),
        "phase": ("Measured phase window: ingest, idle_all, idle_first_30s, or idle_second_30s.", "category", "group key"),
        "source": ("Metric source: cgroup-v2 memory controller or local event-spool sampler.", "category", "group key"),
        "metric": ("Metric name whose definition is specified by its suffix and schema field conventions.", "identifier", "group key"),
        "window": ("Named validator window relative to shutdown: previous_30s=[shutdown-60s,shutdown-30s), current_30s=[shutdown-30s,shutdown).", "category", "group key"),
        "kind": ("Upload classification: runtime when skew-corrected completion precedes shutdown, otherwise shutdown.", "category", "mode across n=3"),
        "ordinal": ("Upload completion order within one arm.", "ordinal", "corresponding ordinal across n=3"),
        "classification": ("Candidate evaluator arm classification.", "category", exact),
        "verdict": ("Exact candidate evaluator result from candidate-<limit>Mi.json.", "category", exact),
        "aggregation": ("Human-readable aggregation contract for this row.", "text", exact),
        "unit": ("Unit of the metric named in this row.", "text", exact),
        "value": ("Campaign total, distinct count, identifier, or boolean for the named integrity metric.", "row unit", "row aggregation"),
    }
    if column in common:
        return common[column]

    if filename == "a_rate_summary.csv":
        if column.startswith("achieved_eps_"):
            stat = column.removeprefix("achieved_eps_")
            return (f"{stat} achieved accepted events divided by replay wall seconds across n=3.", "events/s", f"{stat} across n=3")
        if column == "raw_jsonl_mib":
            return ("Expected uncompressed JSONL bytes for the 90-second replay divided by 2^20; identical in n=3.", "MiB", "median across n=3")
        for metric in ("current", "anon", "file", "kernel"):
            if column.startswith(metric + "_p"):
                percentile = column.split("_")[1]
                source = {"current": "cgroup memory.current", "anon": "memory.stat anon", "file": "memory.stat file", "kernel": "memory.stat kernel"}[metric]
                return (f"Ingest-phase {percentile} of {source}.", "MiB", time_weighted)
        if column.startswith("lifetime_peak_mib_"):
            stat = column.rsplit("_", 1)[1]
            return (f"{stat} exact final in-process cgroup memory.peak across n=3.", "MiB", f"{stat} across n=3")
        if column == "final_current_mib_median":
            return ("Median exact final in-process cgroup memory.current after shutdown drain.", "MiB", "median across n=3")

    if filename in {"a_lifecycle_quantiles.csv", "b_lifecycle_quantiles.csv"}:
        if "_q" in column and column.endswith("_mib"):
            metric, quantile = column.rsplit("_", 2)[0], column.rsplit("_", 2)[1]
            source = {
                "current": "cgroup memory.current",
                "anon": "memory.stat anon",
                "file": "memory.stat file",
                "kernel": "memory.stat kernel",
                "raw_spool": "valid local-spool raw JSONL bytes",
                "raw": "valid local-spool raw JSONL bytes",
                "gzip": "valid local-spool .gz bytes",
                "tmp": "valid local-spool temporary compression bytes",
                "event_files": "valid local-spool raw+gzip+tmp bytes",
            }[metric]
            return (f"{quantile} of {source} at this aligned time.", "MiB", cross)

    if filename == "a_uploads.csv":
        if column.startswith("time_s_"):
            stat = column.rsplit("_", 1)[1]
            return (f"{stat} skew-corrected upload-completion time relative to ingest.", "seconds", f"{stat} across n=3")
        if column.startswith("raw_jsonl_mib_"):
            stat = column.rsplit("_", 1)[1]
            return (f"{stat} raw rotated JSONL file size for this upload ordinal.", "MiB", f"{stat} across n=3")

    if filename == "b_phase_composition.csv" and column in {"p50_mib", "p95_mib", "p99_mib"}:
        return (f"{column[:3]} of the named cgroup/spool metric during the named phase.", "MiB", time_weighted)

    if filename == "b_upload_reclaim_runs.csv":
        definitions = {
            "raw_jsonl_mib": ("Total expected uncompressed JSONL bytes for this 30-second replay.", "MiB", exact),
            "max_ingest_event_spool_mib": ("Maximum valid local-spool raw+gzip+tmp bytes observed during ingest.", "MiB", "per-run maximum"),
            "min_idle_event_spool_mib": ("Minimum valid local-spool raw+gzip+tmp bytes observed during idle.", "MiB", "per-run minimum"),
            "threshold_100mib_seconds_relative_ingest": ("First valid ingest spool sample where raw+gzip+tmp >=100 MiB, relative to ingest; null if never crossed.", "seconds", "first qualifying sample per run"),
            "tmp_first_seconds_relative_idle": ("First valid sample at/after ingest with tmp_bytes>0, expressed relative to idle start; null if absent.", "seconds", "first qualifying sample per run"),
            "runtime_upload_count": ("Number of skew-corrected upload completions before shutdown.", "uploads", "per-run count"),
            "runtime_upload_seconds_relative_idle": ("Last runtime upload completion relative to idle start; null if absent.", "seconds", exact),
            "runtime_upload_node_seconds_relative_idle_uncorrected": ("Diagnostic raw node-clock upload unixNano minus host-clock idle timestamp; intentionally uncorrected and not for plotting.", "seconds", exact),
            "runtime_upload_raw_mib": ("Raw JSONL size of the last runtime upload; null if absent.", "MiB", exact),
            "reclaim_zero_seconds_relative_idle": ("First valid spool sample at/after runtime upload completion with raw+gzip+tmp=0, relative to idle; null if absent.", "seconds", "first qualifying sample per run"),
            "upload_to_reclaim_seconds": ("First zero-spool sample time minus last runtime upload-completion time.", "seconds", "per-run difference"),
            "post_reclaim_observation_seconds": ("Shutdown phase start minus first zero-spool sample time.", "seconds", "per-run difference"),
            "shutdown_upload_count": ("Number of skew-corrected upload completions at/after shutdown.", "uploads", "per-run count"),
            "shutdown_upload_seconds_relative_idle": ("Last shutdown upload completion relative to idle start after node-to-host skew correction; null if absent.", "seconds", exact),
            "shutdown_upload_node_seconds_relative_idle_uncorrected": ("Diagnostic raw node-clock shutdown upload unixNano minus host-clock idle timestamp; intentionally uncorrected.", "seconds", exact),
            "shutdown_upload_raw_mib": ("Raw JSONL size of the last shutdown upload; null if absent.", "MiB", exact),
            "final_current_mib": ("Exact final in-process cgroup memory.current after shutdown drain.", "MiB", exact),
            "final_peak_mib": ("Exact final in-process cgroup memory.peak.", "MiB", exact),
        }
        if column in definitions:
            return definitions[column]
        if column.endswith("_mib_1s_before_reclaim") or column.endswith("_mib_1s_after_reclaim"):
            timing = "one second before" if "before" in column else "one second after"
            metric = column.split("_mib_")[0]
            return (f"Linearly interpolated cgroup {metric} {timing} first zero-spool reclaim; null if no runtime reclaim.", "MiB", "per-run interpolation")
        if column.endswith("_mib_upload_adjacent_pre") or column.endswith("_mib_upload_adjacent_post"):
            side = "last raw sample at or before" if column.endswith("_pre") else "first raw sample after"
            metric = column.split("_mib_upload_")[0]
            return (f"cgroup {metric} from the {side} the same-node-clock runtime upload-completion unixNano; null if no runtime upload.", "MiB", "exact adjacent raw sample per run")

    if filename == "b_upload_reclaim_summary.csv":
        definitions = {
            "n": ("Number of non-null B-arm values summarized for the named metric.", "runs", "count"),
            "min": ("Minimum non-null value for the named metric.", "row unit", "minimum across n<=3"),
            "median": ("Median non-null value for the named metric.", "row unit", "median across n<=3"),
            "max": ("Maximum non-null value for the named metric.", "row unit", "maximum across n<=3"),
        }
        if column in definitions:
            return definitions[column]

    if filename == "b_idle_window_validator_medians.csv":
        if column.startswith("repeat_") and column.endswith("_median_mib"):
            repeat = column.split("_")[1]
            return (f"Unweighted median of raw cgroup samples for repeat {repeat}, named metric, and named 30-second window.", "MiB", "validator median_window() per arm")
        if column == "cross_run_median_mib":
            return ("Median of repeat_1/2/3 raw-sample window medians.", "MiB", "median across n=3 arm medians")

    if filename == "c_candidate_arms.csv":
        definitions = {
            "memory_limit_mib": ("Configured cgroup memory.max.", "MiB", exact),
            "lifetime_peak_mib": ("Exact final in-process cgroup memory.peak.", "MiB", exact),
            "peak_to_limit_ratio": ("Exact memory.peak bytes divided by memory.max bytes.", "fraction", "per-run ratio"),
            "sampled_adjacent_max_current_mib": ("Maximum externally sampled memory.current row used by candidate evaluator for composition.", "MiB", "per-run maximum sampled current"),
            "sampled_adjacent_anon_mib": ("memory.stat anon from the maximum sampled current row.", "MiB", "same adjacent row"),
            "sampled_adjacent_file_mib": ("memory.stat file from the maximum sampled current row.", "MiB", "same adjacent row"),
            "sampled_adjacent_kernel_mib": ("memory.stat kernel from the maximum sampled current row.", "MiB", "same adjacent row"),
            "events_max": ("Exact final memory.events max counter; fresh-cgroup baseline verified zero.", "events", exact),
            "psi_full_total_usec": ("Exact final memory.pressure full total counter; fresh-cgroup baseline verified zero.", "microseconds", exact),
            "events_oom": ("Exact final cgroup memory.events oom counter.", "events", exact),
            "events_oom_kill": ("Exact final cgroup memory.events oom_kill counter.", "events", exact),
            "achieved_events_per_second": ("Accepted replay events divided by replay wall seconds.", "events/s", exact),
            "latency_p99_ms": ("Replay request latency p99.", "milliseconds", exact),
        }
        if column in definitions:
            return definitions[column]

    if filename == "c_candidate_summary.csv":
        if column == "memory_limit_mib":
            return ("Tested cgroup memory.max.", "MiB", "group key")
        if column == "n_runs":
            return common["n_runs"]
        if column.startswith("peak_mib_"):
            stat = column.rsplit("_", 1)[1]
            return (f"{stat} exact final memory.peak across n=3.", "MiB", f"{stat} across n=3")
        if column.startswith("peak_to_limit_"):
            stat = column.rsplit("_", 1)[1]
            return (f"{stat} per-run memory.peak/memory.max ratio.", "fraction", f"{stat} across n=3")
        if column == "peak_spread_fraction":
            return ("(maximum peak - minimum peak) / median peak.", "fraction", "derived across n=3")
        if column.startswith("events_max_"):
            stat = column.rsplit("_", 1)[1]
            return (f"{stat} final memory.events max counter.", "events", f"{stat} across n=3")
        if column.startswith("psi_full_usec_"):
            stat = column.rsplit("_", 1)[1]
            return (f"{stat} final memory.pressure full total counter.", "microseconds", f"{stat} across n=3")
        if column in {"events_oom_sum", "events_oom_kill_sum"}:
            return ("Sum of exact final OOM counter across n=3.", "events", "sum across n=3")
        if column == "verdict_error_count":
            return ("Number of exact candidate evaluator error reasons.", "count", exact)

    if filename == "integrity_totals.csv":
        if column == "metric":
            return ("Integrity total or invariant name.", "identifier", "group key")
        if column in {"value", "unit", "aggregation"}:
            return common[column]

    raise RuntimeError(f"missing exact column definition for {filename}:{column}")


def write_column_definitions(csv_paths):
    rows = []
    for path in csv_paths:
        with path.open(newline="") as handle:
            columns = next(csv.reader(handle))
        for column in columns:
            definition, unit, aggregation = column_definition(path.name, column)
            rows.append({
                "file": path.name,
                "column": column,
                "definition": definition,
                "unit": unit,
                "aggregation": aggregation,
            })
    write_csv(OUT / "column_definitions.csv", rows, ["file", "column", "definition", "unit", "aggregation"])


def calculate_a(arms):
    summary_rows = []
    lifecycle_rows = []
    upload_rows = []
    result = []
    metrics = ("current_mib", "anon_mib", "file_mib", "kernel_mib")
    grid = [round(-10 + 0.5 * index, 1) for index in range(231)]  # through t=105s

    for rate in A_RATES:
        runs = [arms[f"A-rate{rate}-r{repeat}"] for repeat in REPEATS]
        ingest_weighted = {metric: [] for metric in metrics}
        for run in runs:
            for metric in metrics:
                ingest_weighted[metric].extend(phase_value_weights(
                    memory_points(run, metric), run["phases"]["ingest"], run["phases"]["idle"]
                ))

        tw = {metric: weighted_summary(ingest_weighted[metric]) for metric in metrics}
        peaks = [run["final"]["peak_mib"] for run in runs]
        finals = [run["final"]["current_mib"] for run in runs]
        achieved = [run["report"]["replay"]["achieved_events_per_second"] for run in runs]
        raw = [run["report"]["replay"]["expected_jsonl_bytes"] / MIB for run in runs]
        phase_medians = {
            name: statistics.median(run["phases"][name] - run["phases"]["ingest"] for run in runs)
            for name in ("baseline", "ingest", "idle", "shutdown", "complete")
        }
        item = {
            "target_events_per_second": rate,
            "n_runs": 3,
            "achieved_events_per_second": summary(achieved),
            "raw_jsonl_mib": summary(raw),
            "ingest_time_weighted_mib": tw,
            "lifetime_peak_mib": summary(peaks),
            "final_current_mib": summary(finals),
            "phase_boundary_seconds_relative_ingest": {k: rounded(v) for k, v in phase_medians.items()},
            "uploads": [],
        }
        summary_rows.append({
            "target_events_per_second": rate,
            "n_runs": 3,
            "achieved_eps_min": rounded(min(achieved)),
            "achieved_eps_median": rounded(statistics.median(achieved)),
            "achieved_eps_max": rounded(max(achieved)),
            "raw_jsonl_mib": rounded(statistics.median(raw)),
            "current_p50_mib": tw["current_mib"]["p50"],
            "current_p95_mib": tw["current_mib"]["p95"],
            "current_p99_mib": tw["current_mib"]["p99"],
            "anon_p50_mib": tw["anon_mib"]["p50"],
            "anon_p95_mib": tw["anon_mib"]["p95"],
            "anon_p99_mib": tw["anon_mib"]["p99"],
            "file_p50_mib": tw["file_mib"]["p50"],
            "file_p95_mib": tw["file_mib"]["p95"],
            "file_p99_mib": tw["file_mib"]["p99"],
            "kernel_p50_mib": tw["kernel_mib"]["p50"],
            "kernel_p95_mib": tw["kernel_mib"]["p95"],
            "kernel_p99_mib": tw["kernel_mib"]["p99"],
            "lifetime_peak_mib_min": rounded(min(peaks)),
            "lifetime_peak_mib_median": rounded(statistics.median(peaks)),
            "lifetime_peak_mib_max": rounded(max(peaks)),
            "final_current_mib_median": rounded(statistics.median(finals)),
        })

        for t in grid:
            row = {"target_events_per_second": rate, "seconds_relative_ingest": t, "n_runs": 3}
            for metric in metrics:
                values = [interpolate(memory_points(run, metric, True, metric == "current_mib"), t) for run in runs]
                stats = cross_quantiles(values)
                prefix = metric.removesuffix("_mib")
                for quantile in ("q0", "q25", "q50", "q75", "q100"):
                    row[f"{prefix}_{quantile}_mib"] = stats[quantile]
            spool_values = [interpolate(spool_points(run, "raw_mib", True), t) for run in runs]
            stats = cross_quantiles(spool_values)
            for quantile in ("q0", "q25", "q50", "q75", "q100"):
                row[f"raw_spool_{quantile}_mib"] = stats[quantile]
            lifecycle_rows.append(row)

        upload_counts = {len(run["uploads"]) for run in runs}
        if len(upload_counts) != 1:
            raise RuntimeError(f"A rate {rate}: upload ordinal mismatch {upload_counts}")
        for ordinal in range(1, next(iter(upload_counts)) + 1):
            points = [run["uploads"][ordinal - 1] for run in runs]
            times = [point["host_t"] - run["phases"]["ingest"] for point, run in zip(points, runs)]
            sizes = [point["raw_mib"] for point in points]
            kinds = [point["kind"] for point in points]
            upload = {
                "ordinal": ordinal,
                "kind": statistics.mode(kinds),
                "seconds_relative_ingest": summary(times),
                "raw_jsonl_mib": summary(sizes),
            }
            item["uploads"].append(upload)
            upload_rows.append({
                "target_events_per_second": rate,
                "ordinal": ordinal,
                "kind": upload["kind"],
                "time_s_min": rounded(min(times)),
                "time_s_median": rounded(statistics.median(times)),
                "time_s_max": rounded(max(times)),
                "raw_jsonl_mib_min": rounded(min(sizes)),
                "raw_jsonl_mib_median": rounded(statistics.median(sizes)),
                "raw_jsonl_mib_max": rounded(max(sizes)),
            })
        result.append(item)
    return result, summary_rows, lifecycle_rows, upload_rows


def calculate_b(arms):
    composition_rows = []
    reclaim_rows = []
    reclaim_summary_rows = []
    lifecycle_rows = []
    idle_validator_rows = []
    result = []
    memory_metrics = ("current_mib", "anon_mib", "file_mib", "kernel_mib")
    spool_metrics = ("raw_mib", "gzip_mib", "tmp_mib", "event_files_mib")

    for rate in B_RATES:
        runs = [arms[f"B-rate{rate}-r{repeat}"] for repeat in REPEATS]
        phase_defs = {
            "ingest": lambda run: (run["phases"]["ingest"], run["phases"]["idle"]),
            "idle_all": lambda run: (run["phases"]["idle"], run["phases"]["shutdown"]),
            "idle_first_30s": lambda run: (run["phases"]["idle"], min(run["phases"]["idle"] + 30, run["phases"]["shutdown"])),
            "idle_second_30s": lambda run: (run["phases"]["idle"] + 30, run["phases"]["shutdown"]),
        }
        phase_result = {}
        for phase_name, bounds in phase_defs.items():
            phase_result[phase_name] = {}
            for metric in memory_metrics:
                weighted = []
                for run in runs:
                    start, end = bounds(run)
                    weighted.extend(phase_value_weights(memory_points(run, metric), start, end))
                stats = weighted_summary(weighted)
                phase_result[phase_name][metric] = stats
                composition_rows.append({
                    "target_events_per_second": rate,
                    "phase": phase_name,
                    "source": "cgroup_v2",
                    "metric": metric,
                    "p50_mib": stats["p50"],
                    "p95_mib": stats["p95"],
                    "p99_mib": stats["p99"],
                })

        # Exact gate used by validate_collector_memory.py: unweighted median of
        # raw samples in each of the two final 30-second windows, then present
        # all three run medians and their cross-run median.
        for metric in memory_metrics:
            for window_name, offset_start, offset_end in (
                ("previous_30s", -60.0, -30.0),
                ("current_30s", -30.0, 0.0),
            ):
                medians = []
                for run in runs:
                    shutdown = run["phases"]["shutdown"]
                    values = [
                        row[metric] for row in run["memory"]
                        if shutdown + offset_start <= row["t"] < shutdown + offset_end
                    ]
                    medians.append(statistics.median(values))
                idle_validator_rows.append({
                    "target_events_per_second": rate,
                    "window": window_name,
                    "metric": metric,
                    "repeat_1_median_mib": rounded(medians[0]),
                    "repeat_2_median_mib": rounded(medians[1]),
                    "repeat_3_median_mib": rounded(medians[2]),
                    "cross_run_median_mib": rounded(statistics.median(medians)),
                })
            for metric in spool_metrics:
                weighted = []
                for run in runs:
                    start, end = bounds(run)
                    weighted.extend(phase_value_weights(spool_points(run, metric), start, end))
                stats = weighted_summary(weighted)
                phase_result[phase_name][metric] = stats
                composition_rows.append({
                    "target_events_per_second": rate,
                    "phase": phase_name,
                    "source": "local_event_spool",
                    "metric": metric,
                    "p50_mib": stats["p50"],
                    "p95_mib": stats["p95"],
                    "p99_mib": stats["p99"],
                })

        run_events = []
        for run in runs:
            ingest = run["phases"]["ingest"]
            idle = run["phases"]["idle"]
            shutdown = run["phases"]["shutdown"]
            repeat = run["report"]["config"]["repeat"]
            ingest_spool = [row for row in run["spool"] if ingest <= row["t"] < idle]
            idle_spool = [row for row in run["spool"] if idle <= row["t"] < shutdown]
            threshold = next((row for row in ingest_spool if row["event_files_mib"] >= 100), None)
            tmp_first = next((row for row in run["spool"] if row["t"] >= ingest and row["tmp_mib"] > 0), None)
            runtime_uploads = [point for point in run["uploads"] if point["kind"] == "runtime"]
            shutdown_uploads = [point for point in run["uploads"] if point["kind"] == "shutdown"]
            last_runtime_upload = runtime_uploads[-1] if runtime_uploads else None
            adjacent_pre = None
            adjacent_post = None
            if last_runtime_upload:
                adjacent_pre = next((row for row in reversed(run["memory"]) if row["t"] <= last_runtime_upload["node_t"]), None)
                adjacent_post = next((row for row in run["memory"] if row["t"] > last_runtime_upload["node_t"]), None)
            reclaim = None
            if last_runtime_upload:
                reclaim = next((row for row in run["spool"] if row["t"] >= last_runtime_upload["host_t"] and row["event_files_mib"] == 0), None)

            def memory_at(target):
                return {
                    metric: interpolate(memory_points(run, metric), target)
                    for metric in memory_metrics
                }

            before = memory_at(reclaim["t"] - 1.0) if reclaim else {metric: None for metric in memory_metrics}
            after = memory_at(reclaim["t"] + 1.0) if reclaim else {metric: None for metric in memory_metrics}
            event = {
                "arm": run["arm"],
                "target_events_per_second": rate,
                "repeat": repeat,
                "raw_jsonl_mib": run["report"]["replay"]["expected_jsonl_bytes"] / MIB,
                "max_ingest_event_spool_mib": max(row["event_files_mib"] for row in ingest_spool),
                "min_idle_event_spool_mib": min(row["event_files_mib"] for row in idle_spool),
                "threshold_100mib_seconds_relative_ingest": threshold["t"] - ingest if threshold else None,
                "tmp_first_seconds_relative_idle": tmp_first["t"] - idle if tmp_first else None,
                "runtime_upload_count": len(runtime_uploads),
                "runtime_upload_seconds_relative_idle": last_runtime_upload["host_t"] - idle if last_runtime_upload else None,
                "runtime_upload_node_seconds_relative_idle_uncorrected": last_runtime_upload["node_t"] - idle if last_runtime_upload else None,
                "runtime_upload_raw_mib": last_runtime_upload["raw_mib"] if last_runtime_upload else None,
                "reclaim_zero_seconds_relative_idle": reclaim["t"] - idle if reclaim else None,
                "upload_to_reclaim_seconds": reclaim["t"] - last_runtime_upload["host_t"] if reclaim and last_runtime_upload else None,
                "post_reclaim_observation_seconds": shutdown - reclaim["t"] if reclaim else None,
                "shutdown_upload_count": len(shutdown_uploads),
                "shutdown_upload_seconds_relative_idle": shutdown_uploads[-1]["host_t"] - idle if shutdown_uploads else None,
                "shutdown_upload_node_seconds_relative_idle_uncorrected": shutdown_uploads[-1]["node_t"] - idle if shutdown_uploads else None,
                "shutdown_upload_raw_mib": shutdown_uploads[-1]["raw_mib"] if shutdown_uploads else None,
                "final_current_mib": run["final"]["current_mib"],
                "final_peak_mib": run["final"]["peak_mib"],
            }
            for metric in memory_metrics:
                label = metric.removesuffix("_mib")
                event[f"{label}_mib_1s_before_reclaim"] = before[metric]
                event[f"{label}_mib_1s_after_reclaim"] = after[metric]
            for metric in ("current_mib", "anon_mib", "file_mib", "kernel_mib", "slab_mib", "file_dirty_mib"):
                label = metric.removesuffix("_mib")
                event[f"{label}_mib_upload_adjacent_pre"] = adjacent_pre[metric] if adjacent_pre else None
                event[f"{label}_mib_upload_adjacent_post"] = adjacent_post[metric] if adjacent_post else None
            run_events.append(event)
            reclaim_rows.append({key: rounded(value) if isinstance(value, float) else value for key, value in event.items()})

        numeric_fields = [
            key for key in run_events[0]
            if key not in {"arm", "target_events_per_second", "repeat"}
            and all(isinstance(event[key], (int, float)) or event[key] is None for event in run_events)
        ]
        aggregate = {field: summary([event[field] for event in run_events]) for field in numeric_fields}
        for field, stats in aggregate.items():
            unit = "seconds" if "seconds" in field else ("count" if field.endswith("count") else "MiB")
            reclaim_summary_rows.append({
                "target_events_per_second": rate,
                "metric": field,
                "n": stats["n"],
                "min": stats["min"],
                "median": stats["median"],
                "max": stats["max"],
                "unit": unit,
                "aggregation": "min/median/max across n=3 independent B arms; null runs excluded",
            })

        grid = [round(-10 + 0.5 * index, 1) for index in range(201)]  # through t=90s
        for t in grid:
            row = {"target_events_per_second": rate, "seconds_relative_ingest": t, "n_runs": 3}
            for metric in memory_metrics:
                values = [interpolate(memory_points(run, metric, True), t) for run in runs]
                stats = cross_quantiles(values)
                prefix = metric.removesuffix("_mib")
                for quantile in ("q0", "q25", "q50", "q75", "q100"):
                    row[f"{prefix}_{quantile}_mib"] = stats[quantile]
            for metric in spool_metrics:
                values = [interpolate(spool_points(run, metric, True), t) for run in runs]
                stats = cross_quantiles(values)
                prefix = metric.removesuffix("_mib")
                for quantile in ("q0", "q25", "q50", "q75", "q100"):
                    row[f"{prefix}_{quantile}_mib"] = stats[quantile]
            lifecycle_rows.append(row)
        result.append({
            "target_events_per_second": rate,
            "n_runs": 3,
            "phase_composition_time_weighted_mib": phase_result,
            "run_lifecycle_events": run_events,
            "lifecycle_event_summary": aggregate,
        })
    return result, composition_rows, reclaim_rows, reclaim_summary_rows, lifecycle_rows, idle_validator_rows


def calculate_c(arms):
    arm_rows = []
    summary_rows = []
    result = []
    for limit in C_LIMITS:
        verdict = json.loads((SOURCE / f"candidate-{limit}Mi.json").read_text())
        runs = [arms[f"C-limit{limit}Mi-r{repeat}"] for repeat in REPEATS]
        peaks = [run["final"]["peak_mib"] for run in runs]
        ratios = [peak / limit for peak in peaks]
        events_max = [run["final"]["events_max"] for run in runs]
        psi = [run["final"]["psi_full_total_usec"] for run in runs]
        spread = (max(peaks) - min(peaks)) / statistics.median(peaks)
        item = {
            "memory_limit_mib": limit,
            "n_runs": 3,
            "lifetime_peak_mib": summary(peaks),
            "peak_to_limit_ratio": summary(ratios),
            "three_run_peak_spread_fraction_of_median": rounded(spread),
            "events_max": summary(events_max),
            "events_max_sum": sum(events_max),
            "psi_full_total_usec": summary(psi),
            "psi_full_total_usec_sum": sum(psi),
            "events_oom_sum": sum(run["final"]["events_oom"] for run in runs),
            "events_oom_kill_sum": sum(run["final"]["events_oom_kill"] for run in runs),
            "verdict": verdict["verdict"],
            "verdict_errors": verdict["errors"],
            "arms": [],
        }
        candidate_by_arm = {candidate["armName"]: candidate for candidate in verdict["arms"]}
        for run in runs:
            arm = run["arm"]
            candidate = candidate_by_arm[arm]
            peak_composition = candidate["peakComposition"]
            arm_item = {
                "arm": arm,
                "repeat": run["report"]["config"]["repeat"],
                "memory_limit_mib": limit,
                "lifetime_peak_mib": run["final"]["peak_mib"],
                "peak_to_limit_ratio": run["final"]["peak_mib"] / limit,
                "sampled_adjacent_max_current_mib": peak_composition["currentBytes"] / MIB,
                "sampled_adjacent_anon_mib": peak_composition["anonBytes"] / MIB,
                "sampled_adjacent_file_mib": peak_composition["fileBytes"] / MIB,
                "sampled_adjacent_kernel_mib": peak_composition["kernelBytes"] / MIB,
                "events_max": run["final"]["events_max"],
                "psi_full_total_usec": run["final"]["psi_full_total_usec"],
                "events_oom": run["final"]["events_oom"],
                "events_oom_kill": run["final"]["events_oom_kill"],
                "achieved_events_per_second": run["report"]["replay"]["achieved_events_per_second"],
                "latency_p99_ms": run["report"]["replay"]["latency_p99_ms"],
                "classification": candidate["classification"],
            }
            item["arms"].append(arm_item)
            arm_rows.append({key: rounded(value) if isinstance(value, float) else value for key, value in arm_item.items()})
        result.append(item)
        summary_rows.append({
            "memory_limit_mib": limit,
            "n_runs": 3,
            "peak_mib_min": rounded(min(peaks)),
            "peak_mib_median": rounded(statistics.median(peaks)),
            "peak_mib_max": rounded(max(peaks)),
            "peak_to_limit_min": rounded(min(ratios)),
            "peak_to_limit_median": rounded(statistics.median(ratios)),
            "peak_to_limit_max": rounded(max(ratios)),
            "peak_spread_fraction": rounded(spread),
            "events_max_min": min(events_max),
            "events_max_median": statistics.median(events_max),
            "events_max_max": max(events_max),
            "events_max_sum": sum(events_max),
            "psi_full_usec_min": min(psi),
            "psi_full_usec_median": statistics.median(psi),
            "psi_full_usec_max": max(psi),
            "psi_full_usec_sum": sum(psi),
            "events_oom_sum": item["events_oom_sum"],
            "events_oom_kill_sum": item["events_oom_kill_sum"],
            "verdict": verdict["verdict"],
            "verdict_error_count": len(verdict["errors"]),
        })
    return result, arm_rows, summary_rows


def calculate_integrity(arms, discrepancies):
    reports = [run["report"] for run in arms.values()]
    logs = [report["collectorLogs"][0] for report in reports]
    replay = [report["replay"] for report in reports]
    remote = [report["remote"] for report in reports]
    runtime = [report["runtime"] for report in reports]
    ray_runtime = [report["rayRuntime"] for report in reports]
    completion = json.loads((SOURCE / "completion.json").read_text())

    totals = {
        "executed_arms": len(completion["executedArms"]),
        "reports": len(reports),
        "completed_reports": sum(report["completed"] is True for report in reports),
        "planned_events": sum(item["planned_events"] for item in replay),
        "sent_events": sum(item["sent_events"] for item in replay),
        "accepted_events": sum(item["accepted_events"] for item in replay),
        "remote_lines": sum(item["lines"] for item in remote),
        "remote_unique_event_ids": sum(item["uniqueEventIDs"] for item in remote),
        "expected_raw_jsonl_bytes": sum(item["expected_jsonl_bytes"] for item in replay),
        "remote_raw_jsonl_bytes": sum(item["rawJSONLBytes"] for item in remote),
        "uploaded_raw_jsonl_bytes": sum(item["uploadedBytes"] for item in logs),
        "remote_stored_compressed_bytes": sum(item["storedBytes"] for item in remote),
        "remote_event_objects": sum(item["objects"] for item in remote),
        "uploads": sum(item["uploads"] for item in logs),
        "requests": sum(item["requests"] for item in replay),
        "accepted_requests": sum(item["accepted_requests"] for item in replay),
        "non_200_responses": sum(item["non_200_responses"] for item in replay),
        "retries": sum(item["retries"] for item in replay),
        "duplicate_event_ids": sum(item["duplicateEventIDs"] for item in remote),
        "malformed_lines": sum(item["malformedLines"] for item in remote),
        "unexpected_event_ids": sum(item["unexpectedEventIDs"] for item in remote),
        "upload_failures": sum(item["uploadFailures"] for item in logs),
        "rotation_queue_full": sum(item["rotationQueueFull"] for item in logs),
        "disk_pressure_503s": sum(item["diskPressure503s"] for item in logs),
        "collector_restarts": sum(item["restartCount"] for item in runtime),
        "ray_restarts": sum(item["restartCount"] for item in ray_runtime),
        "final_events_oom": sum(item["finalCgroupMemory"]["eventsOOM"] for item in logs),
        "final_events_oom_kill": sum(item["finalCgroupMemory"]["eventsOOMKill"] for item in logs),
        "cgroup_read_errors": sum(item["cgroupMemoryReadErrors"] for item in logs),
        "invalid_spool_samples": sum(report["eventSpool"]["invalidSamples"] for report in reports),
        "memory_detail_read_errors": sum(report["memoryDetail"]["readErrors"] for report in reports),
        "unique_arm_names": len(set(report["config"]["armName"] for report in reports)),
        "unique_pod_uids": len(set(report["runtime"]["podUID"] for report in reports)),
        "unique_collector_container_ids": len(set(report["runtime"]["containerID"] for report in reports)),
        "unique_sessions": len(set(report["sessionName"] for report in reports)),
        "unique_remote_prefixes": len(set(report["remote"]["prefix"] for report in reports)),
        "collector_image_ids": sorted(set(report["runtime"]["imageID"] for report in reports)),
        "ray_image_ids": sorted(set(report["rayRuntime"]["imageID"] for report in reports)),
        "selected_memory_limit": completion["selectedMemoryLimit"],
    }
    checks = {
        "27_arms_and_reports": totals["executed_arms"] == totals["reports"] == totals["completed_reports"] == 27,
        "event_counts_reconcile": totals["planned_events"] == totals["sent_events"] == totals["accepted_events"] == totals["remote_lines"] == totals["remote_unique_event_ids"],
        "raw_bytes_reconcile": totals["expected_raw_jsonl_bytes"] == totals["remote_raw_jsonl_bytes"] == totals["uploaded_raw_jsonl_bytes"],
        "request_counts_reconcile": totals["requests"] == totals["accepted_requests"],
        "no_corruption_or_transport_failure": all(totals[key] == 0 for key in (
            "non_200_responses", "retries", "duplicate_event_ids", "malformed_lines",
            "unexpected_event_ids", "upload_failures", "rotation_queue_full", "disk_pressure_503s",
        )),
        "no_restart_or_oom": all(totals[key] == 0 for key in (
            "collector_restarts", "ray_restarts", "final_events_oom", "final_events_oom_kill",
        )),
        "samplers_clean": totals["cgroup_read_errors"] == totals["invalid_spool_samples"] == totals["memory_detail_read_errors"] == 0,
        "identities_unique": all(totals[key] == 27 for key in (
            "unique_arm_names", "unique_pod_uids", "unique_collector_container_ids", "unique_sessions", "unique_remote_prefixes",
        )),
        "single_image_per_component": len(totals["collector_image_ids"]) == len(totals["ray_image_ids"]) == 1,
        "selected_512Mi": totals["selected_memory_limit"] == "512Mi",
    }
    for name, passed in checks.items():
        if not passed:
            discrepancies.append({"scope": "integrity", "check": name, "detail": "recomputed integrity check failed"})

    rows = []
    units = {
        "planned_events": "events", "sent_events": "events", "accepted_events": "events",
        "remote_lines": "events", "remote_unique_event_ids": "events",
        "expected_raw_jsonl_bytes": "bytes", "remote_raw_jsonl_bytes": "bytes",
        "uploaded_raw_jsonl_bytes": "bytes", "remote_stored_compressed_bytes": "bytes",
        "requests": "requests", "accepted_requests": "requests", "uploads": "files",
        "remote_event_objects": "objects",
    }
    for metric, value in totals.items():
        rows.append({
            "metric": metric,
            "value": json.dumps(value, separators=(",", ":")) if isinstance(value, (list, dict)) else value,
            "unit": units.get(metric, "count" if isinstance(value, int) else "identifier"),
            "aggregation": "sum across 27 arms" if metric not in {"selected_memory_limit", "collector_image_ids", "ray_image_ids"} and not metric.startswith("unique_") else "campaign exact value or distinct count",
        })
    for check, passed in checks.items():
        rows.append({"metric": f"check:{check}", "value": str(passed).lower(), "unit": "boolean", "aggregation": "recomputed equality/invariant"})
    return {"totals": totals, "checks": checks}, rows


def schema():
    common = {
        "MiB": "bytes / 1,048,576 (2^20)",
        "time_weighted_percentile": "Treat each cgroup/spool sample as piecewise-constant until the next sample, clip each interval to the named phase, pool interval durations across n=3, then take the weighted percentile.",
        "cross_run_lifecycle_quantile": "Align each run at its measured ingest phase start, linearly interpolate each raw series onto a 0.5-second real-time grid, then calculate q0/q25/q50/q75/q100 across n=3 at each grid point.",
        "dense_sample_time": "Cgroup/spool samples retain raw kind-node unixNano and are differenced against host phase timestamps, matching the formal validator and published percentile contract. Clock probes measured the node about 20ms ahead; this uncertainty is below the 250ms sample cadence and is not silently removed.",
        "lifetime_peak": "Exact final in-process cgroup-v2 memory.peak; median/min/max are across n=3. This is not max(memory.current median trajectory).",
        "upload_time": "Collector log upload-completion unixNano converted from node to host clock using linear interpolation of measured node-minus-host skewNano between bracketing probes; reported relative to measured phase boundaries.",
        "upload_bytes": "Raw rotated JSONL file size (rotationTask.size), not gzip object bytes.",
    }
    return {
        "schema_version": 1,
        "source": str(SOURCE),
        "common_definitions": common,
        "files": {
            "a_rate_summary.csv": {
                "row": "one target event rate; n=3 independent A arms",
                "aggregation": "time-weighted ingest percentiles plus cross-run min/median/max exact final values",
                "units": "rate=events/s; memory=MiB; raw_jsonl=MiB",
            },
            "a_lifecycle_quantiles.csv": {
                "row": "one target rate and one 0.5-second grid point",
                "aggregation": common["cross_run_lifecycle_quantile"],
                "units": "time=seconds relative to ingest; memory=MiB",
            },
            "a_uploads.csv": {
                "row": "one upload ordinal at one A target rate, summarized across n=3",
                "aggregation": "min/median/max across corresponding ordinal in n=3",
                "units": "time=seconds relative to ingest; size=raw JSONL MiB",
            },
            "b_phase_composition.csv": {
                "row": "one B target rate, phase window, source, and memory/spool metric",
                "aggregation": common["time_weighted_percentile"],
                "units": "MiB",
            },
            "b_lifecycle_quantiles.csv": {
                "row": "one B target rate and one 0.5-second grid point",
                "aggregation": common["cross_run_lifecycle_quantile"],
                "units": "time=seconds relative to ingest; memory/spool=MiB",
            },
            "b_upload_reclaim_runs.csv": {
                "row": "one B arm",
                "aggregation": "exact per-run event times and linearly interpolated cgroup composition 1 second before/after first observed zero-spool reclaim",
                "units": "time=seconds relative to ingest/idle; memory=MiB",
            },
            "b_upload_reclaim_summary.csv": {
                "row": "one B target rate and one lifecycle metric",
                "aggregation": "min/median/max across n=3 independent B arms; null runs excluded",
                "units": "stated per row",
            },
            "b_idle_window_validator_medians.csv": {
                "row": "one B target rate, one final-idle 30-second window, and one cgroup metric",
                "aggregation": "unweighted raw-sample median per arm exactly matching validator median_window(), plus median of the three arm medians",
                "units": "MiB",
            },
            "c_candidate_arms.csv": {
                "row": "one C arm",
                "aggregation": "exact final cgroup counters and candidate-evaluator adjacent sampled composition",
                "units": "memory=MiB; ratio=fraction; PSI=microseconds; latency=milliseconds",
            },
            "c_candidate_summary.csv": {
                "row": "one tested memory limit; n=3",
                "aggregation": "cross-run min/median/max, sums for counters, and exact validator verdict",
                "units": "memory=MiB; ratio/spread=fraction; PSI=microseconds",
            },
            "integrity_totals.csv": {
                "row": "one campaign total or recomputed invariant",
                "aggregation": "sum/distinct/equality across all 27 arms, stated per row",
                "units": "stated per row",
            },
            "column_definitions.csv": {
                "row": "one data CSV column",
                "aggregation": "exact data dictionary; no measurement aggregation",
                "units": "definition supplies the data-column unit",
            },
            "results.json": {
                "row": "nested campaign summary plus discrepancy list; dense lifecycle grids remain in CSV",
                "aggregation": "same definitions as CSV files",
                "units": "field suffix and this schema",
            },
        },
        "field_conventions": {
            "p50/p95/p99": "time-weighted percentile when used in A rate or B phase summaries",
            "q0/q25/q50/q75/q100": "cross-run lifecycle quantile at a fixed aligned time",
            "min/median/max": "across n=3 independent arms",
            "events_max": "final absolute memory.events max counter; baseline was zero in these fresh cgroups, so it is also the campaign-arm delta",
            "psi_full_total_usec": "final absolute cgroup memory.pressure full total counter; baseline was zero in these fresh cgroups, so it is also the campaign-arm delta",
            "peak_to_limit_ratio": "exact final memory.peak bytes divided by configured memory.max bytes",
            "reclaim_zero": "first valid event-spool sample at or after runtime upload completion where raw+gzip+tmp event-file bytes equal zero",
        },
    }


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    report_paths = sorted(SOURCE.glob("*/*/collector-memory-report.json"))
    arms = {}
    for path in report_paths:
        arm = path.relative_to(SOURCE).parts[0]
        arms[arm] = load_arm(arm)
    if len(arms) != 27:
        raise RuntimeError(f"expected 27 arms, found {len(arms)}")

    discrepancies = []
    a, a_rows, a_lifecycle, a_uploads = calculate_a(arms)
    b, b_composition, b_reclaim, b_reclaim_summary, b_lifecycle, b_idle_validator = calculate_b(arms)
    c, c_arms, c_summary = calculate_c(arms)
    integrity, integrity_rows = calculate_integrity(arms, discrepancies)

    # Cross-check candidate JSON peak evidence against the report final counters.
    for candidate in c:
        for arm in candidate["arms"]:
            run = arms[arm["arm"]]
            if abs(arm["lifetime_peak_mib"] - run["final"]["peak_mib"]) > 1e-12:
                discrepancies.append({"scope": arm["arm"], "check": "candidate_peak", "detail": "candidate/report final peak differs"})

    # Fresh-cgroup C counters must start at zero for final absolute counters to
    # also be valid deltas.
    for limit in C_LIMITS:
        for repeat in REPEATS:
            run = arms[f"C-limit{limit}Mi-r{repeat}"]
            first = run["memory"][0]
            if first["events_max"] != 0 or first["psi_full_total_usec"] != 0:
                discrepancies.append({
                    "scope": run["arm"],
                    "check": "fresh_cgroup_counter_baseline",
                    "detail": f"first events_max={first['events_max']} psi_full_total_usec={first['psi_full_total_usec']}",
                })

    results = {
        "schema_version": 1,
        "source": str(SOURCE),
        "generated_from_reports": len(arms),
        "definitions": "schema.json",
        "A": a,
        "B": b,
        "C": c,
        "integrity": integrity,
        "discrepancies": discrepancies,
    }

    (OUT / "schema.json").write_text(json.dumps(schema(), indent=2) + "\n")
    (OUT / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    write_csv(OUT / "a_rate_summary.csv", a_rows, list(a_rows[0]))
    write_csv(OUT / "a_lifecycle_quantiles.csv", a_lifecycle, list(a_lifecycle[0]))
    write_csv(OUT / "a_uploads.csv", a_uploads, list(a_uploads[0]))
    write_csv(OUT / "b_phase_composition.csv", b_composition, list(b_composition[0]))
    write_csv(OUT / "b_lifecycle_quantiles.csv", b_lifecycle, list(b_lifecycle[0]))
    write_csv(OUT / "b_upload_reclaim_runs.csv", b_reclaim, list(b_reclaim[0]))
    write_csv(OUT / "b_upload_reclaim_summary.csv", b_reclaim_summary, list(b_reclaim_summary[0]))
    write_csv(OUT / "b_idle_window_validator_medians.csv", b_idle_validator, list(b_idle_validator[0]))
    write_csv(OUT / "c_candidate_arms.csv", c_arms, list(c_arms[0]))
    write_csv(OUT / "c_candidate_summary.csv", c_summary, list(c_summary[0]))
    write_csv(OUT / "integrity_totals.csv", integrity_rows, list(integrity_rows[0]))
    csv_paths = [
        OUT / "a_rate_summary.csv",
        OUT / "a_lifecycle_quantiles.csv",
        OUT / "a_uploads.csv",
        OUT / "b_phase_composition.csv",
        OUT / "b_lifecycle_quantiles.csv",
        OUT / "b_upload_reclaim_runs.csv",
        OUT / "b_upload_reclaim_summary.csv",
        OUT / "b_idle_window_validator_medians.csv",
        OUT / "c_candidate_arms.csv",
        OUT / "c_candidate_summary.csv",
        OUT / "integrity_totals.csv",
    ]
    write_column_definitions(csv_paths)

    readme = """# Ray Summit Collector formal r7 result bundle

This bundle is recomputed read-only from `/private/tmp/kuberay-collector-memory-formal-20260813-r7`.

- `SUMMARY.md`: concise human-readable A/B/C/integrity result.
- `results.json`: nested A/B/C/integrity results and discrepancies.
- `schema.json`: exact definitions, units, and aggregation contracts.
- `column_definitions.csv`: exact definition, unit, and aggregation for every data CSV column.
- `a_rate_summary.csv`: A n=3 rate summary.
- `a_lifecycle_quantiles.csv`: four-rate 0.5-second lifecycle quantiles.
- `a_uploads.csv`: A upload completion timing and raw sizes.
- `b_phase_composition.csv`: B ingest/idle cgroup and spool composition.
- `b_lifecycle_quantiles.csv`: B 2k/5k 0.5-second lifecycle quantiles.
- `b_upload_reclaim_runs.csv`: B threshold, upload, reclaim, and pre/post composition per run.
- `b_upload_reclaim_summary.csv`: B lifecycle event min/median/max across n=3.
- `b_idle_window_validator_medians.csv`: exact two-window plateau medians used by the validator.
- `c_candidate_arms.csv`: C per-arm resource-pressure evidence.
- `c_candidate_summary.csv`: C per-limit verdict summary.
- `integrity_totals.csv`: 27-arm reconciliation and invariant totals.
- `build_bundle.py`: reproducible extractor.

Read `schema.json` before plotting. Headline P95/P99 values are time-weighted; upload markers are skew-corrected completion timestamps. Dense cgroup/spool samples retain the formal validator's raw node timestamps, with the measured ~20ms clock boundary documented rather than hidden.
"""
    (OUT / "README.md").write_text(readme)

    manifest = {"schema_version": 1, "source": str(SOURCE), "files": {}}
    owned_files = {
        "README.md", "SUMMARY.md", "validation.json", "build_bundle.py",
        "schema.json", "results.json", "column_definitions.csv",
        "a_rate_summary.csv", "a_lifecycle_quantiles.csv", "a_uploads.csv",
        "b_phase_composition.csv", "b_lifecycle_quantiles.csv",
        "b_upload_reclaim_runs.csv", "b_upload_reclaim_summary.csv",
        "b_idle_window_validator_medians.csv", "c_candidate_arms.csv",
        "c_candidate_summary.csv", "integrity_totals.csv",
    }
    for path in sorted(OUT / name for name in owned_files):
        if not path.is_file():
            continue
        manifest["files"][path.name] = {
            "bytes": path.stat().st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }
    (OUT / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(OUT)


if __name__ == "__main__":
    main()
