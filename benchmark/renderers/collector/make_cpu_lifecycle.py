#!/usr/bin/env python3
"""Build a formal-r7 Collector CPU lifecycle chart from cgroup v2 counters.

The chart derives interval millicores from the cumulative cpu.stat usage_usec
counter.  It does not extrapolate after the last cgroup sample.
"""

from __future__ import annotations

import bisect
import csv
import glob
import importlib.util
import json
import math
import statistics
import sys
from dataclasses import dataclass
from pathlib import Path


SOURCE = Path("/private/tmp/kuberay-collector-memory-formal-20260813-r7")
OUT = Path("/private/tmp/ray-summit-collector-results-r7/charts")
HELPER = Path("/private/tmp/collector-memory-lifecycle-4-rates/make_lifecycle_plots.py")
NAME = "07-a-cpu-lifecycle-1k-vs-5k"
RATES = (1000, 5000)
REPEATS = (1, 2, 3)
T_MIN, T_PLOT_MAX, T_DATA_MAX, DT = -10.0, 108.0, 105.25, 0.25
Y_MAX = 2200.0
MIB = 1024 * 1024


spec = importlib.util.spec_from_file_location("lifecycle_helper", HELPER)
if spec is None or spec.loader is None:
    raise RuntimeError(f"cannot import {HELPER}")
m = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = m
spec.loader.exec_module(m)
m.FONT = OUT / "Basic-Regular.ttf"

BG, PLOT_BG, FG, MUTED = m.BG, m.PLOT_BG, m.FG, m.MUTED
GRID, FRAME = m.GRID, m.FRAME
BLUE, YELLOW, RED, GREEN = m.CURRENT_COLOR, m.UPLOAD_COLOR, "#FF7272", m.ANON_COLOR
PHASES = [
    ("Baseline", -10.0, 0.0, "#66717D"),
    ("Ingest", 0.0, 90.0, "#285477"),
    ("Idle", 90.0, 105.0, "#57426F"),
    ("Shutdown", 105.0, 108.0, "#25614E"),
]


@dataclass
class CpuRun:
    rate: int
    repeat: int
    report_path: Path
    times: list[float]
    usage_usec: list[int]
    interval_start: list[float]
    interval_end: list[float]
    interval_mcpu: list[float]
    nr_throttled: list[int]
    throttled_usec: list[int]
    upload_times: list[float]
    phases: dict[str, float]
    raw_mib: float
    cpu_request_mcpu: float
    cpu_limit_mcpu: float


def median(values):
    return float(statistics.median(list(values)))


def percentile(values: list[float], q: float) -> float:
    xs = sorted(values)
    pos = (len(xs) - 1) * q
    lo, hi = math.floor(pos), math.ceil(pos)
    if lo == hi:
        return xs[lo]
    return xs[lo] + (xs[hi] - xs[lo]) * (pos - lo)


def parse_cpu_mcpu(value: str) -> float:
    if value.endswith("m"):
        return float(value[:-1])
    return float(value) * 1000.0


def load_run(rate: int, repeat: int) -> CpuRun:
    hits = glob.glob(str(SOURCE / f"A-rate{rate}-r{repeat}" / "*" / "collector-memory-report.json"))
    if len(hits) != 1:
        raise RuntimeError(f"expected one report for rate={rate}, repeat={repeat}; got {hits}")
    report_path = Path(hits[0])
    report = json.loads(report_path.read_text())
    phase_ns = {row["name"]: int(row["timeNano"]) for row in report["phases"]}
    ingest_ns = phase_ns["ingest"]
    phases = {name: (value - ingest_ns) / 1e9 for name, value in phase_ns.items()}

    rows = []
    with (report_path.parent / "cgroup_samples.csv").open(newline="") as f:
        for row in csv.DictReader(f):
            if row["container"].endswith("/collector"):
                rows.append((
                    (int(row["time_nano"]) - ingest_ns) / 1e9,
                    int(row["cpu_usage_usec"]),
                    int(row["nr_throttled"]),
                    int(row["throttled_usec"]),
                ))
    rows.sort()
    if len(rows) < 300:
        raise RuntimeError(f"too few CPU samples in {report_path.parent / 'cgroup_samples.csv'}")
    if any(b[0] <= a[0] for a, b in zip(rows, rows[1:])):
        raise RuntimeError(f"non-monotonic timestamps in {report_path}")
    if any(b[1] < a[1] for a, b in zip(rows, rows[1:])):
        raise RuntimeError(f"cpu_usage_usec reset in {report_path}")
    if rows[0][0] > T_MIN or rows[-1][0] < T_DATA_MAX:
        raise RuntimeError(f"insufficient common CPU coverage in {report_path}: {rows[0][0]:.3f}..{rows[-1][0]:.3f}")

    starts, ends, rates = [], [], []
    for left, right in zip(rows, rows[1:]):
        dt_seconds = right[0] - left[0]
        delta_cpu_usec = right[1] - left[1]
        # microseconds CPU / second wall / 1000 = millicores.
        starts.append(left[0])
        ends.append(right[0])
        rates.append(delta_cpu_usec / dt_seconds / 1000.0)

    log = report["collectorLogs"][0]
    uploads = [(int(row["unixNano"]) - ingest_ns) / 1e9 for row in log["uploadTimeline"]]
    config = report["config"]
    return CpuRun(
        rate=rate,
        repeat=repeat,
        report_path=report_path,
        times=[row[0] for row in rows],
        usage_usec=[row[1] for row in rows],
        interval_start=starts,
        interval_end=ends,
        interval_mcpu=rates,
        nr_throttled=[row[2] for row in rows],
        throttled_usec=[row[3] for row in rows],
        upload_times=uploads,
        phases=phases,
        raw_mib=int(report["replay"]["expected_jsonl_bytes"]) / MIB,
        cpu_request_mcpu=parse_cpu_mcpu(config["cpuRequest"]),
        cpu_limit_mcpu=parse_cpu_mcpu(config["cpuLimit"]),
    )


def usage_at(run: CpuRun, t: float) -> float:
    index = bisect.bisect_right(run.times, t) - 1
    if index < 0 or index >= len(run.times) - 1:
        raise ValueError(f"{t} outside counter coverage")
    t0, t1 = run.times[index], run.times[index + 1]
    u0, u1 = run.usage_usec[index], run.usage_usec[index + 1]
    ratio = (t - t0) / (t1 - t0)
    return u0 + ratio * (u1 - u0)


def rate_at(run: CpuRun, t: float) -> float:
    index = bisect.bisect_right(run.interval_start, t) - 1
    if index < 0 or index >= len(run.interval_mcpu) or t > run.interval_end[index]:
        raise ValueError(f"{t} outside interval-rate coverage")
    return run.interval_mcpu[index]


def run_stats(run: CpuRun) -> dict[str, float | int]:
    idle_t = run.phases["idle"]
    ingest_mean = (usage_at(run, idle_t) - usage_at(run, 0.0)) / idle_t / 1000.0
    ingest_rates = [
        value for start, end, value in zip(run.interval_start, run.interval_end, run.interval_mcpu)
        if 0.0 <= (start + end) / 2 <= idle_t
    ]
    lifecycle_rates = [
        value for start, value in zip(run.interval_start, run.interval_mcpu)
        if T_MIN <= start
    ]
    return {
        "ingest_mean_mcpu": ingest_mean,
        "ingest_interval_p95_mcpu": percentile(ingest_rates, 0.95),
        "lifecycle_interval_peak_mcpu": max(lifecycle_rates),
        "nr_throttled_delta": run.nr_throttled[-1] - run.nr_throttled[0],
        "throttled_usec_delta": run.throttled_usec[-1] - run.throttled_usec[0],
        "first_sample_s": run.times[0],
        "last_sample_s": run.times[-1],
        "phase_complete_s": run.phases["complete"],
    }


def aggregate(runs: list[CpuRun]) -> tuple[dict[int, dict], dict]:
    count = int(round((T_DATA_MAX - T_MIN) / DT))
    grid = [round(T_MIN + i * DT, 6) for i in range(count + 1)]
    chart = {}
    summary = {}
    for rate in RATES:
        rr = [run for run in runs if run.rate == rate]
        values = [[rate_at(run, t) for run in rr] for t in grid]
        run_metric = [run_stats(run) for run in rr]
        metric_summary = {}
        for key in ("ingest_mean_mcpu", "ingest_interval_p95_mcpu", "lifecycle_interval_peak_mcpu"):
            vals = [float(row[key]) for row in run_metric]
            metric_summary[key] = {"median": median(vals), "min": min(vals), "max": max(vals), "per_repeat": vals}
        upload_count = len(rr[0].upload_times)
        if any(len(run.upload_times) != upload_count for run in rr):
            raise RuntimeError(f"upload ordinal mismatch for rate={rate}")
        uploads = []
        for index in range(upload_count):
            vals = [run.upload_times[index] for run in rr]
            uploads.append({"ordinal": index + 1, "median_s": median(vals), "min_s": min(vals), "max_s": max(vals), "per_repeat_s": vals})
        chart[rate] = {
            "grid": grid,
            "median": [median(v) for v in values],
            "min": [min(v) for v in values],
            "max": [max(v) for v in values],
            "uploads": uploads,
            "raw_mib": rr[0].raw_mib,
            "request_mcpu": rr[0].cpu_request_mcpu,
            "limit_mcpu": rr[0].cpu_limit_mcpu,
        }
        summary[str(rate)] = {
            "n": len(rr),
            "raw_mib": rr[0].raw_mib,
            "request_mcpu": rr[0].cpu_request_mcpu,
            "limit_mcpu": rr[0].cpu_limit_mcpu,
            **metric_summary,
            "uploads": uploads,
            "nr_throttled_delta_total": sum(int(row["nr_throttled_delta"]) for row in run_metric),
            "throttled_usec_delta_total": sum(int(row["throttled_usec_delta"]) for row in run_metric),
            "first_sample_s_range": [min(float(row["first_sample_s"]) for row in run_metric), max(float(row["first_sample_s"]) for row in run_metric)],
            "last_sample_s_range": [min(float(row["last_sample_s"]) for row in run_metric), max(float(row["last_sample_s"]) for row in run_metric)],
            "phase_complete_s_range": [min(float(row["phase_complete_s"]) for row in run_metric), max(float(row["phase_complete_s"]) for row in run_metric)],
        }
    return chart, summary


def dashed_horizontal(parts: list[str], x0: float, x1: float, y: float, color: str, width: float = 4.0):
    dash, gap = 24.0, 16.0
    x = x0
    while x < x1:
        parts.append(m.rect(x, y - width / 2, min(dash, x1 - x), width, fill=color, opacity=.78))
        x += dash + gap


def plot_panel(parts: list[str], data: dict, stats: dict, rate: int, y0: float, *, show_x_labels: bool):
    x0, width, height = 170.0, 2860.0, 500.0
    sx = lambda t: x0 + (t - T_MIN) / (T_PLOT_MAX - T_MIN) * width
    sy = lambda value: y0 + height - value / Y_MAX * height
    parts.append(m.text(x0, y0 - 82, f"{rate//1000}k events/s", 39, weight=500))
    s = stats[str(rate)]
    mean = s["ingest_mean_mcpu"]
    p95 = s["ingest_interval_p95_mcpu"]
    peak = s["lifecycle_interval_peak_mcpu"]
    stat_text = (
        f"raw {data['raw_mib']:.1f} MiB · ingest mean {mean['median']:.1f} mCPU "
        f"[{mean['min']:.1f}–{mean['max']:.1f}] · interval P95 {p95['median']:.1f} · per-run peak {peak['median']:.1f} mCPU"
    )
    parts.append(m.text(x0 + 380, y0 - 82, stat_text, 25, fill=MUTED))
    parts.append(m.rect(x0, y0, width, height, fill=PLOT_BG, stroke=FRAME, width=2))
    for label, start, end, color in PHASES:
        parts.append(m.rect(sx(start), y0, sx(end) - sx(start), height, fill=color, opacity=.115))
        parts.append(m.rect(sx(start), y0 - 40, sx(end) - sx(start), 32, fill=color, opacity=.62))
        fs = 18 if end - start <= 4 else 23
        parts.append(m.text((sx(start) + sx(end)) / 2, y0 - 16, label, fs, anchor="middle", weight=500))
    for value in (0, 100, 500, 1000, 1500, 2000, 2200):
        y = sy(value)
        parts.append(m.line(x0, y, x0 + width, y, stroke=GRID, width=1.4, opacity=.64))
        parts.append(m.text(x0 - 22, y + 9, value, 24, fill=MUTED, anchor="end"))
    for value in (-10, 0, 30, 60, 90, 105, 108):
        x = sx(value)
        parts.append(m.line(x, y0, x, y0 + height, stroke=GRID, width=1.2, opacity=.55))
        if show_x_labels:
            parts.append(m.text(x, y0 + height + 42, value, 24, fill=MUTED, anchor="middle"))

    upper = [(sx(t), sy(v)) for t, v in zip(data["grid"], data["max"])]
    lower = [(sx(t), sy(v)) for t, v in zip(data["grid"], data["min"])]
    center = [(sx(t), sy(v)) for t, v in zip(data["grid"], data["median"])]
    parts.append(m.area(upper, lower, fill=BLUE, opacity=.22))
    parts.append(m.path(center, stroke=BLUE, width=6))

    dashed_horizontal(parts, x0, x0 + width, sy(data["request_mcpu"]), YELLOW, 4)
    dashed_horizontal(parts, x0, x0 + width, sy(data["limit_mcpu"]), RED, 4)
    parts.append(m.text(x0 + width - 16, sy(data["request_mcpu"]) - 10, "CPU request 100m", 21, fill=YELLOW, anchor="end", weight=500))
    parts.append(m.text(x0 + width - 16, sy(data["limit_mcpu"]) - 10, "CPU limit 2,000m", 21, fill=RED, anchor="end", weight=500))

    for upload in data["uploads"]:
        t = upload["median_s"]
        parts.append(m.rect(sx(t) - 2, y0, 4, height, fill=YELLOW, opacity=.42))
        parts.append(m.diamond(sx(t), y0 + 92, 10, fill=YELLOW, stroke=FG, width=2))
        label = f"{t:.1f}s"
        parts.append(m.text(sx(t) - 8, y0 + 132, label, 20, fill=YELLOW, anchor="end", weight=500))

    # The shutdown cgroup sample ends before the log-derived completion marker.
    parts.append(m.rect(sx(T_DATA_MAX) - 2, y0, 4, height, fill=MUTED, opacity=.55))
    if rate == 5000:
        parts.append(m.text(sx(T_DATA_MAX) - 10, y0 + height - 18, "last common CPU sample", 19, fill=MUTED, anchor="end"))


def build_chart(chart: dict, stats: dict) -> str:
    parts = m.svg_header(
        "Collector CPU lifecycle at 1k and 5k events per second",
        "Two formal-r7 CPU lifecycle panels derived from cgroup v2 cumulative cpu.stat usage_usec counters.",
    )
    parts.append(m.text(120, 100, "Collector CPU Lifecycle — 1k/s vs 5k/s", 68, weight=500))
    parts.append(m.text(120, 155, "Formal r7 A matrix · interval mCPU from cumulative cgroup v2 cpu.stat usage_usec · n=3 fresh Pods/rate", 29, fill=MUTED))

    parts.append(m.circle(155, 222, 10, fill=BLUE, stroke=BLUE, width=1))
    parts.append(m.text(180, 231, "median interval CPU", 24, weight=500))
    parts.append(m.rect(520, 211, 48, 20, fill=BLUE, opacity=.25))
    parts.append(m.text(585, 231, "min–max across repeats", 24, weight=500))
    parts.append(m.diamond(1010, 221, 10, fill=YELLOW, stroke=FG, width=2))
    parts.append(m.text(1038, 231, "upload complete (median time)", 24, weight=500))
    parts.append(m.text(3025, 231, "Shared axes · 0.25 s alignment grid", 24, fill=MUTED, anchor="end"))

    plot_panel(parts, chart[1000], stats, 1000, 355, show_x_labels=False)
    plot_panel(parts, chart[5000], stats, 5000, 1040, show_x_labels=True)
    y_label = m.text(62, 900, "CPU usage (millicores)", 31, anchor="middle", cls="cpu-y")
    parts.append(y_label.replace('class="cpu-y"', 'transform="rotate(-90 62 900)"'))
    parts.append(m.text(1600, 1622, "Seconds from ingest start", 29, anchor="middle", weight=500))
    parts.append(m.rect(130, 1660, 2940, 78, fill="#162131", opacity=.96))
    parts.append(m.text(165, 1710,
                        "Evidence boundary: sampled CPU covers baseline, ingest, idle, runtime uploads, and shutdown onset; it stops at 105.25 s and does not extrapolate the final cgroup tail.",
                        24, fill=FG, weight=500))
    parts.append(m.text(3050, 1774,
                        "One Collector sidecar cgroup in the head Pod · configured --role=Worker · CPU request 100m / limit 2 · no throttling observed",
                        22, fill=MUTED, anchor="end"))
    parts.append("</svg>")
    return "\n".join(parts)


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    runs = [load_run(rate, repeat) for rate in RATES for repeat in REPEATS]
    if len({run.cpu_request_mcpu for run in runs}) != 1 or len({run.cpu_limit_mcpu for run in runs}) != 1:
        raise RuntimeError("CPU resources differ across formal r7 A arms")
    chart, stats = aggregate(runs)
    svg_path = OUT / f"{NAME}.svg"
    png_path = OUT / f"{NAME}.png"
    svg_path.write_text(build_chart(chart, stats))
    m.rasterize(svg_path, png_path)

    summary = {
        "source": str(SOURCE),
        "artifacts_sufficient": True,
        "counter": "cgroup v2 cpu.stat usage_usec cumulative CPU microseconds",
        "series_method": "delta cpu_usage_usec / delta wall time; interval rate assigned to a 0.25 s aligned grid; n=3 pointwise median/min/max",
        "statistics_method": "per-run metrics first, then median/min/max across n=3",
        "plotted_time_seconds": [T_MIN, T_PLOT_MAX],
        "counter_series_time_seconds": [T_MIN, T_DATA_MAX],
        "evidence_boundary": "The last common CPU rate sample is 105.25 s. Final shutdown upload completion occurs later, so the chart marks it from logs but does not extrapolate CPU after the counter stops.",
        "rates": stats,
        "qa": {
            "run_count": len(runs),
            "all_usage_counters_monotonic": True,
            "all_cover_baseline_to_shutdown_onset": all(run.times[0] <= T_MIN and run.times[-1] >= T_DATA_MAX for run in runs),
            "all_have_expected_upload_timeline": all(len(run.upload_times) == (1 if run.rate == 1000 else 3) for run in runs),
            "all_cpu_requests_100m": all(run.cpu_request_mcpu == 100 for run in runs),
            "all_cpu_limits_2000m": all(run.cpu_limit_mcpu == 2000 for run in runs),
            "no_throttling_observed": all(run.nr_throttled[-1] == run.nr_throttled[0] and run.throttled_usec[-1] == run.throttled_usec[0] for run in runs),
            "png_nonempty": png_path.exists() and png_path.stat().st_size > 100_000,
            "svg_nonempty": svg_path.exists() and svg_path.stat().st_size > 5_000,
        },
        "outputs": {"png": str(png_path), "svg": str(svg_path)},
    }
    summary["qa"]["all_checks_pass"] = all(summary["qa"].values())
    stats_path = OUT / f"{NAME}-stats.json"
    stats_path.write_text(json.dumps(summary, indent=2) + "\n")
    print(png_path)
    print(svg_path)
    print(stats_path)


if __name__ == "__main__":
    main()
