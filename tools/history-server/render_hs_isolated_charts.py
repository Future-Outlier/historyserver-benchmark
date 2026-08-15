#!/usr/bin/env python3
"""Render the final fixed-profile History Server charts.

The renderer is intentionally fail-closed. It first runs the benchmark's own
campaign validator, then independently binds the report and cgroup evidence to
the fixed profile and exact five-run contract. It never falls back to an older
campaign, a partial source corpus, or a different arm.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import statistics
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from PIL import Image


MIB = 1024 * 1024
TASK_COUNTS = (1000, 5000, 10000, 50000)
ARM_NAMES = tuple(f"isolated-r{index}" for index in range(1, 6))
PHASE_LABELS = (
    "Idle Baseline",
    "Load Session",
    "Idle",
    "Count Tasks",
    "Idle",
    "Query Returns 10k Tasks",
    "Tail",
)
EXPECTED_PHASES = (
    "startup-baseline",
    "cold-request",
    "post-cold-quiet",
    "count-request",
    "post-count-quiet",
    "detail-request",
    "post-detail-quiet",
    "arm-end",
)
CHECKPOINT_LABELS = (
    "before-cold-request",
    "after-cold-response",
    "before-count-request",
    "after-count-response",
    "before-detail-request",
    "after-detail-response",
)

FIG_BG = "#11151b"
AX_BG = "#171d25"
TEXT = "#f4f4f5"
MUTED = "#a8adb5"
GRID = "#323842"
BLUE = "#66b5f5"
ORANGE = "#ff9954"
GREEN = "#63d894"
GOLD = "#e7bd42"
PHASE_COLORS = (
    "#3b3d42",
    "#17354a",
    "#292d34",
    "#30283d",
    "#292d34",
    "#452b1c",
    "#1d392a",
)


class RenderError(RuntimeError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RenderError(message)


def read_json(path: Path, label: str) -> dict[str, Any]:
    require(path.is_file(), f"missing {label}: {path}")
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise RenderError(f"could not read {label} {path}: {exc}") from exc
    require(isinstance(value, dict), f"{label} is not a JSON object: {path}")
    return value


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def png_dimensions(path: Path) -> dict[str, int]:
    with Image.open(path) as image:
        return {"width": image.size[0], "height": image.size[1]}


def set_style() -> None:
    sns.set_theme(context="talk", style="darkgrid")
    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "figure.facecolor": FIG_BG,
            "axes.facecolor": AX_BG,
            "axes.edgecolor": "#4d5561",
            "axes.labelcolor": TEXT,
            "axes.titlecolor": TEXT,
            "xtick.color": MUTED,
            "ytick.color": MUTED,
            "text.color": TEXT,
            "grid.color": GRID,
            "grid.alpha": 0.55,
            "legend.facecolor": AX_BG,
            "legend.edgecolor": "none",
            "savefig.facecolor": FIG_BG,
        }
    )


@dataclass(frozen=True)
class Sample:
    time_nano: int
    container: str
    current_bytes: int
    peak_bytes: int
    cpu_usage_usec: int
    nr_throttled: int
    throttled_usec: int
    nr_periods: int


@dataclass(frozen=True)
class Arm:
    task_count: int
    name: str
    root: Path
    run_dir: Path
    report_path: Path
    cgroup_path: Path
    report: dict[str, Any]
    samples: tuple[Sample, ...]
    cold_wall_seconds: float
    lifetime_peak_bytes: int


@dataclass(frozen=True)
class Campaign:
    task_count: int
    root: Path
    matrix: dict[str, Any]
    arms: tuple[Arm, ...]


def parse_manifest(path: Path) -> dict[str, Any]:
    manifest = read_json(path, "renderer manifest")
    require(manifest.get("schemaVersion") == 1, "renderer manifest schemaVersion must be 1")
    campaigns = manifest.get("campaigns")
    require(isinstance(campaigns, dict), "renderer manifest campaigns must be an object")
    require(set(campaigns) == {str(item) for item in TASK_COUNTS}, "renderer manifest task keys differ")
    campaign_paths = []
    for task_count in TASK_COUNTS:
        raw = campaigns[str(task_count)]
        require(isinstance(raw, str) and raw.startswith("/"), f"campaign {task_count} path must be absolute")
        campaign_paths.append(raw)
    require(len(set(campaign_paths)) == len(campaign_paths), "renderer manifest reuses a campaign root")
    validator = manifest.get("validator")
    require(isinstance(validator, str) and validator.startswith("/"), "validator path must be absolute")
    profile = manifest.get("profile")
    require(isinstance(profile, dict), "renderer manifest profile must be an object")
    expected_profile = {
        "cpuRequest": "1",
        "cpuLimit": "2",
        "memoryRequest": "1Gi",
        "memoryLimit": "12Gi",
        "protocol": "isolated-request-v1",
        "queryConcurrency": 1,
        "environment": "GOMAXPROCS=2,GODEBUG=gctrace=1",
        "repeats": 5,
        "lifecycleTaskCount": 50000,
        "lifecycleArm": "isolated-r1",
    }
    require(profile == expected_profile, "renderer manifest fixed profile differs")
    return manifest


def run_campaign_validator(validator: Path, root: Path) -> None:
    require(validator.is_file(), f"missing benchmark validator: {validator}")
    require(root.is_dir(), f"missing campaign root: {root}")
    result = subprocess.run(
        [sys.executable, str(validator), str(root), "--expected-kind", "hs-isolated-request"],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise RenderError(f"campaign validator rejected {root}: {detail}")
    require(result.stdout.strip() == "HS-SWEEP-VALID", f"unexpected validator output for {root}")


def parse_cgroup(path: Path, expected_container: str, expected_peak: int) -> tuple[Sample, ...]:
    require(path.is_file(), f"missing cgroup CSV: {path}")
    samples: list[Sample] = []
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        required = {
            "time_nano",
            "container",
            "anon_bytes",
            "current_bytes",
            "peak_bytes",
            "cpu_usage_usec",
            "nr_throttled",
            "throttled_usec",
            "nr_periods",
        }
        require(reader.fieldnames is not None and set(reader.fieldnames) == required, f"cgroup CSV schema differs: {path}")
        for index, row in enumerate(reader, start=2):
            try:
                sample = Sample(
                    time_nano=int(row["time_nano"]),
                    container=row["container"],
                    current_bytes=int(row["current_bytes"]),
                    peak_bytes=int(row["peak_bytes"]),
                    cpu_usage_usec=int(row["cpu_usage_usec"]),
                    nr_throttled=int(row["nr_throttled"]),
                    throttled_usec=int(row["throttled_usec"]),
                    nr_periods=int(row["nr_periods"]),
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise RenderError(f"invalid cgroup row {index} in {path}: {exc}") from exc
            samples.append(sample)
    require(len(samples) >= 3, f"too few cgroup samples: {path}")
    require({sample.container for sample in samples} == {expected_container}, f"cgroup container differs: {path}")
    monotonic_fields = ("time_nano", "peak_bytes", "cpu_usage_usec", "nr_throttled", "throttled_usec", "nr_periods")
    for previous, current in zip(samples, samples[1:]):
        for field in monotonic_fields:
            left = getattr(previous, field)
            right = getattr(current, field)
            require(right > left if field == "time_nano" else right >= left, f"cgroup {field} reversed: {path}")
    require(max(sample.peak_bytes for sample in samples) == expected_peak, f"cgroup lifetime peak differs from report: {path}")
    return tuple(samples)


def find_run_files(arm_root: Path) -> tuple[Path, Path, Path]:
    reports = sorted(arm_root.glob("*/bench-report.json"))
    require(len(reports) == 1, f"expected exactly one bench-report.json under {arm_root}, found {len(reports)}")
    run_dir = reports[0].parent
    cgroup = run_dir / "cgroup_samples.csv"
    require(cgroup.is_file(), f"missing cgroup CSV beside report: {cgroup}")
    return run_dir, reports[0], cgroup


def phase_map(report: dict[str, Any]) -> dict[str, int]:
    phases = report.get("historyServerPhases")
    require(isinstance(phases, list), "historyServerPhases missing")
    require(tuple(item.get("phase") for item in phases) == EXPECTED_PHASES, "History Server phase order differs")
    values = {item["phase"]: item.get("timeNano") for item in phases}
    require(all(type(value) is int and value > 0 for value in values.values()), "History Server phase timestamp invalid")
    return values


def checkpoint_map(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    checkpoints = report.get("hsCPUCheckpoints")
    require(isinstance(checkpoints, list), "hsCPUCheckpoints missing")
    require(tuple(item.get("label") for item in checkpoints) == CHECKPOINT_LABELS, "History Server checkpoint order differs")
    return {item["label"]: item for item in checkpoints}


def load_arm(task_count: int, campaign_root: Path, arm_name: str, expected_config: dict[str, Any]) -> Arm:
    arm_root = campaign_root / arm_name
    require(arm_root.is_dir(), f"missing arm directory: {arm_root}")
    run_dir, report_path, cgroup_path = find_run_files(arm_root)
    report = read_json(report_path, f"{task_count} {arm_name} report")
    require(report.get("completed") is True, f"{task_count} {arm_name} report is not completed")
    config = report.get("config")
    require(isinstance(config, dict), f"{task_count} {arm_name} config missing")
    required_config = {
        "TaskCount": task_count,
        "HSCPURequest": "1",
        "HSCPULimit": "2",
        "HSMemoryRequest": "1Gi",
        "HSMemoryLimit": "12Gi",
        "HSProtocol": "isolated-request-v1",
        "HSQueryConcurrency": 1,
        "HSEnv": "GOMAXPROCS=2,GODEBUG=gctrace=1",
    }
    for key, value in required_config.items():
        require(config.get(key) == value, f"{task_count} {arm_name} config {key} differs")
    for key, value in expected_config.items():
        require(config.get(key) == value, f"{task_count} {arm_name} matrix/report {key} differs")
    pod = report.get("hsPodEvidence")
    require(isinstance(pod, dict) and pod.get("valid") is True and pod.get("problems") == [], f"{task_count} {arm_name} pod evidence invalid")
    require((pod.get("cpuRequest"), pod.get("cpuLimit")) == ("1", "2"), f"{task_count} {arm_name} pod CPU differs")
    require((pod.get("memoryRequest"), pod.get("memoryLimit")) == ("1Gi", "12Gi"), f"{task_count} {arm_name} pod memory differs")
    require(pod.get("restartCount") == 0 and pod.get("oomKilled") is False, f"{task_count} {arm_name} restarted or OOM-killed")
    validation = report.get("hsValidation")
    require(isinstance(validation, dict), f"{task_count} {arm_name} hsValidation missing")
    require(validation.get("measurementValid") is True and validation.get("valid") is True and validation.get("problems") == [], f"{task_count} {arm_name} validation invalid")
    require(validation.get("expectedTaskAttempts") == task_count, f"{task_count} {arm_name} attempt count differs")
    isolation = report.get("hsRequestIsolation")
    require(isinstance(isolation, dict) and isolation.get("protocol") == "isolated-request-v1", f"{task_count} {arm_name} isolation protocol differs")
    require(isolation.get("valid") is True and isolation.get("problems") == [], f"{task_count} {arm_name} isolation invalid")
    requests = isolation.get("requests")
    require(isinstance(requests, list) and [item.get("name") for item in requests] == ["cold", "count", "detail"], f"{task_count} {arm_name} request order differs")
    phase_map(report)
    checkpoint_map(report)
    history_server = report.get("historyServer")
    require(isinstance(history_server, dict), f"{task_count} {arm_name} historyServer missing")
    cold_nano = history_server.get("enterColdLatency")
    require(type(cold_nano) is int and cold_nano > 0, f"{task_count} {arm_name} cold latency invalid")
    peak_bytes = validation.get("lifetimeMemoryPeakBytes")
    require(type(peak_bytes) is int and peak_bytes > 0, f"{task_count} {arm_name} lifetime peak invalid")
    expected_container = f"{pod.get('podName')}/{pod.get('containerName')}"
    samples = parse_cgroup(cgroup_path, expected_container, peak_bytes)
    return Arm(
        task_count=task_count,
        name=arm_name,
        root=arm_root,
        run_dir=run_dir,
        report_path=report_path,
        cgroup_path=cgroup_path,
        report=report,
        samples=samples,
        cold_wall_seconds=cold_nano / 1_000_000_000,
        lifetime_peak_bytes=peak_bytes,
    )


def load_campaign(task_count: int, root: Path, validator: Path) -> Campaign:
    run_campaign_validator(validator, root)
    matrix = read_json(root / "expected-matrix.json", f"{task_count} expected matrix")
    require(matrix.get("kind") == "hs-isolated-request", f"{task_count} matrix kind differs")
    arms = matrix.get("arms")
    require(isinstance(arms, list) and [item.get("name") for item in arms] == list(ARM_NAMES), f"{task_count} matrix arm order differs")
    matrix_source = matrix.get("source")
    require(isinstance(matrix_source, dict), f"{task_count} matrix source missing")
    require(matrix_source.get("expectedBenchmarkAttempts") == task_count, f"{task_count} matrix source attempts differ")
    loaded: list[Arm] = []
    for arm_doc in arms:
        config = arm_doc.get("config")
        require(isinstance(config, dict), f"{task_count} matrix arm config missing")
        loaded.append(load_arm(task_count, root, arm_doc["name"], config))
    require(len({arm.report["executionNamespaceUID"] for arm in loaded}) == 5, f"{task_count} reused execution namespace UID")
    require(len({arm.report["hsPodEvidence"]["podUID"] for arm in loaded}) == 5, f"{task_count} reused pod UID")
    require(len({arm.report["hsPodEvidence"]["containerID"] for arm in loaded}) == 5, f"{task_count} reused container ID")
    return Campaign(task_count=task_count, root=root, matrix=matrix, arms=tuple(loaded))


def figure_header(fig: plt.Figure, title: str, subtitle: str) -> None:
    fig.text(0.06, 0.92, title, fontsize=24, weight="bold", color=TEXT, ha="left", va="top")
    fig.text(0.06, 0.855, subtitle, fontsize=12.5, color=MUTED, ha="left", va="top")


def style_axis(ax: plt.Axes) -> None:
    ax.set_facecolor(AX_BG)
    ax.grid(True, linewidth=0.8, color=GRID, alpha=0.55)
    ax.set_axisbelow(True)
    for spine in ax.spines.values():
        spine.set_color("#4d5561")
        spine.set_linewidth(1.1)


def format_seconds(value: float) -> str:
    if value < 1:
        return f"{value:.2f} s"
    if value < 10:
        return f"{value:.2f} s"
    return f"{value:.1f} s"


def format_mib(value: float) -> str:
    if value >= 1000:
        return f"{value:,.0f} MiB"
    return f"{value:.1f} MiB" if value < 100 else f"{value:.0f} MiB"


def save_exact(fig: plt.Figure, path: Path, expected_size: tuple[int, int]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=100, facecolor=FIG_BG)
    plt.close(fig)
    with Image.open(path) as image:
        require(image.size == expected_size, f"render size {image.size} differs for {path}")


def render_cold_scaling(campaigns: Sequence[Campaign], out: Path) -> list[float]:
    medians = [statistics.median(arm.cold_wall_seconds for arm in campaign.arms) for campaign in campaigns]
    labels = ["1k", "5k", "10k", "50k"]
    x = np.arange(len(labels))
    fig = plt.figure(figsize=(12, 9), dpi=100, facecolor=FIG_BG)
    figure_header(fig, "Cold-load wall time vs tasks per session", "Request: 1 CPU · 1 GiB memory  |  Limit: 2 CPUs · 12 GiB memory")
    ax = fig.add_axes([0.14, 0.15, 0.80, 0.61])
    style_axis(ax)
    sns.lineplot(x=x, y=medians, color=BLUE, linewidth=3.2, marker="o", markersize=10, ax=ax)
    ax.set_xticks(x, labels)
    ax.set_xlabel("Tasks per session", fontsize=15, labelpad=12)
    ax.set_ylabel("Cold-load wall time (seconds)", fontsize=15, labelpad=12)
    top = max(medians) * 1.23
    ax.set_ylim(0, max(top, 1.0))
    ax.set_xlim(-0.25, len(x) - 0.65)
    for xpos, value in zip(x, medians):
        ax.annotate(
            format_seconds(value),
            (xpos, value),
            xytext=(0, 12),
            textcoords="offset points",
            ha="center",
            color=TEXT,
            fontsize=13,
            weight="bold",
        )
    save_exact(fig, out, (1200, 900))
    return medians


def render_memory_scaling(campaigns: Sequence[Campaign], out: Path) -> list[float]:
    medians = [statistics.median(arm.lifetime_peak_bytes for arm in campaign.arms) / MIB for campaign in campaigns]
    labels = ["1k", "5k", "10k", "50k"]
    fig = plt.figure(figsize=(12, 9), dpi=100, facecolor=FIG_BG)
    figure_header(fig, "Whole-container memory peak vs tasks per session", "Request: 1 CPU · 1 GiB memory  |  Limit: 2 CPUs · 12 GiB memory")
    ax = fig.add_axes([0.14, 0.15, 0.80, 0.61])
    style_axis(ax)
    sns.barplot(x=labels, y=medians, color=BLUE, edgecolor="#a9d7f8", linewidth=1.4, errorbar=None, ax=ax)
    ax.set_xlabel("Tasks per session", fontsize=15, labelpad=12)
    ax.set_ylabel("Whole-container lifetime peak (MiB)", fontsize=15, labelpad=12)
    ax.set_ylim(0, max(medians) * 1.23)
    for bar, value in zip(ax.patches, medians):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            value + max(medians) * 0.03,
            format_mib(value),
            ha="center",
            va="bottom",
            fontsize=13,
            weight="bold",
            color=TEXT,
        )
    save_exact(fig, out, (1200, 900))
    return medians


def lifecycle_boundaries(arm: Arm) -> tuple[list[float], int]:
    phases = phase_map(arm.report)
    checkpoints = checkpoint_map(arm.report)
    start = phases["startup-baseline"]
    absolute = [
        start,
        checkpoints["before-cold-request"]["timeNano"],
        checkpoints["after-cold-response"]["timeNano"],
        checkpoints["before-count-request"]["timeNano"],
        checkpoints["after-count-response"]["timeNano"],
        checkpoints["before-detail-request"]["timeNano"],
        checkpoints["after-detail-response"]["timeNano"],
        phases["arm-end"],
    ]
    require(all(type(item) is int for item in absolute), "lifecycle boundary timestamp invalid")
    require(all(right > left for left, right in zip(absolute, absolute[1:])), "lifecycle boundaries are not strictly increasing")
    first_sample = arm.samples[0].time_nano
    last_sample = arm.samples[-1].time_nano
    require(first_sample - start <= 1_000_000_000, "cgroup series begins too late for baseline")
    require(last_sample >= absolute[-1] - 1_000_000_000, "cgroup series ends too early for tail")
    return [(item - start) / 1_000_000_000 for item in absolute], start


def phase_bands(ax: plt.Axes, boundaries: Sequence[float]) -> None:
    require(len(boundaries) == 8, "phase boundary count differs")
    for index, (left, right, label, color) in enumerate(zip(boundaries, boundaries[1:], PHASE_LABELS, PHASE_COLORS)):
        ax.axvspan(left, right, color=color, alpha=0.44 if index else 0.58, zorder=0)
        ax.axvline(right, color="#68707c", linestyle=(0, (2, 3)), linewidth=1.0, alpha=0.85, zorder=1)
        width = right - left
        display = label
        if label == "Count Tasks" and width < (boundaries[-1] - boundaries[0]) * 0.09:
            display = "Count\nTasks"
        elif label == "Query Returns 10k Tasks" and width < (boundaries[-1] - boundaries[0]) * 0.13:
            display = "Query Returns\n10k Tasks"
        ax.text(
            (left + right) / 2,
            0.985,
            display,
            transform=ax.get_xaxis_transform(),
            ha="center",
            va="top",
            fontsize=9.5 if "\n" in display else 10.5,
            weight="bold",
            color="#e6e7ea",
            linespacing=0.9,
            zorder=5,
        )


def lifecycle_arrays(arm: Arm, origin: int, end_seconds: float) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    selected = [sample for sample in arm.samples if origin <= sample.time_nano <= origin + int(end_seconds * 1_000_000_000) + 1_000_000_000]
    require(len(selected) >= 3, "too few lifecycle samples inside phase window")
    memory_x = np.array([(item.time_nano - origin) / 1_000_000_000 for item in selected])
    current_mib = np.array([item.current_bytes / MIB for item in selected])
    peak_mib = np.array([item.peak_bytes / MIB for item in selected])
    cpu_x = []
    cpu_m = []
    for previous, current in zip(selected, selected[1:]):
        delta_nano = current.time_nano - previous.time_nano
        delta_cpu = current.cpu_usage_usec - previous.cpu_usage_usec
        require(delta_nano > 0 and delta_cpu >= 0, "invalid cgroup CPU interval")
        cpu_x.append(((previous.time_nano + current.time_nano) / 2 - origin) / 1_000_000_000)
        cpu_m.append(delta_cpu * 1_000_000 / delta_nano)
    return np.array(cpu_x), np.array(cpu_m), memory_x, np.column_stack((current_mib, peak_mib))


def render_cpu_lifecycle(arm: Arm, out: Path) -> dict[str, float]:
    boundaries, origin = lifecycle_boundaries(arm)
    cpu_x, cpu_m, _, _ = lifecycle_arrays(arm, origin, boundaries[-1])
    fig = plt.figure(figsize=(24, 5.2), dpi=100, facecolor=FIG_BG)
    figure_header(fig, "History Server CPU lifecycle — 50k tasks", "Request 1 CPU / 1 GiB · limit 2 CPUs / 12 GiB")
    ax = fig.add_axes([0.065, 0.18, 0.91, 0.56])
    style_axis(ax)
    phase_bands(ax, boundaries)
    sns.lineplot(x=cpu_x, y=cpu_m, color=BLUE, linewidth=3.0, ax=ax, zorder=4)
    ax.axhline(1000, color=GOLD, linewidth=1.8, linestyle=(0, (7, 6)), zorder=2)
    ax.axhline(2000, color=GOLD, linewidth=1.8, linestyle=(0, (7, 6)), zorder=2)
    y_top = max(2200.0, float(np.max(cpu_m)) * 1.12)
    ax.set_ylim(0, y_top)
    ax.set_xlim(0, boundaries[-1])
    label_x = boundaries[-1] - max(boundaries[-1] * 0.012, 0.15)
    ax.text(label_x, 1000, "1 CPU request", color=GOLD, fontsize=11.5, ha="right", va="bottom", weight="bold", bbox={"facecolor": AX_BG, "edgecolor": "none", "alpha": 0.78, "pad": 1.5})
    ax.text(label_x, 2000, "2 CPU limit", color=GOLD, fontsize=11.5, ha="right", va="bottom", weight="bold", bbox={"facecolor": AX_BG, "edgecolor": "none", "alpha": 0.78, "pad": 1.5})
    ax.set_xlabel("Seconds after idle baseline began", fontsize=13, labelpad=8)
    ax.set_ylabel("CPU usage (millicores)", fontsize=13, labelpad=10)
    windows = {item["name"]: item for item in arm.report["hsRequestIsolation"]["windows"]}
    cpu_seconds = {name: windows[name]["cpuUsageUsec"] / 1_000_000 for name in ("cold", "count", "detail")}
    save_exact(fig, out, (2400, 520))
    return cpu_seconds


def render_memory_lifecycle(arm: Arm, out: Path) -> dict[str, float]:
    boundaries, origin = lifecycle_boundaries(arm)
    _, _, memory_x, memory_values = lifecycle_arrays(arm, origin, boundaries[-1])
    current_mib = memory_values[:, 0]
    peak_mib = memory_values[:, 1]
    fig = plt.figure(figsize=(24, 5.2), dpi=100, facecolor=FIG_BG)
    figure_header(fig, "History Server memory lifecycle — 50k tasks", "Request 1 GiB · 12 GiB limit was measurement headroom, not a recommendation")
    ax = fig.add_axes([0.065, 0.18, 0.91, 0.56])
    style_axis(ax)
    phase_bands(ax, boundaries)
    sns.lineplot(x=memory_x, y=current_mib, color=ORANGE, linewidth=3.0, label="memory.current", ax=ax, zorder=4)
    sns.lineplot(x=memory_x, y=peak_mib, color=GREEN, linewidth=2.8, label="cumulative memory.peak", ax=ax, zorder=4)
    ax.axhline(1024, color=GOLD, linewidth=1.8, linestyle=(0, (7, 6)), zorder=2)
    y_top = max(1150.0, float(np.max(peak_mib)) * 1.18)
    ax.set_ylim(0, y_top)
    ax.set_xlim(0, boundaries[-1])
    label_x = boundaries[-1] - max(boundaries[-1] * 0.012, 0.15)
    ax.text(label_x, 1024, "1 GiB request", color=GOLD, fontsize=11.5, ha="right", va="bottom", weight="bold", bbox={"facecolor": AX_BG, "edgecolor": "none", "alpha": 0.78, "pad": 1.5})
    ax.set_xlabel("Seconds after idle baseline began", fontsize=13, labelpad=8)
    ax.set_ylabel("Whole-container memory (MiB)", fontsize=13, labelpad=10)
    ax.legend(loc="upper left", bbox_to_anchor=(0.01, 0.84), frameon=False, ncol=2, fontsize=11.5)
    peak = float(np.max(peak_mib))
    last = float(current_mib[-1])
    ax.text(0.985, 0.80, f"Lifetime peak: {format_mib(peak)}\nLast sample: {format_mib(last)}", transform=ax.transAxes, ha="right", va="top", fontsize=11.5, weight="bold", color=TEXT, bbox={"facecolor": "#12161c", "edgecolor": "#3c424c", "alpha": 0.90, "pad": 5})
    save_exact(fig, out, (2400, 520))
    return {"lifetimePeakMiB": peak, "lastCurrentMiB": last}


def make_contact_sheet(paths: Sequence[Path], out: Path) -> None:
    require(len(paths) == 4, "contact sheet requires four charts")
    opened = [Image.open(path).convert("RGB") for path in paths]
    try:
        require(opened[0].size == (1200, 900) and opened[1].size == (1200, 900), "scaling chart dimensions differ")
        require(opened[2].size == (2400, 520) and opened[3].size == (2400, 520), "lifecycle chart dimensions differ")
        canvas = Image.new("RGB", (2400, 1940), FIG_BG)
        canvas.paste(opened[0], (0, 0))
        canvas.paste(opened[1], (1200, 0))
        canvas.paste(opened[2], (0, 900))
        canvas.paste(opened[3], (0, 1420))
        canvas.save(out)
    finally:
        for image in opened:
            image.close()


def image_batches(output_dir: Path) -> list[dict[str, Any]]:
    presentation_id = "1MixELf2mG9scH4OgJNY6GSKVEXftbcEtFNgbEn9DsaM"
    mapping = (
        ("hs_cpu_scaling_img_v2", output_dir / "slide17-cold-load-scaling.png"),
        ("hs_memory_scaling_img_v2", output_dir / "slide17-memory-scaling.png"),
        ("hs_cpu_lifecycle_img", output_dir / "slide18-cpu-lifecycle.png"),
        ("hs_memory_lifecycle_img", output_dir / "slide18-memory-lifecycle.png"),
    )
    return [
        {
            "presentation_id": presentation_id,
            "image_uris": str(path.resolve()),
            "write_control": {"requiredRevisionId": "__FRESH_REVISION_ID__"},
            "requests": [
                {
                    "replaceImage": {
                        "imageObjectId": object_id,
                        "url": str(path.resolve()),
                        "imageReplaceMethod": "CENTER_INSIDE",
                    }
                }
            ],
        }
        for object_id, path in mapping
    ]


def self_test(output_dir: Path) -> None:
    """Exercise Seaborn/Pillow and exact-size output without formal data."""
    with tempfile.TemporaryDirectory(prefix="hs-chart-self-test-", dir="/private/tmp") as temporary:
        root = Path(temporary)
        # The actual data path is separately fail-closed; this checks rendering
        # and exact pixel contracts only.
        labels = ["1k", "5k", "10k", "50k"]
        x = np.arange(4)
        fig = plt.figure(figsize=(12, 9), dpi=100, facecolor=FIG_BG)
        figure_header(fig, "Self-test", "Synthetic values — never used in final charts")
        ax = fig.add_axes([0.14, 0.15, 0.80, 0.61])
        style_axis(ax)
        sns.lineplot(x=x, y=[0.2, 0.8, 1.5, 7.0], color=BLUE, marker="o", ax=ax)
        ax.set_xticks(x, labels)
        save_exact(fig, root / "scale-a.png", (1200, 900))
        fig = plt.figure(figsize=(12, 9), dpi=100, facecolor=FIG_BG)
        figure_header(fig, "Self-test", "Synthetic values — never used in final charts")
        ax = fig.add_axes([0.14, 0.15, 0.80, 0.61])
        style_axis(ax)
        sns.barplot(x=labels, y=[40, 120, 240, 1200], color=BLUE, errorbar=None, ax=ax)
        save_exact(fig, root / "scale-b.png", (1200, 900))
        for name in ("life-a.png", "life-b.png"):
            fig = plt.figure(figsize=(24, 5.2), dpi=100, facecolor=FIG_BG)
            figure_header(fig, "Self-test", "Synthetic values — never used in final charts")
            ax = fig.add_axes([0.065, 0.18, 0.91, 0.56])
            style_axis(ax)
            boundaries = [0, 10, 15, 23, 24, 32, 34, 42]
            phase_bands(ax, boundaries)
            sns.lineplot(x=np.linspace(0, 42, 80), y=np.sin(np.linspace(0, 8, 80)) + 2, color=BLUE, ax=ax)
            save_exact(fig, root / name, (2400, 520))
        make_contact_sheet([root / "scale-a.png", root / "scale-b.png", root / "life-a.png", root / "life-b.png"], root / "contact.png")
        with Image.open(root / "contact.png") as image:
            require(image.size == (2400, 1940), "self-test contact sheet dimensions differ")
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "renderer-ready.json").write_text(
        json.dumps(
            {
                "status": "dry-schema-valid",
                "finalDataUsed": False,
                "syntheticArtifactsPersisted": False,
                "expectedOutputs": [
                    "slide17-cold-load-scaling.png",
                    "slide17-memory-scaling.png",
                    "slide18-cpu-lifecycle.png",
                    "slide18-memory-lifecycle.png",
                    "qa-contact-sheet.png",
                    "chart-data.json",
                    "chart-output-manifest.json",
                    "slides-image-batches.json",
                ],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )


def render_all(manifest_path: Path, output_dir: Path) -> None:
    manifest = parse_manifest(manifest_path)
    validator = Path(manifest["validator"])
    campaigns = [
        load_campaign(task_count, Path(manifest["campaigns"][str(task_count)]), validator)
        for task_count in TASK_COUNTS
    ]
    output_dir.mkdir(parents=True, exist_ok=True)
    cold_path = output_dir / "slide17-cold-load-scaling.png"
    memory_path = output_dir / "slide17-memory-scaling.png"
    cpu_lifecycle_path = output_dir / "slide18-cpu-lifecycle.png"
    memory_lifecycle_path = output_dir / "slide18-memory-lifecycle.png"
    cold_medians = render_cold_scaling(campaigns, cold_path)
    memory_medians = render_memory_scaling(campaigns, memory_path)
    lifecycle_campaign = next(item for item in campaigns if item.task_count == 50000)
    lifecycle_arm = next((item for item in lifecycle_campaign.arms if item.name == "isolated-r1"), None)
    require(lifecycle_arm is not None, "missing predeclared 50k isolated-r1 lifecycle arm")
    cpu_windows = render_cpu_lifecycle(lifecycle_arm, cpu_lifecycle_path)
    memory_summary = render_memory_lifecycle(lifecycle_arm, memory_lifecycle_path)
    contact_path = output_dir / "qa-contact-sheet.png"
    charts = [cold_path, memory_path, cpu_lifecycle_path, memory_lifecycle_path]
    make_contact_sheet(charts, contact_path)
    boundaries, origin = lifecycle_boundaries(lifecycle_arm)
    data = {
        "schemaVersion": 1,
        "profile": manifest["profile"],
        "scaling": [
            {
                "taskCount": campaign.task_count,
                "coldWallSeconds": [arm.cold_wall_seconds for arm in campaign.arms],
                "medianColdWallSeconds": cold_medians[index],
                "lifetimeMemoryPeakBytes": [arm.lifetime_peak_bytes for arm in campaign.arms],
                "medianLifetimeMemoryPeakBytes": int(statistics.median(arm.lifetime_peak_bytes for arm in campaign.arms)),
                "medianLifetimeMemoryPeakMiB": memory_medians[index],
                "campaignRoot": str(campaign.root),
                "reportPaths": [str(arm.report_path) for arm in campaign.arms],
            }
            for index, campaign in enumerate(campaigns)
        ],
        "lifecycle": {
            "taskCount": 50000,
            "arm": "isolated-r1",
            "reportPath": str(lifecycle_arm.report_path),
            "cgroupPath": str(lifecycle_arm.cgroup_path),
            "originTimeNano": origin,
            "phaseLabels": list(PHASE_LABELS),
            "phaseBoundariesSeconds": boundaries,
            "requestCPUSeconds": cpu_windows,
            **memory_summary,
        },
    }
    (output_dir / "chart-data.json").write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
    outputs = [*charts, contact_path, output_dir / "chart-data.json"]
    output_manifest = {
        "schemaVersion": 1,
        "status": "formal-data-rendered",
        "files": [
            {
                "path": str(path.resolve()),
                "sha256": sha256(path),
                **(png_dimensions(path) if path.suffix.lower() == ".png" else {}),
            }
            for path in outputs
        ],
    }
    (output_dir / "chart-output-manifest.json").write_text(json.dumps(output_manifest, indent=2, sort_keys=True) + "\n")
    (output_dir / "slides-image-batches.json").write_text(json.dumps(image_batches(output_dir), indent=2, sort_keys=True) + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--dry-schema-check", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        set_style()
        parse_manifest(args.manifest)
        if args.dry_schema_check:
            self_test(args.output_dir)
            print("HS-CHART-RENDERER-DRY-SCHEMA-VALID")
            return 0
        render_all(args.manifest, args.output_dir)
        print(f"HS-CHARTS-RENDERED out={args.output_dir.resolve()}")
        return 0
    except RenderError as exc:
        print(f"HS-CHARTS-BLOCKED: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
