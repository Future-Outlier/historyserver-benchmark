#!/usr/bin/env python3
"""Render four final Collector charts from formal benchmark artifacts only."""

from __future__ import annotations

import csv
import json
import math
import os
import statistics
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, MultipleLocator
import seaborn as sns


OUT = Path(os.environ.get("RAY_SUMMIT_CHART_OUT", "/private/tmp/ray-summit-final/assets"))
RATE_ROOT = Path(os.environ.get("RAY_SUMMIT_RATE_ROOT", "/path/to/rate-campaign"))
CAP_RUN = Path(os.environ.get("RAY_SUMMIT_CAP_RUN", "/path/to/cap-campaign/representative-run"))

BG = "#171717"
PANEL = "#1B1B1B"
FG = "#F3F3F3"
MUTED = "#B8B8B8"
GRID = "#FFFFFF"
BLUE = "#6CB6FF"
ORANGE = "#FF9D57"
GREEN = "#63D69A"
PHASE_COLORS = ("#FFFFFF", BLUE, "#9A7BEF", GREEN)

TARGETS = (250, 500, 1000, 2000, 3000)
REPEATS = (1, 2, 3)
ROLES = ("Head", "Worker")


def configure_theme() -> None:
    sns.set_theme(
        context="talk",
        style="darkgrid",
        font="DejaVu Sans",
        rc={
            "figure.facecolor": BG,
            "axes.facecolor": PANEL,
            "axes.edgecolor": "#4A4A4A",
            "axes.labelcolor": FG,
            "axes.titlecolor": FG,
            "xtick.color": MUTED,
            "ytick.color": MUTED,
            "text.color": FG,
            "grid.color": GRID,
            "grid.alpha": 0.09,
            "grid.linewidth": 0.8,
            "legend.facecolor": PANEL,
            "legend.edgecolor": "none",
            "savefig.facecolor": BG,
            "savefig.edgecolor": BG,
        },
    )


def one(path: Path, pattern: str) -> Path:
    matches = sorted(path.glob(pattern))
    if len(matches) != 1:
        raise RuntimeError(f"expected one {pattern} under {path}, found {len(matches)}")
    return matches[0]


def role_for_pod(pod: str) -> str:
    if "-head-" in pod:
        return "Head"
    if "-worker-" in pod:
        return "Worker"
    raise RuntimeError(f"unknown Collector pod role: {pod}")


def nearest_rank_p95(values: list[int]) -> float:
    ordered = sorted(values)
    return ordered[math.ceil(0.95 * len(ordered)) - 1] / 1024**2


def load_scaling() -> dict[str, list[dict[str, float]]]:
    observations: list[dict[str, float | int | str]] = []
    for target in TARGETS:
        for repeat in REPEATS:
            arm = RATE_ROOT / f"rate{target}-r{repeat}"
            report_path = one(arm, "*/bench-report.json")
            report = json.loads(report_path.read_text(encoding="utf-8"))
            if report.get("completed") is not True:
                raise RuntimeError(f"incomplete formal report: {report_path}")
            if int(report["config"]["TargetTaskRate"]) != target:
                raise RuntimeError(f"target mismatch in {report_path}")

            selected: dict[str, dict] = {}
            for window in report["collectorWindows"]:
                if window.get("validForSizing") is not True:
                    continue
                role = role_for_pod(window["pod"])
                prior = selected.get(role)
                if prior is None or float(window["eventsPerSecond"]) > float(prior["eventsPerSecond"]):
                    selected[role] = window
            if set(selected) != set(ROLES):
                raise RuntimeError(f"missing valid Collector windows in {report_path}")

            current: dict[str, list[int]] = defaultdict(list)
            with (report_path.parent / "cgroup_samples.csv").open(newline="", encoding="utf-8") as handle:
                for row in csv.DictReader(handle):
                    if not row["container"].endswith("/collector"):
                        continue
                    role = role_for_pod(row["container"].split("/", 1)[0])
                    current[role].append(int(row["current_bytes"]))

            for role in ROLES:
                observations.append(
                    {
                        "target": target,
                        "repeat": repeat,
                        "role": role,
                        "events": float(selected[role]["eventsPerSecond"]),
                        "cpu_m": float(selected[role]["avgCores"]) * 1000.0,
                        "memory_mib": nearest_rank_p95(current[role]),
                    }
                )

    grouped: dict[str, list[dict[str, float]]] = {role: [] for role in ROLES}
    for role in ROLES:
        for target in TARGETS:
            rows = [row for row in observations if row["role"] == role and row["target"] == target]
            if len(rows) != len(REPEATS):
                raise RuntimeError(f"incomplete formal repeats for {role} target {target}")
            grouped[role].append(
                {
                    "target": float(target),
                    "events": statistics.fmean(float(row["events"]) for row in rows),
                    "cpu_m": statistics.fmean(float(row["cpu_m"]) for row in rows),
                    "memory_mib": statistics.fmean(float(row["memory_mib"]) for row in rows),
                }
            )
    return grouped


def load_lifecycle() -> dict[str, object]:
    report = json.loads((CAP_RUN / "bench-report.json").read_text(encoding="utf-8"))
    if report.get("completed") is not True:
        raise RuntimeError("cap-validation r2 report is incomplete")
    gates = report.get("collectorIngressGates", [])
    if len(gates) != 2 or not all(gate.get("valid") is True for gate in gates):
        raise RuntimeError("cap-validation r2 Collector ingress gates did not pass")

    raw: dict[str, list[dict[str, int]]] = defaultdict(list)
    with (CAP_RUN / "cgroup_samples.csv").open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if not row["container"].endswith("/collector"):
                continue
            role = role_for_pod(row["container"].split("/", 1)[0])
            raw[role].append(
                {
                    "time_ns": int(row["time_nano"]),
                    "usage_us": int(row["cpu_usage_usec"]),
                    "current_bytes": int(row["current_bytes"]),
                    "anon_bytes": int(row["anon_bytes"]),
                }
            )
    if set(raw) != set(ROLES):
        raise RuntimeError("cap-validation r2 is missing a Collector role")
    for rows in raw.values():
        rows.sort(key=lambda row: row["time_ns"])

    t0 = min(rows[0]["time_ns"] for rows in raw.values())
    series: dict[str, dict[str, list[float]]] = {}
    for role in ROLES:
        rows = raw[role]
        elapsed = [(row["time_ns"] - t0) / 1e9 for row in rows]
        current = [row["current_bytes"] / 1024**2 for row in rows]
        anon = [row["anon_bytes"] / 1024**2 for row in rows]
        cpu_t: list[float] = []
        cpu_m: list[float] = []
        cpu_dt: list[float] = []
        for before, after in zip(rows, rows[1:]):
            dt = (after["time_ns"] - before["time_ns"]) / 1e9
            value = ((after["usage_us"] - before["usage_us"]) / 1e6) / dt * 1000.0
            cpu_t.append((after["time_ns"] - t0) / 1e9)
            cpu_m.append(value)
            cpu_dt.append(dt)
        series[role] = {
            "elapsed": elapsed,
            "current": current,
            "anon": anon,
            "cpu_t": cpu_t,
            "cpu_m": cpu_m,
            "cpu_dt": cpu_dt,
        }

    pod_stop_ns: int | None = None
    for event in report.get("timeline", []):
        if event.get("kind") != "pod" or "/collector" not in event.get("name", ""):
            continue
        if not event.get("event", "").startswith("terminated"):
            continue
        at_ns = int(datetime.fromisoformat(event["at"]).timestamp() * 1e9)
        pod_stop_ns = at_ns if pod_stop_ns is None else min(pod_stop_ns, at_ns)
    if pod_stop_ns is None:
        raise RuntimeError("formal timeline lacks Collector Pod stop evidence")

    pod_stop_s = (pod_stop_ns - t0) / 1e9
    return {
        "series": series,
        "pod_stop_s": pod_stop_s,
        "driver_rate": float(report["job"]["driverRateTPS"]),
    }


def finish_axis(ax: plt.Axes) -> None:
    ax.grid(True, which="major", alpha=0.09)
    ax.grid(False, which="minor")
    sns.despine(ax=ax, top=True, right=True)
    ax.tick_params(labelsize=13)
    ax.xaxis.label.set_size(15)
    ax.yaxis.label.set_size(15)


def save(fig: plt.Figure, filename: str) -> Path:
    path = OUT / filename
    fig.savefig(path, dpi=200, facecolor=BG, edgecolor=BG)
    plt.close(fig)
    return path


def render_scaling_chart(
    grouped: dict[str, list[dict[str, float]]],
    *,
    metric: str,
    title: str,
    subtitle: str,
    ylabel: str,
    ylim: tuple[float, float],
    filename: str,
) -> Path:
    fig, ax = plt.subplots(figsize=(8, 6))
    # Keep title, subtitle, legend, and plot in separate vertical zones.  The
    # generous left margin prevents the long metric label from being clipped
    # after this image is scaled down on the slide.
    fig.subplots_adjust(left=0.19, right=0.95, bottom=0.18, top=0.68)
    fig.suptitle(title, x=0.08, y=0.94, ha="left", fontsize=22, fontweight="bold", color=FG)
    fig.text(
        0.08,
        0.855,
        subtitle,
        ha="left",
        color=MUTED,
        fontsize=13.5,
    )

    handles = []
    for role, color, marker in (("Head", BLUE, "o"), ("Worker", ORANGE, "s")):
        rows = grouped[role]
        line = sns.lineplot(
            x=[row["events"] for row in rows],
            y=[row[metric] for row in rows],
            ax=ax,
            color=color,
            marker=marker,
            linewidth=3.0,
            markersize=8.5,
            errorbar=None,
            sort=False,
            legend=False,
        )
        handles.append(line.lines[-1])

    ax.set_xlabel("Peak ingress (events/s)", labelpad=9)
    ax.set_ylabel(ylabel, labelpad=10)
    ax.set_xlim(250, 5200)
    ax.set_ylim(*ylim)
    ax.xaxis.set_major_locator(MultipleLocator(1000))
    ax.xaxis.set_major_formatter(FuncFormatter(lambda value, _: "0" if value == 0 else f"{value / 1000:g}k"))
    fig.legend(
        handles,
        ["Head Collector", "Worker Collector"],
        loc="upper left",
        bbox_to_anchor=(0.075, 0.80),
        ncol=2,
        frameon=False,
        fontsize=12.5,
        handlelength=2.4,
        columnspacing=1.8,
    )
    finish_axis(ax)
    return save(fig, filename)


def add_phases(ax: plt.Axes, phase_ax: plt.Axes, pod_stop_s: float) -> None:
    phase_bounds = [
        (0.0, 29.0, "Startup"),
        (29.0, 52.0, "Collect Events"),
        (52.0, 88.0, "Wait for TTL"),
        (88.0, pod_stop_s, "Finish & Upload"),
    ]
    for index, (left, right, label) in enumerate(phase_bounds):
        ax.axvspan(left, right, color=PHASE_COLORS[index], alpha=0.055, linewidth=0, zorder=0)
        phase_ax.axvspan(left, right, color=PHASE_COLORS[index], alpha=0.13, linewidth=0)

    for boundary in (29.0, 52.0, 88.0, pod_stop_s):
        phase_ax.axvline(boundary, color="#575757", linewidth=0.8, alpha=0.8)

    for x, label in ((14.5, "Startup"), (40.5, "Collect Events"), (70.0, "Wait for TTL")):
        phase_ax.text(
            x,
            0.5,
            label,
            transform=phase_ax.get_xaxis_transform(),
            ha="center",
            va="center",
            color=MUTED,
            fontsize=9.5,
            fontweight="bold",
        )

    # The final phase is narrower than its label. Keep the label readable in
    # the independent band and use a short leader to its exact interval.
    phase_ax.text(
        83.0,
        0.62,
        "Finish & Upload",
        transform=phase_ax.get_xaxis_transform(),
        ha="center",
        va="center",
        color=MUTED,
        fontsize=9.2,
        fontweight="bold",
    )
    phase_ax.hlines(
        0.12,
        87.0,
        (88.0 + pod_stop_s) / 2,
        color=MUTED,
        linewidth=0.9,
        alpha=0.8,
    )
    phase_ax.set_xlim(0, pod_stop_s + 0.8)
    phase_ax.set_ylim(0, 1)
    phase_ax.set_xticks([])
    phase_ax.set_yticks([])
    phase_ax.grid(False)
    for spine in phase_ax.spines.values():
        spine.set_visible(False)

    ax.axvline(pod_stop_s, color=FG, linestyle=(0, (3, 3)), linewidth=1.25, alpha=0.85, zorder=3)
    ax.text(
        pod_stop_s - 1.5,
        0.075,
        "Pod Stops",
        transform=ax.get_xaxis_transform(),
        ha="right",
        va="bottom",
        color=FG,
        fontsize=9.5,
        fontweight="bold",
    )


def lifecycle_figure(title: str, subtitle: str) -> tuple[plt.Figure, plt.Axes, plt.Axes]:
    fig, ax = plt.subplots(figsize=(12, 2.6))
    # The header, phase band, and data axes occupy separate vertical zones so
    # they remain distinct when the chart is scaled down on a 16:9 slide.
    fig.subplots_adjust(left=0.11, right=0.94, bottom=0.29, top=0.585)
    phase_ax = fig.add_axes([0.11, 0.61, 0.83, 0.072], facecolor=PANEL)
    fig.suptitle(title, x=0.06, y=0.96, ha="left", fontsize=21, fontweight="bold", color=FG)
    fig.text(0.06, 0.805, subtitle, ha="left", color=MUTED, fontsize=11.5)
    return fig, ax, phase_ax


def render_cpu_lifecycle(lifecycle: dict[str, object]) -> Path:
    series = lifecycle["series"]
    pod_stop_s = float(lifecycle["pod_stop_s"])
    fig, ax, phase_ax = lifecycle_figure(
        "Collector CPU lifecycle",
        "50k no-op tasks · 2,270 task/s",
    )
    add_phases(ax, phase_ax, pod_stop_s)
    for role, color in (("Head", BLUE), ("Worker", ORANGE)):
        values = series[role]
        sns.lineplot(
            x=values["cpu_t"],
            y=values["cpu_m"],
            ax=ax,
            color=color,
            linewidth=2.25,
            label=f"{role} Collector",
            errorbar=None,
            sort=False,
            legend=False,
        )
    ax.set_xlim(0, pod_stop_s + 0.8)
    ax.set_ylim(0, 1060)
    ax.set_xlabel("Seconds since first Collector sample", labelpad=8)
    ax.set_ylabel("Interval CPU (mCPU)", labelpad=10)
    ax.yaxis.set_major_locator(MultipleLocator(200))
    handles, labels = ax.get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        loc="upper right",
        bbox_to_anchor=(0.94, 0.91),
        ncol=2,
        frameon=False,
        fontsize=10.5,
        handlelength=2.2,
    )

    final_x = max(float(series[role]["cpu_t"][-1]) for role in ROLES)
    final_values = {role: float(series[role]["cpu_m"][-1]) for role in ROLES}
    final_dt = statistics.fmean(float(series[role]["cpu_dt"][-1]) for role in ROLES)
    ax.scatter(
        [final_x, final_x],
        [final_values["Head"], final_values["Worker"]],
        color=[BLUE, ORANGE],
        edgecolor=FG,
        linewidth=1.0,
        s=68,
        zorder=5,
    )
    ax.annotate(
        f"Final {final_dt:.3f} s interval\nHead {final_values['Head']:.0f}m · Worker {final_values['Worker']:.0f}m",
        xy=(final_x, max(final_values.values())),
        xytext=(61, 610),
        color=FG,
        fontsize=10.0,
        fontweight="bold",
        arrowprops={
            "arrowstyle": "->",
            "color": MUTED,
            "lw": 1.15,
            "shrinkA": 5,
            "shrinkB": 4,
        },
        bbox={"boxstyle": "round,pad=0.25", "facecolor": PANEL, "edgecolor": "none", "alpha": 0.9},
        ha="left",
        va="center",
    )
    finish_axis(ax)
    return save(fig, "collector-cpu-lifecycle-wide.png")


def render_memory_lifecycle(lifecycle: dict[str, object]) -> Path:
    series = lifecycle["series"]
    pod_stop_s = float(lifecycle["pod_stop_s"])
    fig, ax, phase_ax = lifecycle_figure(
        "Collector memory lifecycle",
        "50k no-op tasks · whole-container memory use",
    )
    add_phases(ax, phase_ax, pod_stop_s)
    for role, color in (("Head", BLUE), ("Worker", ORANGE)):
        values = series[role]
        sns.lineplot(
            x=values["elapsed"],
            y=values["current"],
            ax=ax,
            color=color,
            linewidth=2.6,
            label=f"{role} Collector",
            errorbar=None,
            sort=False,
            legend=False,
        )
    ax.set_xlim(0, pod_stop_s + 0.8)
    ax.set_ylim(0, 132)
    ax.set_xlabel("Seconds since first Collector sample", labelpad=8)
    ax.set_ylabel("memory.current (MiB)", labelpad=10)
    ax.yaxis.set_major_locator(MultipleLocator(20))
    handles, labels = ax.get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        loc="upper right",
        bbox_to_anchor=(0.94, 0.91),
        ncol=2,
        frameon=False,
        fontsize=10.5,
        handlelength=2.2,
    )
    ax.annotate(
        "~110 MiB plateau\nmostly file-backed charge",
        xy=(72, 110),
        xytext=(56, 52),
        color=FG,
        fontsize=10.0,
        fontweight="bold",
        arrowprops={
            "arrowstyle": "->",
            "color": MUTED,
            "lw": 1.15,
            "shrinkA": 5,
            "shrinkB": 3,
        },
        bbox={"boxstyle": "round,pad=0.25", "facecolor": PANEL, "edgecolor": "none", "alpha": 0.9},
        ha="left",
        va="center",
    )
    finish_axis(ax)
    return save(fig, "collector-memory-lifecycle-wide.png")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    configure_theme()
    grouped = load_scaling()
    lifecycle = load_lifecycle()

    paths = [
        render_scaling_chart(
            grouped,
            metric="cpu_m",
            title="CPU vs peak event ingress",
            subtitle="50k no-op tasks · approx. 250–2,270 task/s",
            ylabel="Peak 10 s mean CPU\n(mCPU)",
            ylim=(0, 122),
            filename="collector-cpu-scaling-side.png",
        ),
        render_scaling_chart(
            grouped,
            metric="memory_mib",
            title="Memory vs peak event ingress",
            subtitle="50k no-op tasks · memory.current includes Linux file cache",
            ylabel="Lifecycle memory.current\nP95 (MiB)",
            ylim=(0, 125),
            filename="collector-memory-scaling-side.png",
        ),
        render_cpu_lifecycle(lifecycle),
        render_memory_lifecycle(lifecycle),
    ]

    summary = {
        "paths": [str(path) for path in paths],
        "scaling": grouped,
        "lifecycle": {
            "pod_stop_s": lifecycle["pod_stop_s"],
            "driver_rate": lifecycle["driver_rate"],
            "final_cpu": {
                role: {
                    "interval_s": lifecycle["series"][role]["cpu_dt"][-1],
                    "mCPU": lifecycle["series"][role]["cpu_m"][-1],
                }
                for role in ROLES
            },
            "memory_current_p95_mib": {
                role: nearest_rank_p95(
                    [int(value * 1024**2) for value in lifecycle["series"][role]["current"]]
                )
                for role in ROLES
            },
            "final_memory_current_mib": {
                role: lifecycle["series"][role]["current"][-1] for role in ROLES
            },
        },
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
