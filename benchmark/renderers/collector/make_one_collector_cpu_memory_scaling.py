#!/usr/bin/env python3
"""Render formal-r7 one-Collector CPU and fixed-30s memory scaling charts."""

from __future__ import annotations

import csv
import importlib.util
import json
import math
import statistics
import subprocess
import sys
from pathlib import Path


ROOT = Path("/private/tmp/ray-summit-collector-results-r7")
OUT = ROOT / "charts"
MEM_HELPER = Path("/private/tmp/collector-memory-lifecycle-4-rates/make_lifecycle_plots.py")
CPU_HELPER = OUT / "make_cpu_lifecycle.py"
RATES = (1000, 2000, 3000, 5000)
REPEATS = (1, 2, 3)
COMBINED = "10-one-collector-cpu-memory-scaling"
CPU_ONLY = "10a-one-collector-cpu-scaling"
MEMORY_ONLY = "10b-one-collector-memory-fixed-30s"


def import_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot import {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


m = import_module("scaling_memory_helper", MEM_HELPER)
cpu = import_module("scaling_cpu_helper", CPU_HELPER)
m.FONT = OUT / "Basic-Regular.ttf"
cpu.T_DATA_MAX = 105.0  # all A arms cover this; only 90 s ingest is summarized.

BG, PLOT_BG, FG, MUTED = m.BG, m.PLOT_BG, m.FG, m.MUTED
GRID, FRAME = m.GRID, m.FRAME
BLUE, ORANGE, YELLOW, RED, GREEN = m.CURRENT_COLOR, m.FILE_COLOR, m.UPLOAD_COLOR, "#FF7272", m.ANON_COLOR


def median(values):
    return float(statistics.median(list(values)))


def regression(xs: list[float], ys: list[float]) -> dict[str, float]:
    x_mean, y_mean = sum(xs) / len(xs), sum(ys) / len(ys)
    slope = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys)) / sum((x - x_mean) ** 2 for x in xs)
    intercept = y_mean - slope * x_mean
    residual = sum((y - (intercept + slope * x)) ** 2 for x, y in zip(xs, ys))
    total = sum((y - y_mean) ** 2 for y in ys)
    return {"slope_per_1k_eps": slope, "intercept": intercept, "r_squared": 1.0 - residual / total}


def svg_header(width: int, height: int, title: str, desc: str) -> list[str]:
    return [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        f'<title>{m.esc(title)}</title>',
        f'<desc>{m.esc(desc)}</desc>',
        '<style>text{font-family:Arial,Helvetica,sans-serif;font-variant-numeric:tabular-nums;} path,line{vector-effect:non-scaling-stroke;}</style>',
        m.rect(0, 0, width, height, fill=BG),
    ]


def dashed_horizontal(parts: list[str], x0: float, x1: float, y: float, color: str, width: float, scale: float):
    dash, gap = 16.0 * scale, 10.0 * scale
    x = x0
    while x < x1:
        parts.append(m.rect(x, y - width / 2, min(dash, x1 - x), width, fill=color, opacity=.78))
        x += dash + gap


def draw_whisker(parts: list[str], x: float, y_low: float, y_high: float, color: str, scale: float):
    top, bottom = min(y_low, y_high), max(y_low, y_high)
    parts.append(m.rect(x - 2.5 * scale, top, 5 * scale, max(3 * scale, bottom - top), fill=color, opacity=.50))
    parts.append(m.circle(x, top, 4.5 * scale, fill=color, stroke=FG, width=1.2 * scale))
    parts.append(m.circle(x, bottom, 4.5 * scale, fill=color, stroke=FG, width=1.2 * scale))


def draw_axes(parts: list[str], x0: float, y0: float, width: float, height: float,
              y_max: float, y_ticks: tuple[float, ...], scale: float):
    sx = lambda rate: x0 + ((rate / 1000.0) - 0.7) / (5.3 - 0.7) * width
    sy = lambda value: y0 + height - value / y_max * height
    parts.append(m.rect(x0, y0, width, height, fill=PLOT_BG, stroke=FRAME, width=1.5 * scale))
    for value in y_ticks:
        y = sy(value)
        parts.append(m.line(x0, y, x0 + width, y, stroke=GRID, width=1.0 * scale, opacity=.65))
        parts.append(m.text(x0 - 12 * scale, y + 5 * scale, f"{value:g}", int(15 * scale), fill=MUTED, anchor="end"))
    for rate in RATES:
        x = sx(rate)
        parts.append(m.line(x, y0, x, y0 + height, stroke=GRID, width=.9 * scale, opacity=.45))
        parts.append(m.text(x, y0 + height + 30 * scale, f"{rate//1000}k", int(17 * scale), anchor="middle", weight=500))
    return sx, sy


def load_cpu_metrics() -> dict[int, dict]:
    output = {}
    for rate in RATES:
        runs = [cpu.load_run(rate, repeat) for repeat in REPEATS]
        rows = [cpu.run_stats(run) for run in runs]
        metrics = {}
        for key in ("ingest_mean_mcpu", "ingest_interval_p95_mcpu"):
            values = [float(row[key]) for row in rows]
            metrics[key] = {"median": median(values), "min": min(values), "max": max(values), "per_repeat": values}
        output[rate] = metrics
    return output


def load_memory_snapshot() -> tuple[dict[int, dict], float]:
    rows = list(csv.DictReader((ROOT / "a_lifecycle_quantiles.csv").open(newline="")))
    output = {}
    for rate in RATES:
        row = next(
            item for item in rows
            if int(item["target_events_per_second"]) == rate and float(item["seconds_relative_ingest"]) == 30.0
        )
        output[rate] = {
            "current": {"min": float(row["current_q0_mib"]), "median": float(row["current_q50_mib"]), "max": float(row["current_q100_mib"])},
            "file": {"min": float(row["file_q0_mib"]), "median": float(row["file_q50_mib"]), "max": float(row["file_q100_mib"])},
            "raw_spool": {"min": float(row["raw_spool_q0_mib"]), "median": float(row["raw_spool_q50_mib"]), "max": float(row["raw_spool_q100_mib"])},
        }
    uploads = list(csv.DictReader((ROOT / "a_uploads.csv").open(newline="")))
    earliest_upload = min(float(row["time_s_min"]) for row in uploads)
    return output, earliest_upload


def draw_cpu_plot(parts: list[str], metrics: dict[int, dict], *, x0: float, y0: float, width: float, height: float, scale: float):
    sx, sy = draw_axes(parts, x0, y0, width, height, 120.0, (0, 20, 40, 60, 80, 100, 120), scale)
    points = []
    for rate in RATES:
        item = metrics[rate]["ingest_mean_mcpu"]
        x = sx(rate)
        draw_whisker(parts, x, sy(item["min"]), sy(item["max"]), BLUE, scale)
        parts.append(m.circle(x, sy(item["median"]), 7 * scale, fill=BLUE, stroke=FG, width=1.5 * scale))
        points.append((x, sy(item["median"])))
    parts.append(m.path(points, stroke=BLUE, width=4.5 * scale))


def draw_memory_plot(parts: list[str], snapshot: dict[int, dict], *, x0: float, y0: float, width: float, height: float, scale: float):
    sx, sy = draw_axes(parts, x0, y0, width, height, 175.0, (0, 40, 80, 120, 160, 175), scale)
    current_points = []
    for rate in RATES:
        x = sx(rate)
        current = snapshot[rate]["current"]
        draw_whisker(parts, x, sy(current["min"]), sy(current["max"]), BLUE, scale)
        parts.append(m.circle(x, sy(current["median"]), 7 * scale, fill=BLUE, stroke=FG, width=1.5 * scale))
        current_points.append((x, sy(current["median"])))
    parts.append(m.path(current_points, stroke=BLUE, width=4.5 * scale))


def cpu_callout(metrics: dict[int, dict]) -> str:
    return "Interval P95 medians: " + " · ".join(f"{rate//1000}k {metrics[rate]['ingest_interval_p95_mcpu']['median']:.1f}m" for rate in RATES)


def build_combined(cpu_metrics: dict[int, dict], memory: dict[int, dict], earliest_upload: float, fits: dict[str, dict]) -> str:
    parts = svg_header(3200, 1800, "How Task Throughput Drives Collector CPU and Memory", "Formal r7 per-Collector CPU and fixed-30s memory scaling.")
    parts.append(m.text(120, 100, "How Task Throughput Drives Collector CPU and Memory", 66, weight=500))
    parts.append(m.text(120, 155, "Through per-Collector event ingress · 895 B/event · n=3/rate", 29, fill=MUTED))
    panels = [(120, "CPU vs Event Ingress", BLUE),
              (1640, "Memory vs Event Ingress (30 s window)", BLUE)]
    for x, title, color in panels:
        parts.append(m.rect(x, 235, 1440, 1385, fill="#0D131C", opacity=.98, stroke=FRAME, width=2))
        parts.append(m.text(x + 45, 315, title, 39, weight=500))
        parts.append(m.rect(x + 45, 345, 180, 5, fill=color, opacity=.9))

    draw_cpu_plot(parts, cpu_metrics, x0=250, y0=395, width=1180, height=1030, scale=1.05)
    parts.append(m.text(840, 1520, "Target event ingress (events/s)", 25, anchor="middle", weight=500))
    parts.append(m.text(250, 380, "CPU (mCPU)", 22, fill=MUTED, weight=500))

    draw_memory_plot(parts, memory, x0=1770, y0=395, width=1180, height=1030, scale=1.05)
    parts.append(m.text(2360, 1520, "Target event ingress (events/s)", 25, anchor="middle", weight=500))
    parts.append(m.text(1770, 380, "Memory (MiB)", 22, fill=MUTED, weight=500))
    parts.append("</svg>")
    return "\n".join(parts)


def build_cpu_only(cpu_metrics: dict[int, dict]) -> str:
    parts = svg_header(1200, 900, "One Collector CPU vs event ingress", "Formal r7 steady-ingest CPU mean with n=3 min-max whiskers.")
    parts.append(m.text(55, 70, "CPU vs Event Ingress", 42, weight=500))
    parts.append(m.circle(65, 112, 7, fill=BLUE, stroke=BLUE, width=1)); parts.append(m.text(84, 119, "Collector", 17))
    draw_cpu_plot(parts, cpu_metrics, x0=105, y0=155, width=1030, height=610, scale=.82)
    parts.append(m.text(620, 840, "Target event ingress (events/s)", 19, anchor="middle", weight=500))
    parts.append(m.text(105, 145, "CPU (mCPU)", 16, fill=MUTED, weight=500))
    parts.append("</svg>")
    return "\n".join(parts)


def build_memory_only(memory: dict[int, dict], earliest_upload: float, fits: dict[str, dict]) -> str:
    parts = svg_header(1200, 900, "One Collector memory after 30 seconds of ingest", "Formal r7 fixed-30s memory with n=3 min-max whiskers.")
    parts.append(m.text(55, 70, "Memory vs Event Ingress", 40, weight=500))
    parts.append(m.circle(65, 112, 7, fill=BLUE, stroke=BLUE, width=1)); parts.append(m.text(84, 119, "Memory", 17))
    draw_memory_plot(parts, memory, x0=105, y0=155, width=1030, height=610, scale=.82)
    parts.append(m.text(620, 840, "Target event ingress (events/s)", 19, anchor="middle", weight=500))
    parts.append(m.text(105, 145, "Memory (MiB)", 16, fill=MUTED, weight=500))
    parts.append("</svg>")
    return "\n".join(parts)


def save(name: str, svg: str, width: int, height: int) -> tuple[Path, Path]:
    svg_path, png_path = OUT / f"{name}.svg", OUT / f"{name}.png"
    svg_path.write_text(svg)
    subprocess.run([
        "/opt/homebrew/bin/magick", "-font", str(m.FONT), str(svg_path),
        "-resize", f"{width}x{height}!", "-strip", str(png_path),
    ], check=True)
    return svg_path, png_path


def png_dimensions(path: Path) -> tuple[int, int]:
    raw = subprocess.check_output([
        "/opt/homebrew/bin/magick", "identify", "-format", "%w %h", str(path),
    ], text=True)
    width, height = raw.split()
    return int(width), int(height)


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    cpu_metrics = load_cpu_metrics()
    memory, earliest_upload = load_memory_snapshot()
    xs = [rate / 1000.0 for rate in RATES]
    fits = {
        "current": regression(xs, [memory[rate]["current"]["median"] for rate in RATES]),
        "file": regression(xs, [memory[rate]["file"]["median"] for rate in RATES]),
    }
    memory_only = "--memory-only" in sys.argv[1:]
    if memory_only:
        outputs = [
            (OUT / f"{COMBINED}.svg", OUT / f"{COMBINED}.png"),
            (OUT / f"{CPU_ONLY}.svg", OUT / f"{CPU_ONLY}.png"),
            save(MEMORY_ONLY, build_memory_only(memory, earliest_upload, fits), 1200, 900),
        ]
        if not all(path.exists() for pair in outputs[:2] for path in pair):
            raise RuntimeError("combined and CPU assets must exist before --memory-only regeneration")
    else:
        outputs = [
            save(COMBINED, build_combined(cpu_metrics, memory, earliest_upload, fits), 3200, 1800),
            save(CPU_ONLY, build_cpu_only(cpu_metrics), 1200, 900),
            save(MEMORY_ONLY, build_memory_only(memory, earliest_upload, fits), 1200, 900),
        ]
    contact = OUT / f"{COMBINED}-contact-sheet.png"
    subprocess.run([
        "/opt/homebrew/bin/magick", "montage", "-font", str(m.FONT),
        str(outputs[0][1]), str(outputs[1][1]), str(outputs[2][1]),
        "-thumbnail", "980x620", "-tile", "3x1", "-geometry", "980x620+20+20",
        "-background", BG, str(contact),
    ], check=True)

    qa = {
        "source": str(ROOT),
        "cpu_metric": "per-run 90-second ingest mean from cumulative cgroup v2 cpu.stat usage_usec; median/min/max across n=3",
        "memory_metric": "a_lifecycle_quantiles.csv at seconds_relative_ingest=30.0; q50 with q0-q100 across n=3",
        "earliest_upload_completion_seconds": earliest_upload,
        "rates": {
            str(rate): {
                "cpu": cpu_metrics[rate],
                "memory_t30": memory[rate],
            }
            for rate in RATES
        },
        "fits": fits,
        "outputs": {
            name: {"svg": str(svg), "png": str(png)}
            for name, (svg, png) in zip((COMBINED, CPU_ONLY, MEMORY_ONLY), outputs)
        },
        "contact_sheet": str(contact),
        "qa": {
            "all_png_nonempty": all(png.stat().st_size > 70_000 for _, png in outputs),
            "all_svg_nonempty": all(svg.stat().st_size > 5_000 for svg, _ in outputs),
            "all_no_nan_or_inf": all("nan" not in svg.read_text().lower() and "inf" not in svg.read_text().lower() for svg, _ in outputs),
            "combined_has_required_title_and_subtitle": all(
                required in outputs[0][0].read_text()
                for required in (
                    "How Task Throughput Drives Collector CPU and Memory",
                    "Through per-Collector event ingress · 895 B/event · n=3/rate",
                )
            ),
            "memory_snapshot_precedes_upload": earliest_upload > 30.0,
            "cpu_rate_count": len(cpu_metrics),
            "memory_rate_count": len(memory),
            "contact_sheet_nonempty": contact.stat().st_size > 100_000,
            "combined_png_is_3200x1800": png_dimensions(outputs[0][1]) == (3200, 1800),
            "individual_pngs_are_1200x900": all(png_dimensions(png) == (1200, 900) for _, png in outputs[1:]),
            "minimal_annotations_removed": all(
                forbidden not in "\n".join(svg.read_text() for svg, _ in outputs)
                for forbidden in (
                    "90 s cumulative-counter mean · median + min–max",
                    "Interval P95 medians",
                    "CPU request 100m",
                    "CPU limit 2 cores",
                    "earliest upload completion",
                    "memory.current fit:",
                    "memory.stat.file fit:",
                    "Scope boundary:",
                    "Formal r7 A ·",
                )
            ),
            "audience_facing_memory_labels": all(
                internal_name not in "\n".join(svg.read_text() for svg, _ in outputs)
                for internal_name in ("memory.current", "memory.stat.file", "configured --role=Worker")
            ),
            "axis_titles_present": all(
                required in "\n".join(svg.read_text() for svg, _ in outputs)
                for required in ("CPU (mCPU)", "Memory (MiB)")
            ),
            "parallel_panel_titles_present": all(
                required in "\n".join(svg.read_text() for svg, _ in outputs)
                for required in ("CPU vs Event Ingress", "Memory vs Event Ingress (30 s window)")
            ),
            "memory_only_title_updated": (
                ">Memory vs Event Ingress</text>" in outputs[2][0].read_text()
                and "Memory vs Event Ingress (30 s window)" not in outputs[2][0].read_text()
            ),
        },
    }
    qa["qa"]["all_checks_pass"] = all([
        qa["qa"]["all_png_nonempty"], qa["qa"]["all_svg_nonempty"], qa["qa"]["all_no_nan_or_inf"],
        qa["qa"]["combined_has_required_title_and_subtitle"], qa["qa"]["memory_snapshot_precedes_upload"],
        qa["qa"]["cpu_rate_count"] == 4, qa["qa"]["memory_rate_count"] == 4, qa["qa"]["contact_sheet_nonempty"],
        qa["qa"]["combined_png_is_3200x1800"], qa["qa"]["individual_pngs_are_1200x900"],
        qa["qa"]["minimal_annotations_removed"],
        qa["qa"]["audience_facing_memory_labels"], qa["qa"]["axis_titles_present"],
        qa["qa"]["parallel_panel_titles_present"], qa["qa"]["memory_only_title_updated"],
    ])
    qa_path = OUT / f"{COMBINED}-qa.json"
    qa_path.write_text(json.dumps(qa, indent=2) + "\n")
    for svg, png in outputs:
        print(png)
        print(svg)
    print(contact)
    print(qa_path)


if __name__ == "__main__":
    main()
