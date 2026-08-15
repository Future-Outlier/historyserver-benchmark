#!/usr/bin/env python3
"""Build slide-ready Collector memory lifecycle figures from formal r7 artifacts.

This script intentionally uses only the Python standard library.  It renders SVG
directly and asks ImageMagick to rasterize the SVG at 3200x1800.
"""

from __future__ import annotations

import csv
import glob
import hashlib
import html
import json
import math
import os
import statistics
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


SOURCE = Path("/private/tmp/kuberay-collector-memory-formal-20260813-r7")
OUT = Path("/private/tmp/collector-memory-lifecycle-4-rates")
FONT = OUT / "Basic-Regular.ttf"
RATES = (1000, 2000, 3000, 5000)
REPEATS = (1, 2, 3)
MIB = 1024 * 1024
T_MIN, T_MAX, DT = -10.0, 108.0, 0.25
Y_MAX = 270.0
WIDTH, HEIGHT = 3200, 1800

RATE_COLORS = {
    1000: "#58B7FF",
    2000: "#FFB340",
    3000: "#4ED5A1",
    5000: "#E985C3",
}
CURRENT_COLOR = "#67B7FF"
FILE_COLOR = "#FF9D58"
ANON_COLOR = "#62D8A7"
UPLOAD_COLOR = "#F7D154"
BG = "#05070B"
PLOT_BG = "#111720"
FG = "#F4F6FA"
MUTED = "#AEB7C5"
GRID = "#34404D"
FRAME = "#6B7580"


@dataclass
class Run:
    rate: int
    repeat: int
    report_path: Path
    samples_path: Path
    t: list[float]
    current: list[float]
    file: list[float]
    anon: list[float]
    upload_times: list[float]
    raw_mib: float
    peak_mib: float
    complete_t: float


def median(values: Iterable[float]) -> float:
    return float(statistics.median(list(values)))


def esc(value: object) -> str:
    return html.escape(str(value), quote=True)


def text(x: float, y: float, value: object, size: int, *, fill: str = FG,
         anchor: str = "start", weight: int = 400, opacity: float = 1.0,
         cls: str = "") -> str:
    klass = f' class="{esc(cls)}"' if cls else ""
    return (
        f'<text x="{x:.1f}" y="{y:.1f}" fill="{fill}" font-size="{size}" '
        f'font-weight="{weight}" text-anchor="{anchor}" opacity="{opacity:.3f}"{klass}>'
        f'{esc(value)}</text>'
    )


def line(x1: float, y1: float, x2: float, y2: float, *, stroke: str,
         width: float = 2, opacity: float = 1.0, dash: str | None = None) -> str:
    d = f' stroke-dasharray="{dash}"' if dash else ""
    return (
        f'<line x1="{x1:.2f}" y1="{y1:.2f}" x2="{x2:.2f}" y2="{y2:.2f}" '
        f'stroke="{stroke}" stroke-width="{width}" opacity="{opacity:.3f}"{d}/>'
    )


def rect(x: float, y: float, w: float, h: float, *, fill: str,
         opacity: float = 1.0, stroke: str | None = None, width: float = 1.0) -> str:
    s = f' stroke="{stroke}" stroke-width="{width}"' if stroke else ""
    return (
        f'<rect x="{x:.2f}" y="{y:.2f}" width="{w:.2f}" height="{h:.2f}" '
        f'fill="{fill}" opacity="{opacity:.3f}"{s}/>'
    )


def circle(x: float, y: float, r: float, *, fill: str, stroke: str = FG,
           width: float = 2.0) -> str:
    return (
        f'<circle cx="{x:.2f}" cy="{y:.2f}" r="{r:.2f}" fill="{fill}" '
        f'stroke="{stroke}" stroke-width="{width}"/>'
    )


def diamond(x: float, y: float, r: float, *, fill: str, stroke: str = FG,
            width: float = 2.0) -> str:
    pts = f"{x:.2f},{y-r:.2f} {x+r:.2f},{y:.2f} {x:.2f},{y+r:.2f} {x-r:.2f},{y:.2f}"
    return f'<polygon points="{pts}" fill="{fill}" stroke="{stroke}" stroke-width="{width}"/>'


def path(points: list[tuple[float, float]], *, stroke: str, width: float = 4,
         opacity: float = 1.0, fill: str = "none", dash: str | None = None) -> str:
    if not points:
        return ""
    ds = f' stroke-dasharray="{dash}"' if dash else ""
    if fill == "none":
        # ImageMagick's legacy SVG delegate fills open paths/polylines black and
        # also darkens standalone <line> strokes.  A dense chain of filled SVG
        # circles survives both SVG and PNG rendering with the exact series
        # color while preserving the intended visual line.
        render_points = points
        if len(points) <= 10 and len(points) > 1:
            render_points = []
            for segment_index, ((x1, y1), (x2, y2)) in enumerate(zip(points, points[1:])):
                steps = max(8, int(math.hypot(x2 - x1, y2 - y1) / max(width * 0.55, 1.0)))
                for step in range(steps):
                    if segment_index and step == 0:
                        continue
                    alpha = step / steps
                    render_points.append((x1 + alpha * (x2 - x1), y1 + alpha * (y2 - y1)))
            render_points.append(points[-1])
        dots = []
        radius = max(2.0, width * 0.52)
        for i, (x, y) in enumerate(render_points):
            if dash and len(render_points) > 20:
                if dash.startswith("5") and i % 4 not in (0, 1):
                    continue
                if not dash.startswith("5") and i % 5 == 4:
                    continue
            dots.append(
                f'<circle cx="{x:.2f}" cy="{y:.2f}" r="{radius:.2f}" '
                f'fill="{stroke}" stroke="none" opacity="{opacity:.3f}"/>'
            )
        return "\n".join(dots)
    d = "M " + " L ".join(f"{x:.2f} {y:.2f}" for x, y in points)
    return (
        f'<path d="{d}" fill="{fill}" stroke="{stroke}" stroke-width="{width}" '
        f'stroke-linejoin="round" stroke-linecap="round" opacity="{opacity:.3f}"{ds}/>'
    )


def area(upper: list[tuple[float, float]], lower: list[tuple[float, float]], *,
         fill: str, opacity: float) -> str:
    if not upper or not lower:
        return ""
    pts = upper + list(reversed(lower))
    d = "M " + " L ".join(f"{x:.2f} {y:.2f}" for x, y in pts) + " Z"
    return f'<path d="{d}" fill="{fill}" opacity="{opacity:.3f}" stroke="none"/>'


def find_one(pattern: str) -> Path:
    hits = glob.glob(pattern)
    if len(hits) != 1:
        raise RuntimeError(f"expected exactly one match for {pattern!r}, got {hits}")
    return Path(hits[0])


def load_run(rate: int, repeat: int) -> Run:
    base = SOURCE / f"A-rate{rate}-r{repeat}"
    report_path = find_one(str(base / "*" / "collector-memory-report.json"))
    samples_path = report_path.parent / "collector_memory_samples.csv"
    report = json.loads(report_path.read_text())
    phases = {p["name"]: int(p["timeNano"]) for p in report["phases"]}
    ingest_ns = phases["ingest"]
    complete_t = (phases["complete"] - ingest_ns) / 1e9

    rows: list[tuple[float, float, float, float]] = []
    with samples_path.open(newline="") as f:
        for row in csv.DictReader(f):
            if not row["container"].endswith("/collector"):
                continue
            rows.append((
                (int(row["time_nano"]) - ingest_ns) / 1e9,
                int(row["current_bytes"]) / MIB,
                int(row["file_bytes"]) / MIB,
                int(row["anon_bytes"]) / MIB,
            ))
    rows.sort()
    if not rows or rows[0][0] > T_MIN or rows[-1][0] < 105.0:
        raise RuntimeError(f"insufficient lifecycle coverage in {samples_path}")

    collector = report["collectorLogs"][0]
    final = collector["finalCgroupMemory"]
    final_t = (int(final["unixNano"]) - ingest_ns) / 1e9
    final_current = int(final["currentBytes"]) / MIB
    # currentBytes is measured in-process immediately after final upload.  The
    # component breakdown is intentionally not fabricated after the sampler ends.
    rows.append((final_t, final_current, math.nan, math.nan))
    rows.append((complete_t, final_current, math.nan, math.nan))
    rows.sort()

    return Run(
        rate=rate,
        repeat=repeat,
        report_path=report_path,
        samples_path=samples_path,
        t=[r[0] for r in rows],
        current=[r[1] for r in rows],
        file=[r[2] for r in rows],
        anon=[r[3] for r in rows],
        upload_times=[(int(u["unixNano"]) - ingest_ns) / 1e9 for u in collector["uploadTimeline"]],
        raw_mib=int(report["replay"]["expected_jsonl_bytes"]) / MIB,
        peak_mib=int(final["peakBytes"]) / MIB,
        complete_t=complete_t,
    )


def interp(ts: list[float], ys: list[float], x: float) -> float | None:
    # Linear scan is acceptable for ~430 source samples and ~480 output points.
    if x < ts[0] or x > ts[-1]:
        return None
    lo, hi = 0, len(ts) - 1
    while lo + 1 < hi:
        mid = (lo + hi) // 2
        if ts[mid] <= x:
            lo = mid
        else:
            hi = mid
    if ts[lo] == x:
        return None if math.isnan(ys[lo]) else ys[lo]
    y0, y1 = ys[lo], ys[hi]
    if math.isnan(y0) or math.isnan(y1):
        return None
    if ts[hi] == ts[lo]:
        return y1
    f = (x - ts[lo]) / (ts[hi] - ts[lo])
    return y0 + f * (y1 - y0)


def common_grid() -> list[float]:
    count = int(round((T_MAX - T_MIN) / DT))
    return [round(T_MIN + i * DT, 6) for i in range(count + 1)]


def aggregate(runs: list[Run]) -> dict[int, dict[str, object]]:
    grid = common_grid()
    result: dict[int, dict[str, object]] = {}
    for rate in RATES:
        rr = [r for r in runs if r.rate == rate]
        if len(rr) != 3:
            raise RuntimeError(f"rate {rate}: expected n=3, got {len(rr)}")
        metrics: dict[str, list[float | None]] = {}
        for name in ("current", "file", "anon"):
            med, low, high = [], [], []
            for t in grid:
                vals = [v for r in rr if (v := interp(r.t, getattr(r, name), t)) is not None]
                med.append(median(vals) if vals else None)
                low.append(min(vals) if vals else None)
                high.append(max(vals) if vals else None)
            metrics[f"{name}_median"] = med
            metrics[f"{name}_min"] = low
            metrics[f"{name}_max"] = high
        upload_counts = {len(r.upload_times) for r in rr}
        if len(upload_counts) != 1:
            raise RuntimeError(f"rate {rate}: inconsistent upload count {upload_counts}")
        upload_times = [median(r.upload_times[i] for r in rr) for i in range(upload_counts.pop())]
        result[rate] = {
            "grid": grid,
            **metrics,
            "upload_times": upload_times,
            "raw_mib": median(r.raw_mib for r in rr),
            "peak_median": median(r.peak_mib for r in rr),
            "peak_min": min(r.peak_mib for r in rr),
            "peak_max": max(r.peak_mib for r in rr),
        }
    return result


def value_at(grid: list[float], values: list[float | None], t: float) -> float:
    pairs = [(x, y) for x, y in zip(grid, values) if y is not None]
    ts = [p[0] for p in pairs]
    ys = [float(p[1]) for p in pairs]
    value = interp(ts, ys, t)
    return value if value is not None else ys[-1]


def svg_header(title_value: str, desc_value: str) -> list[str]:
    return [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{HEIGHT}" viewBox="0 0 {WIDTH} {HEIGHT}">',
        f'<title>{esc(title_value)}</title>',
        f'<desc>{esc(desc_value)}</desc>',
        '<style>text{font-family:Arial,Helvetica,sans-serif;font-variant-numeric:tabular-nums;} path,line{vector-effect:non-scaling-stroke;}</style>',
        rect(0, 0, WIDTH, HEIGHT, fill=BG),
    ]


PHASES = [
    ("Baseline", -10.0, 0.0, "#66717D"),
    ("Ingest", 0.0, 90.0, "#285477"),
    ("Idle", 90.0, 105.0, "#57426F"),
    ("Shutdown", 105.0, 108.0, "#25614E"),
]


def draw_plot_base(parts: list[str], x0: float, y0: float, w: float, h: float,
                   *, show_y_labels: bool = True, show_x_labels: bool = True,
                   phase_labels: bool = True) -> tuple[callable, callable]:
    sx = lambda t: x0 + (t - T_MIN) / (T_MAX - T_MIN) * w
    sy = lambda v: y0 + h - v / Y_MAX * h
    parts.append(rect(x0, y0, w, h, fill=PLOT_BG, stroke=FRAME, width=2))
    for label, a, b, color in PHASES:
        parts.append(rect(sx(a), y0, sx(b) - sx(a), h, fill=color, opacity=0.12))
    for value in (0, 50, 100, 150, 200, 250, 270):
        y = sy(value)
        parts.append(line(x0, y, x0 + w, y, stroke=GRID, width=1.6, opacity=0.72))
        if show_y_labels:
            parts.append(text(x0 - 24, y + 10, value, 27, fill=MUTED, anchor="end"))
    for value in (-10, 0, 30, 60, 90, 108):
        x = sx(value)
        parts.append(line(x, y0, x, y0 + h, stroke=GRID, width=1.4, opacity=0.58))
        if show_x_labels:
            parts.append(text(x, y0 + h + 43, value, 27, fill=MUTED, anchor="middle"))
    if phase_labels:
        for label, a, b, color in PHASES:
            parts.append(rect(sx(a), y0 - 43, sx(b) - sx(a), 35, fill=color, opacity=0.55))
            size = 19 if (b - a) <= 3.1 else 23
            parts.append(text((sx(a) + sx(b)) / 2, y0 - 17, label, size, fill=FG, anchor="middle", weight=500))
    return sx, sy


def valid_points(grid: list[float], values: list[float | None], sx, sy) -> list[tuple[float, float]]:
    return [(sx(t), sy(float(v))) for t, v in zip(grid, values) if v is not None]


def draw_upload_markers(parts: list[str], data: dict[str, object], sx, sy,
                        values: list[float | None], *, color: str) -> None:
    grid = data["grid"]
    for t in data["upload_times"]:
        v = value_at(grid, values, float(t))
        parts.append(diamond(sx(float(t)), sy(v), 11, fill=color, stroke=FG, width=3))


def build_overlay(data: dict[int, dict[str, object]]) -> str:
    parts = svg_header(
        "Collector memory lifecycle across event rates",
        "Four median memory.current curves with min-max bands across three runs, aligned to ingest start.",
    )
    parts.append(text(145, 105, "Collector Memory Lifecycle Across Event Rates", 72, weight=500))
    parts.append(text(145, 164, "90 s continuous ingest · 15 s idle · n=3 per rate · Collector request 128 MiB / limit 1 GiB", 31, fill=MUTED))

    legend_y = 235
    for i, rate in enumerate(RATES):
        x = 160 + i * 760
        d = data[rate]
        color = RATE_COLORS[rate]
        parts.append(line(x, legend_y - 10, x + 70, legend_y - 10, stroke=color, width=9))
        label = f"{rate//1000}k/s · raw {d['raw_mib']:.1f} MiB · lifetime peak {d['peak_median']:.1f} MiB"
        parts.append(text(x + 88, legend_y, label, 27, fill=FG, weight=500))

    x0, y0, w, h = 205, 350, 2860, 1180
    sx, sy = draw_plot_base(parts, x0, y0, w, h)
    parts.append(text(66, y0 + h / 2, "memory.current (MiB)", 34, fill=FG, anchor="middle",
                      cls="y-axis"))
    # Rotate y-axis label around its own anchor.
    parts[-1] = parts[-1].replace('class="y-axis"', f'transform="rotate(-90 66 {y0+h/2:.1f})"')

    for rate in RATES:
        d = data[rate]
        grid = d["grid"]
        color = RATE_COLORS[rate]
        upper = valid_points(grid, d["current_max"], sx, sy)
        lower = valid_points(grid, d["current_min"], sx, sy)
        parts.append(area(upper, lower, fill=color, opacity=0.16))
        parts.append(path(valid_points(grid, d["current_median"], sx, sy), stroke=color, width=7))
        draw_upload_markers(parts, d, sx, sy, d["current_median"], color=color)

    parts.append(text(x0 + w / 2, 1629, "Seconds from ingest start", 34, fill=FG, anchor="middle"))
    parts.append(diamond(215, 1703, 11, fill=UPLOAD_COLOR, stroke=FG, width=3))
    parts.append(text(240, 1713, "Upload complete", 27, fill=MUTED))
    parts.append(text(540, 1713, "Line = median; band = min–max across 3 fresh Pods", 27, fill=MUTED))
    parts.append(text(3035, 1713, "Fixed event size: 895 B JSONL", 27, fill=MUTED, anchor="end"))
    parts.append("</svg>")
    return "\n".join(parts)


def build_components(data: dict[int, dict[str, object]]) -> str:
    parts = svg_header(
        "Collector memory lifecycle components by event rate",
        "Four panels showing median memory.current, file cache, anonymous memory, current min-max bands, and uploads.",
    )
    parts.append(text(115, 100, "Why Collector Memory Rises and Falls", 68, weight=500))
    parts.append(text(115, 157, "Whole-container current is dominated by file cache; anonymous memory stays near 20–28 MiB", 31, fill=MUTED))

    # Shared legend.
    legend_y = 220
    legend = [(CURRENT_COLOR, "memory.current", None), (FILE_COLOR, "file cache", "14 10"), (ANON_COLOR, "anonymous", "5 9")]
    for i, (color, label, dash) in enumerate(legend):
        x = 185 + i * 390
        parts.append(circle(x + 12, legend_y - 9, 10, fill=color, stroke=FG, width=2))
        parts.append(text(x + 38, legend_y, label, 27, fill=FG))
    parts.append(diamond(1460, legend_y - 9, 10, fill=UPLOAD_COLOR, stroke=FG, width=3))
    parts.append(text(1483, legend_y, "upload complete", 27, fill=FG))
    parts.append(text(3010, legend_y, "Current band = min–max, n=3", 27, fill=MUTED, anchor="end"))

    panel_positions = {
        1000: (160, 345),
        2000: (1660, 345),
        3000: (160, 985),
        5000: (1660, 985),
    }
    pw, ph = 1380, 475
    for rate in RATES:
        x0, y0 = panel_positions[rate]
        d = data[rate]
        parts.append(text(x0, y0 - 75, f"{rate//1000}k events/s", 36, fill=FG, weight=500))
        stats = f"90 s raw {d['raw_mib']:.1f} MiB · lifetime peak {d['peak_median']:.1f} [{d['peak_min']:.1f}–{d['peak_max']:.1f}] MiB"
        parts.append(text(x0, y0 - 35, stats, 25, fill=MUTED))
        show_y = rate in (1000, 3000)
        show_x = rate in (3000, 5000)
        sx, sy = draw_plot_base(parts, x0, y0, pw, ph, show_y_labels=show_y,
                                show_x_labels=show_x, phase_labels=False)
        grid = d["grid"]
        parts.append(area(valid_points(grid, d["current_max"], sx, sy),
                          valid_points(grid, d["current_min"], sx, sy),
                          fill=CURRENT_COLOR, opacity=0.14))
        parts.append(path(valid_points(grid, d["current_median"], sx, sy), stroke=CURRENT_COLOR, width=6))
        parts.append(path(valid_points(grid, d["file_median"], sx, sy), stroke=FILE_COLOR, width=5, dash="14 10"))
        parts.append(path(valid_points(grid, d["anon_median"], sx, sy), stroke=ANON_COLOR, width=5, dash="5 9"))
        draw_upload_markers(parts, d, sx, sy, d["current_median"], color=UPLOAD_COLOR)

    # Shared axes and compact phase key.
    parts.append(text(53, 890, "Memory (MiB)", 32, fill=FG, anchor="middle"))
    parts[-1] = parts[-1].replace('</text>', '</text>').replace('text-anchor="middle"', 'text-anchor="middle" transform="rotate(-90 53 890)"')
    parts.append(text(1600, 1575, "Seconds from ingest start", 32, fill=FG, anchor="middle"))
    key_x, key_y, key_w = 530, 1650, 2140
    for label, a, b, color in PHASES:
        xa = key_x + (a - T_MIN) / (T_MAX - T_MIN) * key_w
        xb = key_x + (b - T_MIN) / (T_MAX - T_MIN) * key_w
        parts.append(rect(xa, key_y, xb - xa, 28, fill=color, opacity=0.72))
        size = 18 if (b-a) <= 3.1 else 22
        parts.append(text((xa + xb) / 2, key_y + 56, label, size, fill=MUTED, anchor="middle"))
    parts.append(text(1600, 1760, "Measured in the Collector cgroup · fixed 895 B/event · request 128 MiB / limit 1 GiB", 26, fill=MUTED, anchor="middle"))
    parts.append("</svg>")
    return "\n".join(parts)


def build_two_rate_long(data: dict[int, dict[str, object]]) -> str:
    """Render the slide-focused 1k/s versus 5k/s component lifecycle."""
    parts = svg_header(
        "Collector memory lifecycle at 1k and 5k events per second",
        "Two aligned horizontal panels showing current, file cache, anonymous memory, variability, and uploads.",
    )
    parts.append(text(125, 96, "Collector Memory Lifecycle: 1k/s vs 5k/s", 67, weight=500))
    parts.append(text(
        125, 150,
        "90 s continuous ingest · 15 s idle · n=3 fresh Pods per rate · request 128 MiB / limit 1 GiB",
        30, fill=MUTED,
    ))

    # Shared legend, with both color and line style carrying meaning.
    legend_y = 215
    legend = [
        (CURRENT_COLOR, "memory.current median", None),
        (FILE_COLOR, "file cache median", "14 10"),
        (ANON_COLOR, "anonymous median", "5 9"),
    ]
    for i, (color, label, dash) in enumerate(legend):
        x = 175 + i * 610
        parts.append(circle(x + 12, legend_y - 9, 10, fill=color, stroke=FG, width=2))
        parts.append(text(x + 38, legend_y, label, 27, fill=FG))
    parts.append(diamond(2110, legend_y - 9, 11, fill=UPLOAD_COLOR, stroke=FG, width=3))
    parts.append(text(2138, legend_y, "upload complete", 27, fill=FG))
    parts.append(text(3045, legend_y, "Current band = min–max", 27, fill=MUTED, anchor="end"))

    x0, w, h = 205, 2860, 520
    panel_y = {1000: 385, 5000: 1110}
    for rate in (1000, 5000):
        y0 = panel_y[rate]
        d = data[rate]
        parts.append(text(x0, y0 - 84, f"{rate//1000}k events/s", 39, fill=FG, weight=500))
        stats = (
            f"90 s raw {d['raw_mib']:.1f} MiB · lifetime peak median "
            f"{d['peak_median']:.1f} MiB [{d['peak_min']:.1f}–{d['peak_max']:.1f}]"
        )
        parts.append(text(x0 + 300, y0 - 84, stats, 27, fill=MUTED))
        sx, sy = draw_plot_base(
            parts, x0, y0, w, h,
            show_y_labels=True, show_x_labels=True, phase_labels=True,
        )
        grid = d["grid"]
        parts.append(area(
            valid_points(grid, d["current_max"], sx, sy),
            valid_points(grid, d["current_min"], sx, sy),
            fill=CURRENT_COLOR, opacity=0.14,
        ))
        parts.append(path(
            valid_points(grid, d["current_median"], sx, sy),
            stroke=CURRENT_COLOR, width=7,
        ))
        parts.append(path(
            valid_points(grid, d["file_median"], sx, sy),
            stroke=FILE_COLOR, width=6, dash="14 10",
        ))
        parts.append(path(
            valid_points(grid, d["anon_median"], sx, sy),
            stroke=ANON_COLOR, width=6, dash="5 9",
        ))
        draw_upload_markers(
            parts, d, sx, sy, d["current_median"], color=UPLOAD_COLOR,
        )
        parts.append(text(
            65, y0 + h / 2, "Memory (MiB)", 30, fill=FG, anchor="middle",
            cls=f"two-rate-y-{rate}",
        ))
        parts[-1] = parts[-1].replace(
            f'class="two-rate-y-{rate}"',
            f'transform="rotate(-90 65 {y0+h/2:.1f})"',
        )
        parts.append(text(
            x0 + w / 2, y0 + h + 86, "Seconds from ingest start", 30,
            fill=FG, anchor="middle",
        ))

    parts.append(text(
        3045, 1762,
        "Collector cgroup · fixed event size 895 B JSONL · shared axes: −10…108 s and 0…270 MiB",
        25, fill=MUTED, anchor="end",
    ))
    parts.append("</svg>")
    return "\n".join(parts)


def write_data(data: dict[int, dict[str, object]]) -> None:
    with (OUT / "lifecycle-summary.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["rate_events_per_second", "runs", "raw_mib", "lifetime_peak_median_mib",
                    "lifetime_peak_min_mib", "lifetime_peak_max_mib", "median_upload_times_seconds"])
        for rate in RATES:
            d = data[rate]
            w.writerow([rate, 3, f"{d['raw_mib']:.6f}", f"{d['peak_median']:.6f}",
                        f"{d['peak_min']:.6f}", f"{d['peak_max']:.6f}",
                        ";".join(f"{x:.3f}" for x in d["upload_times"])])

    fields = ["rate_events_per_second", "time_seconds", "current_median_mib", "current_min_mib",
              "current_max_mib", "file_median_mib", "anon_median_mib"]
    with (OUT / "lifecycle-plot-data.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for rate in RATES:
            d = data[rate]
            for i, t in enumerate(d["grid"]):
                def fmt(name: str) -> str:
                    value = d[name][i]
                    return "" if value is None else f"{value:.6f}"
                w.writerow({
                    "rate_events_per_second": rate,
                    "time_seconds": f"{t:.3f}",
                    "current_median_mib": fmt("current_median"),
                    "current_min_mib": fmt("current_min"),
                    "current_max_mib": fmt("current_max"),
                    "file_median_mib": fmt("file_median"),
                    "anon_median_mib": fmt("anon_median"),
                })


def rasterize(svg_path: Path, png_path: Path) -> None:
    if not FONT.is_file():
        raise RuntimeError(
            f"missing {FONT}; download Basic-Regular.ttf from "
            "https://raw.githubusercontent.com/google/fonts/main/ofl/basic/Basic-Regular.ttf"
        )
    subprocess.run([
        "/opt/homebrew/bin/magick", "-font", str(FONT), str(svg_path),
        "-resize", f"{WIDTH}x{HEIGHT}!", "-strip", str(png_path)
    ], check=True)


def sha256(path_value: Path) -> str:
    h = hashlib.sha256()
    with path_value.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    runs = [load_run(rate, repeat) for rate in RATES for repeat in REPEATS]
    data = aggregate(runs)
    write_data(data)

    overlay_svg = OUT / "collector-memory-lifecycle-overlay.svg"
    overlay_png = OUT / "collector-memory-lifecycle-overlay.png"
    components_svg = OUT / "collector-memory-lifecycle-components.svg"
    components_png = OUT / "collector-memory-lifecycle-components.png"
    two_rate_svg = OUT / "collector-memory-lifecycle-1k-vs-5k.svg"
    two_rate_png = OUT / "collector-memory-lifecycle-1k-vs-5k.png"
    overlay_svg.write_text(build_overlay(data))
    components_svg.write_text(build_components(data))
    two_rate_svg.write_text(build_two_rate_long(data))
    rasterize(overlay_svg, overlay_png)
    rasterize(components_svg, components_png)
    rasterize(two_rate_svg, two_rate_png)

    checks = {
        "source_root": str(SOURCE),
        "authoritative_matrix": "A continuous-ingest arms only",
        "arms_loaded": len(runs),
        "rates": list(RATES),
        "runs_per_rate": 3,
        "time_alignment": "seconds relative to each arm's ingest phase",
        "time_domain_seconds": [T_MIN, T_MAX],
        "shared_y_domain_mib": [0, Y_MAX],
        "current_post_sampler_source": "collectorLogs.finalCgroupMemory currentBytes only",
        "component_post_sampler_values_fabricated": False,
        "all_source_reports_completed": all(json.loads(r.report_path.read_text())["completed"] for r in runs),
        "all_runs_cover_baseline_to_shutdown": all(r.t[0] <= T_MIN and r.t[-1] >= 105 for r in runs),
        "upload_counts_consistent_within_rate": True,
        "outputs": {},
    }
    for p in (overlay_svg, overlay_png, components_svg, components_png,
              two_rate_svg, two_rate_png,
              OUT / "lifecycle-summary.csv", OUT / "lifecycle-plot-data.csv"):
        checks["outputs"][p.name] = {"bytes": p.stat().st_size, "sha256": sha256(p)}
    checks["all_checks_pass"] = all([
        checks["arms_loaded"] == 12,
        checks["all_source_reports_completed"],
        checks["all_runs_cover_baseline_to_shutdown"],
        overlay_png.stat().st_size > 100_000,
        components_png.stat().st_size > 100_000,
        two_rate_png.stat().st_size > 100_000,
    ])
    (OUT / "qa.json").write_text(json.dumps(checks, indent=2) + "\n")

    readme = f"""# Collector memory lifecycle figures

Source: `{SOURCE}` formal r7, A continuous-ingest arms only (4 rates x 3 fresh Pods).

- `collector-memory-lifecycle-overlay.png`: slide version; median `memory.current`, min-max band, and median upload-completion markers.
- `collector-memory-lifecycle-components.png`: analysis version; median `memory.current`, `memory.stat.file`, and `memory.stat.anon` by rate.
- `collector-memory-lifecycle-1k-vs-5k.png`: slide-focused two-panel comparison with shared axes and component lifecycles.
- All panels share `0–270 MiB` and align `t=0` to the report's `ingest` phase.
- Fixed phases: baseline `-10–0 s`, ingest `0–90 s`, idle `90–105 s`, shutdown `105–108 s`.
- A component line stops when the external sampler stops. Only exact in-process final `memory.current` is appended; final `file`/`anon` are not invented.

Regenerate:

```bash
python3 {OUT / 'make_lifecycle_plots.py'}
```
"""
    (OUT / "README.md").write_text(readme)
    print(overlay_png)
    print(components_png)
    print(two_rate_png)
    print(OUT / "qa.json")


if __name__ == "__main__":
    main()
