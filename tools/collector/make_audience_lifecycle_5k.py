#!/usr/bin/env python3
"""Render audience-facing formal-r7 5k Collector lifecycle assets."""

from __future__ import annotations

import importlib.util
import json
import re
import subprocess
import sys
from pathlib import Path


CHARTS = Path("/private/tmp/ray-summit-collector-results-r7/charts")
MEM_HELPER = Path("/private/tmp/collector-memory-lifecycle-4-rates/make_lifecycle_plots.py")
CPU_HELPER = CHARTS / "make_cpu_lifecycle.py"
SOURCE_GENERATOR = Path(
    "/private/tmp/kuberay-hs-source-prep/historyserver/test/benchmark/sweeps/collector_event_replay.py"
)
SEPARATE_NOOP_EVIDENCE = Path(
    "/private/tmp/ray-summit-final-20260810/collector-render-summary.json"
)

RATE = 5000
T_MIN, T_MAX = -10.0, 108.0
CPU_Y_MAX, MEMORY_Y_MAX = 1200.0, 260.0
CPU_LONG_Y_MAX = 2100.0
MEMORY_LONG_Y_MAX = 550.0
REQUEST_COLOR = "#82D4B0"
LIMIT_COLOR = "#E5968E"
COMBINED = "11-collector-resource-lifecycle-5k-audience"
CPU_LONG = "11a-collector-cpu-lifecycle-5k-long"
MEMORY_LONG = "11b-collector-memory-lifecycle-5k-long"


def import_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot import {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


m = import_module("audience_lifecycle_memory", MEM_HELPER)
cpu = import_module("audience_lifecycle_cpu", CPU_HELPER)
m.FONT = CHARTS / "Basic-Regular.ttf"

BG, PLOT_BG, FG, MUTED = m.BG, m.PLOT_BG, m.FG, m.MUTED
GRID, FRAME = m.GRID, m.FRAME
BLUE, YELLOW = m.CURRENT_COLOR, m.UPLOAD_COLOR
PHASES = (
    ("Baseline", -10.0, 0.0, "#66717D"),
    ("Ingest", 0.0, 90.0, "#285477"),
    ("Idle", 90.0, 105.0, "#57426F"),
    ("Shutdown", 105.0, 108.0, "#25614E"),
)


def svg_header(width: int, height: int, title: str, desc: str) -> list[str]:
    return [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        f"<title>{m.esc(title)}</title>",
        f"<desc>{m.esc(desc)}</desc>",
        '<style>text{font-family:Arial,Helvetica,sans-serif;font-variant-numeric:tabular-nums;} path,line{vector-effect:non-scaling-stroke;}</style>',
        m.rect(0, 0, width, height, fill=BG),
    ]


def rasterize(svg_path: Path, png_path: Path, width: int, height: int) -> None:
    subprocess.run([
        "/opt/homebrew/bin/magick", "-font", str(m.FONT), str(svg_path),
        "-resize", f"{width}x{height}!", "-strip", str(png_path),
    ], check=True)


def png_dimensions(path: Path) -> tuple[int, int]:
    raw = subprocess.check_output([
        "/opt/homebrew/bin/magick", "identify", "-format", "%w %h", str(path),
    ], text=True)
    width, height = raw.split()
    return int(width), int(height)


def valid_points(grid, values, sx, sy):
    return [(sx(t), sy(float(value))) for t, value in zip(grid, values) if value is not None]


def draw_legend(
    parts: list[str],
    x: float,
    y: float,
    series_label: str,
    scale: float,
    *,
    show_series: bool = True,
    font_scale: float | None = None,
) -> None:
    text_scale = font_scale if font_scale is not None else scale
    if show_series:
        parts.append(m.path([(x, y), (x + 62 * scale, y)], stroke=BLUE, width=5 * scale))
        parts.append(m.circle(x + 31 * scale, y, 6 * scale, fill=BLUE, stroke=FG, width=1 * scale))
        parts.append(m.text(x + 78 * scale, y + 7 * text_scale, series_label, int(19 * text_scale), weight=500))
    marker_x = x + (260 * scale if show_series else 0)
    parts.append(m.line(
        marker_x, y - 15 * scale, marker_x, y + 15 * scale,
        stroke=YELLOW, width=4 * scale, opacity=.78,
    ))
    parts.append(m.text(marker_x + 20 * scale, y + 7 * text_scale, "Upload complete", int(19 * text_scale), weight=500))


def draw_panel(
    parts: list[str],
    *,
    title: str,
    series_label: str,
    grid,
    values,
    upload_times,
    x0: float,
    y0: float,
    width: float,
    height: float,
    y_max: float,
    y_ticks: tuple[float, ...],
    show_x_ticks: bool,
    scale: float,
    title_y: float,
    axis_title: str,
    legend_x: float,
    show_series_legend: bool = True,
    guides: tuple[tuple[str, float, str], ...] = (),
    font_scale: float | None = None,
) -> None:
    text_scale = font_scale if font_scale is not None else scale
    sx = lambda t: x0 + (t - T_MIN) / (T_MAX - T_MIN) * width
    sy = lambda value: y0 + height - float(value) / y_max * height

    parts.append(m.text(x0, title_y, title, int(34 * text_scale), weight=500))
    if show_series_legend:
        draw_legend(
            parts,
            legend_x,
            title_y - 8 * scale,
            series_label,
            scale,
            show_series=True,
            font_scale=text_scale,
        )
    parts.append(m.text(x0, y0 - 52 * scale, axis_title, int(18 * text_scale), fill=MUTED, weight=500))
    parts.append(m.rect(x0, y0, width, height, fill=PLOT_BG, stroke=FRAME, width=1.5 * scale))

    for label, start, end, color in PHASES:
        parts.append(m.rect(sx(start), y0, sx(end) - sx(start), height, fill=color, opacity=.09))
        parts.append(m.rect(sx(start), y0 - 38 * scale, sx(end) - sx(start), 27 * scale, fill=color, opacity=.62))
        font_size = int((14 if end - start <= 4 else 17) * text_scale)
        parts.append(m.text((sx(start) + sx(end)) / 2, y0 - 18 * scale, label, font_size, anchor="middle", weight=500))

    for value in y_ticks:
        y = sy(value)
        parts.append(m.line(x0, y, x0 + width, y, stroke=GRID, width=1.0 * scale, opacity=.62))
        parts.append(m.text(x0 - 14 * scale, y + 6 * text_scale, f"{value:g}", int(15 * text_scale), fill=MUTED, anchor="end"))

    for value in (-10, 0, 30, 60, 90, 105, 108):
        x = sx(value)
        parts.append(m.line(x, y0, x, y0 + height, stroke=GRID, width=.9 * scale, opacity=.45))
        if show_x_ticks:
            parts.append(m.text(x, y0 + height + 31 * scale, value, int(15 * text_scale), fill=MUTED, anchor="middle"))

    # Resource guides render above the grid but below the blue data series.
    guide_labels: list[tuple[float, str, str]] = []
    for label, value, color in guides:
        y = sy(value)
        parts.append(m.line(
            x0, y, x0 + width, y,
            stroke=color,
            width=5.5,
            opacity=1.0,
            dash="30.0 12.0",
        ))
        label_y = y + 23 * scale if y - y0 < 35 * scale else y - 8 * scale
        guide_labels.append((label_y, label, color))

    parts.append(m.path(valid_points(grid, values, sx, sy), stroke=BLUE, width=5 * scale))

    for label_y, label, color in guide_labels:
        parts.append(m.text(
            x0 + width - 120 * scale,
            label_y,
            label,
            int(16 * text_scale),
            fill=color,
            anchor="end",
            weight=500,
        ))

    for t in upload_times:
        x = sx(t)
        label_y = y0 + height / 2
        parts.append(m.rect(x - 2 * scale, y0, 4 * scale, height, fill=YELLOW, opacity=.62))
        parts.append(m.rect(
            x - 104 * scale,
            label_y - 21 * text_scale,
            208 * scale,
            31 * text_scale,
            fill=PLOT_BG,
            opacity=.94,
        ))
        parts.append(m.text(
            x,
            label_y + 2 * text_scale,
            "Upload complete",
            int(14 * text_scale),
            fill=YELLOW,
            anchor="middle",
            weight=500,
        ))


def build_combined(cpu_data: dict, memory_data: dict) -> str:
    parts = svg_header(
        3200, 1800,
        "Collector Resource Lifecycle — 5k events/s",
        "Audience-facing formal-r7 CPU and memory median lifecycles with upload-completion markers.",
    )
    parts.append(m.text(120, 100, "Collector Resource Lifecycle — 5k events/s", 66, weight=500))
    parts.append(m.text(
        120, 154,
        "Formal r7 · n=3 · 5k events/s · comparable to ≈2,270 tasks/s in a separate no-op workload",
        27, fill=MUTED,
    ))

    parts.append(m.rect(110, 220, 2980, 680, fill="#0D131C", opacity=.98, stroke=FRAME, width=2))
    draw_panel(
        parts,
        title="CPU",
        series_label="CPU",
        grid=cpu_data["grid"],
        values=cpu_data["median"],
        upload_times=[row["median_s"] for row in cpu_data["uploads"]],
        x0=210, y0=405, width=2820, height=395,
        y_max=CPU_Y_MAX, y_ticks=(0, 200, 400, 600, 800, 1000, 1200),
        show_x_ticks=False, scale=1.0, title_y=300,
        axis_title="CPU (mCPU)", legend_x=430,
    )

    parts.append(m.rect(110, 955, 2980, 680, fill="#0D131C", opacity=.98, stroke=FRAME, width=2))
    draw_panel(
        parts,
        title="Memory",
        series_label="Memory",
        grid=memory_data["grid"],
        values=memory_data["current_median"],
        upload_times=memory_data["upload_times"],
        x0=210, y0=1140, width=2820, height=395,
        y_max=MEMORY_Y_MAX, y_ticks=(0, 50, 100, 150, 200, 250),
        show_x_ticks=True, scale=1.0, title_y=1035,
        axis_title="Memory (MiB)", legend_x=430,
    )
    parts.append(m.text(1620, 1605, "Seconds from ingest start", 24, anchor="middle", weight=500))
    parts.append("</svg>")
    return "\n".join(parts)


def build_long_panel(*, cpu_panel: bool, cpu_data: dict, memory_data: dict) -> str:
    title = "CPU" if cpu_panel else "Memory"
    parts = svg_header(
        3000, 650,
        f"Collector {title} lifecycle at 5k events/s",
        f"Audience-facing formal-r7 {title.lower()} median lifecycle with upload-completion markers.",
    )
    if cpu_panel:
        grid = cpu_data["grid"]
        values = cpu_data["median"]
        uploads = [row["median_s"] for row in cpu_data["uploads"]]
        y_max = CPU_LONG_Y_MAX
        y_ticks = (0, 500, 1000, 1500, 2000)
        axis_title = "CPU (mCPU)"
        guides = (
            ("CPU request 100m", 100.0, REQUEST_COLOR),
            ("CPU limit 2000m", 2000.0, LIMIT_COLOR),
        )
    else:
        grid = memory_data["grid"]
        values = memory_data["current_median"]
        uploads = memory_data["upload_times"]
        y_max = MEMORY_LONG_Y_MAX
        y_ticks = (0, 100, 200, 300, 400, 500)
        axis_title = "Memory (MiB)"
        guides = (
            ("Memory request 128 MiB", 128.0, REQUEST_COLOR),
            ("Memory limit 512 MiB", 512.0, LIMIT_COLOR),
        )

    draw_panel(
        parts,
        title=title,
        series_label=title,
        grid=grid,
        values=values,
        upload_times=uploads,
        x0=150, y0=155, width=2760, height=380,
        y_max=y_max, y_ticks=y_ticks,
        show_x_ticks=True, scale=.9, title_y=62,
        axis_title=axis_title, legend_x=560,
        show_series_legend=False,
        guides=guides,
        font_scale=1.7,
    )
    if not cpu_panel:
        parts.append(m.text(1530, 622, "Seconds from ingest start", 32, anchor="middle", weight=500))
    parts.append("</svg>")
    return "\n".join(parts)


def save(name: str, svg: str, width: int, height: int) -> tuple[Path, Path]:
    svg_path = CHARTS / f"{name}.svg"
    png_path = CHARTS / f"{name}.png"
    svg_path.write_text(svg)
    rasterize(svg_path, png_path, width, height)
    return svg_path, png_path


def main() -> None:
    # The shared CPU helper aggregates both formal rates before indexing the
    # audience-facing 5k series.
    cpu_runs = [
        cpu.load_run(rate, repeat)
        for rate in cpu.RATES
        for repeat in cpu.REPEATS
    ]
    cpu_data, cpu_stats = cpu.aggregate(cpu_runs)
    memory_runs = [m.load_run(rate, repeat) for rate in m.RATES for repeat in m.REPEATS]
    memory_data = m.aggregate(memory_runs)[RATE]

    formal_generator = SOURCE_GENERATOR.read_text()
    if '"eventType": "TASK_LIFECYCLE_EVENT"' not in formal_generator:
        raise RuntimeError("formal generator no longer emits TASK_LIFECYCLE_EVENT")
    if '"taskId": _b64(' not in formal_generator:
        raise RuntimeError("formal generator no longer assigns a per-event taskId")
    noop = json.loads(SEPARATE_NOOP_EVIDENCE.read_text())
    noop_task_rate = float(noop["lifecycle"]["driver_rate"])
    head_max = max(noop["scaling"]["Head"], key=lambda row: row["events"])
    noop_event_rate = float(head_max["events"])
    if not (2260 <= noop_task_rate <= 2280 and 4880 <= noop_event_rate <= 4910):
        raise RuntimeError("separate no-op evidence drifted")

    long_only = "--long-only" in sys.argv[1:]
    memory_long_only = "--memory-long-only" in sys.argv[1:]
    combined_paths = (CHARTS / f"{COMBINED}.svg", CHARTS / f"{COMBINED}.png")
    cpu_long_paths = (CHARTS / f"{CPU_LONG}.svg", CHARTS / f"{CPU_LONG}.png")
    if memory_long_only:
        if not all(path.exists() for path in combined_paths + cpu_long_paths):
            raise RuntimeError("combined and CPU-long assets must exist before --memory-long-only regeneration")
        outputs = [
            combined_paths,
            cpu_long_paths,
            save(MEMORY_LONG, build_long_panel(cpu_panel=False, cpu_data=cpu_data[RATE], memory_data=memory_data), 3000, 650),
        ]
    elif long_only:
        if not all(path.exists() for path in combined_paths):
            raise RuntimeError("combined audience chart must exist before --long-only regeneration")
        outputs = [
            combined_paths,
            save(CPU_LONG, build_long_panel(cpu_panel=True, cpu_data=cpu_data[RATE], memory_data=memory_data), 3000, 650),
            save(MEMORY_LONG, build_long_panel(cpu_panel=False, cpu_data=cpu_data[RATE], memory_data=memory_data), 3000, 650),
        ]
    else:
        outputs = [
            save(COMBINED, build_combined(cpu_data[RATE], memory_data), 3200, 1800),
            save(CPU_LONG, build_long_panel(cpu_panel=True, cpu_data=cpu_data[RATE], memory_data=memory_data), 3000, 650),
            save(MEMORY_LONG, build_long_panel(cpu_panel=False, cpu_data=cpu_data[RATE], memory_data=memory_data), 3000, 650),
        ]

    contact = CHARTS / f"{COMBINED}-contact-sheet.png"
    subprocess.run([
        "/opt/homebrew/bin/magick", "montage", "-font", str(m.FONT),
        str(outputs[0][1]), str(outputs[1][1]), str(outputs[2][1]),
        "-thumbnail", "980x560", "-tile", "3x1", "-geometry", "980x560+20+20",
        "-background", BG, str(contact),
    ], check=True)

    slide_scale = CHARTS / "11-lifecycle-guides-slide-scale-qa.png"
    slide_scale_content = CHARTS / ".11-lifecycle-guides-slide-scale-content.png"
    subprocess.run([
        "/opt/homebrew/bin/magick", "montage", "-font", str(m.FONT),
        str(outputs[1][1]), str(outputs[2][1]),
        "-thumbnail", "1520x329", "-tile", "1x2", "-geometry", "1520x329+0+18",
        "-background", BG, str(slide_scale_content),
    ], check=True)
    subprocess.run([
        "/opt/homebrew/bin/magick", str(slide_scale_content),
        "-gravity", "center", "-background", BG, "-extent", "1600x900", str(slide_scale),
    ], check=True)
    slide_scale_content.unlink()

    all_svg = "\n".join(svg.read_text() for svg, _ in outputs)
    combined_svg = outputs[0][0].read_text()
    cpu_long_svg = outputs[1][0].read_text()
    memory_long_svg = outputs[2][0].read_text()
    forbidden = (
        "memory.current", "memory.stat.file", "file cache", "anonymous",
        "min–max", "QA", "1,250",
    )
    qa = {
        "source": str(cpu.SOURCE),
        "formal_generator": str(SOURCE_GENERATOR),
        "formal_event_semantics": "one TASK_LIFECYCLE_EVENT with a unique taskId per synthetic event",
        "task_rate_translation": {
            "requested_1250_tasks_per_second_supported": False,
            "reason": "formal r7 does not model four events per task",
            "separate_noop_source": str(SEPARATE_NOOP_EVIDENCE),
            "separate_noop_task_rate_per_second": noop_task_rate,
            "separate_noop_head_peak_ingress_events_per_second": noop_event_rate,
            "label_used": "comparable to ≈2,270 tasks/s in a separate no-op workload",
        },
        "cpu": cpu_stats[str(RATE)],
        "memory": {
            "lifetime_peak_mib": {
                "median": memory_data["peak_median"],
                "min": memory_data["peak_min"],
                "max": memory_data["peak_max"],
            },
            "upload_completion_median_seconds": memory_data["upload_times"],
        },
        "outputs": {
            name: {"svg": str(svg), "png": str(png)}
            for name, (svg, png) in zip((COMBINED, CPU_LONG, MEMORY_LONG), outputs)
        },
        "contact_sheet": str(contact),
        "slide_scale_visual_qa": str(slide_scale),
        "qa": {
            "all_png_nonempty": all(png.stat().st_size > 60_000 for _, png in outputs),
            "all_svg_nonempty": all(svg.stat().st_size > 5_000 for svg, _ in outputs),
            "all_no_nan_or_inf": all("nan" not in svg.read_text().lower() and "inf" not in svg.read_text().lower() for svg, _ in outputs),
            "combined_is_3200x1800": png_dimensions(outputs[0][1]) == (3200, 1800),
            "long_panels_are_3000x650": all(png_dimensions(png) == (3000, 650) for _, png in outputs[1:]),
            "long_panel_aspect_matches_60000x13000": abs((3000 / 650) - (60000 / 13000)) < 1e-12,
            "audience_labels_only": all(value not in all_svg for value in forbidden),
            "phase_labels_present": all(label in all_svg for label, *_ in PHASES),
            "upload_markers_present": all(svg.read_text().count(YELLOW) >= 3 for svg, _ in outputs),
            "corrected_noop_label_present": "comparable to ≈2,270 tasks/s in a separate no-op workload" in outputs[0][0].read_text(),
            "long_panels_have_no_self_legend": cpu_long_svg.count(">CPU</text>") == 1 and memory_long_svg.count(">Memory</text>") == 1,
            "long_panels_keep_upload_legend": all("Upload complete" in svg for svg in (cpu_long_svg, memory_long_svg)),
            "memory_long_has_x_axis_title": "Seconds from ingest start" in memory_long_svg,
            "cpu_long_omits_x_axis_title": "Seconds from ingest start" not in cpu_long_svg,
            "combined_has_no_resource_guides": all(
                label not in combined_svg
                for label in ("CPU request", "CPU limit", "Memory request", "Memory limit")
            ),
            "long_resource_guides_present": all(
                label in cpu_long_svg + memory_long_svg
                for label in (
                    "CPU request 100m",
                    "CPU limit 2000m",
                    "Memory request 128 MiB",
                    "Memory limit 512 MiB",
                )
            ),
            "guide_colors_distinct_from_series": len({REQUEST_COLOR, LIMIT_COLOR, BLUE, YELLOW}) == 4,
            "all_guides_span_full_plot_width": all(
                re.search(
                    rf'<line x1="150\.00" y1="[^"]+" x2="2910\.00" y2="[^"]+" stroke="{re.escape(color)}"',
                    svg,
                ) is not None
                for svg in (cpu_long_svg, memory_long_svg)
                for color in (REQUEST_COLOR, LIMIT_COLOR)
            ),
            "guide_style_survives_slide_scale": all(
                expected in svg
                for svg in (cpu_long_svg, memory_long_svg)
                for expected in (
                    'stroke-width="5.5"',
                    'opacity="1.000"',
                    'stroke-dasharray="30.0 12.0"',
                )
            ),
            "cpu_axis_shows_2000m_limit": ">2000</text>" in cpu_long_svg,
            "memory_axis_shows_512m_limit": ">500</text>" in memory_long_svg and "Memory limit 512 MiB" in memory_long_svg,
            "contact_sheet_nonempty": contact.stat().st_size > 80_000,
            "slide_scale_visual_qa_nonempty": slide_scale.stat().st_size > 80_000,
            "slide_scale_visual_qa_is_1600x900": png_dimensions(slide_scale) == (1600, 900),
        },
    }
    qa["qa"]["all_checks_pass"] = all(qa["qa"].values())
    qa_path = CHARTS / f"{COMBINED}-qa.json"
    qa_path.write_text(json.dumps(qa, indent=2) + "\n")
    for svg, png in outputs:
        print(png)
        print(svg)
    print(contact)
    print(qa_path)


if __name__ == "__main__":
    main()
