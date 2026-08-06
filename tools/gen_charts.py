#!/usr/bin/env python3
"""Render the benchmark charts as SVG, one light and one dark variant each.

    python3 tools/gen_charts.py results/derived.csv docs/img

GitHub picks the variant with <picture media="(prefers-color-scheme: dark)">.
Stdlib only — no plotting dependency, so the charts regenerate anywhere.
"""
import csv
import html
import pathlib
import sys

W, H = 720, 380
PAD_L, PAD_R, PAD_T, PAD_B = 74, 24, 78, 58

THEME = {
    "light": {
        "surface": "#fcfcfb", "ink": "#0b0b0b", "ink2": "#52514e", "muted": "#898781",
        "grid": "#e1e0d9", "axis": "#c3c2b7",
        "s1": "#2a78d6", "s2": "#eb6834", "s3": "#1baf7a",
    },
    "dark": {
        "surface": "#1a1a19", "ink": "#ffffff", "ink2": "#c3c2b7", "muted": "#898781",
        "grid": "#2c2c2a", "axis": "#383835",
        "s1": "#3987e5", "s2": "#d95926", "s3": "#199e70",
    },
}

# Single quotes inside the attribute: nested double quotes make invalid XML.
FONT = "system-ui,-apple-system,'Segoe UI',sans-serif"


class Chart:
    """A single plot area with a linear y scale and categorical x slots."""

    def __init__(self, t, title, subtitle, ymax, ylabel, ticks=5):
        self.t, self.title, self.subtitle = t, title, subtitle
        self.ymax, self.ylabel, self.ticks = ymax or 1, ylabel, ticks
        self.parts = []

    def y(self, v):
        return H - PAD_B - (v / self.ymax) * (H - PAD_T - PAD_B)

    # --- log-log mode, for scaling curves spanning several orders of magnitude ---

    def logframe(self, xdecades, ydecades, xfmt, yfmt):
        """xdecades/ydecades are (min_exp, max_exp) powers of ten."""
        import math
        t = self.t
        self.xd, self.yd = xdecades, ydecades
        out = [f'<rect width="{W}" height="{H}" fill="{t["surface"]}" rx="8"/>',
               f'<text x="{PAD_L - 46}" y="26" font-family="{FONT}" font-size="16" '
               f'font-weight="600" fill="{t["ink"]}">{html.escape(self.title)}</text>',
               f'<text x="{PAD_L - 46}" y="44" font-family="{FONT}" font-size="12" '
               f'fill="{t["muted"]}">{html.escape(self.subtitle)}</text>',
               f'<text x="{PAD_L - 46}" y="66" font-family="{FONT}" font-size="11" '
               f'fill="{t["muted"]}">{html.escape(self.ylabel)}</text>']
        for e in range(ydecades[0], ydecades[1] + 1):
            y = self.ly(10.0 ** e)
            out.append(f'<line x1="{PAD_L}" y1="{y:.1f}" x2="{W - PAD_R}" y2="{y:.1f}" '
                       f'stroke="{t["grid"]}" stroke-width="1"/>')
            out.append(f'<text x="{PAD_L - 8}" y="{y + 4:.1f}" text-anchor="end" font-family="{FONT}" '
                       f'font-size="11" fill="{t["muted"]}">{yfmt(10.0 ** e)}</text>')
        for e in range(xdecades[0], xdecades[1] + 1):
            x = self.lx(10.0 ** e)
            anchor = "end" if e == xdecades[1] else ("start" if e == xdecades[0] else "middle")
            out.append(f'<text x="{x:.1f}" y="{H - PAD_B + 20:.1f}" text-anchor="{anchor}" '
                       f'font-family="{FONT}" font-size="12" fill="{t["ink2"]}">{xfmt(10.0 ** e)}</text>')
        out.append(f'<line x1="{PAD_L}" y1="{H - PAD_B}" x2="{W - PAD_R}" y2="{H - PAD_B}" '
                   f'stroke="{t["axis"]}" stroke-width="1"/>')
        self.parts = out

    def lx(self, v):
        import math
        lo, hi = self.xd
        return PAD_L + (math.log10(v) - lo) / (hi - lo) * (W - PAD_L - PAD_R)

    def ly(self, v):
        import math
        lo, hi = self.yd
        return H - PAD_B - (math.log10(v) - lo) / (hi - lo) * (H - PAD_T - PAD_B)

    def line(self, pts, color, labels=None):
        d = " ".join(f"{'M' if i == 0 else 'L'}{self.lx(x):.1f},{self.ly(y):.1f}"
                     for i, (x, y) in enumerate(pts))
        self.parts.append(f'<path d="{d}" fill="none" stroke="{color}" stroke-width="2" '
                          f'stroke-linejoin="round"/>')
        for i, (x, y) in enumerate(pts):
            self.parts.append(f'<circle cx="{self.lx(x):.1f}" cy="{self.ly(y):.1f}" r="4.5" '
                              f'fill="{color}" stroke="{self.t["surface"]}" stroke-width="2"/>')
            if labels and labels[i]:
                self.parts.append(f'<text x="{self.lx(x):.1f}" y="{self.ly(y) - 12:.1f}" '
                                  f'text-anchor="middle" font-family="{FONT}" font-size="11" '
                                  f'fill="{self.t["ink"]}">{html.escape(labels[i])}</text>')

    def frame(self, xlabels):
        t = self.t
        out = [
            f'<rect width="{W}" height="{H}" fill="{t["surface"]}" rx="8"/>',
            f'<text x="{PAD_L - 46}" y="26" font-family="{FONT}" font-size="16" '
            f'font-weight="600" fill="{t["ink"]}">{html.escape(self.title)}</text>',
            f'<text x="{PAD_L - 46}" y="44" font-family="{FONT}" font-size="12" '
            f'fill="{t["muted"]}">{html.escape(self.subtitle)}</text>',
        ]
        for i in range(self.ticks + 1):
            v = self.ymax * i / self.ticks
            y = self.y(v)
            out.append(f'<line x1="{PAD_L}" y1="{y:.1f}" x2="{W - PAD_R}" y2="{y:.1f}" '
                       f'stroke="{t["grid"]}" stroke-width="1"/>')
            out.append(f'<text x="{PAD_L - 8}" y="{y + 4:.1f}" text-anchor="end" font-family="{FONT}" '
                       f'font-size="11" fill="{t["muted"]}" font-variant-numeric="tabular-nums">{fmt(v)}</text>')
        out.append(f'<line x1="{PAD_L}" y1="{H - PAD_B}" x2="{W - PAD_R}" y2="{H - PAD_B}" '
                   f'stroke="{t["axis"]}" stroke-width="1"/>')
        out.append(f'<text x="{PAD_L - 46}" y="66" font-family="{FONT}" font-size="11" '
                   f'fill="{t["muted"]}">{html.escape(self.ylabel)}</text>')
        slot = (W - PAD_L - PAD_R) / len(xlabels)
        for i, lab in enumerate(xlabels):
            x = PAD_L + slot * (i + 0.5)
            out.append(f'<text x="{x:.1f}" y="{H - PAD_B + 20:.1f}" text-anchor="middle" '
                       f'font-family="{FONT}" font-size="12" fill="{t["ink2"]}">{html.escape(lab)}</text>')
        self.parts = out
        return slot

    def xticks(self, values, vmax, caption):
        t = self.t
        for v in values:
            x = PAD_L + (v / vmax) * (W - PAD_L - PAD_R)
            self.parts.append(f'<text x="{x:.1f}" y="{H - PAD_B + 20:.1f}" text-anchor="middle" '
                              f'font-family="{FONT}" font-size="12" fill="{t["ink2"]}" '
                              f'font-variant-numeric="tabular-nums">{fmt(v)}</text>')
        self.parts.append(f'<text x="{W - PAD_R}" y="{H - PAD_B + 38:.1f}" text-anchor="end" '
                          f'font-family="{FONT}" font-size="11" fill="{t["muted"]}">'
                          f'{html.escape(caption)}</text>')

    def legend(self, entries):
        x = PAD_L
        for label, color in entries:
            self.parts.append(f'<rect x="{x}" y="{H - 22}" width="10" height="10" rx="2" fill="{color}"/>')
            self.parts.append(f'<text x="{x + 15}" y="{H - 13}" font-family="{FONT}" font-size="11" '
                              f'fill="{self.t["ink2"]}">{html.escape(label)}</text>')
            x += 22 + 6.6 * len(label)

    def bars(self, slot, series, values_by_series, labels_by_series=None):
        """Grouped bars: series = [(name, color)], values aligned to x slots."""
        t = self.t
        n = len(series)
        # 2px surface gap between adjacent fills, per the mark spec.
        bw = min(46, (slot - 22) / n - 2)
        for si, (name, color) in enumerate(series):
            for i, v in enumerate(values_by_series[si]):
                if v is None:
                    continue
                cx = PAD_L + slot * (i + 0.5)
                x = cx - (n * (bw + 2) - 2) / 2 + si * (bw + 2)
                y = self.y(v)
                h = max(0.0, H - PAD_B - y)
                self.parts.append(
                    f'<path d="M{x:.1f},{H - PAD_B} L{x:.1f},{y + 4:.1f} '
                    f'Q{x:.1f},{y:.1f} {x + 4:.1f},{y:.1f} L{x + bw - 4:.1f},{y:.1f} '
                    f'Q{x + bw:.1f},{y:.1f} {x + bw:.1f},{y + 4:.1f} L{x + bw:.1f},{H - PAD_B} Z" '
                    f'fill="{color}"/>' if h > 6 else
                    f'<rect x="{x:.1f}" y="{y:.1f}" width="{bw:.1f}" height="{h:.1f}" fill="{color}"/>')
                labs = labels_by_series[si] if labels_by_series else None
                lab = labs[i] if labs else None
                if lab:
                    self.parts.append(
                        f'<text x="{x + bw / 2:.1f}" y="{y - 6:.1f}" text-anchor="middle" '
                        f'font-family="{FONT}" font-size="11" fill="{t["ink"]}" '
                        f'font-variant-numeric="tabular-nums">{html.escape(lab)}</text>')

    def dots(self, xs, ys, color, labels=None):
        """Scatter in data space; xs already scaled to 0..1 of the plot width."""
        t = self.t
        for i, (xf, v) in enumerate(zip(xs, ys)):
            x = PAD_L + xf * (W - PAD_L - PAD_R)
            y = self.y(v)
            self.parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="5" fill="{color}" '
                              f'stroke="{t["surface"]}" stroke-width="2"/>')
            if labels and labels[i]:
                self.parts.append(f'<text x="{x:.1f}" y="{y - 12:.1f}" text-anchor="middle" '
                                  f'font-family="{FONT}" font-size="10" fill="{t["muted"]}">'
                                  f'{html.escape(labels[i])}</text>')

    def hband(self, lo, hi, color, label):
        y1, y2 = self.y(hi), self.y(lo)
        self.parts.append(f'<rect x="{PAD_L}" y="{y1:.1f}" width="{W - PAD_L - PAD_R}" '
                          f'height="{y2 - y1:.1f}" fill="{color}" opacity="0.13"/>')
        self.parts.append(f'<text x="{W - PAD_R - 6}" y="{y1 - 6:.1f}" text-anchor="end" '
                          f'font-family="{FONT}" font-size="11" fill="{self.t["ink2"]}">'
                          f'{html.escape(label)}</text>')

    def note(self, text, y=None):
        self.parts.append(f'<text x="{W - PAD_R}" y="{y or PAD_T - 14}" text-anchor="end" '
                          f'font-family="{FONT}" font-size="11" fill="{self.t["muted"]}">'
                          f'{html.escape(text)}</text>')

    def svg(self):
        return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" '
                f'height="{H}" role="img">' + "".join(self.parts) + "</svg>\n")


def fmt(v):
    if v == 0:
        return "0"
    if v >= 1000:
        return f"{v:,.0f}"
    if v >= 10:
        return f"{v:.0f}"
    if v >= 1:
        return f"{v:.1f}"
    return f"{v:.2f}"


def load(path):
    with open(path) as f:
        rows = [r for r in csv.DictReader(f)]
    for r in rows:
        for k, v in r.items():
            if k in ("run", "gzip", "collector_log_capture"):
                continue
            try:
                r[k] = float(v) if v not in ("", None) else None
            except ValueError:
                pass
    return rows


def pick(rows, name):
    return next((r for r in rows if r["run"] == name), None)


def build(mode, rows, outdir):
    t = THEME[mode]
    A = [pick(rows, n) for n in ("rerun-A-n1000", "A-n5000", "A-n10000", "A-n50000", "A-n100000")]
    C = [pick(rows, n) for n in ("C-gzip-n1000", "C-gzip-n5000", "C-gzip-n10000", "C-gzip-n50000", "C-gzip-n100000")]
    xlabels = ["1k", "5k", "10k", "50k", "100k"]

    # 1. storage: total bytes stored, raw vs gzip
    raw = [r["raw_mib"] for r in C]          # raw is identical either way; C rows carry both
    gz = [r["stored_mib"] for r in C]
    ch = Chart(t, "Storage per session", "event data written to object storage, by session size", 320, "MiB stored")
    slot = ch.frame(xlabels)
    ch.bars(slot, [("no compression", t["s1"]), ("gzip", t["s2"])], [raw, gz],
            [[f"{v:.0f}" for v in raw], [f"{v:.1f}" for v in gz]])
    ch.legend([("no compression", t["s1"]), ("gzip", t["s2"])])
    ch.note("gzip ratio 0.091 at every size")
    write(ch, outdir, "storage", mode)

    # 2. per-task storage constant
    ch = Chart(t, "Storage cost per task", "constant across three orders of magnitude", 4, "KiB per task")
    slot = ch.frame(xlabels)
    rawpt = [r["raw_b_per_task"] / 1024 for r in C]
    gzpt = [r["stored_b_per_task"] / 1024 for r in C]
    ch.bars(slot, [("raw", t["s1"]), ("gzip", t["s2"])], [rawpt, gzpt],
            [[f"{v:.2f}" for v in rawpt], [f"{v:.2f}" for v in gzpt]])
    ch.legend([("raw", t["s1"]), ("gzip", t["s2"])])
    ch.note("3.15 KiB -> 0.29 KiB  (91% saved)")
    write(ch, outdir, "storage-per-task", mode)

    # 3. The scaling curve. Log-log because the range spans three orders of
    # magnitude. The 500m series stops at 50k on purpose: at 100k that
    # configuration never returned a response, so there is no latency to plot.
    N6 = [1000, 5000, 10000, 20000, 50000, 100000]
    s500 = [2.28, 9.83, 19.86, 37.60, 97.85]
    s4 = [0.69, 3.17, 6.25, 14.45, 30.52, 62.67]
    ch = Chart(t, "History Server cold load vs session size",
               "GET /enter_cluster, log-log: a straight line means cost scales linearly with tasks",
               1, "seconds (log scale)")
    ch.logframe((3, 5), (-1, 3),
                lambda v: {1e3: "1k", 1e4: "10k", 1e5: "100k tasks"}[v],
                lambda v: {0.1: "0.1", 1.0: "1", 10.0: "10", 100.0: "100", 1000.0: "1,000"}[v])
    ch.line(list(zip(N6[:5], s500)), t["s2"],
            [None, None, None, None, "97.8s"])
    ch.line(list(zip(N6, s4)), t["s1"],
            [None, None, None, None, "30.5s", "62.7s"])
    ch.legend([("500m (shipped)", t["s2"]), ("4 cores", t["s1"])])
    ch.note("500m stops at 50k: at 100k it never returned a response", y=H - 13)
    write(ch, outdir, "hs-load", mode)

    # 4. The headline: 100k, where the gap stops being a gap.
    ch = Chart(t, "How much CPU the History Server actually needs",
               "100k-task session; only resources.limits.cpu changed", 80, "seconds", 4)
    slot = ch.frame(["500m", "1", "1.5", "2", "3", "4", "8", "none"])
    vals = [None, 71.2, 67.2, 64.4, 64.7, 63.8, 66.3, 66.8]
    ch.bars(slot, [("cold load", t["s1"])], [vals],
            [[None, "71.2s", "67.2s", "64.4s", "64.7s", "63.8s", "66.3s", "66.8s"]])
    # 500m has no bar because that configuration never returned a response.
    for i, word in enumerate(("never", "completed")):
        ch.parts.append(f'<text x="{PAD_L + slot * 0.5:.1f}" y="{H - PAD_B - 22 + i * 14:.1f}" '
                        f'text-anchor="middle" font-family="{FONT}" font-size="11" '
                        f'fill="{t["s2"]}">{word}</text>')
    ch.note("the plateau starts at 2 cores; the load only wants ~1.2")
    write(ch, outdir, "hs-cpu-limit", mode)

    # 5. Per-task cost across the whole axis: the knee only exists under the quota.
    labels6 = ["1k", "5k", "10k", "20k", "50k", "100k"]
    ch = Chart(t, "Cold load per task", "flat means the cost scales linearly with task count", 3,
               "milliseconds per task", 6)
    slot = ch.frame(labels6)
    ch.bars(slot, [("500m (shipped)", t["s2"]), ("4 cores", t["s1"])],
            [[2.28, 1.97, 1.99, 1.88, 1.96, None],
             [0.69, 0.63, 0.63, 0.72, 0.61, 0.63]],
            [["2.3", "2.0", "2.0", "1.9", "2.0", None],
             ["0.69", "0.63", "0.63", "0.72", "0.61", "0.63"]])
    ch.legend([("500m (shipped)", t["s2"]), ("4 cores", t["s1"])])
    ch.note("no 100k bar for 500m: that load never completed")
    write(ch, outdir, "hs-load-knee", mode)

    # 4. HS memory
    mem = [r["hs_peak_mib"] for r in A]
    mem4 = [pick(rows, f"hscpu4-n{n}")["hs_peak_mib"] for n in (1000, 5000, 10000, 50000, 100000)]
    ch = Chart(t, "History Server memory", "peak container heap while one session is loaded", 2400, "MiB")
    slot = ch.frame(xlabels)
    ch.bars(slot, [("500m", t["s2"]), ("4 cores", t["s1"])], [mem, mem4],
            [[f"{v:.0f}" for v in mem], [f"{v:.0f}" for v in mem4]])
    ch.legend([("500m", t["s2"]), ("4 cores", t["s1"])])
    ch.note("~23 KiB per task, retained in the snapshot cache")
    write(ch, outdir, "hs-memory", mode)

    # 5. collector CPU per event vs node event rate
    pts = sorted((r["node_peak_events_per_s"], r["collector_us_per_event"])
                 for r in rows if r["node_peak_events_per_s"] and r["collector_us_per_event"])
    ch = Chart(t, "Collector CPU per event", "every run, 1k to 100k tasks and 200 to 7,850 events/s", 200, "microseconds of CPU per event", 4)
    ch.frame([""] * 5)
    ch.xticks([0, 2000, 4000, 6000, 8000], 8000, "events per second on one Ray node")
    ch.hband(115, 131, t["s3"], "measured band 115-131 us")
    xs = [p[0] / 8000 for p in pts]
    ch.dots(xs, [p[1] for p in pts], t["s1"])
    ch.note("flat: cost is per event, not per session")
    write(ch, outdir, "collector-cpu", mode)

    # 6. collector memory flatness
    cmem = [r["collector_worker_peak_mib"] for r in A]
    hmem = [r["collector_head_peak_mib"] for r in A]
    ch = Chart(t, "Collector memory", "sidecar heap does not grow with session size", 200, "MiB (anon)")
    slot = ch.frame(xlabels)
    ch.bars(slot, [("worker sidecar", t["s1"]), ("head sidecar", t["s2"])], [cmem, hmem],
            [[f"{v:.0f}" for v in cmem], [f"{v:.0f}" for v in hmem]])
    ch.legend([("worker sidecar", t["s1"]), ("head sidecar", t["s2"])])
    ch.note("streaming design: events go to disk, not to the heap")
    write(ch, outdir, "collector-memory", mode)

    # 7. when data reaches storage
    during = [0, 0, 142.9]
    flush = [30.8, 153.3, 159.9]
    ch = Chart(t, "When events reach object storage", "5-minute rotation means short jobs upload only at shutdown", 320, "MiB uploaded")
    slot = ch.frame(["10k tasks", "50k tasks", "100k tasks"])
    ch.bars(slot, [("during the job", t["s1"]), ("during shutdown flush", t["s2"])], [during, flush],
            [[f"{v:.0f}" for v in during], [f"{v:.0f}" for v in flush]])
    ch.legend([("during the job", t["s1"]), ("during shutdown flush", t["s2"])])
    ch.note("anything in orange is lost if the pod is SIGKILLed")
    write(ch, outdir, "flush-split", mode)

    # 8. axis B: concurrency does not raise the event rate
    B = [pick(rows, n) for n in ("B-cpus0.5", "B-cpus0.2", "B-cpus0.1", "B-cpus0.05")]
    ch = Chart(t, "Task concurrency vs event rate", "20k tasks, varying num_cpus per task", 6000, "per second", 4)
    slot = ch.frame(["0.5 (4/node)", "0.2 (10/node)", "0.1 (20/node)", "0.05 (40/node)"])
    ch.bars(slot, [("driver tasks/s", t["s1"]), ("peak node events/s", t["s2"])],
            [[r["driver_tps"] for r in B], [r["node_peak_events_per_s"] for r in B]],
            [[f'{r["driver_tps"]:.0f}' for r in B], [f'{r["node_peak_events_per_s"]:.0f}' for r in B]])
    ch.legend([("driver tasks/s", t["s1"]), ("peak node events/s", t["s2"])])
    ch.note("more concurrency made it slower, not faster")
    write(ch, outdir, "rate-vs-concurrency", mode)


def write(ch, outdir, name, mode):
    path = pathlib.Path(outdir) / f"{name}-{mode}.svg"
    path.write_text(ch.svg())


def main():
    src = sys.argv[1] if len(sys.argv) > 1 else "results/derived.csv"
    outdir = sys.argv[2] if len(sys.argv) > 2 else "docs/img"
    pathlib.Path(outdir).mkdir(parents=True, exist_ok=True)
    rows = load(src)
    for mode in ("light", "dark"):
        build(mode, rows, outdir)
    print(f"wrote charts to {outdir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
