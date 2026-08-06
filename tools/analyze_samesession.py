#!/usr/bin/env python3
"""Summarize the same-session CPU sweep: median, spread, and whether any two
CPU limits are actually distinguishable given the observed run-to-run variation.

Every cell read byte-identical data, so the only variable left is the limit
itself plus host noise - which is exactly what the spread measures.
"""
import json
import glob
import statistics
import sys

order = {"500m": 0.5, "1": 1.0, "1500m": 1.5, "2": 2.0, "3": 3.0, "4": 4.0, "none": 99.0}
by_cpu = {}
for p in sorted(glob.glob(f"{sys.argv[1]}/*/*/bench-report.json")):
    r = json.load(open(p))
    hs, cfg = r["historyServer"], r["config"]
    if not hs.get("enterMeasured"):
        continue
    cpu = cfg.get("HSCPULimit") or "500m"
    gc = hs.get("gc") or {}
    by_cpu.setdefault(cpu, []).append((hs["enterColdLatency"] * 1e-9, gc.get("finalPercent"), gc.get("gomaxprocs")))

print(f"{'cpu':>6s} {'n':>2s} {'median':>8s} {'min':>7s} {'max':>7s} {'spread':>7s} {'stdev':>6s} {'GC%':>4s} {'P':>2s}")
rows = []
for cpu, vals in sorted(by_cpu.items(), key=lambda kv: order.get(kv[0], 0)):
    times = sorted(v[0] for v in vals)
    med = statistics.median(times)
    sd = statistics.stdev(times) if len(times) > 1 else 0.0
    gc = next((v[1] for v in vals if v[1] is not None), "-")
    P = next((v[2] for v in vals if v[2] is not None), "-")
    rows.append((cpu, med, times[0], times[-1], sd, len(times)))
    print(f"{cpu:>6s} {len(times):2d} {med:7.1f}s {times[0]:6.1f}s {times[-1]:6.1f}s "
          f"{times[-1]-times[0]:6.1f}s {sd:5.1f}s {str(gc):>4s} {str(P):>2s}")

print("\nIs the difference between neighbouring limits larger than the noise?")
for (c1, m1, lo1, hi1, sd1, n1), (c2, m2, lo2, hi2, sd2, n2) in zip(rows, rows[1:]):
    gap = m1 - m2
    noise = max(sd1, sd2)
    verdict = "distinguishable" if abs(gap) > 2 * noise and noise > 0 else "NOT distinguishable"
    print(f"  {c1:>5s} -> {c2:<5s}  median gap {gap:+6.1f}s   worst stdev {noise:5.1f}s   {verdict}")

best = min(rows, key=lambda r: r[1])
print(f"\nfastest median: {best[0]} at {best[1]:.1f}s")
for cpu, med, lo, hi, sd, n in rows:
    if med <= best[1] + 2 * max(sd, best[4]):
        print(f"  {cpu} is within noise of the fastest ({med:.1f}s)")
