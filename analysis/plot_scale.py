#!/usr/bin/env python3
"""Generate scale overhead figure from scale_results.csv."""
import csv, sys, os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

DATA = "data/scale_results.csv"
OUT  = "paper/arxiv/figures/fig5_scale_overhead.pdf"

os.makedirs(os.path.dirname(OUT), exist_ok=True)

rows = []
with open(DATA) as f:
    for r in csv.DictReader(f):
        rows.append({k: float(v) for k, v in r.items()})

n   = [r["n_consumers"] for r in rows]
mem = [r["rss_kb"] / 1024 for r in rows]          # MB
rps = [r["req_per_sec_5s_interval"] for r in rows]

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(7, 3))

# Memory
ax1.plot(n, mem, "o-", color="#1a6696", linewidth=2, markersize=5)
ax1.set_xlabel("Active consumers")
ax1.set_ylabel("RSS memory (MB)")
ax1.set_title("Memory vs. Consumer Count")
ax1.xaxis.set_major_formatter(mticker.FuncFormatter(
    lambda x, _: f"{int(x):,}"))
ax1.yaxis.set_major_formatter(mticker.FuncFormatter(
    lambda x, _: f"{x:.1f}"))
ax1.grid(True, alpha=0.3)

# API request rate
ax2.plot(n, rps, "s--", color="#c0392b", linewidth=2, markersize=5)
ax2.set_xlabel("Active consumers")
ax2.set_ylabel("API requests/s (5 s interval)")
ax2.set_title("Poll Overhead vs. Consumer Count")
ax2.xaxis.set_major_formatter(mticker.FuncFormatter(
    lambda x, _: f"{int(x):,}"))
ax2.grid(True, alpha=0.3)

# Annotations at key points
for row in rows:
    if int(row["n_consumers"]) in (50, 200, 1000):
        ax1.annotate(f"{row['rss_kb']/1024:.1f}",
                     (row["n_consumers"], row["rss_kb"]/1024),
                     textcoords="offset points", xytext=(4, 4), fontsize=7)
        ax2.annotate(f"{row['req_per_sec_5s_interval']:.0f}",
                     (row["n_consumers"], row["req_per_sec_5s_interval"]),
                     textcoords="offset points", xytext=(4, 4), fontsize=7)

plt.tight_layout()
plt.savefig(OUT, bbox_inches="tight")
print(f"Saved → {OUT}")
