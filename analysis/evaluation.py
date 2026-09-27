#!/usr/bin/env python3
"""
Paper evaluation analysis for nats-lens.

Reads CSV files produced by nats-lens-eval and generates four figures
for the paper's evaluation section.

Usage:
    python analysis/evaluation.py --data data/ --out paper/figures/

Requires: pip install matplotlib numpy pandas
"""

import argparse
import os
import sys
from pathlib import Path

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    import numpy as np
    import pandas as pd
except ImportError:
    print("Install deps: pip install matplotlib numpy pandas")
    sys.exit(1)

# ── Style constants ───────────────────────────────────────────────────────────

BG       = "#0f172a"
PANEL_BG = "#1e293b"
TEXT     = "#e2e8f0"
GRID     = "#334155"

COLORS = {
    "ACK_WAIT_VIOLATION":  "#ef4444",
    "SEQUENCE_GAP":        "#f97316",
    "MAX_PENDING_THROTTLE":"#eab308",
    "NAK_STORM":           "#a855f7",
    "MISSING_PROGRESS":    "#3b82f6",
    "baseline":            "#64748b",
    "nats_lens":           "#22c55e",
}

SCENARIO_LABELS = {
    "ACK_WAIT_VIOLATION":  "Ack-Wait\nViolation",
    "SEQUENCE_GAP":        "Sequence\nGap",
    "MAX_PENDING_THROTTLE":"Max-Pending\nThrottle",
    "NAK_STORM":           "NAK\nStorm",
    "MISSING_PROGRESS":    "Missing\nProgress",
}

def set_dark_style(ax):
    ax.set_facecolor(PANEL_BG)
    ax.tick_params(colors=TEXT, labelsize=9)
    ax.xaxis.label.set_color(TEXT)
    ax.yaxis.label.set_color(TEXT)
    ax.title.set_color(TEXT)
    for spine in ax.spines.values():
        spine.set_edgecolor(GRID)
    ax.grid(color=GRID, linewidth=0.5, alpha=0.7)

# ── Figure 1: Detection Coverage ─────────────────────────────────────────────

def fig_coverage(df: pd.DataFrame, outdir: str):
    """
    Side-by-side grouped bar chart comparing nats-lens detection coverage
    against a standard Prometheus NATS exporter (baseline = 0% for all types).

    This is Table 1 of the paper rendered as a figure.
    """
    scenarios  = list(SCENARIO_LABELS.keys())
    rounds     = df["round"].max()
    coverage   = []

    for s in scenarios:
        sub = df[df["scenario"] == s]
        pct = sub["detected"].sum() / len(sub) * 100 if len(sub) > 0 else 0.0
        coverage.append(pct)

    x      = np.arange(len(scenarios))
    width  = 0.35

    fig, ax = plt.subplots(figsize=(9, 5), facecolor=BG)
    set_dark_style(ax)

    bars_baseline = ax.bar(x - width/2, [0]*len(scenarios), width,
                           label="Prometheus NATS exporter (baseline)",
                           color=COLORS["baseline"], alpha=0.8)
    bars_lens     = ax.bar(x + width/2, coverage, width,
                           label="nats-lens",
                           color=[COLORS[s] for s in scenarios], alpha=0.9)

    # Value labels on nats-lens bars
    for bar, pct in zip(bars_lens, coverage):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1.5,
                f"{pct:.0f}%", ha="center", va="bottom",
                color=TEXT, fontsize=9, fontweight="bold")

    # Baseline label
    ax.text(-0.15, 3, "0%\n(none)", ha="center", va="bottom",
            color=COLORS["baseline"], fontsize=8)

    ax.set_xticks(x)
    ax.set_xticklabels([SCENARIO_LABELS[s] for s in scenarios], color=TEXT, fontsize=9)
    ax.set_ylim(0, 115)
    ax.set_ylabel("Detection Rate (%)", color=TEXT)
    ax.set_title(f"Figure 1: Violation Detection Coverage\n"
                 f"nats-lens vs. standard Prometheus NATS metrics (n={rounds} rounds each)",
                 color=TEXT, fontsize=11)

    legend = ax.legend(facecolor=PANEL_BG, edgecolor=GRID, labelcolor=TEXT, fontsize=9)

    plt.tight_layout()
    out = Path(outdir) / "fig1_detection_coverage.pdf"
    plt.savefig(out, facecolor=BG, bbox_inches="tight", dpi=150)
    print(f"  Saved {out}")
    plt.close()

# ── Figure 2: Detection Latency CDF ──────────────────────────────────────────

def fig_latency_cdf(df: pd.DataFrame, outdir: str, poll_interval_secs: float = 3.0):
    """
    CDF of detection latency (ms) per violation type.
    Vertical dashed lines show 1× and 2× poll interval for reference.
    """
    detected = df[df["detected"] == True].copy()
    if detected.empty:
        print("  No detected violations — skipping latency CDF")
        return

    fig, ax = plt.subplots(figsize=(8, 5), facecolor=BG)
    set_dark_style(ax)

    for scenario in SCENARIO_LABELS:
        sub = detected[detected["scenario"] == scenario]["detection_latency_ms"].dropna()
        if sub.empty:
            continue
        vals = np.sort(sub.values)
        cdf  = np.arange(1, len(vals) + 1) / len(vals)
        ax.plot(vals, cdf, label=SCENARIO_LABELS[scenario].replace("\n", " "),
                color=COLORS[scenario], linewidth=2)

    # Reference lines for poll interval
    poll_ms = poll_interval_secs * 1000
    ax.axvline(poll_ms,     color=GRID, linestyle="--", linewidth=1, alpha=0.8)
    ax.axvline(poll_ms * 2, color=GRID, linestyle=":",  linewidth=1, alpha=0.8)
    ax.text(poll_ms     + 50, 0.05, "1× poll", color=TEXT, fontsize=7)
    ax.text(poll_ms * 2 + 50, 0.05, "2× poll", color=TEXT, fontsize=7)

    ax.set_xlabel("Detection Latency (ms)", color=TEXT)
    ax.set_ylabel("CDF", color=TEXT)
    ax.set_ylim(0, 1.05)
    ax.set_title("Figure 2: Detection Latency CDF by Violation Type\n"
                 "(time from violation injection to first nats-lens event)",
                 color=TEXT, fontsize=11)
    ax.legend(facecolor=PANEL_BG, edgecolor=GRID, labelcolor=TEXT, fontsize=8,
              loc="lower right")

    plt.tight_layout()
    out = Path(outdir) / "fig2_detection_latency_cdf.pdf"
    plt.savefig(out, facecolor=BG, bbox_inches="tight", dpi=150)
    print(f"  Saved {out}")
    plt.close()

# ── Figure 3: False Positive Rate ────────────────────────────────────────────

def fig_false_positives(fp: pd.DataFrame, outdir: str):
    """
    Cumulative false violation count over time during healthy operation.
    A flat line at zero is the ideal result.
    """
    if fp.empty:
        print("  No false positive data — skipping FP figure")
        return

    fig, ax = plt.subplots(figsize=(8, 4), facecolor=BG)
    set_dark_style(ax)

    ax.step(fp["elapsed_secs"], fp["violations_count"],
            where="post", color=COLORS["nats_lens"], linewidth=2)
    ax.fill_between(fp["elapsed_secs"], fp["violations_count"],
                    step="post", alpha=0.15, color=COLORS["nats_lens"])

    total = fp["violations_count"].iloc[-1] if not fp.empty else 0
    duration = fp["elapsed_secs"].max() if not fp.empty else 0
    fp_per_min = total / (duration / 60) if duration > 0 else 0

    ax.set_xlabel("Time (seconds)", color=TEXT)
    ax.set_ylabel("Cumulative False Violations", color=TEXT)
    ax.set_ylim(bottom=0)
    ax.set_title(
        f"Figure 3: False Positive Rate Under Healthy Operation\n"
        f"Total: {total} false violations over {duration}s "
        f"({fp_per_min:.2f}/min)",
        color=TEXT, fontsize=11,
    )

    plt.tight_layout()
    out = Path(outdir) / "fig3_false_positive_rate.pdf"
    plt.savefig(out, facecolor=BG, bbox_inches="tight", dpi=150)
    print(f"  Saved {out}")
    plt.close()

# ── Figure 4: Latency Boxplots ────────────────────────────────────────────────

def fig_latency_boxplots(df: pd.DataFrame, outdir: str):
    """
    Box plots of detection latency per violation type —
    shows spread, median, and outliers side by side.
    """
    detected = df[df["detected"] == True].copy()
    if detected.empty:
        print("  No detected violations — skipping boxplots")
        return

    scenarios = [s for s in SCENARIO_LABELS if not detected[detected["scenario"] == s].empty]
    data      = [detected[detected["scenario"] == s]["detection_latency_ms"].dropna().values
                 for s in scenarios]

    fig, ax = plt.subplots(figsize=(9, 5), facecolor=BG)
    set_dark_style(ax)

    bp = ax.boxplot(
        data,
        patch_artist=True,
        medianprops=dict(color="white", linewidth=2),
        whiskerprops=dict(color=TEXT),
        capprops=dict(color=TEXT),
        flierprops=dict(marker="o", color=TEXT, markersize=3, alpha=0.5),
    )
    for patch, scenario in zip(bp["boxes"], scenarios):
        patch.set_facecolor(COLORS[scenario])
        patch.set_alpha(0.75)

    ax.set_xticks(range(1, len(scenarios) + 1))
    ax.set_xticklabels([SCENARIO_LABELS[s].replace("\n", " ") for s in scenarios],
                       color=TEXT, fontsize=9)
    ax.set_ylabel("Detection Latency (ms)", color=TEXT)
    ax.set_title("Figure 4: Detection Latency Distribution per Violation Type",
                 color=TEXT, fontsize=11)

    plt.tight_layout()
    out = Path(outdir) / "fig4_latency_boxplots.pdf"
    plt.savefig(out, facecolor=BG, bbox_inches="tight", dpi=150)
    print(f"  Saved {out}")
    plt.close()

# ── Summary statistics table ─────────────────────────────────────────────────

def print_stats_table(df: pd.DataFrame, fp: pd.DataFrame):
    print()
    print("  DETECTION STATISTICS")
    print(f"  {'Scenario':<25} {'Coverage':>10} {'P50 ms':>10} {'P95 ms':>10} {'P99 ms':>10}")
    print(f"  {'─'*25} {'─'*10} {'─'*10} {'─'*10} {'─'*10}")

    for scenario in SCENARIO_LABELS:
        sub = df[df["scenario"] == scenario]
        if sub.empty:
            continue
        detected_sub = sub[sub["detected"] == True]["detection_latency_ms"].dropna()
        coverage = sub["detected"].sum() / len(sub) * 100

        if detected_sub.empty:
            p50, p95, p99 = "N/A", "N/A", "N/A"
        else:
            p50 = f"{np.percentile(detected_sub, 50):.0f}"
            p95 = f"{np.percentile(detected_sub, 95):.0f}"
            p99 = f"{np.percentile(detected_sub, 99):.0f}"

        print(f"  {scenario:<25} {coverage:>9.1f}% {p50:>10} {p95:>10} {p99:>10}")

    print()
    if not fp.empty:
        total_fp   = fp["violations_count"].iloc[-1]
        duration_s = fp["elapsed_secs"].max()
        print(f"  False positives: {total_fp} in {duration_s}s "
              f"({total_fp / (duration_s/60):.2f}/min)")
    print()

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default="data",  help="Directory with CSV files")
    parser.add_argument("--out",  default="paper/figures", help="Output directory for figures")
    parser.add_argument("--poll-interval", type=float, default=3.0,
                        help="Poll interval used during evaluation (seconds)")
    args = parser.parse_args()

    os.makedirs(args.out, exist_ok=True)

    # Load CSVs
    detection_csv = Path(args.data) / "detection_results.csv"
    fp_csv        = Path(args.data) / "false_positive_results.csv"

    if not detection_csv.exists():
        print(f"ERROR: {detection_csv} not found.")
        print("Run: cargo run -p nats-lens-eval -- --nats nats://localhost:4222")
        sys.exit(1)

    df = pd.read_csv(detection_csv)
    fp = pd.read_csv(fp_csv) if fp_csv.exists() else pd.DataFrame()

    print(f"\n  Loaded {len(df)} detection rows, {len(fp)} FP rows")
    print(f"  Generating figures → {args.out}/\n")

    os.makedirs("paper/figures", exist_ok=True)

    fig_coverage(df, args.out)
    fig_latency_cdf(df, args.out, poll_interval_secs=args.poll_interval)
    fig_false_positives(fp, args.out)
    fig_latency_boxplots(df, args.out)
    print_stats_table(df, fp)

    print(f"  All figures saved to {args.out}/")
    print()

if __name__ == "__main__":
    main()
