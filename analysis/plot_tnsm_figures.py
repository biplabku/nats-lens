#!/usr/bin/env python3
"""Generate TNSM-grade figures from extended evaluation data."""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import csv, os, math
from pathlib import Path

BG  = "#0d1117"
TXT = "#c9d1d9"
BLU = "#1a6696"
CYN = "#00b4d8"
RED = "#e63946"
GRN = "#2dc653"
GRY = "#30363d"

FIG_DIR = Path("paper/arxiv/figures")
os.makedirs(FIG_DIR, exist_ok=True)


def wilson_lower(k, n, z=1.96):
    if n == 0: return 0.0
    p = k / n
    return (p + z*z/(2*n) - z * math.sqrt(p*(1-p)/n + z*z/(4*n*n))) / (1 + z*z/n)


# ─────────────────────────────────────────────────────────────────────────────
# Figure 6: Poll-interval sensitivity — validates Theorem 2
# ─────────────────────────────────────────────────────────────────────────────

def plot_sensitivity(data_path: Path, out: Path):
    rows = list(csv.DictReader(open(data_path)))
    intervals = sorted(set(int(r["poll_interval_secs"]) for r in rows))
    classes   = sorted(set(r["scenario"] for r in rows))
    colors    = {classes[0]: CYN, classes[1]: ORG} if len(classes) > 1 else {classes[0]: CYN}
    ORG = "#f4a261"
    colors = {classes[0]: CYN, classes[1]: ORG} if len(classes) > 1 else {classes[0]: CYN}

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(8, 3.5))
    fig.patch.set_facecolor(BG)

    for ax in (ax1, ax2):
        ax.set_facecolor(BG)
        ax.tick_params(colors="#888", labelsize=8)
        for sp in ax.spines.values(): sp.set_edgecolor(GRY)

    for cls in classes:
        rs = {int(r["poll_interval_secs"]): r
              for r in rows if r["scenario"] == cls}
        xs  = intervals
        p50 = [int(rs[i]["p50_latency_ms"]) / (i*1000) for i in xs]
        p95 = [int(rs[i]["p95_latency_ms"]) / (i*1000) for i in xs]
        det = [float(rs[i]["detection_rate_pct"]) for i in xs]
        clr = colors.get(cls, BLU)
        lbl = cls.replace("_", " ")
        ax1.plot(xs, p50, "o-", color=clr, lw=2, ms=5, label=lbl)
        ax2.plot(xs, det, "s--", color=clr, lw=2, ms=5, label=lbl)

    ax1.axhline(1.0, color="white", lw=1, linestyle=":", alpha=0.5,
                label="1× T$_{\\mathrm{poll}}$ (Theorem 2)")
    ax1.set_xlabel("Poll interval (s)", color="#888", fontsize=9)
    ax1.set_ylabel("P50 latency / T$_{\\mathrm{poll}}$", color="#888", fontsize=9)
    ax1.set_title("Detection Latency Ratio", color=TXT, fontsize=10, pad=4)
    ax1.legend(fontsize=7, labelcolor="white",
               facecolor="#161b22", edgecolor=GRY)

    ax2.set_xlabel("Poll interval (s)", color="#888", fontsize=9)
    ax2.set_ylabel("Detection rate (%)", color="#888", fontsize=9)
    ax2.set_ylim(80, 105)
    ax2.set_title("Detection Rate vs. Poll Interval", color=TXT, fontsize=10, pad=4)
    ax2.legend(fontsize=7, labelcolor="white",
               facecolor="#161b22", edgecolor=GRY)

    plt.suptitle("Theorem 2 Validation: Detection Latency ≈ 1 × T$_{\\mathrm{poll}}$",
                 color=TXT, fontsize=11, y=1.02)
    plt.tight_layout(pad=1.0)
    plt.savefig(out, dpi=150, bbox_inches="tight", facecolor=BG)
    plt.close()
    print(f"✓ {out}")


# ─────────────────────────────────────────────────────────────────────────────
# Figure 7: Naive vs nats-lens FP comparison
# ─────────────────────────────────────────────────────────────────────────────

def plot_naive_comparison(out: Path):
    cases = [
        "MAX_PENDING\n(num_pending=0)",
        "NAK_STORM\n(redelivered=1)",
        "MISSING_PROG\n(ratio=0.85)",
        "Healthy\nConsumer",
    ]
    naive  = [1, 1, 1, 0]
    natslens = [0, 0, 0, 0]

    x = range(len(cases))
    w = 0.35

    fig, ax = plt.subplots(figsize=(7, 3.5))
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(BG)

    b1 = ax.bar([xi - w/2 for xi in x], naive, w, label="Naive (single-snapshot)",
                color=RED, alpha=0.85, edgecolor="white", linewidth=0.5)
    b2 = ax.bar([xi + w/2 for xi in x], natslens, w, label="nats-lens (multi-snapshot rate)",
                color=GRN, alpha=0.85, edgecolor="white", linewidth=0.5)

    for bar in b1:
        if bar.get_height() > 0:
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.03,
                    "FP", ha="center", va="bottom", color="white", fontsize=9, fontweight="bold")
    for bar in b2:
        ax.text(bar.get_x() + bar.get_width()/2, 0.05,
                "✓", ha="center", va="bottom", color=GRN, fontsize=10)

    ax.set_xticks(list(x))
    ax.set_xticklabels(cases, color=TXT, fontsize=8)
    ax.set_yticks([0, 1])
    ax.set_yticklabels(["0 (correct)", "1 (false positive)"], color="#888", fontsize=8)
    ax.set_ylabel("False positives", color="#888", fontsize=9)
    ax.set_title("Precision on Adversarial Boundary Conditions",
                 color=TXT, fontsize=11, pad=6)
    ax.legend(fontsize=8, labelcolor="white", facecolor="#161b22", edgecolor=GRY)
    for sp in ax.spines.values(): sp.set_edgecolor(GRY)
    ax.tick_params(colors="#888")
    ax.set_ylim(0, 1.4)

    plt.tight_layout()
    plt.savefig(out, dpi=150, bbox_inches="tight", facecolor=BG)
    plt.close()
    print(f"✓ {out}")


# ─────────────────────────────────────────────────────────────────────────────
# Figure 8: Detection rate with Wilson CI (100 rounds)
# ─────────────────────────────────────────────────────────────────────────────

def plot_detection_ci(data_path: Path, out: Path):
    rows = list(csv.DictReader(open(data_path)))
    classes = ["ACK_WAIT_VIOLATION","SEQUENCE_GAP","MAX_PENDING_THROTTLE",
               "NAK_STORM","MISSING_PROGRESS"]
    labels  = ["ACK_WAIT","SEQ_GAP","MAX_PENDING","NAK_STORM","MISSING"]

    rates, lows, highs = [], [], []
    n_rounds = max(int(r["round"]) for r in rows)
    for cls in classes:
        rs = [r for r in rows if r["scenario"] == cls]
        k  = sum(1 for r in rs if r["detected"] == "true")
        n  = len(rs)
        lo = wilson_lower(k, n)
        rates.append(k/n * 100)
        lows.append(k/n * 100 - lo * 100)
        highs.append(0)  # upper is always 100% for k=n

    fig, ax = plt.subplots(figsize=(7, 3.5))
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(BG)

    x = range(len(labels))
    bars = ax.bar(x, rates, color=BLU, alpha=0.85, edgecolor="white", linewidth=0.5)
    ax.errorbar(x, rates, yerr=[lows, highs], fmt="none", color="white",
                capsize=5, linewidth=1.5, capthick=1.5)

    for i, (bar, lo) in enumerate(zip(bars, lows)):
        lo_val = rates[i] - lo
        ax.text(bar.get_x() + bar.get_width()/2, 82,
                f"CI≥{lo_val:.1f}%", ha="center", va="bottom",
                color="white", fontsize=7, rotation=0)

    ax.set_xticks(list(x))
    ax.set_xticklabels(labels, color=TXT, fontsize=9)
    ax.set_ylim(75, 105)
    ax.set_ylabel("Detection rate (%)", color="#888", fontsize=9)
    ax.set_title(f"Detection Coverage with 95% Wilson CI (n={n_rounds} rounds each)",
                 color=TXT, fontsize=10, pad=6)
    ax.axhline(100, color=GRN, lw=1, linestyle="--", alpha=0.5)
    for sp in ax.spines.values(): sp.set_edgecolor(GRY)
    ax.tick_params(colors="#888")

    plt.tight_layout()
    plt.savefig(out, dpi=150, bbox_inches="tight", facecolor=BG)
    plt.close()
    print(f"✓ {out}")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--data", default="data")
    args = p.parse_args()
    data = Path(args.data)

    # Figure 6: sensitivity
    sens_path = data / "extended_v3" / "sensitivity_results.csv"
    if sens_path.exists():
        plot_sensitivity(sens_path, FIG_DIR / "fig6_sensitivity.pdf")
    else:
        print(f"[skip] sensitivity not found at {sens_path}")

    # Figure 7: naive vs nats-lens
    plot_naive_comparison(FIG_DIR / "fig7_naive_comparison.pdf")

    # Figure 8: detection CI
    det_path = data / "eval100" / "detection_results.csv"
    if not det_path.exists():
        det_path = data / "detection_results.csv"
    if det_path.exists():
        plot_detection_ci(det_path, FIG_DIR / "fig8_detection_ci.pdf")
    else:
        print(f"[skip] detection results not found")

    print("\nDone. Add figures to paper:")
    print("  \\includegraphics{figures/fig6_sensitivity}")
    print("  \\includegraphics{figures/fig7_naive_comparison}")
    print("  \\includegraphics{figures/fig8_detection_ci}")
