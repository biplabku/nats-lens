#!/usr/bin/env python3
"""Generate all documentation diagrams for nats-lens."""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.patheffects as pe
import numpy as np
import os

OUT = "/Users/bidas/Desktop/eb1a/research/nats-lens/docs/images"
os.makedirs(OUT, exist_ok=True)

BG   = "#0d1117"
BLUE = "#1a6696"
CYAN = "#00b4d8"
RED  = "#e63946"
GRN  = "#2dc653"
ORG  = "#f4a261"
GRY  = "#30363d"
TXT  = "#c9d1d9"

# ─────────────────────────────────────────────────────────────────────────────
# Figure 1: Architecture diagram
# ─────────────────────────────────────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(10, 5))
fig.patch.set_facecolor(BG)
ax.set_facecolor(BG)
ax.set_xlim(0, 10); ax.set_ylim(0, 5); ax.axis("off")

def box(ax, x, y, w, h, color, label, sublabel="", fontsize=11):
    r = mpatches.FancyBboxPatch((x, y), w, h,
        boxstyle="round,pad=0.1", facecolor=color, edgecolor="white",
        linewidth=1.2, alpha=0.9)
    ax.add_patch(r)
    ax.text(x+w/2, y+h/2 + (0.15 if sublabel else 0), label,
            ha="center", va="center", color="white",
            fontsize=fontsize, fontweight="bold")
    if sublabel:
        ax.text(x+w/2, y+h/2 - 0.25, sublabel,
                ha="center", va="center", color="#aaa", fontsize=8)

def arrow(ax, x1, y1, x2, y2, color=CYAN, label=""):
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(arrowstyle="->", color=color, lw=1.8))
    if label:
        mx, my = (x1+x2)/2, (y1+y2)/2
        ax.text(mx+0.05, my+0.12, label, color=color, fontsize=7.5, ha="center")

# NATS Server
box(ax, 3.8, 1.8, 2.4, 1.4, "#1a3a5c", "NATS Server", "JetStream enabled")

# Consumers
for i, (lang, clr) in enumerate([("Go", "#00add8"), ("Python", "#3572a5"), ("Rust", "#ce4a21")]):
    bx = 0.1 + i * 1.15
    box(ax, bx, 3.3, 1.05, 0.65, clr, lang, fontsize=9)
    arrow(ax, bx+0.52, 3.3, 4.3+i*0.3, 3.2, color="#555")

# nats-lens
box(ax, 3.8, 0.3, 2.4, 1.1, BLUE, "nats-lens", "read-only observer")
arrow(ax, 5.0, 1.8, 5.0, 1.4, color=CYAN, label="$JS.API.*")

# Output channels
outputs = [
    (7.5, 3.8, ORG,  "Web UI",       ":8080"),
    (7.5, 2.9, GRN,  "Prometheus",   "/metrics"),
    (7.5, 2.0, CYAN, "NATS Events",  "violations.*"),
    (7.5, 1.1, RED,  "REST API",     "/api/streams"),
]
for ox, oy, clr, lbl, sub in outputs:
    box(ax, ox, oy, 1.9, 0.65, clr, lbl, sub, fontsize=9)
    arrow(ax, 6.2, 0.85, ox, oy+0.32, color=clr)

ax.text(5.0, 4.85, "nats-lens Architecture", color=TXT,
        fontsize=14, fontweight="bold", ha="center", va="top")
ax.text(5.0, 4.55, "One observer, zero code changes, any language",
        color="#888", fontsize=10, ha="center")

plt.tight_layout(pad=0.3)
plt.savefig(f"{OUT}/architecture.png", dpi=150, bbox_inches="tight",
            facecolor=BG)
plt.close()
print("✓ architecture.png")

# ─────────────────────────────────────────────────────────────────────────────
# Figure 2: Violation timeline — num_redelivered behaviour
# ─────────────────────────────────────────────────────────────────────────────
fig, axes = plt.subplots(1, 3, figsize=(13, 3.5))
fig.patch.set_facecolor(BG)
t = np.linspace(0, 60, 500)

scenarios = [
    ("ACK_WAIT_VIOLATION",
     "num_redelivered grows\n(new messages exceed ack_wait each cycle)",
     lambda t: np.where(t < 8, 0, np.clip((t-8)*1.8, 0, 30)).astype(float),
     GRN, RED, 8, "Detection\n~8s"),
    ("NAK_STORM",
     "num_redelivered stable & elevated\n(same messages cycling)",
     lambda t: np.where(t < 6, np.clip((t)*1.2, 0, 8),
                        8 + np.random.default_rng(42).normal(0, 0.3, len(t))).astype(float),
     GRN, ORG, 6, "Detection\n~6s"),
    ("SEQUENCE_GAP",
     "first_seq jumps past ack_floor\n(detected in single snapshot)",
     lambda t: np.where(t < 4, 100 + t*0, 100 + np.clip((t-4)*20, 0, 400)).astype(float),
     GRN, CYAN, 4, "Detection\n~3s"),
]

for ax, (name, subtitle, fn, ok_clr, viol_clr, det_t, det_lbl) in zip(axes, scenarios):
    ax.set_facecolor(BG)
    y = fn(t)
    # healthy region
    ax.fill_between(t[t<det_t], y[t<det_t], alpha=0.15, color=ok_clr)
    ax.plot(t[t<det_t], y[t<det_t], color=ok_clr, lw=2)
    # violation region
    ax.fill_between(t[t>=det_t], y[t>=det_t], alpha=0.15, color=viol_clr)
    ax.plot(t[t>=det_t], y[t>=det_t], color=viol_clr, lw=2)
    # detection line
    ax.axvline(det_t+2, color="white", lw=1.2, linestyle="--", alpha=0.7)
    ax.text(det_t+3, ax.get_ylim()[1]*0.85 if ax.get_ylim()[1] > 1 else 0.85,
            det_lbl, color="white", fontsize=8)
    ax.set_title(name, color=TXT, fontsize=10, fontweight="bold", pad=4)
    ax.set_xlabel("Time (s)", color="#888", fontsize=8)
    ax.tick_params(colors="#888", labelsize=7)
    for spine in ax.spines.values():
        spine.set_edgecolor(GRY)
    ax.text(0.5, -0.22, subtitle, transform=ax.transAxes,
            color="#888", fontsize=7.5, ha="center")

axes[0].set_ylabel("num_redelivered", color="#888", fontsize=8)
axes[2].set_ylabel("first_seq − ack_floor", color="#888", fontsize=8)

fig.suptitle("What nats-lens Monitors: Key Metric Signatures",
             color=TXT, fontsize=12, fontweight="bold", y=1.02)
plt.tight_layout(pad=1.2)
plt.savefig(f"{OUT}/violation_timelines.png", dpi=150, bbox_inches="tight",
            facecolor=BG)
plt.close()
print("✓ violation_timelines.png")

# ─────────────────────────────────────────────────────────────────────────────
# Figure 3: Quick-start flow
# ─────────────────────────────────────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(9, 2.2))
fig.patch.set_facecolor(BG)
ax.set_facecolor(BG); ax.axis("off")
ax.set_xlim(0, 9); ax.set_ylim(0, 2.2)

steps = [
    (0.1,  "#1a3a5c",  "1",  "Start NATS",    "docker run nats --jetstream"),
    (2.35, BLUE,       "2",  "Run nats-lens",  "cargo install nats-lens"),
    (4.6,  "#1a5c3a",  "3",  "Open Dashboard", "http://localhost:8080"),
    (6.85, "#5c1a1a",  "4",  "Get Alerts",     "via NATS / Prometheus / UI"),
]
for x, clr, num, title, sub in steps:
    r = mpatches.FancyBboxPatch((x, 0.3), 2.1, 1.5,
        boxstyle="round,pad=0.1", facecolor=clr, edgecolor="white",
        linewidth=1, alpha=0.85)
    ax.add_patch(r)
    ax.text(x+0.25, 1.55, num, color=CYAN, fontsize=16, fontweight="bold")
    ax.text(x+1.05, 1.35, title, color="white", fontsize=9, fontweight="bold", ha="center")
    ax.text(x+1.05, 0.85, sub, color="#aaa", fontsize=7.5, ha="center")
    if x < 6.85:
        ax.annotate("", xy=(x+2.25, 1.05), xytext=(x+2.1, 1.05),
                    arrowprops=dict(arrowstyle="->", color=CYAN, lw=1.5))

ax.text(4.5, 2.1, "Up and running in under 2 minutes",
        color=TXT, fontsize=11, fontweight="bold", ha="center")
plt.tight_layout(pad=0.2)
plt.savefig(f"{OUT}/quickstart.png", dpi=150, bbox_inches="tight", facecolor=BG)
plt.close()
print("✓ quickstart.png")

print(f"\nAll diagrams saved to {OUT}/")
