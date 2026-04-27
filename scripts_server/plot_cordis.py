"""
Analysis plots for CORDIS experiments (URs 51-70).
Run from ~/TVD: .venv/bin/python3 scripts_server/plot_cordis.py
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import seaborn as sns
from pathlib import Path

sns.set_theme(style="whitegrid", font_scale=1.1)
OUTDIR = Path("results/cordis_all/plots")
OUTDIR.mkdir(exist_ok=True)

df = pd.read_csv("results/cordis_all/summary.csv")

MODE_LABELS  = {"tvd-aa": "TVD-AA", "tvd-caa": "TVD-CAA"}
VAR_LABELS   = {"Random": "Random", "Stats Guided": "Stats-Guided"}
MODE_COLORS  = {"tvd-aa": "#4C72B0", "tvd-caa": "#DD8452"}
VAR_STYLES   = {"Random": "--", "Stats Guided": "-"}
VAR_MARKERS  = {"Random": "o", "Stats Guided": "s"}

# ── helper ────────────────────────────────────────────────────────────────────
def savefig(name):
    p = OUTDIR / name
    plt.savefig(p, dpi=150, bbox_inches="tight")
    print(f"  saved {p}")
    plt.close()


# ── 1. ecoverage vs sources_explored (efficiency scatter) ─────────────────────
print("Plot 1: efficiency scatter")
fig, axes = plt.subplots(1, 2, figsize=(12, 5), sharey=True)
for ax, mode in zip(axes, ["tvd-aa", "tvd-caa"]):
    sub = df[df["mode"] == mode]
    for var, grp in sub.groupby("variant"):
        ax.errorbar(
            grp["sources_explored"], grp["ecoverage_final"],
            xerr=grp["sources_explored_std"], yerr=grp["ecoverage_final_std"],
            fmt=VAR_MARKERS[var], ls="none", alpha=0.7,
            color=MODE_COLORS[mode], mfc="white" if var == "Random" else MODE_COLORS[mode],
            label=VAR_LABELS[var], capsize=3
        )
    ax.set_title(MODE_LABELS[mode], fontsize=13)
    ax.set_xlabel("Sources Explored")
    ax.set_xlim(left=0)
    handles, labels = ax.get_legend_handles_labels()
    by_label = dict(zip(labels, handles))
    ax.legend(by_label.values(), by_label.keys())
axes[0].set_ylabel("E-Coverage")
fig.suptitle("Efficiency: E-Coverage vs Sources Explored (all URs, θ averaged)", y=1.01)
savefig("1_efficiency_scatter.png")


# ── 2. ecoverage & sources_explored by theta ──────────────────────────────────
print("Plot 2: theta effect")
fig, axes = plt.subplots(2, 2, figsize=(13, 8))
metrics = [("ecoverage_final", "E-Coverage"), ("sources_explored", "Sources Explored")]
for col, (metric, ylabel) in enumerate(metrics):
    for row, mode in enumerate(["tvd-aa", "tvd-caa"]):
        ax = axes[row][col]
        sub = df[df["mode"] == mode]
        for var, grp in sub.groupby("variant"):
            agg = grp.groupby("theta")[metric].mean()
            std = grp.groupby("theta")[metric + "_std"].mean()
            ax.plot(agg.index, agg.values, VAR_STYLES[var],
                    marker=VAR_MARKERS[var], color=MODE_COLORS[mode],
                    mfc="white" if var == "Random" else MODE_COLORS[mode],
                    label=VAR_LABELS[var])
            ax.fill_between(agg.index, agg - std, agg + std,
                            alpha=0.15, color=MODE_COLORS[mode])
        ax.set_title(f"{MODE_LABELS[mode]} — {ylabel}")
        ax.set_xlabel("θ")
        ax.set_ylabel(ylabel)
        ax.set_xticks([0.6, 0.8, 1.0])
        ax.legend()
fig.suptitle("Effect of θ on E-Coverage and Sources Explored", fontsize=13)
plt.tight_layout()
savefig("2_theta_effect.png")


# ── 3. per-UR ecoverage heatmap (Stats Guided only) ──────────────────────────
print("Plot 3: per-UR heatmap")
sg = df[df["variant"] == "Stats Guided"].copy()
pivot = sg.pivot_table(index="UR_id", columns=["mode", "theta"],
                       values="ecoverage_final", aggfunc="mean")
pivot.columns = [f"{MODE_LABELS[m]}\nθ={t}" for m, t in pivot.columns]
pivot = pivot.sort_index()

fig, ax = plt.subplots(figsize=(13, 8))
sns.heatmap(pivot, annot=True, fmt=".2f", cmap="YlGn",
            vmin=0, vmax=1, ax=ax, linewidths=0.4,
            cbar_kws={"label": "E-Coverage"})
ax.set_title("E-Coverage per UR — Stats-Guided AM", fontsize=13)
ax.set_xlabel("")
ax.set_ylabel("UR")
plt.tight_layout()
savefig("3_ur_heatmap_ecoverage.png")


# ── 4. AA vs CAA direct comparison (Stats Guided, θ=0.8) ─────────────────────
print("Plot 4: AA vs CAA bar comparison")
sub = df[(df["variant"] == "Stats Guided") & (df["theta"] == 0.8)].copy()
sub["mode_label"] = sub["mode"].map(MODE_LABELS)

fig, axes = plt.subplots(1, 3, figsize=(15, 5))
metrics = [
    ("ecoverage_final", "ecoverage_final_std", "E-Coverage"),
    ("ucoverage_final", "ucoverage_final_std", "U-Coverage"),
    ("sources_explored", "sources_explored_std", "Sources Explored"),
]
for ax, (metric, std_col, label) in zip(axes, metrics):
    pivot = sub.pivot_table(index="UR_id", columns="mode_label",
                            values=metric, aggfunc="mean")
    pivot_std = sub.pivot_table(index="UR_id", columns="mode_label",
                                values=std_col, aggfunc="mean")
    x = np.arange(len(pivot))
    w = 0.35
    for i, (col, color) in enumerate(zip(pivot.columns, ["#4C72B0", "#DD8452"])):
        yerr = pivot_std[col] if col in pivot_std.columns and not pivot_std[col].isna().all() else None
        ax.bar(x + i*w - w/2, pivot[col], w, yerr=yerr,
               label=col, color=color, alpha=0.85, capsize=3)
    ax.set_xticks(x)
    ax.set_xticklabels(pivot.index, rotation=45, ha="right", fontsize=8)
    ax.set_xlabel("UR")
    ax.set_ylabel(label)
    ax.set_title(label)
    ax.legend()
fig.suptitle("TVD-AA vs TVD-CAA — Stats-Guided AM, θ=0.8", fontsize=13)
plt.tight_layout()
savefig("4_aa_vs_caa_bars.png")


# ── 5. Sources explored: Random vs Stats Guided (both modes) ─────────────────
print("Plot 5: exploration efficiency")
agg = df.groupby(["mode", "variant", "theta"])["sources_explored"].mean().reset_index()

fig, axes = plt.subplots(1, 2, figsize=(12, 5), sharey=False)
for ax, mode in zip(axes, ["tvd-aa", "tvd-caa"]):
    sub = agg[agg["mode"] == mode]
    for var, grp in sub.groupby("variant"):
        ax.plot(grp["theta"], grp["sources_explored"],
                VAR_STYLES[var], marker=VAR_MARKERS[var],
                color=MODE_COLORS[mode],
                mfc="white" if var == "Random" else MODE_COLORS[mode],
                label=VAR_LABELS[var], lw=2)
    ax.set_title(MODE_LABELS[mode])
    ax.set_xlabel("θ")
    ax.set_ylabel("Avg Sources Explored (out of 26)")
    ax.set_xticks([0.6, 0.8, 1.0])
    ax.legend()
    ax.set_ylim(bottom=0)
fig.suptitle("Sources Explored: Random vs Stats-Guided", fontsize=13)
plt.tight_layout()
savefig("5_sources_explored.png")


# ── 6. penalty distribution (tvd-caa only) ────────────────────────────────────
print("Plot 6: CAA penalty")
caa = df[df["mode"] == "tvd-caa"].copy()

fig, axes = plt.subplots(1, 2, figsize=(12, 5))
for ax, var in zip(axes, ["Random", "Stats Guided"]):
    sub = caa[caa["variant"] == var]
    pivot = sub.pivot_table(index="UR_id", columns="theta",
                            values="penalty_final", aggfunc="mean")
    pivot.plot(kind="bar", ax=ax, colormap="Blues", width=0.7, legend=True)
    ax.set_title(f"TVD-CAA — {VAR_LABELS[var]}")
    ax.set_xlabel("UR")
    ax.set_ylabel("Penalty")
    ax.set_ylim(0, 1)
    ax.tick_params(axis="x", rotation=45)
    ax.legend(title="θ")
fig.suptitle("Penalty per UR — TVD-CAA mode", fontsize=13)
plt.tight_layout()
savefig("6_caa_penalty.png")

print(f"\nAll plots saved to {OUTDIR}/")
