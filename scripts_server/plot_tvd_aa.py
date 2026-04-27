"""
TVD-AA analysis: U-coverage evolution and stopping behaviour.
One figure per theta per variant — no overlapping lines.
Run from ~/TVD: .venv/bin/python3 scripts_server/plot_tvd_aa.py
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

sns.set_theme(style="whitegrid", font_scale=1.1)
OUTDIR = Path("results/cordis_all/plots")
OUTDIR.mkdir(exist_ok=True)

df = pd.read_csv("results/cordis_all/steps.csv")
aa = df[df["mode"] == "tvd-aa"].copy()
UR_IDS   = sorted(aa["UR_id"].unique())
THETAS   = sorted(aa["theta"].unique())
COLOR    = "#4C72B0"
COLOR_RND = "#DD8452"
N_SOURCES = 26
ncols = 5
nrows = (len(UR_IDS) + ncols - 1) // ncols  # 4 rows

def savefig(name):
    p = OUTDIR / name
    plt.savefig(p, dpi=150, bbox_inches="tight")
    print(f"  saved {p}")
    plt.close()


# ── Random + Stats Guided together: one figure per theta ─────────────────────
sg  = aa[aa["variant"] == "Stats Guided"].copy()
rnd = aa[aa["variant"] == "Random"].copy()
stop_df = rnd.groupby(["UR_id", "theta", "seed"])["step"].max().reset_index()
stop_df.columns = ["UR_id", "theta", "seed", "stop_step"]

for theta in THETAS:
    fig, axes = plt.subplots(nrows, ncols,
                             figsize=(ncols * 3.0, nrows * 2.5),
                             sharex=False, sharey=True)
    axes = axes.flatten()

    for ax, ur in zip(axes, UR_IDS):
        # Random: mean ± std
        r = rnd[(rnd["UR_id"] == ur) & (rnd["theta"] == theta)]
        if not r.empty:
            agg = r.groupby("step")["ucoverage_current"].agg(["mean", "std"]).reset_index()
            ax.plot(agg["step"], agg["mean"], color=COLOR_RND, lw=2, label="Random")
            ax.fill_between(agg["step"],
                            (agg["mean"] - agg["std"].fillna(0)).clip(0),
                            (agg["mean"] + agg["std"].fillna(0)).clip(1),
                            color=COLOR_RND, alpha=0.2)
            mean_stop = stop_df[(stop_df["UR_id"] == ur) &
                                (stop_df["theta"] == theta)]["stop_step"].mean()
            ax.axvline(x=mean_stop, color=COLOR_RND, lw=1, ls="--", alpha=0.7)

        # Stats Guided: single trajectory
        s = sg[(sg["UR_id"] == ur) & (sg["theta"] == theta)].sort_values("step")
        if not s.empty:
            steps = s["step"].values
            cov   = s["ucoverage_current"].values
            ax.plot(steps, cov, color=COLOR, lw=2, marker="o", ms=4, label="Stats-Guided")
            ax.scatter([steps[-1]], [cov[-1]], color=COLOR,
                       marker="D", s=60, zorder=5, edgecolors="black", linewidths=0.5)
            # horizontal line at SG final coverage for comparison with Random
            ax.axhline(y=cov[-1], color=COLOR, lw=1, ls="--", alpha=0.6)

        ax.set_title(f"UR {ur}", fontsize=9)
        ax.set_xlabel("Sources explored", fontsize=8)
        ax.set_ylim(-0.05, 1.05)
        ax.set_xlim(left=0.5)
        ax.tick_params(labelsize=7)

    for ax in axes[len(UR_IDS):]:
        ax.set_visible(False)

    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower right", fontsize=10,
               title="dashed = Random avg stop\ndiamond = SG stop")
    fig.suptitle(f"TVD-AA — U-Coverage Evolution: Random vs Stats-Guided  (θ={theta})", fontsize=13)
    plt.tight_layout(rect=[0, 0, 1, 0.96])
    savefig(f"aa_ucov_theta{theta}.png")


# ── Stopping step comparison (one bar chart per theta) ───────────────────────
sg_stops  = sg.groupby(["UR_id", "theta"])["step"].max()
rnd_stops = stop_df.groupby(["UR_id", "theta"])["stop_step"].mean()

for theta in THETAS:
    fig, ax = plt.subplots(figsize=(13, 5))
    sg_s  = sg_stops.xs(theta, level="theta")
    rnd_s = rnd_stops.xs(theta, level="theta")
    x = np.arange(len(UR_IDS))
    w = 0.35
    ax.bar(x - w/2, [sg_s.get(ur, 0)  for ur in UR_IDS], w,
           label="Stats-Guided", color=COLOR, alpha=0.85)
    ax.bar(x + w/2, [rnd_s.get(ur, 0) for ur in UR_IDS], w,
           label="Random (avg)", color=COLOR_RND, alpha=0.85)
    ax.axhline(N_SOURCES, color="gray", lw=1, ls="--", label=f"max ({N_SOURCES} sources)")
    ax.set_xticks(x)
    ax.set_xticklabels(UR_IDS, rotation=45, ha="right")
    ax.set_ylabel("Sources explored until stop")
    ax.set_title(f"TVD-AA: Stopping Step — Stats-Guided vs Random  (θ={theta})")
    ax.set_ylim(0, N_SOURCES + 2)
    ax.legend()
    plt.tight_layout()
    savefig(f"aa_stopping_theta{theta}.png")

print(f"\nAll plots saved to {OUTDIR}/")
