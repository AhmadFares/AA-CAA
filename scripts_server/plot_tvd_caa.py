"""
TVD-CAA analysis: penalty evolution.
Run from ~/TVD: .venv/bin/python3 scripts_server/plot_tvd_caa.py
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

sns.set_theme(style="whitegrid", font_scale=1.1)
OUTDIR = Path("results/cordis_all/plots")
OUTDIR.mkdir(exist_ok=True)

df  = pd.read_csv("results/cordis_all/steps.csv")
caa = df[df["mode"] == "tvd-caa"].copy()

sg  = caa[caa["variant"] == "Stats Guided"].copy()
rnd = caa[caa["variant"] == "Random"].copy()

UR_IDS = sorted(caa["UR_id"].unique())
THETAS = sorted(caa["theta"].unique())
COLOR_SG  = "#4C72B0"
COLOR_RND = "#DD8452"
N_SOURCES = 26
ncols = 5
nrows = (len(UR_IDS) + ncols - 1) // ncols

def savefig(name):
    p = OUTDIR / name
    plt.savefig(p, dpi=150, bbox_inches="tight")
    print(f"  saved {p}")
    plt.close()

# stop step per Random run (last non-prune step)
rnd_notprune = rnd[rnd["source_selected"] != "__PRUNE__"]
stop_df = (rnd_notprune.groupby(["UR_id", "theta", "seed"])["step"]
                       .max().reset_index()
                       .rename(columns={"step": "stop_step"}))

for theta in THETAS:
    fig, axes = plt.subplots(nrows, ncols,
                             figsize=(ncols * 3.0, nrows * 2.5),
                             sharex=False, sharey=True)
    axes = axes.flatten()

    for ax, ur in zip(axes, UR_IDS):
        # Random: mean ± std including prune step
        r = rnd[(rnd["UR_id"] == ur) & (rnd["theta"] == theta)]
        if not r.empty:
            agg = r.groupby("step")["penalty_current"].agg(["mean", "std"]).reset_index()
            ax.plot(agg["step"], agg["mean"], color=COLOR_RND, lw=2, label="Random")
            ax.fill_between(agg["step"],
                            (agg["mean"] - agg["std"].fillna(0)).clip(0),
                            (agg["mean"] + agg["std"].fillna(0)).clip(1),
                            color=COLOR_RND, alpha=0.2)
            mean_stop = stop_df[(stop_df["UR_id"] == ur) &
                                (stop_df["theta"] == theta)]["stop_step"].mean()
            ax.axvline(x=mean_stop, color=COLOR_RND, lw=1, ls="--", alpha=0.7)
            # mark mean prune point with diamond
            r_prune = r[r["source_selected"] == "__PRUNE__"]
            if not r_prune.empty:
                mean_prune_step = r_prune["step"].mean()
                mean_prune_pen  = r_prune["penalty_current"].mean()
                ax.scatter([mean_prune_step], [mean_prune_pen],
                           color=COLOR_RND, marker="D", s=60, zorder=5,
                           edgecolors="black", linewidths=0.5)

        # Stats-Guided: full trajectory including prune step (connected)
        s_all = sg[(sg["UR_id"] == ur) & (sg["theta"] == theta)].sort_values("step")
        if not s_all.empty:
            ax.plot(s_all["step"], s_all["penalty_current"],
                    color=COLOR_SG, lw=2, marker="o", ms=4, label="Stats-Guided")
            s_prune = s_all[s_all["source_selected"] == "__PRUNE__"]
            if not s_prune.empty:
                ax.scatter(s_prune["step"], s_prune["penalty_current"],
                           color=COLOR_SG, marker="D", s=60, zorder=5,
                           edgecolors="black", linewidths=0.5)
                ax.axhline(y=s_prune["penalty_current"].values[0],
                           color=COLOR_SG, lw=1, ls="--", alpha=0.6)

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
    fig.suptitle(f"TVD-CAA — Penalty Evolution: Random vs Stats-Guided  (θ={theta})", fontsize=13)
    plt.tight_layout(rect=[0, 0, 1, 0.96])
    savefig(f"caa_penalty_theta{theta}.png")

print(f"\nAll plots saved to {OUTDIR}/")
