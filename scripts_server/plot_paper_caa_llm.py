"""
Paper-ready CAA figure: 2 rows × 3 cols.
  Row 1: AV-Coverage evolution per dataset
  Row 2: Penalty evolution per dataset
Column titles = dataset name. No figure title.

Run from ~/TVD:
  .venv/bin/python3 scripts_server/plot_paper_caa_llm.py
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.lines import Line2D
import seaborn as sns
from pathlib import Path

sns.set_theme(style="whitegrid", font_scale=2.0)
plt.rcParams.update({"axes.titlesize": 20, "axes.labelsize": 17,
                     "xtick.labelsize": 15, "ytick.labelsize": 15})

COLOR_SG    = "#4C72B0"
COLOR_RND   = "#DD8452"
COLOR_ALL   = "#2ca02c"
COLOR_LLM_G = "#9467bd"
COLOR_LLM_A = "#e377c2"

GRID   = np.linspace(0, 1, 300)
EPS    = 0.05
OUTDIR = Path("results/paper_plots/llm_results")
OUTDIR.mkdir(exist_ok=True)

DATASETS = [
    dict(
        name="MovieLens",
        files=["results/caa_auto_geo_v3/steps.csv"],
        split="geo", lo=201, hi=240,
    ),
    dict(
        name="CORDIS",
        files=["results/caa_auto_cordis_v3/steps.csv"],
        split="candidates", lo=301, hi=340,
    ),
    dict(
        name="MIMIC-IV",
        files=["results/caa_auto_mimic_v3/steps.csv"],
        split="admissions", lo=401, hi=440,
    ),
]

# ── helpers ───────────────────────────────────────────────────────────────────

def get_totals(df):
    als = df[(df["variant"] == "All Source") & (df["source_selected"] != "__PRUNE__")]
    return als.groupby("UR_id")["sources_explored"].max().to_dict()


def ff(steps, values, total):
    if len(steps) == 0 or total <= 0:
        return np.zeros_like(GRID)
    norm = steps / total
    out, idx = np.zeros_like(GRID), 0
    for i, g in enumerate(GRID):
        while idx + 1 < len(norm) and norm[idx + 1] <= g:
            idx += 1
        out[i] = values[idx]
    return out


def stop_frac(steps, total):
    return steps[-1] / total if total > 0 and len(steps) > 0 else np.nan


def traj(df_var, ur, total, metric, seed=None):
    sub = df_var[df_var["UR_id"] == ur]
    if seed is not None:
        sub = sub[sub["seed"] == seed]
    sub = sub[sub["source_selected"] != "__PRUNE__"].sort_values("step")
    if sub.empty:
        return None, None
    return ff(sub["step"].values, sub[metric].values, total), \
           stop_frac(sub["step"].values, total)


def aggregate(df_var, ur_ids, totals, metric, seed=None):
    trajs, stops = [], []
    for ur in ur_ids:
        total = totals.get(ur)
        if not total:
            continue
        m, s = traj(df_var, ur, total, metric, seed=seed)
        if m is not None:
            trajs.append(m); stops.append(s)
    if not trajs:
        return None, None
    return np.array(trajs).mean(axis=0), np.nanmean(stops)


def plot_col(ax_top, ax_bot, ds, sqrt_scale=False):
    frames = [pd.read_csv(f) for f in ds["files"]]
    df_all = pd.concat(frames, ignore_index=True)
    df_all = df_all[(df_all["split"] == ds["split"])
                    & df_all["UR_id"].between(ds["lo"], ds["hi"])
                    & (df_all["eps"] == EPS)]

    totals = get_totals(df_all)
    df_c   = df_all[df_all["source_selected"] != "__PRUNE__"]
    ur_ids = [u for u in range(ds["lo"], ds["hi"] + 1) if totals.get(u, 0) > 0]

    for ax, metric, ylabel, better in [
        (ax_top, "ecoverage_current", "AV-Coverage", "↑ better"),
        (ax_bot, "penalty_current",   "Penalty",     "↑ better"),
    ]:
        # Random
        rnd   = df_c[df_c["variant"] == "Random"]
        seeds = sorted(rnd["seed"].dropna().unique())
        sc, ss = [], []
        for s in seeds:
            m, ms = aggregate(rnd, ur_ids, totals, metric, seed=s)
            if m is not None:
                sc.append(m); ss.append(ms)
        if sc:
            ax.plot(GRID, np.array(sc).mean(axis=0), color=COLOR_RND, lw=2)
            ax.axvline(np.nanmean(ss), color=COLOR_RND, lw=2.0, ls=(0,(4,2)), alpha=0.95)

        # Stats Guided
        sg = df_c[df_c["variant"] == "Stats Guided"]
        m, s = aggregate(sg, ur_ids, totals, metric)
        if m is not None:
            ax.plot(GRID, m, color=COLOR_SG, lw=2.5)
            ax.axvline(s, color=COLOR_SG, lw=2.0, ls=(0,(4,2)), alpha=0.95)

        # LLM Guided
        llmg = df_c[df_c["variant"] == "LLM Guided"]
        m, s = aggregate(llmg, ur_ids, totals, metric)
        if m is not None:
            ax.plot(GRID, m, color=COLOR_LLM_G, lw=2.5)
            ax.axvline(s, color=COLOR_LLM_G, lw=2.0, ls=(0,(4,2)), alpha=0.95)

        # LLM Adaptive
        llma = df_c[df_c["variant"] == "LLM Adaptive"]
        m, s = aggregate(llma, ur_ids, totals, metric)
        if m is not None:
            ax.plot(GRID, m, color=COLOR_LLM_A, lw=2.5)
            ax.axvline(s, color=COLOR_LLM_A, lw=2.0, ls=(0,(4,2)), alpha=0.95)

        # All Source
        als = df_c[df_c["variant"] == "All Source"]
        m, _ = aggregate(als, ur_ids, totals, metric)
        if m is not None:
            ax.plot(GRID, m, color=COLOR_ALL, lw=1.8)

        ax.set_xlim(0, 1); ax.set_ylim(-0.03, 1.03)
        if sqrt_scale:
            ax.set_xscale('log')
            ax.set_xlim(0.01, 1)
            ax.set_xticks([0.02, 0.05, 0.1, 0.25, 0.5, 1.0])
            ax.set_xticklabels(["2%", "5%", "10%", "25%", "50%", "100%"], fontsize=13)
        else:
            ax.set_xlim(0, 1)
            ax.xaxis.set_major_formatter(mticker.PercentFormatter(xmax=1, decimals=0))
        ax.set_ylabel(f"{ylabel} (avg. over 40 URs)", fontsize=17)
        ax.annotate(better, xy=(0.98, 0.03), xycoords="axes fraction",
                    ha="right", fontsize=14, color="dimgray", style="italic")

    xlabel = "Fraction of sources explored (log scale)" if sqrt_scale else "Fraction of sources explored"
    ax_bot.set_xlabel(xlabel, fontsize=17)
    ax_top.set_title(ds["name"], fontsize=20, fontweight="bold", pad=10)


# ── figure ────────────────────────────────────────────────────────────────────

handles = [
    Line2D([0],[0], color=COLOR_RND,   lw=2,   label="Random"),
    Line2D([0],[0], color=COLOR_SG,    lw=2.5, label="TVD"),
    Line2D([0],[0], color=COLOR_LLM_G, lw=2.5, label="LLM Guided"),
    Line2D([0],[0], color=COLOR_LLM_A, lw=2.5, label="LLM Adaptive"),
    Line2D([0],[0], color=COLOR_ALL,   lw=1.8, label="All Source"),
    Line2D([0],[0], color="gray",      lw=2.0, ls=(0,(4,2)), label="avg. stop"),
]

for sqrt_scale, suffix in [(False, ""), (True, "_sqrt")]:
    fig, axes = plt.subplots(2, 3, figsize=(18, 8.0), sharey="row")
    for col, ds in enumerate(DATASETS):
        plot_col(axes[0, col], axes[1, col], ds, sqrt_scale=sqrt_scale)
    for row in range(2):
        for col in range(1, 3):
            axes[row, col].set_ylabel("")
    fig.legend(handles=handles, loc="lower center", ncol=6, fontsize=14,
               bbox_to_anchor=(0.5, -0.03), frameon=True)
    plt.tight_layout()
    out = OUTDIR / f"paper_caa_Final{suffix}.pdf"
    plt.savefig(out, dpi=200, bbox_inches="tight")
    plt.savefig(str(out).replace(".pdf", ".png"), dpi=200, bbox_inches="tight")
    print(f"Saved {out}")
    plt.close()
