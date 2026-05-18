"""
Paper-ready AA figure with Prune step: 1 row × 3 cols (MovieLens | CORDIS | MIMIC-IV).
Same as paper_aa_Final but x-axis extended: 0%–100% trajectory + "Prune" point
showing average AA-Coverage after pruning for each variant.

Run from ~/TVD:
  .venv/bin/python3 scripts_server/plot_paper_aa_with_prune.py
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

GRID     = np.linspace(0, 1, 300)
THETA    = 0.8
X_PRUNE  = 1.10   # where the prune dot is drawn
X_SEP    = 1.025  # vertical separator line position
X_MAX    = 1.17   # right edge of axes
OUTDIR   = Path("results/paper_plots/llm_results")
OUTDIR.mkdir(exist_ok=True)

DATASETS = [
    dict(
        name="MovieLens",
        step_files=["results/aa_auto_geo_v2/steps.csv",
                    "results/aa_auto_geo_v3/steps.csv"],
        sum_files =["results/aa_auto_geo_v2/summary.csv",
                    "results/aa_auto_geo_v3/summary.csv"],
        split="geo", lo=201, hi=240,
    ),
    dict(
        name="CORDIS",
        step_files=["results/aa_auto_cordis_v2/steps.csv",
                    "results/aa_auto_cordis_v3/steps.csv"],
        sum_files =["results/aa_auto_cordis_v2/summary.csv",
                    "results/aa_auto_cordis_v3/summary.csv"],
        split="candidates", lo=301, hi=340,
    ),
    dict(
        name="MIMIC-IV",
        step_files=["results/aa_auto_mimic_v3/steps.csv"],
        sum_files =["results/aa_auto_mimic_v3/summary.csv"],
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


def traj(df_var, ur, total, seed=None):
    sub = df_var[df_var["UR_id"] == ur]
    if seed is not None:
        sub = sub[sub["seed"] == seed]
    sub = sub[sub["source_selected"] != "__PRUNE__"].sort_values("step")
    if sub.empty:
        return None, None
    return ff(sub["step"].values, sub["ucoverage_current"].values, total), \
           stop_frac(sub["step"].values, total)


def aggregate(df_var, ur_ids, totals, seed=None):
    trajs, stops = [], []
    for ur in ur_ids:
        total = totals.get(ur)
        if not total:
            continue
        m, s = traj(df_var, ur, total, seed=seed)
        if m is not None:
            trajs.append(m); stops.append(s)
    if not trajs:
        return None, None
    return np.array(trajs).mean(axis=0), np.nanmean(stops)


def prune_val(df_sum, variant, lo, hi, split):
    """Average ucoverage_final over URs for a given variant."""
    sub = df_sum[(df_sum["variant"] == variant)
                 & df_sum["UR_id"].between(lo, hi)
                 & (df_sum["split"] == split)]
    if sub.empty:
        return None
    return sub["ucoverage_final"].mean()


def plot_dataset(ax, ds, sqrt_scale=False):
    # ── trajectory data ──────────────────────────────────────────────────────
    frames = [pd.read_csv(f) for f in ds["step_files"]]
    df_all = pd.concat(frames, ignore_index=True)
    df_all = df_all[(df_all["split"] == ds["split"])
                    & df_all["UR_id"].between(ds["lo"], ds["hi"])
                    & (df_all["theta"] == THETA)]

    totals = get_totals(df_all)
    df_c   = df_all[df_all["source_selected"] != "__PRUNE__"]
    ur_ids = [u for u in range(ds["lo"], ds["hi"] + 1) if totals.get(u, 0) > 0]

    # ── prune summary data ────────────────────────────────────────────────────
    sframes = [pd.read_csv(f) for f in ds["sum_files"]]
    df_sum  = pd.concat(sframes, ignore_index=True)
    df_sum  = df_sum[df_sum["theta"] == THETA]

    def plot_variant(df_var, color, lw, seed=None):
        m, s = aggregate(df_var, ur_ids, totals, seed=seed)
        if m is not None:
            ax.plot(GRID, m, color=color, lw=lw)
            if s is not None and not np.isnan(s):
                ax.axvline(s, color=color, lw=2.0, ls=(0, (4, 2)), alpha=0.95)

    def plot_prune(variant, color, marker_size=10, zorder=5):
        v = prune_val(df_sum, variant, ds["lo"], ds["hi"], ds["split"])
        if v is not None:
            ax.plot(X_PRUNE, v, marker="o", color=color,
                    markersize=marker_size, zorder=zorder, clip_on=False)

    # Random
    rnd   = df_c[df_c["variant"] == "Random"]
    seeds = sorted(rnd["seed"].dropna().unique())
    sc, ss = [], []
    for s in seeds:
        m, ms = aggregate(rnd, ur_ids, totals, seed=s)
        if m is not None:
            sc.append(m); ss.append(ms)
    if sc:
        ax.plot(GRID, np.array(sc).mean(axis=0), color=COLOR_RND, lw=2)
        ax.axvline(np.nanmean(ss), color=COLOR_RND, lw=2.0, ls=(0, (4, 2)), alpha=0.95)
    plot_prune("Random", COLOR_RND)

    # Stats Guided
    plot_variant(df_c[df_c["variant"] == "Stats Guided"], COLOR_SG, 2.5)
    plot_prune("Stats Guided", COLOR_SG, zorder=10)

    # LLM Guided
    plot_variant(df_c[df_c["variant"] == "LLM Guided"], COLOR_LLM_G, 2.5)
    plot_prune("LLM Guided", COLOR_LLM_G)

    # LLM Adaptive
    plot_variant(df_c[df_c["variant"] == "LLM Adaptive"], COLOR_LLM_A, 2.5)
    plot_prune("LLM Adaptive", COLOR_LLM_A)

    # All Source
    plot_variant(df_c[df_c["variant"] == "All Source"], COLOR_ALL, 1.8)
    plot_prune("All Source", COLOR_ALL)

    # ── axis decoration ───────────────────────────────────────────────────────
    ax.axhline(THETA, color="gray", lw=1, ls=":", alpha=0.7)
    ax.axvline(X_SEP, color="gray", lw=1.2, ls="--", alpha=0.4)

    ax.set_xlim(0, X_MAX)
    ax.set_ylim(-0.03, 1.03)

    if sqrt_scale:
        ax.set_xscale('log')
        ax.set_xlim(0.01, X_MAX)
        ticks      = [0.02, 0.05, 0.1, 0.25, 0.5, 1.0, X_PRUNE]
        ticklabels = ["2%", "5%", "10%", "25%", "50%", "100%", "Prune"]
        ax.set_xlabel("Fraction of sources explored (log scale)", fontsize=17)
    else:
        ax.set_xlim(0, X_MAX)
        ticks      = [0, 0.25, 0.5, 0.75, 1.0, X_PRUNE]
        ticklabels = ["0%", "25%", "50%", "75%", "100%", "Prune"]
        ax.set_xlabel("Fraction of sources explored", fontsize=17)
    ax.set_xticks(ticks)
    ax.set_xticklabels(ticklabels, fontsize=13)
    ax.set_title(ds["name"], fontsize=20, fontweight="bold", pad=10)
    ax.annotate("↑ better", xy=(0.98, 0.03), xycoords="axes fraction",
                ha="right", fontsize=14, color="dimgray", style="italic")


# ── figure ────────────────────────────────────────────────────────────────────

handles = [
    Line2D([0],[0], color=COLOR_RND,   lw=2,   label="Random"),
    Line2D([0],[0], color=COLOR_SG,    lw=2.5, label="TVD"),
    Line2D([0],[0], color=COLOR_LLM_G, lw=2.5, label="LLM Guided"),
    Line2D([0],[0], color=COLOR_LLM_A, lw=2.5, label="LLM Adaptive"),
    Line2D([0],[0], color=COLOR_ALL,   lw=1.8, label="All Source"),
    Line2D([0],[0], color="gray",      lw=2.0, ls=(0,(4,2)), label="avg. stop"),
    Line2D([0],[0], color="gray",      lw=0,   marker="o", markersize=8, label="after prune"),
]

for sqrt_scale, suffix in [(False, ""), (True, "_sqrt")]:
    fig, axes = plt.subplots(1, 3, figsize=(20, 4.5), sharey=True)
    for ax, ds in zip(axes, DATASETS):
        plot_dataset(ax, ds, sqrt_scale=sqrt_scale)
    axes[0].set_ylabel("AA-Coverage (avg. over 40 URs)", fontsize=17)
    fig.legend(handles=handles, loc="lower center", ncol=7, fontsize=14,
               bbox_to_anchor=(0.5, -0.04), frameon=True)
    plt.tight_layout()
    out = OUTDIR / f"paper_aa_with_prune{suffix}.pdf"
    plt.savefig(out, dpi=200, bbox_inches="tight")
    plt.savefig(str(out).replace(".pdf", ".png"), dpi=200, bbox_inches="tight")
    print(f"Saved {out}")
    plt.close()
