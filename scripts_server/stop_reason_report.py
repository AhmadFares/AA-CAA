"""
Deduce and report stop reason for every TVD-AA run from steps.csv.
Run from ~/TVD: .venv/bin/python3 scripts_server/stop_reason_report.py
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
N_SOURCES = 26
EPS = 1e-9  # floating point tolerance

# ── Deduce stop reason per run ────────────────────────────────────────────────
# For Stats Guided seed is NaN; group key differs
last = (aa.sort_values("step")
          .groupby(["UR_id", "theta", "variant", "seed"], dropna=False)
          .last()
          .reset_index())

def stop_reason(row):
    cov = row["ucoverage_current"]
    src = row["sources_explored"]
    theta = row["theta"]
    if cov >= theta - EPS:
        return "coverage_met"
    elif src < N_SOURCES:
        return "zero_gain"      # only Stats-Guided can reach here
    else:
        return "exhausted"

last["stop_reason"] = last.apply(stop_reason, axis=1)

# ── Print summary table ───────────────────────────────────────────────────────
print("=" * 60)
print("STOP REASON SUMMARY — TVD-AA")
print("=" * 60)

summary = (last.groupby(["variant", "theta", "stop_reason"])
               .size()
               .reset_index(name="count"))
pivot = summary.pivot_table(index=["variant", "theta"],
                            columns="stop_reason", values="count",
                            fill_value=0)
print(pivot.to_string())
print()

# Per-UR breakdown
print("=" * 60)
print("STOP REASON PER UR (Stats Guided, counts across thetas)")
print("=" * 60)
sg_last = last[last["variant"] == "Stats Guided"]
ur_pivot = sg_last.groupby(["UR_id", "stop_reason"]).size().unstack(fill_value=0)
print(ur_pivot.to_string())
print()

print("=" * 60)
print("STOP REASON PER UR (Random, avg seeds per theta)")
print("=" * 60)
rnd_last = last[last["variant"] == "Random"]
rnd_pivot = (rnd_last.groupby(["UR_id", "theta", "stop_reason"])
                     .size()
                     .reset_index(name="count")
                     .pivot_table(index="UR_id", columns=["theta", "stop_reason"],
                                  values="count", fill_value=0))
print(rnd_pivot.to_string())

# ── Save CSV ──────────────────────────────────────────────────────────────────
out_path = Path("results/cordis_all/stop_reasons.csv")
last[["UR_id", "theta", "variant", "seed", "sources_explored",
      "ucoverage_current", "stop_reason"]].to_csv(out_path, index=False)
print(f"\nDetailed stop reasons saved to {out_path}")

# ── Plot: stop reason breakdown per variant × theta ───────────────────────────
REASON_COLORS = {
    "coverage_met": "#2ca02c",
    "zero_gain":    "#1f77b4",
    "exhausted":    "#d62728",
}

fig, axes = plt.subplots(1, 2, figsize=(13, 5))
for ax, variant in zip(axes, ["Stats Guided", "Random"]):
    sub = last[last["variant"] == variant]
    counts = (sub.groupby(["theta", "stop_reason"])
                 .size()
                 .reset_index(name="count"))
    thetas = sorted(counts["theta"].unique())
    reasons = ["coverage_met", "zero_gain", "exhausted"]
    x = np.arange(len(thetas))
    w = 0.25
    for i, reason in enumerate(reasons):
        vals = [counts.loc[(counts["theta"] == t) &
                           (counts["stop_reason"] == reason), "count"].sum()
                for t in thetas]
        ax.bar(x + i*w - w, vals, w, label=reason,
               color=REASON_COLORS[reason], alpha=0.85)
    ax.set_xticks(x)
    ax.set_xticklabels([f"θ={t}" for t in thetas])
    ax.set_title(variant)
    ax.set_ylabel("Number of runs")
    ax.legend()
fig.suptitle("TVD-AA: Stop Reason per Variant × θ", fontsize=13)
plt.tight_layout()
p = OUTDIR / "aa_stop_reasons.png"
plt.savefig(p, dpi=150, bbox_inches="tight")
print(f"Plot saved to {p}")
plt.close()

# ── Plot: per-UR stop reason heatmap (Stats Guided) ───────────────────────────
sg_heat = (sg_last.groupby(["UR_id", "theta", "stop_reason"])
                  .size()
                  .reset_index(name="count"))
# encode: coverage_met=2, zero_gain=1, exhausted=0
reason_code = {"coverage_met": 2, "zero_gain": 1, "exhausted": 0}
# pick dominant reason per UR/theta
dominant = (sg_heat.sort_values("count", ascending=False)
                   .groupby(["UR_id", "theta"])
                   .first()
                   .reset_index())
dominant["code"] = dominant["stop_reason"].map(reason_code)
heat = dominant.pivot(index="UR_id", columns="theta", values="stop_reason")
heat.columns = [f"θ={t}" for t in heat.columns]

fig, ax = plt.subplots(figsize=(7, 9))
# map to numeric for color
code_heat = heat.replace(reason_code)
cmap = plt.cm.get_cmap("RdYlGn", 3)
im = ax.imshow(code_heat.values.astype(float), cmap=cmap, vmin=-0.5, vmax=2.5, aspect="auto")
for i in range(len(heat)):
    for j in range(len(heat.columns)):
        ax.text(j, i, heat.iloc[i, j].replace("_", "\n"),
                ha="center", va="center", fontsize=7.5)
ax.set_xticks(range(len(heat.columns)))
ax.set_xticklabels(heat.columns)
ax.set_yticks(range(len(heat.index)))
ax.set_yticklabels(heat.index)
ax.set_title("TVD-AA — Stats-Guided: Stop Reason per UR × θ\n"
             "green=coverage_met  yellow=zero_gain  red=exhausted", fontsize=11)
plt.tight_layout()
p = OUTDIR / "aa_sg_stop_heatmap.png"
plt.savefig(p, dpi=150, bbox_inches="tight")
print(f"Plot saved to {p}")
plt.close()
