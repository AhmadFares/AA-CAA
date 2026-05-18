# TVD: Tuple-Value Discovery

Code for the paper **"Tuple-Value Discovery"** (TVD-AA and TVD-CAA algorithms).

---

## Requirements

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## Data Setup

Experiments use three datasets. Download instructions for each are below.
Place all raw files under `data/raw/` as described.

### MovieLens

1. Download **MovieLens 1M** from [https://grouplens.org/datasets/movielens/1m/](https://grouplens.org/datasets/movielens/1m/)
2. Extract `ratings.dat`, `users.dat`, `movies.dat` into `data/raw/Movie_Lens/`
3. Build the geo split:
   ```bash
   python scripts_server/generate_movielens_geo_split.py
   ```
   This creates `data/generated_splits/MovieLens/geo/` (60 sources, split by zip-code region).

### CORDIS

1. Download the H2020 projects CSV from [https://cordis.europa.eu/datalab/](https://cordis.europa.eu/datalab/)
   - Look for **H2020 projects** → export as CSV
2. Pre-split by coordinator country and place files as `data/raw/cordis/src_1.csv` … `src_26.csv`
   (use `data/raw/cordis/country_index.csv` in this repo for the country-to-source mapping)
3. Build the candidates split:
   ```bash
   python scripts_server/build_cordis_splits.py
   ```
   This creates `data/generated_splits/CORDIS/candidates/` (26 sources, one per country).

### MIMIC-IV

Access requires credentialing via PhysioNet:
[https://physionet.org/content/mimiciv/](https://physionet.org/content/mimiciv/)

Once you have access:
1. Download `admissions.csv` and `diagnoses_icd.csv` and place them under `data/mimic_iv/`
2. Build the split:
   ```bash
   python scripts_server/generate_mimic_split.py
   ```
   This creates `data/generated_splits/MIMIC/admissions/` (90 sources, split by admission location × decade).

---

## Running Experiments

All paper experiments (TVD-AA and TVD-CAA on all three datasets) are run with:

```bash
bash scripts_server/run_all_experiments.sh
```

This runs the non-LLM variants (TVD, Random, All Source) by default.
To also run the LLM-guided variants, provide a Groq API key:

```bash
LLM_API_KEY=gsk_... bash scripts_server/run_all_experiments.sh
```

Results are written to `results/` as `steps.csv` and `summary.csv` per run.

**UR ranges:** MovieLens geo: 201–240 · CORDIS: 301–340 · MIMIC-IV: 401–440

---

## Reproducing Figures

All figure scripts are under `scripts_server/` and read from `results/`.
Run from the repo root:

```bash
# AA trajectory figure (Figure X in the paper)
python scripts_server/plot_paper_aa_with_prune.py

# CAA trajectory figure
python scripts_server/plot_paper_caa_with_prune.py

# Timing comparison figure
python scripts_server/plot_timing_comparison.py

# LLM variants — AA
python scripts_server/plot_paper_aa_llm.py

# LLM variants — CAA
python scripts_server/plot_paper_caa_llm.py
```

Output PDFs and PNGs are saved to `results/paper_plots/`.

---

## Repository Structure

```
SQL_Variants/
  methods/AttributeMatch.py   # TVD-AA and TVD-CAA algorithms
  scripts/run_experiments.py  # experiment entry point
  scripts/generate_splits.py  # split loading utilities

helpers/
  test_cases.py               # UR definitions (URs 201–440)
  auto_ur_generator.py        # automatic UR generation from split statistics
  llm_selector.py             # LLM-guided source selection

scripts_server/
  run_all_experiments.sh      # run all paper experiments
  generate_*_split.py         # dataset-specific split builders
  rebuild_split_stats.py      # recompute source statistics
  plot_paper_*.py             # figure scripts
  plot_timing_comparison.py   # timing figure
```
