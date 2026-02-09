# TVD

## Codebase Overview

This repository contains the **experimental and algorithmic code** for running
**Tuple-Value Discovery (TVD)** methods and evaluating them under different source
construction settings.

The code is organized around:

* TVD algorithms,
* SQL-based query strategies,
* source construction utilities,
* experiment runners.

---

## Repository Structure

### Core logic

* `sql_builders.py`  
  SQL query construction utilities (fixed and rewritten queries).

* `utils.py`  
  Shared helpers: UR parsing, coverage, penalty, pruning utilities, logging helpers.

* `Source_Constructors.py`  
  Functions to generate synthetic source tables (random, skewed, high-penalty,
  low-coverage, etc.).

---

### SQL_Variants/

Main implementation of TVD variants and execution pipeline.

* `SQL_Variants/core/`  
  Core components shared by all methods (SQL builders, utilities, execution helpers).

* `SQL_Variants/methods/`  
  TVD methods (e.g., AttributeMatch, TupleMatch).

* `SQL_Variants/scripts/`  
  Entry-point scripts for running experiments and sanity checks.

---

### Scripts (root level)

* `generate_splits.py`  
  Generates source tables from a base dataset (MovieLens-1M).  
  **Must be run first.**

* `generate_splits_tus.py`  
  Generates source tables from a base dataset (TUS).  
  **Must be run first.**
  
* `Check1.py`  
  Runs a **single UR** against generated sources (debug / sanity check).
  The UR, dataset, and execution parameters are configured in `GENERAL_CONFIG`.

* `run_experiments.py`  
  Runs **full experimental sweeps** (multiple URs, splits, configurations).
  All experiment settings (datasets, URs, variants, thresholds, modes) are defined
  in `GENERAL_CONFIG`.

---

## How to Run

## Data setup (TUS)

This repository uses a processed subset of the **TUS-small dataset** for experiments.

We select 20 queries from TUS and reduce them to user requests (URs), resulting in
**UR21–UR40** (see `helpers/test_cases`).  
For each UR, only candidate tables whose schema fully contains the UR attributes are
retained, leading to a variable number of candidates per UR.

The resulting candidate tables are provided as a GitHub Release asset.

### Download and extract candidates

From the repository root:

```
tar -xzf tus_candidates_UR21-UR40.tar.gz
````

This creates:

```
data/tus_20_selected/candidates/UR21 … UR40
```

### Generate experimental source splits

```
PYTHONPATH=$PWD python SQL_Variants/scripts/generate_splits_tus.py
```

For each UR, this produces the following folders under
`data/generated_splits/UR{ur}/`:

```
candidates/    # unchanged sources
low_penalty/   # low-penalty variants
high_penalty/  # high-penalty variants
low_coverage/  # low-coverage variants
```

---

## Data setup (MovieLens)

We use a **preprocessed version of MovieLens-1M**, where the original MovieLens tables
(`ratings`, `users`, `movies`) are joined into a **single relational table**.

The resulting table contains the following columns:

```
UserID, MovieID, Rating, Timestamp,
Gender, Age, Occupation, Zip-code,
Title, Genres
```

This unified table is the input used by all MovieLens-based TVD experiments.

The processed MovieLens table is provided as a GitHub Release asset
(`movielens_1m.csv.tar.gz`).

From the repository root, extract the archive:

```
tar -xzf movielens_1m.csv.tar.gz
```

### Generate source tables (MovieLens)

```
python generate_splits.py
```

This generates synthetic source tables from the unified MovieLens table, according
to the configured source construction strategies and each UR.

---

### Run a single UR (debug / inspection)

```
python Check1.py
```

Outputs:

* constructed table,
* coverage,
* penalty,
* runtime statistics.

---

### Run full experiments

```
python run_experiments.py
```

Runs all configured experiments and logs metrics for analysis.

---

## Notes

* This codebase focuses on **TVD methods only**.
* Naming conventions:

  * `TVD-AA` = former `tvd-uni`
  * `TVD-AV` = former `tvd-exi`

---

## Intended Use

This repository is intended for:

* reproducing experiments,
* running controlled evaluations of TVD strategies,
* extending or modifying offline TVD algorithms.

It is **not** intended as a packaged library or end-user tool.

