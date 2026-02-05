# TVD

# — Codebase Overview

This repository contains the **experimental and algorithmic code** for running **Tuple-Value Discovery (TVD)** methods and evaluating them under different source construction settings.

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
  Functions to generate synthetic source tables (random, skewed, high-penalty, low-coverage, etc.).

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
  Generates source tables from a base dataset (e.g., MovieLens-1M).
  **Must be run first.**

* `Check1.py`
  Runs a **single UR** against generated sources (debug / sanity check).

* `run_experiments.py`
  Runs **full experimental sweeps** (multiple URs, splits, configurations).

---

## How to Run

### 1. Dataset

Download **MovieLens-1M** manually and place it locally.

No dataset is bundled with this repository.

---

### 2. Generate source tables

```bash
python generate_splits.py
```

This creates multiple source tables according to the configured construction strategy.

---

### 3. Run a single UR (debug / inspection)

```bash
python Check1.py
```

Outputs:

* constructed table,
* coverage,
* penalty,
* runtime statistics.

---

### 4. Run full experiments

```bash
python run_experiments.py
```

Runs all configured experiments and logs metrics for analysis.

---

## Notes

* This codebase focuses on ** TVD methods only**.
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

---

