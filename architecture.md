# Architecture: data-assembler

This document records the design decisions behind the data-assembler's AI-ready store so they
are not accidentally undone. Code and schema changes should be checked against the invariants
in §7 before merging. (This supersedes the earlier `groundtruths.md`.)

## 1. Purpose

The data-assembler turns neutron-reflectometry artifacts (reduced data, fit problems, run
metadata) into a neutral, typed, **AI-ready store** (Parquet, mirrored as JSON). This store is
the facility's source of truth. Downstream representations — notably the ISAAC AI-Ready Record
produced by `nr-isaac-format` — are *exports* derived from it, never the other way around.

## 2. Two distinct entities

The store captures **two conceptually separate things**, each independently queryable:

1. **Run data** — what was *measured*. One physical acquisition = one run = one `reflectivity`
   record (`q/r/dr/dq`, `run_number`, instrument metadata).
2. **Fit results** — what was *inferred*. The outcome of a fitting process: layer parameters
   with uncertainties, χ², assumptions. A single fit may span **many runs** (co-refinement),
   and the **same run-set may have several alternative fits**. The fit is the
   `reflectivity_model` record (the first-class "fit" entity; `fit` is a schema alias).

Run data and fit results must never be conflated. A run is valid AI-ready data with no fit; a
fit *references* runs but is not owned by any one of them.

## 3. Storage principle: keep everything at the run level

The reflectivity table is **one record per run** — the most scalable, facility-oriented
granularity. Higher-level grouping (state, series, angle) is **never** a column in the
reflectivity table; it is recovered by foreign-key query:

- `reflectivity.sample_id`, `reflectivity.environment_id` — nullable FKs.
- A **state** (one physical condition measured at several angles) is *derived* as **the set of
  runs sharing `(sample_id, environment_id)`**. There is no `state_id` column, by design.

This keeps the run table pure and append-friendly, and lets any consumer reconstruct states,
samples, and fits without the store committing to one grouping.

## 4. Two orthogonal domain concepts: state vs co-refinement

Independent; never merged in code or schema:

- **State** — one *physical condition* (e.g. "in D2O at OCV"), typically measured at several
  incident angles. Each angle is a **partial** = its own run.
- **Co-refinement** — a *fitting strategy* that ties parameters across runs/states
  (`shared_parameters` / `unshared_parameters`). It describes the **fit**, not the data layout.

A multi-state co-refinement produces many runs (across several states) sharing one sample, plus
**one** fit linking all those runs with per-dataset parameters.

## 5. Entity–relationship model

| Entity | Table | Key | Links out |
|---|---|---|---|
| Run (measured) | `reflectivity` | `id` (`run_number` for humans) | `sample_id`, `environment_id` |
| Sample (physical) | `sample` | `id` | `environment_ids[]`, `fit_ids[]` |
| Environment (condition) | `environment` | `id` | `measurement_ids[]` |
| Fit (inferred) | `reflectivity_model` (alias `fit`) | `id` | `measurement_ids[]` (all runs), `sample_id`/`sample_ids[]` |

The **fit** record carries: `measurement_ids[]` (every run the fit used — never silently
length-1); `datasets[]` (one per run: `{dataset_index, measurement_id, run_number,
chi_squared, layers[]}` — per-dataset fitted parameters, so a co-refinement is self-contained);
top-level `layers`/`chi_squared`/`dataset_index` mirroring the primary dataset (back-compat for
the ISAAC writer); `fit_strategy`, `shared_parameters[]`, `unshared_parameters[]`, `model_json`
(full ground truth). "Multiple fits per run-set" needs no special structure — each fit is its
own row keyed by `id`.

## 6. Multi-state co-refinement: the explicit-states contract

State membership is **explicit and user-named** — never inferred from data-file names, never
from a shared-sample assumption. The authoritative input is `run_info.json` `states[]`:

```jsonc
{
  "sample_description": "…default stack…",
  "states": [
    { "name": "D2O OCV",                          // user-given, authoritative identity
      "data_files": [ {"file": "…230539_1…"}, … ],// this state's angles
      "extra_description": "OCV in D2O, pH 8.25",  // this state's conditions
      "sample_description": "…optional override…"  // a state MAY have its own sample
    },
    { "name": "H2O OCV", "data_files": [ … ], "extra_description": "…" }
  ]
  // flat "data_files"/"data_file" are DERIVED/legacy; states[] is authoritative
}
```

When `states[]` is absent the run is single-state (the whole `data_files` list is one state) —
the back-compatible path. Assembly rules (`assembler.py: _assemble_workflow` /
`_assemble_multistate`):

- Each state → its **own** environment record (from its conditions) and, when it declares one,
  its **own** sample; otherwise the shared sample. `sample_id`/`environment_id` are assigned
  **per state and not assumed shared**.
- **Sample identity** (`run_info.distinct_sample`, default `false`): co-refined states are
  usually *one* physical sample under several conditions → they share one `sample_id`. When
  AuRE flags the states as **distinct physical samples** (`distinct_sample: true`), each state
  gets its own `sample_id` (state 0 keeps the primary `result.sample`; states 1.. become
  `additional_samples`), each sample links only to its own state's environment, and the fit
  records every sample in `sample_ids[]`. Orthogonal to per-state structure.
- Every run is tagged with its state's `(sample_id, environment_id)`.
- The whole co-refinement is **one** fit (`fit_strategy: multi_state_coref`) over every run.

Export (in `nr-isaac-format`): group by `(sample_id, environment_id)` → **one ISAAC record per
state**; the shared fit supplies descriptors. No manifest, no file-name parsing.

## 7. Ground-truth invariants (do not break)

1. **One reflectivity record per run.** `ingest`/`ingest-workflow` emit a record for *every*
   data file, never just the first.
2. **No grouping columns in `reflectivity`.** State/series/angle are derived from FKs.
3. **The fit links all its runs.** `measurement_ids` reflects true cardinality; a
   co-refinement records every dataset in `datasets[]`, not only the selected one.
4. **Additive schema.** New fields are nullable; existing Parquet stays readable. Never
   repurpose a field's meaning silently.
5. **ISAAC is downstream.** No ISAAC concept (series, record-per-state, ULIDs) leaks into the
   store schema. The store depends on no exporter.
6. **Run data and fit results stay separable.** "Runs of sample S" must not require a fit;
   "fits over run-set R" must not require re-deriving from raw files.

## 8. Code map

- `writers/schemas.py` — the PyArrow table schemas (the contract).
- `workflow/assembler.py` — `assemble` (single measurement) and `assemble_workflow` /
  `_assemble_multistate` (pull from an AuRE run dir → N runs + one fit).
- `workflow/result.py` — `AssemblyResult` (carries `reflectivities` + per-state
  `environments`/`samples` + the fit).
- `workflow/builders/` — per-record builders (`reflectivity`, `sample`, `environment`,
  `reflectivity_model`).
- `writers/{parquet_writer,json_writer}.py` — write every run (file per `run_number`) + each
  sample/environment (file per `id`) + the fit.
- `parsers/` — `reduced`, `parquet`, `model` (refl1d/bumps), `manifest`.

## 9. Consequences for queries

First-class queries against the store: *runs of a sample* (`reflectivity WHERE sample_id`); *a
state* (`WHERE sample_id AND environment_id`); *runs in a fit* (`fit.measurement_ids`); *fits
for a run* (`fit WHERE run_id IN measurement_ids`); *fits for a sample* (`sample.fit_ids`);
*per-dataset fitted layers* (`fit.datasets[]`).
