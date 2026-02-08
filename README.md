# Data Assembler

Automated reflectivity data ingestion workflow for the scientific data lakehouse.

## Installation

```bash
pip install -e .
```

For development:
```bash
pip install -e ".[dev]"
```

## Quick Start

```bash
# Ingest reduced data with parquet metadata and model
data-assembler ingest \
    -r ~/data/REFL_218386_combined_data_auto.txt \
    -p ~/data/parquet/ \
    -m ~/data/model.json \
    -o ~/output/

# With environment description and JSON output
data-assembler ingest \
    -r ~/data/reduced.txt \
    -p ~/data/parquet/ \
    -m ~/data/corefine-model.json \
    -e "Electrochemical cell, THF electrolyte, steady-state OCV" \
    -o ~/output/ --json

# Dry run (parse and validate without writing)
data-assembler ingest -r data.txt -o output/ --dry-run

# Output JSON alongside Parquet (useful for AI/LLM consumption)
data-assembler ingest -r data.txt -o output/ --json

# Debug mode with full schema and missing field analysis
data-assembler ingest -r data.txt -o output/ --debug

# Batch: process an entire sample history from a YAML manifest
data-assembler batch experiment.yaml --json
```

## CLI Commands

### `ingest` — Main ingestion workflow

Assembles data from multiple sources into Iceberg-ready Parquet files.

```bash
data-assembler ingest [OPTIONS]
```

**Required Options:**
| Option | Description |
|--------|-------------|
| `-r, --reduced PATH` | Path to reduced reflectivity data file (`.txt`) |
| `-o, --output PATH` | Output directory for Parquet files |

**Optional Options:**
| Option | Description |
|--------|-------------|
| `-p, --parquet PATH` | Directory containing parquet files from nexus-processor |
| `-m, --model PATH` | Path to refl1d/bumps model JSON file |
| `--model-dataset-index N` | 1-based index of the dataset in a co-refinement model. If omitted, auto-detected by matching reflectivity data. |
| `-e, --environment TEXT` | Description text for the environment record (e.g. `'Sample cell, flowing N2'`) |
| `--sample-id UUID` | UUID of an existing sample to reuse (skips creating a new sample record) |
| `--dry-run` | Parse and assemble but don't write output |
| `--json` | Also write JSON files (in addition to Parquet) |
| `--debug` | Write debug JSON with full schema and missing fields |

**Examples:**

```bash
# Minimal: just reduced data
data-assembler ingest -r reduced.txt -o output/

# With parquet metadata (adds environment conditions from DAS logs)
data-assembler ingest -r reduced.txt -p parquet_dir/ -o output/

# With model (adds sample layer structure and reflectivity model record)
data-assembler ingest -r reduced.txt -m model.json -o output/

# Full pipeline with all sources
data-assembler ingest -r reduced.txt -p parquet_dir/ -m model.json -o output/ --json

# Co-refinement model: explicitly select dataset 2
data-assembler ingest -r reduced.txt -m corefine.json --model-dataset-index 2 -o output/

# Co-refinement model: auto-detect dataset by matching R values
data-assembler ingest -r reduced.txt -m corefine.json -o output/

# Override the environment description
data-assembler ingest -r reduced.txt -p parquet_dir/ \
    -e "Electrochemical cell, THF electrolyte" -o output/

# Reuse an existing sample across measurements
data-assembler ingest -r run2.txt -m model.json \
    --sample-id "e2baf3d3-5aaf-4ad9-9164-827ede801195" -o output/
```

---

### `batch` — Process a sample history

Processes a YAML manifest describing a single physical sample and its
ordered measurement history. The first measurement creates the sample
record; all subsequent measurements reuse the same sample ID.

```bash
data-assembler batch MANIFEST [OPTIONS]
```

**Options:**
| Option | Description |
|--------|-------------|
| `--dry-run` | Parse and validate but don't write output |
| `--json` | Also write JSON files (in addition to Parquet) |

**Examples:**

```bash
# Process all measurements in a manifest
data-assembler batch experiment.yaml

# With JSON output for debugging / AI consumption
data-assembler batch experiment.yaml --json

# Validate without writing
data-assembler batch experiment.yaml --dry-run
```

#### Manifest Format

The manifest is a YAML file with three sections: `sample`, `output`, and
`measurements`. Fields in `sample` (like `model` and `model_dataset_index`)
act as defaults that individual measurements can override.

```yaml
title: "IPTS-34347 Cu/THF non-aqueous experiment"   # optional

sample:
  description: "Cu in THF on Si"        # overrides auto-extracted description
  model: /path/to/corefine-model.json   # default model for all measurements
  model_dataset_index: 1                # default dataset index (1-based)

output: /path/to/output/

measurements:
  - name: "Steady-state OCV"
    reduced: /path/to/REFL_218386_reduced.txt
    parquet: /path/to/parquet/           # optional
    model: /path/to/corefine-model.json  # overrides sample.model
    model_dataset_index: 1               # overrides sample.model_dataset_index
    environment: "Electrochemical cell, THF electrolyte, steady-state OCV"

  - name: "Final OCV"
    reduced: /path/to/REFL_218393_reduced.txt
    model_dataset_index: 2               # different dataset from same model
    environment: "Electrochemical cell, THF electrolyte, final OCV"
```

Each measurement produces its own reflectivity, environment, and
reflectivity_model records. The sample record is created once and linked
to all environments.

---

### `detect` — Identify file type

Detects the type of a data file.

```bash
data-assembler detect PATH [OPTIONS]
```

**Options:**
| Option | Description |
|--------|-------------|
| `--json` | Output result as JSON |

**Examples:**

```bash
data-assembler detect ~/data/REFL_218386_combined_data_auto.txt
# Output: reduced (text)

data-assembler detect ~/data/model.json --json
# Output: {"file_type": "model", "format": "json"}
```

---

### `find` — Locate related files

Searches for related data files by run number.

```bash
data-assembler find [OPTIONS]
```

**Options:**
| Option | Description |
|--------|-------------|
| `--run NUMBER` | Run number to search for |
| `--search-path PATH` | Directory to search in |
| `--json` | Output result as JSON |

**Examples:**

```bash
data-assembler find --run 218386 --search-path ~/data/
# Finds reduced files, parquet directories, and model files matching run 218386

data-assembler find --run 218386 --search-path ~/data/ --json
```

---

## Output

### Parquet Files

Output is organized for Iceberg ingestion.

**Single measurement** (`ingest`):

```
output/
├── reflectivity/
│   └── facility=SNS/
│       └── year=2025/
│           └── 218386.parquet
├── sample/
│   └── <uuid>.parquet
├── environment/
│   └── <uuid>.parquet
├── reflectivity_model/          # Only when --model is provided
│   └── <uuid>.parquet
└── json/                        # Only with --json flag
    ├── reflectivity.json
    ├── sample.json
    ├── environment.json
    └── reflectivity_model.json
```

**Batch** (`batch`): one sample, multiple measurements with per-run JSON:

```
output/
├── reflectivity/
│   └── facility=SNS/
│       └── year=2025/
│           ├── 218386.parquet
│           └── 218393.parquet
├── sample/
│   └── <uuid>.parquet              # Written once with all environment IDs
├── environment/
│   ├── <uuid>.parquet              # One per measurement
│   └── <uuid>.parquet
├── reflectivity_model/
│   ├── <uuid>.parquet
│   └── <uuid>.parquet
└── json/                           # Only with --json flag
    ├── sample.json                 # Top-level, with complete environment_ids
    ├── 218386/                     # Per-run subdirectory
    │   ├── reflectivity.json
    │   ├── environment.json
    │   └── reflectivity_model.json
    └── 218393/
        ├── reflectivity.json
        ├── environment.json
        └── reflectivity_model.json
```

### Tables

| Table | Description | Key Fields |
|-------|-------------|------------|
| **reflectivity** | Measurement data | Q, R, dR, dQ arrays; run metadata; facility partitioning |
| **sample** | Sample description | Layer stack, composition, geometry |
| **environment** | Measurement conditions | Temperature, pressure, potential, humidity, description |
| **reflectivity_model** | Fit model from refl1d/bumps | Layer parameters, software provenance, dataset index, full JSON |

### Co-Refinement Models

Model JSON files from refl1d/bumps may contain multiple co-refined datasets
(experiments). The assembler handles this by:

1. **Explicit selection** — pass `--model-dataset-index N` (1-based) to pick a specific dataset.
2. **Auto-detection** — when no index is given, the assembler interpolates each experiment's
   probe R values onto the reduced Q grid and selects the best match by mean relative error.

The selected `dataset_index` (0-based) is recorded in the `reflectivity_model` table
for traceability.

### Debug Output

With `--debug`, generates `debug_schema.json` containing:
- Full records with all populated fields
- Missing field indicators
- Field coverage percentages per record type
- Warnings and fields needing review

## Python API

### Single measurement

```python
from assembler.parsers import ReducedParser, ParquetParser, ModelParser
from assembler.workflow import DataAssembler
from assembler.writers.parquet_writer import write_assembly_to_parquet

# Parse input files
reduced = ReducedParser().parse("reduced.txt")
parquet = ParquetParser().parse_directory("parquet/", run_number=218386)
model = ModelParser().parse("model.json", dataset_index=0)  # 0-based index, or omit for auto

# Assemble into unified schema
assembler = DataAssembler()
result = assembler.assemble(
    reduced=reduced,
    parquet=parquet,
    model=model,
    environment_description="Electrochemical cell, THF electrolyte",
)

# Check result
print(result.summary())
if result.needs_human_review:
    print("Review needed:", result.needs_review)

# Write output
paths = write_assembly_to_parquet(result, "output/")
```

### Reusing a sample across measurements

```python
# First run — creates the sample
result1 = assembler.assemble(reduced=run1, parquet=pq1, model=model)
sample_id = result1.sample["id"]
write_assembly_to_parquet(result1, "output/")

# Second run — reuses the same sample
result2 = assembler.assemble(
    reduced=run2, model=model, sample_id=sample_id,
    environment_description="Post-cycling",
)
write_assembly_to_parquet(result2, "output/")
```

### Batch from manifest

```python
from assembler.parsers import ManifestParser

manifest = ManifestParser().parse("experiment.yaml")
errors = manifest.validate()
if errors:
    raise ValueError(errors)

# The batch CLI command handles the full processing loop;
# see `data-assembler batch --help` for details.
```

## Documentation

See [docs/architecture.md](docs/architecture.md) for technical details on:
- Data model and schema definitions
- Instrument system and DAS log extraction
- Parser formats
- Extension points

## Development

```bash
# Run tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=assembler
```

## License

BSD-3-Clause
