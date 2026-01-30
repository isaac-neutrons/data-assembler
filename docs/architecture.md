# Data Assembler - Technical Architecture

## Overview

The Data Assembler is an automated reflectivity data ingestion workflow that combines three data sources into a unified schema for the scientific data lakehouse. It produces Iceberg-compatible Parquet files with linked records following a hierarchical data model.

## Data Sources

| Source | Format | Key Data |
|--------|--------|----------|
| **Reduced Data** | Text file (`.txt`) | Q, R, dR, dQ arrays; reduction metadata (version, time, angles) |
| **Parquet Metadata** | Parquet (from nexus-processor) | DAS logs for environment conditions; run metadata |
| **Model Data** | JSON (refl1d/bumps) | Layer stack with materials, thicknesses, and SLD values |

## Architecture

### Component Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              CLI (cli/main.py)                               │
│                    Commands: ingest, detect, find                            │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Parsers (parsers/)                                   │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐              │
│  │  ReducedParser  │  │  ParquetParser  │  │   ModelParser   │              │
│  │                 │  │                 │  │                 │              │
│  │  → ReducedData  │  │  → ParquetData  │  │  → ModelData    │              │
│  │    - q, r, dr   │  │    - metadata   │  │    - layers     │              │
│  │    - run info   │  │    - daslogs    │  │    - materials  │              │
│  │    - angles     │  │    - sample     │  │    - SLDs       │              │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘              │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     DataAssembler (workflow/assembler.py)                    │
│                                                                              │
│  Orchestrates the assembly process:                                          │
│  1. Calls record builders with parsed data                                   │
│  2. Links records via UUID relationships                                     │
│  3. Collects warnings, errors, and review flags                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Record Builders (workflow/builders/)                      │
│  ┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────┐  │
│  │  reflectivity.py    │  │   environment.py    │  │     sample.py       │  │
│  │                     │  │                     │  │                     │  │
│  │  Reduced + Parquet  │  │  Parquet daslogs    │  │  Model layers       │  │
│  │  → Reflectivity     │  │  → Environment      │  │  → Sample           │  │
│  │    record           │  │    record           │  │    record           │  │
│  └─────────────────────┘  └─────────────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│               Instrument Handlers (instruments/)                             │
│                                                                              │
│  InstrumentRegistry → REF_L (or GenericInstrument)                          │
│                                                                              │
│  Provides:                                                                   │
│  - extract_environment(): Temperature, pressure from DAS logs               │
│  - extract_metadata(): Slit widths, motor positions                         │
│  - defaults: facility, probe, technique, environment description            │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      Writers (writers/)                                      │
│  ┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────┐  │
│  │   ParquetWriter     │  │    JSONWriter       │  │     schemas.py      │  │
│  │                     │  │                     │  │                     │  │
│  │  Iceberg-ready      │  │  Debug/AI output    │  │  PyArrow schemas    │  │
│  │  partitioned files  │  │  with full schema   │  │  for validation     │  │
│  └─────────────────────┘  └─────────────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Data Model

### Record Hierarchy

Records are linked via UUIDs in a hierarchy that reflects the scientific workflow:

```
Sample (top level)
│
├── environment_ids: [uuid, ...]
│
└── Environment (experimental conditions)
    │
    ├── sample_id: uuid
    ├── measurement_ids: [uuid, ...]
    │
    └── Reflectivity (measurement data)
        │
        ├── sample_id: uuid
        └── environment_id: uuid
```

### Schema Definitions

All schemas are defined as PyArrow schemas in `writers/schemas.py` for Iceberg compatibility.

#### Reflectivity Record

| Field | Type | Source |
|-------|------|--------|
| `id` | UUID | Generated |
| `sample_id` | UUID | Linked from Sample |
| `environment_id` | UUID | Linked from Environment |
| `proposal_number` | string | Parquet metadata (IPTS) |
| `facility` | string | Instrument defaults |
| `laboratory` | string | Instrument defaults |
| `instrument_name` | string | Parquet metadata |
| `run_number` | string | Reduced/Parquet |
| `run_title` | string | Parquet metadata |
| `run_start` | timestamp | Parquet metadata |
| `probe` | string | Instrument defaults ("neutrons") |
| `technique` | string | Instrument defaults ("reflectivity") |
| `technique_description` | string | Instrument defaults |
| `is_simulated` | bool | Default false |
| `raw_file_path` | string | Parquet metadata |
| `reflectivity` | struct | Nested measurement data |

The `reflectivity` struct contains:
- `q`, `r`, `dr`, `dq`: float arrays from reduced data
- `measurement_geometry`: string ("front reflection" or "back reflection")
- `reduction_time`: timestamp
- `reduction_version`: string

#### Sample Record

| Field | Type | Source |
|-------|------|--------|
| `id` | UUID | Generated |
| `description` | string | Generated from layers |
| `main_composition` | string | Thickest layer material |
| `geometry` | string | Optional |
| `layers` | list[struct] | Model JSON layers |
| `layers_json` | string | JSON serialization |
| `substrate_json` | string | Bottom layer JSON |
| `environment_ids` | list[UUID] | Linked |

#### Environment Record

| Field | Type | Source |
|-------|------|--------|
| `id` | UUID | Generated |
| `sample_id` | UUID | Linked from Sample |
| `description` | string | Instrument defaults |
| `ambient_medium` | string | Model ambient layer |
| `temperature` | float | DAS logs |
| `pressure` | float | DAS logs |
| `relative_humidity` | float | DAS logs |
| `measurement_ids` | list[UUID] | Linked |

## Instrument System

The instrument system allows instrument-specific handling of DAS logs and defaults.

### InstrumentRegistry

A decorator-based registry that maps instrument IDs to handlers:

```python
@InstrumentRegistry.register
class REF_L(Instrument):
    name = "REF_L"
    aliases = ["BL4B", "BL-4B"]
    defaults = InstrumentDefaults(
        facility="SNS",
        laboratory="ORNL",
        probe="neutrons",
        technique="reflectivity",
        technique_description="Specular neutron reflectometry",
    )
```

### DAS Log Extraction

Each instrument defines which DAS log names to query for environment data:

```python
TEMPERATURE_LOGS = ["SampleTemp", "BL4B:SE:SampleTemp"]

@classmethod
def extract_environment(cls, parquet: ParquetData) -> ExtractedEnvironment:
    temperature = cls.get_daslog_value(parquet, cls.TEMPERATURE_LOGS)
    return ExtractedEnvironment(temperature=temperature)
```

## Parsers

### ReducedParser

Parses the SNS reduced reflectivity text format:

```
# Experiment IPTS-12345 Run 218386
# Reduction quicknxs 4.0.0
# Run title: Sample measurement
# Run start time: 2024-01-15 10:30:00
# Reduction time: 2024-01-15 11:00:00
# DataRun  NormRun  TwoTheta  ...
#   218386  218385    0.60    ...
# Q(1/A)  R         dR        dQ
0.0100   1.0e-1    1.0e-3    5.0e-4
...
```

**Output:** `ReducedData` with:
- `q`, `r`, `dr`, `dq` arrays
- `experiment_id`, `run_number`
- `reduction_version`, `reduction_time`
- `runs[].two_theta` for multi-angle data

### ParquetParser

Reads nexus-processor output directory containing:
- `metadata.parquet` → `MetadataRecord`
- `sample.parquet` → `SampleRecord`
- `daslogs.parquet` → `dict[str, DASLogRecord]`

**Output:** `ParquetData` with metadata, sample, and daslogs dictionary.

### ModelParser

Parses refl1d/bumps JSON model format with reference resolution:

```json
{
  "$schema": "bumps-draft-03",
  "references": { "id1": {"slot": {"value": 100.0}} },
  "object": {
    "sample": {
      "layers": [
        {"name": "air", "thickness": {"$ref": "#/references/..."}, ...}
      ]
    }
  }
}
```

**Output:** `ModelData` with:
- `layers[]` with thickness, interface, material (name, rho, irho)
- Properties: `ambient`, `substrate`, `film_layers`, `total_thickness`

## CLI Commands

### `ingest`

Main workflow command:

```bash
data-assembler ingest \
    -r reduced.txt \      # Required: reduced data
    -p parquet_dir/ \     # Optional: parquet metadata
    -m model.json \       # Optional: layer model
    -o output/ \          # Required: output directory
    --json \              # Also write JSON
    --debug               # Write debug schema JSON
```

### `detect`

Identify file type:

```bash
data-assembler detect /path/to/file --json
```

### `find`

Locate related files by run number:

```bash
data-assembler find --run 218386 --search-path ~/data/
```

## Output Files

### Parquet Output

```
output/
├── reflectivity/
│   └── facility=SNS/
│       └── proposal=IPTS-12345/
│           └── data.parquet
├── sample/
│   └── data.parquet
└── environment/
    └── data.parquet
```

### Debug Output

`debug_schema.json` includes:
- Full records with all fields
- Missing field indicators
- Field coverage percentages
- Data source mapping
- Warnings and review flags

## Error Handling

### AssemblyResult

The assembler returns an `AssemblyResult` containing:
- Assembled records (or None on failure)
- `warnings`: Non-fatal issues
- `errors`: Fatal issues
- `needs_review`: Fields requiring human/AI review

### Review Flags

Fields that couldn't be automatically filled are flagged:

```python
needs_review = {
    "environment_temperature": "Temperature not found in daslogs",
    "sample_composition": "Could not determine main composition"
}
```

## Extension Points

### Adding Instruments

1. Create a new file in `instruments/`
2. Subclass `Instrument`
3. Decorate with `@InstrumentRegistry.register`
4. Implement `extract_environment()` with instrument-specific log names

### Adding Output Formats

1. Create a new writer in `writers/`
2. Accept `AssemblyResult` and output directory
3. Return dict of output paths

### Schema Changes

1. Update PyArrow schema in `writers/schemas.py`
2. Update corresponding builder in `workflow/builders/`
3. Update tests
