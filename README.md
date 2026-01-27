# Data Assembler

Automated reflectivity data ingestion workflow for the scientific data lakehouse.

## Overview

This package provides tools to ingest neutron reflectometry data from multiple sources and assemble them into a unified schema for the data lakehouse:

| Source | Format | Contains |
|--------|--------|----------|
| **Reduced data** | Text file (`.txt`) | Q, R, dR, dQ arrays + reduction metadata |
| **Raw metadata** | Parquet (from nexus-processor) | DAS logs, sample info, instrument config |
| **Model data** | JSON (refl1d/bumps) | Layer structure, materials, fit parameters |

The workflow links these sources together and outputs Parquet files partitioned for Iceberg ingestion.

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
# Basic ingest with all three data sources
data-assembler ingest \
    -r ~/data/REFL_218386_combined_data_auto.txt \
    -p ~/data/parquet/ \
    -m ~/data/model.json \
    -o ~/data/output/

# Also output JSON (for AI/LLM consumption)
data-assembler ingest -r data.txt -o output/ --json

# Debug mode: see full schema with missing field indicators
data-assembler ingest -r data.txt -o output/ --debug
```

## CLI Commands

### `ingest` - Main ingestion workflow

```bash
data-assembler ingest [OPTIONS]

Options:
  -r, --reduced PATH   Path to reduced reflectivity data file (.txt) [required]
  -p, --parquet PATH   Directory containing parquet files from nexus-processor
  -m, --model PATH     Path to refl1d/bumps model JSON file
  -o, --output PATH    Output directory for parquet files [required]
  --dry-run            Parse and assemble but don't write output
  --json               Also write JSON files (in addition to Parquet)
  --debug              Write debug JSON with full schema and missing fields
```

### `detect` - Identify file type

```bash
data-assembler detect /path/to/file
data-assembler detect /path/to/file --json
```

### `find` - Locate related files

```bash
data-assembler find --run 218386 --search-path ~/data/
```

## Python API

```python
from assembler.parsers import ReducedParser, ParquetParser, ModelParser
from assembler.workflow import DataAssembler
from assembler.writers import ParquetWriter

# Parse input files
reduced = ReducedParser().parse("REFL_218386_combined_data_auto.txt")
parquet = ParquetParser().parse_directory("./parquet/", run_number=218386)
model = ModelParser().parse("model.json")

# Assemble into unified schema
assembler = DataAssembler()
result = assembler.assemble(reduced=reduced, parquet=parquet, model=model)

# Access assembled data (dict records matching PyArrow schemas)
print(result.reflectivity)  # Measurement with Q/R/dR/dQ in nested struct
print(result.sample)        # Layer structure from model
print(result.environment)   # Conditions from DAS logs

# Write to parquet
writer = ParquetWriter(output_dir)
paths = writer.write(result)
```

## Instrument Support

The assembler includes instrument-specific handlers for extracting metadata from DAS logs:

- **REF_L** (Liquids Reflectometer, SNS BL-4B) - Temperature sensors, slit widths, motor positions

```python
from assembler.instruments import REF_L

# Extract environment from parquet data
env = REF_L.extract_environment(parquet_data)
meta = REF_L.extract_metadata(parquet_data)
```

## Output Schema

Output records are dictionaries that match PyArrow schemas defined in `writers/schemas.py`.

### Reflectivity Table
- `proposal_number`, `facility`, `laboratory`, `instrument_name`
- `run_number`, `run_title`, `run_start`
- `probe`, `technique`, `is_simulated`
- `reflectivity` (nested struct):
  - `q`, `r`, `dr`, `dq` (arrays)
  - `measurement_geometry`, `reduction_time`, `reduction_version`

### Sample Table  
- `description`, `main_composition`, `geometry`
- `layers[]` with layer_number, material, thickness, roughness, sld
- `substrate_json`

### Environment Table
- `description`, `ambient_medium`
- `temperature`, `pressure`, `relative_humidity`
- `measurement_ids`

## Debug Output

Use `--debug` to generate `debug_schema.json` with:
- Full schema showing all fields
- Missing field indicators with descriptions
- Field coverage percentages
- Data source summary (what came from where)

```json
{
  "field_coverage": {
    "reflectivity": {"coverage_pct": 79.2, "missing_field_names": ["sample_id"]},
    "sample": {"coverage_pct": 70.6, "missing_field_names": ["geometry"]},
    "environment": {"coverage_pct": 38.5, "missing_field_names": ["pressure"]}
  }
}
```

## Architecture

The assembler uses a simple data flow:

```
Parsers → Record Builders → Writers
            ↓
     Dict records matching
      PyArrow schemas
```

- **Parsers** read input files into intermediate data structures
- **Record Builders** convert parsed data directly to schema-ready dicts
- **Writers** output dicts to Parquet/JSON files

## Project Structure

```
data-assembler/
├── src/assembler/
│   ├── cli/           # Click-based CLI
│   ├── instruments/   # Instrument-specific handlers (REF_L, etc.)
│   ├── parsers/       # File parsers (reduced, parquet, model)
│   ├── tools/         # File detection, finding utilities
│   ├── workflow/      # Assembly orchestration and record builders
│   └── writers/       # Parquet/JSON output and schemas
├── tests/
└── docs/
```

## Development

```bash
# Run tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=assembler
```

## License

BSD-3-Clause
