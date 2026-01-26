# Data Assembler

Automated reflectivity data ingestion workflow for the scientific data lakehouse.

## Overview

This package provides tools to ingest reflectometry data from multiple sources:

- **Raw data** (HDF5/NeXus → Parquet via nexus-processor)
- **Reduced data** (text files with Q, R, dR, dQ)
- **Model data** (refl1d/bumps JSON)

The workflow links these sources together, enriches reduced data with metadata from raw files, and structures everything according to the lakehouse schema.

## Installation

```bash
pip install -e .
```

For development:
```bash
pip install -e ".[dev]"
```

## Usage

### CLI

```bash
# Ingest a reduced data file (auto-discovers related files)
data-assembler ingest /path/to/REFL_218386_combined_data_auto.txt

# Ingest with explicit file paths
data-assembler ingest \
    --reduced /path/to/REFL_218386_combined_data_auto.txt \
    --parquet-dir /path/to/parquet_output/ \
    --model /path/to/model.json
```

### Python API

```python
from assembler.workflow import IngestWorkflow
from assembler.parsers import ReducedParser, ParquetParser, ModelParser

# Create workflow
workflow = IngestWorkflow()

# Ingest data
result = workflow.ingest(
    reduced_file="/path/to/REFL_218386_combined_data_auto.txt",
    parquet_dir="/path/to/parquet_output/",
    model_file="/path/to/model.json"
)

# Access assembled data
print(result.measurement)
print(result.sample)
print(result.environment)
```

## Project Structure

```
data-assembler/
├── src/
│   └── assembler/
│       ├── models/           # Pydantic models (lakehouse schema)
│       ├── parsers/          # File parsers
│       ├── tools/            # Agent-callable tools
│       ├── workflow/         # Orchestration logic
│       ├── ai/               # AI-assisted components
│       └── cli/              # Command-line interface
├── tests/
└── docs/
```

## License

MIT
