#!/bin/bash
# Example commands for data-assembler CLI

# Full ingestion with all data sources (dry run)
# This uses REF_L-specific handling for DAS logs
data-assembler ingest \
    -r ~/data/REFL_218386_combined_data_auto.txt \
    -p ~/data/isaac/expt11/parquet/ \
    -m ~/data/expt11-refl1d/Cu-THF-corefine-expt11-1-expt.json \
    -o /tmp/lakehouse \
    --dry-run

# With JSON output for AI-ready consumers
data-assembler ingest \
    -r ~/data/REFL_218386_combined_data_auto.txt \
    -p ~/data/isaac/expt11/parquet/ \
    -o /tmp/lakehouse \
    --json

# With debug output to see schema coverage
data-assembler ingest \
    -r ~/data/REFL_218386_combined_data_auto.txt \
    -p ~/data/isaac/expt11/parquet/ \
    -o /tmp/lakehouse \
    --debug

# Detect file type
data-assembler detect ~/data/REFL_218386_combined_data_auto.txt

# Find related files for a run
data-assembler find --run 218386 \
    -s ~/data \
    -s ~/data/isaac/expt11/parquet