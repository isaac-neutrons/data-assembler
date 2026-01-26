"""
File parsers for data ingestion.

Provides parsers for:
- Parquet files (nexus-processor output)
- Reduced text files (reflectivity data)
- Model JSON files (refl1d/bumps)
"""

from assembler.parsers.model_parser import (
    FitParameter,
    ModelData,
    ModelLayer,
    ModelMaterial,
    ModelParser,
    extract_layers_for_sample,
)
from assembler.parsers.parquet_parser import (
    DASLogRecord,
    InstrumentRecord,
    MetadataRecord,
    ParquetData,
    ParquetParser,
    SampleRecord,
    UserRecord,
)
from assembler.parsers.reduced_parser import (
    ReducedData,
    ReducedParser,
    ReductionRun,
    extract_run_number_from_filename,
)

__all__ = [
    # Parquet
    "ParquetParser",
    "ParquetData",
    "MetadataRecord",
    "SampleRecord",
    "InstrumentRecord",
    "UserRecord",
    "DASLogRecord",
    # Reduced
    "ReducedParser",
    "ReducedData",
    "ReductionRun",
    "extract_run_number_from_filename",
    # Model
    "ModelParser",
    "ModelData",
    "ModelLayer",
    "ModelMaterial",
    "FitParameter",
    "extract_layers_for_sample",
]

