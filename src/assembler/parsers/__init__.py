"""
File parsers for data ingestion.

Provides parsers for:
- Parquet files (nexus-processor output): ParquetParser
- Reduced text files (reflectivity data): ReducedParser
- Model JSON files (refl1d/bumps): ModelParser

For data types returned by parsers, import from the specific module::

    from assembler.parsers.parquet_parser import ParquetData, MetadataRecord
    from assembler.parsers.reduced_parser import ReducedData, ReductionRun
    from assembler.parsers.model_parser import ModelData, ModelLayer
"""

from .model_parser import ModelParser
from .parquet_parser import ParquetParser
from .reduced_parser import ReducedParser

__all__ = [
    "ModelParser",
    "ParquetParser",
    "ReducedParser",
]

