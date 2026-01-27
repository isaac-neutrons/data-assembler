"""
Data assembler for combining multiple data sources.

The main orchestrator for the ingestion workflow.
"""

import logging
from typing import Optional

from assembler.parsers import ModelData, ParquetData, ReducedData

from .record_builders import (
    build_environment_record,
    build_reflectivity_record,
    build_sample_record,
)
from .result import AssemblyResult

logger = logging.getLogger(__name__)


class DataAssembler:
    """
    Assembles data from multiple sources into schema-ready records.

    Workflow:
    1. Parse reduced data → Reflectivity record (Q, R, dR, dQ + metadata)
    2. Parse parquet files → Environment record from DASlogs
    3. Parse model JSON → Sample record with layers

    Example:
        assembler = DataAssembler()

        # From parsed data
        result = assembler.assemble(
            reduced=reduced_data,
            parquet=parquet_data,
            model=model_data,
        )

        # Check result
        if result.is_complete:
            refl = result.reflectivity
            print(f"Assembled {len(refl['reflectivity']['q'])} Q points")
        if result.needs_human_review:
            print(f"Review needed: {result.needs_review}")
    """

    def assemble(
        self,
        reduced: Optional[ReducedData] = None,
        parquet: Optional[ParquetData] = None,
        model: Optional[ModelData] = None,
    ) -> AssemblyResult:
        """
        Assemble data from parsed sources into schema-ready records.

        At minimum, reduced data is required. Parquet and model data
        enrich the result with additional metadata.

        Args:
            reduced: Parsed reduced reflectivity data (required)
            parquet: Parsed parquet metadata (optional)
            model: Parsed model JSON data (optional)

        Returns:
            AssemblyResult with assembled records and any issues
        """
        result = AssemblyResult()

        if reduced is None:
            result.errors.append("Reduced data is required")
            return result

        result.reduced_file = reduced.file_path

        # Step 1: Build Reflectivity record from reduced + parquet
        result.reflectivity = build_reflectivity_record(
            reduced=reduced,
            parquet=parquet,
            warnings=result.warnings,
            errors=result.errors,
            needs_review=result.needs_review,
        )

        # Step 2: Build Environment record from parquet daslogs
        if parquet is not None:
            result.environment = build_environment_record(
                parquet=parquet,
                warnings=result.warnings,
                errors=result.errors,
                needs_review=result.needs_review,
            )
            result.parquet_dir = parquet.directory_path if hasattr(parquet, 'directory_path') else None

        # Step 3: Build Sample record from model
        if model is not None:
            result.sample = build_sample_record(
                model=model,
                warnings=result.warnings,
                errors=result.errors,
                needs_review=result.needs_review,
            )
            result.model_file = model.file_path

        return result
