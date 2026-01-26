"""
Data assembler for combining multiple data sources.

The main orchestrator for the ingestion workflow.
"""

import logging
from typing import Optional

from assembler.parsers import ModelData, ParquetData, ReducedData

from .builders import build_environment, build_reflectivity, build_sample
from .result import AssemblyResult

logger = logging.getLogger(__name__)


class DataAssembler:
    """
    Assembles data from multiple sources into lakehouse models.

    Workflow:
    1. Parse reduced data → Reflectivity base data (Q, R, dR, dQ)
    2. Parse parquet files → Measurement metadata, Environment
    3. Parse model JSON → Sample with layers

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
            print(f"Assembled {len(result.reflectivity.q)} Q points")
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
        Assemble data from parsed sources into target models.

        At minimum, reduced data is required. Parquet and model data
        enrich the result with additional metadata.

        Args:
            reduced: Parsed reduced reflectivity data (required)
            parquet: Parsed parquet metadata (optional)
            model: Parsed model JSON data (optional)

        Returns:
            AssemblyResult with assembled models and any issues
        """
        result = AssemblyResult()

        if reduced is None:
            result.errors.append("Reduced data is required")
            return result

        result.reduced_file = reduced.file_path

        # Step 1: Build Reflectivity from reduced + parquet
        result.reflectivity = build_reflectivity(reduced, parquet, result)

        # Step 2: Build Environment from parquet daslogs
        if parquet is not None:
            result.environment = build_environment(parquet, result)
            result.parquet_dir = parquet.directory_path if hasattr(parquet, 'directory_path') else None

        # Step 3: Build Sample from model
        if model is not None:
            result.sample = build_sample(model, result)
            result.model_file = model.file_path

        return result
