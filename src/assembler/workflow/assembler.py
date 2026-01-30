"""
Data assembler for combining multiple data sources.

The main orchestrator for the ingestion workflow.
"""

import logging
from typing import Optional

from assembler.parsers.model_parser import ModelData
from assembler.parsers.parquet_parser import ParquetData
from assembler.parsers.reduced_parser import ReducedData

from .builders import (
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

        # Step 1: Build Reflectivity record from reduced + parquet + model
        result.reflectivity = build_reflectivity_record(
            reduced=reduced,
            parquet=parquet,
            warnings=result.warnings,
            errors=result.errors,
            needs_review=result.needs_review,
            model=model,
        )

        # Step 2: Build Environment record from parquet daslogs
        if parquet is not None:
            result.environment = build_environment_record(
                parquet=parquet,
                warnings=result.warnings,
                errors=result.errors,
                needs_review=result.needs_review,
                model=model,
            )

        # Step 3: Build Sample record from model
        if model is not None:
            result.sample = build_sample_record(
                model=model,
                warnings=result.warnings,
                errors=result.errors,
                needs_review=result.needs_review,
            )
            result.model_file = model.file_path

        # Step 4: Link IDs across the hierarchy (sample -> environment -> measurement)
        self._link_record_ids(result)

        return result

    def _link_record_ids(self, result: AssemblyResult) -> None:
        """
        Link record IDs across the hierarchy.

        Hierarchy: Sample -> Environment -> Reflectivity (measurement)
        - Environment gets sample_id
        - Reflectivity gets environment_id and sample_id
        - Environment tracks measurement_ids
        - Sample tracks environment_ids
        """
        sample_id = result.sample["id"] if result.sample else None
        environment_id = result.environment["id"] if result.environment else None
        reflectivity_id = result.reflectivity["id"] if result.reflectivity else None

        # Link environment to sample
        if result.environment:
            result.environment["sample_id"] = sample_id

        # Link reflectivity to environment and sample
        if result.reflectivity:
            result.reflectivity["environment_id"] = environment_id
            result.reflectivity["sample_id"] = sample_id

        # Track measurement IDs in environment
        if result.environment and reflectivity_id:
            result.environment["measurement_ids"] = [reflectivity_id]

        # Track environment IDs in sample
        if result.sample and environment_id:
            result.sample["environment_ids"] = [environment_id]
