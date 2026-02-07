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
    build_reflectivity_model_record,
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
        environment_description: Optional[str] = None,
    ) -> AssemblyResult:
        """
        Assemble data from parsed sources into schema-ready records.

        At minimum, reduced data is required. Parquet and model data
        enrich the result with additional metadata.

        If the model contains multiple experiments (co-refinement) and
        no dataset_index was explicitly set on the model, the assembler
        will auto-detect the matching experiment by comparing Q arrays
        from the reduced data against each experiment's probe.

        Args:
            reduced: Parsed reduced reflectivity data (required)
            parquet: Parsed parquet metadata (optional)
            model: Parsed model JSON data (optional)
            environment_description: Optional description text for the environment record

        Returns:
            AssemblyResult with assembled records and any issues
        """
        result = AssemblyResult()

        if reduced is None:
            result.errors.append("Reduced data is required")
            return result

        result.reduced_file = reduced.file_path

        # Auto-detect dataset if model has multiple experiments and none was chosen
        if model is not None and model.dataset_index is None and model.num_datasets > 1:
            matched = self._auto_detect_dataset(model, reduced)
            if matched is not None:
                logger.info(
                    f"Auto-detected model dataset {matched + 1}/{model.num_datasets} "
                    f"by reflectivity matching"
                )
                model.select_dataset(matched)
                result.warnings.append(
                    f"Auto-selected model dataset {matched + 1} of "
                    f"{model.num_datasets} (matched by reflectivity data)"
                )
            else:
                result.warnings.append(
                    f"Model has {model.num_datasets} datasets but could not "
                    f"auto-detect match; using first dataset. "
                    f"Consider passing --model-dataset-index."
                )
                model.select_dataset(0)

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
                description_override=environment_description,
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

        # Step 5: Build Reflectivity Model record (needs measurement IDs from linking)
        if model is not None and model.raw_json is not None:
            measurement_ids = (
                [result.reflectivity["id"]] if result.reflectivity else []
            )
            result.reflectivity_model = build_reflectivity_model_record(
                model=model,
                measurement_ids=measurement_ids,
                warnings=result.warnings,
                errors=result.errors,
                needs_review=result.needs_review,
            )

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

    @staticmethod
    def _auto_detect_dataset(
        model: ModelData, reduced: ReducedData
    ) -> Optional[int]:
        """
        Auto-detect which experiment in a co-refinement model matches the reduced data.

        Uses two strategies:
        1. Q-range comparison (min/max) — works when experiments have different Q ranges
        2. R-value correlation — interpolates model R onto reduced Q grid and compares
           using mean absolute relative error. Works when Q grids overlap.

        Args:
            model: Parsed model with raw_json containing multiple experiments
            reduced: Parsed reduced data with Q and R arrays

        Returns:
            0-based experiment index, or None if no match found
        """
        if not reduced.q or len(reduced.q) == 0:
            return None

        from bisect import bisect_left

        reduced_q_min = min(reduced.q)
        reduced_q_max = max(reduced.q)

        best_idx: Optional[int] = None
        best_score = float("inf")

        for i in range(model.num_datasets):
            probe_q = model.get_probe_q(i)
            probe_r = model.get_probe_r(i)
            if not probe_q or not probe_r:
                continue

            # Strategy 1: Q-range score (relative difference in boundaries)
            q_min = min(probe_q)
            q_max = max(probe_q)
            min_diff = abs(reduced_q_min - q_min) / max(reduced_q_min, 1e-10)
            max_diff = abs(reduced_q_max - q_max) / max(reduced_q_max, 1e-10)
            q_range_score = min_diff + max_diff

            # Strategy 2: R-value comparison via linear interpolation
            # Interpolate model R at each reduced Q point
            r_score = float("inf")
            if reduced.r and len(reduced.r) == len(reduced.q) and len(probe_q) >= 2:
                # Ensure probe_q is sorted for interpolation
                if probe_q[0] <= probe_q[-1]:
                    sorted_q = probe_q
                    sorted_r = probe_r
                else:
                    pairs = sorted(zip(probe_q, probe_r))
                    sorted_q = [p[0] for p in pairs]
                    sorted_r = [p[1] for p in pairs]

                total_err = 0.0
                n_matched = 0
                for j, rq in enumerate(reduced.q):
                    # Only compare where reduced Q falls within model Q range
                    if rq < sorted_q[0] or rq > sorted_q[-1]:
                        continue
                    # Find interpolation position
                    pos = bisect_left(sorted_q, rq)
                    if pos == 0:
                        interp_r = sorted_r[0]
                    elif pos >= len(sorted_q):
                        interp_r = sorted_r[-1]
                    else:
                        # Linear interpolation
                        q0, q1 = sorted_q[pos - 1], sorted_q[pos]
                        r0, r1 = sorted_r[pos - 1], sorted_r[pos]
                        t = (rq - q0) / (q1 - q0) if q1 != q0 else 0.5
                        interp_r = r0 + t * (r1 - r0)

                    # Relative error (use abs(reduced R) to normalize)
                    rr = reduced.r[j]
                    denom = max(abs(rr), 1e-12)
                    total_err += abs(interp_r - rr) / denom
                    n_matched += 1

                if n_matched > 0:
                    r_score = total_err / n_matched

            # Combined score: prefer R-value match when Q ranges are similar
            if r_score < float("inf"):
                score = r_score  # R-value match is the primary discriminator
            else:
                score = q_range_score

            if score < best_score:
                best_score = score
                best_idx = i

        # Accept if R-value mean relative error < 10% or Q-range score < 0.5
        if best_idx is not None and best_score < 0.5:
            return best_idx

        return None
