"""
Reflectivity record builder.
"""

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Optional, Type

from assembler.instruments import Instrument, InstrumentRegistry
from assembler.parsers.model_parser import ModelData
from assembler.parsers.parquet_parser import ParquetData
from assembler.parsers.reduced_parser import ReducedData

logger = logging.getLogger(__name__)


def build_reflectivity_record(
    reduced: ReducedData,
    parquet: Optional[ParquetData],
    warnings: list[str],
    errors: list[str],
    needs_review: dict[str, Any],
    instrument_handler: Optional[Type[Instrument]] = None,
    model: Optional[ModelData] = None,
) -> Optional[dict[str, Any]]:
    """
    Build a reflectivity record from reduced and parquet data.

    The reduced data provides Q, R, dR, dQ arrays and reduction metadata.
    The parquet data provides run metadata (proposal, title, timestamps).
    The model data provides measurement geometry (front/back reflection).

    Args:
        reduced: Parsed reduced reflectivity data (required)
        parquet: Parsed parquet metadata (optional, enriches result)
        warnings: List to append warnings to
        errors: List to append errors to
        needs_review: Dict to record fields needing review
        instrument_handler: Optional specific instrument handler to use
        model: Optional model data for geometry determination

    Returns:
        Dict matching REFLECTIVITY_SCHEMA, or None on error
    """
    try:
        # Get metadata from parquet if available, else from reduced header
        if parquet and parquet.metadata:
            meta = parquet.metadata
            proposal_number = meta.experiment_identifier or "Unknown"
            run_number = str(meta.run_number)
            run_title = meta.title or reduced.run_title or "Unknown"
            instrument = meta.instrument_id

            # Get instrument handler
            if instrument_handler is None:
                instrument_handler = InstrumentRegistry.get_handler(instrument)

            facility = instrument_handler.defaults.facility

            # Parse start time
            if meta.start_time:
                try:
                    run_start = datetime.fromisoformat(meta.start_time.replace("Z", "+00:00"))
                except ValueError:
                    run_start = datetime.now(timezone.utc)
                    warnings.append(f"Could not parse start_time: {meta.start_time}")
            else:
                run_start = datetime.now(timezone.utc)

            raw_file_path = meta.source_path
        else:
            # Fall back to reduced file header
            proposal_number = reduced.experiment_id or "Unknown"
            run_number = str(reduced.run_number) if reduced.run_number else "Unknown"
            run_title = reduced.run_title or "Unknown"
            instrument = "Unknown"

            # Get instrument handler
            if instrument_handler is None:
                instrument_handler = InstrumentRegistry.get_handler(instrument)

            facility = instrument_handler.defaults.facility
            run_start = reduced.run_start_time or datetime.now(timezone.utc)
            raw_file_path = None

            if proposal_number == "Unknown":
                warnings.append("Proposal number not found, using 'Unknown'")

        logger.debug(f"Using instrument handler: {instrument_handler.name}")

        # Determine measurement geometry from model layer order
        # If first layer is ambient (thickness=0, typically air) -> back reflection
        # If first layer is substrate or film -> front reflection
        measurement_geometry = None
        if model and model.layers:
            first_layer = model.layers[0]
            # Check if first layer is ambient (zero thickness, typically "air" or similar)
            if first_layer.thickness == 0:
                # First layer is ambient medium -> beam enters from ambient side
                measurement_geometry = "back reflection"
            else:
                # First layer has thickness -> beam reflects from front
                measurement_geometry = "front reflection"
        else:
            needs_review["measurement_geometry"] = (
                "Could not determine geometry - no model data provided"
            )

        # Build the record matching REFLECTIVITY_SCHEMA
        record = {
            # Base fields
            "id": str(uuid.uuid4()),
            "created_at": datetime.now(timezone.utc),
            "is_deleted": False,
            # Relationship fields (to be linked by assembler)
            "sample_id": None,
            "environment_id": None,
            # Measurement fields
            "proposal_number": proposal_number,
            "facility": facility,
            "laboratory": instrument_handler.defaults.laboratory,
            "probe": instrument_handler.defaults.probe,
            "technique": instrument_handler.defaults.technique,
            "technique_description": instrument_handler.defaults.technique_description,
            "is_simulated": False,
            "run_title": run_title,
            "run_number": run_number,
            "run_start": run_start,
            "raw_file_path": raw_file_path,
            "instrument_name": instrument,
            # Reflectivity-specific fields as nested struct
            "reflectivity": {
                "measurement_geometry": measurement_geometry,
                "reduction_time": reduced.reduction_time,
                "reduction_version": reduced.reduction_version,
                "q": reduced.q,
                "r": reduced.r,
                "dr": reduced.dr,
                "dq": reduced.dq,
            },
        }

        # Run instrument-specific validation (creates a temp object for validation)
        # We pass the record data for validation
        validation_warnings = _validate_reflectivity_data(record)
        for warning in validation_warnings:
            warnings.append(warning)

        return record

    except Exception as e:
        errors.append(f"Failed to build Reflectivity record: {e}")
        logger.exception("Error building Reflectivity record")
        return None


def _validate_reflectivity_data(
    record: dict[str, Any],
) -> list[str]:
    """Validate reflectivity data and return warnings."""
    warnings = []
    refl_data = record.get("reflectivity", {})

    q = refl_data.get("q", [])
    r = refl_data.get("r", [])
    dr = refl_data.get("dr", [])
    dq = refl_data.get("dq", [])

    # Check array lengths match
    if not (len(q) == len(r) == len(dr) == len(dq)):
        warnings.append(
            f"Array length mismatch: q={len(q)}, r={len(r)}, dr={len(dr)}, dq={len(dq)}"
        )

    # Check for reasonable data size
    if len(q) < 10:
        warnings.append(f"Only {len(q)} data points - unusually small dataset")

    # Check Q range
    if q:
        q_min, q_max = min(q), max(q)
        if q_max < 0.01:
            warnings.append(f"Q range very small: {q_min:.4f} - {q_max:.4f} Å⁻¹")

    return warnings
