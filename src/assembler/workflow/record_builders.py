"""
Record builders for the data assembly workflow.

Each builder converts parsed data directly into schema-ready records
(dictionaries matching writers/schemas.py).
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional, Type

from assembler.instruments import Instrument, InstrumentRegistry
from assembler.parsers import ModelData, ParquetData, ReducedData

logger = logging.getLogger(__name__)


# Mapping of instrument prefixes to facilities
FACILITY_INSTRUMENTS = {
    "REF_L": "SNS",
    "REF_M": "SNS",
    "BL-4B": "SNS",
    "CG-1D": "HFIR",
}


def detect_facility(instrument: Optional[str]) -> str:
    """
    Detect facility from instrument name.

    Args:
        instrument: Instrument identifier string

    Returns:
        The detected facility string (defaults to 'SNS')
    """
    if instrument:
        instrument_upper = instrument.upper()
        for name, facility in FACILITY_INSTRUMENTS.items():
            if name in instrument_upper:
                return facility
    return "SNS"


def build_reflectivity_record(
    reduced: ReducedData,
    parquet: Optional[ParquetData],
    warnings: list[str],
    errors: list[str],
    needs_review: dict[str, Any],
    instrument_handler: Optional[Type[Instrument]] = None,
) -> Optional[dict[str, Any]]:
    """
    Build a reflectivity record from reduced and parquet data.

    The reduced data provides Q, R, dR, dQ arrays and reduction metadata.
    The parquet data provides run metadata (proposal, title, timestamps).

    Args:
        reduced: Parsed reduced reflectivity data (required)
        parquet: Parsed parquet metadata (optional, enriches result)
        warnings: List to append warnings to
        errors: List to append errors to
        needs_review: Dict to record fields needing review
        instrument_handler: Optional specific instrument handler to use

    Returns:
        Dict matching REFLECTIVITY_SCHEMA, or None on error
    """
    try:
        # Get metadata from parquet if available, else from reduced header
        if parquet and parquet.metadata:
            meta = parquet.metadata
            proposal_number = meta.experiment_identifier or "UNKNOWN"
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
            proposal_number = reduced.experiment_id or "UNKNOWN"
            run_number = str(reduced.run_number) if reduced.run_number else "UNKNOWN"
            run_title = reduced.run_title or "Unknown"
            instrument = "REF_L"  # Default assumption

            # Get instrument handler
            if instrument_handler is None:
                instrument_handler = InstrumentRegistry.get_handler(instrument)

            facility = instrument_handler.defaults.facility
            run_start = reduced.run_start_time or datetime.now(timezone.utc)
            raw_file_path = None

            if proposal_number == "UNKNOWN":
                warnings.append("Proposal number not found, using 'UNKNOWN'")

        logger.debug(f"Using instrument handler: {instrument_handler.name}")

        # Build reduction parameters dict if any are set
        reduction_parameters = None
        if any([reduced.q_summing, reduced.tof_weighted, reduced.bck_in_q, reduced.theta_offset]):
            reduction_parameters = json.dumps({
                "q_summing": reduced.q_summing,
                "tof_weighted": reduced.tof_weighted,
                "bck_in_q": reduced.bck_in_q,
                "theta_offset": reduced.theta_offset,
            })

        # Get measurement geometry from first run if available
        measurement_geometry = None
        if reduced.runs:
            measurement_geometry = reduced.runs[0].two_theta

        # Build the record matching REFLECTIVITY_SCHEMA
        record = {
            # Base fields
            "id": None,  # Will be set by database or caller
            "created_at": datetime.now(timezone.utc),
            "is_deleted": False,
            # Measurement fields
            "proposal_number": proposal_number,
            "facility": facility,
            "laboratory": instrument_handler.defaults.laboratory if hasattr(instrument_handler.defaults, 'laboratory') else "ORNL",
            "probe": "neutrons",
            "technique": "reflectivity",
            "technique_description": None,
            "is_simulated": False,
            "run_title": run_title,
            "run_number": run_number,
            "run_start": run_start,
            "raw_file_path": raw_file_path,
            "instrument_name": instrument,
            "sample_id": None,
            # Reflectivity-specific fields as nested struct
            "reflectivity": {
                "measurement_geometry": measurement_geometry,
                "reduction_time": reduced.reduction_time,
                "reduction_version": reduced.reduction_version,
                "reduction_parameters": reduction_parameters,
                "q": reduced.q,
                "r": reduced.r,
                "dr": reduced.dr,
                "dq": reduced.dq,
            },
        }

        # Run instrument-specific validation (creates a temp object for validation)
        # We pass the record data for validation
        validation_warnings = _validate_reflectivity_data(record, instrument_handler)
        for warning in validation_warnings:
            warnings.append(warning)

        return record

    except Exception as e:
        errors.append(f"Failed to build Reflectivity record: {e}")
        logger.exception("Error building Reflectivity record")
        return None


def build_environment_record(
    parquet: ParquetData,
    warnings: list[str],
    errors: list[str],
    needs_review: dict[str, Any],
    instrument_handler: Optional[Type[Instrument]] = None,
) -> Optional[dict[str, Any]]:
    """
    Build an environment record from parquet daslogs.

    Uses instrument-specific handlers to extract environment data
    from DAS logs with appropriate naming conventions.

    Args:
        parquet: Parsed parquet data containing daslogs
        warnings: List to append warnings to
        errors: List to append errors to
        needs_review: Dict to record fields needing review
        instrument_handler: Optional specific instrument handler to use

    Returns:
        Dict matching ENVIRONMENT_SCHEMA, or None on error
    """
    try:
        # Get the appropriate instrument handler
        if instrument_handler is None:
            instrument_handler = InstrumentRegistry.get_handler(parquet.instrument_id)

        logger.debug(f"Using instrument handler: {instrument_handler.name}")

        # Extract environment using instrument-specific logic
        extracted = instrument_handler.extract_environment(parquet)

        # Also extract additional metadata for logging
        metadata = instrument_handler.extract_metadata(parquet)
        if metadata.extra:
            logger.debug(f"Instrument metadata: {metadata.extra}")

        # Generate description
        if extracted.description:
            description = extracted.description
        else:
            description = _generate_environment_description(
                temperature=extracted.temperature,
                pressure=extracted.pressure,
                sample_name=parquet.sample.name if parquet.sample else None,
            )

        # Build the record matching ENVIRONMENT_SCHEMA
        record = {
            # Base fields
            "id": None,
            "created_at": datetime.now(timezone.utc),
            "is_deleted": False,
            # Environment fields
            "description": description,
            "ambient_medium": None,  # Would need material extraction
            "temperature": extracted.temperature,
            "pressure": extracted.pressure,
            "relative_humidity": extracted.relative_humidity,
            "measurement_ids": [],
        }

        # Flag for review if key values missing
        if extracted.temperature is None:
            needs_review["environment_temperature"] = (
                f"Temperature not found in daslogs (checked: {instrument_handler.name} sensors)"
            )

        return record

    except Exception as e:
        errors.append(f"Failed to build Environment record: {e}")
        logger.exception("Error building Environment record")
        return None


def build_sample_record(
    model: ModelData,
    warnings: list[str],
    errors: list[str],
    needs_review: dict[str, Any],
) -> Optional[dict[str, Any]]:
    """
    Build a sample record from model JSON data.

    Converts the layer stack from the fitting model into
    the sample schema format.

    Args:
        model: Parsed model JSON with layer definitions
        warnings: List to append warnings to
        errors: List to append errors to
        needs_review: Dict to record fields needing review

    Returns:
        Dict matching SAMPLE_SCHEMA, or None on error
    """
    try:
        layers_structs = []
        layers_json_list = []
        substrate = None
        substrate_json = None

        for i, model_layer in enumerate(model.layers):
            layer_dict = {
                "name": model_layer.name,
                "thickness": model_layer.thickness,
                "interface": model_layer.interface,
                "material": {
                    "name": model_layer.material.name,
                    "rho": model_layer.material.rho,
                    "irho": model_layer.material.irho,
                },
            }

            # Last layer with zero thickness is substrate
            if i == len(model.layers) - 1 and model_layer.thickness == 0:
                substrate = layer_dict
                substrate_json = json.dumps(layer_dict)
            else:
                layers_json_list.append(layer_dict)
                # Build struct for schema
                layers_structs.append({
                    "layer_number": len(layers_structs) + 1,
                    "material": model_layer.material.name,
                    "thickness": model_layer.thickness,
                    "roughness": model_layer.interface,
                    "sld": model_layer.material.rho,
                })

            # Flag generic layer names for review
            if model_layer.name.lower() in ["material", "layer", "film"]:
                needs_review[f"layer_{i}_name"] = (
                    f"Generic layer name '{model_layer.name}' - "
                    f"SLD: {model_layer.material.rho:.2f}, "
                    f"thickness: {model_layer.thickness:.1f} Å"
                )

        # Determine main composition from thickest layer
        main_composition = _determine_main_composition(layers_json_list)

        # Generate description
        ambient_name = model.ambient.material.name if model.ambient else "air"
        description = f"{main_composition} in {ambient_name}"
        if substrate:
            description += f" on {substrate['material']['name']}"

        # Build the record matching SAMPLE_SCHEMA
        record = {
            # Base fields
            "id": None,
            "created_at": datetime.now(timezone.utc),
            "is_deleted": False,
            # Sample fields
            "description": description,
            "main_composition": main_composition,
            "geometry": None,
            "environment_ids": [],
            "layers_json": json.dumps(layers_json_list) if layers_json_list else None,
            "layers": layers_structs,
            "substrate_json": substrate_json,
        }

        return record

    except Exception as e:
        errors.append(f"Failed to build Sample record: {e}")
        logger.exception("Error building Sample record")
        return None


# --- Private helper functions ---


def _validate_reflectivity_data(
    record: dict[str, Any],
    instrument_handler: Type[Instrument],
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


def _determine_main_composition(layers: list[dict]) -> str:
    """Determine main composition from thickest layer."""
    main_composition = "Unknown"
    max_thickness = 0

    for layer in layers:
        thickness = layer.get("thickness", 0)
        if thickness and thickness > max_thickness:
            max_thickness = thickness
            material = layer.get("material", {})
            main_composition = material.get("name", "Unknown") if isinstance(material, dict) else "Unknown"

    return main_composition


def _generate_environment_description(
    temperature: Optional[float] = None,
    pressure: Optional[float] = None,
    sample_name: Optional[str] = None,
) -> str:
    """Generate human-readable environment description."""
    parts = []

    if sample_name:
        parts.append(f"Sample: {sample_name}")

    if temperature is not None:
        parts.append(f"T={temperature:.1f}K")

    if pressure is not None:
        if pressure < 1000:
            parts.append(f"P={pressure:.1f}Pa")
        else:
            parts.append(f"P={pressure / 1000:.2f}kPa")

    if parts:
        return ", ".join(parts)
    return "Standard conditions"
