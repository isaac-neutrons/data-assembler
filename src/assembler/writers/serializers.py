"""
Serialization utilities for converting Pydantic models to Parquet records.

This module handles the transformation of complex Python types (enums,
datetimes, nested structures) into Parquet-compatible formats.
"""

import json
from datetime import datetime
from enum import Enum
from typing import Any

from assembler.models.environment import Environment
from assembler.models.measurement import Reflectivity
from assembler.models.sample import Sample


def serialize_value(value: Any) -> Any:
    """
    Serialize a value to a Parquet-compatible type.

    Handles:
    - Enums -> string values
    - datetime -> preserved as-is (PyArrow handles conversion)
    - dicts/Pydantic models -> JSON strings
    - lists of primitives -> preserved as-is
    - lists of complex objects -> JSON strings
    - None -> preserved as None

    Args:
        value: Any Python value to serialize

    Returns:
        Parquet-compatible representation
    """
    if value is None:
        return None
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value
    if isinstance(value, dict):
        return json.dumps(value)
    if isinstance(value, list):
        # Check if list contains complex objects
        if value and hasattr(value[0], "model_dump"):
            return json.dumps([item.model_dump() for item in value])
        if value and isinstance(value[0], dict):
            return json.dumps(value)
        # List of primitives - keep as-is for PyArrow
        return value
    if hasattr(value, "model_dump"):
        return json.dumps(value.model_dump())
    return value


def reflectivity_to_record(measurement: Reflectivity) -> dict[str, Any]:
    """
    Convert a Reflectivity measurement to a flat dict for Parquet.

    Args:
        measurement: The Reflectivity model instance

    Returns:
        Dict with keys matching REFLECTIVITY_SCHEMA
    """
    # Handle facility - could be Enum or string
    facility = measurement.facility
    if hasattr(facility, "value"):
        facility = facility.value

    return {
        # Base fields
        "id": str(measurement.id),
        "created_at": measurement.created_at,
        "is_deleted": measurement.is_deleted,
        # Measurement fields
        "proposal_number": measurement.proposal_number,
        "facility": facility,
        "lab": measurement.lab,
        "probe": serialize_value(measurement.probe),
        "technique": serialize_value(measurement.technique),
        "technique_description": measurement.technique_description,
        "is_simulated": measurement.is_simulated,
        "run_title": measurement.run_title,
        "run_number": measurement.run_number,
        "run_start": measurement.run_start,
        "raw_file_path": measurement.raw_file_path,
        "instrument_name": measurement.instrument_name,
        "sample_id": str(measurement.sample_id) if measurement.sample_id else None,
        # Reflectivity fields
        "q": measurement.q,
        "r": measurement.r,
        "dr": measurement.dr,
        "dq": measurement.dq,
        "measurement_geometry": measurement.measurement_geometry,
        "reduction_time": measurement.reduction_time,
        "reduction_version": measurement.reduction_version,
        "reduction_parameters": serialize_value(measurement.reduction_parameters),
    }


def sample_to_record(sample: Sample) -> dict[str, Any]:
    """
    Convert a Sample to a flat dict for Parquet.

    Args:
        sample: The Sample model instance

    Returns:
        Dict with keys matching SAMPLE_SCHEMA
    """
    # layers_json and substrate_json are stored as JSON strings in schema
    layers_json = None
    if sample.layers:
        if hasattr(sample.layers[0], "model_dump"):
            layers_json = json.dumps([layer.model_dump() for layer in sample.layers])
        else:
            layers_json = json.dumps(sample.layers)

    substrate_json = None
    if sample.substrate:
        if hasattr(sample.substrate, "model_dump"):
            substrate_json = json.dumps(sample.substrate.model_dump())
        else:
            substrate_json = json.dumps(sample.substrate)

    return {
        "id": str(sample.id),
        "created_at": sample.created_at,
        "is_deleted": sample.is_deleted,
        "description": sample.description,
        "main_composition": sample.main_composition,
        "geometry": serialize_value(sample.geometry),
        "environment_ids": [str(eid) for eid in sample.environment_ids]
        if sample.environment_ids
        else [],
        "layers_json": layers_json,
        "substrate_json": substrate_json,
    }


def environment_to_record(env: Environment) -> dict[str, Any]:
    """
    Convert an Environment to a flat dict for Parquet.

    Args:
        env: The Environment model instance

    Returns:
        Dict with keys matching ENVIRONMENT_SCHEMA
    """
    # Handle ambient_medium - could be Material object or None
    ambient_medium = env.ambient_medium
    if ambient_medium is not None:
        if hasattr(ambient_medium, "name"):
            ambient_medium = ambient_medium.name
        elif hasattr(ambient_medium, "model_dump"):
            ambient_medium = serialize_value(ambient_medium)
        # else keep as-is if it's already a string

    return {
        "id": str(env.id),
        "created_at": env.created_at,
        "is_deleted": env.is_deleted,
        "description": env.description,
        "ambient_medium": ambient_medium,
        "temperature": env.temperature,
        "pressure": env.pressure,
        "relative_humidity": env.relative_humidity,
        "measurement_ids": [str(mid) for mid in env.measurement_ids] if env.measurement_ids else [],
        "temperature_min": env.temperature_min,
        "temperature_max": env.temperature_max,
        "magnetic_field": env.magnetic_field,
        # source_daslogs is stored as JSON string in schema
        "source_daslogs": json.dumps(env.source_daslogs) if env.source_daslogs else None,
    }
