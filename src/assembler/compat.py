"""
Schema compatibility utilities for raven_ai alignment.

This module provides utilities to validate that data-assembler output
is compatible with the raven_ai schema used for querying the lakehouse.

The raven_ai package is optional - validation only runs if installed.
"""

import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Track if raven_ai is available
_RAVEN_AI_AVAILABLE = False
_raven_models: dict = {}

try:
    from raven_ai.models.environment import Environment as RavenEnvironment
    from raven_ai.models.material import Material as RavenMaterial
    from raven_ai.models.measurement import Measurement as RavenMeasurement
    from raven_ai.models.measurement import Reflectivity as RavenReflectivity
    from raven_ai.models.sample import Sample as RavenSample

    _RAVEN_AI_AVAILABLE = True
    _raven_models = {
        "Measurement": RavenMeasurement,
        "Reflectivity": RavenReflectivity,
        "Sample": RavenSample,
        "Material": RavenMaterial,
        "Environment": RavenEnvironment,
    }
    logger.info("raven_ai available - schema validation enabled")
except ImportError:
    logger.debug("raven_ai not installed - schema validation disabled")


def is_raven_ai_available() -> bool:
    """Check if raven_ai is installed and available."""
    return _RAVEN_AI_AVAILABLE


def validate_against_raven(
    model_name: str,
    data: dict[str, Any],
    strict: bool = False,
) -> tuple[bool, Optional[str]]:
    """
    Validate a data dict against the corresponding raven_ai model.

    Args:
        model_name: Name of the model (e.g., "Reflectivity", "Sample")
        data: Dictionary of model fields
        strict: If True, raise exception on validation failure

    Returns:
        Tuple of (is_valid, error_message)

    Raises:
        ValueError: If strict=True and validation fails
        RuntimeError: If raven_ai is not installed
    """
    if not _RAVEN_AI_AVAILABLE:
        if strict:
            raise RuntimeError(
                "raven_ai not installed. Install with: pip install data-assembler[validation]"
            )
        return True, None  # Skip validation if not available

    raven_model = _raven_models.get(model_name)
    if not raven_model:
        return False, f"Unknown model: {model_name}"

    try:
        # Attempt to create raven_ai model from our data
        raven_model(**data)
        return True, None
    except Exception as e:
        error_msg = f"Schema compatibility error for {model_name}: {e}"
        if strict:
            raise ValueError(error_msg) from e
        return False, error_msg


def get_parquet_schema_mapping(model_name: str) -> dict[str, str]:
    """
    Get the mapping from Pydantic field names to Parquet column types.

    Args:
        model_name: Name of the model

    Returns:
        Dict mapping field names to PyArrow type strings
    """
    # Common fields for all DataModel subclasses
    base_fields = {
        "id": "string",
        "created_at": "timestamp[us, tz=UTC]",
        "is_deleted": "bool",
    }

    measurement_fields = {
        **base_fields,
        "proposal_number": "string",
        "facility": "string",  # Enum serialized as string
        "lab": "string",
        "probe": "string",  # Enum serialized as string
        "technique": "string",  # Enum serialized as string
        "technique_description": "string",
        "is_simulated": "bool",
        "run_title": "string",
        "run_number": "string",
        "run_start": "timestamp[us, tz=UTC]",
        "raw_file_path": "string",
        "instrument_name": "string",
        "sample_id": "string",
    }

    reflectivity_fields = {
        **measurement_fields,
        "q": "list<double>",
        "r": "list<double>",
        "dr": "list<double>",
        "dq": "list<double>",
        "measurement_geometry": "double",
        "reduction_time": "timestamp[us, tz=UTC]",
        "reduction_version": "string",
        "reduction_parameters": "string",  # JSON serialized
    }

    sample_fields = {
        **base_fields,
        "description": "string",
        "environment_ids": "list<string>",
        "substrate": "struct",  # Nested Layer
        "main_composition": "string",
        "geometry": "string",
        "layers": "list<struct>",  # List of Layer
    }

    environment_fields = {
        **base_fields,
        "description": "string",
        "ambient_medium": "string",
        "temperature": "double",
        "pressure": "double",
        "relative_humidity": "double",
        "measurement_ids": "list<string>",
    }

    schemas = {
        "Measurement": measurement_fields,
        "Reflectivity": reflectivity_fields,
        "Sample": sample_fields,
        "Environment": environment_fields,
    }

    return schemas.get(model_name, base_fields)


# Schema field mappings between data-assembler and raven_ai
FIELD_MAPPINGS = {
    "Measurement": {
        # data-assembler field -> raven_ai field
        "id": "Id",  # raven_ai uses capital Id for RavenDB
    },
    "Reflectivity": {
        "id": "Id",
    },
    "Sample": {
        "id": "Id",
    },
}


# Type transformation notes for documentation
TYPE_NOTES = """
Schema Type Differences and Parquet Mapping
============================================

1. ID Fields
   - raven_ai: `Id: Optional[str]` (RavenDB convention)
   - data-assembler: `id: Optional[str]` (Python convention)
   - Parquet: `id STRING` (lowercase)
   - Resolution: Transform on read/write if needed

2. Reflectivity Data Arrays
   - raven_ai: `q_1_angstrom: float`, `r: float`, etc. (per-point records)
   - data-assembler: `q: list[float]`, `r: list[float]` (full arrays)
   - Parquet: Can store either way
   - Resolution: data-assembler stores full curves

3. Enum Fields (facility, probe, technique)
   - raven_ai: `str` with documented allowed values
   - data-assembler: `Enum` types for type safety
   - Parquet: `STRING` (enum values as strings)
   - Resolution: Serialize enums to their string values

4. Nested Objects (Layer, Material)
   - Both: Pydantic BaseModel
   - Parquet: `STRUCT` type with nested fields

5. Datetime Fields
   - Both: `datetime` with timezone-aware defaults
   - Parquet: `TIMESTAMP[us, tz=UTC]`

6. Dict Fields (reduction_parameters)
   - data-assembler: `Optional[dict]` for provenance
   - Parquet: Serialize as JSON string
"""
