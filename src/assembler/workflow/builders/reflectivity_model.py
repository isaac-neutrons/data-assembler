"""
Reflectivity model record builder.

Builds a record matching REFLECTIVITY_MODEL_SCHEMA from a parsed
refl1d/bumps model JSON file.
"""

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from assembler.parsers.model_parser import ModelData

logger = logging.getLogger(__name__)


def build_reflectivity_model_record(
    model: ModelData,
    measurement_ids: list[str],
    warnings: list[str],
    errors: list[str],
    needs_review: dict[str, Any],
) -> Optional[dict[str, Any]]:
    """
    Build a reflectivity model record from parsed model data.

    Extracts software provenance, fit summary, and layer information
    from the parsed model. Stores the full JSON for reproducibility.

    Args:
        model: Parsed model JSON data (must include raw_json)
        measurement_ids: List of reflectivity measurement IDs this model refers to
        warnings: List to append warnings to
        errors: List to append errors to
        needs_review: Dict to record fields needing review

    Returns:
        Dict matching REFLECTIVITY_MODEL_SCHEMA, or None on error
    """
    try:
        raw = model.raw_json or {}

        # Extract model name from the fit problem object
        obj = raw.get("object", {})
        model_name = obj.get("name") or None

        # Extract software/library info
        libraries = raw.get("libraries", {})
        # Prefer refl1d as the primary software, fall back to bumps
        if "refl1d" in libraries:
            software = "refl1d"
            software_version = libraries["refl1d"].get("version", "unknown")
            schema_version = libraries["refl1d"].get("schema_version", "unknown")
        elif "bumps" in libraries:
            software = "bumps"
            software_version = libraries["bumps"].get("version", "unknown")
            schema_version = libraries["bumps"].get("schema_version", "unknown")
        else:
            software = "unknown"
            software_version = "unknown"
            schema_version = raw.get("$schema", "unknown")
            warnings.append("Could not determine modeling software from libraries")

        # Count experiments
        models_list = obj.get("models", [])
        num_experiments = len(models_list)

        # Count parameters (references dict holds all parameters)
        references = raw.get("references", {})
        num_parameters = len(references)
        num_free_parameters = sum(
            1
            for ref in references.values()
            if isinstance(ref, dict) and not ref.get("fixed", True)
        )

        # Build layers list from the parsed ModelData (first experiment)
        layers_structs = []
        for i, layer in enumerate(model.layers):
            layers_structs.append(
                {
                    "layer_number": i + 1,
                    "name": layer.name,
                    "thickness": layer.thickness,
                    "interface": layer.interface,
                    "sld": layer.material.rho,
                    "isld": layer.material.irho,
                }
            )

        # Serialize the full JSON for the model_json column
        model_json = json.dumps(raw, default=str)

        record = {
            # Base fields
            "id": str(uuid.uuid4()),
            "created_at": datetime.now(timezone.utc),
            "is_deleted": False,
            # Relationships
            "measurement_ids": measurement_ids,
            # Model identification
            "model_name": model_name,
            "model_file_path": model.file_path or None,
            # Software provenance
            "software": software,
            "software_version": software_version,
            "schema_version": schema_version,
            # Fit summary
            "num_experiments": num_experiments,
            "dataset_index": model.dataset_index,
            "num_parameters": num_parameters,
            "num_free_parameters": num_free_parameters,
            # Layers
            "layers": layers_structs,
            # Full JSON
            "model_json": model_json,
        }

        return record

    except Exception as e:
        errors.append(f"Failed to build Reflectivity Model record: {e}")
        logger.exception("Error building Reflectivity Model record")
        return None
