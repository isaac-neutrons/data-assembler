"""
Sample record builder.
"""

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from assembler.parsers.model_parser import ModelData
from assembler.workflow.builders.utils import determine_main_composition

logger = logging.getLogger(__name__)


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
                layers_structs.append(
                    {
                        "layer_number": len(layers_structs) + 1,
                        "material": model_layer.material.name,
                        "thickness": model_layer.thickness,
                        "roughness": model_layer.interface,
                        "sld": model_layer.material.rho,
                    }
                )

            # Flag generic layer names for review
            if model_layer.name.lower() in ["material", "layer", "film"]:
                needs_review[f"layer_{i}_name"] = (
                    f"Generic layer name '{model_layer.name}' - "
                    f"SLD: {model_layer.material.rho:.2f}, "
                    f"thickness: {model_layer.thickness:.1f} Å"
                )

        # Determine main composition from thickest layer
        main_composition = determine_main_composition(layers_json_list)

        # Generate description
        ambient_name = model.ambient.material.name if model.ambient else "air"
        description = f"{main_composition} in {ambient_name}"
        if substrate:
            description += f" on {substrate['material']['name']}"

        # Build the record matching SAMPLE_SCHEMA
        record = {
            # Base fields
            "id": str(uuid.uuid4()),
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
