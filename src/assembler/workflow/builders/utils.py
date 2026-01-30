"""
Utility functions and constants for record builders.
"""

import logging

logger = logging.getLogger(__name__)


def determine_main_composition(layers: list[dict]) -> str:
    """Determine main composition from thickest layer."""
    main_composition = "Unknown"
    max_thickness = 0

    for layer in layers:
        thickness = layer.get("thickness", 0)
        if thickness and thickness > max_thickness:
            max_thickness = thickness
            material = layer.get("material", {})
            main_composition = (
                material.get("name", "Unknown") if isinstance(material, dict) else "Unknown"
            )

    return main_composition
