"""
Utility functions and constants for record builders.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


def generate_environment_description(
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
