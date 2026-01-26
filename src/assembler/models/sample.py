"""
Sample model for experimental samples.

Represents a complete sample with its layer structure and metadata.
"""

from typing import Optional

from pydantic import Field

from assembler.models.base import DataModel
from assembler.models.layer import Layer


class Sample(DataModel):
    """
    A complete sample with layer structure.

    Represents the physical sample measured in a reflectometry experiment,
    including its layer stack, substrate, and environmental context.

    Attributes:
        description: Human-readable sample description
        environment_ids: Links to Environment documents
        substrate: The substrate layer (bottom of stack)
        main_composition: Primary material chemical formula
        geometry: Sample geometry description
        layers: Ordered list of layers (top to bottom, excluding substrate)
    """

    description: str = Field(
        ...,
        description="Human-readable sample description",
    )

    environment_ids: list[str] = Field(
        default_factory=list,
        description="IDs of related Environment documents",
        min_length=0,  # Changed from 1 to allow creation before linking
    )

    substrate: Optional[Layer] = Field(
        default=None,
        description="Substrate layer (bottom of stack)",
    )

    main_composition: Optional[str] = Field(
        default=None,
        description="Primary material chemical formula (validated with periodictable)",
    )

    geometry: Optional[str] = Field(
        default=None,
        description="Sample geometry description",
    )

    layers: list[Layer] = Field(
        default_factory=list,
        description="Ordered list of layers (top to bottom, excluding substrate)",
        min_length=0,
        max_length=10,  # Reasonable limit for thin film samples
    )

    # Extended fields for provenance
    source_model_file: Optional[str] = Field(
        default=None,
        description="Path to the model file this sample was extracted from",
    )

    @classmethod
    def from_layer_stack(
        cls,
        layers: list[Layer],
        description: str,
        **kwargs,
    ) -> "Sample":
        """
        Create a Sample from a list of layers.

        Automatically identifies substrate (bottom layer with zero thickness)
        and separates it from the layer stack.

        Args:
            layers: Full layer stack (top to bottom)
            description: Sample description
            **kwargs: Additional Sample fields

        Returns:
            Sample instance
        """
        # Find substrate (typically Si at bottom with zero thickness)
        substrate = None
        film_layers = []

        for i, layer in enumerate(layers):
            if i == len(layers) - 1 and layer.is_substrate:
                substrate = layer
            else:
                film_layers.append(layer)

        # Determine main composition from thickest layer
        main_comp = None
        if film_layers:
            thickest = max(film_layers, key=lambda l: l.thickness)
            main_comp = thickest.material.composition

        return cls(
            description=description,
            substrate=substrate,
            layers=film_layers,
            main_composition=main_comp,
            **kwargs,
        )

    @property
    def total_thickness(self) -> float:
        """Calculate total film thickness (excluding substrate)."""
        return sum(layer.thickness for layer in self.layers)

    @property
    def layer_summary(self) -> str:
        """Get a summary string of the layer structure."""
        parts = []
        for layer in self.layers:
            name = layer.name or layer.material.composition
            parts.append(f"{name}({layer.thickness:.0f}Å)")
        if self.substrate:
            parts.append(f"{self.substrate.name or 'substrate'}")
        return " / ".join(parts) if parts else "empty"

    def __str__(self) -> str:
        """String representation."""
        return f"Sample: {self.description} [{self.layer_summary}]"
