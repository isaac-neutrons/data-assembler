"""
Layer model for sample structure.

Represents a single layer in a layered sample stack.
"""

from typing import Optional

from pydantic import BaseModel, Field

from assembler.models.material import Material


class Layer(BaseModel):
    """
    A single layer in a sample stack.

    Represents one layer of material with a specific thickness
    and interface roughness.

    Attributes:
        name: Layer name/identifier (e.g., "Cu", "adhesion layer")
        material: Material composition and properties
        thickness: Layer thickness in Ångströms
        interface: Interface roughness (sigma) in Ångströms
    """

    name: Optional[str] = Field(
        default=None,
        description="Layer name or identifier",
    )

    material: Material = Field(
        ...,
        description="Material composition and properties",
    )

    thickness: float = Field(
        ...,
        description="Layer thickness in Ångströms (Å)",
        ge=0,
    )

    interface: Optional[float] = Field(
        default=None,
        description="Interface roughness (sigma) in Ångströms (Å)",
        ge=0,
    )

    @classmethod
    def from_model_data(
        cls,
        name: str,
        thickness: float,
        interface: float,
        rho: float,
        irho: float = 0.0,
    ) -> "Layer":
        """
        Create a Layer from refl1d model parameters.

        Args:
            name: Layer name from model
            thickness: Thickness in Å
            interface: Interface roughness in Å
            rho: SLD value
            irho: Imaginary SLD value

        Returns:
            Layer instance
        """
        return cls(
            name=name,
            material=Material.from_sld(name, sld=rho, isld=irho),
            thickness=thickness,
            interface=interface,
        )

    def __str__(self) -> str:
        """String representation."""
        name = self.name or self.material.composition
        return f"{name}: {self.thickness:.1f} Å"

    @property
    def is_substrate(self) -> bool:
        """Check if this layer is a substrate (zero thickness)."""
        return self.thickness == 0 and self.name and "si" in self.name.lower()

    @property
    def is_ambient(self) -> bool:
        """Check if this layer is ambient/solvent (zero thickness at top)."""
        return self.thickness == 0 and not self.is_substrate
