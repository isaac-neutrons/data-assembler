"""
Material model for sample composition.

Represents a material with its chemical composition and physical properties.
"""

from typing import Optional

from pydantic import BaseModel, Field


class Material(BaseModel):
    """
    Material composition and properties.

    Used in Layer definitions to describe the material at each layer
    of a sample stack.

    Attributes:
        composition: Chemical formula (e.g., "Cu", "SiO2", "D2O")
        mass: Sample mass in grams (optional)
        density: Material density in g/cm³ (optional)
        sld: Scattering length density in 10⁻⁶ Å⁻² (optional, for neutrons)
        isld: Imaginary SLD (absorption) in 10⁻⁶ Å⁻² (optional)
    """

    composition: str = Field(
        ...,
        description="Chemical formula (e.g., 'Cu', 'SiO2', 'D2O')",
        min_length=1,
    )

    mass: Optional[float] = Field(
        default=None,
        description="Sample mass in grams",
        ge=0,
    )

    density: Optional[float] = Field(
        default=None,
        description="Material density in g/cm³",
        gt=0,
    )

    # Extended fields for reflectometry
    sld: Optional[float] = Field(
        default=None,
        description="Scattering length density (rho) in 10⁻⁶ Å⁻²",
        alias="rho",
    )

    isld: Optional[float] = Field(
        default=None,
        description="Imaginary SLD (absorption, irho) in 10⁻⁶ Å⁻²",
        alias="irho",
    )

    @classmethod
    def from_sld(
        cls,
        name: str,
        sld: float,
        isld: float = 0.0,
        density: Optional[float] = None,
    ) -> "Material":
        """
        Create a Material from SLD values.

        Args:
            name: Material name/composition
            sld: Scattering length density (rho)
            isld: Imaginary SLD (irho), defaults to 0
            density: Optional density

        Returns:
            Material instance
        """
        return cls(
            composition=name,
            sld=sld,
            isld=isld,
            density=density,
        )

    def __str__(self) -> str:
        """String representation."""
        if self.sld is not None:
            return f"{self.composition} (ρ={self.sld:.2f})"
        return self.composition
