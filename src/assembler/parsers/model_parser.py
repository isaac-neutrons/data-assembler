"""
Parser for refl1d/bumps model JSON files.

Parses the JSON format used by refl1d and bumps for fitted models,
extracting layer structures, materials, and fit parameters.
"""

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional


@dataclass
class ModelMaterial:
    """Material from a refl1d model."""

    name: str
    rho: float  # SLD
    irho: float = 0.0  # Imaginary SLD (absorption)

    @classmethod
    def from_json(cls, data: dict, references: dict) -> "ModelMaterial":
        """Create from JSON data with reference resolution."""
        name = data.get("name", "unknown")

        rho = cls._resolve_parameter(data.get("rho"), references)
        irho = cls._resolve_parameter(data.get("irho"), references)

        return cls(name=name, rho=rho, irho=irho)

    @staticmethod
    def _resolve_parameter(param: Any, references: dict) -> float:
        """Resolve a parameter value, following references if needed."""
        if param is None:
            return 0.0

        if isinstance(param, (int, float)):
            return float(param)

        if isinstance(param, dict):
            # Check if it's a reference
            if param.get("__class__") == "Reference":
                ref_id = param.get("id")
                if ref_id and ref_id in references:
                    ref_data = references[ref_id]
                    return ModelMaterial._extract_value(ref_data)

            # Direct value
            return ModelMaterial._extract_value(param)

        return 0.0

    @staticmethod
    def _extract_value(param_data: dict) -> float:
        """Extract the numeric value from a parameter dict."""
        # Check for slot.value
        slot = param_data.get("slot", {})
        if isinstance(slot, dict):
            value = slot.get("value")
            if value is not None:
                return float(value)

        # Direct value
        value = param_data.get("value")
        if value is not None:
            return float(value)

        return 0.0


@dataclass
class ModelLayer:
    """Layer from a refl1d model."""

    name: str
    thickness: float
    interface: float
    material: ModelMaterial

    @classmethod
    def from_json(cls, data: dict, references: dict) -> "ModelLayer":
        """Create from JSON data with reference resolution."""
        name = data.get("name", "unknown")

        thickness = ModelMaterial._resolve_parameter(data.get("thickness"), references)
        interface = ModelMaterial._resolve_parameter(data.get("interface"), references)

        # Parse material
        material_data = data.get("material", {})
        material = ModelMaterial.from_json(material_data, references)

        return cls(
            name=name,
            thickness=thickness,
            interface=interface,
            material=material,
        )


@dataclass
class FitParameter:
    """A fitted parameter from the model."""

    id: str
    name: str
    value: float
    fixed: bool
    bounds: Optional[tuple[float, float]] = None

    @classmethod
    def from_json(cls, param_id: str, data: dict) -> "FitParameter":
        """Create from JSON reference data."""
        name = data.get("name", param_id)
        fixed = data.get("fixed", True)

        # Extract value from slot
        slot = data.get("slot", {})
        if isinstance(slot, dict):
            value = slot.get("value", 0.0)
        else:
            value = 0.0

        # Extract bounds
        bounds_data = data.get("bounds")
        bounds = None
        if bounds_data and isinstance(bounds_data, list) and len(bounds_data) == 2:
            bounds = (float(bounds_data[0]), float(bounds_data[1]))

        return cls(
            id=param_id,
            name=name,
            value=float(value) if value is not None else 0.0,
            fixed=fixed,
            bounds=bounds,
        )


@dataclass
class ModelData:
    """
    Complete parsed data from a refl1d/bumps model JSON file.

    Contains the layer structure, materials, fit parameters,
    and probe data.
    """

    # File info
    file_path: str
    schema_version: Optional[str] = None

    # Layer stack (top to bottom)
    layers: list[ModelLayer] = field(default_factory=list)

    # Fit parameters
    parameters: list[FitParameter] = field(default_factory=list)

    # Probe data (if present)
    q: list[float] = field(default_factory=list)
    r: list[float] = field(default_factory=list)
    dr: list[float] = field(default_factory=list)
    dq: list[float] = field(default_factory=list)

    # Experiment metadata
    intensity: Optional[float] = None
    background: Optional[float] = None

    @property
    def num_layers(self) -> int:
        """Number of layers in the stack."""
        return len(self.layers)

    @property
    def substrate(self) -> Optional[ModelLayer]:
        """Get the substrate layer (last in stack with zero thickness)."""
        if self.layers:
            last = self.layers[-1]
            if last.thickness == 0:
                return last
        return None

    @property
    def ambient(self) -> Optional[ModelLayer]:
        """Get the ambient layer (first in stack with zero thickness)."""
        if self.layers:
            first = self.layers[0]
            if first.thickness == 0:
                return first
        return None

    @property
    def film_layers(self) -> list[ModelLayer]:
        """Get film layers (non-zero thickness)."""
        return [l for l in self.layers if l.thickness > 0]

    @property
    def total_thickness(self) -> float:
        """Total film thickness."""
        return sum(l.thickness for l in self.film_layers)

    def get_layer_by_name(self, name: str) -> Optional[ModelLayer]:
        """Find a layer by name."""
        for layer in self.layers:
            if layer.name.lower() == name.lower():
                return layer
        return None

    def get_parameter_by_name(self, name: str) -> Optional[FitParameter]:
        """Find a parameter by name."""
        for param in self.parameters:
            if name.lower() in param.name.lower():
                return param
        return None


class ModelParser:
    """
    Parser for refl1d/bumps model JSON files.

    Handles the bumps JSON schema format used by refl1d for
    storing fitted models with parameter references.

    Usage:
        parser = ModelParser()
        data = parser.parse("/path/to/model.json")

        for layer in data.layers:
            print(f"{layer.name}: {layer.thickness} Å, ρ={layer.material.rho}")
    """

    def __init__(self):
        """Initialize the parser."""
        pass

    def parse(self, file_path: str | Path) -> ModelData:
        """
        Parse a refl1d/bumps model JSON file.

        Args:
            file_path: Path to the JSON file

        Returns:
            ModelData with parsed structure and parameters

        Raises:
            FileNotFoundError: If file doesn't exist
            ValueError: If JSON format is invalid
        """
        file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        with open(file_path, "r") as f:
            data = json.load(f)

        return self.parse_dict(data, str(file_path))

    def parse_dict(self, data: dict, file_path: str = "") -> ModelData:
        """
        Parse model from dictionary.

        Args:
            data: Parsed JSON as dictionary
            file_path: Optional file path for reference

        Returns:
            ModelData with parsed structure and parameters
        """
        result = ModelData(file_path=file_path)

        # Get schema version
        result.schema_version = data.get("$schema")

        # Get references dictionary (for resolving parameter values)
        references = data.get("references", {})

        # Parse fit parameters from references
        for param_id, param_data in references.items():
            if param_data.get("__class__") == "bumps.parameter.Parameter":
                result.parameters.append(FitParameter.from_json(param_id, param_data))

        # Navigate to the experiment/sample structure
        obj = data.get("object", {})
        sample = obj.get("sample", {})

        # Parse layers
        layers_data = sample.get("layers", [])
        for layer_data in layers_data:
            layer = ModelLayer.from_json(layer_data, references)
            result.layers.append(layer)

        # Parse probe data
        probe = obj.get("probe", {})
        result.q = self._extract_numpy_array(probe.get("Q", {}))
        result.r = self._extract_numpy_array(probe.get("R", {}))
        result.dr = self._extract_numpy_array(probe.get("dR", {}))
        result.dq = self._extract_numpy_array(probe.get("dQ", {}))

        # Parse intensity and background
        result.intensity = ModelMaterial._resolve_parameter(probe.get("intensity"), references)
        result.background = ModelMaterial._resolve_parameter(probe.get("background"), references)

        return result

    def _extract_numpy_array(self, array_data: Any) -> list[float]:
        """Extract values from a bumps NumpyArray structure."""
        if not array_data:
            return []

        if isinstance(array_data, list):
            return [float(v) for v in array_data]

        if isinstance(array_data, dict):
            # Handle bumps.util.NumpyArray format
            values = array_data.get("values", [])
            if values:
                return [float(v) for v in values]

        return []


def extract_layers_for_sample(model: ModelData) -> list[dict]:
    """
    Extract layer information suitable for creating a Sample.

    Converts ModelLayer objects to dictionaries with the fields
    needed for the Sample model.

    Args:
        model: Parsed model data

    Returns:
        List of layer dictionaries
    """
    layers = []

    for layer in model.layers:
        layers.append(
            {
                "name": layer.name,
                "thickness": layer.thickness,
                "interface": layer.interface,
                "material": {
                    "composition": layer.material.name,
                    "sld": layer.material.rho,
                    "isld": layer.material.irho,
                },
            }
        )

    return layers
