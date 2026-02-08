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
    rho_std: Optional[float] = None  # Uncertainty on SLD (std deviation)
    irho_std: Optional[float] = None  # Uncertainty on imaginary SLD

    @classmethod
    def from_json(
        cls,
        data: dict,
        references: dict,
        error_data: Optional[dict[str, Any]] = None,
    ) -> "ModelMaterial":
        """Create from JSON data with reference resolution."""
        name = data.get("name", "unknown")

        rho = cls._resolve_parameter(data.get("rho"), references)
        irho = cls._resolve_parameter(data.get("irho"), references)
        rho_std = cls._resolve_std(data.get("rho"), references, error_data)
        irho_std = cls._resolve_std(data.get("irho"), references, error_data)

        return cls(name=name, rho=rho, irho=irho, rho_std=rho_std, irho_std=irho_std)

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
    def _resolve_std(
        param: Any,
        references: dict,
        error_data: Optional[dict[str, Any]] = None,
    ) -> Optional[float]:
        """Resolve the uncertainty (std deviation) for a parameter.

        Checks two sources:
          1. An inline ``std`` field on the Parameter entry in *references*.
          2. The companion *error_data* dict (from ``-err.json``), keyed by
             parameter name.

        Returns ``None`` when the parameter is fixed or has no uncertainty.
        """
        if not isinstance(param, dict):
            return None  # literal value — no uncertainty

        if param.get("__class__") != "Reference":
            return None

        ref_id = param.get("id")
        if not ref_id or ref_id not in references:
            return None

        ref = references[ref_id]

        # Fixed parameters have no uncertainty
        if ref.get("fixed", True):
            return None

        # 1. Inline std on the Parameter entry itself
        if "std" in ref:
            return float(ref["std"])

        # 2. Companion error data, keyed by parameter name
        if error_data:
            name = ref.get("name", "")
            if name in error_data and "std" in error_data[name]:
                return float(error_data[name]["std"])

        return None

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
    thickness_std: Optional[float] = None  # Uncertainty on thickness
    interface_std: Optional[float] = None  # Uncertainty on interface roughness

    @classmethod
    def from_json(
        cls,
        data: dict,
        references: dict,
        error_data: Optional[dict[str, Any]] = None,
    ) -> "ModelLayer":
        """Create from JSON data with reference resolution."""
        name = data.get("name", "unknown")

        thickness = ModelMaterial._resolve_parameter(data.get("thickness"), references)
        interface = ModelMaterial._resolve_parameter(data.get("interface"), references)
        thickness_std = ModelMaterial._resolve_std(data.get("thickness"), references, error_data)
        interface_std = ModelMaterial._resolve_std(data.get("interface"), references, error_data)

        # Parse material
        material_data = data.get("material", {})
        material = ModelMaterial.from_json(material_data, references, error_data)

        return cls(
            name=name,
            thickness=thickness,
            interface=interface,
            material=material,
            thickness_std=thickness_std,
            interface_std=interface_std,
        )


@dataclass
class ModelData:
    """
    Complete parsed data from a refl1d/bumps model JSON file.

    Contains the layer structure and materials.
    """

    # File info
    file_path: str

    # Layer stack (top to bottom)
    layers: list[ModelLayer] = field(default_factory=list)

    # Raw JSON dict for the full model (for reproducibility)
    raw_json: Optional[dict] = None

    # Which dataset/experiment was selected (0-indexed), None = not explicitly chosen
    dataset_index: Optional[int] = None

    # Companion error data (from -err.json), keyed by parameter name
    error_data: Optional[dict[str, Any]] = None

    @property
    def num_layers(self) -> int:
        """Number of layers in the stack."""
        return len(self.layers)

    @property
    def num_datasets(self) -> int:
        """Number of experiments/datasets in the model."""
        if self.raw_json:
            obj = self.raw_json.get("object", {})
            return len(obj.get("models", []))
        return 0

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

    def select_dataset(self, index: int) -> None:
        """
        Re-parse layers from a specific experiment/dataset (0-indexed).

        For co-refinement models with multiple experiments, this selects
        which experiment's sample layer stack to use.

        Args:
            index: 0-based experiment index

        Raises:
            ValueError: If raw_json is not available or index is out of range
        """
        if self.raw_json is None:
            raise ValueError("No raw JSON available for dataset selection")

        references = self.raw_json.get("references", {})
        obj = self.raw_json.get("object", {})
        models = obj.get("models", [])

        if index < 0 or index >= len(models):
            raise ValueError(
                f"Dataset index {index} out of range (0-{len(models) - 1})"
            )

        sample = models[index].get("sample") or {}
        self.layers = []
        for layer_data in sample.get("layers", []):
            self.layers.append(ModelLayer.from_json(layer_data, references, self.error_data))
        self.dataset_index = index

    def get_probe_q(self, index: int) -> list[float]:
        """
        Get Q values from an experiment's probe.

        Args:
            index: 0-based experiment index

        Returns:
            List of Q values, or empty list if unavailable
        """
        if self.raw_json is None:
            return []
        obj = self.raw_json.get("object", {})
        models = obj.get("models", [])
        if index < 0 or index >= len(models):
            return []
        probe = models[index].get("probe", {})
        q_data = probe.get("Q", {})
        if isinstance(q_data, dict):
            return q_data.get("values", [])
        return []

    def get_probe_r(self, index: int) -> list[float]:
        """
        Get R (reflectivity) values from an experiment's probe.

        Args:
            index: 0-based experiment index

        Returns:
            List of R values, or empty list if unavailable
        """
        if self.raw_json is None:
            return []
        obj = self.raw_json.get("object", {})
        models = obj.get("models", [])
        if index < 0 or index >= len(models):
            return []
        probe = models[index].get("probe", {})
        r_data = probe.get("R", {})
        if isinstance(r_data, dict):
            return r_data.get("values", [])
        return []


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

    def parse(self, file_path: str | Path, dataset_index: Optional[int] = None) -> ModelData:
        """
        Parse a refl1d/bumps model JSON file.

        Args:
            file_path: Path to the JSON file
            dataset_index: 0-based index of the experiment to parse layers from.
                          None means auto-detect later (defaults to first experiment).

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

        # Look for companion error file (<name>-err.json)
        error_data = None
        err_path = file_path.with_name(file_path.stem + "-err.json")
        if err_path.exists():
            with open(err_path, "r") as f:
                error_data = json.load(f)

        return self.parse_dict(
            data, str(file_path), raw_json=data,
            dataset_index=dataset_index, error_data=error_data,
        )

    def parse_dict(
        self,
        data: dict,
        file_path: str = "",
        raw_json: Optional[dict] = None,
        dataset_index: Optional[int] = None,
        error_data: Optional[dict[str, Any]] = None,
    ) -> ModelData:
        """
        Parse model from dictionary.

        Args:
            data: Parsed JSON as dictionary
            file_path: Optional file path for reference
            raw_json: Raw JSON dict to store for reproducibility
            dataset_index: 0-based index of the experiment to parse layers from.
                          None means auto-detect later (defaults to first experiment).
            error_data: Companion error data dict (from ``-err.json``), keyed by
                        parameter name, each entry containing at least ``"std"``.

        Returns:
            ModelData with parsed structure and parameters
        """
        result = ModelData(
            file_path=file_path,
            raw_json=raw_json or data,
            dataset_index=dataset_index,
            error_data=error_data,
        )

        # Get references dictionary (for resolving parameter values)
        references = data.get("references", {})

        # Navigate to the experiment/sample structure
        obj = data.get("object", {})

        if dataset_index is not None:
            # Explicit dataset selection
            models_list = obj.get("models", [])
            if 0 <= dataset_index < len(models_list):
                sample = models_list[dataset_index].get("sample") or {}
            else:
                sample = {}
        else:
            # No explicit selection: try object.sample, fall back to first experiment
            sample = obj.get("sample") or {}
            if not sample.get("layers") and obj.get("models"):
                models_list = obj["models"]
                if models_list:
                    sample = models_list[0].get("sample") or {}

        # Parse layers
        layers_data = sample.get("layers", [])
        for layer_data in layers_data:
            layer = ModelLayer.from_json(layer_data, references, error_data)
            result.layers.append(layer)

        return result


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
