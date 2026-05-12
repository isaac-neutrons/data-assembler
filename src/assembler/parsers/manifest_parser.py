"""
Manifest file parser for batch sample assembly.

Parses YAML manifest files that describe a sample and its measurement
history, enabling batch processing of multiple measurements that share
the same physical sample.
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import yaml

logger = logging.getLogger(__name__)


@dataclass
class ManifestMeasurement:
    """A single measurement entry in the manifest."""

    name: str
    reduced: str
    parquet: Optional[str] = None
    nexus_file: Optional[str] = None
    model: Optional[str] = None
    model_dataset_index: Optional[int] = None
    environment: Optional[str] = None

    def validate(self) -> list[str]:
        """Validate measurement fields, returning a list of errors."""
        errors = []
        if not self.reduced:
            errors.append(f"Measurement '{self.name}': 'reduced' is required")
        elif not Path(self.reduced).exists():
            errors.append(f"Measurement '{self.name}': reduced file not found: {self.reduced}")
        if self.parquet and not Path(self.parquet).exists():
            errors.append(f"Measurement '{self.name}': parquet directory not found: {self.parquet}")
        if self.model and not Path(self.model).exists():
            errors.append(f"Measurement '{self.name}': model file not found: {self.model}")
        if self.model_dataset_index is not None and self.model_dataset_index < 1:
            errors.append(f"Measurement '{self.name}': model_dataset_index must be >= 1")
        return errors


@dataclass
class ManifestSample:
    """Sample definition in the manifest."""

    description: Optional[str] = None
    model: Optional[str] = None
    model_dataset_index: Optional[int] = None

    def validate(self) -> list[str]:
        """Validate sample fields, returning a list of errors."""
        errors = []
        if self.model and not Path(self.model).exists():
            errors.append(f"Sample model file not found: {self.model}")
        if self.model_dataset_index is not None and self.model_dataset_index < 1:
            errors.append("Sample model_dataset_index must be >= 1")
        return errors


@dataclass
class Manifest:
    """
    Parsed manifest describing a sample and its measurements.

    The manifest defines a single physical sample and an ordered list
    of measurements taken on that sample over time. Each measurement
    can have its own environment conditions, model file, and dataset index.

    The sample record is created from the first measurement's model
    (or from sample.model if specified). All measurements share the
    same sample_id.
    """

    sample: ManifestSample = field(default_factory=ManifestSample)
    output: str = ""
    measurements: list[ManifestMeasurement] = field(default_factory=list)

    # Optional metadata
    title: Optional[str] = None

    def validate(self, check_files: bool = True) -> list[str]:
        """
        Validate the manifest, returning a list of errors.

        Args:
            check_files: Whether to check that referenced files exist

        Returns:
            List of error messages (empty if valid)
        """
        errors = []

        if not self.output:
            errors.append("'output' directory is required")

        if not self.measurements:
            errors.append("At least one measurement is required")

        if check_files:
            errors.extend(self.sample.validate())
            for m in self.measurements:
                errors.extend(m.validate())

        return errors


class ManifestParser:
    """
    Parser for YAML manifest files.

    Manifest format::

        title: "IPTS-34347 Cu/THF experiment"

        sample:
          description: "Cu in THF on Si"
          model: /path/to/model.json
          model_dataset_index: 1

        output: /path/to/output/

        measurements:
          - name: "Steady-state OCV"
            reduced: /path/to/REFL_218386_reduced.txt
            parquet: /path/to/parquet/
            model: /path/to/model.json
            model_dataset_index: 1
            environment: "Electrochemical cell, THF electrolyte, steady-state OCV"

          - name: "Final OCV"
            reduced: /path/to/REFL_218393_reduced.txt
            nexus_file: /path/to/REF_L_218393.nxs.h5
            model: /path/to/model.json
            model_dataset_index: 2
            environment: "Electrochemical cell, THF electrolyte, final OCV"
    """

    def parse(self, file_path: str | Path) -> Manifest:
        """
        Parse a YAML manifest file.

        Args:
            file_path: Path to the YAML manifest file

        Returns:
            Parsed Manifest

        Raises:
            FileNotFoundError: If the manifest file doesn't exist
            ValueError: If the YAML is malformed or missing required fields
        """
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"Manifest file not found: {file_path}")

        with open(file_path) as f:
            data = yaml.safe_load(f)

        if not isinstance(data, dict):
            raise ValueError(f"Manifest must be a YAML mapping, got {type(data).__name__}")

        return self.parse_dict(data)

    def parse_dict(self, data: dict[str, Any]) -> Manifest:
        """
        Parse a manifest from a dictionary (for testing or programmatic use).

        Args:
            data: Manifest data as a dict

        Returns:
            Parsed Manifest

        Raises:
            ValueError: If required fields are missing
        """
        manifest = Manifest()

        manifest.title = data.get("title")
        manifest.output = data.get("output", "")

        # Parse sample section
        sample_data = data.get("sample", {})
        if isinstance(sample_data, dict):
            manifest.sample = ManifestSample(
                description=sample_data.get("description"),
                model=sample_data.get("model"),
                model_dataset_index=sample_data.get("model_dataset_index"),
            )

        # Parse measurements
        measurements_data = data.get("measurements", [])
        if not isinstance(measurements_data, list):
            raise ValueError("'measurements' must be a list")

        for i, m_data in enumerate(measurements_data):
            if not isinstance(m_data, dict):
                raise ValueError(f"Measurement {i + 1} must be a mapping")

            name = m_data.get("name", f"Measurement {i + 1}")
            manifest.measurements.append(
                ManifestMeasurement(
                    name=name,
                    reduced=m_data.get("reduced", ""),
                    parquet=m_data.get("parquet"),
                    nexus_file=m_data.get("nexus_file"),
                    model=m_data.get("model"),
                    model_dataset_index=m_data.get("model_dataset_index"),
                    environment=m_data.get("environment"),
                )
            )

        return manifest
