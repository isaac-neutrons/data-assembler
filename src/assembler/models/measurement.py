"""
Measurement models for experimental data.

Includes base Measurement and specialized Reflectivity models.
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Optional

import numpy as np
from pydantic import Field, field_validator
from numpy.typing import NDArray

from assembler.models.base import DataModel


class Facility(str, Enum):
    """Supported neutron/X-ray facilities."""

    SNS = "SNS"
    HFIR = "HFIR"
    LCLS = "LCLS"
    NIST = "NIST"
    OTHER = "OTHER"


class Probe(str, Enum):
    """Radiation probe types."""

    NEUTRONS = "neutrons"
    XRAY = "xray"
    OTHER = "other"


class Technique(str, Enum):
    """Measurement techniques."""

    REFLECTIVITY = "reflectivity"
    SANS = "SANS"
    EIS = "EIS"
    OTHER = "other"


class Measurement(DataModel):
    """
    Base measurement model for all experimental data.

    Contains metadata common to all measurement types:
    proposal info, facility, timing, and file references.

    Attributes:
        proposal_number: Experiment proposal ID (e.g., "IPTS-34347")
        facility: Facility where measurement was performed
        lab: Laboratory/institution
        probe: Radiation probe type (neutrons, xray)
        technique: Measurement technique
        technique_description: Detailed technique description
        is_simulated: Whether data is from simulation
        run_title: Experiment run title
        run_number: Run identifier
        run_start: Run start timestamp
        raw_file_path: Path to raw data file
    """

    proposal_number: str = Field(
        ...,
        description="Experiment proposal ID (e.g., 'IPTS-34347')",
    )

    facility: Facility = Field(
        ...,
        description="Facility where measurement was performed",
    )

    lab: Optional[str] = Field(
        default=None,
        description="Laboratory or institution",
    )

    probe: Probe = Field(
        ...,
        description="Radiation probe type",
    )

    technique: Technique = Field(
        ...,
        description="Measurement technique",
    )

    technique_description: Optional[str] = Field(
        default=None,
        description="Detailed technique description",
    )

    is_simulated: bool = Field(
        default=False,
        description="Whether data is from simulation",
    )

    run_title: str = Field(
        ...,
        description="Experiment run title",
    )

    run_number: str = Field(
        ...,
        description="Run identifier",
    )

    run_start: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Run start timestamp",
    )

    raw_file_path: Optional[str] = Field(
        default=None,
        description="Path to raw data file",
    )

    # Extended fields
    instrument_name: Optional[str] = Field(
        default=None,
        description="Instrument name (e.g., 'REF_L')",
    )

    sample_id: Optional[str] = Field(
        default=None,
        description="Link to Sample document",
    )

    @classmethod
    def detect_facility(cls, instrument_id: str) -> Facility:
        """Detect facility from instrument ID."""
        instrument_upper = instrument_id.upper()
        if instrument_upper.startswith("REF_") or instrument_upper in ("BL-4B", "USANS"):
            return Facility.SNS
        elif instrument_upper.startswith("CG-"):
            return Facility.HFIR
        return Facility.OTHER


class Reflectivity(Measurement):
    """
    Reflectivity measurement data.

    Extends Measurement with reflectivity-specific data:
    Q vectors, reflectivity values, and uncertainties.

    Attributes:
        q: Momentum transfer Q values in Å⁻¹
        r: Reflectivity values (normalized)
        dr: Reflectivity uncertainties
        dq: Q resolution (FWHM) in Å⁻¹
        measurement_geometry: Scattering angle configuration
        reduction_time: When data reduction was performed
    """

    model_config = DataModel.model_config.copy()
    model_config["arbitrary_types_allowed"] = True

    # Override technique default
    technique: Technique = Field(
        default=Technique.REFLECTIVITY,
        description="Measurement technique",
    )

    # Reflectivity data arrays
    q: list[float] = Field(
        ...,
        description="Momentum transfer Q values in Å⁻¹",
        min_length=1,
    )

    r: list[float] = Field(
        ...,
        description="Reflectivity values (normalized)",
        min_length=1,
    )

    dr: list[float] = Field(
        ...,
        description="Reflectivity uncertainties",
        min_length=1,
    )

    dq: list[float] = Field(
        ...,
        description="Q resolution (FWHM) in Å⁻¹",
        min_length=1,
    )

    # Reduction metadata
    measurement_geometry: Optional[float] = Field(
        default=None,
        description="Scattering angle (two-theta) in degrees",
    )

    reduction_time: Optional[datetime] = Field(
        default=None,
        description="When data reduction was performed",
    )

    reduction_version: Optional[str] = Field(
        default=None,
        description="Version of reduction software",
    )

    # Reduction parameters
    reduction_parameters: Optional[dict] = Field(
        default=None,
        description="Additional reduction parameters",
    )

    @field_validator("r", "dr", "dq")
    @classmethod
    def validate_array_length(cls, v, info):
        """Validate that arrays have matching length."""
        # Note: Cross-field validation would need model_validator
        return v

    @property
    def q_range(self) -> tuple[float, float]:
        """Get Q range (min, max)."""
        return (min(self.q), max(self.q))

    @property
    def num_points(self) -> int:
        """Number of data points."""
        return len(self.q)

    def to_numpy(self) -> dict[str, NDArray]:
        """Convert data arrays to numpy."""
        return {
            "q": np.array(self.q),
            "r": np.array(self.r),
            "dr": np.array(self.dr),
            "dq": np.array(self.dq),
        }

    def __str__(self) -> str:
        """String representation."""
        q_min, q_max = self.q_range
        return (
            f"Reflectivity: Run {self.run_number} "
            f"[Q: {q_min:.4f} - {q_max:.4f} Å⁻¹, {self.num_points} pts]"
        )
