"""
Environment model for experimental conditions.

Represents the physical environment during a measurement.
"""

from typing import Optional

from pydantic import Field

from assembler.models.base import DataModel
from assembler.models.material import Material


class Environment(DataModel):
    """
    Experimental environment/conditions.

    Captures the physical conditions during a measurement:
    temperature, pressure, humidity, and ambient medium.

    Attributes:
        description: Human-readable environment description
        ambient_medium: The medium surrounding the sample (e.g., solvent)
        temperature: Sample temperature in Kelvin
        pressure: Ambient pressure in Pa (optional)
        relative_humidity: Relative humidity percentage (optional)
        measurement_ids: Links to related Measurement documents
    """

    description: str = Field(
        ...,
        description="Human-readable environment description",
    )

    ambient_medium: Optional[Material] = Field(
        default=None,
        description="Medium surrounding the sample (e.g., solvent, air)",
    )

    temperature: Optional[float] = Field(
        default=None,
        description="Sample temperature in Kelvin",
        gt=0,  # Must be positive (absolute temperature)
    )

    pressure: Optional[float] = Field(
        default=None,
        description="Ambient pressure in Pascals",
        ge=0,
    )

    relative_humidity: Optional[float] = Field(
        default=None,
        description="Relative humidity as percentage (0-100)",
        ge=0,
        le=100,
    )

    measurement_ids: list[str] = Field(
        default_factory=list,
        description="IDs of related Measurement documents",
    )

    # Extended fields from DASlogs
    temperature_min: Optional[float] = Field(
        default=None,
        description="Minimum temperature during measurement (K)",
    )

    temperature_max: Optional[float] = Field(
        default=None,
        description="Maximum temperature during measurement (K)",
    )

    magnetic_field: Optional[float] = Field(
        default=None,
        description="Applied magnetic field (Tesla)",
    )

    # Source tracking
    source_daslogs: Optional[list[str]] = Field(
        default=None,
        description="Names of DASlogs used to populate this environment",
    )

    @classmethod
    def from_daslogs(
        cls,
        daslogs: dict[str, dict],
        description: Optional[str] = None,
        ambient_material: Optional[Material] = None,
    ) -> "Environment":
        """
        Create an Environment from DASlog data.

        Args:
            daslogs: Dictionary mapping log names to their data
                    Expected structure: {log_name: {average_value, min_value, max_value}}
            description: Optional description (auto-generated if not provided)
            ambient_material: The ambient medium/solvent

        Returns:
            Environment instance
        """
        # Extract temperature from common log names
        temp = None
        temp_min = None
        temp_max = None

        temp_logs = ["SampleTemp", "temperature", "sample_temperature", "temp"]
        for log_name in temp_logs:
            if log_name in daslogs:
                log_data = daslogs[log_name]
                temp = log_data.get("average_value")
                temp_min = log_data.get("min_value")
                temp_max = log_data.get("max_value")
                break

        # Extract pressure
        pressure = None
        pressure_logs = ["pressure", "chamber_pressure", "vacuum"]
        for log_name in pressure_logs:
            if log_name in daslogs:
                pressure = daslogs[log_name].get("average_value")
                break

        # Extract humidity
        humidity = None
        humidity_logs = ["humidity", "relative_humidity", "RH"]
        for log_name in humidity_logs:
            if log_name in daslogs:
                humidity = daslogs[log_name].get("average_value")
                break

        # Auto-generate description if not provided
        if description is None:
            parts = []
            if temp is not None:
                parts.append(f"T={temp:.1f}K")
            if pressure is not None:
                parts.append(f"P={pressure:.1f}Pa")
            if ambient_material:
                parts.append(f"in {ambient_material.composition}")
            description = ", ".join(parts) if parts else "Standard conditions"

        # Track which logs were used
        used_logs = [
            log
            for log in list(daslogs.keys())
            if any(
                log.lower() in name.lower() for name in temp_logs + pressure_logs + humidity_logs
            )
        ]

        return cls(
            description=description,
            ambient_medium=ambient_material,
            temperature=temp,
            temperature_min=temp_min,
            temperature_max=temp_max,
            pressure=pressure,
            relative_humidity=humidity,
            source_daslogs=used_logs if used_logs else None,
        )

    @property
    def temperature_variation(self) -> Optional[float]:
        """Calculate temperature variation during measurement."""
        if self.temperature_min is not None and self.temperature_max is not None:
            return self.temperature_max - self.temperature_min
        return None

    def __str__(self) -> str:
        """String representation."""
        return f"Environment: {self.description}"
