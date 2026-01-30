"""
REF_L (Liquids Reflectometer) instrument handler.

REF_L is the Liquids Reflectometer at the Spallation Neutron Source (SNS),
Oak Ridge National Laboratory. It specializes in neutron reflectometry
of liquid surfaces and interfaces.

Beamline: BL-4B
Facility: SNS

DAS Log naming conventions:
- Prefix: BL4B: for most logs
- Temperature: BL4B:SE:* (Sample Environment)
- Motors: BL4B:Mot:* (e.g., thi, ths, tthd for theta positions)
- Slits: S1HWidth, S2HWidth, S3HWidth, SiHWidth
- Detector: BL4B:Det:*
"""

from __future__ import annotations

from assembler.parsers.parquet_parser import ParquetData

from .base import (
    ExtractedEnvironment,
    Instrument,
    InstrumentDefaults,
    InstrumentRegistry,
)


@InstrumentRegistry.register
class REF_L(Instrument):
    """
    Handler for the REF_L (Liquids Reflectometer) at SNS.

    REF_L is optimized for measuring free liquid surfaces and
    liquid-solid interfaces using horizontal sample geometry.
    """

    name = "REF_L"
    aliases = ["BL4B", "BL-4B"]
    beamline = "BL-4B"

    defaults = InstrumentDefaults(
        facility="SNS",
        laboratory="ORNL",
        probe="neutrons",
        technique="reflectivity",
        technique_description="Specular neutron reflectometry",
        # REF_L typically uses wavelengths around 6 Angstroms
        wavelength=6.0,
        wavelength_spread=0.02,  # ~2% dλ/λ
    )

    # DAS log names for various parameters
    # Temperature sensors available on REF_L
    TEMPERATURE_LOGS = [
        "SampleTemp",  # Generic fallback
    ]

    @classmethod
    def extract_environment(cls, parquet: ParquetData) -> ExtractedEnvironment:
        """
        Extract environment conditions from REF_L DAS logs.

        REF_L has multiple temperature sensors for different sample
        environments (liquid troughs, circulators, etc.). We try each
        in order of preference.

        Args:
            parquet: Parsed parquet data with DAS logs

        Returns:
            ExtractedEnvironment with temperature and other conditions
        """
        # Try temperature sensors in order of preference
        temperature = None

        for log_name in cls.TEMPERATURE_LOGS:
            value = cls.get_daslog_value(parquet, [log_name])
            # Temperature of 0 is often a "not connected" indicator
            if value is not None and value != 0.0:
                temperature = value
                break

        # Build description
        description_parts = []
        if temperature is not None:
            description_parts.append(f"T={temperature:.1f}K")

        return ExtractedEnvironment(
            temperature=temperature,
            description=", ".join(description_parts) if description_parts else None,
        )
