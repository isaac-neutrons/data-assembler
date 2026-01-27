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

import math
from typing import Optional

from assembler.parsers.parquet_parser import ParquetData

from .base import (
    ExtractedEnvironment,
    ExtractedMetadata,
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
        # REF_L typically uses wavelengths around 6 Angstroms
        wavelength=6.0,
        wavelength_spread=0.02,  # ~2% dλ/λ
    )

    # DAS log names for various parameters
    # Temperature sensors available on REF_L
    TEMPERATURE_LOGS = [
        "SampleTemp",  # Generic fallback
    ]

    # Motor positions
    THETA_INCIDENT = ["thi", "BL4B:Mot:thi.RBV"]  # Incident theta
    THETA_SAMPLE = ["ths", "BL4B:Mot:ths.RBV"]  # Sample theta
    THETA_DETECTOR = ["tthd", "BL4B:Mot:tthd.RBV"]  # Detector 2theta

    # Slit widths
    SLIT_LOGS = {
        "S1": ["S1HWidth"],  # Slit 1 horizontal width
        "S2": ["S2HWidth"],  # Slit 2 horizontal width
        "S3": ["S3HWidth"],  # Slit 3 horizontal width
        "Si": ["SiHWidth"],  # Incident slit width
    }

    # Detector configuration
    DETECTOR_DISTANCE = [
        "BL4B:CS:Autoreduce:DistanceSampleDetector",
        "distance_sample_detector",
    ]
    DETECTOR_FREQUENCY = [
        "frequency",
        "BL4B:Det:TH:BL:Frequency",
    ]
    WAVELENGTH = [
        "BL4B:Det:TH:BL:Lambda",
        "BL4B:Chop:Skf1:WavelengthUserReq",
    ]

    # Operating mode
    OPERATING_MODE = ["BL4B:CS:ExpPl:OperatingMode"]

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
        source_log = None

        for log_name in cls.TEMPERATURE_LOGS:
            value = cls.get_daslog_value(parquet, [log_name])
            # Temperature of 0 is often a "not connected" indicator
            if value is not None and value != 0.0:
                temperature = value
                source_log = log_name
                break

        # Build description
        description_parts = []
        if source_log:
            # Make log name more human-readable
            if "PolyScience" in source_log:
                description_parts.append("PolyScience circulator")
            elif "LT:Temperature" in source_log:
                description_parts.append("Liquid trough")
            elif "langmuir" in source_log.lower():
                description_parts.append("Langmuir trough")

        if temperature is not None:
            description_parts.append(f"T={temperature:.1f}K")

        # Check for any non-zero temperatures to report range
        temp_min, temp_max = None, None
        for log_name in cls.TEMPERATURE_LOGS:
            value = cls.get_daslog_value(parquet, [log_name])
            if value is not None and value != 0.0:
                if temp_min is None:
                    temp_min = temp_max = value
                else:
                    temp_min = min(temp_min, value)
                    temp_max = max(temp_max, value)

        source_logs = [log for log in cls.TEMPERATURE_LOGS if log in parquet.daslogs]

        return ExtractedEnvironment(
            temperature=temperature,
            temperature_min=temp_min,
            temperature_max=temp_max,
            description=", ".join(description_parts) if description_parts else None,
            source_logs=source_logs if source_logs else None,
        )

    @classmethod
    def extract_metadata(cls, parquet: ParquetData) -> ExtractedMetadata:
        """
        Extract REF_L-specific metadata from DAS logs.

        Includes motor positions, slit widths, detector configuration.

        Args:
            parquet: Parsed parquet data with DAS logs

        Returns:
            ExtractedMetadata with instrument configuration
        """
        # Extract slit widths
        slit_widths = {}
        for slit_name, log_names in cls.SLIT_LOGS.items():
            value = cls.get_daslog_value(parquet, log_names)
            if value is not None:
                slit_widths[slit_name] = value

        # Detector distance (-1 often means "not set")
        detector_distance = cls.get_daslog_value(parquet, cls.DETECTOR_DISTANCE)
        if detector_distance is not None and detector_distance < 0:
            detector_distance = None

        # Wavelength
        wavelength = cls.get_daslog_value(parquet, cls.WAVELENGTH)

        # Frequency
        frequency = cls.get_daslog_value(parquet, cls.DETECTOR_FREQUENCY)

        # Operating mode (string)
        operating_mode = cls.get_daslog_string(parquet, cls.OPERATING_MODE)

        # Motor positions
        theta_incident = cls.get_daslog_value(parquet, cls.THETA_INCIDENT)
        theta_sample = cls.get_daslog_value(parquet, cls.THETA_SAMPLE)
        theta_detector = cls.get_daslog_value(parquet, cls.THETA_DETECTOR)

        extra = {}
        if theta_incident is not None:
            extra["theta_incident"] = theta_incident
        if theta_sample is not None:
            extra["theta_sample"] = theta_sample
        if theta_detector is not None:
            extra["theta_detector"] = theta_detector

        return ExtractedMetadata(
            operating_mode=operating_mode,
            slit_widths=slit_widths if slit_widths else None,
            detector_distance=detector_distance,
            wavelength=wavelength,
            frequency=frequency,
            extra=extra if extra else None,
        )

    @classmethod
    def validate_data(
        cls,
        reflectivity=None,
        sample=None,
        environment=None,
    ) -> list[str]:
        """
        REF_L-specific data validation.

        Checks for common issues with REF_L data.

        Returns:
            List of warning messages
        """
        warnings = []

        if reflectivity:
            # REF_L typically measures Q range 0.005 - 0.3 Å⁻¹
            if reflectivity.q:
                q_min, q_max = min(reflectivity.q), max(reflectivity.q)
                if q_min < 0.001:
                    warnings.append(f"Q_min ({q_min:.4f}) unusually low for REF_L")
                if q_max > 0.5:
                    warnings.append(f"Q_max ({q_max:.4f}) unusually high for REF_L")

        if sample:
            # REF_L is for liquids - check if sample description suggests liquid
            if sample.description:
                desc_lower = sample.description.lower()
                # These are fine for liquids reflectometer
                liquid_keywords = ["liquid", "water", "solvent", "solution", "thf"]
                solid_keywords = ["silicon", "si", "wafer", "substrate"]

                # Having only solid keywords might indicate wrong instrument
                has_liquid = any(kw in desc_lower for kw in liquid_keywords)
                has_solid_only = any(kw in desc_lower for kw in solid_keywords) and not has_liquid

                if has_solid_only and "on" not in desc_lower:
                    warnings.append(
                        "Sample description suggests solid-only sample; "
                        "REF_L is optimized for liquid surfaces"
                    )

        return warnings
