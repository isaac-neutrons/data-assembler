"""
Base classes for instrument-specific handling.

Provides the abstract interface and registry for instrument handlers.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional, Type

from assembler.parsers.parquet_parser import ParquetData


@dataclass
class InstrumentDefaults:
    """Default values for an instrument."""

    facility: str
    laboratory: str = "ORNL"
    probe: str = "neutrons"
    technique: str = "reflectivity"
    measurement_geometry: Optional[float] = None
    wavelength: Optional[float] = None
    wavelength_spread: Optional[float] = None


@dataclass
class ExtractedEnvironment:
    """Environment data extracted by instrument handler."""

    temperature: Optional[float] = None
    temperature_min: Optional[float] = None
    temperature_max: Optional[float] = None
    pressure: Optional[float] = None
    magnetic_field: Optional[float] = None
    relative_humidity: Optional[float] = None
    description: Optional[str] = None
    source_logs: Optional[list[str]] = None


@dataclass
class ExtractedMetadata:
    """Additional metadata extracted by instrument handler."""

    sample_position: Optional[str] = None
    operating_mode: Optional[str] = None
    slit_widths: Optional[dict[str, float]] = None
    detector_distance: Optional[float] = None
    wavelength: Optional[float] = None
    frequency: Optional[float] = None
    extra: Optional[dict[str, Any]] = None


class Instrument(ABC):
    """
    Abstract base class for instrument-specific handlers.

    Each instrument subclass knows how to:
    1. Identify itself from data
    2. Extract environment conditions from DAS logs
    3. Provide default values for missing fields
    4. Validate instrument-specific data
    """

    # Class attributes - override in subclasses
    name: str = "UNKNOWN"
    aliases: list[str] = []
    beamline: Optional[str] = None
    defaults: InstrumentDefaults = InstrumentDefaults(facility="SNS")

    @classmethod
    def matches(cls, instrument_id: Optional[str]) -> bool:
        """
        Check if this handler matches the given instrument ID.

        Args:
            instrument_id: Instrument identifier string

        Returns:
            True if this handler should be used for the instrument
        """
        if not instrument_id:
            return False

        instrument_upper = instrument_id.upper()
        if cls.name.upper() in instrument_upper:
            return True
        for alias in cls.aliases:
            if alias.upper() in instrument_upper:
                return True
        return False

    @classmethod
    @abstractmethod
    def extract_environment(
        cls,
        parquet: ParquetData,
    ) -> ExtractedEnvironment:
        """
        Extract environment conditions from DAS logs.

        Args:
            parquet: Parsed parquet data with DAS logs

        Returns:
            ExtractedEnvironment with available values
        """
        pass

    @classmethod
    def extract_metadata(
        cls,
        parquet: ParquetData,
    ) -> ExtractedMetadata:
        """
        Extract additional instrument-specific metadata.

        Override in subclasses for instrument-specific extraction.

        Args:
            parquet: Parsed parquet data with DAS logs

        Returns:
            ExtractedMetadata with available values
        """
        return ExtractedMetadata()

    @classmethod
    def get_daslog_value(
        cls,
        parquet: ParquetData,
        log_names: list[str],
        prefer_average: bool = True,
    ) -> Optional[float]:
        """
        Get a value from DAS logs, trying multiple possible names.

        Args:
            parquet: Parsed parquet data
            log_names: List of possible log names to try
            prefer_average: If True, return average_value; else value_numeric

        Returns:
            The first found value, or None
        """
        for name in log_names:
            if name in parquet.daslogs:
                log = parquet.daslogs[name]
                if prefer_average and log.average_value is not None:
                    import math
                    if not math.isnan(log.average_value):
                        return log.average_value
                if log.value_numeric is not None:
                    return log.value_numeric
        return None

    @classmethod
    def get_daslog_string(
        cls,
        parquet: ParquetData,
        log_names: list[str],
    ) -> Optional[str]:
        """
        Get a string value from DAS logs.

        Args:
            parquet: Parsed parquet data
            log_names: List of possible log names to try

        Returns:
            The first found string value, or None
        """
        for name in log_names:
            if name in parquet.daslogs:
                log = parquet.daslogs[name]
                if log.value:
                    return log.value
        return None

    @classmethod
    def validate_data(
        cls,
        reflectivity: Optional[dict[str, Any]] = None,
        sample: Optional[dict[str, Any]] = None,
        environment: Optional[dict[str, Any]] = None,
    ) -> list[str]:
        """
        Perform instrument-specific validation.

        Override in subclasses for specific validation rules.

        Args:
            reflectivity: The reflectivity record dict
            sample: The sample record dict
            environment: The environment record dict

        Returns:
            List of warning/error messages
        """
        return []


class InstrumentRegistry:
    """
    Registry of instrument handlers.

    Maintains a mapping of instrument IDs to handler classes,
    enabling automatic selection of the appropriate handler.
    """

    _handlers: dict[str, Type[Instrument]] = {}

    @classmethod
    def register(cls, handler: Type[Instrument]) -> Type[Instrument]:
        """
        Register an instrument handler.

        Can be used as a decorator:
            @InstrumentRegistry.register
            class MyInstrument(Instrument):
                ...

        Args:
            handler: The instrument handler class

        Returns:
            The handler class (for decorator use)
        """
        cls._handlers[handler.name] = handler
        for alias in handler.aliases:
            cls._handlers[alias] = handler
        return handler

    @classmethod
    def get_handler(cls, instrument_id: Optional[str]) -> Type[Instrument]:
        """
        Get the appropriate handler for an instrument.

        Args:
            instrument_id: Instrument identifier string

        Returns:
            The matching handler class, or a generic Instrument
        """
        if not instrument_id:
            return GenericInstrument

        # First check direct match
        if instrument_id in cls._handlers:
            return cls._handlers[instrument_id]

        # Then check all handlers for pattern match
        for handler in set(cls._handlers.values()):
            if handler.matches(instrument_id):
                return handler

        return GenericInstrument

    @classmethod
    def list_instruments(cls) -> list[str]:
        """List all registered instrument names."""
        return list(set(h.name for h in cls._handlers.values()))


class GenericInstrument(Instrument):
    """
    Generic instrument handler for unknown instruments.

    Provides basic extraction using common DAS log naming conventions.
    """

    name = "GENERIC"
    aliases = []
    defaults = InstrumentDefaults(facility="SNS")

    @classmethod
    def extract_environment(cls, parquet: ParquetData) -> ExtractedEnvironment:
        """Extract environment using generic log names."""
        temperature = cls.get_daslog_value(
            parquet,
            ["SampleTemp", "Temperature", "Temp", "sample_temperature"],
        )
        pressure = cls.get_daslog_value(
            parquet,
            ["Pressure", "VacuumPressure", "sample_pressure"],
        )

        return ExtractedEnvironment(
            temperature=temperature,
            pressure=pressure,
        )
