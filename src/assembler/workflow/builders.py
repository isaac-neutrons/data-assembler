"""
Model builders for the data assembly workflow.

Each builder converts parsed data into target schema models.
"""

import logging
from datetime import datetime, timezone
from typing import Optional, Type

from assembler.instruments import Instrument, InstrumentRegistry
from assembler.models import (
    Environment,
    Facility,
    Layer,
    Material,
    Probe,
    Reflectivity,
    Sample,
    Technique,
)
from assembler.parsers import ModelData, ParquetData, ReducedData

from .result import AssemblyResult

logger = logging.getLogger(__name__)


# Mapping of instrument prefixes to facilities (fallback if no handler)
FACILITY_INSTRUMENTS = {
    "REF_L": Facility.SNS,
    "REF_M": Facility.SNS,
    "BL-4B": Facility.SNS,
    "CG-1D": Facility.HFIR,
}


def detect_facility(instrument: Optional[str]) -> Facility:
    """
    Detect facility from instrument name.

    Args:
        instrument: Instrument identifier string

    Returns:
        The detected Facility enum value (defaults to SNS)
    """
    if instrument:
        instrument_upper = instrument.upper()
        for name, facility in FACILITY_INSTRUMENTS.items():
            if name in instrument_upper:
                return facility
    return Facility.SNS


def build_reflectivity(
    reduced: ReducedData,
    parquet: Optional[ParquetData],
    result: AssemblyResult,
    instrument_handler: Optional[Type[Instrument]] = None,
) -> Optional[Reflectivity]:
    """
    Build Reflectivity model from reduced and parquet data.

    The reduced data provides Q, R, dR, dQ arrays and reduction metadata.
    The parquet data provides run metadata (proposal, title, timestamps).

    Args:
        reduced: Parsed reduced reflectivity data (required)
        parquet: Parsed parquet metadata (optional, enriches result)
        result: AssemblyResult to record warnings/errors
        instrument_handler: Optional specific instrument handler to use

    Returns:
        Assembled Reflectivity model, or None on error
    """
    try:
        # Get metadata from parquet if available, else from reduced header
        if parquet and parquet.metadata:
            meta = parquet.metadata
            proposal_number = meta.experiment_identifier or "UNKNOWN"
            run_number = str(meta.run_number)
            run_title = meta.title or reduced.run_title or "Unknown"
            instrument = meta.instrument_id

            # Get instrument handler
            if instrument_handler is None:
                instrument_handler = InstrumentRegistry.get_handler(instrument)

            facility = instrument_handler.defaults.facility

            # Parse start time
            if meta.start_time:
                try:
                    run_start = datetime.fromisoformat(meta.start_time.replace("Z", "+00:00"))
                except ValueError:
                    run_start = datetime.now(timezone.utc)
                    result.warnings.append(f"Could not parse start_time: {meta.start_time}")
            else:
                run_start = datetime.now(timezone.utc)

            raw_file_path = meta.source_path
        else:
            # Fall back to reduced file header
            proposal_number = reduced.experiment_id or "UNKNOWN"
            run_number = str(reduced.run_number) if reduced.run_number else "UNKNOWN"
            run_title = reduced.run_title or "Unknown"
            instrument = "REF_L"  # Default assumption

            # Get instrument handler
            if instrument_handler is None:
                instrument_handler = InstrumentRegistry.get_handler(instrument)

            facility = instrument_handler.defaults.facility
            run_start = reduced.run_start_time or datetime.now(timezone.utc)
            raw_file_path = None

            if proposal_number == "UNKNOWN":
                result.warnings.append("Proposal number not found, using 'UNKNOWN'")

        logger.debug(f"Using instrument handler: {instrument_handler.name}")

        # Build reduction parameters dict if any are set
        reduction_parameters = None
        if any([reduced.q_summing, reduced.tof_weighted, reduced.bck_in_q, reduced.theta_offset]):
            reduction_parameters = {
                "q_summing": reduced.q_summing,
                "tof_weighted": reduced.tof_weighted,
                "bck_in_q": reduced.bck_in_q,
                "theta_offset": reduced.theta_offset,
            }

        # Build the model
        reflectivity = Reflectivity(
            proposal_number=proposal_number,
            run_number=run_number,
            run_title=run_title,
            facility=facility,
            instrument_name=instrument,
            probe=Probe.NEUTRONS,
            technique=Technique.REFLECTIVITY,
            raw_file_path=raw_file_path,
            run_start=run_start,
            # Data arrays from reduced
            q=reduced.q,
            r=reduced.r,
            dr=reduced.dr,
            dq=reduced.dq,
            # Reduction metadata
            reduction_time=reduced.reduction_time,
            reduction_version=reduced.reduction_version,
            reduction_parameters=reduction_parameters,
        )

        # Set measurement geometry from first run if available
        if reduced.runs:
            reflectivity.measurement_geometry = reduced.runs[0].two_theta

        # Run instrument-specific validation
        validation_warnings = instrument_handler.validate_data(reflectivity=reflectivity)
        for warning in validation_warnings:
            result.warnings.append(warning)

        return reflectivity

    except Exception as e:
        result.errors.append(f"Failed to build Reflectivity: {e}")
        logger.exception("Error building Reflectivity")
        return None


def build_environment(
    parquet: ParquetData,
    result: AssemblyResult,
    instrument_handler: Optional[Type[Instrument]] = None,
) -> Optional[Environment]:
    """
    Build Environment model from parquet daslogs.

    Uses instrument-specific handlers to extract environment data
    from DAS logs with appropriate naming conventions.

    Args:
        parquet: Parsed parquet data containing daslogs
        result: AssemblyResult to record warnings/issues
        instrument_handler: Optional specific instrument handler to use

    Returns:
        Assembled Environment model, or None on error
    """
    try:
        # Get the appropriate instrument handler
        if instrument_handler is None:
            instrument_handler = InstrumentRegistry.get_handler(parquet.instrument_id)

        logger.debug(f"Using instrument handler: {instrument_handler.name}")

        # Extract environment using instrument-specific logic
        extracted = instrument_handler.extract_environment(parquet)

        # Also extract additional metadata for logging
        metadata = instrument_handler.extract_metadata(parquet)
        if metadata.extra:
            logger.debug(f"Instrument metadata: {metadata.extra}")

        # Generate description
        if extracted.description:
            description = extracted.description
        else:
            description = _generate_environment_description(
                temperature=extracted.temperature,
                pressure=extracted.pressure,
                sample_name=parquet.sample.name if parquet.sample else None,
            )

        # Collect source daslog names
        source_daslogs = extracted.source_logs
        if source_daslogs is None:
            source_daslogs = list(parquet.daslogs.keys()) if parquet.daslogs else None

        environment = Environment(
            description=description,
            temperature=extracted.temperature,
            temperature_min=extracted.temperature_min,
            temperature_max=extracted.temperature_max,
            pressure=extracted.pressure,
            magnetic_field=extracted.magnetic_field,
            relative_humidity=extracted.relative_humidity,
            source_daslogs=source_daslogs if source_daslogs else None,
        )

        # Flag for review if key values missing
        if extracted.temperature is None:
            result.needs_review["environment_temperature"] = (
                f"Temperature not found in daslogs (checked: {instrument_handler.name} sensors)"
            )

        return environment

    except Exception as e:
        result.errors.append(f"Failed to build Environment: {e}")
        logger.exception("Error building Environment")
        return None


def build_sample(
    model: ModelData,
    result: AssemblyResult,
) -> Optional[Sample]:
    """
    Build Sample model from model JSON data.

    Converts the layer stack from the fitting model into
    Layer objects with Material definitions.

    Args:
        model: Parsed model JSON with layer definitions
        result: AssemblyResult to record warnings/issues

    Returns:
        Assembled Sample model, or None on error
    """
    try:
        layers = []
        substrate = None

        for i, model_layer in enumerate(model.layers):
            # Create Material
            material = Material(
                composition=model_layer.material.name,
                rho=model_layer.material.rho,
                irho=model_layer.material.irho,
            )

            # Create Layer
            layer = Layer(
                name=model_layer.name,
                material=material,
                thickness=model_layer.thickness,
                interface=model_layer.interface,
            )

            # Last layer with zero thickness is substrate
            if i == len(model.layers) - 1 and model_layer.thickness == 0:
                substrate = layer
            else:
                layers.append(layer)

            # Flag generic layer names for review
            if model_layer.name.lower() in ["material", "layer", "film"]:
                result.needs_review[f"layer_{i}_name"] = (
                    f"Generic layer name '{model_layer.name}' - "
                    f"SLD: {model_layer.material.rho:.2f}, "
                    f"thickness: {model_layer.thickness:.1f} Å"
                )

        # Determine main composition from thickest layer
        main_composition = _determine_main_composition(layers)

        # Generate description
        ambient_name = model.ambient.material.name if model.ambient else "air"
        description = f"{main_composition} in {ambient_name}"
        if substrate:
            description += f" on {substrate.material.composition}"

        sample = Sample(
            description=description,
            layers=layers,
            substrate=substrate,
            main_composition=main_composition,
        )

        return sample

    except Exception as e:
        result.errors.append(f"Failed to build Sample: {e}")
        logger.exception("Error building Sample")
        return None


# --- Private helper functions ---


def _determine_main_composition(layers: list[Layer]) -> str:
    """Determine main composition from thickest layer."""
    main_composition = "Unknown"
    max_thickness = 0

    for layer in layers:
        if layer.thickness and layer.thickness > max_thickness:
            max_thickness = layer.thickness
            main_composition = layer.material.composition

    return main_composition


def _generate_environment_description(
    temperature: Optional[float] = None,
    pressure: Optional[float] = None,
    sample_name: Optional[str] = None,
) -> str:
    """Generate human-readable environment description."""
    parts = []

    if sample_name:
        parts.append(f"Sample: {sample_name}")

    if temperature is not None:
        parts.append(f"T={temperature:.1f}K")

    if pressure is not None:
        if pressure < 1000:
            parts.append(f"P={pressure:.1f}Pa")
        else:
            parts.append(f"P={pressure / 1000:.2f}kPa")

    if parts:
        return ", ".join(parts)
    return "Standard conditions"
