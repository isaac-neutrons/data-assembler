"""
Environment record builder.
"""

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Optional, Type

from assembler.instruments import Instrument, InstrumentRegistry
from assembler.parsers.parquet_parser import ParquetData
from assembler.workflow.builders.utils import generate_environment_description

logger = logging.getLogger(__name__)


def build_environment_record(
    parquet: ParquetData,
    warnings: list[str],
    errors: list[str],
    needs_review: dict[str, Any],
    instrument_handler: Optional[Type[Instrument]] = None,
) -> Optional[dict[str, Any]]:
    """
    Build an environment record from parquet daslogs.

    Uses instrument-specific handlers to extract environment data
    from DAS logs with appropriate naming conventions.

    Args:
        parquet: Parsed parquet data containing daslogs
        warnings: List to append warnings to
        errors: List to append errors to
        needs_review: Dict to record fields needing review
        instrument_handler: Optional specific instrument handler to use

    Returns:
        Dict matching ENVIRONMENT_SCHEMA, or None on error
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
            description = generate_environment_description(
                temperature=extracted.temperature,
                pressure=extracted.pressure,
                sample_name=parquet.sample.name if parquet.sample else None,
            )

        # Default to "Sample cell" if no meaningful description
        if not description or description == "Standard conditions":
            description = "Sample cell"

        # Build the record matching ENVIRONMENT_SCHEMA
        record = {
            # Base fields
            "id": str(uuid.uuid4()),
            "created_at": datetime.now(timezone.utc),
            "is_deleted": False,
            # Relationship field (to be linked by assembler)
            "sample_id": None,
            # Environment fields
            "description": description,
            "ambient_medium": None,  # Would need material extraction
            "temperature": extracted.temperature,
            "pressure": extracted.pressure,
            "relative_humidity": extracted.relative_humidity,
            "measurement_ids": [],
        }

        # Flag for review if key values missing
        if extracted.temperature is None:
            needs_review["environment_temperature"] = (
                f"Temperature not found in daslogs (checked: {instrument_handler.name} sensors)"
            )

        return record

    except Exception as e:
        errors.append(f"Failed to build Environment record: {e}")
        logger.exception("Error building Environment record")
        return None
