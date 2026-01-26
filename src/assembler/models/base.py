"""
Base model for all data lakehouse entities.

Provides common fields and behavior matching the raven_ai DataModel.
"""

from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, Field, ConfigDict


class DataModel(BaseModel):
    """
    Base model for all lakehouse documents.

    Provides:
    - Unique ID field
    - Creation timestamp
    - Soft delete support
    - JSON serialization config
    """

    model_config = ConfigDict(
        # Allow arbitrary types for flexibility
        arbitrary_types_allowed=True,
        # Use enum values in serialization
        use_enum_values=True,
        # Validate on assignment
        validate_assignment=True,
        # Populate by field name or alias
        populate_by_name=True,
    )

    # Unique document identifier (set by database or generated)
    id: Optional[str] = Field(default=None, alias="Id")

    # Timestamps
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Soft delete flag
    is_deleted: bool = Field(default=False)

    def __hash__(self) -> int:
        """Hash using id if available, otherwise object id."""
        return hash(self.id or id(self))

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return self.model_dump(by_alias=True, exclude_none=True)

    def to_json(self) -> str:
        """Convert to JSON string."""
        return self.model_dump_json(by_alias=True, exclude_none=True)
