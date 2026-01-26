"""
Assembly result dataclass.

Holds the output of the data assembly process.
"""

from dataclasses import dataclass, field
from typing import Any, Optional

from assembler.models import Environment, Reflectivity, Sample


@dataclass
class AssemblyResult:
    """
    Result of assembling data from multiple sources.

    Contains the assembled models and any issues encountered.

    Attributes:
        reflectivity: The assembled Reflectivity measurement
        sample: The assembled Sample with layer stack
        environment: The assembled Environment conditions
        reduced_file: Path to source reduced data file
        parquet_dir: Path to source parquet directory
        model_file: Path to source model JSON file
        warnings: Non-fatal issues encountered
        errors: Fatal issues that prevented assembly
        needs_review: Fields requiring human/AI review
    """

    # Assembled models
    reflectivity: Optional[Reflectivity] = None
    sample: Optional[Sample] = None
    environment: Optional[Environment] = None

    # Source files used
    reduced_file: Optional[str] = None
    parquet_dir: Optional[str] = None
    model_file: Optional[str] = None

    # Issues and warnings
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    # Fields that need AI/human assistance
    needs_review: dict[str, Any] = field(default_factory=dict)

    @property
    def is_complete(self) -> bool:
        """Check if all primary models are assembled."""
        return self.reflectivity is not None

    @property
    def has_errors(self) -> bool:
        """Check if there were any errors."""
        return len(self.errors) > 0

    @property
    def has_sample(self) -> bool:
        """Check if sample model was assembled."""
        return self.sample is not None

    @property
    def has_environment(self) -> bool:
        """Check if environment model was assembled."""
        return self.environment is not None

    @property
    def needs_human_review(self) -> bool:
        """Check if any fields need human review."""
        return len(self.needs_review) > 0
