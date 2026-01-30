"""
Assembly result dataclass.

Holds the output of the data assembly process.
"""

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class AssemblyResult:
    """
    Result of assembling data from multiple sources.

    Contains the assembled records (dicts matching schema) and any issues encountered.

    Attributes:
        reflectivity: The assembled reflectivity record (dict matching REFLECTIVITY_SCHEMA)
        sample: The assembled sample record (dict matching SAMPLE_SCHEMA)
        environment: The assembled environment record (dict matching ENVIRONMENT_SCHEMA)
        reduced_file: Path to source reduced data file
        parquet_dir: Path to source parquet directory
        model_file: Path to source model JSON file
        warnings: Non-fatal issues encountered
        errors: Fatal issues that prevented assembly
        needs_review: Fields requiring human/AI review
    """

    # Assembled records (dicts matching schemas)
    reflectivity: Optional[dict[str, Any]] = None
    sample: Optional[dict[str, Any]] = None
    environment: Optional[dict[str, Any]] = None

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
    def needs_human_review(self) -> bool:
        """Check if any fields need human review."""
        return len(self.needs_review) > 0

    def summary(self) -> str:
        """Generate a human-readable summary."""
        lines = ["Assembly Summary:"]

        if self.reflectivity:
            refl = self.reflectivity
            refl_data = refl.get("reflectivity", {})
            q = refl_data.get("q", [])
            lines.append(f"  Reflectivity: {refl.get('run_number')} - {refl.get('run_title')}")
            lines.append(f"    Facility: {refl.get('facility')}")
            lines.append(f"    Q points: {len(q)}")
            if q:
                lines.append(f"    Q range: {min(q):.4f} - {max(q):.4f} Å⁻¹")
        else:
            lines.append("  Reflectivity: Not assembled")

        if self.sample:
            lines.append(f"  Sample: {self.sample.get('description', 'Unknown')}")
            layers = self.sample.get("layers", [])
            lines.append(f"    Layers: {len(layers)}")
        else:
            lines.append("  Sample: Not assembled")

        if self.environment:
            lines.append(f"  Environment: {self.environment.get('description', 'Unknown')}")
            temp = self.environment.get("temperature")
            if temp:
                lines.append(f"    Temperature: {temp:.1f} K")
        else:
            lines.append("  Environment: Not assembled")

        if self.warnings:
            lines.append(f"\nWarnings ({len(self.warnings)}):")
            for w in self.warnings[:5]:  # Limit to first 5
                lines.append(f"  - {w}")

        if self.errors:
            lines.append(f"\nErrors ({len(self.errors)}):")
            for e in self.errors:
                lines.append(f"  - {e}")

        return "\n".join(lines)
