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
        reflectivity: The primary reflectivity record (dict matching REFLECTIVITY_SCHEMA).
            For a single-angle measurement this is the only run; for a multi-angle
            state it is the first partial. Kept as the primary for back-compat.
        additional_reflectivities: Any further per-run reflectivity records (the other
            angles/partials of the same state). Use the ``reflectivities`` property to
            iterate all runs (primary first).
        sample: The assembled sample record (dict matching SAMPLE_SCHEMA)
        environment: The assembled environment record (dict matching ENVIRONMENT_SCHEMA)
        reflectivity_model: The assembled fit record (dict matching REFLECTIVITY_MODEL_SCHEMA).
            One fit per assembly; for a co-refinement it links all runs and carries
            per-dataset parameters.
        reduced_file: Path to source reduced data file
        parquet_dir: Path to source parquet directory
        model_file: Path to source model JSON file
        warnings: Non-fatal issues encountered
        errors: Fatal issues that prevented assembly
        needs_review: Fields requiring human/AI review
    """

    # Assembled records (dicts matching schemas)
    reflectivity: Optional[dict[str, Any]] = None
    additional_reflectivities: list[dict[str, Any]] = field(default_factory=list)
    sample: Optional[dict[str, Any]] = None
    # Further per-state samples for a multi-state co-refinement whose states do
    # not share one physical sample (usually empty: co-refinement shares a sample).
    additional_samples: list[dict[str, Any]] = field(default_factory=list)
    environment: Optional[dict[str, Any]] = None
    # Further per-state environments (one condition each) of a multi-state run.
    additional_environments: list[dict[str, Any]] = field(default_factory=list)
    reflectivity_model: Optional[dict[str, Any]] = None

    # External references (for linking to existing records)
    external_sample_id: Optional[str] = None

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
    def reflectivities(self) -> list[dict[str, Any]]:
        """All per-run reflectivity records (primary first).

        For a single run this is ``[reflectivity]``; for a multi-angle state it is
        the primary partial followed by ``additional_reflectivities``. Writers and
        exporters should iterate this rather than ``reflectivity`` alone.
        """
        out: list[dict[str, Any]] = []
        if self.reflectivity is not None:
            out.append(self.reflectivity)
        out.extend(self.additional_reflectivities)
        return out

    @property
    def environments(self) -> list[dict[str, Any]]:
        """All distinct environment records (primary first), de-duplicated by id."""
        return self._distinct(self.environment, self.additional_environments)

    @property
    def samples(self) -> list[dict[str, Any]]:
        """All distinct sample records (primary first), de-duplicated by id."""
        return self._distinct(self.sample, self.additional_samples)

    @staticmethod
    def _distinct(
        primary: Optional[dict[str, Any]], rest: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        seen: set = set()
        for rec in ([primary] if primary else []) + list(rest):
            if not rec:
                continue
            key = rec.get("id", id(rec))
            if key in seen:
                continue
            seen.add(key)
            out.append(rec)
        return out

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
            q = refl.get("q", []) or []
            lines.append(f"  Reflectivity: {refl.get('run_number')} - {refl.get('run_title')}")
            lines.append(f"    Facility: {refl.get('facility')}")
            lines.append(f"    Q points: {len(q)}")
            if q:
                lines.append(f"    Q range: {min(q):.4f} - {max(q):.4f} Å⁻¹")
            if self.additional_reflectivities:
                extra = ", ".join(
                    str(r.get("run_number")) for r in self.additional_reflectivities
                )
                lines.append(
                    f"    +{len(self.additional_reflectivities)} more run(s): {extra}"
                )
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

        if self.reflectivity_model:
            rm = self.reflectivity_model
            lines.append(f"  Reflectivity Model: {rm.get('model_name', 'Unknown')}")
            lines.append(
                f"    Software: {rm.get('software', '?')} {rm.get('software_version', '')}"
            )
            lines.append(f"    Experiments: {rm.get('num_experiments', 0)}")
            model_layers = rm.get("layers", [])
            lines.append(f"    Layers: {len(model_layers)}")
        else:
            lines.append("  Reflectivity Model: Not assembled")

        if self.warnings:
            lines.append(f"\nWarnings ({len(self.warnings)}):")
            for w in self.warnings[:5]:  # Limit to first 5
                lines.append(f"  - {w}")

        if self.errors:
            lines.append(f"\nErrors ({len(self.errors)}):")
            for e in self.errors:
                lines.append(f"  - {e}")

        return "\n".join(lines)
