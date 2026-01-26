"""
Validation utilities for data ingestion.

Provides schema validation, cross-reference checks, and data quality validation.
"""

import logging
from dataclasses import dataclass, field
from typing import Any, Optional

from pydantic import ValidationError

from assembler.models import Environment, Reflectivity, Sample
from assembler.workflow import AssemblyResult

logger = logging.getLogger(__name__)


@dataclass
class ValidationIssue:
    """A single validation issue."""

    field: str
    message: str
    severity: str  # "error", "warning", "info"
    value: Optional[Any] = None


@dataclass
class ValidationResult:
    """Result of validating assembled data."""

    is_valid: bool
    issues: list[ValidationIssue] = field(default_factory=list)

    @property
    def errors(self) -> list[ValidationIssue]:
        """Get only error-level issues."""
        return [i for i in self.issues if i.severity == "error"]

    @property
    def warnings(self) -> list[ValidationIssue]:
        """Get only warning-level issues."""
        return [i for i in self.issues if i.severity == "warning"]

    def add_error(self, field: str, message: str, value: Any = None) -> None:
        """Add an error issue."""
        self.issues.append(
            ValidationIssue(field=field, message=message, severity="error", value=value)
        )
        self.is_valid = False

    def add_warning(self, field: str, message: str, value: Any = None) -> None:
        """Add a warning issue."""
        self.issues.append(
            ValidationIssue(field=field, message=message, severity="warning", value=value)
        )

    def add_info(self, field: str, message: str, value: Any = None) -> None:
        """Add an info issue."""
        self.issues.append(
            ValidationIssue(field=field, message=message, severity="info", value=value)
        )


class DataValidator:
    """
    Validates assembled data against schema and quality rules.

    Validation levels:
    1. Schema validation - Pydantic model validation
    2. Cross-reference validation - Links between models
    3. Data quality validation - Scientific data quality checks
    4. Compatibility validation - Optional raven_ai compatibility

    Usage:
        validator = DataValidator()
        result = validator.validate(assembly_result)

        if result.is_valid:
            print("Validation passed!")
        else:
            for issue in result.errors:
                print(f"ERROR: {issue.field}: {issue.message}")
    """

    def __init__(
        self,
        check_quality: bool = True,
        check_compatibility: bool = False,
    ):
        """
        Initialize the validator.

        Args:
            check_quality: Whether to run data quality checks
            check_compatibility: Whether to check raven_ai compatibility
        """
        self.check_quality = check_quality
        self.check_compatibility = check_compatibility

    def validate(self, assembly: AssemblyResult) -> ValidationResult:
        """
        Validate an assembly result.

        Args:
            assembly: The assembly result to validate

        Returns:
            ValidationResult with issues found
        """
        result = ValidationResult(is_valid=True)

        # Check for assembly errors first
        for error in assembly.errors:
            result.add_error("assembly", error)

        # Add assembly warnings
        for warning in assembly.warnings:
            result.add_warning("assembly", warning)

        # Validate Reflectivity
        if assembly.reflectivity:
            self._validate_reflectivity(assembly.reflectivity, result)

        # Validate Sample
        if assembly.sample:
            self._validate_sample(assembly.sample, result)

        # Validate Environment
        if assembly.environment:
            self._validate_environment(assembly.environment, result)

        # Cross-reference validation
        self._validate_cross_references(assembly, result)

        # Data quality checks
        if self.check_quality:
            self._validate_data_quality(assembly, result)

        # Compatibility checks
        if self.check_compatibility:
            self._validate_compatibility(assembly, result)

        # Note items needing review
        for key, value in assembly.needs_review.items():
            result.add_info(key, f"Needs review: {value}")

        return result

    def _validate_reflectivity(self, refl: Reflectivity, result: ValidationResult) -> None:
        """Validate Reflectivity model."""
        # Array length consistency
        n_points = len(refl.q)

        if len(refl.r) != n_points:
            result.add_error(
                "reflectivity.r",
                f"R array length ({len(refl.r)}) doesn't match Q ({n_points})",
            )

        if len(refl.dr) != n_points:
            result.add_error(
                "reflectivity.dr",
                f"dR array length ({len(refl.dr)}) doesn't match Q ({n_points})",
            )

        if len(refl.dq) != n_points:
            result.add_error(
                "reflectivity.dq",
                f"dQ array length ({len(refl.dq)}) doesn't match Q ({n_points})",
            )

        # Required field checks
        if refl.proposal_number == "UNKNOWN":
            result.add_warning("reflectivity.proposal_number", "Proposal number is UNKNOWN")

        if refl.run_number == "UNKNOWN":
            result.add_warning("reflectivity.run_number", "Run number is UNKNOWN")

    def _validate_sample(self, sample: Sample, result: ValidationResult) -> None:
        """Validate Sample model."""
        # Check for layers
        if not sample.layers and sample.substrate is None:
            result.add_warning("sample.layers", "Sample has no layers defined")

        # Check main composition
        if sample.main_composition == "Unknown":
            result.add_warning("sample.main_composition", "Main composition is unknown")

        # Check for generic layer names
        for i, layer in enumerate(sample.layers):
            if layer.name.lower() in ["material", "layer", "film", "unknown"]:
                result.add_warning(
                    f"sample.layers[{i}].name",
                    f"Generic layer name '{layer.name}'",
                )

    def _validate_environment(self, env: Environment, result: ValidationResult) -> None:
        """Validate Environment model."""
        # Check for missing temperature
        if env.temperature is None:
            result.add_info("environment.temperature", "Temperature not recorded")

        # Check for reasonable temperature range (if present)
        if env.temperature is not None:
            if env.temperature < 0 or env.temperature > 1000:
                result.add_warning(
                    "environment.temperature",
                    f"Unusual temperature: {env.temperature}K",
                    env.temperature,
                )

    def _validate_cross_references(
        self, assembly: AssemblyResult, result: ValidationResult
    ) -> None:
        """Validate cross-references between models."""
        # If we have both reflectivity and sample, they should be linked
        if assembly.reflectivity and assembly.sample:
            # In future: check that sample_id on reflectivity points to sample
            pass

        # If we have environment, it should have measurement reference
        if assembly.environment and assembly.reflectivity:
            # In future: check measurement_ids includes the reflectivity
            pass

    def _validate_data_quality(self, assembly: AssemblyResult, result: ValidationResult) -> None:
        """Run data quality checks on scientific data."""
        if assembly.reflectivity is None:
            return

        refl = assembly.reflectivity

        # Check Q range
        q_min, q_max = refl.q_range
        if q_min <= 0:
            result.add_error(
                "reflectivity.q",
                f"Q values must be positive, found min={q_min}",
            )

        # Check for negative reflectivity values
        negative_r = sum(1 for r in refl.r if r < 0)
        if negative_r > 0:
            result.add_warning(
                "reflectivity.r",
                f"{negative_r} negative reflectivity values found",
            )

        # Check for reflectivity > 1 at low Q (should be ~1 for total reflection)
        high_r_count = sum(1 for i, r in enumerate(refl.r) if refl.q[i] < 0.02 and r > 1.5)
        if high_r_count > len(refl.r) * 0.1:
            result.add_warning(
                "reflectivity.r",
                "Many R values > 1.5 at low Q - check normalization",
            )

        # Check uncertainties are positive
        negative_dr = sum(1 for dr in refl.dr if dr < 0)
        if negative_dr > 0:
            result.add_error(
                "reflectivity.dr",
                f"{negative_dr} negative uncertainty values found",
            )

        # Check dQ values
        if refl.dq:
            negative_dq = sum(1 for dq in refl.dq if dq < 0)
            if negative_dq > 0:
                result.add_error(
                    "reflectivity.dq",
                    f"{negative_dq} negative dQ values found",
                )

        # Check data point count
        if len(refl.q) < 10:
            result.add_warning(
                "reflectivity.q",
                f"Only {len(refl.q)} data points - unusually small dataset",
            )

    def _validate_compatibility(
        self, assembly: AssemblyResult, result: ValidationResult
    ) -> None:
        """Check compatibility with raven_ai schema."""
        from assembler.compat import is_raven_ai_available, validate_against_raven

        if not is_raven_ai_available():
            result.add_info(
                "compatibility",
                "raven_ai not installed - skipping compatibility check",
            )
            return

        # Validate Reflectivity
        if assembly.reflectivity:
            data = assembly.reflectivity.model_dump()
            is_valid, error = validate_against_raven("Reflectivity", data)
            if not is_valid:
                result.add_warning("compatibility.reflectivity", error or "Incompatible")

        # Validate Sample
        if assembly.sample:
            data = assembly.sample.model_dump()
            is_valid, error = validate_against_raven("Sample", data)
            if not is_valid:
                result.add_warning("compatibility.sample", error or "Incompatible")


def validate_assembly(
    assembly: AssemblyResult,
    check_quality: bool = True,
    check_compatibility: bool = False,
) -> ValidationResult:
    """
    Convenience function to validate an assembly result.

    Args:
        assembly: The assembly result to validate
        check_quality: Whether to run data quality checks
        check_compatibility: Whether to check raven_ai compatibility

    Returns:
        ValidationResult with issues found
    """
    validator = DataValidator(
        check_quality=check_quality,
        check_compatibility=check_compatibility,
    )
    return validator.validate(assembly)
