"""
Validation utilities for data ingestion.

Provides schema validation, cross-reference checks, and data quality validation.
"""

import logging
from dataclasses import dataclass, field
from typing import Any, Optional

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
    Validates assembled data records against schema and quality rules.

    Validation levels:
    1. Schema validation - Check required fields and types
    2. Cross-reference validation - Links between records
    3. Data quality validation - Scientific data quality checks

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

        # Validate Reflectivity record
        if assembly.reflectivity:
            self._validate_reflectivity(assembly.reflectivity, result)

        # Validate Sample record
        if assembly.sample:
            self._validate_sample(assembly.sample, result)

        # Validate Environment record
        if assembly.environment:
            self._validate_environment(assembly.environment, result)

        # Cross-reference validation
        self._validate_cross_references(assembly, result)

        # Data quality checks
        if self.check_quality:
            self._validate_data_quality(assembly, result)

        # Note items needing review
        for key, value in assembly.needs_review.items():
            result.add_info(key, f"Needs review: {value}")

        return result

    def _validate_reflectivity(self, record: dict[str, Any], result: ValidationResult) -> None:
        """Validate reflectivity record."""
        # Get reflectivity data from nested struct
        refl_data = record.get("reflectivity", {})
        
        q = refl_data.get("q", [])
        r = refl_data.get("r", [])
        dr = refl_data.get("dr", [])
        dq = refl_data.get("dq", [])
        
        n_points = len(q)

        # Array length consistency
        if len(r) != n_points:
            result.add_error(
                "reflectivity.r",
                f"R array length ({len(r)}) doesn't match Q ({n_points})",
            )

        if len(dr) != n_points:
            result.add_error(
                "reflectivity.dr",
                f"dR array length ({len(dr)}) doesn't match Q ({n_points})",
            )

        if len(dq) != n_points:
            result.add_error(
                "reflectivity.dq",
                f"dQ array length ({len(dq)}) doesn't match Q ({n_points})",
            )

        # Required field checks
        if record.get("proposal_number") == "UNKNOWN":
            result.add_warning("reflectivity.proposal_number", "Proposal number is UNKNOWN")

        if record.get("run_number") == "UNKNOWN":
            result.add_warning("reflectivity.run_number", "Run number is UNKNOWN")

    def _validate_sample(self, record: dict[str, Any], result: ValidationResult) -> None:
        """Validate sample record."""
        layers = record.get("layers", [])
        substrate_json = record.get("substrate_json")
        
        # Check for layers
        if not layers and substrate_json is None:
            result.add_warning("sample.layers", "Sample has no layers defined")

        # Check main composition
        if record.get("main_composition") == "Unknown":
            result.add_warning("sample.main_composition", "Main composition is unknown")

        # Check for generic layer names
        for i, layer in enumerate(layers):
            layer_name = layer.get("material", "")
            if layer_name and layer_name.lower() in ["material", "layer", "film", "unknown"]:
                result.add_warning(
                    f"sample.layers[{i}].material",
                    f"Generic layer name '{layer_name}'",
                )

    def _validate_environment(self, record: dict[str, Any], result: ValidationResult) -> None:
        """Validate environment record."""
        temperature = record.get("temperature")
        
        # Check for missing temperature
        if temperature is None:
            result.add_info("environment.temperature", "Temperature not recorded")

        # Check for reasonable temperature range (if present)
        if temperature is not None:
            if temperature < 0 or temperature > 1000:
                result.add_warning(
                    "environment.temperature",
                    f"Unusual temperature: {temperature}K",
                    temperature,
                )

    def _validate_cross_references(
        self, assembly: AssemblyResult, result: ValidationResult
    ) -> None:
        """Validate cross-references between records."""
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

        record = assembly.reflectivity
        refl_data = record.get("reflectivity", {})
        
        q = refl_data.get("q", [])
        r = refl_data.get("r", [])
        dr = refl_data.get("dr", [])
        dq = refl_data.get("dq", [])

        if not q:
            return

        # Check Q range
        q_min, q_max = min(q), max(q)
        if q_min <= 0:
            result.add_error(
                "reflectivity.q",
                f"Q values must be positive, found min={q_min}",
            )

        # Check for negative reflectivity values
        negative_r = sum(1 for rv in r if rv < 0)
        if negative_r > 0:
            result.add_warning(
                "reflectivity.r",
                f"{negative_r} negative reflectivity values found",
            )

        # Check for reflectivity > 1 at low Q (should be ~1 for total reflection)
        high_r_count = sum(1 for i, rv in enumerate(r) if q[i] < 0.02 and rv > 1.5)
        if high_r_count > len(r) * 0.1:
            result.add_warning(
                "reflectivity.r",
                "Many R values > 1.5 at low Q - check normalization",
            )

        # Check uncertainties are positive
        negative_dr = sum(1 for drv in dr if drv < 0)
        if negative_dr > 0:
            result.add_error(
                "reflectivity.dr",
                f"{negative_dr} negative uncertainty values found",
            )

        # Check dQ values
        if dq:
            negative_dq = sum(1 for dqv in dq if dqv < 0)
            if negative_dq > 0:
                result.add_error(
                    "reflectivity.dq",
                    f"{negative_dq} negative dQ values found",
                )

        # Check data point count
        if len(q) < 10:
            result.add_warning(
                "reflectivity.q",
                f"Only {len(q)} data points - unusually small dataset",
            )


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
