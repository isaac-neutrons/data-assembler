"""
Main CLI entry point for data-assembler using Click.

Usage:
    data-assembler ingest --reduced FILE [--parquet DIR] [--model FILE] --output DIR
    data-assembler detect FILE
    data-assembler find --run NUMBER [--search-path DIR]...
    data-assembler validate --reduced FILE [--parquet DIR]
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from typing import Optional

import click

from assembler.parsers import ModelParser, ParquetParser, ReducedParser
from assembler.tools import FileFinder, detect_file, extract_run_number
from assembler.validation import DataValidator
from assembler.workflow import AssemblyResult, DataAssembler
from assembler.writers import write_assembly_to_json, write_assembly_to_parquet


def setup_logging(verbose: bool = False, debug: bool = False) -> None:
    """Configure logging based on verbosity."""
    if debug:
        level = logging.DEBUG
    elif verbose:
        level = logging.INFO
    else:
        level = logging.WARNING

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


class Config:
    """Shared configuration for CLI commands."""

    def __init__(self) -> None:
        self.verbose = False
        self.debug = False


pass_config = click.make_pass_decorator(Config, ensure=True)


@click.group()
@click.option("-v", "--verbose", is_flag=True, help="Enable verbose output")
@click.option("--debug", is_flag=True, help="Enable debug output")
@click.version_option(version="0.1.0", prog_name="data-assembler")
@click.pass_context
def cli(ctx: click.Context, verbose: bool, debug: bool) -> None:
    """Automated reflectivity data ingestion for scientific data lakehouse."""
    ctx.ensure_object(Config)
    ctx.obj.verbose = verbose
    ctx.obj.debug = debug
    setup_logging(verbose=verbose, debug=debug)


@cli.command()
@click.argument("file", type=click.Path(exists=True))
@click.option("--json", "as_json", is_flag=True, help="Output as JSON")
@pass_config
def detect(config: Config, file: str, as_json: bool) -> None:
    """Detect file type and extract identifiers.

    Analyzes a file to determine its type (reduced, parquet, model, raw)
    and extracts identifiers like run number, IPTS, and instrument.

    Example:
        data-assembler detect REF_L_218386_combined.txt
    """
    file_path = Path(file)
    info = detect_file(file_path)

    if as_json:
        output = {
            "path": info.path,
            "filename": info.filename,
            "file_type": info.file_type.value,
            "run_number": info.run_number,
            "ipts": info.ipts,
            "instrument": info.instrument,
        }
        click.echo(json.dumps(output, indent=2))
    else:
        click.echo(f"File: {info.filename}")
        click.echo(f"Path: {info.path}")
        click.echo(f"Type: {info.file_type.value}")
        click.echo(f"Run Number: {info.run_number or 'Not found'}")
        click.echo(f"IPTS: {info.ipts or 'Not found'}")
        click.echo(f"Instrument: {info.instrument or 'Not found'}")


@cli.command()
@click.option("--run", "-r", type=int, help="Run number to search for")
@click.option(
    "--from-file",
    "-f",
    "from_file",
    type=click.Path(exists=True),
    help="Extract run number from this file",
)
@click.option(
    "--search-path",
    "-s",
    "search_paths",
    multiple=True,
    type=click.Path(exists=True),
    help="Directory to search (can be specified multiple times)",
)
@click.option("--json", "as_json", is_flag=True, help="Output as JSON")
@pass_config
def find(
    config: Config,
    run: Optional[int],
    from_file: Optional[str],
    search_paths: tuple[str, ...],
    as_json: bool,
) -> None:
    """Find related files for a run.

    Searches directories to find all files related to a run number,
    including reduced data, parquet metadata, and model files.

    Example:
        data-assembler find --run 218386 -s /data/reduced -s /data/parquet
    """
    # Determine run number
    run_number: Optional[int] = None

    if run:
        run_number = run
    elif from_file:
        run_number = extract_run_number(from_file)
        if run_number is None:
            raise click.ClickException(f"Could not extract run number from: {from_file}")

    if run_number is None:
        raise click.ClickException("Must specify --run NUMBER or --from-file FILE")

    # Determine search paths
    paths = list(search_paths) if search_paths else ["."]

    # Find related files
    finder = FileFinder(paths)
    related = finder.find_related_files(run_number)

    if as_json:
        output = {
            "run_number": related.run_number,
            "ipts": related.ipts,
            "completeness_score": related.completeness_score(),
            "files": {
                "reduced": related.reduced_file,
                "raw": related.raw_file,
                "model": related.model_file,
                "metadata_parquet": related.metadata_parquet,
                "sample_parquet": related.sample_parquet,
                "users_parquet": related.users_parquet,
                "daslogs_parquet": related.daslogs_parquet,
                "instrument_parquet": related.instrument_parquet,
                "other_parquet": related.other_parquet,
            },
        }
        click.echo(json.dumps(output, indent=2))
    else:
        click.echo(f"Run Number: {related.run_number}")
        click.echo(f"IPTS: {related.ipts or 'Not found'}")
        click.echo(f"Completeness: {related.completeness_score():.0%}")
        click.echo("\nFiles found:")
        click.echo(f"  Reduced: {related.reduced_file or 'Not found'}")
        click.echo(f"  Raw: {related.raw_file or 'Not found'}")
        click.echo(f"  Model: {related.model_file or 'Not found'}")
        click.echo(f"  Metadata Parquet: {related.metadata_parquet or 'Not found'}")
        click.echo(f"  Sample Parquet: {related.sample_parquet or 'Not found'}")
        click.echo(f"  DASLogs Parquet: {related.daslogs_parquet or 'Not found'}")
        if related.other_parquet:
            click.echo(f"  Other Parquet: {len(related.other_parquet)} file(s)")


@cli.command()
@click.option(
    "--reduced",
    "-r",
    required=True,
    type=click.Path(exists=True),
    help="Path to reduced reflectivity data file",
)
@click.option(
    "--parquet",
    "-p",
    type=click.Path(exists=True),
    help="Directory containing parquet files",
)
@click.option(
    "--model",
    "-m",
    type=click.Path(exists=True),
    help="Path to model JSON file",
)
@click.option("--json", "as_json", is_flag=True, help="Output as JSON")
@pass_config
def validate(
    config: Config,
    reduced: str,
    parquet: Optional[str],
    model: Optional[str],
    as_json: bool,
) -> None:
    """Validate assembled data without writing.

    Parses input files, assembles them, and runs validation checks.
    Reports any errors or warnings found.

    Example:
        data-assembler validate --reduced REF_L_218386.txt --parquet ./parquet/
    """
    logger = logging.getLogger("validate")

    # Parse input files
    logger.info(f"Parsing reduced file: {reduced}")
    reduced_parser = ReducedParser()
    try:
        reduced_data = reduced_parser.parse(reduced)
    except Exception as e:
        raise click.ClickException(f"Error parsing reduced file: {e}")

    parquet_data = None
    if parquet:
        logger.info(f"Parsing parquet directory: {parquet}")
        parquet_parser = ParquetParser()
        try:
            run_number = extract_run_number(reduced)
            parquet_data = parquet_parser.parse_directory(parquet, run_number=run_number)
        except Exception as e:
            raise click.ClickException(f"Error parsing parquet files: {e}")

    model_data = None
    if model:
        logger.info(f"Parsing model file: {model}")
        model_parser = ModelParser()
        try:
            model_data = model_parser.parse(model)
        except Exception as e:
            raise click.ClickException(f"Error parsing model file: {e}")

    # Assemble
    logger.info("Assembling data...")
    assembler = DataAssembler()
    result = assembler.assemble(reduced=reduced_data, parquet=parquet_data, model=model_data)

    # Validate
    logger.info("Validating...")
    validator = DataValidator()
    validation = validator.validate(result)

    if as_json:
        output = {
            "is_valid": validation.is_valid,
            "errors": [
                {"field": i.field, "message": i.message, "severity": i.severity}
                for i in validation.errors
            ],
            "warnings": [
                {"field": i.field, "message": i.message, "severity": i.severity}
                for i in validation.warnings
            ],
            "assembly": {
                "has_reflectivity": result.reflectivity is not None,
                "has_sample": result.sample is not None,
                "has_environment": result.environment is not None,
                "assembly_errors": result.errors,
                "assembly_warnings": result.warnings,
                "needs_review": result.needs_review,
            },
        }
        click.echo(json.dumps(output, indent=2))
    else:
        status = (
            click.style("PASSED", fg="green")
            if validation.is_valid
            else click.style("FAILED", fg="red")
        )
        click.echo(f"Validation: {status}")
        click.echo()

        if validation.errors:
            click.echo(click.style("Errors:", fg="red"))
            for issue in validation.errors:
                click.echo(f"  ✗ {issue.field}: {issue.message}")

        if validation.warnings:
            click.echo(click.style("Warnings:", fg="yellow"))
            for issue in validation.warnings:
                click.echo(f"  ⚠ {issue.field}: {issue.message}")

        if result.needs_review:
            click.echo(click.style("\nNeeds Review:", fg="cyan"))
            for field, reason in result.needs_review.items():
                click.echo(f"  ? {field}: {reason}")

        click.echo()
        _print_assembly_summary(result)

    if not validation.is_valid:
        sys.exit(1)


@cli.command()
@click.option(
    "--reduced",
    "-r",
    required=True,
    type=click.Path(exists=True),
    help="Path to reduced reflectivity data file (.txt)",
)
@click.option(
    "--parquet",
    "-p",
    type=click.Path(exists=True),
    help="Directory containing parquet files from nexus-processor",
)
@click.option(
    "--model",
    "-m",
    type=click.Path(exists=True),
    help="Path to refl1d/bumps model JSON file",
)
@click.option(
    "--output",
    "-o",
    required=True,
    type=click.Path(),
    help="Output directory for parquet files",
)
@click.option("--skip-validation", is_flag=True, help="Skip validation step")
@click.option("--dry-run", is_flag=True, help="Parse and validate but don't write output")
@click.option("--json", "as_json", is_flag=True, help="Also write JSON files (in addition to Parquet)")
@click.option("--debug", "debug_output", is_flag=True, help="Write debug JSON with full schema and missing field indicators")
@pass_config
def ingest(
    config: Config,
    reduced: str,
    parquet: Optional[str],
    model: Optional[str],
    output: str,
    skip_validation: bool,
    dry_run: bool,
    as_json: bool,
    debug_output: bool,
) -> None:
    """Ingest data and write to parquet.

    Parse, assemble, validate, and write data to parquet files
    for ingestion into the lakehouse.

    Example:
        data-assembler ingest --reduced REF_L_218386.txt --parquet ./parquet/ --output ./lakehouse/
    """
    logger = logging.getLogger("ingest")

    # Parse input files
    logger.info(f"Parsing reduced file: {reduced}")
    reduced_parser = ReducedParser()
    try:
        reduced_data = reduced_parser.parse(reduced)
    except Exception as e:
        raise click.ClickException(f"Error parsing reduced file: {e}")

    parquet_data = None
    if parquet:
        logger.info(f"Parsing parquet directory: {parquet}")
        parquet_parser = ParquetParser()
        try:
            run_number = extract_run_number(reduced)
            parquet_data = parquet_parser.parse_directory(parquet, run_number=run_number)
        except Exception as e:
            raise click.ClickException(f"Error parsing parquet files: {e}")

    model_data = None
    if model:
        logger.info(f"Parsing model file: {model}")
        model_parser = ModelParser()
        try:
            model_data = model_parser.parse(model)
        except Exception as e:
            raise click.ClickException(f"Error parsing model file: {e}")

    # Assemble
    logger.info("Assembling data...")
    assembler = DataAssembler()
    result = assembler.assemble(reduced=reduced_data, parquet=parquet_data, model=model_data)

    if result.has_errors:
        click.echo(click.style("Assembly errors:", fg="red"), err=True)
        for error in result.errors:
            click.echo(f"  - {error}", err=True)
        sys.exit(1)

    # Validate
    if not skip_validation:
        logger.info("Validating assembly...")
        validator = DataValidator()
        validation = validator.validate(result)

        if not validation.is_valid:
            click.echo(click.style("Validation failed:", fg="red"), err=True)
            for issue in validation.issues:
                click.echo(f"  [{issue.severity}] {issue.field}: {issue.message}", err=True)
            sys.exit(1)

        if validation.warnings:
            for issue in validation.warnings:
                click.echo(
                    click.style(f"Warning: {issue.field}: {issue.message}", fg="yellow"),
                    err=True,
                )

    # Report assembly result
    _print_assembly_summary(result)

    # Write output
    if dry_run:
        logger.info("Dry run - skipping output")
        click.echo(click.style("\nDry run - no files written", fg="cyan"))
        # Still write debug output in dry-run mode
        if debug_output:
            output_path = Path(output)
            output_path.mkdir(parents=True, exist_ok=True)
            debug_path = _write_debug_json(result, reduced_data, parquet_data, model_data, output_path)
            click.echo(click.style(f"\nDebug output: {debug_path}", fg="cyan"))
        return

    logger.info(f"Writing to: {output}")
    output_path = Path(output)
    output_path.mkdir(parents=True, exist_ok=True)

    try:
        # Write parquet files
        paths = write_assembly_to_parquet(result, output_path)

        # Write JSON files if requested (for AI-ready data consumers)
        if as_json:
            json_dir = output_path / "json"
            json_paths = write_assembly_to_json(result, json_dir)
            for name, path in json_paths.items():
                paths[f"{name}_json"] = path

        # Write debug JSON if requested
        if debug_output:
            debug_path = _write_debug_json(result, reduced_data, parquet_data, model_data, output_path)
            paths["debug"] = str(debug_path)

        click.echo(click.style("\nOutput files:", fg="green"))
        for table_name, path in paths.items():
            click.echo(f"  {table_name}: {path}")
    except Exception as e:
        raise click.ClickException(f"Error writing output: {e}")


def _write_debug_json(
    result: AssemblyResult,
    reduced_data,
    parquet_data,
    model_data,
    output_path: Path,
) -> Path:
    """
    Write comprehensive debug JSON showing full schema with missing field indicators.
    
    This helps identify what data is available, what's missing, and where it comes from.
    """
    from datetime import datetime, timezone
    from enum import Enum
    
    def serialize_value(v, field_name: str = ""):
        """Serialize a value, marking missing fields clearly."""
        if v is None:
            return {"_value": None, "_status": "MISSING", "_note": "Field not populated"}
        if isinstance(v, Enum):
            return v.value
        if isinstance(v, datetime):
            return v.isoformat()
        if isinstance(v, list):
            if len(v) == 0:
                return {"_value": [], "_status": "EMPTY_LIST"}
            # For large arrays, summarize
            if len(v) > 10 and all(isinstance(x, (int, float)) for x in v):
                return {
                    "_type": "array",
                    "_length": len(v),
                    "_min": min(v),
                    "_max": max(v),
                    "_first_5": v[:5],
                    "_status": "OK",
                }
            return v
        if hasattr(v, "model_dump"):
            return _model_to_debug_dict(v)
        return v

    def _model_to_debug_dict(model) -> dict:
        """Convert a Pydantic model to debug dict with field status."""
        if model is None:
            return {"_status": "MISSING", "_note": "Model not created"}
        
        result = {}
        # Get all fields from the model class (not instance)
        for field_name, field_info in model.__class__.model_fields.items():
            value = getattr(model, field_name, None)
            serialized = serialize_value(value, field_name)
            
            # Add field metadata
            if isinstance(serialized, dict) and "_status" in serialized:
                serialized["_field_description"] = field_info.description or ""
                if field_info.is_required():
                    serialized["_required"] = True
            
            result[field_name] = serialized
        
        return result

    def _source_summary(reduced, parquet, model) -> dict:
        """Summarize what data sources were provided."""
        summary = {
            "reduced": {
                "provided": reduced is not None,
                "file": reduced.file_path if reduced else None,
                "fields_with_data": [],
                "fields_missing": [],
            },
            "parquet": {
                "provided": parquet is not None,
                "metadata_present": parquet.metadata is not None if parquet else False,
                "sample_present": parquet.sample is not None if parquet else False,
                "daslogs_count": len(parquet.daslogs) if parquet else 0,
                "daslog_names": sorted(parquet.daslogs.keys())[:50] if parquet else [],  # First 50
            },
            "model": {
                "provided": model is not None,
                "file": model.file_path if model and hasattr(model, "file_path") else None,
                "layers_count": len(model.layers) if model and model.layers else 0,
                "total_thickness": model.total_thickness if model else None,
            },
        }
        
        # Analyze reduced data fields
        if reduced:
            for attr in ["experiment_id", "run_number", "run_title", "run_start_time", 
                        "reduction_time", "reduction_version", "q_summing", "tof_weighted",
                        "bck_in_q", "theta_offset"]:
                val = getattr(reduced, attr, None)
                if val is not None:
                    summary["reduced"]["fields_with_data"].append(attr)
                else:
                    summary["reduced"]["fields_missing"].append(attr)
        
        return summary

    # Build debug output
    debug_output = {
        "_meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "purpose": "Debug output showing full schema with missing field indicators",
            "legend": {
                "MISSING": "Field is None/not populated",
                "EMPTY_LIST": "Field is an empty list",
                "OK": "Field has data",
            },
        },
        "data_sources": _source_summary(reduced_data, parquet_data, model_data),
        "assembled_data": {
            "reflectivity": _model_to_debug_dict(result.reflectivity) if result.reflectivity else {
                "_status": "NOT_ASSEMBLED",
                "_note": "Reflectivity model was not created",
            },
            "sample": _model_to_debug_dict(result.sample) if result.sample else {
                "_status": "NOT_ASSEMBLED", 
                "_note": "Sample model was not created (requires --model file)",
            },
            "environment": _model_to_debug_dict(result.environment) if result.environment else {
                "_status": "NOT_ASSEMBLED",
                "_note": "Environment model was not created",
            },
        },
        "assembly_errors": result.errors if result.errors else [],
        "assembly_warnings": result.warnings if result.warnings else [],
    }
    
    # Add field coverage summary
    def count_fields(data: dict, prefix: str = "") -> tuple[int, int, list]:
        """Count populated vs missing fields."""
        populated = 0
        missing = 0
        missing_fields = []
        
        for key, value in data.items():
            if key.startswith("_"):
                continue
            full_key = f"{prefix}.{key}" if prefix else key
            if isinstance(value, dict):
                if value.get("_status") == "MISSING":
                    missing += 1
                    missing_fields.append(full_key)
                elif "_status" in value:
                    populated += 1
                else:
                    # Nested dict, recurse
                    p, m, mf = count_fields(value, full_key)
                    populated += p
                    missing += m
                    missing_fields.extend(mf)
            else:
                populated += 1
        return populated, missing, missing_fields
    
    coverage = {}
    for section in ["reflectivity", "sample", "environment"]:
        data = debug_output["assembled_data"].get(section, {})
        if isinstance(data, dict) and data.get("_status") not in ["NOT_ASSEMBLED", "MISSING"]:
            p, m, mf = count_fields(data)
            coverage[section] = {
                "populated_fields": p,
                "missing_fields": m,
                "coverage_pct": round(100 * p / (p + m), 1) if (p + m) > 0 else 0,
                "missing_field_names": mf,
            }
        else:
            coverage[section] = {"status": "not_assembled"}
    
    debug_output["field_coverage"] = coverage
    
    # Write to file
    debug_path = output_path / "debug_schema.json"
    with open(debug_path, "w") as f:
        json.dump(debug_output, f, indent=2, default=str)
    
    return debug_path


def _print_assembly_summary(result: AssemblyResult) -> None:
    """Print a summary of the assembly result."""
    click.echo("Assembly Summary:")
    if result.reflectivity:
        r = result.reflectivity
        click.echo(f"  Reflectivity: {r.run_number} - {r.run_title}")
        facility = r.facility.value if hasattr(r.facility, "value") else r.facility
        click.echo(f"    Facility: {facility or 'Unknown'}")
        click.echo(f"    Q points: {len(r.q) if r.q else 0}")
        if r.q:
            click.echo(f"    Q range: {min(r.q):.4f} - {max(r.q):.4f} Å⁻¹")
    else:
        click.echo("  Reflectivity: Not assembled")

    if result.sample:
        s = result.sample
        click.echo(f"  Sample: {s.description}")
        click.echo(f"    Layers: {len(s.layers) if s.layers else 0}")
        click.echo(f"    Main composition: {s.main_composition}")
    else:
        click.echo("  Sample: Not assembled")

    if result.environment:
        e = result.environment
        click.echo(f"  Environment: {e.description}")
        if e.temperature:
            click.echo(f"    Temperature: {e.temperature:.1f} K")
    else:
        click.echo("  Environment: Not assembled")


def app(args: Optional[list[str]] = None) -> int:
    """
    Main application entry point (for testing).

    Args:
        args: Command-line arguments (defaults to sys.argv[1:])

    Returns:
        Exit code (0 for success)
    """
    try:
        cli(args, standalone_mode=False)
        return 0
    except click.ClickException as e:
        e.show()
        return 1
    except SystemExit as e:
        return e.code if isinstance(e.code, int) else 1
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        return 1


def main() -> None:
    """CLI entry point."""
    cli()


if __name__ == "__main__":
    main()
