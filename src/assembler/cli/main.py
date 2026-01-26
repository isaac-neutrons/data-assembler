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
from assembler.writers import write_assembly_to_parquet


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
@click.option("--json", "as_json", is_flag=True, help="Output result as JSON")
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
    if as_json:
        _print_assembly_summary_json(result)
    else:
        _print_assembly_summary(result)

    # Write output
    if dry_run:
        logger.info("Dry run - skipping output")
        click.echo(click.style("\nDry run - no files written", fg="cyan"))
        return

    logger.info(f"Writing to: {output}")
    output_path = Path(output)
    output_path.mkdir(parents=True, exist_ok=True)

    try:
        paths = write_assembly_to_parquet(result, output_path)
        click.echo(click.style("\nOutput files:", fg="green"))
        for table_name, path in paths.items():
            click.echo(f"  {table_name}: {path}")
    except Exception as e:
        raise click.ClickException(f"Error writing output: {e}")


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


def _print_assembly_summary_json(result: AssemblyResult) -> None:
    """Print assembly summary as JSON."""
    output: dict = {
        "reflectivity": None,
        "sample": None,
        "environment": None,
    }
    if result.reflectivity:
        facility = result.reflectivity.facility
        if hasattr(facility, "value"):
            facility = facility.value
        output["reflectivity"] = {
            "run_number": result.reflectivity.run_number,
            "run_title": result.reflectivity.run_title,
            "facility": facility,
            "q_points": len(result.reflectivity.q) if result.reflectivity.q else 0,
            "q_range": (
                [min(result.reflectivity.q), max(result.reflectivity.q)]
                if result.reflectivity.q
                else None
            ),
        }
    if result.sample:
        output["sample"] = {
            "description": result.sample.description,
            "layers": len(result.sample.layers) if result.sample.layers else 0,
            "main_composition": result.sample.main_composition,
        }
    if result.environment:
        output["environment"] = {
            "description": result.environment.description,
            "temperature": result.environment.temperature,
        }
    click.echo(json.dumps(output, indent=2))


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
