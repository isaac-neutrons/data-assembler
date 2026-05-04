"""
Main CLI entry point for data-assembler using Click.

Usage:
    data-assembler ingest --reduced FILE [--parquet DIR] [--model FILE] --output DIR
    data-assembler detect FILE
    data-assembler find --run NUMBER [--search-path DIR]...
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from typing import Optional

import click

from assembler.parsers import ModelParser, ParquetParser, ReducedParser
from assembler.tools import FileFinder
from assembler.tools.detection import detect_file, extract_run_number
from assembler.workflow import AssemblyResult, DataAssembler
from assembler.writers.json_writer import write_assembly_to_json
from assembler.writers.ravendb_writer import write_assembly_to_ravendb
from assembler.writers.parquet_writer import ParquetWriter, write_assembly_to_parquet


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
    "--model-dataset-index",
    type=int,
    default=None,
    help="1-based index of the dataset in a co-refinement model file. "
    "If omitted, auto-detected by matching Q ranges against the reduced data.",
)
@click.option(
    "--environment",
    "-e",
    type=str,
    default=None,
    help="Description text for the environment record (e.g. 'Sample cell, flowing N2').",
)
@click.option(
    "--sample-id",
    type=str,
    default=None,
    help="UUID of an existing sample to link to (skips creating a new sample record). "
    "Use this when assembling additional measurements of the same physical sample.",
)
@click.option(
    "--output",
    "-o",
    required=True,
    type=click.Path(),
    help="Output directory for parquet files",
)
@click.option("--dry-run", is_flag=True, help="Parse and assemble but don't write output")
@click.option(
    "--json", "as_json", is_flag=True, help="Also write JSON files (in addition to Parquet)"
)
@click.option(
    "--ravendb", "as_ravendb", is_flag=True, help="Also store them in RavenDB (in addition to Parquet)"
)
@click.option(
    "--debug",
    "debug_output",
    is_flag=True,
    help="Write debug JSON with full schema and missing field indicators",
)
@pass_config
def ingest(
    config: Config,
    reduced: str,
    parquet: Optional[str],
    model: Optional[str],
    model_dataset_index: Optional[int],
    environment: Optional[str],
    sample_id: Optional[str],
    output: str,
    dry_run: bool,
    as_json: bool,
    as_ravendb: bool,
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
        # Convert 1-based CLI index to 0-based internal index
        ds_index = (model_dataset_index - 1) if model_dataset_index is not None else None
        try:
            model_data = model_parser.parse(model, dataset_index=ds_index)
        except Exception as e:
            raise click.ClickException(f"Error parsing model file: {e}")

    # Assemble
    logger.info("Assembling data...")
    assembler = DataAssembler()
    result = assembler.assemble(
        reduced=reduced_data,
        parquet=parquet_data,
        model=model_data,
        environment_description=environment,
        sample_id=sample_id,
    )

    if result.has_errors:
        click.echo(click.style("Assembly errors:", fg="red"), err=True)
        for error in result.errors:
            click.echo(f"  - {error}", err=True)
        sys.exit(1)

    # Report assembly warnings
    if result.warnings:
        for warning in result.warnings:
            click.echo(
                click.style(f"Warning: {warning}", fg="yellow"),
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
            debug_path = _write_debug_json(
                result, reduced_data, parquet_data, model_data, output_path
            )
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

        # Store in RavenDB if requested (for AI-ready data consumers)
        if as_ravendb:
            db_paths = write_assembly_to_ravendb(result)
            for name, path in db_paths.items():
                paths[f"{name}_ravendb"] = path

        # Write debug JSON if requested
        if debug_output:
            debug_path = _write_debug_json(
                result, reduced_data, parquet_data, model_data, output_path
            )
            paths["debug"] = str(debug_path)

        click.echo(click.style("\nOutput files:", fg="green"))
        for table_name, path in paths.items():
            click.echo(f"  {table_name}: {path}")
    except Exception as e:
        raise click.ClickException(f"Error writing output: {e}")


@cli.command()
@click.argument("manifest", type=click.Path(exists=True))
@click.option("--dry-run", is_flag=True, help="Parse and validate but don't write output")
@click.option(
    "--json", "as_json", is_flag=True, help="Also write JSON files (in addition to Parquet)"
)
@pass_config
def batch(config: Config, manifest: str, dry_run: bool, as_json: bool) -> None:
    """Process a YAML manifest describing a sample and its measurements.

    The manifest defines a single physical sample and an ordered list of
    measurements. The first measurement creates the sample record; all
    subsequent measurements reuse the same sample ID.

    Example:
        data-assembler batch experiment.yaml --json
    """
    from assembler.parsers.manifest_parser import ManifestParser

    logger = logging.getLogger("batch")

    # Parse manifest
    logger.info(f"Parsing manifest: {manifest}")
    parser = ManifestParser()
    try:
        manifest_data = parser.parse(manifest)
    except Exception as e:
        raise click.ClickException(f"Error parsing manifest: {e}")

    # Validate
    errors = manifest_data.validate()
    if errors:
        click.echo(click.style("Manifest validation errors:", fg="red"), err=True)
        for error in errors:
            click.echo(f"  - {error}", err=True)
        sys.exit(1)

    title = manifest_data.title or Path(manifest).stem
    click.echo(click.style(f"Batch: {title}", fg="cyan", bold=True))
    click.echo(f"  Output: {manifest_data.output}")
    click.echo(f"  Measurements: {len(manifest_data.measurements)}")
    click.echo()

    output_path = Path(manifest_data.output)
    output_path.mkdir(parents=True, exist_ok=True)

    reduced_parser = ReducedParser()
    parquet_parser = ParquetParser()
    model_parser = ModelParser()
    assembler = DataAssembler()

    sample_id: Optional[str] = None
    sample_record: Optional[dict] = None
    all_environment_ids: list[str] = []
    all_paths: dict[str, list[str]] = {}
    total_warnings: list[str] = []

    for i, measurement in enumerate(manifest_data.measurements):
        step = f"[{i + 1}/{len(manifest_data.measurements)}]"
        click.echo(click.style(f"{step} {measurement.name}", fg="cyan"))

        # Parse reduced
        try:
            reduced_data = reduced_parser.parse(measurement.reduced)
        except Exception as e:
            raise click.ClickException(
                f"{step} Error parsing reduced file: {e}"
            )

        # Parse parquet (optional)
        parquet_data = None
        if measurement.parquet:
            try:
                run_number = extract_run_number(measurement.reduced)
                parquet_data = parquet_parser.parse_directory(
                    measurement.parquet, run_number=run_number
                )
            except Exception as e:
                raise click.ClickException(
                    f"{step} Error parsing parquet files: {e}"
                )

        # Parse model (optional)
        model_data = None
        model_file = measurement.model or manifest_data.sample.model
        if model_file:
            # Determine dataset index: measurement-level overrides sample-level
            ds_index_1based = (
                measurement.model_dataset_index
                or manifest_data.sample.model_dataset_index
            )
            ds_index = (ds_index_1based - 1) if ds_index_1based is not None else None
            try:
                model_data = model_parser.parse(model_file, dataset_index=ds_index)
            except Exception as e:
                raise click.ClickException(
                    f"{step} Error parsing model file: {e}"
                )

        # Assemble
        result = assembler.assemble(
            reduced=reduced_data,
            parquet=parquet_data,
            model=model_data,
            environment_description=measurement.environment,
            sample_id=sample_id,  # None for first measurement
        )

        if result.has_errors:
            click.echo(click.style(f"  Errors:", fg="red"), err=True)
            for error in result.errors:
                click.echo(f"    - {error}", err=True)
            sys.exit(1)

        for warning in result.warnings:
            click.echo(click.style(f"  Warning: {warning}", fg="yellow"), err=True)
        total_warnings.extend(result.warnings)

        # First measurement: capture the sample
        if i == 0 and result.sample:
            sample_record = result.sample
            sample_id = sample_record["id"]
            # Apply description override from manifest if provided
            if manifest_data.sample.description:
                sample_record["description"] = manifest_data.sample.description
            # Don't let per-measurement writers write the sample;
            # it will be written once at the end with all environment_ids.
            result.sample = None
            click.echo(f"  Sample: {sample_record['description']} ({sample_id[:8]}...)")
        elif sample_id:
            click.echo(f"  Sample: {sample_id[:8]}... (existing)")
        else:
            click.echo(click.style("  Warning: No sample created", fg="yellow"))

        # Track environment IDs
        if result.environment:
            all_environment_ids.append(result.environment["id"])

        # Print measurement summary
        if result.reflectivity:
            r = result.reflectivity
            q = r.get("q", []) or []
            click.echo(f"  Reflectivity: run {r.get('run_number')} ({len(q)} Q points)")

        if result.environment:
            click.echo(f"  Environment: {result.environment['description']}")

        if result.reflectivity_model:
            rm = result.reflectivity_model
            ds_idx = rm.get("dataset_index")
            ds_display = ds_idx + 1 if ds_idx is not None else "?"
            click.echo(
                f"  Model: dataset {ds_display}"
                f" of {rm.get('num_experiments', '?')}"
            )

        # Write output (unless dry run)
        if not dry_run:
            try:
                paths = write_assembly_to_parquet(result, output_path)

                if as_json:
                    # Write per-measurement JSON (use run number subfolder to avoid overwrites)
                    run_num = result.reflectivity.get("run_number", f"m{i+1}") if result.reflectivity else f"m{i+1}"
                    json_dir = output_path / "json" / str(run_num)
                    json_paths = write_assembly_to_json(result, json_dir)
                    for name, path in json_paths.items():
                        paths[f"{name}_json"] = path

                for table_name, path in paths.items():
                    all_paths.setdefault(table_name, []).append(str(path))

            except Exception as e:
                raise click.ClickException(
                    f"{step} Error writing output: {e}"
                )

        click.echo()

    # Update sample with all collected environment IDs and write once
    if sample_record and not dry_run:
        sample_record["environment_ids"] = all_environment_ids
        try:
            writer = ParquetWriter(output_path)
            sample_path = writer.write_sample(sample_record)
            all_paths.setdefault("sample", []).append(str(sample_path))

            if as_json:
                json_dir = output_path / "json"
                json_dir.mkdir(parents=True, exist_ok=True)
                from assembler.writers.json_writer import JSONWriter
                json_writer = JSONWriter(json_dir)
                json_writer.write_sample(sample_record)
        except Exception as e:
            raise click.ClickException(f"Error writing sample: {e}")

    # Print batch summary
    click.echo(click.style("─" * 50, fg="cyan"))
    click.echo(click.style(f"Batch complete: {title}", fg="green", bold=True))
    if sample_record:
        click.echo(f"  Sample: {sample_record.get('description')} ({sample_id})")
    click.echo(f"  Measurements: {len(manifest_data.measurements)}")
    click.echo(f"  Environments: {len(all_environment_ids)}")
    if total_warnings:
        click.echo(f"  Warnings: {len(total_warnings)}")

    if dry_run:
        click.echo(click.style("\nDry run - no files written", fg="cyan"))
    elif all_paths:
        click.echo(click.style("\nOutput files:", fg="green"))
        for table_name, paths_list in sorted(all_paths.items()):
            for p in paths_list:
                click.echo(f"  {table_name}: {p}")


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
            # For large numeric arrays, summarize
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
        if isinstance(v, dict):
            # Recursively serialize dict values
            return {k: serialize_value(val, k) for k, val in v.items()}
        if hasattr(v, "model_dump"):
            return _model_to_debug_dict(v)
        return v

    def _model_to_debug_dict(model) -> dict:
        """Convert a Pydantic model or dict to debug dict with field status."""
        if model is None:
            return {"_status": "MISSING", "_note": "Model not created"}

        # Handle plain dicts (AssemblyResult stores dicts, not Pydantic models)
        if isinstance(model, dict):
            result = {}
            for field_name, value in model.items():
                serialized = serialize_value(value, field_name)
                result[field_name] = serialized
            return result

        # Handle Pydantic models
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
            for attr in [
                "experiment_id",
                "run_number",
                "run_title",
                "run_start_time",
                "reduction_time",
                "reduction_version",
                "q_summing",
                "tof_weighted",
                "bck_in_q",
                "theta_offset",
            ]:
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
            "reflectivity": _model_to_debug_dict(result.reflectivity)
            if result.reflectivity
            else {
                "_status": "NOT_ASSEMBLED",
                "_note": "Reflectivity model was not created",
            },
            "sample": _model_to_debug_dict(result.sample)
            if result.sample
            else {
                "_status": "NOT_ASSEMBLED",
                "_note": "Sample model was not created (requires --model file)",
            },
            "environment": _model_to_debug_dict(result.environment)
            if result.environment
            else {
                "_status": "NOT_ASSEMBLED",
                "_note": "Environment model was not created",
            },
            "reflectivity_model": _model_to_debug_dict(result.reflectivity_model)
            if result.reflectivity_model
            else {
                "_status": "NOT_ASSEMBLED",
                "_note": "Reflectivity model was not created (requires --model file)",
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
    for section in ["reflectivity", "sample", "environment", "reflectivity_model"]:
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
        click.echo(f"  Reflectivity: {r['run_number']} - {r['run_title']}")
        facility = r.get("facility", "Unknown")
        click.echo(f"    Facility: {facility or 'Unknown'}")
        q = r.get("q", []) or []
        click.echo(f"    Q points: {len(q) if q else 0}")
        if q:
            click.echo(f"    Q range: {min(q):.4f} - {max(q):.4f} Å⁻¹")
    else:
        click.echo("  Reflectivity: Not assembled")

    if result.sample:
        s = result.sample
        click.echo(f"  Sample: {s['description']}")
        layers = s.get("layers", [])
        click.echo(f"    Layers: {len(layers) if layers else 0}")
        click.echo(f"    Main composition: {s.get('main_composition')}")
    elif result.external_sample_id:
        click.echo(f"  Sample: {result.external_sample_id} (existing)")
    else:
        click.echo("  Sample: Not assembled")

    if result.environment:
        e = result.environment
        click.echo(f"  Environment: {e['description']}")
        temp = e.get("temperature")
        if temp:
            click.echo(f"    Temperature: {temp:.1f} K")
        potential = e.get("potential")
        if potential is not None:
            click.echo(f"    Potential: {potential} V")
    else:
        click.echo("  Environment: Not assembled")

    if result.reflectivity_model:
        rm = result.reflectivity_model
        click.echo(f"  Reflectivity Model: {rm.get('model_name', 'Unknown')}")
        click.echo(
            f"    Software: {rm.get('software', '?')} {rm.get('software_version', '')}"
        )
        click.echo(f"    Experiments: {rm.get('num_experiments', 0)}")
        click.echo(
            f"    Parameters: {rm.get('num_free_parameters', 0)} free / "
            f"{rm.get('num_parameters', 0)} total"
        )
        model_layers = rm.get("layers", [])
        click.echo(f"    Layers: {len(model_layers)}")
    else:
        click.echo("  Reflectivity Model: Not assembled")


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
