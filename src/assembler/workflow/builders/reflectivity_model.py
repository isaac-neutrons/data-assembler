"""
Reflectivity model record builder.

Builds a record matching REFLECTIVITY_MODEL_SCHEMA from a parsed
refl1d/bumps model JSON file.
"""

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from assembler.parsers.model_parser import ModelData, ModelLayer

logger = logging.getLogger(__name__)


def _layers_to_structs(layers: list[ModelLayer]) -> list[dict[str, Any]]:
    """Convert parsed ModelLayer objects to REFLECTIVITY_MODEL layer structs."""
    return [
        {
            "layer_number": i + 1,
            "name": layer.name,
            "thickness": layer.thickness,
            "thickness_std": layer.thickness_std,
            "interface": layer.interface,
            "interface_std": layer.interface_std,
            "sld": layer.material.rho,
            "sld_std": layer.material.rho_std,
            "isld": layer.material.irho,
            "isld_std": layer.material.irho_std,
        }
        for i, layer in enumerate(layers)
    ]


def build_reflectivity_model_record(
    model: ModelData,
    measurement_ids: list[str],
    warnings: list[str],
    errors: list[str],
    needs_review: dict[str, Any],
    chi_squared: Optional[float] = None,
    *,
    datasets: Optional[list[dict[str, Any]]] = None,
    sample_id: Optional[str] = None,
    sample_ids: Optional[list[str]] = None,
    fit_strategy: Optional[str] = None,
    shared_parameters: Optional[list[str]] = None,
    unshared_parameters: Optional[list[str]] = None,
) -> Optional[dict[str, Any]]:
    """
    Build a reflectivity model (fit) record from parsed model data.

    Extracts software provenance, fit summary, and layer information from the
    parsed model. Stores the full JSON for reproducibility. This is the
    first-class FIT entity: ``measurement_ids`` links every run the fit used and
    ``datasets`` carries per-dataset fitted parameters.

    Args:
        model: Parsed model JSON data (must include raw_json)
        measurement_ids: ALL reflectivity run IDs this fit constrains (length N for
            an N-dataset co-refinement)
        warnings: List to append warnings to
        errors: List to append errors to
        needs_review: Dict to record fields needing review
        chi_squared: Overall reduced chi-squared of the fit
        datasets: Optional per-dataset entries, one per run, each a dict with keys
            ``dataset_index`` (int), ``measurement_id`` (str), ``run_number`` (str),
            ``chi_squared`` (float|None) and ``layers`` (list[ModelLayer]). When
            omitted, a single entry is synthesized from ``model.layers`` (the
            single-run / single-dataset case). The top-level ``layers`` mirror the
            primary dataset for back-compat with the ISAAC writer.
        sample_id / sample_ids: Sample(s) the fit constrains.
        fit_strategy: ``single`` | ``single_state_coref`` | ``multi_state_coref``;
            defaulted from ``num_experiments`` when not given.
        shared_parameters / unshared_parameters: tied/free parameter names.

    Returns:
        Dict matching REFLECTIVITY_MODEL_SCHEMA, or None on error
    """
    try:
        raw = model.raw_json or {}

        # Extract model name from the fit problem object
        obj = raw.get("object", {})
        model_name = obj.get("name") or None

        # Extract software/library info
        libraries = raw.get("libraries", {})
        # Prefer refl1d as the primary software, fall back to bumps
        if "refl1d" in libraries:
            software = "refl1d"
            software_version = libraries["refl1d"].get("version", "unknown")
            schema_version = libraries["refl1d"].get("schema_version", "unknown")
        elif "bumps" in libraries:
            software = "bumps"
            software_version = libraries["bumps"].get("version", "unknown")
            schema_version = libraries["bumps"].get("schema_version", "unknown")
        else:
            software = "unknown"
            software_version = "unknown"
            schema_version = raw.get("$schema", "unknown")
            warnings.append("Could not determine modeling software from libraries")

        # Count experiments
        models_list = obj.get("models", [])
        num_experiments = len(models_list)

        # Count parameters (references dict holds all parameters)
        references = raw.get("references", {})
        num_parameters = len(references)
        num_free_parameters = sum(
            1
            for ref in references.values()
            if isinstance(ref, dict) and not ref.get("fixed", True)
        )

        # Per-dataset fitted parameters. When the caller did not provide them
        # (single-run / single-dataset path), synthesize one entry from the
        # parsed model's currently-selected layers.
        if datasets is None:
            single_mid = measurement_ids[0] if measurement_ids else None
            datasets = [
                {
                    "dataset_index": model.dataset_index,
                    "measurement_id": single_mid,
                    "run_number": None,
                    "chi_squared": chi_squared,
                    "layers": model.layers,
                }
            ]

        datasets_structs = [
            {
                "dataset_index": d.get("dataset_index"),
                "measurement_id": d.get("measurement_id"),
                "run_number": d.get("run_number"),
                "chi_squared": d.get("chi_squared"),
                "layers": _layers_to_structs(d.get("layers") or []),
            }
            for d in datasets
        ]

        # Top-level layers mirror the primary/selected dataset (back-compat for
        # consumers reading the flat list, e.g. the ISAAC writer). Prefer the
        # dataset whose index matches model.dataset_index, else the first.
        primary = next(
            (d for d in datasets_structs if d["dataset_index"] == model.dataset_index),
            datasets_structs[0] if datasets_structs else None,
        )
        layers_structs = primary["layers"] if primary else _layers_to_structs(model.layers)

        if fit_strategy is None:
            fit_strategy = "single" if num_experiments <= 1 else "single_state_coref"

        # Serialize the full JSON for the model_json column
        model_json = json.dumps(raw, default=str)

        record = {
            # Base fields
            "id": str(uuid.uuid4()),
            "created_at": datetime.now(timezone.utc),
            "is_deleted": False,
            # Relationships
            "measurement_ids": measurement_ids,
            "sample_id": sample_id,
            "sample_ids": sample_ids or ([sample_id] if sample_id else []),
            # Fitting strategy + assumptions
            "fit_strategy": fit_strategy,
            "shared_parameters": shared_parameters or [],
            "unshared_parameters": unshared_parameters or [],
            # Model identification
            "model_name": model_name,
            "model_file_path": model.file_path or None,
            # Software provenance
            "software": software,
            "software_version": software_version,
            "schema_version": schema_version,
            # Fit summary
            "num_experiments": num_experiments,
            "dataset_index": model.dataset_index,
            "num_parameters": num_parameters,
            "num_free_parameters": num_free_parameters,
            "chi_squared": chi_squared,
            # Layers (primary dataset) + per-dataset breakdown
            "layers": layers_structs,
            "datasets": datasets_structs,
            # Full JSON
            "model_json": model_json,
        }

        return record

    except Exception as e:
        errors.append(f"Failed to build Reflectivity Model record: {e}")
        logger.exception("Error building Reflectivity Model record")
        return None
