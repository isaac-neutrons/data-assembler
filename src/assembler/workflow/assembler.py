"""
Data assembler for combining multiple data sources.

The main orchestrator for the ingestion workflow.
"""

import json
import logging
from pathlib import Path
from typing import Any, Optional

from assembler.parsers.model_parser import ModelData
from assembler.parsers.parquet_parser import ParquetData
from assembler.parsers.reduced_parser import ReducedData

from .builders import (
    build_environment_record,
    build_reflectivity_model_record,
    build_reflectivity_record,
    build_sample_record,
)
from .result import AssemblyResult

logger = logging.getLogger(__name__)


class DataAssembler:
    """
    Assembles data from multiple sources into schema-ready records.

    Workflow:
    1. Parse reduced data → Reflectivity record (Q, R, dR, dQ + metadata)
    2. Parse parquet files → Environment record from DASlogs
    3. Parse model JSON → Sample record with layers

    Example:
        assembler = DataAssembler()

        # From parsed data
        result = assembler.assemble(
            reduced=reduced_data,
            parquet=parquet_data,
            model=model_data,
        )

        # Check result
        if result.is_complete:
            refl = result.reflectivity
            print(f"Assembled {len(refl['q'])} Q points")
        if result.needs_human_review:
            print(f"Review needed: {result.needs_review}")
    """

    def assemble(
        self,
        reduced: Optional[ReducedData] = None,
        parquet: Optional[ParquetData] = None,
        model: Optional[ModelData] = None,
        environment_description: Optional[str] = None,
        sample_id: Optional[str] = None,
        raw_file_path: Optional[str] = None,
        conditions: Optional[dict[str, Any]] = None,
        chi_squared: Optional[float] = None,
    ) -> AssemblyResult:
        """
        Assemble data from parsed sources into schema-ready records.

        At minimum, reduced data is required. Parquet and model data
        enrich the result with additional metadata.

        If the model contains multiple experiments (co-refinement) and
        no dataset_index was explicitly set on the model, the assembler
        will auto-detect the matching experiment by comparing Q arrays
        from the reduced data against each experiment's probe.

        Args:
            reduced: Parsed reduced reflectivity data (required)
            parquet: Parsed parquet metadata (optional)
            model: Parsed model JSON data (optional)
            environment_description: Optional description text for the environment record
            sample_id: Optional UUID of an existing sample to link to instead of
                creating a new sample record
            raw_file_path: Optional path to the raw data (nexus) file. When provided,
                this overrides any path discovered in parquet metadata and is used
                to populate the reflectivity record's `raw_file_path` field.

        Returns:
            AssemblyResult with assembled records and any issues
        """
        result = AssemblyResult()

        if reduced is None:
            result.errors.append("Reduced data is required")
            return result

        result.reduced_file = reduced.file_path

        # Auto-detect dataset if model has multiple experiments and none was chosen
        if model is not None and model.dataset_index is None and model.num_datasets > 1:
            matched = self._auto_detect_dataset(model, reduced)
            if matched is not None:
                logger.info(
                    f"Auto-detected model dataset {matched + 1}/{model.num_datasets} "
                    f"by reflectivity matching"
                )
                model.select_dataset(matched)
                result.warnings.append(
                    f"Auto-selected model dataset {matched + 1} of "
                    f"{model.num_datasets} (matched by reflectivity data)"
                )
            else:
                result.warnings.append(
                    f"Model has {model.num_datasets} datasets but could not "
                    f"auto-detect match; using first dataset. "
                    f"Consider passing --model-dataset-index."
                )
                model.select_dataset(0)

        # Step 1: Build Reflectivity record from reduced + parquet + model
        result.reflectivity = build_reflectivity_record(
            reduced=reduced,
            parquet=parquet,
            warnings=result.warnings,
            errors=result.errors,
            needs_review=result.needs_review,
            model=model,
            raw_file_path_override=raw_file_path,
        )

        # Step 2: Build Environment record from parquet daslogs
        if parquet is not None:
            result.environment = build_environment_record(
                parquet=parquet,
                warnings=result.warnings,
                errors=result.errors,
                needs_review=result.needs_review,
                model=model,
                description_override=environment_description,
                conditions=conditions,
            )
        elif environment_description is not None or conditions:
            # Create a minimal environment record from the description/conditions alone
            result.environment = self._build_minimal_environment(
                description=environment_description,
                model=model,
                conditions=conditions,
            )

        # Step 3: Build Sample record from model (skip if reusing existing sample)
        if sample_id is not None:
            result.external_sample_id = sample_id
            logger.info(f"Using existing sample: {sample_id}")
        elif model is not None:
            result.sample = build_sample_record(
                model=model,
                warnings=result.warnings,
                errors=result.errors,
                needs_review=result.needs_review,
            )

        if model is not None:
            result.model_file = model.file_path

        # Step 4: Link IDs across the hierarchy (sample -> environment -> measurement)
        self._link_record_ids(result)

        # Step 5: Build Reflectivity Model (fit) record (needs measurement IDs from linking)
        if model is not None and model.raw_json is not None:
            measurement_ids = [result.reflectivity["id"]] if result.reflectivity else []
            fit_sample_id = result.sample["id"] if result.sample else result.external_sample_id
            result.reflectivity_model = build_reflectivity_model_record(
                model=model,
                measurement_ids=measurement_ids,
                warnings=result.warnings,
                errors=result.errors,
                needs_review=result.needs_review,
                chi_squared=chi_squared,
                sample_id=fit_sample_id,
            )
            if result.reflectivity_model and result.sample:
                result.sample["fit_ids"] = [result.reflectivity_model["id"]]

        return result

    def assemble_workflow(
        self,
        run_dir: str | Path,
        dataset_index: Optional[int] = None,
        sample_id: Optional[str] = None,
    ) -> AssemblyResult:
        """Assemble records by PULLING from a standard refl1d/AuRE run directory.

        Reads, from ``run_dir``:
          - ``run_info.json``    → reduced data file(s) + sample_description
          - ``problem.json``     → fitted model
          - ``refl1d_output/**/<name>-err.json`` → per-parameter σ
          - ``final_state.json`` → goodness-of-fit χ² + experimental conditions
            (``state.states[].extra_description``)

        Free-text conditions are parsed into structured electrochemical fields.
        Returns an AssemblyResult; the caller writes it out (parquet/json).
        """
        from assembler.parsers import ModelParser, ReducedParser
        from assembler.parsers.conditions import parse_conditions

        run_dir = Path(run_dir)
        result = AssemblyResult()

        run_info_path = run_dir / "run_info.json"
        if not run_info_path.is_file():
            result.errors.append(f"run_info.json not found in {run_dir}")
            return result
        run_info = json.loads(run_info_path.read_text())

        # Explicit, user-named multi-state co-refinement: states[] is authoritative
        # (never inferred from file names). When absent, the flat data_files below
        # are one single state.
        states = run_info.get("states")
        if states:
            return self._assemble_multistate(run_dir, run_info, states, dataset_index, sample_id)

        # All listed data files are runs of ONE measurement state (partials =
        # different incident angles). Each becomes its own reflectivity record;
        # together they are one state (downstream: one ISAAC record, N series).
        data_files = run_info.get("data_files") or []
        file_paths = [(df.get("file") if isinstance(df, dict) else df) for df in data_files]
        if not file_paths and run_info.get("data_file"):
            file_paths = [run_info["data_file"]]
        if not file_paths:
            result.errors.append(f"No data files listed in {run_info_path}")
            return result
        for fp in file_paths:
            if not fp or not Path(fp).is_file():
                result.errors.append(f"Reduced data file from run_info.json not found: {fp}")
                return result
        reduced_list = [ReducedParser().parse(fp) for fp in file_paths]

        # Fitted model + σ companion from the refl1d output.
        model = None
        problem = run_dir / "problem.json"
        if problem.is_file():
            err_path = self._find_err_json(run_dir)
            model = ModelParser().parse(problem, dataset_index=dataset_index, err_path=err_path)

        # Goodness-of-fit + conditions from the workflow state.
        chi_squared, extra_description = self._read_fit_state(run_dir)
        conditions = parse_conditions(extra_description)

        # Primary run → full assemble (builds sample, environment, and a fit record).
        assembled = self.assemble(
            reduced=reduced_list[0],
            model=model,
            environment_description=extra_description,
            conditions=conditions,
            chi_squared=chi_squared,
            sample_id=sample_id,
        )
        if assembled.has_errors:
            return assembled

        # Additional runs (other angles of the same state) → reflectivity-only.
        for reduced_i in reduced_list[1:]:
            refl_i = build_reflectivity_record(
                reduced=reduced_i,
                parquet=None,
                warnings=assembled.warnings,
                errors=assembled.errors,
                needs_review=assembled.needs_review,
                model=model,
            )
            if refl_i is not None:
                assembled.additional_reflectivities.append(refl_i)

        # Re-link so every run carries sample_id/environment_id and the
        # environment tracks all run ids.
        self._link_record_ids(assembled)

        # Rebuild the FIT as a first-class entity spanning all runs, with
        # per-dataset fitted parameters (overrides the single-run fit that
        # assemble() built for the primary).
        if model is not None and model.raw_json is not None and len(reduced_list) > 1:
            datasets = []
            n_runs = len(reduced_list)
            for i, (reduced_i, refl_i) in enumerate(zip(reduced_list, assembled.reflectivities)):
                idx = self._auto_detect_dataset(model, reduced_i)
                if idx is None:
                    # Positional fallback: in a single-state co-refinement the
                    # partials and the problem's experiments are 1:1 in order.
                    if model.num_datasets == n_runs:
                        idx = i
                    else:
                        idx = model.dataset_index or 0
                        assembled.warnings.append(
                            f"Could not match run {refl_i.get('run_number')} to a fit "
                            f"dataset ({model.num_datasets} datasets vs {n_runs} runs); "
                            f"using dataset {idx} — per-dataset fit params may be wrong."
                        )
                        assembled.needs_review[f"fit_dataset_{refl_i.get('run_number')}"] = (
                            "ambiguous dataset assignment"
                        )
                try:
                    layers_i = model.layers_for_dataset(idx)
                except ValueError:
                    layers_i = model.layers
                run_num = refl_i.get("run_number")
                datasets.append(
                    {
                        "dataset_index": idx,
                        "measurement_id": refl_i.get("id"),
                        "run_number": str(run_num) if run_num is not None else None,
                        "chi_squared": None,
                        "layers": layers_i,
                    }
                )
            fit_sample_id = (
                assembled.sample["id"] if assembled.sample else assembled.external_sample_id
            )
            assembled.reflectivity_model = build_reflectivity_model_record(
                model=model,
                measurement_ids=[r["id"] for r in assembled.reflectivities if r.get("id")],
                warnings=assembled.warnings,
                errors=assembled.errors,
                needs_review=assembled.needs_review,
                chi_squared=chi_squared,
                datasets=datasets,
                sample_id=fit_sample_id,
            )
            if assembled.reflectivity_model and assembled.sample:
                assembled.sample["fit_ids"] = [assembled.reflectivity_model["id"]]

        # The human sample description is the run's stack summary.
        sample_description = run_info.get("sample_description")
        if assembled.sample and sample_description:
            assembled.sample["description"] = sample_description

        return assembled

    def _assemble_multistate(
        self,
        run_dir: Path,
        run_info: dict,
        states: list[dict],
        dataset_index: Optional[int],
        sample_id: Optional[str],
    ) -> AssemblyResult:
        """Assemble an explicit multi-state co-refinement (one record set per state).

        Each entry of ``states`` is a user-named physical condition with its own
        ``data_files`` (the angles) and ``extra_description`` (conditions). Every
        state gets its own environment and is tagged with a per-state
        ``(sample_id, environment_id)`` — not assumed shared — and the whole
        co-refinement is ONE fit linking every run across every state. State
        identity comes from the explicit ``states[]`` block, never from file names.
        """
        from assembler.parsers import ModelParser, ReducedParser
        from assembler.parsers.conditions import parse_conditions

        result = AssemblyResult()

        # Fitted model (shared structure across all states of a co-refinement).
        model = None
        problem = run_dir / "problem.json"
        if problem.is_file():
            err_path = self._find_err_json(run_dir)
            model = ModelParser().parse(problem, dataset_index=dataset_index, err_path=err_path)

        # Shared physical sample (the common case). A state may carry its own
        # ``sample_description``; distinct descriptions get their own sample record
        # so sample_id is not forced to be shared.
        shared_sample = None
        if sample_id is not None:
            result.external_sample_id = sample_id
        elif model is not None:
            shared_sample = build_sample_record(
                model=model,
                warnings=result.warnings,
                errors=result.errors,
                needs_review=result.needs_review,
            )
            result.sample = shared_sample
        shared_sid = shared_sample["id"] if shared_sample else sample_id
        sample_desc = run_info.get("sample_description")
        if shared_sample and sample_desc:
            shared_sample["description"] = sample_desc

        # Identity: do the co-refined states denote distinct physical samples
        # (a sample per state) or one sample under several conditions (shared,
        # the default)? Orthogonal to per-state structure — set by AuRE's
        # ``distinct_sample`` flag. Tracking maps each owned sample id to the
        # environments of its own states so links stay per-sample.
        distinct_sample = bool(run_info.get("distinct_sample", False))
        sample_by_id: dict[str, dict] = {}
        if shared_sample:
            sample_by_id[shared_sid] = shared_sample
        sample_env_ids: dict[str, list[str]] = {}
        per_state_sids: list[str] = []

        all_run_ids: list[str] = []
        reduced_refl_pairs: list[tuple] = []  # (ReducedData, refl) across all states
        env_ids: list[str] = []

        for si, state in enumerate(states):
            files = [
                (df.get("file") if isinstance(df, dict) else df)
                for df in (state.get("data_files") or [])
            ]
            files = [f for f in files if f]
            if not files:
                result.errors.append(f"State '{state.get('name')}' has no data_files.")
                return result
            for f in files:
                if not Path(f).is_file():
                    result.errors.append(
                        f"Data file not found for state '{state.get('name')}': {f}"
                    )
                    return result
            st_reduced = [ReducedParser().parse(f) for f in files]

            extra = state.get("extra_description")
            env = self._build_minimal_environment(
                description=extra, model=model, conditions=parse_conditions(extra)
            )
            # Per-state sample. By default every state shares one physical
            # sample. A state gets its OWN sample record when either (a) the
            # run is flagged ``distinct_sample`` (co-refined states are
            # different physical samples) — state 0 keeps the shared/primary
            # sample, states 1.. each get their own — or (b) it carries a
            # distinct ``sample_description``.
            state_sid = shared_sid
            state_sample_obj = shared_sample
            state_sample_desc = state.get("sample_description")
            wants_distinct = (distinct_sample and si > 0) or (
                state_sample_desc and state_sample_desc != sample_desc
            )
            if wants_distinct and model is not None:
                state_sample = build_sample_record(
                    model=model,
                    warnings=result.warnings,
                    errors=result.errors,
                    needs_review=result.needs_review,
                )
                if state_sample_desc:
                    state_sample["description"] = state_sample_desc
                elif sample_desc:
                    state_sample["description"] = sample_desc
                result.additional_samples.append(state_sample)
                state_sid = state_sample["id"]
                state_sample_obj = state_sample
            if state_sample_obj is not None:
                sample_by_id[state_sid] = state_sample_obj
            per_state_sids.append(state_sid)

            st_refls: list[dict] = []
            for r in st_reduced:
                refl = build_reflectivity_record(
                    reduced=r,
                    parquet=None,
                    warnings=result.warnings,
                    errors=result.errors,
                    needs_review=result.needs_review,
                    model=model,
                )
                if refl is None:
                    continue
                refl["sample_id"] = state_sid
                refl["environment_id"] = env["id"]
                st_refls.append(refl)
                all_run_ids.append(refl["id"])
                reduced_refl_pairs.append((r, refl))
            env["measurement_ids"] = [r["id"] for r in st_refls]
            env_ids.append(env["id"])
            sample_env_ids.setdefault(state_sid, []).append(env["id"])

            if si == 0:
                result.reflectivity = st_refls[0] if st_refls else None
                result.additional_reflectivities.extend(st_refls[1:])
                result.environment = env
            else:
                result.additional_reflectivities.extend(st_refls)
                result.additional_environments.append(env)

        # Link each owned sample to only the environments of its own states
        # (shared sample → all envs; distinct samples → their own).
        for sid, evs in sample_env_ids.items():
            s = sample_by_id.get(sid)
            if s is not None:
                s["environment_ids"] = evs

        # ONE fit over every run across every state.
        if model is not None and model.raw_json is not None:
            chi_squared, _ = self._read_fit_state(run_dir)
            n = len(reduced_refl_pairs)
            datasets: list[dict] = []
            for i, (red, refl) in enumerate(reduced_refl_pairs):
                idx = self._auto_detect_dataset(model, red)
                if idx is None:
                    if model.num_datasets == n:
                        idx = i
                    else:
                        idx = model.dataset_index or 0
                        result.warnings.append(
                            f"Could not match run {refl.get('run_number')} to a fit "
                            f"dataset ({model.num_datasets} datasets vs {n} runs); "
                            f"using dataset {idx} — per-dataset fit params may be wrong."
                        )
                try:
                    layers_i = model.layers_for_dataset(idx)
                except ValueError:
                    layers_i = model.layers
                run_num = refl.get("run_number")
                datasets.append(
                    {
                        "dataset_index": idx,
                        "measurement_id": refl.get("id"),
                        "run_number": str(run_num) if run_num is not None else None,
                        "chi_squared": None,
                        "layers": layers_i,
                    }
                )
            # When the states are distinct samples the fit constrains all of
            # them; record every sample id (deduped, order-preserved). The
            # primary ``sample_id`` stays state 0's.
            fit_sample_ids = (
                list(dict.fromkeys(per_state_sids)) if distinct_sample else None
            )
            result.reflectivity_model = build_reflectivity_model_record(
                model=model,
                measurement_ids=all_run_ids,
                warnings=result.warnings,
                errors=result.errors,
                needs_review=result.needs_review,
                chi_squared=chi_squared,
                datasets=datasets,
                sample_id=shared_sid,
                sample_ids=fit_sample_ids,
                fit_strategy="multi_state_coref",
            )
            if result.reflectivity_model:
                fid = result.reflectivity_model["id"]
                target_sids = (
                    list(dict.fromkeys(per_state_sids))
                    if distinct_sample
                    else ([shared_sid] if shared_sample else [])
                )
                for sid in target_sids:
                    s = sample_by_id.get(sid)
                    if s is not None:
                        s.setdefault("fit_ids", []).append(fid)

        return result

    @staticmethod
    def _find_err_json(run_dir: Path) -> Optional[str]:
        """Locate the refl1d fit-uncertainty file under ``refl1d_output/``."""
        output_dir = run_dir / "refl1d_output"
        if not output_dir.is_dir():
            return None
        matches = sorted(output_dir.rglob("*-err.json"))
        return str(matches[0]) if matches else None

    @staticmethod
    def _read_fit_state(run_dir: Path) -> tuple[Optional[float], Optional[str]]:
        """Pull χ² and the experimental-condition text from the workflow state.

        Prefers ``final_state.json``; falls back to
        ``checkpoints/004_fitting.json``. Returns ``(chi_squared, extra_description)``.
        """

        def _from_state(state: dict) -> tuple[Optional[float], Optional[str]]:
            chi = state.get("best_chi2") or state.get("current_chi2")
            extra = None
            states = state.get("states") or []
            if states and isinstance(states[0], dict):
                extra = states[0].get("extra_description")
            return chi, extra

        final = run_dir / "final_state.json"
        if final.is_file():
            data = json.loads(final.read_text())
            state = data.get("state") or {}
            chi2, extra = _from_state(state)
            return (data.get("final_chi2") if data.get("final_chi2") is not None else chi2), extra

        ckpt = run_dir / "checkpoints" / "004_fitting.json"
        if ckpt.is_file():
            data = json.loads(ckpt.read_text())
            return _from_state(data.get("state") or {})

        return None, None

    def _link_record_ids(self, result: AssemblyResult) -> None:
        """
        Link record IDs across the hierarchy.

        Hierarchy: Sample -> Environment -> Reflectivity (measurement)
        - Every reflectivity run gets sample_id and environment_id (foreign keys)
        - Environment tracks measurement_ids (all runs of this state)
        - Sample tracks environment_ids

        When external_sample_id is set (reusing an existing sample),
        that ID is used for linking without creating a new sample record.
        Safe to call repeatedly (idempotent); call again after appending
        additional run records so the foreign keys cover every run.
        """
        sample_id = result.sample["id"] if result.sample else result.external_sample_id
        environment_id = result.environment["id"] if result.environment else None
        reflectivity_ids = [r["id"] for r in result.reflectivities if r.get("id")]

        # Stamp foreign keys on every run record (kept at the run level so a
        # state is derivable as runs sharing (sample_id, environment_id)).
        for refl in result.reflectivities:
            if sample_id:
                refl["sample_id"] = sample_id
            if environment_id:
                refl["environment_id"] = environment_id

        # Track measurement IDs in environment (all runs of this state)
        if result.environment and reflectivity_ids:
            result.environment["measurement_ids"] = reflectivity_ids

        # Track environment IDs in sample
        if result.sample and environment_id:
            result.sample["environment_ids"] = [environment_id]

    @staticmethod
    def _build_minimal_environment(
        description: Optional[str] = None,
        model: Optional[ModelData] = None,
        conditions: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """
        Build a minimal environment record when no parquet data is available.

        Used when the user provides --environment but no --parquet directory.
        Creates a record with the description and ambient medium (from model),
        but without instrument-extracted fields like temperature.

        Args:
            description: User-provided environment description
            model: Optional model data for ambient medium extraction

        Returns:
            Dict matching ENVIRONMENT_SCHEMA
        """
        import uuid
        from datetime import datetime, timezone

        ambient_medium = None
        if model and model.ambient:
            ambient_medium = model.ambient.material.name

        cond = conditions or {}
        return {
            "id": str(uuid.uuid4()),
            "created_at": datetime.now(timezone.utc),
            "description": description,
            "ambient_medium": {
                "name": ambient_medium,
                "mass": None,
                "density": None,
            },
            "temperature": None,
            "pressure": None,
            "relative_humidity": None,
            "potential": cond.get("potential"),
            "potential_scale": cond.get("potential_scale"),
            "control_mode": cond.get("control_mode"),
            "electrolyte": cond.get("electrolyte"),
            "pH": cond.get("pH"),
            "measurement_ids": [],
        }

    @staticmethod
    def _auto_detect_dataset(model: ModelData, reduced: ReducedData) -> Optional[int]:
        """
        Auto-detect which experiment in a co-refinement model matches the reduced data.

        Uses two strategies:
        1. Q-range comparison (min/max) — works when experiments have different Q ranges
        2. R-value correlation — interpolates model R onto reduced Q grid and compares
           using mean absolute relative error. Works when Q grids overlap.

        Args:
            model: Parsed model with raw_json containing multiple experiments
            reduced: Parsed reduced data with Q and R arrays

        Returns:
            0-based experiment index, or None if no match found
        """
        if not reduced.q or len(reduced.q) == 0:
            return None

        from bisect import bisect_left

        reduced_q_min = min(reduced.q)
        reduced_q_max = max(reduced.q)

        best_idx: Optional[int] = None
        best_score = float("inf")

        for i in range(model.num_datasets):
            probe_q = model.get_probe_q(i)
            probe_r = model.get_probe_r(i)
            if not probe_q or not probe_r:
                continue

            # Strategy 1: Q-range score (relative difference in boundaries)
            q_min = min(probe_q)
            q_max = max(probe_q)
            min_diff = abs(reduced_q_min - q_min) / max(reduced_q_min, 1e-10)
            max_diff = abs(reduced_q_max - q_max) / max(reduced_q_max, 1e-10)
            q_range_score = min_diff + max_diff

            # Strategy 2: R-value comparison via linear interpolation
            # Interpolate model R at each reduced Q point
            r_score = float("inf")
            if reduced.r and len(reduced.r) == len(reduced.q) and len(probe_q) >= 2:
                # Ensure probe_q is sorted for interpolation
                if probe_q[0] <= probe_q[-1]:
                    sorted_q = probe_q
                    sorted_r = probe_r
                else:
                    pairs = sorted(zip(probe_q, probe_r))
                    sorted_q = [p[0] for p in pairs]
                    sorted_r = [p[1] for p in pairs]

                total_err = 0.0
                n_matched = 0
                for j, rq in enumerate(reduced.q):
                    # Only compare where reduced Q falls within model Q range
                    if rq < sorted_q[0] or rq > sorted_q[-1]:
                        continue
                    # Find interpolation position
                    pos = bisect_left(sorted_q, rq)
                    if pos == 0:
                        interp_r = sorted_r[0]
                    elif pos >= len(sorted_q):
                        interp_r = sorted_r[-1]
                    else:
                        # Linear interpolation
                        q0, q1 = sorted_q[pos - 1], sorted_q[pos]
                        r0, r1 = sorted_r[pos - 1], sorted_r[pos]
                        t = (rq - q0) / (q1 - q0) if q1 != q0 else 0.5
                        interp_r = r0 + t * (r1 - r0)

                    # Relative error (use abs(reduced R) to normalize)
                    rr = reduced.r[j]
                    denom = max(abs(rr), 1e-12)
                    total_err += abs(interp_r - rr) / denom
                    n_matched += 1

                if n_matched > 0:
                    r_score = total_err / n_matched

            # Combined score: prefer R-value match when Q ranges are similar
            if r_score < float("inf"):
                score = r_score  # R-value match is the primary discriminator
            else:
                score = q_range_score

            if score < best_score:
                best_score = score
                best_idx = i

        # Accept if R-value mean relative error < 10% or Q-range score < 0.5
        if best_idx is not None and best_score < 0.5:
            return best_idx

        return None
