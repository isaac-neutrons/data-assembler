"""
Tests for workflow components (Phase 1B).
"""

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from assembler.parsers.model_parser import ModelData, ModelLayer, ModelMaterial
from assembler.parsers.parquet_parser import MetadataRecord, ParquetData
from assembler.parsers.reduced_parser import ReducedData
from assembler.tools import (
    FileType,
)
from assembler.tools.detection import (
    detect_file_type,
    extract_ipts,
    extract_run_number,
)
from assembler.tools.types import FileInfo, RelatedFiles
from assembler.workflow import AssemblyResult, DataAssembler
from assembler.workflow.builders import build_reflectivity_model_record
from assembler.writers import ParquetWriter
from assembler.writers.parquet_writer import write_assembly_to_parquet


class TestFileDetection:
    """Tests for file type detection utilities."""

    def test_detect_reduced_file(self, tmp_path):
        """Test detection of reduced text files."""
        reduced_file = tmp_path / "REFL_218386_reduced.txt"
        reduced_file.write_text(
            "# Experiment IPTS-12345\n# Q [1/Angstrom]\n# Reduction\n1.0\t0.5\t0.01\t0.001"
        )

        file_type = detect_file_type(reduced_file)
        assert file_type == FileType.REDUCED

    def test_detect_parquet_file(self, tmp_path):
        """Test detection of parquet metadata files."""
        # Use the actual format from nexus-processor
        parquet_file = tmp_path / "REF_L_218386.parquet"
        # Create a minimal parquet file
        table = pa.table({"run_number": ["218386"]})
        pq.write_table(table, parquet_file)

        file_type = detect_file_type(parquet_file)
        assert file_type == FileType.PARQUET

    def test_detect_model_file(self, tmp_path):
        """Test detection of model JSON files."""
        model_file = tmp_path / "model_218386.json"
        model_file.write_text('{"sample": {"layers": []}}')

        file_type = detect_file_type(model_file)
        assert file_type == FileType.MODEL

    def test_extract_run_number_various_formats(self):
        """Test run number extraction from various filename formats."""
        assert extract_run_number("REFL_218386_reduced.txt") == 218386
        assert extract_run_number("REF_L_218386.parquet") == 218386
        assert extract_run_number("model_218386.json") == 218386

    def test_extract_ipts(self):
        """Test IPTS extraction from paths."""
        path = Path("/HFIR/CG1D/IPTS-12345/nexus/file.nxs")
        assert extract_ipts(path) == "IPTS-12345"

        path = Path("/data/IPTS-67890/reduced/file.txt")
        assert extract_ipts(path) == "IPTS-67890"

        path = Path("/home/user/data/file.txt")
        assert extract_ipts(path) is None


class TestDataAssembler:
    """Tests for the DataAssembler workflow."""

    def test_assemble_creates_reflectivity(self):
        """Test that assembler creates Reflectivity from parsed data."""
        assembler = DataAssembler()

        # Use proper data classes with required fields
        reduced_data = ReducedData(
            file_path="/tmp/test.txt",
            q=[0.01, 0.02, 0.03],
            r=[1.0, 0.8, 0.5],
            dr=[0.01, 0.01, 0.02],
            dq=[0.001, 0.001, 0.002],
        )

        # Create ParquetData with a MetadataRecord
        parquet_data = ParquetData(
            metadata=MetadataRecord(
                instrument_id="REF_L",
                run_number=218386,
                run_id="REF_L_218386",
                title="Test Run",
                start_time="2024-01-15T10:30:00Z",
                experiment_identifier="IPTS-12345",
            ),
        )

        result = assembler.assemble(
            reduced=reduced_data,
            parquet=parquet_data,
        )

        assert isinstance(result, AssemblyResult)
        assert result.reflectivity is not None
        assert result.reflectivity["run_number"] == "218386"
        assert result.reflectivity["q"] == [0.01, 0.02, 0.03]
        assert result.reflectivity["r"] == [1.0, 0.8, 0.5]

    def test_assemble_creates_sample_from_model(self):
        """Test that assembler creates Sample from model data."""
        assembler = DataAssembler()

        reduced_data = ReducedData(
            file_path="/tmp/test.txt",
            q=[0.01, 0.02],
            r=[1.0, 0.8],
            dr=[0.01, 0.01],
            dq=[0.001, 0.001],
        )

        parquet_data = ParquetData(
            metadata=MetadataRecord(
                instrument_id="REF_L",
                run_number=218386,
                run_id="REF_L_218386",
                title="Test",
                start_time="2024-01-15T00:00:00Z",
                experiment_identifier="IPTS-12345",
            ),
        )

        model_data = ModelData(
            file_path="/tmp/model.json",
            layers=[
                ModelLayer(
                    name="Gold",
                    thickness=100.0,
                    interface=5.0,
                    material=ModelMaterial(name="Au", rho=4.5),
                ),
                ModelLayer(
                    name="Silicon",
                    thickness=0.0,  # Substrate
                    interface=3.0,
                    material=ModelMaterial(name="Si", rho=2.07),
                ),
            ],
        )

        result = assembler.assemble(
            reduced=reduced_data,
            parquet=parquet_data,
            model=model_data,
        )

        assert result.sample is not None

    def test_assemble_without_parquet_uses_meta_block(self):
        """No-parquet ingest of the fixture file populates everything except raw_file_path."""
        from assembler.parsers.reduced_parser import ReducedParser

        fixture = Path(__file__).parent / "data" / "REFL_226658_2_226659_partial.txt"
        reduced_data = ReducedParser().parse(fixture)

        result = DataAssembler().assemble(reduced=reduced_data)

        assert result.reflectivity is not None
        refl = result.reflectivity
        assert refl["proposal_number"] == "IPTS-36897"
        assert refl["run_number"] == "226659"
        assert refl["run_title"] == "Sample_5-226658-2."
        assert refl["run_start"].year == 2026
        assert refl["instrument_name"] == "REF_L"
        assert refl["raw_file_path"] is None  # only gap when parquet is absent
        assert len(refl["q"]) > 0

    def test_assemble_raw_file_path_override(self):
        """raw_file_path arg overrides whatever parquet would have supplied."""
        reduced_data = ReducedData(
            file_path="/tmp/REFL_218386.txt",
            q=[0.01],
            r=[1.0],
            dr=[0.01],
            dq=[0.001],
        )
        parquet_data = ParquetData(
            metadata=MetadataRecord(
                instrument_id="REF_L",
                run_number=218386,
                run_id="REF_L_218386",
                title="t",
                start_time="2024-01-15T00:00:00Z",
                experiment_identifier="IPTS-1",
                source_path="/old/path.nxs.h5",
            ),
        )

        result = DataAssembler().assemble(
            reduced=reduced_data,
            parquet=parquet_data,
            raw_file_path="/override/path.nxs.h5",
        )
        assert result.reflectivity["raw_file_path"] == "/override/path.nxs.h5"

    def test_assemble_creates_environment(self):
        """Test that assembler creates Environment from metadata."""
        assembler = DataAssembler()

        reduced_data = ReducedData(
            file_path="/tmp/test.txt",
            q=[0.01, 0.02],
            r=[1.0, 0.8],
            dr=[0.01, 0.01],
            dq=[0.001, 0.001],
        )

        parquet_data = ParquetData(
            metadata=MetadataRecord(
                instrument_id="REF_L",
                run_number=218386,
                run_id="REF_L_218386",
                title="Test",
                start_time="2024-01-15T00:00:00Z",
                experiment_identifier="IPTS-12345",
            ),
            daslogs={
                "SampleTemp": type(
                    "DASLog",
                    (),
                    {
                        "average_value": 298.0,
                        "min_value": 295.0,
                        "max_value": 301.0,
                    },
                )(),
            },
        )

        result = assembler.assemble(reduced=reduced_data, parquet=parquet_data)

        assert result.environment is not None
        assert result.environment["temperature"] == 298.0


class TestParquetWriter:
    """Tests for Parquet output writing."""

    def test_write_reflectivity(self):
        """Test writing reflectivity record to Parquet."""
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = ParquetWriter(tmpdir)

            refl_record = {
                "id": None,
                "created_at": datetime.now(timezone.utc),
                "is_deleted": False,
                "proposal_number": "IPTS-12345",
                "facility": "SNS",
                "laboratory": "ORNL",
                "probe": "neutrons",
                "technique": "reflectivity",
                "technique_description": None,
                "is_simulated": False,
                "run_title": "Test Run",
                "run_number": "218386",
                "run_start": datetime(2024, 1, 15, tzinfo=timezone.utc),
                "raw_file_path": None,
                "instrument_name": "REF_L",
                "sample_id": None,
                "measurement_geometry": None,
                "reduction_time": None,
                "reduction_version": None,
                "q": [0.01, 0.02, 0.03],
                "r": [1.0, 0.8, 0.5],
                "dr": [0.01, 0.01, 0.02],
                "dq": [0.001, 0.001, 0.002],
            }

            path = writer.write_reflectivity(refl_record)

            assert path.exists()
            assert path.suffix == ".parquet"

            # Read single file using ParquetFile to avoid dataset discovery
            pf = pq.ParquetFile(str(path))
            table = pf.read()
            # Reflectivity data are flat
            assert "q" in table.column_names
            assert table.num_rows == 1

    def test_write_assembly(self):
        """Test writing full assembly result to Parquet."""
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = ParquetWriter(tmpdir)

            refl_record = {
                "id": None,
                "created_at": datetime.now(timezone.utc),
                "is_deleted": False,
                "proposal_number": "IPTS-12345",
                "facility": "SNS",
                "laboratory": "ORNL",
                "probe": "neutrons",
                "technique": "reflectivity",
                "technique_description": None,
                "is_simulated": False,
                "run_title": "Test Run",
                "run_number": "218386",
                "run_start": datetime.now(timezone.utc),
                "raw_file_path": None,
                "instrument_name": "REF_L",
                "sample_id": None,
                "measurement_geometry": None,
                "reduction_time": None,
                "reduction_version": None,
                "q": [0.01, 0.02],
                "r": [1.0, 0.8],
                "dr": [0.01, 0.01],
                "dq": [0.001, 0.001],
            }

            sample_record = {
                "id": None,
                "created_at": datetime.now(timezone.utc),
                "is_deleted": False,
                "description": "Test",
                "main_composition": None,
                "geometry": None,
                "environment_ids": [],
                "layers_json": None,
                "layers": [],
                "substrate_json": None,
            }

            env_record = {
                "id": None,
                "created_at": datetime.now(timezone.utc),
                "is_deleted": False,
                "description": "Test environment",
                "ambient_medium": None,
                "temperature": 298.0,
                "pressure": None,
                "relative_humidity": None,
                "measurement_ids": [],
            }

            assembly = AssemblyResult(
                reflectivity=refl_record,
                sample=sample_record,
                environment=env_record,
            )

            paths = writer.write(assembly)

            assert "reflectivity" in paths
            assert "sample" in paths
            assert "environment" in paths

            assert Path(paths["sample"]).exists()
            assert Path(paths["environment"]).exists()

    def test_partitioning(self):
        """Test that files are partitioned correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = ParquetWriter(
                tmpdir,
                partition_by_facility=True,
                partition_by_year=True,
            )

            refl_record = {
                "id": None,
                "created_at": datetime.now(timezone.utc),
                "is_deleted": False,
                "proposal_number": "IPTS-12345",
                "facility": "SNS",
                "laboratory": "ORNL",
                "probe": "neutrons",
                "technique": "reflectivity",
                "technique_description": None,
                "is_simulated": False,
                "run_title": "Test Run",
                "run_number": "218386",
                "run_start": datetime(2024, 1, 15, tzinfo=timezone.utc),
                "raw_file_path": None,
                "instrument_name": "REF_L",
                "sample_id": None,
                "measurement_geometry": None,
                "reduction_time": None,
                "reduction_version": None,
                "q": [0.01],
                "r": [1.0],
                "dr": [0.01],
                "dq": [0.001],
            }

            path = writer.write_reflectivity(refl_record)

            # Check path contains partition directories
            path_str = str(path)
            assert "facility=SNS" in path_str
            assert "year=2024" in path_str


class TestIntegration:
    """Integration tests for the full workflow."""

    def test_full_workflow(self):
        """Test complete workflow from parsed data to Parquet output."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # 1. Assemble data
            assembler = DataAssembler()

            reduced_data = ReducedData(
                file_path="/tmp/test.txt",
                q=[0.01, 0.02, 0.03, 0.04, 0.05],
                r=[1.0, 0.9, 0.7, 0.4, 0.2],
                dr=[0.01, 0.01, 0.01, 0.02, 0.02],
                dq=[0.001, 0.001, 0.001, 0.001, 0.001],
            )

            parquet_data = ParquetData(
                metadata=MetadataRecord(
                    instrument_id="REF_L",
                    run_number=218386,
                    run_id="REF_L_218386",
                    title="Integration Test",
                    start_time="2024-01-15T00:00:00Z",
                    experiment_identifier="IPTS-12345",
                ),
                daslogs={
                    "SampleTemp": type(
                        "DASLog",
                        (),
                        {
                            "average_value": 300.0,
                            "min_value": 298.0,
                            "max_value": 302.0,
                        },
                    )(),
                },
            )

            model_data = ModelData(
                file_path="/tmp/model.json",
                layers=[
                    ModelLayer(
                        name="Au",
                        thickness=100.0,
                        interface=5.0,
                        material=ModelMaterial(name="Au", rho=4.5),
                    ),
                ],
            )

            result = assembler.assemble(
                reduced=reduced_data,
                parquet=parquet_data,
                model=model_data,
            )

            # 2. Check assembly succeeded
            assert not result.has_errors, f"Assembly failed: {result.errors}"

            # 3. Write to Parquet
            paths = write_assembly_to_parquet(result, tmpdir)

            # 4. Verify output
            assert len(paths) >= 1
            for path in paths.values():
                assert Path(path).exists()
                # Read single file using ParquetFile to avoid dataset discovery
                pf = pq.ParquetFile(str(path))
                table = pf.read()
                assert table.num_rows == 1


class TestAssembleWorkflow:
    """Pull-based ingestion from a standard refl1d/AuRE workflow run directory."""

    def _make_run_dir(self, tmp_path):
        run = tmp_path / "run"
        run.mkdir()
        reduced = Path(__file__).parent / "data" / "REFL_226658_2_226659_partial.txt"
        (run / "run_info.json").write_text(
            json.dumps(
                {
                    "data_file": str(reduced),
                    "data_files": [{"file": str(reduced), "label": "REFL_226658"}],
                    "sample_description": "Cu / Ti on Si",
                }
            )
        )
        (run / "final_state.json").write_text(
            json.dumps(
                {
                    "final_chi2": 1.23,
                    "state": {
                        "best_chi2": 1.23,
                        "states": [
                            {"extra_description": "OCV in 0.1 M NaHCO3 electrolyte, pH 8.25"}
                        ],
                    },
                }
            )
        )
        return run

    def test_pull_parses_conditions_into_environment(self, tmp_path):
        run = self._make_run_dir(tmp_path)
        result = DataAssembler().assemble_workflow(run)

        assert not result.has_errors, result.errors
        assert result.reflectivity is not None
        env = result.environment
        assert env is not None
        assert env["control_mode"] == "open_circuit"
        assert env["pH"] == 8.25
        assert env["electrolyte"] == {"name": "NaHCO3", "concentration_M": 0.1}
        # description carried verbatim from the run state
        assert "OCV" in env["description"]

    def test_missing_run_info_is_an_error(self, tmp_path):
        empty = tmp_path / "empty"
        empty.mkdir()
        result = DataAssembler().assemble_workflow(empty)
        assert result.has_errors

    def test_read_fit_state_prefers_final_state(self, tmp_path):
        run = self._make_run_dir(tmp_path)
        chi, extra = DataAssembler._read_fit_state(run)
        assert chi == 1.23
        assert "OCV" in extra

    def test_find_err_json_locates_refl1d_output(self, tmp_path):
        run = tmp_path / "run"
        (run / "refl1d_output" / "fit_iter0_dream").mkdir(parents=True)
        err = run / "refl1d_output" / "fit_iter0_dream" / "None-err.json"
        err.write_text("{}")
        assert DataAssembler._find_err_json(run) == str(err)
        assert DataAssembler._find_err_json(tmp_path / "nope") is None

    def _make_multi_run_dir(self, tmp_path, n=3):
        """A run whose state lists *n* partials (angles). No model/problem.json."""
        run = tmp_path / "run"
        run.mkdir()
        reduced = Path(__file__).parent / "data" / "REFL_226658_2_226659_partial.txt"
        (run / "run_info.json").write_text(
            json.dumps(
                {
                    "data_files": [
                        {"file": str(reduced), "label": f"angle{i}"} for i in range(n)
                    ],
                    "sample_description": "Cu / Ti on Si",
                }
            )
        )
        # A fit state supplies the condition text → an environment is created,
        # which the runs link to (so the state is recoverable).
        (run / "final_state.json").write_text(
            json.dumps(
                {
                    "final_chi2": 1.0,
                    "state": {"states": [{"extra_description": "OCV in D2O"}]},
                }
            )
        )
        return run

    def test_multi_file_run_emits_one_record_per_run(self, tmp_path):
        """Every partial in the state becomes its own reflectivity record."""
        run = self._make_multi_run_dir(tmp_path, n=3)
        result = DataAssembler().assemble_workflow(run)

        assert not result.has_errors, result.errors
        assert len(result.reflectivities) == 3
        assert len(result.additional_reflectivities) == 2
        # Every run is tagged with the shared environment (the state's condition),
        # so the partials are recoverable as one state downstream.
        assert all(r.get("environment_id") for r in result.reflectivities)
        env_ids = {r["environment_id"] for r in result.reflectivities}
        assert len(env_ids) == 1  # same state → same environment for all angles
        assert len(result.environment["measurement_ids"]) == 3

    def test_single_file_run_unchanged(self, tmp_path):
        """A one-partial state still yields exactly one run (back-compat)."""
        run = self._make_multi_run_dir(tmp_path, n=1)
        result = DataAssembler().assemble_workflow(run)
        assert not result.has_errors, result.errors
        assert len(result.reflectivities) == 1
        assert result.additional_reflectivities == []

    def _make_multistate_run_dir(self, tmp_path):
        """A co-refinement run with explicit, user-named states[] (no model)."""
        run = tmp_path / "run"
        run.mkdir()
        reduced = Path(__file__).parent / "data" / "REFL_226658_2_226659_partial.txt"

        def df():
            return {"file": str(reduced), "label": "angle"}

        (run / "run_info.json").write_text(
            json.dumps(
                {
                    "sample_description": "Cu / Ti on Si",
                    "states": [
                        {
                            "name": "D2O OCV",
                            "extra_description": "OCV in 0.1 M NaHCO3 electrolyte, pH 8.25",
                            "data_files": [df(), df()],
                        },
                        {"name": "H2O OCV", "extra_description": "OCV in H2O", "data_files": [df()]},
                    ],
                }
            )
        )
        return run

    def test_multistate_groups_runs_per_state(self, tmp_path):
        """Explicit states[] → per-state environments; runs tagged by state, not merged."""
        run = self._make_multistate_run_dir(tmp_path)
        result = DataAssembler().assemble_workflow(run)

        assert not result.has_errors, result.errors
        # 2 + 1 = 3 runs across two states
        assert len(result.reflectivities) == 3
        assert len(result.environments) == 2
        # each run carries its state's environment_id; two distinct states
        env_ids = {r.get("environment_id") for r in result.reflectivities}
        assert len(env_ids) == 2
        # each environment tracks its own runs (2 angles + 1 angle)
        assert sorted(len(e["measurement_ids"]) for e in result.environments) == [1, 2]
        # the D2O state's conditions were parsed into its own environment
        d2o = next(e for e in result.environments if e.get("pH") == 8.25)
        assert d2o["control_mode"] == "open_circuit"

    @staticmethod
    def _layer(name, rho, thick=500.0):
        return {
            "name": name,
            "thickness": {"value": thick},
            "interface": {"value": 10.0},
            "material": {"name": name, "rho": {"value": rho}},
        }

    def _add_problem_json(self, run, n_models):
        """Write a minimal parseable refl1d problem.json with *n_models* experiments."""
        sample = {"layers": [self._layer("Cu", 6.5), self._layer("Si", 2.07, 0.0)]}
        (run / "problem.json").write_text(
            json.dumps(
                {
                    "object": {
                        "name": "fit",
                        "models": [{"sample": sample} for _ in range(n_models)],
                    },
                    "libraries": {"refl1d": {"version": "1.0", "schema_version": "v1"}},
                    "references": {},
                }
            )
        )

    def _set_run_info_flag(self, run, **flags):
        ri = json.loads((run / "run_info.json").read_text())
        ri.update(flags)
        (run / "run_info.json").write_text(json.dumps(ri))

    def test_multistate_default_shares_one_sample(self, tmp_path):
        """Default (no distinct_sample): co-refined states share one physical sample."""
        run = self._make_multistate_run_dir(tmp_path)  # 2 states: 2 + 1 runs
        self._add_problem_json(run, n_models=3)
        result = DataAssembler().assemble_workflow(run)

        assert not result.has_errors, result.errors
        assert len(result.samples) == 1  # one shared sample
        sids = {r.get("sample_id") for r in result.reflectivities}
        assert len(sids) == 1  # every run shares it
        # the shared sample links to both states' environments
        assert len(result.samples[0]["environment_ids"]) == 2
        assert result.reflectivity_model["sample_ids"] == list(sids)

    def test_multistate_distinct_sample_assigns_one_sample_per_state(self, tmp_path):
        """distinct_sample=True: each co-refined state is its own physical sample."""
        run = self._make_multistate_run_dir(tmp_path)  # 2 states: 2 + 1 runs
        self._add_problem_json(run, n_models=3)
        self._set_run_info_flag(run, distinct_sample=True)
        result = DataAssembler().assemble_workflow(run)

        assert not result.has_errors, result.errors
        # two distinct physical samples (one per state)
        assert len(result.samples) == 2
        sids = {r.get("sample_id") for r in result.reflectivities}
        assert len(sids) == 2
        # the fit spans both samples
        fit = result.reflectivity_model
        assert set(fit["sample_ids"]) == sids
        assert fit["sample_id"] in sids
        # each sample links only to its own state's environment, and to the fit
        for s in result.samples:
            assert len(s["environment_ids"]) == 1
            assert s["fit_ids"] == [fit["id"]]


class TestFitRecord:
    """The reflectivity_model record as a first-class fit entity."""

    def _model(self, num_experiments=2):
        return ModelData(
            file_path="/tmp/problem.json",
            layers=[
                ModelLayer(
                    name="Cu",
                    thickness=500.0,
                    interface=10.0,
                    material=ModelMaterial(name="Cu", rho=6.5),
                ),
            ],
            raw_json={
                "object": {"name": "cu_fit", "models": [{} for _ in range(num_experiments)]},
                "libraries": {"refl1d": {"version": "1.0", "schema_version": "v1"}},
                "references": {"p1": {"fixed": False}, "p2": {"fixed": True}},
            },
            dataset_index=0,
        )

    def test_fit_record_links_all_runs_with_per_dataset_params(self):
        model = self._model(num_experiments=2)
        datasets = [
            {
                "dataset_index": 0,
                "measurement_id": "run-a",
                "run_number": "100",
                "chi_squared": 1.1,
                "layers": model.layers,
            },
            {
                "dataset_index": 1,
                "measurement_id": "run-b",
                "run_number": "101",
                "chi_squared": 2.2,
                "layers": model.layers,
            },
        ]
        rec = build_reflectivity_model_record(
            model,
            measurement_ids=["run-a", "run-b"],
            warnings=[],
            errors=[],
            needs_review={},
            chi_squared=1.5,
            datasets=datasets,
            sample_id="sample-x",
        )
        assert rec["measurement_ids"] == ["run-a", "run-b"]
        assert rec["sample_id"] == "sample-x"
        assert rec["sample_ids"] == ["sample-x"]
        assert rec["num_experiments"] == 2
        assert rec["fit_strategy"] == "single_state_coref"
        assert len(rec["datasets"]) == 2
        assert [d["dataset_index"] for d in rec["datasets"]] == [0, 1]
        assert rec["datasets"][1]["measurement_id"] == "run-b"
        # top-level layers mirror the primary (dataset_index 0)
        assert len(rec["layers"]) == 1
        assert len(rec["datasets"][0]["layers"]) == 1

    def test_single_dataset_synthesized_when_not_provided(self):
        model = self._model(num_experiments=1)
        rec = build_reflectivity_model_record(
            model,
            measurement_ids=["run-a"],
            warnings=[],
            errors=[],
            needs_review={},
            chi_squared=1.0,
        )
        assert rec["fit_strategy"] == "single"
        assert len(rec["datasets"]) == 1
        assert rec["datasets"][0]["measurement_id"] == "run-a"

    def test_fit_record_round_trips_through_parquet(self, tmp_path):
        """The nested datasets[] struct must validate against the schema."""
        model = self._model(num_experiments=2)
        datasets = [
            {"dataset_index": i, "measurement_id": f"r{i}", "run_number": str(i),
             "chi_squared": None, "layers": model.layers}
            for i in range(2)
        ]
        rec = build_reflectivity_model_record(
            model, ["r0", "r1"], [], [], {}, chi_squared=1.0,
            datasets=datasets, sample_id="s",
        )
        path = ParquetWriter(tmp_path).write_reflectivity_model(rec)
        back = pq.ParquetFile(str(path)).read().to_pylist()[0]
        assert len(back["datasets"]) == 2
        assert back["measurement_ids"] == ["r0", "r1"]
        assert back["sample_id"] == "s"


class TestAssemblyResultReflectivities:
    """The reflectivities property aggregates primary + additional runs."""

    def test_reflectivities_property(self):
        r = AssemblyResult(
            reflectivity={"id": "a", "run_number": "1"},
            additional_reflectivities=[
                {"id": "b", "run_number": "2"},
                {"id": "c", "run_number": "3"},
            ],
        )
        assert [x["id"] for x in r.reflectivities] == ["a", "b", "c"]
        assert r.is_complete

    def test_reflectivities_empty(self):
        assert AssemblyResult().reflectivities == []
        assert not AssemblyResult().is_complete
