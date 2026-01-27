"""
Tests for workflow components (Phase 1B).
"""

import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from assembler.parsers import ModelData, ParquetData, ReducedData, MetadataRecord, ModelLayer, ModelMaterial
from assembler.tools import (
    FileInfo,
    FileType,
    RelatedFiles,
    detect_file_type,
    extract_ipts,
    extract_run_number,
)
from assembler.validation import DataValidator, ValidationResult
from assembler.workflow import AssemblyResult, DataAssembler
from assembler.writers import ParquetWriter, write_assembly_to_parquet


class TestFileDetection:
    """Tests for file type detection utilities."""

    def test_detect_reduced_file(self, tmp_path):
        """Test detection of reduced text files."""
        reduced_file = tmp_path / "REFL_218386_reduced.txt"
        reduced_file.write_text("# Experiment IPTS-12345\n# Q [1/Angstrom]\n# Reduction\n1.0\t0.5\t0.01\t0.001")
        
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
        assert result.reflectivity["reflectivity"]["q"] == [0.01, 0.02, 0.03]
        assert result.reflectivity["reflectivity"]["r"] == [1.0, 0.8, 0.5]

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
            ]
        )
        
        result = assembler.assemble(
            reduced=reduced_data,
            parquet=parquet_data,
            model=model_data,
        )
        
        assert result.sample is not None

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
                "SampleTemp": type("DASLog", (), {
                    "average_value": 298.0,
                    "min_value": 295.0, 
                    "max_value": 301.0,
                })(),
            },
        )
        
        result = assembler.assemble(reduced=reduced_data, parquet=parquet_data)
        
        assert result.environment is not None
        assert result.environment["temperature"] == 298.0


class TestValidation:
    """Tests for the validation layer."""

    def test_validate_assembly_passes(self):
        """Test validation of valid assembly with reflectivity record."""
        validator = DataValidator()
        
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
            "reflectivity": {
                "measurement_geometry": None,
                "reduction_time": None,
                "reduction_version": None,
                "reduction_parameters": None,
                "q": [0.01, 0.02, 0.03],
                "r": [1.0, 0.8, 0.5],
                "dr": [0.01, 0.01, 0.02],
                "dq": [0.001, 0.001, 0.002],
            },
        }
        
        assembly = AssemblyResult(reflectivity=refl_record)
        result = validator.validate(assembly)
        
        assert isinstance(result, ValidationResult)
        # Check for critical errors
        errors = [i for i in result.issues if i.severity == "error"]
        assert len(errors) == 0, f"Unexpected errors: {errors}"

    def test_validate_array_length_mismatch(self):
        """Test validation catches array length mismatches."""
        validator = DataValidator()
        
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
            "reflectivity": {
                "measurement_geometry": None,
                "reduction_time": None,
                "reduction_version": None,
                "reduction_parameters": None,
                "q": [0.01, 0.02, 0.03],
                "r": [1.0, 0.8],  # Wrong length!
                "dr": [0.01, 0.01, 0.02],
                "dq": [0.001, 0.001, 0.002],
            },
        }
        
        assembly = AssemblyResult(reflectivity=refl_record)
        result = validator.validate(assembly)
        
        errors = [i for i in result.issues if i.severity == "error"]
        assert len(errors) > 0
        assert any("length" in str(i.message).lower() for i in errors)

    def test_validate_negative_uncertainties(self):
        """Test validation catches negative uncertainties."""
        validator = DataValidator()
        
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
            "reflectivity": {
                "measurement_geometry": None,
                "reduction_time": None,
                "reduction_version": None,
                "reduction_parameters": None,
                "q": [0.01, 0.02, 0.03],
                "r": [1.0, 0.8, 0.5],
                "dr": [-0.01, 0.01, 0.02],  # Negative!
                "dq": [0.001, 0.001, 0.002],
            },
        }
        
        assembly = AssemblyResult(reflectivity=refl_record)
        result = validator.validate(assembly)
        
        warnings = [i for i in result.issues if i.severity in ("warning", "error")]
        assert len(warnings) > 0

    def test_validate_sample(self):
        """Test validation of sample record."""
        validator = DataValidator()
        
        sample_record = {
            "id": None,
            "created_at": datetime.now(timezone.utc),
            "is_deleted": False,
            "description": "Test sample",
            "main_composition": "Au",
            "geometry": None,
            "environment_ids": [],
            "layers_json": None,
            "layers": [
                {
                    "layer_number": 1,
                    "material": "Au",
                    "thickness": 100.0,
                    "roughness": 5.0,
                    "sld": 4.5,
                }
            ],
            "substrate_json": None,
        }
        
        assembly = AssemblyResult(sample=sample_record)
        result = validator.validate(assembly)
        # Should pass or only have minor warnings
        critical_errors = [i for i in result.issues if i.severity == "error"]
        assert len(critical_errors) == 0


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
                "reflectivity": {
                    "measurement_geometry": None,
                    "reduction_time": None,
                    "reduction_version": None,
                    "reduction_parameters": None,
                    "q": [0.01, 0.02, 0.03],
                    "r": [1.0, 0.8, 0.5],
                    "dr": [0.01, 0.01, 0.02],
                    "dq": [0.001, 0.001, 0.002],
                },
            }
            
            path = writer.write_reflectivity(refl_record)
            
            assert path.exists()
            assert path.suffix == ".parquet"
            
            # Read single file using ParquetFile to avoid dataset discovery
            pf = pq.ParquetFile(str(path))
            table = pf.read()
            # Reflectivity data is now in nested 'reflectivity' struct
            assert "reflectivity" in table.column_names
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
                "reflectivity": {
                    "measurement_geometry": None,
                    "reduction_time": None,
                    "reduction_version": None,
                    "reduction_parameters": None,
                    "q": [0.01, 0.02],
                    "r": [1.0, 0.8],
                    "dr": [0.01, 0.01],
                    "dq": [0.001, 0.001],
                },
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
            
            assert Path(paths["reflectivity"]).exists()
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
                "reflectivity": {
                    "measurement_geometry": None,
                    "reduction_time": None,
                    "reduction_version": None,
                    "reduction_parameters": None,
                    "q": [0.01],
                    "r": [1.0],
                    "dr": [0.01],
                    "dq": [0.001],
                },
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
                    "SampleTemp": type("DASLog", (), {
                        "average_value": 300.0,
                        "min_value": 298.0,
                        "max_value": 302.0,
                    })(),
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
                ]
            )
            
            result = assembler.assemble(
                reduced=reduced_data,
                parquet=parquet_data,
                model=model_data,
            )
            
            # 2. Validate
            validator = DataValidator()
            validation = validator.validate(result)
            assert validation.is_valid, f"Validation failed: {validation.issues}"
            
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
