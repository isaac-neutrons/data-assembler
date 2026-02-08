"""
Tests for file parsers.
"""

import pytest
import json
import tempfile
from pathlib import Path

from assembler.parsers import (
    ReducedParser,
    ModelParser,
    ManifestParser,
)
from assembler.parsers.reduced_parser import ReducedData, extract_run_number_from_filename
from assembler.parsers.model_parser import ModelData, ModelLayer
from assembler.parsers.manifest_parser import Manifest, ManifestSample, ManifestMeasurement


class TestReducedParser:
    """Tests for the reduced text file parser."""
    
    @pytest.fixture
    def sample_reduced_content(self):
        """Sample reduced data file content."""
        return """# Data file for Q1 Q2 Q3 Q4 R1 R2 R3 R4 dR1 dR2 dR3 dR4 dQ1 dQ2 dQ3 dQ4
# combined data set from reflectivity reduction
# Datafile created by QuickNXS 2.0.38
# Date: 2024-01-15 10:30:45
# Type: Specular
# Input file indices: 218386,218387,218388
# Extracted states: ++
# Scaling factors: 1.0
# IPTS: IPTS-12345
# Sequence number: 1
# Direct beam runs: 218300,218301
# ======================================================
0.0100   1.00000   0.01000   0.00050
0.0200   0.50000   0.00500   0.00100
0.0300   0.25000   0.00250   0.00150
0.0400   0.12500   0.00125   0.00200
"""
    
    def test_parse_content(self, sample_reduced_content):
        """Test parsing reduced file content."""
        parser = ReducedParser()
        data = parser.parse_content(sample_reduced_content, "test.txt")
        
        assert len(data.q) == 4
        assert len(data.r) == 4
        assert data.q[0] == pytest.approx(0.01)
        assert data.r[0] == pytest.approx(1.0)
        # Note: The parser extracts experiment_id from specific header patterns
        # For this test content, it may be None if the pattern doesn't match
    
    def test_parse_file(self, sample_reduced_content, tmp_path):
        """Test parsing from a file."""
        file_path = tmp_path / "REFL_218386_test.txt"
        file_path.write_text(sample_reduced_content)
        
        parser = ReducedParser()
        data = parser.parse(str(file_path))
        
        assert len(data.q) == 4
        assert data.file_path == str(file_path)
    
    def test_extract_run_number(self):
        """Test run number extraction from filename."""
        assert extract_run_number_from_filename("REFL_218386_combined_data_auto.txt") == 218386
        assert extract_run_number_from_filename("REF_L_123456.txt") == 123456
        assert extract_run_number_from_filename("no_number.txt") is None


class TestModelParser:
    """Tests for the model JSON parser."""
    
    @pytest.fixture
    def sample_model_json(self):
        """Sample model JSON data."""
        return {
            "$schema": "bumps-draft-03",
            "references": {
                "ref1": {
                    "__class__": "bumps.parameter.Parameter",
                    "name": "layer1 thickness",
                    "fixed": False,
                    "slot": {"value": 100.0},
                    "bounds": [50.0, 200.0],
                },
                "ref2": {
                    "__class__": "bumps.parameter.Parameter",
                    "name": "layer1 rho",
                    "fixed": True,
                    "slot": {"value": 6.0e-6},
                },
                "ref3": {
                    "__class__": "bumps.parameter.Parameter",
                    "name": "layer1 interface",
                    "fixed": False,
                    "slot": {"value": 5.0},
                },
            },
            "object": {
                "name": "test_model",
                "sample": {
                    "layers": [
                        {
                            "name": "air",
                            "thickness": 0.0,
                            "interface": 0.0,
                            "material": {
                                "name": "air",
                                "rho": 0.0,
                                "irho": 0.0,
                            },
                        },
                        {
                            "name": "film",
                            "thickness": {"__class__": "Reference", "id": "ref1"},
                            "interface": {"__class__": "Reference", "id": "ref3"},
                            "material": {
                                "name": "Cu",
                                "rho": {"__class__": "Reference", "id": "ref2"},
                                "irho": 0.0,
                            },
                        },
                        {
                            "name": "substrate",
                            "thickness": 0.0,
                            "interface": 3.0,
                            "material": {
                                "name": "Si",
                                "rho": 2.07e-6,
                                "irho": 0.0,
                            },
                        },
                    ],
                },
                "probe": {
                    "Q": {"values": [0.01, 0.02, 0.03]},
                    "R": {"values": [1.0, 0.5, 0.25]},
                    "dR": {"values": [0.01, 0.01, 0.01]},
                },
            },
        }
    
    def test_parse_dict(self, sample_model_json):
        """Test parsing model from dictionary."""
        parser = ModelParser()
        data = parser.parse_dict(sample_model_json)
        
        assert len(data.layers) == 3
        assert data.layers[0].name == "air"
        assert data.layers[1].name == "film"
        assert data.layers[2].name == "substrate"
    
    def test_reference_resolution(self, sample_model_json):
        """Test that references are resolved correctly."""
        parser = ModelParser()
        data = parser.parse_dict(sample_model_json)
        
        film = data.layers[1]
        assert film.thickness == pytest.approx(100.0)
        assert film.interface == pytest.approx(5.0)
        assert film.material.rho == pytest.approx(6.0e-6)
    
    def test_parse_file(self, sample_model_json, tmp_path):
        """Test parsing from file."""
        file_path = tmp_path / "model.json"
        file_path.write_text(json.dumps(sample_model_json))
        
        parser = ModelParser()
        data = parser.parse(str(file_path))
        
        assert len(data.layers) == 3
        assert data.file_path == str(file_path)
    
    def test_layer_properties(self, sample_model_json):
        """Test layer access properties."""
        parser = ModelParser()
        data = parser.parse_dict(sample_model_json)
        
        assert data.substrate.name == "substrate"
        assert data.ambient.name == "air"
        assert len(data.film_layers) == 1
        assert data.total_thickness == pytest.approx(100.0)

    def test_std_none_without_error_data(self, sample_model_json):
        """Test that std fields are None when no error data is provided."""
        parser = ModelParser()
        data = parser.parse_dict(sample_model_json)

        film = data.layers[1]
        # Free parameters but no error source → None
        assert film.thickness_std is None
        assert film.interface_std is None
        assert film.material.rho_std is None  # fixed, so also None
        assert film.material.irho_std is None  # literal value

    def test_std_inline_on_parameter(self, sample_model_json):
        """Test resolving std directly from a Parameter entry (inline)."""
        # Add std to the free parameter entries
        sample_model_json["references"]["ref1"]["std"] = 3.5
        sample_model_json["references"]["ref3"]["std"] = 0.8

        parser = ModelParser()
        data = parser.parse_dict(sample_model_json)

        film = data.layers[1]
        assert film.thickness_std == pytest.approx(3.5)
        assert film.interface_std == pytest.approx(0.8)
        # ref2 (rho) is fixed → no std even if we added one
        assert film.material.rho_std is None

    def test_std_from_error_data(self, sample_model_json):
        """Test resolving std from companion error_data dict."""
        error_data = {
            "layer1 thickness": {"std": 4.2, "mean": 100.1},
            "layer1 interface": {"std": 1.1, "mean": 5.1},
        }

        parser = ModelParser()
        data = parser.parse_dict(sample_model_json, error_data=error_data)

        film = data.layers[1]
        assert film.thickness_std == pytest.approx(4.2)
        assert film.interface_std == pytest.approx(1.1)
        # rho (ref2) is fixed → None
        assert film.material.rho_std is None

    def test_std_inline_takes_precedence(self, sample_model_json):
        """Test that inline std takes precedence over companion error_data."""
        sample_model_json["references"]["ref1"]["std"] = 2.0
        error_data = {
            "layer1 thickness": {"std": 9.9},
        }

        parser = ModelParser()
        data = parser.parse_dict(sample_model_json, error_data=error_data)

        film = data.layers[1]
        # Inline wins
        assert film.thickness_std == pytest.approx(2.0)

    def test_std_fixed_param_ignored(self, sample_model_json):
        """Test that fixed parameters get None std even with error data."""
        error_data = {
            "layer1 rho": {"std": 0.001},
        }

        parser = ModelParser()
        data = parser.parse_dict(sample_model_json, error_data=error_data)

        film = data.layers[1]
        # ref2 is fixed → always None
        assert film.material.rho_std is None

    def test_companion_err_json_autoloaded(self, sample_model_json, tmp_path):
        """Test that parse() auto-loads a companion -err.json file."""
        # Write the model file
        model_path = tmp_path / "mymodel.json"
        model_path.write_text(json.dumps(sample_model_json))

        # Write a companion error file
        err_data = {
            "layer1 thickness": {"std": 5.5, "mean": 100.0},
            "layer1 interface": {"std": 0.9, "mean": 5.0},
        }
        err_path = tmp_path / "mymodel-err.json"
        err_path.write_text(json.dumps(err_data))

        parser = ModelParser()
        data = parser.parse(str(model_path))

        film = data.layers[1]
        assert film.thickness_std == pytest.approx(5.5)
        assert film.interface_std == pytest.approx(0.9)
        # Literal / fixed values still None
        assert data.layers[0].thickness_std is None
        assert data.layers[2].interface_std is None


class TestParquetParser:
    """Tests for the parquet parser."""
    
    def test_import(self):
        """Test that ParquetParser can be imported."""
        from assembler.parsers import ParquetParser
        from assembler.parsers.parquet_parser import ParquetData
        assert ParquetParser is not None
        assert ParquetData is not None


class TestManifestParser:
    """Tests for the YAML manifest parser."""

    @pytest.fixture
    def minimal_manifest_data(self):
        """Minimal valid manifest data as a dict."""
        return {
            "output": "/tmp/test_output",
            "measurements": [
                {
                    "name": "First measurement",
                    "reduced": "/some/path/REFL_218386_reduced.txt",
                },
            ],
        }

    @pytest.fixture
    def full_manifest_data(self, tmp_path):
        """Full manifest data with all fields populated."""
        # Create real files so validation passes
        reduced1 = tmp_path / "REFL_218386_reduced.txt"
        reduced1.write_text("0.01 1.0 0.1 0.001\n")
        reduced2 = tmp_path / "REFL_218393_reduced.txt"
        reduced2.write_text("0.01 1.0 0.1 0.001\n")
        model = tmp_path / "model.json"
        model.write_text("{}")
        parquet_dir = tmp_path / "parquet"
        parquet_dir.mkdir()

        return {
            "title": "IPTS-34347 Cu/THF experiment",
            "sample": {
                "description": "Cu in THF on Si",
                "model": str(model),
                "model_dataset_index": 1,
            },
            "output": str(tmp_path / "output"),
            "measurements": [
                {
                    "name": "Steady-state OCV",
                    "reduced": str(reduced1),
                    "parquet": str(parquet_dir),
                    "model_dataset_index": 1,
                    "environment": "Cell, THF, steady-state OCV",
                },
                {
                    "name": "Final OCV",
                    "reduced": str(reduced2),
                    "model_dataset_index": 2,
                    "environment": "Cell, THF, final OCV",
                },
            ],
        }

    def test_parse_dict_minimal(self, minimal_manifest_data):
        """Test parsing a minimal manifest dict."""
        parser = ManifestParser()
        manifest = parser.parse_dict(minimal_manifest_data)

        assert manifest.output == "/tmp/test_output"
        assert len(manifest.measurements) == 1
        assert manifest.measurements[0].name == "First measurement"
        assert manifest.title is None

    def test_parse_dict_full(self, full_manifest_data):
        """Test parsing a fully populated manifest dict."""
        parser = ManifestParser()
        manifest = parser.parse_dict(full_manifest_data)

        assert manifest.title == "IPTS-34347 Cu/THF experiment"
        assert manifest.sample.description == "Cu in THF on Si"
        assert manifest.sample.model_dataset_index == 1
        assert len(manifest.measurements) == 2

        m1, m2 = manifest.measurements
        assert m1.name == "Steady-state OCV"
        assert m1.model_dataset_index == 1
        assert m1.environment == "Cell, THF, steady-state OCV"
        assert m1.parquet is not None
        assert m2.name == "Final OCV"
        assert m2.model_dataset_index == 2
        assert m2.parquet is None

    def test_parse_yaml_file(self, full_manifest_data, tmp_path):
        """Test parsing from a YAML file on disk."""
        import yaml

        manifest_file = tmp_path / "manifest.yaml"
        manifest_file.write_text(yaml.dump(full_manifest_data, default_flow_style=False))

        parser = ManifestParser()
        manifest = parser.parse(str(manifest_file))

        assert manifest.title == "IPTS-34347 Cu/THF experiment"
        assert len(manifest.measurements) == 2

    def test_parse_nonexistent_file(self):
        """Test parsing a file that does not exist."""
        parser = ManifestParser()
        with pytest.raises(FileNotFoundError):
            parser.parse("/nonexistent/manifest.yaml")

    def test_parse_non_mapping(self, tmp_path):
        """Test parsing a YAML file that isn't a mapping."""
        manifest_file = tmp_path / "bad.yaml"
        manifest_file.write_text("- just a list\n- of items\n")

        parser = ManifestParser()
        with pytest.raises(ValueError, match="YAML mapping"):
            parser.parse(str(manifest_file))

    def test_parse_measurements_not_list(self):
        """Test parsing when measurements is not a list."""
        parser = ManifestParser()
        with pytest.raises(ValueError, match="must be a list"):
            parser.parse_dict({
                "output": "/tmp/out",
                "measurements": "not-a-list",
            })

    def test_parse_measurement_not_mapping(self):
        """Test parsing when a measurement entry is not a mapping."""
        parser = ManifestParser()
        with pytest.raises(ValueError, match="must be a mapping"):
            parser.parse_dict({
                "output": "/tmp/out",
                "measurements": ["just a string"],
            })

    def test_measurement_default_name(self):
        """Test that measurements get default names when not provided."""
        parser = ManifestParser()
        manifest = parser.parse_dict({
            "output": "/tmp/out",
            "measurements": [
                {"reduced": "/some/file.txt"},
                {"reduced": "/some/other.txt"},
            ],
        })
        assert manifest.measurements[0].name == "Measurement 1"
        assert manifest.measurements[1].name == "Measurement 2"

    def test_validate_no_output(self):
        """Test validation: missing output."""
        manifest = Manifest(
            measurements=[ManifestMeasurement(name="m1", reduced="/some/file.txt")],
        )
        errors = manifest.validate(check_files=False)
        assert any("output" in e.lower() for e in errors)

    def test_validate_no_measurements(self):
        """Test validation: no measurements."""
        manifest = Manifest(output="/tmp/out")
        errors = manifest.validate(check_files=False)
        assert any("measurement" in e.lower() for e in errors)

    def test_validate_missing_reduced(self):
        """Test validation: measurement without reduced file."""
        manifest = Manifest(
            output="/tmp/out",
            measurements=[ManifestMeasurement(name="m1", reduced="")],
        )
        errors = manifest.validate(check_files=True)
        assert any("reduced" in e.lower() for e in errors)

    def test_validate_nonexistent_file(self):
        """Test validation: reduced file doesn't exist."""
        manifest = Manifest(
            output="/tmp/out",
            measurements=[ManifestMeasurement(
                name="m1",
                reduced="/nonexistent/file.txt",
            )],
        )
        errors = manifest.validate(check_files=True)
        assert any("not found" in e.lower() for e in errors)

    def test_validate_bad_dataset_index(self, tmp_path):
        """Test validation: model_dataset_index < 1."""
        reduced = tmp_path / "reduced.txt"
        reduced.write_text("0.01 1.0 0.1 0.001\n")

        manifest = Manifest(
            output="/tmp/out",
            measurements=[ManifestMeasurement(
                name="m1",
                reduced=str(reduced),
                model_dataset_index=0,
            )],
        )
        errors = manifest.validate(check_files=True)
        assert any("model_dataset_index" in e for e in errors)

    def test_validate_sample_bad_dataset_index(self, tmp_path):
        """Test validation: sample model_dataset_index < 1."""
        reduced = tmp_path / "reduced.txt"
        reduced.write_text("0.01 1.0 0.1 0.001\n")

        manifest = Manifest(
            output="/tmp/out",
            sample=ManifestSample(model_dataset_index=0),
            measurements=[ManifestMeasurement(name="m1", reduced=str(reduced))],
        )
        errors = manifest.validate(check_files=True)
        assert any("model_dataset_index" in e for e in errors)

    def test_validate_success(self, tmp_path):
        """Test validation: everything valid."""
        reduced = tmp_path / "REFL_218386.txt"
        reduced.write_text("0.01 1.0 0.1 0.001\n")

        manifest = Manifest(
            output=str(tmp_path / "output"),
            measurements=[ManifestMeasurement(
                name="m1",
                reduced=str(reduced),
                environment="test env",
            )],
        )
        errors = manifest.validate(check_files=True)
        assert errors == []

    def test_sample_defaults_empty(self):
        """Test that an empty sample section yields defaults."""
        parser = ManifestParser()
        manifest = parser.parse_dict({
            "output": "/tmp/out",
            "measurements": [{"reduced": "/f.txt"}],
        })
        assert manifest.sample.description is None
        assert manifest.sample.model is None
        assert manifest.sample.model_dataset_index is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
