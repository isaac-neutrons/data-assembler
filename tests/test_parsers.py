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
)
from assembler.parsers.reduced_parser import ReducedData, extract_run_number_from_filename
from assembler.parsers.model_parser import ModelData, ModelLayer


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
        
        assert data.schema_version == "bumps-draft-03"
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
    
    def test_probe_data(self, sample_model_json):
        """Test probe data extraction."""
        parser = ModelParser()
        data = parser.parse_dict(sample_model_json)
        
        assert len(data.q) == 3
        assert len(data.r) == 3
        assert data.q[0] == pytest.approx(0.01)


class TestParquetParser:
    """Tests for the parquet parser."""
    
    def test_import(self):
        """Test that ParquetParser can be imported."""
        from assembler.parsers import ParquetParser
        from assembler.parsers.parquet_parser import ParquetData
        assert ParquetParser is not None
        assert ParquetData is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
