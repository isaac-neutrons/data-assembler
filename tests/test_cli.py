"""
Tests for the CLI module.
"""

import json
import os
import tempfile
from pathlib import Path

import pytest

from assembler.cli.main import app


class TestCLIDetect:
    """Tests for the detect command."""

    def test_detect_reduced_file(self, tmp_path):
        """Test detecting a reduced file."""
        # Create a mock reduced file
        reduced_file = tmp_path / "REFL_218386_combined_data_auto.txt"
        reduced_file.write_text("""# Experiment IPTS-12345
# Run 218386
# Reduction v1.0
# Q [1/Angstrom] R dR dQ
0.01 1.0 0.1 0.001
0.02 0.9 0.1 0.001
""")

        result = app(["detect", str(reduced_file)])
        assert result == 0

    def test_detect_json_output(self, tmp_path):
        """Test JSON output from detect."""
        reduced_file = tmp_path / "REF_L_123456_test.txt"
        reduced_file.write_text("""# Experiment IPTS-99999
# Q [1/Angstrom] R dR dQ
0.01 1.0 0.1 0.001
""")

        result = app(["detect", "--json", str(reduced_file)])
        assert result == 0

    def test_detect_nonexistent_file(self):
        """Test detecting a file that doesn't exist."""
        result = app(["detect", "/nonexistent/file.txt"])
        assert result == 1


class TestCLIFind:
    """Tests for the find command."""

    def test_find_with_run_number(self, tmp_path):
        """Test finding files by run number."""
        # Create a mock reduced file
        reduced_file = tmp_path / "REFL_218386_combined.txt"
        reduced_file.write_text("test")

        result = app(["find", "--run", "218386", "-s", str(tmp_path)])
        assert result == 0

    def test_find_no_run_number(self):
        """Test find without run number."""
        result = app(["find", "-s", "."])
        assert result == 1  # Should fail without run number


class TestCLIIngest:
    """Tests for the ingest command."""

    def test_ingest_dry_run(self, tmp_path):
        """Test ingest with --dry-run."""
        reduced_file = tmp_path / "REFL_218386_test.txt"
        reduced_file.write_text("""# Experiment IPTS-12345
# Run 218386
# Reduction timestamp: 2025-01-01 12:00:00
# Q [1/Angstrom] R dR dQ
0.01 1.0 0.1 0.001
0.02 0.9 0.1 0.001
""")

        output_dir = tmp_path / "output"
        result = app([
            "ingest",
            "--reduced", str(reduced_file),
            "--output", str(output_dir),
            "--dry-run",
        ])
        assert result == 0
        # With --dry-run, output dir should not be created
        # (or may exist but be empty)

    def test_ingest_writes_output(self, tmp_path):
        """Test ingest writes parquet files."""
        reduced_file = tmp_path / "REFL_218386_test.txt"
        reduced_file.write_text("""# Experiment IPTS-12345
# Run 218386
# Reduction timestamp: 2025-01-01 12:00:00
# Q [1/Angstrom] R dR dQ
0.01 1.0 0.1 0.001
0.02 0.9 0.1 0.001
""")

        output_dir = tmp_path / "output"
        result = app([
            "ingest",
            "--reduced", str(reduced_file),
            "--output", str(output_dir),
        ])
        assert result == 0

        # Check output was created
        assert output_dir.exists()
        # Should have a reflectivity directory
        refl_dirs = list(output_dir.glob("reflectivity/**/*.parquet"))
        assert len(refl_dirs) >= 1

    def test_ingest_writes_manifest_json(self, tmp_path):
        """Test ingest --json writes JSON files to output directory."""
        reduced_file = tmp_path / "REFL_218386_test.txt"
        reduced_file.write_text("""# Experiment IPTS-12345
# Run 218386
# Reduction timestamp: 2025-01-01 12:00:00
# Q [1/Angstrom] R dR dQ
0.01 1.0 0.1 0.001
0.02 0.9 0.1 0.001
""")

        output_dir = tmp_path / "output"
        result = app([
            "ingest",
            "--reduced", str(reduced_file),
            "--output", str(output_dir),
            "--json",
        ])
        assert result == 0

        # Check JSON directory was created
        json_dir = output_dir / "json"
        assert json_dir.exists()

        # Check reflectivity.json was created
        refl_json = json_dir / "reflectivity.json"
        assert refl_json.exists()

        # Verify JSON content matches schema
        import json
        with open(refl_json) as f:
            data = json.load(f)

        # Should have the same fields as the parquet schema
        assert "id" in data
        assert "run_number" in data
        # Reflectivity data is now in nested 'reflectivity' struct
        #assert "reflectivity" in data
        assert "q" in data
        assert "r" in data
        assert int(data["run_number"]) == 218386
        assert len(data["q"]) == 2


class TestCLIHelp:
    """Tests for CLI help."""

    def test_help_returns_zero(self):
        """Test that --help exits cleanly."""
        # Click handles --help via the app() wrapper which returns 0
        result = app(["--help"])
        assert result == 0

    def test_command_help(self):
        """Test command-level help."""
        result = app(["ingest", "--help"])
        assert result == 0

    def test_batch_help(self):
        """Test batch command help."""
        result = app(["batch", "--help"])
        assert result == 0


class TestCLIBatch:
    """Tests for the batch command."""

    @pytest.fixture
    def manifest_yaml(self, tmp_path):
        """Create a minimal valid YAML manifest with real files."""
        import yaml

        reduced1 = tmp_path / "REFL_100001_reduced.txt"
        reduced1.write_text(
            "# Experiment IPTS-12345\n"
            "# Run 100001\n"
            "# Reduction timestamp: 2025-01-01 12:00:00\n"
            "# Q [1/Angstrom] R dR dQ\n"
            "0.01 1.0 0.1 0.001\n"
            "0.02 0.9 0.1 0.001\n"
        )

        reduced2 = tmp_path / "REFL_100002_reduced.txt"
        reduced2.write_text(
            "# Experiment IPTS-12345\n"
            "# Run 100002\n"
            "# Reduction timestamp: 2025-01-02 14:00:00\n"
            "# Q [1/Angstrom] R dR dQ\n"
            "0.01 0.8 0.1 0.001\n"
            "0.02 0.7 0.1 0.001\n"
        )

        # Minimal model JSON so the assembler can create a sample record
        model_file = tmp_path / "model.json"
        model_json = {
            "$schema": "bumps-draft-03",
            "references": {
                "ref1": {
                    "__class__": "bumps.parameter.Parameter",
                    "name": "thickness",
                    "fixed": False,
                    "slot": {"value": 50.0},
                    "bounds": [10.0, 200.0],
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
                            "material": {"name": "air", "rho": 0.0, "irho": 0.0},
                        },
                        {
                            "name": "film",
                            "thickness": {"__class__": "Reference", "id": "ref1"},
                            "interface": 3.0,
                            "material": {"name": "Cu", "rho": 6.0e-6, "irho": 0.0},
                        },
                        {
                            "name": "substrate",
                            "thickness": 0.0,
                            "interface": 2.0,
                            "material": {"name": "Si", "rho": 2.07e-6, "irho": 0.0},
                        },
                    ],
                },
                "probe": {
                    "Q": {"values": [0.01, 0.02]},
                    "R": {"values": [1.0, 0.5]},
                    "dR": {"values": [0.01, 0.01]},
                },
            },
        }
        model_file.write_text(json.dumps(model_json))

        output_dir = tmp_path / "batch_output"

        data = {
            "title": "Test batch",
            "sample": {
                "description": "Test sample",
                "model": str(model_file),
            },
            "output": str(output_dir),
            "measurements": [
                {
                    "name": "Run 1",
                    "reduced": str(reduced1),
                    "environment": "Condition A",
                    "model": str(model_file),
                },
                {
                    "name": "Run 2",
                    "reduced": str(reduced2),
                    "environment": "Condition B",
                    "model": str(model_file),
                },
            ],
        }

        manifest_file = tmp_path / "manifest.yaml"
        manifest_file.write_text(yaml.dump(data, default_flow_style=False))
        return manifest_file, output_dir

    def test_batch_dry_run(self, manifest_yaml):
        """Test batch --dry-run parses and validates without writing."""
        manifest_file, output_dir = manifest_yaml
        result = app(["batch", str(manifest_file), "--dry-run"])
        assert result == 0
        # In dry-run mode, output dir should not contain parquet files
        if output_dir.exists():
            parquet_files = list(output_dir.rglob("*.parquet"))
            assert len(parquet_files) == 0

    def test_batch_writes_output(self, manifest_yaml):
        """Test batch writes parquet files for each measurement."""
        manifest_file, output_dir = manifest_yaml
        result = app(["batch", str(manifest_file)])
        assert result == 0

        # Should have output directory
        assert output_dir.exists()

        # Should have reflectivity parquet for each measurement
        refl_files = list(output_dir.rglob("reflectivity/**/*.parquet"))
        assert len(refl_files) == 2

        # Should have environment parquet for each measurement
        env_files = list(output_dir.glob("environment/*.parquet"))
        assert len(env_files) == 2

        # Should have exactly one sample parquet
        sample_files = list(output_dir.glob("sample/*.parquet"))
        assert len(sample_files) == 1

    def test_batch_json_output(self, manifest_yaml):
        """Test batch --json writes per-run JSON subdirectories."""
        manifest_file, output_dir = manifest_yaml
        result = app(["batch", str(manifest_file), "--json"])
        assert result == 0

        json_dir = output_dir / "json"
        assert json_dir.exists()

        # Per-run subdirectories
        assert (json_dir / "100001").exists()
        assert (json_dir / "100002").exists()

        # Each run should have reflectivity and environment JSON
        for run in ["100001", "100002"]:
            assert (json_dir / run / "reflectivity.json").exists()
            assert (json_dir / run / "environment.json").exists()

        # Top-level sample.json only (not in per-run dirs)
        assert (json_dir / "sample.json").exists()
        assert not (json_dir / "100001" / "sample.json").exists()
        assert not (json_dir / "100002" / "sample.json").exists()

    def test_batch_sample_linking(self, manifest_yaml):
        """Test that all measurements reference the same sample."""
        manifest_file, output_dir = manifest_yaml
        result = app(["batch", str(manifest_file), "--json"])
        assert result == 0

        json_dir = output_dir / "json"

        # Read all records
        with open(json_dir / "sample.json") as f:
            sample = json.load(f)
        with open(json_dir / "100001" / "reflectivity.json") as f:
            refl1 = json.load(f)
        with open(json_dir / "100002" / "reflectivity.json") as f:
            refl2 = json.load(f)
        with open(json_dir / "100001" / "environment.json") as f:
            env1 = json.load(f)
        with open(json_dir / "100002" / "environment.json") as f:
            env2 = json.load(f)

        # All records reference the same sample
        assert refl1["sample_id"] == sample["id"]
        assert refl2["sample_id"] == sample["id"]
        assert env1["sample_id"] == sample["id"]
        assert env2["sample_id"] == sample["id"]

        # Sample has both environment IDs
        assert set(sample["environment_ids"]) == {env1["id"], env2["id"]}

        # Sample description from manifest
        assert sample["description"] == "Test sample"

    def test_batch_nonexistent_manifest(self):
        """Test batch with a nonexistent manifest file."""
        result = app(["batch", "/nonexistent/manifest.yaml"])
        assert result == 1

    def test_batch_invalid_manifest(self, tmp_path):
        """Test batch with a manifest missing required fields."""
        import yaml

        manifest_file = tmp_path / "bad.yaml"
        manifest_file.write_text(yaml.dump({"title": "No output or measurements"}))

        result = app(["batch", str(manifest_file)])
        assert result == 1

