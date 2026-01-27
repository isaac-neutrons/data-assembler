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
        assert "reflectivity" in data
        assert "q" in data["reflectivity"]
        assert "r" in data["reflectivity"]
        assert int(data["run_number"]) == 218386
        assert len(data["reflectivity"]["q"]) == 2


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

