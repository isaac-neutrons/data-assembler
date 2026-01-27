"""
Integration tests using real data files.

These tests use actual parquet files from run 218386.
Skip if files are not available.
"""

import os
from pathlib import Path

import pytest

from assembler.parsers import ParquetParser, ReducedParser
from assembler.tools import FileType
from assembler.tools.detection import detect_file_type, extract_run_number
from assembler.workflow import DataAssembler
from assembler.writers import ParquetWriter


# Paths to real test data
PARQUET_DIR = Path(os.path.expanduser("~/data/isaac/expt11/parquet"))
REDUCED_FILE = Path(os.path.expanduser("~/data/REFL_218386_combined_data_auto.txt"))
MODEL_FILE = Path(os.path.expanduser("~/data/expt11-refl1d/Cu-THF-corefine-expt11-1-expt.json"))


@pytest.fixture
def real_parquet_dir():
    """Skip test if real parquet directory is not available."""
    if not PARQUET_DIR.exists():
        pytest.skip(f"Real parquet data not available at {PARQUET_DIR}")
    return PARQUET_DIR


@pytest.fixture
def real_reduced_file():
    """Skip test if real reduced file is not available."""
    if not REDUCED_FILE.exists():
        pytest.skip(f"Real reduced data not available at {REDUCED_FILE}")
    return REDUCED_FILE


class TestRealParquetData:
    """Tests using real parquet files from run 218386."""

    def test_parquet_parser_reads_metadata(self, real_parquet_dir):
        """Test that ParquetParser can read real metadata file."""
        parser = ParquetParser()
        data = parser.parse_directory(real_parquet_dir, run_number=218386)

        assert data.metadata is not None
        assert data.metadata.run_number == 218386
        assert data.metadata.instrument_id == "REF_L"

    def test_parquet_parser_reads_sample(self, real_parquet_dir):
        """Test that ParquetParser can read sample info."""
        parser = ParquetParser()
        data = parser.parse_directory(real_parquet_dir, run_number=218386)

        assert data.sample is not None
        # Sample info should be present
        assert data.sample.run_number == 218386

    def test_parquet_parser_reads_daslogs(self, real_parquet_dir):
        """Test that ParquetParser can read DAS logs."""
        parser = ParquetParser()
        data = parser.parse_directory(real_parquet_dir, run_number=218386)

        assert data.daslogs is not None
        # Should have temperature logs
        temp_logs = [k for k in data.daslogs.keys() if "Temp" in k or "temp" in k]
        assert len(temp_logs) > 0, "Expected temperature-related DAS logs"


class TestRealReducedData:
    """Tests using real reduced data file."""

    def test_detect_file_type(self, real_reduced_file):
        """Test file type detection on real reduced file."""
        file_type = detect_file_type(real_reduced_file)
        assert file_type == FileType.REDUCED

    def test_extract_run_number(self, real_reduced_file):
        """Test run number extraction from real filename."""
        run_number = extract_run_number(real_reduced_file)
        assert run_number == 218386

    def test_reduced_parser_reads_data(self, real_reduced_file):
        """Test that ReducedParser can read real reduced file."""
        parser = ReducedParser()
        data = parser.parse(real_reduced_file)

        assert data is not None
        assert len(data.q) > 0
        assert len(data.r) == len(data.q)
        assert len(data.dr) == len(data.q)

        # Check Q range is reasonable for reflectivity
        assert min(data.q) > 0, "Q should be positive"
        assert max(data.q) < 1.0, "Q should be < 1 Å⁻¹ for typical reflectivity"


class TestFullWorkflowWithRealData:
    """Full workflow tests with real data."""

    def test_assemble_from_real_parquet(self, real_parquet_dir, real_reduced_file):
        """Test assembling data from real parquet and reduced files."""
        # Parse real data
        parquet_parser = ParquetParser()
        parquet_data = parquet_parser.parse_directory(real_parquet_dir, run_number=218386)

        reduced_parser = ReducedParser()
        reduced_data = reduced_parser.parse(real_reduced_file)

        # Assemble
        assembler = DataAssembler()
        result = assembler.assemble(
            reduced=reduced_data,
            parquet=parquet_data,
        )

        # Check result
        assert result.reflectivity is not None
        assert result.reflectivity["run_number"] == "218386"
        refl_data = result.reflectivity.get("reflectivity", {})
        assert len(refl_data.get("q", [])) > 0

        # Check no assembly errors
        assert not result.has_errors, f"Assembly errors: {result.errors}"

    def test_write_real_data_to_parquet(self, real_parquet_dir, real_reduced_file, tmp_path):
        """Test writing assembled real data to parquet."""
        import pyarrow.parquet as pq

        # Parse real data
        parquet_parser = ParquetParser()
        parquet_data = parquet_parser.parse_directory(real_parquet_dir, run_number=218386)

        reduced_parser = ReducedParser()
        reduced_data = reduced_parser.parse(real_reduced_file)

        # Assemble
        assembler = DataAssembler()
        result = assembler.assemble(
            reduced=reduced_data,
            parquet=parquet_data,
        )

        # Write to parquet
        writer = ParquetWriter(tmp_path)
        paths = writer.write(result)

        # Verify files were created
        assert "reflectivity" in paths
        refl_path = Path(paths["reflectivity"])
        assert refl_path.exists()

        # Read back and verify
        pf = pq.ParquetFile(str(refl_path))
        table = pf.read()
        assert table.num_rows == 1
        assert table.column("run_number")[0].as_py() == "218386"

        # Check Q data was preserved
        q_data = table.column("q")[0].as_py()
        assert len(q_data) > 100, "Expected many Q points from real data"
