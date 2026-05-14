"""Tests for ``data-assembler ingest``'s --state-in / --state-out flags."""

from __future__ import annotations

import json

import pytest

from assembler.cli.main import app
from assembler.state import empty_state, load_state, save_state, update_stage


_REDUCED_HEADER = (
    "# Experiment IPTS-12345\n"
    "# Run 218386\n"
    "# Reduction timestamp: 2025-01-01 12:00:00\n"
    "# Q [1/Angstrom] R dR dQ\n"
    "0.01 1.0 0.1 0.001\n"
    "0.02 0.9 0.1 0.001\n"
)


def _make_reduced(tmp_path):
    p = tmp_path / "REFL_218386_test.txt"
    p.write_text(_REDUCED_HEADER)
    return p


def test_no_reduced_no_state_in_errors(tmp_path):
    """Without --reduced or --state-in, ingest errors cleanly."""
    rc = app(["ingest", "--output", str(tmp_path / "out")])
    assert rc != 0


def test_state_in_supplies_reduced_and_output(tmp_path):
    reduced = _make_reduced(tmp_path)
    out_root = tmp_path / "shared"

    wstate = empty_state()
    update_stage(wstate, "reduction", result_file=str(reduced))
    wstate["paths"]["output_directory"] = str(out_root)
    state_path = tmp_path / "state.json"
    save_state(wstate, str(state_path))

    rc = app(["ingest", "--state-in", str(state_path)])
    assert rc == 0
    # state-in resolved --output to paths.output_directory + "/assembled"
    assembled = out_root / "assembled"
    assert assembled.exists()
    assert list(assembled.glob("reflectivity/**/*.parquet"))


def test_state_in_missing_reduced_errors(tmp_path):
    """state-in present but reduction.result_file empty -> UsageError."""
    wstate = empty_state()
    wstate["paths"]["output_directory"] = str(tmp_path / "out")
    state_path = tmp_path / "state.json"
    save_state(wstate, str(state_path))

    rc = app(["ingest", "--state-in", str(state_path)])
    assert rc != 0


def test_cli_overrides_state_in(tmp_path):
    """Explicit --reduced wins over state.reduction.result_file."""
    cli_reduced = _make_reduced(tmp_path)
    state_only_reduced = tmp_path / "WILL_NOT_BE_USED.txt"
    state_only_reduced.write_text(_REDUCED_HEADER)

    wstate = empty_state()
    update_stage(wstate, "reduction", result_file=str(state_only_reduced))
    wstate["paths"]["output_directory"] = str(tmp_path / "out")
    state_path = tmp_path / "state.json"
    save_state(wstate, str(state_path))

    rc = app(
        [
            "ingest",
            "--state-in", str(state_path),
            "--reduced", str(cli_reduced),
        ]
    )
    assert rc == 0


def test_state_out_records_ingest_metadata(tmp_path):
    """--state-out writes assembly.metadata.{ingest_dir,parquet_files,ingest_status}."""
    reduced = _make_reduced(tmp_path)
    out_dir = tmp_path / "out"
    state_out = tmp_path / "out_state.json"

    rc = app(
        [
            "ingest",
            "--reduced", str(reduced),
            "--output", str(out_dir),
            "--state-out", str(state_out),
        ]
    )
    assert rc == 0
    s = load_state(str(state_out))
    assert s["schema_version"] == "1"
    meta = s["assembly"]["metadata"]
    assert meta["ingest_status"] == "completed"
    assert meta["ingest_dir"].endswith("/out")
    assert "reflectivity" in meta["parquet_files"]
    assert s["paths"]["assembled_directory"].endswith("/out")


def test_state_in_propagates_to_state_out(tmp_path):
    """Unrelated state fields (run, paths.event_file) flow through."""
    reduced = _make_reduced(tmp_path)
    wstate = empty_state()
    wstate["run"] = 218386
    wstate["paths"]["event_file"] = "/SNS/foo.h5"
    update_stage(wstate, "reduction", result_file=str(reduced))
    wstate["paths"]["output_directory"] = str(tmp_path / "out_root")
    state_path = tmp_path / "in_state.json"
    save_state(wstate, str(state_path))

    state_out = tmp_path / "out_state.json"
    rc = app(["ingest", "--state-in", str(state_path), "--state-out", str(state_out)])
    assert rc == 0
    s = load_state(str(state_out))
    assert s["run"] == 218386
    assert s["paths"]["event_file"] == "/SNS/foo.h5"
    assert s["reduction"]["result_file"] == str(reduced)
