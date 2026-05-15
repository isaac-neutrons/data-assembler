"""Sanity + drift checks for the vendored v1 workflow-state module.

``src/assembler/state.py`` is a verbatim copy of
``src/ndip_state/state.py`` from the ``ndip-workflows`` repo. The two
files must stay in sync — Galaxy tool XMLs there inline the same source.
This test enforces the match when both repos are checked out side-by-side.
"""

from __future__ import annotations

import pathlib

import pytest


ROOT = pathlib.Path(__file__).resolve().parents[1]
VENDORED = ROOT / "src" / "assembler" / "state.py"
UPSTREAM_CANDIDATES = [
    ROOT.parent / "ndip-workflows" / "src" / "ndip_state" / "state.py",
]


def _find_upstream() -> pathlib.Path | None:
    for p in UPSTREAM_CANDIDATES:
        if p.is_file():
            return p
    return None


def test_vendored_state_importable():
    from assembler.state import (
        SCHEMA_VERSION,
        empty_state,
        load_state,
        save_state,
        update_stage,
    )
    assert SCHEMA_VERSION == "1"
    s = empty_state()
    assert s["schema_version"] == "1"
    assert s["assembly"] == {"success": None, "metadata": {}}


def test_vendored_state_roundtrip(tmp_path):
    from assembler.state import empty_state, load_state, save_state, update_stage

    s = empty_state()
    s["paths"]["event_file"] = "/a.h5"
    update_stage(s, "assembly", success=True, isaac_record="/r.json")
    p = tmp_path / "state.json"
    save_state(s, str(p))
    s2 = load_state(str(p))
    assert s2["paths"]["event_file"] == "/a.h5"
    assert s2["assembly"]["success"] is True
    assert s2["assembly"]["isaac_record"] == "/r.json"


def test_vendored_build_state_from_flat():
    """build_state translates a flat operator-facing dict to nested v1."""
    from assembler.state import build_state

    s = build_state({
        "raw_data": "/raw.h5",
        "output_directory": "/out",
        "llm_model": "gpt-4",
    })
    assert s["schema_version"] == "1"
    assert s["paths"]["raw_data"] == "/raw.h5"
    assert s["paths"]["output_directory"] == "/out"
    assert s["llm"]["model"] == "gpt-4"


def test_no_drift_against_ndip_workflows():
    """When ndip-workflows is checked out as a sibling, the two copies match."""
    upstream = _find_upstream()
    if upstream is None:
        pytest.skip("ndip-workflows sibling repo not found; cannot check drift")
    assert VENDORED.read_text().rstrip() == upstream.read_text().rstrip(), (
        f"{VENDORED} has drifted from {upstream} — re-sync the vendored copy."
    )
