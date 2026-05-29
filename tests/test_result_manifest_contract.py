"""Guard the shared ``ndip-tool-result/1`` wire contract.

``result_manifest.py`` is vendored byte-identically across analyzer/nr-analyzer,
data-assembler, and nr-isaac-format. This test pins the contract the orchestrator
(ndip-workflows) relies on, so an accidental edit can't silently change the shape
or status vocabulary the foreign tools emit.
"""

from assembler import result_manifest as rm


def test_schema_constant():
    assert rm.SCHEMA == "ndip-tool-result/1"


def test_valid_status_vocabulary():
    assert rm.VALID_STATUS == {"ok", "failed", "skipped", "dry-run", "needs-reprocessing"}


def test_build_manifest_shape_and_none_dropping():
    m = rm.build_manifest(
        "data-assembler",
        "ok",
        params={"reduced_input": "a.txt", "model_input": None},
        artifacts={"ingest_dir": "/out", "missing": None},
        info={"ingest_status": "completed"},
        exit_code=0,
    )
    assert set(m) >= {
        "tool", "tool_version", "schema", "status", "exit_code",
        "params", "artifacts", "info",
    }
    assert m["tool"] == "data-assembler" and m["schema"] == "ndip-tool-result/1"
    assert isinstance(m["tool_version"], str)
    # None values are dropped from params/artifacts
    assert m["params"] == {"reduced_input": "a.txt"}
    assert m["artifacts"] == {"ingest_dir": "/out"}
    # messages only present when supplied
    assert "messages" not in m


def test_messages_included_when_supplied():
    m = rm.build_manifest("data-assembler", "failed",
                          messages=[{"level": "error", "text": "boom"}])
    assert m["messages"] == [{"level": "error", "text": "boom"}]
