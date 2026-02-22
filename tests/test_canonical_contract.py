import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from common.canonical_result_schema import build_canonical_result, validate_canonical_result


def _fixtures_dir() -> Path:
    return Path(__file__).resolve().parent / "fixtures"


def test_schema_contract():
    payload = build_canonical_result(
        source="bvbrc-mcp-data",
        mode="global",
        data=[{"id": "x"}],
        result_format="json",
        count=1,
        num_found=1,
        status="success",
        correlation={
            "requestId": "req-1",
            "requestCorrelationId": "corr-1",
            "pageCorrelationId": "corr-1:end",
            "batchNumber": 1,
        },
        pagination={"nextCursorId": None, "hasMore": False, "totalBatches": 1},
    )
    validate_canonical_result(payload)


def test_error_shape():
    payload = build_canonical_result(
        source="bvbrc-mcp-data",
        mode="global",
        data=None,
        result_format="none",
        count=0,
        num_found=0,
        status="error",
        error={"code": "INVALID_PARAMETERS", "message": "bad request", "details": {}},
    )
    validate_canonical_result(payload)
    assert payload["status"] == "error"
    assert isinstance(payload["error"], dict)


def test_golden_fixtures_validate():
    for fixture in _fixtures_dir().glob("*.json"):
        payload = json.loads(fixture.read_text(encoding="utf-8"))
        validate_canonical_result(payload)


if __name__ == "__main__":
    test_schema_contract()
    test_error_shape()
    test_golden_fixtures_validate()
    print("canonical contract tests passed")
