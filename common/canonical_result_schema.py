from __future__ import annotations

from typing import Any, Dict, Optional


ALLOWED_FORMATS = {"json", "tsv", "fasta", "none"}
ALLOWED_STATUS = {"success", "error"}


def _as_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        parsed = int(value)
        return parsed if parsed >= 0 else default
    except Exception:
        return default


def build_canonical_result(
    *,
    source: str,
    mode: str,
    data: Any = None,
    result_format: str = "none",
    count: Any = 0,
    num_found: Any = 0,
    status: str = "success",
    correlation: Optional[Dict[str, Any]] = None,
    pagination: Optional[Dict[str, Any]] = None,
    replay: Optional[Dict[str, Any]] = None,
    error: Optional[Dict[str, Any]] = None,
    internal: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    normalized_format = result_format if result_format in ALLOWED_FORMATS else "none"
    normalized_status = status if status in ALLOWED_STATUS else "error"
    correlation_obj = _as_dict(correlation)
    pagination_obj = _as_dict(pagination)
    replay_obj = replay if isinstance(replay, dict) else None
    error_obj = error if isinstance(error, dict) else None

    result = {
        "source": str(source or ""),
        "mode": str(mode or "default"),
        "data": data,
        "format": normalized_format,
        "count": _safe_int(count),
        "numFound": _safe_int(num_found),
        "status": normalized_status,
        "correlation": {
            "requestId": correlation_obj.get("requestId"),
            "requestCorrelationId": correlation_obj.get("requestCorrelationId"),
            "pageCorrelationId": correlation_obj.get("pageCorrelationId"),
            "batchNumber": correlation_obj.get("batchNumber"),
        },
        "pagination": {
            "nextCursorId": pagination_obj.get("nextCursorId"),
            "hasMore": bool(pagination_obj.get("hasMore", False)),
            "totalBatches": _safe_int(pagination_obj.get("totalBatches"), default=0),
        },
        "replay": replay_obj,
        "error": error_obj,
    }
    if internal is not None:
        result["_internal"] = _as_dict(internal)
    return result


def validate_canonical_result(result: Dict[str, Any]) -> None:
    if not isinstance(result, dict):
        raise ValueError("Canonical result must be a dict")
    required_keys = {
        "source", "mode", "data", "format", "count", "numFound",
        "status", "correlation", "pagination", "replay", "error"
    }
    missing = [key for key in required_keys if key not in result]
    if missing:
        raise ValueError(f"Canonical result missing keys: {missing}")
    if result["format"] not in ALLOWED_FORMATS:
        raise ValueError(f"Invalid format: {result['format']}")
    if result["status"] not in ALLOWED_STATUS:
        raise ValueError(f"Invalid status: {result['status']}")
    if not isinstance(result["correlation"], dict):
        raise ValueError("correlation must be an object")
    if not isinstance(result["pagination"], dict):
        raise ValueError("pagination must be an object")
    if result["replay"] is not None and not isinstance(result["replay"], dict):
        raise ValueError("replay must be object|null")
    if result["error"] is not None and not isinstance(result["error"], dict):
        raise ValueError("error must be object|null")
