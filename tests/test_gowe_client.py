"""
Unit tests for GoWeClient.

All tests are offline -- HTTP responses are mocked via httpx's MockTransport.
No live GoWe server is required.
"""

import pytest
import httpx
import json
from unittest.mock import AsyncMock, patch

import sys
from pathlib import Path

# Ensure the mcp_server package is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common.gowe_client import GoWeClient, GoWeError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _envelope(data=None, status="ok", error=None, pagination=None):
    """Build a GoWe response envelope."""
    return {
        "status": status,
        "request_id": "req_test123",
        "timestamp": "2026-06-10T00:00:00Z",
        "data": data,
        "pagination": pagination,
        "error": error,
    }


def _json_response(body, status_code=200):
    """Build an httpx.Response with JSON body."""
    return httpx.Response(
        status_code=status_code,
        json=body,
        request=httpx.Request("GET", "https://test"),
    )


def _make_transport(handler):
    """
    Create an httpx.MockTransport from an async handler.

    handler(request) -> httpx.Response
    """
    return httpx.MockTransport(handler)


# ---------------------------------------------------------------------------
# Constructor tests
# ---------------------------------------------------------------------------

class TestConstructor:
    def test_base_url_normalization_plain(self):
        client = GoWeClient("https://gowe.example.org")
        assert client.base_url == "https://gowe.example.org/api/v1"

    def test_base_url_normalization_trailing_slash(self):
        client = GoWeClient("https://gowe.example.org/")
        assert client.base_url == "https://gowe.example.org/api/v1"

    def test_base_url_normalization_with_api_v1(self):
        client = GoWeClient("https://gowe.example.org/api/v1")
        assert client.base_url == "https://gowe.example.org/api/v1"

    def test_base_url_normalization_with_api_v1_trailing_slash(self):
        client = GoWeClient("https://gowe.example.org/api/v1/")
        assert client.base_url == "https://gowe.example.org/api/v1"

    def test_default_timeout(self):
        client = GoWeClient("https://gowe.example.org")
        assert client.timeout.read == 60

    def test_custom_timeout(self):
        client = GoWeClient("https://gowe.example.org", timeout=120)
        assert client.timeout.read == 120


# ---------------------------------------------------------------------------
# Envelope unwrap tests
# ---------------------------------------------------------------------------

class TestEnvelopeUnwrap:
    def test_unwrap_ok(self):
        client = GoWeClient("https://test")
        data = {"id": "wf_123", "name": "test"}
        result = client._unwrap(_envelope(data=data))
        assert result == data

    def test_unwrap_ok_none_data(self):
        client = GoWeClient("https://test")
        result = client._unwrap(_envelope(data=None))
        assert result is None

    def test_unwrap_error_raises(self):
        client = GoWeClient("https://test")
        with pytest.raises(GoWeError, match="VALIDATION_ERROR"):
            client._unwrap(_envelope(
                status="error",
                error={"code": "VALIDATION_ERROR", "message": "bad input"},
            ))

    def test_unwrap_error_with_details(self):
        client = GoWeClient("https://test")
        with pytest.raises(GoWeError, match="extra info"):
            client._unwrap(_envelope(
                status="error",
                error={
                    "code": "VALIDATION_ERROR",
                    "message": "bad input",
                    "details": "extra info",
                },
            ))

    def test_unwrap_with_pagination(self):
        client = GoWeClient("https://test")
        pag = {"total": 50, "limit": 20, "offset": 0, "has_more": True}
        data, pagination = client._unwrap_with_pagination(
            _envelope(data=[{"id": "1"}], pagination=pag)
        )
        assert data == [{"id": "1"}]
        assert pagination == pag

    def test_unwrap_with_pagination_missing(self):
        client = GoWeClient("https://test")
        data, pagination = client._unwrap_with_pagination(_envelope(data=[]))
        assert data == []
        assert pagination is None


# ---------------------------------------------------------------------------
# Health endpoint tests
# ---------------------------------------------------------------------------

class TestHealth:
    @pytest.mark.asyncio
    async def test_health_success(self):
        health_data = {
            "status": "healthy",
            "version": "0.1.0",
            "uptime": "10h",
            "executors": {"bvbrc": "available"},
        }
        client = GoWeClient("https://test")
        client._request = AsyncMock(return_value=_envelope(data=health_data))

        result = await client.health()
        assert result["status"] == "healthy"
        assert result["version"] == "0.1.0"

    @pytest.mark.asyncio
    async def test_is_healthy_true(self):
        client = GoWeClient("https://test")
        client.health = AsyncMock(return_value={"status": "healthy"})
        assert await client.is_healthy() is True

    @pytest.mark.asyncio
    async def test_is_healthy_false_unhealthy(self):
        client = GoWeClient("https://test")
        client.health = AsyncMock(return_value={"status": "degraded"})
        assert await client.is_healthy() is False

    @pytest.mark.asyncio
    async def test_is_healthy_false_on_error(self):
        client = GoWeClient("https://test")
        client.health = AsyncMock(side_effect=GoWeError("down", error_type="CONNECTION_FAILED"))
        assert await client.is_healthy() is False


# ---------------------------------------------------------------------------
# Apps endpoint tests
# ---------------------------------------------------------------------------

class TestApps:
    @pytest.mark.asyncio
    async def test_list_apps(self):
        apps = [{"id": "GenomeAssembly2", "label": "Genome Assembly"}]
        client = GoWeClient("https://test")
        client._request = AsyncMock(return_value=_envelope(data=apps))

        result = await client.list_apps()
        assert result == apps
        client._request.assert_called_once_with("GET", "/apps", auth_token=None)

    @pytest.mark.asyncio
    async def test_get_app(self):
        app = {"id": "GenomeAssembly2", "parameters": []}
        client = GoWeClient("https://test")
        client._request = AsyncMock(return_value=_envelope(data=app))

        result = await client.get_app("GenomeAssembly2")
        assert result == app
        client._request.assert_called_once_with(
            "GET", "/apps/GenomeAssembly2", auth_token=None
        )

    @pytest.mark.asyncio
    async def test_get_app_cwl_tool(self):
        cwl = {"cwlVersion": "v1.2", "class": "CommandLineTool"}
        client = GoWeClient("https://test")
        client._request = AsyncMock(return_value=_envelope(data=cwl))

        result = await client.get_app_cwl_tool("GenomeAssembly2")
        assert result["class"] == "CommandLineTool"


# ---------------------------------------------------------------------------
# Workflow endpoint tests
# ---------------------------------------------------------------------------

class TestWorkflows:
    @pytest.mark.asyncio
    async def test_register_workflow_dict(self):
        wf = {"id": "wf_abc", "name": "test-wf", "class": "Workflow"}
        client = GoWeClient("https://test")
        client._request = AsyncMock(return_value=_envelope(data=wf))

        cwl_doc = {"cwlVersion": "v1.2", "class": "Workflow"}
        result = await client.register_workflow(cwl_doc, auth_token="tok123")

        assert result["id"] == "wf_abc"
        call_args = client._request.call_args
        body = call_args.kwargs["json_body"]
        assert "cwl" in body
        # dict should be serialized to JSON string
        assert json.loads(body["cwl"]) == cwl_doc

    @pytest.mark.asyncio
    async def test_register_workflow_string(self):
        wf = {"id": "wf_abc"}
        client = GoWeClient("https://test")
        client._request = AsyncMock(return_value=_envelope(data=wf))

        yaml_str = "cwlVersion: v1.2\nclass: Workflow"
        result = await client.register_workflow(yaml_str, auth_token="tok123")

        call_args = client._request.call_args
        body = call_args.kwargs["json_body"]
        assert body["cwl"] == yaml_str

    @pytest.mark.asyncio
    async def test_register_workflow_with_metadata(self):
        client = GoWeClient("https://test")
        client._request = AsyncMock(return_value=_envelope(data={"id": "wf_1"}))

        await client.register_workflow(
            {"class": "Workflow"},
            auth_token="tok",
            name="my-workflow",
            description="A test",
            labels={"project": "demo"},
        )

        body = client._request.call_args.kwargs["json_body"]
        assert body["name"] == "my-workflow"
        assert body["description"] == "A test"
        assert body["labels"] == {"project": "demo"}

    @pytest.mark.asyncio
    async def test_get_workflow(self):
        wf = {"id": "wf_1", "name": "test", "steps": []}
        client = GoWeClient("https://test")
        client._request = AsyncMock(return_value=_envelope(data=wf))

        result = await client.get_workflow("wf_1")
        assert result["id"] == "wf_1"
        client._request.assert_called_once_with(
            "GET", "/workflows/wf_1", auth_token=None
        )

    @pytest.mark.asyncio
    async def test_list_workflows(self):
        wfs = [{"id": "wf_1"}, {"id": "wf_2"}]
        pag = {"total": 2, "limit": 50, "offset": 0, "has_more": False}
        client = GoWeClient("https://test")
        client._request = AsyncMock(return_value=_envelope(data=wfs, pagination=pag))

        data, pagination = await client.list_workflows(limit=10, offset=5)
        assert len(data) == 2
        assert pagination["total"] == 2
        call_args = client._request.call_args
        assert call_args.kwargs["params"] == {"limit": "10", "offset": "5"}

    @pytest.mark.asyncio
    async def test_delete_workflow(self):
        client = GoWeClient("https://test")
        client._request = AsyncMock(return_value=_envelope(data={"deleted": True}))

        result = await client.delete_workflow("wf_1", auth_token="tok")
        assert result["deleted"] is True

    @pytest.mark.asyncio
    async def test_get_workflow_inputs(self):
        inputs = {"contigs": {"type": "File"}, "name": {"type": "string"}}
        client = GoWeClient("https://test")
        client._request = AsyncMock(return_value=_envelope(data=inputs))

        result = await client.get_workflow_inputs("wf_1")
        assert "contigs" in result

    @pytest.mark.asyncio
    async def test_get_workflow_outputs(self):
        outputs = {"result": {"type": "File[]"}}
        client = GoWeClient("https://test")
        client._request = AsyncMock(return_value=_envelope(data=outputs))

        result = await client.get_workflow_outputs("wf_1")
        assert "result" in result

    @pytest.mark.asyncio
    async def test_validate_workflow(self):
        validation = {"valid": True, "warnings": []}
        client = GoWeClient("https://test")
        client._request = AsyncMock(return_value=_envelope(data=validation))

        result = await client.validate_workflow("wf_1")
        assert result["valid"] is True


# ---------------------------------------------------------------------------
# Submission endpoint tests
# ---------------------------------------------------------------------------

class TestSubmissions:
    @pytest.mark.asyncio
    async def test_create_submission(self):
        sub = {"id": "sub_abc", "state": "PENDING", "workflow_id": "wf_1"}
        client = GoWeClient("https://test")
        client._request = AsyncMock(return_value=_envelope(data=sub))

        result = await client.create_submission(
            workflow_id="wf_1",
            inputs={"contigs": {"class": "File", "location": "ws:///user@bvbrc/home/c.fa"}},
            auth_token="tok",
        )

        assert result["id"] == "sub_abc"
        assert result["state"] == "PENDING"
        body = client._request.call_args.kwargs["json_body"]
        assert body["workflow_id"] == "wf_1"
        assert "contigs" in body["inputs"]

    @pytest.mark.asyncio
    async def test_create_submission_with_labels_and_output(self):
        client = GoWeClient("https://test")
        client._request = AsyncMock(return_value=_envelope(data={"id": "sub_1"}))

        await client.create_submission(
            workflow_id="wf_1",
            inputs={},
            auth_token="tok",
            labels={"batch": "42"},
            output_destination="ws:///user@bvbrc/home/results/",
        )

        body = client._request.call_args.kwargs["json_body"]
        assert body["labels"] == {"batch": "42"}
        assert body["output_destination"] == "ws:///user@bvbrc/home/results/"

    @pytest.mark.asyncio
    async def test_dry_run(self):
        dry = {
            "dry_run": True,
            "valid": True,
            "dag_acyclic": True,
            "execution_order": ["step1", "step2"],
            "errors": [],
            "warnings": [],
        }
        client = GoWeClient("https://test")
        client._request = AsyncMock(return_value=_envelope(data=dry))

        result = await client.dry_run("wf_1", inputs={}, auth_token="tok")

        assert result["dry_run"] is True
        assert result["valid"] is True
        call_args = client._request.call_args
        assert call_args.kwargs["params"] == {"dry_run": "true"}

    @pytest.mark.asyncio
    async def test_get_submission(self):
        sub = {
            "id": "sub_1",
            "state": "RUNNING",
            "tasks": [{"id": "t_1", "state": "RUNNING"}],
        }
        client = GoWeClient("https://test")
        client._request = AsyncMock(return_value=_envelope(data=sub))

        result = await client.get_submission("sub_1")
        assert result["state"] == "RUNNING"

    @pytest.mark.asyncio
    async def test_list_submissions(self):
        subs = [{"id": "sub_1"}, {"id": "sub_2"}]
        pag = {"total": 2, "limit": 50, "offset": 0, "has_more": False}
        client = GoWeClient("https://test")
        client._request = AsyncMock(return_value=_envelope(data=subs, pagination=pag))

        data, pagination = await client.list_submissions()
        assert len(data) == 2

    @pytest.mark.asyncio
    async def test_cancel_submission(self):
        client = GoWeClient("https://test")
        client._request = AsyncMock(return_value=_envelope(data={"id": "sub_1", "state": "CANCELLED"}))

        result = await client.cancel_submission("sub_1", auth_token="tok")
        assert result["state"] == "CANCELLED"

    @pytest.mark.asyncio
    async def test_retry_submission(self):
        client = GoWeClient("https://test")
        client._request = AsyncMock(return_value=_envelope(data={"id": "sub_1", "state": "PENDING"}))

        result = await client.retry_submission("sub_1", auth_token="tok")
        assert result["state"] == "PENDING"


# ---------------------------------------------------------------------------
# Task endpoint tests
# ---------------------------------------------------------------------------

class TestTasks:
    @pytest.mark.asyncio
    async def test_list_tasks(self):
        tasks = [{"id": "t_1", "state": "SUCCESS"}, {"id": "t_2", "state": "RUNNING"}]
        client = GoWeClient("https://test")
        client._request = AsyncMock(return_value=_envelope(data=tasks))

        result = await client.list_tasks("sub_1")
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_get_task(self):
        task = {"id": "t_1", "state": "SUCCESS", "exit_code": 0}
        client = GoWeClient("https://test")
        client._request = AsyncMock(return_value=_envelope(data=task))

        result = await client.get_task("sub_1", "t_1")
        assert result["exit_code"] == 0

    @pytest.mark.asyncio
    async def test_get_task_logs(self):
        logs = {"stdout": "hello world\n", "stderr": ""}
        client = GoWeClient("https://test")
        client._request = AsyncMock(return_value=_envelope(data=logs))

        result = await client.get_task_logs("sub_1", "t_1")
        assert result["stdout"] == "hello world\n"


# ---------------------------------------------------------------------------
# Helpers for _request-level tests
# ---------------------------------------------------------------------------

def _mock_async_client(handler):
    """
    Create a mock that replaces httpx.AsyncClient as a constructor.

    Returns an object that supports ``async with Client(...) as c:``
    and routes ``c.request(...)`` through the given handler.

    handler(request) -> httpx.Response   (may also raise httpx exceptions)
    """
    mock_client = AsyncMock()

    async def mock_request(method, url, **kwargs):
        # Build a minimal httpx.Request for the handler
        req = httpx.Request(method, url, headers=kwargs.get("headers"))
        return handler(req)

    mock_client.request = mock_request
    # Make it work as an async context manager
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    # Return a constructor that ignores kwargs and returns our mock
    return lambda **kw: mock_client


# ---------------------------------------------------------------------------
# Error handling tests (_request method)
# ---------------------------------------------------------------------------

class TestErrorHandling:
    @pytest.mark.asyncio
    async def test_400_validation_error(self):
        def handler(request):
            return _json_response(
                _envelope(status="error", error={"code": "VALIDATION_ERROR", "message": "bad"}),
                status_code=400,
            )

        client = GoWeClient("https://test")
        with patch('common.gowe_client.httpx.AsyncClient', _mock_async_client(handler)):
            with pytest.raises(GoWeError) as exc_info:
                await client._request("GET", "/test")

        assert exc_info.value.error_type == "VALIDATION_FAILED"
        assert exc_info.value.status_code == 400

    @pytest.mark.asyncio
    async def test_401_auth_error(self):
        def handler(request):
            return _json_response({"detail": "unauthorized"}, status_code=401)

        client = GoWeClient("https://test")
        with patch('common.gowe_client.httpx.AsyncClient', _mock_async_client(handler)):
            with pytest.raises(GoWeError) as exc_info:
                await client._request("GET", "/test")

        assert exc_info.value.error_type == "AUTH_REQUIRED"
        assert exc_info.value.status_code == 401

    @pytest.mark.asyncio
    async def test_404_not_found(self):
        def handler(request):
            return _json_response(
                _envelope(status="error", error={"code": "NOT_FOUND", "message": "no such workflow"}),
                status_code=404,
            )

        client = GoWeClient("https://test")
        with patch('common.gowe_client.httpx.AsyncClient', _mock_async_client(handler)):
            with pytest.raises(GoWeError) as exc_info:
                await client._request("GET", "/workflows/wf_nope")

        assert exc_info.value.error_type == "NOT_FOUND"
        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_500_server_error(self):
        def handler(request):
            return _json_response({"detail": "internal"}, status_code=500)

        client = GoWeClient("https://test")
        with patch('common.gowe_client.httpx.AsyncClient', _mock_async_client(handler)):
            with pytest.raises(GoWeError) as exc_info:
                await client._request("POST", "/workflows")

        assert exc_info.value.error_type == "SERVER_ERROR"
        assert exc_info.value.status_code == 500

    @pytest.mark.asyncio
    async def test_409_conflict(self):
        def handler(request):
            return _json_response({"detail": "duplicate"}, status_code=409)

        client = GoWeClient("https://test")
        with patch('common.gowe_client.httpx.AsyncClient', _mock_async_client(handler)):
            with pytest.raises(GoWeError) as exc_info:
                await client._request("POST", "/workflows")

        assert exc_info.value.error_type == "CONFLICT"
        assert exc_info.value.status_code == 409

    @pytest.mark.asyncio
    async def test_connection_error(self):
        def handler(request):
            raise httpx.ConnectError("refused")

        client = GoWeClient("https://test")
        with patch('common.gowe_client.httpx.AsyncClient', _mock_async_client(handler)):
            with pytest.raises(GoWeError) as exc_info:
                await client._request("GET", "/health")

        assert exc_info.value.error_type == "CONNECTION_FAILED"

    @pytest.mark.asyncio
    async def test_timeout_error(self):
        def handler(request):
            raise httpx.ReadTimeout("timed out")

        client = GoWeClient("https://test")
        with patch('common.gowe_client.httpx.AsyncClient', _mock_async_client(handler)):
            with pytest.raises(GoWeError) as exc_info:
                await client._request("GET", "/health")

        assert exc_info.value.error_type == "TIMEOUT"

    @pytest.mark.asyncio
    async def test_envelope_error_raised_by_unwrap(self):
        """Even on HTTP 200, if envelope status is 'error', GoWeError is raised."""
        client = GoWeClient("https://test")
        with pytest.raises(GoWeError, match="quota exceeded"):
            client._unwrap(_envelope(
                status="error",
                error={"code": "RATE_LIMIT", "message": "quota exceeded"},
            ))

    @pytest.mark.asyncio
    async def test_auth_header_passed(self):
        """Verify auth token is sent as Authorization header."""
        captured_headers = {}

        def handler(request):
            captured_headers.update(dict(request.headers))
            return _json_response(_envelope(data={}))

        client = GoWeClient("https://test")
        with patch('common.gowe_client.httpx.AsyncClient', _mock_async_client(handler)):
            await client._request("GET", "/test", auth_token="un=user@bvbrc|tokenid=abc")

        assert captured_headers.get("authorization") == "un=user@bvbrc|tokenid=abc"

    @pytest.mark.asyncio
    async def test_no_auth_header_when_none(self):
        """Verify no Authorization header when auth_token is None."""
        captured_headers = {}

        def handler(request):
            captured_headers.update(dict(request.headers))
            return _json_response(_envelope(data={}))

        client = GoWeClient("https://test")
        with patch('common.gowe_client.httpx.AsyncClient', _mock_async_client(handler)):
            await client._request("GET", "/test")

        assert "authorization" not in captured_headers


# ---------------------------------------------------------------------------
# GoWeError tests
# ---------------------------------------------------------------------------

class TestGoWeError:
    def test_basic_error(self):
        err = GoWeError("something broke")
        assert str(err) == "something broke"
        assert err.error_type == "UNKNOWN_ERROR"
        assert err.status_code is None

    def test_typed_error(self):
        err = GoWeError("not found", error_type="NOT_FOUND", status_code=404)
        assert err.error_type == "NOT_FOUND"
        assert err.status_code == 404

    def test_is_exception(self):
        assert issubclass(GoWeError, Exception)
