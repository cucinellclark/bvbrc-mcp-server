"""
GoWe Workflow Engine HTTP Client

Async client for the GoWe CWL workflow engine REST API.

GoWe is a CWL v1.2 workflow engine that replaces the legacy workflow engine.
It manages workflow registration, submission, scheduling, and execution via
multiple backends (local, Docker, Apptainer, distributed workers, BV-BRC).

All responses follow the GoWe envelope format:
    {status, request_id, timestamp, data, pagination?, error?}

Uses httpx (async) for HTTP calls, matching the pattern established by
workflow_engine_client.py.
"""

import httpx
from typing import Dict, Any, Optional, List
import sys
import json


class GoWeClient:
    """HTTP client for the GoWe CWL workflow engine REST API."""

    def __init__(self, base_url: str, timeout: int = 60):
        """
        Initialize GoWe client.

        Args:
            base_url: Base URL for GoWe API (e.g., "https://gowe.software-smithy.org")
                      Trailing slashes and /api/v1 suffix are normalized automatically.
            timeout: Request timeout in seconds (default: 60, higher than legacy
                     client because CWL registration can be slower)
        """
        url = base_url.rstrip('/')
        # Accept URLs with or without /api/v1 suffix
        if not url.endswith('/api/v1'):
            url = url.rstrip('/') + '/api/v1'
        self.base_url = url
        self.timeout = httpx.Timeout(timeout)

    # ------------------------------------------------------------------
    # Response envelope handling
    # ------------------------------------------------------------------

    def _unwrap(self, response_json: Dict[str, Any]) -> Any:
        """
        Unwrap GoWe's standard response envelope and return the data payload.

        GoWe responses follow the format:
            {status: "ok"|"error", request_id, timestamp, data, pagination?, error?}

        Returns the ``data`` field.  Raises GoWeError if the envelope
        indicates an error.
        """
        if response_json.get("status") == "error":
            err = response_json.get("error") or {}
            code = err.get("code", "UNKNOWN")
            message = err.get("message", "Unknown GoWe error")
            details = err.get("details")
            detail_str = f" Details: {details}" if details else ""
            raise GoWeError(
                f"GoWe error ({code}): {message}{detail_str}",
                error_type=code,
            )
        return response_json.get("data")

    def _unwrap_with_pagination(self, response_json: Dict[str, Any]) -> tuple:
        """
        Unwrap envelope and return (data, pagination) tuple.

        Pagination dict contains: {total, limit, offset, has_more}.
        Returns None for pagination if not present.
        """
        data = self._unwrap(response_json)
        pagination = response_json.get("pagination")
        return data, pagination

    # ------------------------------------------------------------------
    # Internal HTTP helpers
    # ------------------------------------------------------------------

    def _auth_headers(self, auth_token: Optional[str] = None) -> Dict[str, str]:
        """Build request headers, optionally including auth."""
        headers = {"Content-Type": "application/json"}
        if auth_token:
            headers["Authorization"] = auth_token
        return headers

    def _extract_error_message(self, response: httpx.Response) -> str:
        """Extract a human-readable error message from an error response."""
        try:
            body = response.json()
            # GoWe envelope error
            err = body.get("error") or {}
            if isinstance(err, dict) and err.get("message"):
                msg = err["message"]
                details = err.get("details")
                if details:
                    msg += f" Details: {details}"
                return msg
            # Fallback to detail field (FastAPI style)
            if body.get("detail"):
                return str(body["detail"])
            return response.text
        except Exception:
            return response.text

    async def _request(
        self,
        method: str,
        path: str,
        *,
        auth_token: Optional[str] = None,
        json_body: Optional[Any] = None,
        params: Optional[Dict[str, str]] = None,
        timeout: Optional[httpx.Timeout] = None,
        expect_status: tuple = (200, 201),
    ) -> Dict[str, Any]:
        """
        Make an HTTP request to GoWe and return the parsed JSON response.

        This centralizes the error-handling pattern so individual methods
        don't repeat the try/except boilerplate.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            path: URL path relative to base_url (e.g., "/workflows")
            auth_token: Optional BV-BRC auth token
            json_body: Optional JSON request body
            params: Optional query parameters
            timeout: Override default timeout for this request
            expect_status: Tuple of HTTP status codes considered successful

        Returns:
            Parsed JSON response body (full envelope)

        Raises:
            GoWeError: On any failure
        """
        url = f"{self.base_url}{path}"

        try:
            async with httpx.AsyncClient(timeout=timeout or self.timeout) as client:
                response = await client.request(
                    method,
                    url,
                    json=json_body,
                    params=params,
                    headers=self._auth_headers(auth_token),
                )

                if response.status_code in expect_status:
                    return response.json()

                error_msg = self._extract_error_message(response)

                if response.status_code == 400:
                    raise GoWeError(
                        f"Validation error: {error_msg}",
                        error_type="VALIDATION_FAILED",
                        status_code=400,
                    )
                elif response.status_code == 401:
                    raise GoWeError(
                        f"Authentication required: {error_msg}",
                        error_type="AUTH_REQUIRED",
                        status_code=401,
                    )
                elif response.status_code == 403:
                    raise GoWeError(
                        f"Permission denied: {error_msg}",
                        error_type="FORBIDDEN",
                        status_code=403,
                    )
                elif response.status_code == 404:
                    raise GoWeError(
                        f"Not found: {error_msg}",
                        error_type="NOT_FOUND",
                        status_code=404,
                    )
                elif response.status_code == 409:
                    raise GoWeError(
                        f"Conflict: {error_msg}",
                        error_type="CONFLICT",
                        status_code=409,
                    )
                elif response.status_code >= 500:
                    raise GoWeError(
                        f"GoWe server error: {error_msg}",
                        error_type="SERVER_ERROR",
                        status_code=response.status_code,
                    )
                else:
                    raise GoWeError(
                        f"Unexpected response: {response.status_code} - {error_msg}",
                        error_type="UNKNOWN_ERROR",
                        status_code=response.status_code,
                    )

        except GoWeError:
            raise
        except httpx.ConnectError as e:
            print(f"Failed to connect to GoWe at {url}: {e}", file=sys.stderr)
            raise GoWeError(
                f"Cannot connect to GoWe at {self.base_url}. Is it running?",
                error_type="CONNECTION_FAILED",
            ) from e
        except httpx.TimeoutException as e:
            print(f"GoWe request timed out: {e}", file=sys.stderr)
            raise GoWeError(
                f"GoWe request timed out after {self.timeout.read}s",
                error_type="TIMEOUT",
            ) from e
        except Exception as e:
            print(f"Unexpected error calling GoWe: {e}", file=sys.stderr)
            raise GoWeError(
                f"Unexpected error: {str(e)}",
                error_type="UNKNOWN_ERROR",
            ) from e

    # ------------------------------------------------------------------
    # Apps (BV-BRC application catalog, proxied through GoWe)
    # ------------------------------------------------------------------

    async def list_apps(self, auth_token: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List available BV-BRC applications.

        Returns:
            List of app dicts with id, label, description fields.
        """
        resp = await self._request("GET", "/apps", auth_token=auth_token)
        return self._unwrap(resp)

    async def get_app(self, app_id: str, auth_token: Optional[str] = None) -> Dict[str, Any]:
        """
        Get detailed schema for a specific BV-BRC application.

        Args:
            app_id: BV-BRC application ID (e.g., "GenomeAssembly2")

        Returns:
            App parameter schema dict.
        """
        resp = await self._request("GET", f"/apps/{app_id}", auth_token=auth_token)
        return self._unwrap(resp)

    async def get_app_cwl_tool(self, app_id: str, auth_token: Optional[str] = None) -> Dict[str, Any]:
        """
        Get an auto-generated CWL CommandLineTool definition for a BV-BRC app.

        Args:
            app_id: BV-BRC application ID (e.g., "GenomeAssembly2")

        Returns:
            CWL CommandLineTool document as a dict.
        """
        resp = await self._request("GET", f"/apps/{app_id}/cwl-tool", auth_token=auth_token)
        return self._unwrap(resp)

    # ------------------------------------------------------------------
    # Workflows (CWL workflow registration and management)
    # ------------------------------------------------------------------

    async def register_workflow(
        self,
        cwl_document: Any,
        auth_token: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        labels: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Register a CWL workflow document with GoWe.

        The CWL document can be a dict (parsed YAML/JSON) or a string
        (raw YAML/JSON content). GoWe parses, validates, and stores it.

        Args:
            cwl_document: CWL workflow document (dict or string)
            auth_token: BV-BRC authentication token
            name: Optional workflow name (overrides CWL label)
            description: Optional description
            labels: Optional key-value labels for filtering

        Returns:
            Dict with workflow id, name, class, version, etc.
        """
        body: Dict[str, Any] = {}

        # GoWe expects the CWL document in a field named "cwl" (string)
        if isinstance(cwl_document, str):
            body["cwl"] = cwl_document
        else:
            body["cwl"] = json.dumps(cwl_document)

        if name:
            body["name"] = name
        if description:
            body["description"] = description
        if labels:
            body["labels"] = labels

        print(f"Registering CWL workflow with GoWe: {self.base_url}/workflows", file=sys.stderr)
        resp = await self._request(
            "POST", "/workflows",
            auth_token=auth_token,
            json_body=body,
            expect_status=(200, 201),
        )
        data = self._unwrap(resp)
        print(f"Workflow registered: {data.get('id', 'unknown')}", file=sys.stderr)
        return data

    async def get_workflow(self, workflow_id: str, auth_token: Optional[str] = None) -> Dict[str, Any]:
        """
        Get a registered workflow by ID.

        Args:
            workflow_id: GoWe workflow identifier

        Returns:
            Full workflow document including CWL content, steps, inputs, outputs.
        """
        resp = await self._request("GET", f"/workflows/{workflow_id}", auth_token=auth_token)
        return self._unwrap(resp)

    async def list_workflows(
        self,
        auth_token: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple:
        """
        List registered workflows.

        Args:
            auth_token: Optional auth token
            limit: Max results per page
            offset: Pagination offset

        Returns:
            Tuple of (list of workflow dicts, pagination dict)
        """
        params = {"limit": str(limit), "offset": str(offset)}
        resp = await self._request("GET", "/workflows", auth_token=auth_token, params=params)
        return self._unwrap_with_pagination(resp)

    async def delete_workflow(self, workflow_id: str, auth_token: str) -> Dict[str, Any]:
        """
        Delete a registered workflow.

        Args:
            workflow_id: GoWe workflow identifier
            auth_token: BV-BRC authentication token

        Returns:
            Confirmation dict.
        """
        resp = await self._request("DELETE", f"/workflows/{workflow_id}", auth_token=auth_token)
        return self._unwrap(resp)

    async def get_workflow_inputs(self, workflow_id: str, auth_token: Optional[str] = None) -> Dict[str, Any]:
        """
        Get the input schema for a registered workflow.

        Returns CWL input definitions with types, defaults, and documentation.

        Args:
            workflow_id: GoWe workflow identifier

        Returns:
            Dict mapping input names to their CWL type definitions.
        """
        resp = await self._request("GET", f"/workflows/{workflow_id}/inputs", auth_token=auth_token)
        return self._unwrap(resp)

    async def get_workflow_outputs(self, workflow_id: str, auth_token: Optional[str] = None) -> Dict[str, Any]:
        """
        Get the output schema for a registered workflow.

        Args:
            workflow_id: GoWe workflow identifier

        Returns:
            Dict mapping output names to their CWL type definitions.
        """
        resp = await self._request("GET", f"/workflows/{workflow_id}/outputs", auth_token=auth_token)
        return self._unwrap(resp)

    async def validate_workflow(self, workflow_id: str, auth_token: Optional[str] = None) -> Dict[str, Any]:
        """
        Validate a registered workflow without executing it.

        Args:
            workflow_id: GoWe workflow identifier

        Returns:
            Validation result dict.
        """
        resp = await self._request("POST", f"/workflows/{workflow_id}/validate", auth_token=auth_token)
        return self._unwrap(resp)

    # ------------------------------------------------------------------
    # Submissions (workflow execution)
    # ------------------------------------------------------------------

    async def create_submission(
        self,
        workflow_id: str,
        inputs: Dict[str, Any],
        auth_token: str,
        labels: Optional[Dict[str, str]] = None,
        output_destination: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a new submission (execute a registered workflow).

        Args:
            workflow_id: ID of a registered workflow
            inputs: CWL input values dict. Files should use
                    {"class": "File", "location": "ws:///user@bvbrc/path"}
                    format. Directories use {"class": "Directory", "location": "ws:///..."}.
            auth_token: BV-BRC authentication token (delegated to tasks)
            labels: Optional key-value labels
            output_destination: Optional output URI (e.g., "ws:///user@bvbrc/home/results/")

        Returns:
            Submission dict with id, state, workflow_id, etc.
        """
        body: Dict[str, Any] = {
            "workflow_id": workflow_id,
            "inputs": inputs,
        }
        if labels:
            body["labels"] = labels
        if output_destination:
            body["output_destination"] = output_destination

        print(f"Creating GoWe submission for workflow {workflow_id}", file=sys.stderr)
        resp = await self._request(
            "POST", "/submissions",
            auth_token=auth_token,
            json_body=body,
            expect_status=(200, 201),
        )
        data = self._unwrap(resp)
        print(f"Submission created: {data.get('id', 'unknown')}", file=sys.stderr)
        return data

    async def dry_run(
        self,
        workflow_id: str,
        inputs: Dict[str, Any],
        auth_token: str,
    ) -> Dict[str, Any]:
        """
        Validate a submission without executing it.

        Checks input types, resolves the DAG, verifies executor availability.

        Args:
            workflow_id: ID of a registered workflow
            inputs: CWL input values dict
            auth_token: BV-BRC authentication token

        Returns:
            Validation result with dry_run, valid, dag_acyclic,
            execution_order, executor_availability, errors, warnings.
        """
        body: Dict[str, Any] = {
            "workflow_id": workflow_id,
            "inputs": inputs,
        }
        resp = await self._request(
            "POST", "/submissions",
            auth_token=auth_token,
            json_body=body,
            params={"dry_run": "true"},
            expect_status=(200, 201),
        )
        return self._unwrap(resp)

    async def get_submission(self, submission_id: str, auth_token: Optional[str] = None) -> Dict[str, Any]:
        """
        Get submission status and details.

        Returns the three-level state hierarchy:
        Submission (PENDING/RUNNING/COMPLETED/FAILED/CANCELLED)
          -> StepInstances (WAITING/READY/DISPATCHED/RUNNING/COMPLETED/FAILED/SKIPPED)
            -> Tasks (PENDING/SCHEDULED/QUEUED/RUNNING/SUCCESS/FAILED/SKIPPED)

        Args:
            submission_id: GoWe submission identifier

        Returns:
            Full submission dict with state, tasks, step instances, etc.
        """
        resp = await self._request("GET", f"/submissions/{submission_id}", auth_token=auth_token)
        return self._unwrap(resp)

    async def list_submissions(
        self,
        auth_token: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple:
        """
        List submissions.

        Args:
            auth_token: Optional auth token
            limit: Max results per page
            offset: Pagination offset

        Returns:
            Tuple of (list of submission dicts, pagination dict)
        """
        params = {"limit": str(limit), "offset": str(offset)}
        resp = await self._request("GET", "/submissions", auth_token=auth_token, params=params)
        return self._unwrap_with_pagination(resp)

    async def cancel_submission(self, submission_id: str, auth_token: str) -> Dict[str, Any]:
        """
        Cancel a running submission.

        Args:
            submission_id: GoWe submission identifier
            auth_token: BV-BRC authentication token

        Returns:
            Updated submission dict.
        """
        print(f"Cancelling GoWe submission {submission_id}", file=sys.stderr)
        resp = await self._request(
            "PUT", f"/submissions/{submission_id}/cancel",
            auth_token=auth_token,
        )
        return self._unwrap(resp)

    async def retry_submission(self, submission_id: str, auth_token: str) -> Dict[str, Any]:
        """
        Retry a failed submission (resets failed steps and tasks).

        Note: Retry support is deferred from the current migration scope
        but the client method is provided for future use.

        Args:
            submission_id: GoWe submission identifier
            auth_token: BV-BRC authentication token

        Returns:
            Updated submission dict.
        """
        print(f"Retrying GoWe submission {submission_id}", file=sys.stderr)
        resp = await self._request(
            "PUT", f"/submissions/{submission_id}/retry",
            auth_token=auth_token,
        )
        return self._unwrap(resp)

    # ------------------------------------------------------------------
    # Tasks (individual work units within a submission)
    # ------------------------------------------------------------------

    async def list_tasks(
        self,
        submission_id: str,
        auth_token: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        List tasks for a submission.

        Args:
            submission_id: GoWe submission identifier

        Returns:
            List of task dicts.
        """
        resp = await self._request(
            "GET", f"/submissions/{submission_id}/tasks",
            auth_token=auth_token,
        )
        return self._unwrap(resp)

    async def get_task(
        self,
        submission_id: str,
        task_id: str,
        auth_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get details for a specific task.

        Args:
            submission_id: GoWe submission identifier
            task_id: Task identifier

        Returns:
            Task dict with state, executor_type, exit_code, etc.
        """
        resp = await self._request(
            "GET", f"/submissions/{submission_id}/tasks/{task_id}",
            auth_token=auth_token,
        )
        return self._unwrap(resp)

    async def get_task_logs(
        self,
        submission_id: str,
        task_id: str,
        auth_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get stdout/stderr logs for a task.

        Args:
            submission_id: GoWe submission identifier
            task_id: Task identifier

        Returns:
            Dict with stdout and stderr strings.
        """
        resp = await self._request(
            "GET", f"/submissions/{submission_id}/tasks/{task_id}/logs",
            auth_token=auth_token,
        )
        return self._unwrap(resp)

    # ------------------------------------------------------------------
    # Health
    # ------------------------------------------------------------------

    async def health(self) -> Dict[str, Any]:
        """
        Check GoWe server health.

        Returns:
            Health dict with status, version, uptime, scheduler state,
            executor availability, and worker counts.
        """
        resp = await self._request(
            "GET", "/health",
            timeout=httpx.Timeout(10),
        )
        return self._unwrap(resp)

    async def is_healthy(self) -> bool:
        """
        Quick health check. Returns True if GoWe is reachable and healthy.
        """
        try:
            data = await self.health()
            return data.get("status") == "healthy"
        except Exception as e:
            print(f"GoWe health check failed: {e}", file=sys.stderr)
            return False


class GoWeError(Exception):
    """Custom exception for GoWe API errors."""

    def __init__(
        self,
        message: str,
        error_type: str = "UNKNOWN_ERROR",
        status_code: Optional[int] = None,
    ):
        """
        Args:
            message: Human-readable error description
            error_type: Error category (e.g., "CONNECTION_FAILED", "VALIDATION_FAILED",
                        "NOT_FOUND", "AUTH_REQUIRED", "SERVER_ERROR", "TIMEOUT")
            status_code: HTTP status code if applicable
        """
        super().__init__(message)
        self.error_type = error_type
        self.status_code = status_code
