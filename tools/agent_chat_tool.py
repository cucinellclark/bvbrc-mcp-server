"""Unified agent chat tool -- MCP entry point for all agent LLM loops.

Exposes a single ``agent_chat`` MCP tool that dispatches to one of six
agent back-ends (data, service, workspace, helpdesk, analysis, planning)
based on the ``agent_type`` parameter.  Each agent's ``run_agent()``
function is imported lazily so the MCP server can start even if an
individual agent has unmet deps.

This bridges the MCP server (external interface) to the agent modules
(internal LLM loops with tool calling).
"""

from __future__ import annotations

import json
import sys
import traceback
from pathlib import Path
from typing import Any, Dict, Optional

from fastmcp import Context, FastMCP

# ---------------------------------------------------------------------------
# sys.path setup -- make the five agent packages importable.
#
# Layout relative to this file:
#   bvbrc-agents/
#     ├── mcp_server/tools/agent_chat_tool.py  <-- HERE
#     ├── agents/data_agent/
#     ├── agents/service_agent/
#     ├── agents/workspace_agent/
#     ├── agents/helpdesk_agent/
#     ├── agents/analysis_agent/
#     ├── agents/planning_agent/
#     └── config/llm_config.py
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent

# Add agents/, config/, and repo root (for shared/) to sys.path
for _subdir in ("agents", "config"):
    _path = str(_REPO_ROOT / _subdir)
    if _path not in sys.path:
        sys.path.insert(0, _path)

# Add repo root so ``shared`` package is importable (shared.tools, shared.prompts)
_root_path = str(_REPO_ROOT)
if _root_path not in sys.path:
    sys.path.insert(0, _root_path)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _normalize_bvbrc_token(token: Optional[str]) -> Optional[str]:
    """Return a raw PATRIC token, stripping a leading ``Bearer `` if present.

    Downstream BV-BRC services (workspace, literature RAG on :12006, etc.)
    expect ``un=...|tokenid=...``, not ``Bearer <token>``.
    """
    if not token:
        return None
    value = token.strip()
    if value.lower().startswith("bearer "):
        value = value[7:].strip()
    return value or None


def _resolve_auth_token(token_provider, token: Optional[str]) -> Optional[str]:
    """Resolve BV-BRC auth token.

    IMPORTANT: MCP HTTP requests carry an OAuth Authorization header for MCP itself.
    That header is *not* the BV-BRC token we need for downstream BV-BRC services.

    Priority:
    1. Explicit token passed to agent_chat (from orchestrator/gateway)
    2. TokenProvider-derived token (e.g. stdio mode env var)
    """
    if token:
        return _normalize_bvbrc_token(token)
    if token_provider:
        return _normalize_bvbrc_token(token_provider.get_token(None))
    return None


def _parse_context(context: Optional[str]) -> dict[str, Any]:
    """Parse context JSON string into a dict."""
    if not context:
        return {}
    try:
        return json.loads(context)
    except (json.JSONDecodeError, TypeError):
        return {"raw_context": context}


def _build_config_kwargs(
    auth_token: Optional[str],
    ctx: dict[str, Any],
) -> dict[str, Any]:
    """Build config kwargs from auth token and LLM override in context."""
    config_kwargs: dict[str, Any] = {"bvbrc_auth_token": auth_token}
    llm_override = ctx.pop("llm_override", None)
    if llm_override and isinstance(llm_override, dict):
        if llm_override.get("base_url"):
            config_kwargs["llm_base_url"] = llm_override["base_url"]
        if llm_override.get("api_key"):
            config_kwargs["llm_api_key"] = llm_override["api_key"]
        if llm_override.get("model"):
            config_kwargs["llm_model"] = llm_override["model"]

    # Forward auto-submit preference to the service agent config
    auto_submit = ctx.get("auto_submit_preference")
    if auto_submit:
        config_kwargs["auto_submit_preference"] = auto_submit

    # Forward GoWe URL override if provided
    gowe_url = ctx.get("gowe_url")
    if gowe_url:
        config_kwargs["gowe_url"] = gowe_url

    # Forward session context for session-based workspace output paths.
    # submit_gowe_job uses these to rewrite output_path under
    # /<workspace_path>/.chats/<session_id>/<subfolder>
    session_id = ctx.get("session_id")
    if session_id:
        config_kwargs["session_id"] = session_id
    workspace_path = ctx.get("workspace_path")
    if workspace_path:
        config_kwargs["workspace_path"] = workspace_path

    return config_kwargs


def _build_tool_trace(result: Any) -> list[dict[str, Any]]:
    """Extract a serializable tool trace from an agent result."""
    return [
        {
            "tool": ex.tool_call.name,
            "arguments": ex.tool_call.arguments,
            "error": ex.error,
            "duration_ms": ex.duration_ms,
        }
        for ex in result.tool_trace
    ]


def _error_response(message: str) -> Dict[str, Any]:
    """Build a standard error response dict."""
    return {
        "answer": message,
        "status": "error",
        "sources": [],
        "iterations_used": 0,
        "elapsed_seconds": 0.0,
        "tool_trace": [],
    }


def _classify_agent_error(error: Exception, agent_type: str) -> str:
    """Classify an agent error and return a user-friendly message.

    Detects common error patterns (context length, auth, rate limits,
    network) and provides actionable guidance instead of raw API errors.
    """
    error_str = str(error).lower()

    # Context length / token limit errors
    if any(
        phrase in error_str
        for phrase in [
            "maximum context length",
            "context_length_exceeded",
            "too many tokens",
            "prompt is too long",
            "input_text",
            "token limit",
        ]
    ):
        return (
            "The conversation has grown too large for the AI model to process. "
            "This can happen when there is a lot of prior conversation history. "
            "Please try starting a new conversation and re-asking your question."
        )

    # Authentication errors
    if any(
        phrase in error_str
        for phrase in [
            "401",
            "unauthorized",
            "authentication",
            "invalid api key",
        ]
    ):
        return (
            "Authentication error. Your session may have expired. "
            "Please try logging in again."
        )

    # Rate limit errors
    if any(
        phrase in error_str
        for phrase in [
            "429",
            "rate_limit",
            "rate limit",
            "too many requests",
        ]
    ):
        return (
            "The AI service is temporarily overloaded. "
            "Please wait a moment and try again."
        )

    # Network / timeout errors
    if any(
        phrase in error_str
        for phrase in [
            "timeout",
            "connection",
            "network",
            "unreachable",
        ]
    ):
        return (
            f"The {agent_type} agent couldn't connect to a required service. "
            "This may be a temporary network issue. Please try again."
        )

    # Default: include the raw error but with a friendlier wrapper
    return f"The {agent_type} agent encountered an error: {error}"


# ---------------------------------------------------------------------------
# Per-agent dispatch helpers
# ---------------------------------------------------------------------------


async def _run_data_agent(
    query: str,
    config_kwargs: dict[str, Any],
    ctx: dict[str, Any],
    progress_callback,
) -> Dict[str, Any]:
    """Import and run the data agent, returning a response dict."""
    from data_agent.agent import run_agent
    from data_agent.models import AgentConfig

    config = AgentConfig(**config_kwargs)
    result = await run_agent(
        query=query,
        config=config,
        context=ctx,
        progress_callback=progress_callback,
    )

    response: Dict[str, Any] = {
        "answer": result.answer,
        "status": result.status,
        "sources": result.sources,
        "iterations_used": result.iterations_used,
        "elapsed_seconds": result.elapsed_seconds,
        "tool_trace": _build_tool_trace(result),
    }
    if result.structured_data:
        response["structured_data"] = result.structured_data
    return response


async def _run_service_agent(
    query: str,
    config_kwargs: dict[str, Any],
    ctx: dict[str, Any],
    progress_callback,
) -> Dict[str, Any]:
    """Import and run the service agent, returning a response dict."""
    from service_agent.agent import run_agent
    from service_agent.models import AgentConfig

    config = AgentConfig(**config_kwargs)
    result = await run_agent(
        query=query,
        config=config,
        context=ctx,
        progress_callback=progress_callback,
    )

    tool_trace = _build_tool_trace(result)

    # Service2: build answer from structured result
    # Lifecycle operations (submit/status/cancel) produce an operation_message;
    # planning results use the full pretty() output.
    if result.operation_message:
        answer = result.operation_message
    elif result.status == "needs_input" and result.question:
        answer = result.question
    elif result.status == "error" and result.error_message:
        answer = result.error_message
    else:
        answer = result.pretty()

    response: Dict[str, Any] = {
        "answer": answer,
        "status": result.status,
        "sources": result.sources,
        "elapsed_seconds": result.elapsed_seconds,
        "tool_trace": tool_trace,
    }
    if result.manifest:
        response["manifest"] = result.manifest
    if result.workflow_plan:
        response["workflow_plan"] = result.workflow_plan
    if result.question:
        response["question"] = result.question
    # Workflow engine persistence metadata
    if result.workflow_id:
        response["workflow_id"] = result.workflow_id
    response["persisted"] = result.persisted
    if result.auto_submitted:
        response["auto_submitted"] = True
    # GoWe-specific fields
    if result.submission_id:
        response["submission_id"] = result.submission_id
    if result.cwl_document:
        response["cwl_document"] = result.cwl_document
    return response


async def _run_workspace_agent(
    query: str,
    config_kwargs: dict[str, Any],
    ctx: dict[str, Any],
    progress_callback,
) -> Dict[str, Any]:
    """Import and run the workspace agent, returning a response dict."""
    from workspace_agent.agent import run_agent
    from workspace_agent.models import AgentConfig

    config = AgentConfig(**config_kwargs)
    result = await run_agent(
        query=query,
        config=config,
        context=ctx,
        progress_callback=progress_callback,
    )

    tool_trace = _build_tool_trace(result)

    return {
        "answer": result.answer,
        "status": result.status,
        # Keep fields aligned with other agent_chat responses
        "sources": [],
        "iterations_used": result.iterations_used,
        "elapsed_seconds": result.elapsed_seconds,
        "tool_trace": tool_trace,
        # Workspace agent structured payload for rich UI rendering
        "items": getattr(result, "items", []),
        "metadata": getattr(result, "metadata", []),
        "ui_grids": getattr(result, "ui_grids", []),
        "previews": getattr(result, "previews", []),
        "paths_explored": getattr(result, "paths_explored", []),
    }


async def _run_helpdesk_agent(
    query: str,
    config_kwargs: dict[str, Any],
    ctx: dict[str, Any],
    progress_callback,
) -> Dict[str, Any]:
    """Import and run the helpdesk agent, returning a response dict."""
    from helpdesk_agent.agent import run_agent
    from helpdesk_agent.models import AgentConfig

    config = AgentConfig(**config_kwargs)
    result = await run_agent(
        query=query,
        config=config,
        context=ctx,
        progress_callback=progress_callback,
    )

    return {
        "answer": result.answer,
        "status": result.status,
        "sources": result.sources,
        "iterations_used": result.iterations_used,
        "elapsed_seconds": result.elapsed_seconds,
        "tool_trace": _build_tool_trace(result),
    }


async def _run_analysis_agent(
    query: str,
    config_kwargs: dict[str, Any],
    ctx: dict[str, Any],
    progress_callback,
) -> Dict[str, Any]:
    """Import and run the analysis agent, returning a response dict."""
    from analysis_agent.agent import run_agent
    from analysis_agent.models import AgentConfig

    # Analysis agent doesn't use auto_submit_preference; remove if present
    config_kwargs.pop("auto_submit_preference", None)

    config = AgentConfig(**config_kwargs)
    result = await run_agent(
        query=query,
        config=config,
        context=ctx,
        progress_callback=progress_callback,
    )

    tool_trace = _build_tool_trace(result)

    return {
        "answer": result.answer,
        "status": result.status,
        "sources": result.sources,
        "iterations_used": result.iterations_used,
        "elapsed_seconds": result.elapsed_seconds,
        "tool_trace": tool_trace,
        # Analysis agent structured payload for rich UI rendering
        "output_files": result.output_files,
        "metrics": result.metrics,
        "previews": result.previews,
        "report_links": result.report_links,
        "step_summaries": result.step_summaries,
    }


async def _run_planning_agent(
    query: str,
    config_kwargs: dict[str, Any],
    ctx: dict[str, Any],
    progress_callback,
) -> Dict[str, Any]:
    """Import and run the planning agent, returning a response dict."""
    from planning_agent.agent import run_agent
    from planning_agent.models import AgentConfig

    # Planning agent doesn't use auto_submit_preference
    config_kwargs.pop("auto_submit_preference", None)

    config = AgentConfig(**config_kwargs)
    result = await run_agent(
        query=query,
        config=config,
        context=ctx,
        progress_callback=progress_callback,
    )

    tool_trace = _build_tool_trace(result)

    response: Dict[str, Any] = {
        "answer": result.answer,
        "status": result.status,
        "sources": result.sources,
        "iterations_used": result.iterations_used,
        "elapsed_seconds": result.elapsed_seconds,
        "tool_trace": tool_trace,
    }

    # Planning-specific fields
    if result.plan:
        response["plan"] = result.plan
    if result.clarification_questions:
        response["clarification_questions"] = result.clarification_questions
    if result.step_execution:
        response["step_execution"] = result.step_execution

    return response


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


def register_agent_chat_tool(
    mcp: FastMCP,
    token_provider=None,
) -> None:
    """Register the unified ``agent_chat`` tool on the MCP server.

    Args:
        mcp: FastMCP server instance.
        token_provider: TokenProvider for extracting auth from HTTP headers.
    """

    @mcp.tool()
    async def agent_chat(
        query: str,
        agent_type: str = "data",
        context: Optional[str] = None,
        token: Optional[str] = None,
        mcp_ctx: Context = None,
    ) -> Dict[str, Any]:
        """Chat with a BV-BRC agent using natural language.

        Runs the specified agent's full LLM loop: the agent's internal LLM
        will analyze the query, call whatever internal tools it needs (data
        searches, workspace operations, service planning, etc.), and return
        a complete answer.

        This is the primary entry point for the orchestrator to delegate
        a user's question to an agent.

        Args:
            query: Natural language question or request for the agent.
            agent_type: Which agent to use: "data", "service", "workspace", "helpdesk", "analysis", or "planning".
            context: Optional JSON string with additional context
                     (conversation history, workspace items, etc.).
            token: Optional auth token override.

        Returns:
            Dict with keys:
              - answer: The agent's natural language response.
              - status: "completed", "max_iterations", "needs_input", or "error".
              - sources: List of BV-BRC collections/services referenced.
              - iterations_used: Number of LLM iterations used.
              - elapsed_seconds: Wall-clock time for the agent run.
              - tool_trace: List of tool calls the agent made internally.

            Additional keys by agent_type:
              - service: manifest, workflow_plan, question, workflow_id, persisted,
                         submission_id (GoWe), cwl_document (GoWe)
              - workspace: items, metadata, ui_grids, previews, paths_explored
              - analysis: output_files, metrics, previews, report_links, step_summaries
        """
        auth_token = _resolve_auth_token(token_provider, token)

        # Safe auth diagnostics (do not log the token value)
        if token:
            print(
                f"agent_chat: received token param (len={len(token)})",
                file=sys.stderr,
            )
        else:
            print("agent_chat: no token param provided", file=sys.stderr)

        if auth_token:
            _looks_like_patric = ("tokenid=" in auth_token) or ("un=" in auth_token)
            print(
                "agent_chat: resolved bvbrc_auth_token "
                f"(len={len(auth_token)}, looks_like_patric={_looks_like_patric})",
                file=sys.stderr,
            )
        else:
            print("agent_chat: resolved bvbrc_auth_token is None", file=sys.stderr)

        ctx = _parse_context(context)
        config_kwargs = _build_config_kwargs(auth_token, ctx)

        # Build progress callback that sends MCP progress notifications
        async def progress_callback(progress, total, message):
            if mcp_ctx is not None:
                await mcp_ctx.report_progress(progress, total, message)

        try:
            if agent_type == "data":
                return await _run_data_agent(
                    query, config_kwargs, ctx, progress_callback
                )
            elif agent_type == "service":
                return await _run_service_agent(
                    query, config_kwargs, ctx, progress_callback
                )
            elif agent_type == "workspace":
                return await _run_workspace_agent(
                    query, config_kwargs, ctx, progress_callback
                )
            elif agent_type == "helpdesk":
                return await _run_helpdesk_agent(
                    query, config_kwargs, ctx, progress_callback
                )
            elif agent_type == "analysis":
                return await _run_analysis_agent(
                    query, config_kwargs, ctx, progress_callback
                )
            elif agent_type == "planning":
                return await _run_planning_agent(
                    query, config_kwargs, ctx, progress_callback
                )
            else:
                return _error_response(f"Unknown agent type: {agent_type}")

        except Exception as e:
            traceback.print_exc(file=sys.stderr)
            return _error_response(_classify_agent_error(e, agent_type))
