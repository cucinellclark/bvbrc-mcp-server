"""MCP wrapper for the unified agent chat tool.

Exposes ``agent_chat`` as an MCP tool that delegates to
``shared.agent_dispatch.dispatch_agent``.  Registration is optional
(gated by ENABLE_AGENT_CHAT_MCP) -- Copilot uses in-process dispatch
and does not need this tool on the public MCP surface.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, Optional

from fastmcp import Context, FastMCP

# Ensure bvbrc-agents repo root is importable (shared.agent_dispatch).
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from shared.agent_dispatch import (  # noqa: E402
    dispatch_agent,
    _normalize_bvbrc_token,
    _parse_context,
)


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

        Runs the specified agent's full LLM loop and returns a complete
        answer.  Intended for debugging; Copilot uses in-process dispatch.

        Args:
            query: Natural language question or request for the agent.
            agent_type: Which agent to use: "data", "service", "workspace",
                        "helpdesk", "analysis", or "planning".
            context: Optional JSON string with additional context
                     (conversation history, workspace items, etc.).
            token: Optional auth token override.

        Returns:
            Dict with keys: answer, status, sources, iterations_used,
            elapsed_seconds, tool_trace, plus agent-specific fields.
        """
        resolved = _normalize_bvbrc_token(token)
        if not resolved and token_provider is not None:
            resolved = _normalize_bvbrc_token(token_provider.get_token(None))

        if token:
            print(
                f"agent_chat: received token param (len={len(token)})",
                file=sys.stderr,
            )
        else:
            print("agent_chat: no token param provided", file=sys.stderr)

        if resolved:
            _looks_like_patric = ("tokenid=" in resolved) or ("un=" in resolved)
            print(
                "agent_chat: resolved bvbrc_auth_token "
                f"(len={len(resolved)}, looks_like_patric={_looks_like_patric})",
                file=sys.stderr,
            )
        else:
            print("agent_chat: resolved bvbrc_auth_token is None", file=sys.stderr)

        ctx = _parse_context(context)
        # External MCP clients (ChatGPT / Claude) have their own tool-approval
        # UX and no Plan/Execute toggle, so they run in execute mode unless
        # the caller says otherwise.
        ctx.setdefault("execution_mode", "execute")

        async def progress_callback(progress, total, message):
            if mcp_ctx is not None:
                await mcp_ctx.report_progress(progress, total, message)

        return await dispatch_agent(
            agent_type=agent_type,
            query=query,
            context=ctx,
            token=resolved or "",
            progress_callback=progress_callback,
        )
