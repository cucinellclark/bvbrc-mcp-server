#!/usr/bin/env python3
"""
BVBRC Consolidated MCP Server

Exposes unified agent tools (search, workspace, GoWe, etc.) to external
MCP clients (ChatGPT, Claude, Cursor). Copilot does not use this process;
the orchestrator dispatches agents in-process.

OAuth is handled by FastMCP's OAuthProvider infrastructure — all discovery
and token endpoints are created automatically. The only custom route is
``/login`` for the BV-BRC login page.

Set ENABLE_AGENT_CHAT_MCP=1 to also register the agent_chat tool (debug only).
"""

from fastmcp import FastMCP
from tools.data_tools import request_download_cancel
from tools.unified_tools import register_unified_tools
from common.token_provider import TokenProvider
from starlette.responses import JSONResponse
import os
import sys
import logging
import logging.handlers
from pathlib import Path
from common.auth import BvbrcOAuthProvider
from common.config import get_config

# ── Centralized file logging for MCP server ──────────────────────────────
_MCP_LOG_DIR = Path(__file__).resolve().parent.parent.parent.parent / "DevEnvironment" / "logs" / "agents"
_MCP_LOG_DIR.mkdir(parents=True, exist_ok=True)

_mcp_formatter = logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s")

# Root logger: DEBUG to file, INFO to stderr
_root = logging.getLogger()
_root.setLevel(logging.DEBUG)

_file_handler = logging.handlers.RotatingFileHandler(
    _MCP_LOG_DIR / "mcp-server.log",
    maxBytes=10 * 1024 * 1024,
    backupCount=5,
    encoding="utf-8",
)
_file_handler.setLevel(logging.DEBUG)
_file_handler.setFormatter(_mcp_formatter)
_root.addHandler(_file_handler)

_console_handler = logging.StreamHandler(sys.stderr)
_console_handler.setLevel(logging.INFO)
_console_handler.setFormatter(_mcp_formatter)
_root.addHandler(_console_handler)

_mcp_logger = logging.getLogger("mcp_server")

# ── Configuration ────────────────────────────────────────────────────────
config = get_config()

# The public URL where this MCP server is reachable by external clients.
# All OAuth metadata URLs are derived from this single value.
#   - PUBLIC_BASE_URL env var takes priority (for temp / dev deployments)
#   - Falls back to config.server_url (which reads openid_config_url)
server_url = (os.environ.get("PUBLIC_BASE_URL") or config.server_url).rstrip("/")
authentication_url = config.authentication_url
port = config.port
mcp_url = config.mcp_url

# ── OAuth provider ───────────────────────────────────────────────────────
# BvbrcOAuthProvider extends FastMCP's OAuthProvider which automatically
# creates all required OAuth/discovery routes:
#   /.well-known/oauth-authorization-server  (RFC 8414)
#   /.well-known/oauth-protected-resource/*  (RFC 9728)
#   /authorize  (OAuth authorization endpoint)
#   /token      (OAuth token endpoint)
#   /register   (RFC 7591 dynamic client registration)
oauth = BvbrcOAuthProvider(
    base_url=server_url,
    authentication_url=authentication_url,
)

# Token provider maps MCP bearer tokens back to PATRIC tokens for API calls
token_provider = TokenProvider(mode="http", oauth_provider=oauth)

# Create FastMCP server with OAuth
mcp = FastMCP("BVBRC Consolidated MCP Server", auth=oauth)

# Register the /login page route (the one thing OAuthProvider doesn't handle)
oauth.register_login_route(mcp)

# ── Tools ────────────────────────────────────────────────────────────────
print("Registering unified agent tools...", file=sys.stderr)
register_unified_tools(mcp, token_provider)

if os.environ.get("ENABLE_AGENT_CHAT_MCP", "").lower() not in ("", "0", "false", "no"):
    from tools.agent_chat_tool import register_agent_chat_tool

    print("Registering agent chat tool (ENABLE_AGENT_CHAT_MCP)...", file=sys.stderr)
    register_agent_chat_tool(mcp, token_provider)

# ── Custom routes ────────────────────────────────────────────────────────

@mcp.custom_route("/mcp/cancel-data-download", methods=["POST"])
async def cancel_data_download_route(request) -> JSONResponse:
    """Cancel an in-flight data download by cooperative cancel token."""
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    cancel_token = str((payload or {}).get("cancel_token", "")).strip()
    if not cancel_token:
        return JSONResponse(
            {"ok": False, "error": "cancel_token is required"},
            status_code=400,
        )

    accepted = request_download_cancel(cancel_token)
    return JSONResponse(
        {
            "ok": bool(accepted),
            "cancel_token": cancel_token,
            "message": "Cancellation requested" if accepted else "Cancellation token rejected",
        },
        status_code=200 if accepted else 400,
    )


# ── Main ─────────────────────────────────────────────────────────────────

def main() -> int:
    print(f"Starting BVBRC Consolidated MCP Server on port {port}...", file=sys.stderr)
    print(f"  - Public base URL: {server_url}", file=sys.stderr)
    print(f"  - Bind address: {mcp_url}:{port}", file=sys.stderr)
    print("  - Unified agent_* tools: enabled", file=sys.stderr)
    print(
        f"  - agent_chat: {'enabled' if os.environ.get('ENABLE_AGENT_CHAT_MCP') else 'disabled'}",
        file=sys.stderr,
    )

    try:
        mcp.run(transport="http", host=mcp_url, port=port)
    except KeyboardInterrupt:
        print("Server stopped.", file=sys.stderr)
    except Exception as e:
        print(f"Server error: {e}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    main()
