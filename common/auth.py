"""BV-BRC OAuth provider for the MCP server.

Implements the MCP OAuth spec (2025-06-18) using FastMCP's OAuthProvider base
class.  All RFC 8414 (Authorization Server Metadata) and RFC 9728 (Protected
Resource Metadata) routes are handled automatically by FastMCP.

The only custom endpoint is ``/login`` — the BV-BRC login page that
authenticates against the PATRIC user service and completes the OAuth
authorization code flow.

Usage::

    provider = BvbrcOAuthProvider(
        base_url="https://dev-7.bv-brc.org",
        authentication_url="https://user.patricbrc.org/authenticate",
    )
    mcp = FastMCP("BV-BRC MCP", auth=provider)
    # All OAuth routes are registered automatically.
    # Add the login page route:
    provider.register_login_route(mcp)
"""

from __future__ import annotations

import json
import os
import secrets
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlencode, urlparse

import requests as http_requests
from authlib.jose.errors import JoseError
from fastmcp import FastMCP
from fastmcp.server.auth.auth import OAuthProvider, AccessToken
from fastmcp.server.auth.jwt_issuer import JWTIssuer, derive_jwt_key
from mcp.server.auth.middleware.auth_context import AuthContextMiddleware
from mcp.server.auth.middleware.bearer_auth import BearerAuthBackend
from mcp.server.auth.provider import AuthorizationCode, RefreshToken, AuthorizationParams
from mcp.server.auth.routes import cors_middleware
from mcp.server.auth.settings import ClientRegistrationOptions
from mcp.shared.auth import OAuthClientInformationFull, OAuthToken
from pydantic import AnyHttpUrl
from starlette.middleware import Middleware
from starlette.middleware.authentication import AuthenticationMiddleware
from starlette.responses import HTMLResponse, JSONResponse, RedirectResponse
from starlette.routing import Route

from common.config import get_config

_config = get_config()
ACCESS_TOKEN_EXPIRES_IN = _config.oauth.access_token_expires_in_seconds
AUTH_CODE_EXPIRES_IN = _config.oauth.authorization_code_expires_in_seconds
ALLOWED_CALLBACK_URLS = _config.oauth.allowed_callback_urls
ALLOWED_CALLBACK_ORIGINS = _config.oauth.allowed_callback_origins
CLIENT_STORE_PATH = Path(_config.oauth.client_store_path)
if not CLIENT_STORE_PATH.is_absolute():
    CLIENT_STORE_PATH = Path(__file__).resolve().parent.parent / CLIENT_STORE_PATH

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _is_localhost_url(url: str) -> bool:
    """Check if a URL targets localhost."""
    try:
        parsed = urlparse(url)
        hostname = (parsed.hostname or "").lower()
        return hostname in ("localhost", "127.0.0.1", "::1") or hostname.startswith("127.")
    except Exception:
        return False


def _is_allowed_redirect_uri(redirect_uri: str) -> bool:
    """Check if a redirect_uri is in the configured allowlist or is localhost."""
    if redirect_uri in ALLOWED_CALLBACK_URLS:
        return True
    try:
        parsed = urlparse(redirect_uri)
        redirect_origin = f"{parsed.scheme}://{parsed.hostname}".lower()
        for origin in ALLOWED_CALLBACK_ORIGINS:
            allowed = urlparse(origin)
            if redirect_origin == f"{allowed.scheme}://{allowed.hostname}".lower():
                return True
    except Exception:
        pass
    return _is_localhost_url(redirect_uri)


# ---------------------------------------------------------------------------
# Request logging — tells us whether ChatGPT actually sent a Bearer token
# ---------------------------------------------------------------------------


class _AuthHeaderLogMiddleware:
    """Log Authorization presence on /mcp without printing the token."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            path = scope.get("path", "")
            if path == "/mcp" or path.startswith("/mcp"):
                headers = {
                    k.decode("latin-1").lower(): v.decode("latin-1")
                    for k, v in scope.get("headers", [])
                }
                auth = headers.get("authorization")
                method = scope.get("method", "?")
                if auth:
                    scheme, _, rest = auth.partition(" ")
                    print(
                        f"[OAUTH] {method} {path} Authorization: "
                        f"scheme={scheme!r} token_len={len(rest)}",
                        file=sys.stderr,
                    )
                else:
                    print(
                        f"[OAUTH] {method} {path} Authorization: MISSING "
                        f"header_names={sorted(headers.keys())}",
                        file=sys.stderr,
                    )
        await self.app(scope, receive, send)


# ---------------------------------------------------------------------------
# OAuth provider
# ---------------------------------------------------------------------------


class BvbrcOAuthProvider(OAuthProvider):
    """BV-BRC OAuth provider using PATRIC authentication.

    Extends FastMCP's ``OAuthProvider`` which automatically creates:
    - ``/.well-known/oauth-authorization-server`` (RFC 8414)
    - ``/.well-known/oauth-protected-resource/mcp`` (RFC 9728)
    - ``/authorize`` (OAuth authorization endpoint)
    - ``/token`` (OAuth token endpoint)
    - ``/register`` (RFC 7591 dynamic client registration)

    This class implements the provider hooks:
    - ``authorize()`` → redirects to our ``/login`` page
    - ``register_client()`` / ``get_client()`` → client store persisted to
      ``CLIENT_STORE_PATH`` so registrations survive restarts
    - ``load_authorization_code()`` / ``exchange_authorization_code()`` →
      auth code store backed by PATRIC tokens
    - ``load_access_token()`` → verifies opaque MCP tokens (mapped to PATRIC)
    """

    def __init__(
        self,
        *,
        base_url: str,
        authentication_url: str,
    ) -> None:
        # Trailing slashes make issuer/resource URLs disagree with ChatGPT's
        # resource indicator (https://host/mcp). Normalize once.
        base_url = str(base_url).rstrip("/")
        super().__init__(
            base_url=base_url,
            required_scopes=["profile", "token"],
            client_registration_options=ClientRegistrationOptions(
                enabled=True,
                valid_scopes=["profile", "token"],
                default_scopes=["profile", "token"],
            ),
        )
        self.authentication_url = authentication_url

        # Registered clients persist on disk. MCP clients (Claude Code,
        # ChatGPT, claude.ai) register once via /register and reuse that
        # client_id on every subsequent re-auth; if we forget it on restart
        # they get "Client ID '...' not found" from /authorize.
        self._clients: Dict[str, OAuthClientInformationFull] = self._load_clients()

        # In-memory stores (lost on restart — forces a re-login, not an error)
        self._auth_codes: Dict[str, Dict[str, Any]] = {}
        # JWT jti -> {patric_token, username, ...}
        self._issued_tokens: Dict[str, Dict[str, Any]] = {}

        # ChatGPT inspects access-token `aud` against the RFC 8707 resource
        # (https://host/mcp). Opaque tokens have no aud, so ChatGPT POSTs
        # /mcp without Authorization. Mint HS256 JWTs the same way FastMCP's
        # OAuthProxy does.
        issuer = str(self.base_url)  # AnyHttpUrl adds a trailing slash
        audience = f"{str(self.base_url).rstrip('/')}/mcp"
        signing_material = os.environ.get("MCP_JWT_SIGNING_KEY") or secrets.token_hex(32)
        self._jwt_issuer = JWTIssuer(
            issuer=issuer,
            audience=audience,
            signing_key=derive_jwt_key(
                high_entropy_material=signing_material,
                salt="bvbrc-mcp-jwt",
            ),
        )
        self._audience = audience
        print(f"[OAUTH] JWT issuer={issuer!r} audience={audience!r}", file=sys.stderr)

    # ------------------------------------------------------------------
    # Route overrides
    # ------------------------------------------------------------------

    def get_routes(self, mcp_path: str | None = None) -> list[Route]:
        """Get all routes with ChatGPT-compatible path layout.

        ChatGPT derives OAuth endpoint paths by prefixing the MCP server
        path (e.g. ``/mcp``) onto the standard OAuth paths.  So when the
        MCP server URL is ``https://host/mcp``, ChatGPT expects:

        - ``POST /mcp/register``   (not ``/register``)
        - ``GET  /mcp/authorize``  (not ``/authorize``)
        - ``POST /mcp/token``      (not ``/token``)

        FastMCP's ``OAuthProvider.get_routes()`` creates these at root
        (``/register``, etc.).  We duplicate them under ``mcp_path`` so
        both root and prefixed paths work.

        We also add a root-level PRM at
        ``/.well-known/oauth-protected-resource`` alongside the standard
        path-specific one at ``/.well-known/oauth-protected-resource/mcp``.
        """
        routes = super().get_routes(mcp_path)

        # ChatGPT probes OIDC discovery (/.well-known/openid-configuration) in
        # addition to RFC 8414 AS metadata. Serve the same document at both.
        for route in list(routes):
            if getattr(route, "path", None) == "/.well-known/oauth-authorization-server":
                routes.append(
                    Route(
                        "/.well-known/openid-configuration",
                        endpoint=route.endpoint,
                        methods=route.methods,
                    )
                )
                break

        if mcp_path:
            prefix = mcp_path.rstrip("/")

            # Duplicate operational OAuth routes under the MCP path prefix.
            # Keep the originals at root so both paths work.
            oauth_paths = {"/authorize", "/token", "/register"}
            for route in list(routes):
                if hasattr(route, "path") and route.path in oauth_paths:
                    routes.append(
                        Route(
                            f"{prefix}{route.path}",
                            endpoint=route.endpoint,
                            methods=route.methods,
                        )
                    )

            # Add root-level PRM at /.well-known/oauth-protected-resource
            if self.base_url:
                from mcp.server.auth.handlers.metadata import ProtectedResourceMetadataHandler
                from mcp.shared.auth import ProtectedResourceMetadata

                resource_url = self._get_resource_url(mcp_path)
                if resource_url:
                    root_metadata = ProtectedResourceMetadata(
                        resource=resource_url,
                        authorization_servers=[self.issuer_url],
                        scopes_supported=self.required_scopes,
                        resource_name="BV-BRC MCP Server",
                    )
                    handler = ProtectedResourceMetadataHandler(root_metadata)
                    routes.append(
                        Route(
                            "/.well-known/oauth-protected-resource",
                            endpoint=cors_middleware(handler.handle, ["GET", "OPTIONS"]),
                            methods=["GET", "OPTIONS"],
                        )
                    )

        return routes

    def get_middleware(self) -> list:
        """Auth middleware plus /mcp Authorization header diagnostics."""
        return [
            Middleware(_AuthHeaderLogMiddleware),
            Middleware(
                AuthenticationMiddleware,
                backend=BearerAuthBackend(self),
            ),
            Middleware(AuthContextMiddleware),
        ]

    # ------------------------------------------------------------------
    # OAuthAuthorizationServerProvider — client management
    # ------------------------------------------------------------------

    async def register_client(self, client_info: OAuthClientInformationFull) -> None:
        """Store a dynamically registered client and persist it to disk."""
        if client_info.client_id:
            self._clients[client_info.client_id] = client_info
            self._save_clients()
            print(
                f"[OAUTH] Registered client: {client_info.client_id} "
                f"({client_info.client_name or 'unnamed'})",
                file=sys.stderr,
            )

    def _load_clients(self) -> Dict[str, OAuthClientInformationFull]:
        """Load the persisted client registry (empty if missing/corrupt)."""
        if not CLIENT_STORE_PATH.exists():
            return {}
        try:
            raw = json.loads(CLIENT_STORE_PATH.read_text())
        except (OSError, ValueError) as e:
            print(f"[OAUTH] Could not read client store {CLIENT_STORE_PATH}: {e}", file=sys.stderr)
            return {}

        clients: Dict[str, OAuthClientInformationFull] = {}
        for client_id, data in raw.items():
            try:
                clients[client_id] = OAuthClientInformationFull.model_validate(data)
            except Exception as e:  # pydantic ValidationError
                print(f"[OAUTH] Skipping bad client record {client_id}: {e}", file=sys.stderr)
        print(f"[OAUTH] Loaded {len(clients)} registered client(s) from {CLIENT_STORE_PATH}", file=sys.stderr)
        return clients

    def _save_clients(self) -> None:
        """Atomically write the client registry to disk (owner-only perms)."""
        try:
            CLIENT_STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                cid: c.model_dump(mode="json", exclude_none=True)
                for cid, c in self._clients.items()
            }
            tmp = CLIENT_STORE_PATH.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(payload, indent=2))
            os.chmod(tmp, 0o600)  # records include client_secret
            os.replace(tmp, CLIENT_STORE_PATH)
        except OSError as e:
            print(f"[OAUTH] Could not write client store {CLIENT_STORE_PATH}: {e}", file=sys.stderr)

    async def get_client(self, client_id: str) -> OAuthClientInformationFull | None:
        """Look up a registered client by ID."""
        return self._clients.get(client_id)

    # ------------------------------------------------------------------
    # OAuthAuthorizationServerProvider — authorization
    # ------------------------------------------------------------------

    async def authorize(
        self,
        client: OAuthClientInformationFull,
        params: AuthorizationParams,
    ) -> str:
        """Return a URL to redirect the user to for authentication.

        The MCP SDK's ``/authorize`` handler validates all OAuth params
        (client_id, redirect_uri, PKCE, etc.) before calling this method.
        We just need to redirect to our login page with the validated params.
        """
        # Generate a unique key to store the authorization request
        request_id = secrets.token_urlsafe(32)

        # Store the validated authorization params for the login handler
        self._auth_codes[f"req_{request_id}"] = {
            "client_id": client.client_id,
            "redirect_uri": str(params.redirect_uri),
            "state": params.state,
            "code_challenge": params.code_challenge,
            "scopes": params.scopes or ["profile", "token"],
            "resource": params.resource,
            "redirect_uri_provided_explicitly": params.redirect_uri_provided_explicitly,
        }

        # Build the login page URL — served by our custom /login route
        base = str(self.base_url).rstrip("/")
        login_params = urlencode({"request_id": request_id})
        return f"{base}/login?{login_params}"

    async def load_authorization_code(
        self,
        client: OAuthClientInformationFull,
        authorization_code: str,
    ) -> AuthorizationCode | None:
        """Load a stored authorization code."""
        data = self._auth_codes.get(authorization_code)
        if not data:
            return None

        if data.get("used"):
            print(f"[OAUTH] Auth code already used: {authorization_code[:20]}...", file=sys.stderr)
            return None

        if data.get("expires_at", 0) < time.time():
            print(f"[OAUTH] Auth code expired: {authorization_code[:20]}...", file=sys.stderr)
            return None

        if data.get("client_id") != client.client_id:
            print(f"[OAUTH] Auth code client_id mismatch", file=sys.stderr)
            return None

        return AuthorizationCode(
            code=authorization_code,
            scopes=data.get("scopes", ["profile", "token"]),
            expires_at=data["expires_at"],
            client_id=data["client_id"],
            code_challenge=data["code_challenge"],
            redirect_uri=data["redirect_uri"],
            redirect_uri_provided_explicitly=data.get("redirect_uri_provided_explicitly", True),
            resource=data.get("resource"),
        )

    async def exchange_authorization_code(
        self,
        client: OAuthClientInformationFull,
        authorization_code: AuthorizationCode,
    ) -> OAuthToken:
        """Exchange an authorization code for an access token.

        The MCP SDK has already validated the code_verifier (PKCE) before
        calling this method. We mark the code as used, store the PATRIC
        token server-side, and return a JWT whose ``aud`` is the MCP resource URL.
        """
        data = self._auth_codes.get(authorization_code.code)
        if not data:
            raise ValueError("Authorization code not found")

        # Mark as used
        data["used"] = True

        user_token = data["user_token"]
        username = data.get("username")
        scopes = list(authorization_code.scopes)
        client_id = client.client_id or "bvbrc-client"

        # JWT with aud = MCP resource URL. ChatGPT echoes
        # resource=https://host/mcp and expects that value in the token.
        jti = secrets.token_urlsafe(32)
        access_token = self._jwt_issuer.issue_access_token(
            client_id=client_id,
            scopes=scopes,
            jti=jti,
            expires_in=ACCESS_TOKEN_EXPIRES_IN,
        )
        expires_at = int(time.time()) + ACCESS_TOKEN_EXPIRES_IN
        self._issued_tokens[jti] = {
            "username": username,
            "patric_token": user_token,
            "client_id": client_id,
            "scopes": scopes,
            "issued_at": time.time(),
            "expires_at": expires_at,
            "resource": authorization_code.resource or self._audience,
        }

        print(
            f"[OAUTH] Token exchange successful for user: {username} "
            f"(jwt jti={jti[:8]}... aud={self._audience})",
            file=sys.stderr,
        )

        return OAuthToken(
            access_token=access_token,
            token_type="Bearer",
            expires_in=ACCESS_TOKEN_EXPIRES_IN,
            scope=" ".join(scopes),
        )

    # ------------------------------------------------------------------
    # OAuthAuthorizationServerProvider — token verification
    # ------------------------------------------------------------------

    async def load_access_token(self, token: str) -> AccessToken | None:
        """Verify a bearer token.

        Accepts:
        1. JWTs we issued (preferred — ChatGPT requires aud = resource URL)
        2. Opaque tokens leftover from earlier builds
        3. Raw PATRIC tokens (un=...|tokenid=...) for direct API use
        """
        if not token or not isinstance(token, str):
            print("[OAUTH] Token verification failed: empty token", file=sys.stderr)
            return None

        token = token.strip()
        now = int(time.time())

        jwt_access = self._access_from_jwt(token, now)
        if jwt_access is not None:
            return jwt_access

        token_info = self._issued_tokens.get(token)
        if token_info:
            expires_at = token_info.get("expires_at")
            if expires_at and expires_at < now:
                print(
                    f"[OAUTH] Issued token expired for {token_info.get('username')}",
                    file=sys.stderr,
                )
                return None
            print(
                f"[OAUTH] Opaque token verified for user: {token_info.get('username')}",
                file=sys.stderr,
            )
            return AccessToken(
                token=token,
                client_id=token_info.get("client_id") or "bvbrc-client",
                scopes=token_info.get("scopes") or ["profile", "token"],
                expires_at=expires_at or (now + ACCESS_TOKEN_EXPIRES_IN),
                resource=token_info.get("resource") or self._audience,
            )

        patric_user = self._parse_patric_username(token)
        if patric_user:
            print(
                f"[OAUTH] PATRIC token accepted for user: {patric_user}",
                file=sys.stderr,
            )
            return AccessToken(
                token=token,
                client_id="bvbrc-client",
                scopes=["profile", "token"],
                expires_at=now + ACCESS_TOKEN_EXPIRES_IN,
                resource=self._audience,
            )

        charset_hint = []
        if "|" in token:
            charset_hint.append("contains '|' (PATRIC-like)")
        if "%" in token:
            charset_hint.append("percent-encoded")
        if token.count(".") == 2:
            charset_hint.append("jwt-shaped")
        hint = ", ".join(charset_hint) or "opaque/unknown"
        print(
            f"[OAUTH] Token verification failed: len={len(token)} "
            f"prefix={token[:12]!r} ({hint}) store_size={len(self._issued_tokens)}",
            file=sys.stderr,
        )
        return None

    def _access_from_jwt(self, token: str, now: int) -> AccessToken | None:
        if token.count(".") != 2:
            return None
        try:
            payload = self._jwt_issuer.verify_token(token)
        except JoseError as e:
            print(f"[OAUTH] JWT verification failed: {e}", file=sys.stderr)
            return None
        except Exception as e:
            print(f"[OAUTH] JWT verification error: {e}", file=sys.stderr)
            return None

        jti = payload.get("jti")
        token_info = self._issued_tokens.get(jti) if jti else None
        if not token_info:
            print(
                f"[OAUTH] JWT valid but jti not in store (jti={str(jti)[:8] if jti else None})",
                file=sys.stderr,
            )
            return None

        expires_at = token_info.get("expires_at")
        if expires_at and expires_at < now:
            print(
                f"[OAUTH] JWT expired for {token_info.get('username')}",
                file=sys.stderr,
            )
            return None

        scope_claim = payload.get("scope") or ""
        scopes = token_info.get("scopes") or (
            scope_claim.split() if scope_claim else ["profile", "token"]
        )
        print(
            f"[OAUTH] JWT verified for user: {token_info.get('username')}",
            file=sys.stderr,
        )
        return AccessToken(
            token=token,
            client_id=token_info.get("client_id") or payload.get("client_id") or "bvbrc-client",
            scopes=scopes,
            expires_at=expires_at or (now + ACCESS_TOKEN_EXPIRES_IN),
            resource=token_info.get("resource") or self._audience,
            claims=dict(payload),
        )

    def resolve_patric_token(self, bearer_token: str | None) -> str | None:
        """Map an MCP bearer token to the underlying PATRIC token for API calls."""
        if not bearer_token:
            return None
        token = bearer_token.strip()

        if token.count(".") == 2:
            try:
                payload = self._jwt_issuer.verify_token(token)
                jti = payload.get("jti")
                info = self._issued_tokens.get(jti) if jti else None
                if info:
                    return info.get("patric_token")
            except Exception:
                pass

        info = self._issued_tokens.get(token)
        if info:
            return info.get("patric_token") or token
        if self._parse_patric_username(token):
            return token
        return None

    @staticmethod
    def _parse_patric_username(token: str) -> str | None:
        """Return username if token looks like a live PATRIC token, else None."""
        if "un=" not in token or "|tokenid=" not in token:
            return None
        try:
            username = None
            expiry = None
            for part in token.split("|"):
                if part.startswith("un="):
                    username = part[3:]
                elif part.startswith("expiry="):
                    try:
                        expiry = int(part[7:])
                    except ValueError:
                        return None
            if not username:
                return None
            if expiry and expiry < int(time.time()):
                print(f"[OAUTH] PATRIC token expired for {username}", file=sys.stderr)
                return None
            return username
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Refresh tokens / revocation — not implemented
    # ------------------------------------------------------------------

    async def load_refresh_token(
        self,
        client: OAuthClientInformationFull,
        refresh_token: str,
    ) -> RefreshToken | None:
        return None

    async def exchange_refresh_token(
        self,
        client: OAuthClientInformationFull,
        refresh_token: RefreshToken,
        scopes: list[str],
    ) -> OAuthToken:
        raise NotImplementedError("Refresh tokens not supported")

    async def revoke_token(
        self,
        token: AccessToken | RefreshToken,
    ) -> None:
        # Remove from issued tokens if present
        if isinstance(token, AccessToken) and token.token in self._issued_tokens:
            del self._issued_tokens[token.token]

    # ------------------------------------------------------------------
    # Login page — registered as a custom route on the MCP server
    # ------------------------------------------------------------------

    def register_login_route(self, mcp: FastMCP) -> None:
        """Register the ``/login`` route on the FastMCP server.

        Registers at both ``/login`` and ``/mcp/login`` so the login
        page works regardless of which authorize path was used.
        """

        provider = self

        async def _login_handler(request):
            if request.method == "GET":
                return await provider._handle_login_get(request)
            else:
                return await provider._handle_login_post(request)

        @mcp.custom_route("/login", methods=["GET", "POST"])
        async def login_page(request):
            return await _login_handler(request)

        @mcp.custom_route("/mcp/login", methods=["GET", "POST"])
        async def login_page_mcp(request):
            return await _login_handler(request)

    async def _handle_login_get(self, request, error_message: str = "") -> HTMLResponse:
        """Render the BV-BRC login page."""
        # request_id comes from query params (GET) or form data (POST error re-render)
        request_id = request.query_params.get("request_id", "")
        if not request_id and request.method == "POST":
            try:
                form = await request.form()
                request_id = form.get("request_id", "")
            except Exception:
                pass
        req_data = self._auth_codes.get(f"req_{request_id}", {})

        # Use the current request path as the form action so the POST
        # goes back to the same route (/login or /mcp/login).
        form_action = request.url.path
        client_id = req_data.get("client_id", "Unknown")

        # Look up client name
        client = self._clients.get(client_id)
        client_name = (client.client_name if client else None) or client_id
        scopes = " ".join(req_data.get("scopes", ["profile", "token"]))

        # Load logo
        logo_b64 = ""
        try:
            with open("images/bvbrc_logo_base64.txt", "r") as f:
                logo_b64 = f.read().strip()
        except Exception:
            pass

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>BV-BRC Login</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #f5f5f5;
            display: flex; justify-content: center; align-items: center;
            min-height: 100vh; margin: 0; padding: 20px;
        }}
        .login-container {{
            background: white; border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            padding: 40px; width: 100%; max-width: 420px;
        }}
        .logo {{ text-align: center; margin-bottom: 30px; }}
        .logo img {{ max-width: 315px; height: auto; display: block; margin: 0 auto; }}
        h1 {{ color: #333; margin: 0 0 10px; font-size: 22px; text-align: center; }}
        .subtitle {{ color: #666; text-align: center; margin-bottom: 30px; font-size: 14px; }}
        .client-info {{
            background: #f5f5f5; border-left: 4px solid #00567A;
            padding: 12px; margin-bottom: 25px; border-radius: 4px;
        }}
        .client-info p {{ margin: 5px 0; font-size: 13px; color: #555; }}
        .client-info strong {{ color: #333; }}
        .form-group {{ margin-bottom: 20px; }}
        label {{ display: block; margin-bottom: 8px; color: #333; font-weight: 500; font-size: 14px; }}
        input[type="text"], input[type="password"] {{
            width: 100%; padding: 12px; border: 1px solid #d0d0d0;
            border-radius: 6px; font-size: 14px; box-sizing: border-box;
        }}
        input:focus {{ outline: none; border-color: #00567A; }}
        button {{
            width: 100%; padding: 14px; background: #00567A; color: white;
            border: none; border-radius: 6px; font-size: 16px; font-weight: 600;
            cursor: pointer;
        }}
        button:hover {{ background: #004560; }}
        .error {{
            background: #fee; border-left: 4px solid #d32f2f;
            padding: 12px; margin-bottom: 20px; border-radius: 4px;
            color: #c33; font-size: 14px; display: none;
        }}
        .info {{ text-align: center; margin-top: 20px; font-size: 12px; color: #666; }}
    </style>
</head>
<body>
    <div class="login-container">
        <div class="logo">
            {"<img src='" + logo_b64 + "' alt='BV-BRC Logo' />" if logo_b64 else "<h2>BV-BRC</h2>"}
        </div>
        <h1>Login</h1>
        <p class="subtitle">Authorize access to your BV-BRC MCP Resources</p>
        <div class="client-info">
            <p><strong>Application:</strong> {client_name}</p>
            <p><strong>Scopes:</strong> {scopes}</p>
        </div>
        {f'<div class="error" style="display:block">{error_message}</div>' if error_message else ''}
        <form method="POST" action="{form_action}">
            <input type="hidden" name="request_id" value="{request_id}">
            <div class="form-group">
                <label for="username">Username</label>
                <input type="text" id="username" name="username" required autofocus
                       placeholder="Enter your BV-BRC username">
            </div>
            <div class="form-group">
                <label for="password">Password</label>
                <input type="password" id="password" name="password" required
                       placeholder="Enter your password">
            </div>
            <button type="submit">Login &amp; Authorize</button>
        </form>
        <p class="info">
            By logging in, you authorize this application to access
            your BV-BRC MCP Resources on your behalf.
        </p>
    </div>
</body>
</html>"""
        return HTMLResponse(content=html)

    async def _handle_login_post(self, request) -> RedirectResponse | HTMLResponse:
        """Handle login form submission: authenticate and redirect with auth code."""
        try:
            form = await request.form()
            username = form.get("username")
            password = form.get("password")
            request_id = form.get("request_id", "")

            if not username or not password:
                return await self._handle_login_get(request, error_message="Username and password are required")

            # Look up the authorization request
            req_key = f"req_{request_id}"
            req_data = self._auth_codes.get(req_key)
            if not req_data:
                return JSONResponse(
                    {"error": "invalid_request", "error_description": "Authorization request not found or expired"},
                    status_code=400,
                )

            # Authenticate against PATRIC
            try:
                response = http_requests.post(
                    self.authentication_url,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    data={"username": username, "password": password},
                    timeout=30,
                )
                if response.status_code != 200:
                    return await self._handle_login_get(
                        request, error_message="Invalid username or password"
                    )
                user_token = response.text.strip()
                if not user_token:
                    raise ValueError("Empty token from auth endpoint")
            except http_requests.RequestException as e:
                print(f"[OAUTH] Auth request failed: {e}", file=sys.stderr)
                return await self._handle_login_get(
                    request, error_message="Authentication service unavailable. Please try again."
                )

            print(f"[OAUTH] Login successful for user: {username}", file=sys.stderr)

            # Generate authorization code
            auth_code = secrets.token_urlsafe(32)
            self._auth_codes[auth_code] = {
                "client_id": req_data["client_id"],
                "redirect_uri": req_data["redirect_uri"],
                "code_challenge": req_data["code_challenge"],
                "scopes": req_data.get("scopes", ["profile", "token"]),
                "resource": req_data.get("resource"),
                "redirect_uri_provided_explicitly": req_data.get("redirect_uri_provided_explicitly", True),
                "user_token": user_token,
                "username": username,
                "expires_at": time.time() + AUTH_CODE_EXPIRES_IN,
                "used": False,
            }

            # Clean up the request entry
            del self._auth_codes[req_key]

            # Redirect back to the client with the authorization code
            redirect_uri = req_data["redirect_uri"]
            params: dict[str, str] = {"code": auth_code}
            if req_data.get("state"):
                params["state"] = req_data["state"]

            redirect_url = f"{redirect_uri}?{urlencode(params)}"
            print(f"[OAUTH] Redirecting to client: {redirect_url[:80]}...", file=sys.stderr)

            return RedirectResponse(url=redirect_url, status_code=302)

        except Exception as e:
            print(f"[OAUTH] Login error: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc(file=sys.stderr)
            return await self._handle_login_get(
                request, error_message="An unexpected error occurred. Please try again."
            )
