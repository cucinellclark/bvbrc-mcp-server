import json
import secrets
import time
import requests
import hashlib
import base64
from uuid import uuid4
from typing import Any, Dict
from urllib.parse import urlencode
from starlette.responses import JSONResponse, HTMLResponse, RedirectResponse

# Temporary in-memory storage for registered OAuth2 clients
# Key: client_id, Value: client information dict
registered_clients = {}

# Temporary in-memory storage for authorization codes
# Key: authorization_code, Value: dict with client_id, redirect_uri, code_challenge, expires_at, user_info
authorization_codes = {}

# Whitelisted callback URLs
ALLOWED_CALLBACK_URLS = [
    "https://chatgpt.com/connector_platform_oauth_redirect"
]

def get_registered_client(client_id: str) -> dict | None:
    """
    Retrieve a registered client by client_id.
    Returns None if client not found.
    """
    return registered_clients.get(client_id)

def openid_configuration(request, openid_config_url: str) -> JSONResponse:
    """
    Serves the OIDC discovery document that ChatGPT expects.
    """
    print("Query params:", dict(request.query_params))
    print("Request path:", request.url.path)
    config = {
            "issuer": "https://www.bv-brc.org",
            "authorization_endpoint": f"{openid_config_url}/oauth2/authorize",
            "token_endpoint": f"{openid_config_url}/oauth2/token",
            "registration_endpoint": f"{openid_config_url}/oauth2/register", # 1
            "response_types_supported": ["code"],
            "grant_types_supported": ["authorization_code"],
            "token_endpoint_auth_methods_supported": ["none", "client_secret_post"],
            "code_challenge_methods_supported": ["S256"],
            "scopes_supported": ["profile", "token"],
            "claims_supported": ["sub", "api_token"]
    }
    return JSONResponse(content=config)

async def oauth2_register(request) -> JSONResponse:
    """
    Registers a new client with the OAuth2 server.
    Implements RFC 7591 OAuth 2.0 Dynamic Client Registration.
    """
    try:
        # Parse request body
        body = await request.json()
        print("Registration request body:", body)
        
        # Validate required fields
        if "redirect_uris" not in body or not body["redirect_uris"]:
            return JSONResponse(
                content={
                    "error": "invalid_client_metadata",
                    "error_description": "redirect_uris is required and must not be empty"
                },
                status_code=400
            )
        
        # Generate unique client_id
        client_id = str(uuid4())
        
        # Get auth method from request
        token_endpoint_auth_method = body.get("token_endpoint_auth_method", "none")
        
        # Generate client_secret if needed (not needed for "none" auth method)
        client_secret = None
        if token_endpoint_auth_method != "none":
            client_secret = secrets.token_hex(32)
        
        # Set timestamps
        client_id_issued_at = int(time.time())
        
        # Build response with client information from request
        response = {
            "client_id": client_id,
            "client_id_issued_at": client_id_issued_at,
            "redirect_uris": body["redirect_uris"],
            "token_endpoint_auth_method": token_endpoint_auth_method,
            "grant_types": body.get("grant_types", ["authorization_code", "refresh_token"]),
            "response_types": body.get("response_types", ["code"]),
        }
        
        # Add client_secret if generated
        if client_secret:
            response["client_secret"] = client_secret
            response["client_secret_expires_at"] = 0  # 0 means never expires
        
        # Add optional fields if provided in request
        optional_fields = [
            "client_name", "scope", "client_uri", "logo_uri", 
            "contacts", "tos_uri", "policy_uri", "jwks_uri", 
            "jwks", "software_id", "software_version"
        ]
        for field in optional_fields:
            if field in body:
                response[field] = body[field]
        
        # Store client information for later retrieval during authorization
        registered_clients[client_id] = response
        
        print(f"Registered new client: {client_id} ({body.get('client_name', 'unnamed')})")
        print(f"Total registered clients: {len(registered_clients)}")
        
        # Return 201 Created with client information
        return JSONResponse(content=response, status_code=201)
        
    except Exception as e:
        print(f"Registration error: {e}")
        return JSONResponse(
            content={
                "error": "server_error",
                "error_description": str(e)
            },
            status_code=500
        )

async def oauth2_authorize(request, authentication_url: str):
    """
    Authorization endpoint - displays login page for user authentication.
    This is where ChatGPT redirects the user to log in.
    """
    print("Authorization request received")
    print("Query params:", dict(request.query_params))
    
    # Extract OAuth2 parameters
    client_id = request.query_params.get("client_id")
    redirect_uri = request.query_params.get("redirect_uri")
    response_type = request.query_params.get("response_type")
    state = request.query_params.get("state")
    code_challenge = request.query_params.get("code_challenge")
    code_challenge_method = request.query_params.get("code_challenge_method")
    scope = request.query_params.get("scope", "")
    
    # Validate required parameters
    if not client_id:
        return JSONResponse(
            content={"error": "invalid_request", "error_description": "client_id is required"},
            status_code=400
        )
    
    if not redirect_uri:
        return JSONResponse(
            content={"error": "invalid_request", "error_description": "redirect_uri is required"},
            status_code=400
        )
    
    if not response_type or response_type != "code":
        return JSONResponse(
            content={"error": "unsupported_response_type", "error_description": "Only 'code' response type is supported"},
            status_code=400
        )
    
    # Validate client exists
    client = get_registered_client(client_id)
    if not client:
        return JSONResponse(
            content={"error": "invalid_client", "error_description": "Client not found"},
            status_code=400
        )
    
    # Validate redirect_uri is whitelisted
    if redirect_uri not in ALLOWED_CALLBACK_URLS:
        return JSONResponse(
            content={
                "error": "invalid_request",
                "error_description": f"redirect_uri '{redirect_uri}' is not whitelisted. Allowed: {ALLOWED_CALLBACK_URLS}"
            },
            status_code=400
        )
    
    # Validate redirect_uri matches registered URIs
    if redirect_uri not in client.get("redirect_uris", []):
        return JSONResponse(
            content={"error": "invalid_request", "error_description": "redirect_uri does not match registered URIs"},
            status_code=400
        )
    
    print(f"Authorizing client: {client.get('client_name', client_id)}")
    print(f"Redirect URI: {redirect_uri}")
    print(f"Code challenge: {code_challenge}")
    
    # Serve login page
    login_html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>BV-BRC Login</title>
        <style>
            body {{
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                display: flex;
                justify-content: center;
                align-items: center;
                min-height: 100vh;
                margin: 0;
                padding: 20px;
            }}
            .login-container {{
                background: white;
                border-radius: 12px;
                box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
                padding: 40px;
                width: 100%;
                max-width: 400px;
            }}
            h1 {{
                color: #333;
                margin: 0 0 10px 0;
                font-size: 24px;
                text-align: center;
            }}
            .subtitle {{
                color: #666;
                text-align: center;
                margin-bottom: 30px;
                font-size: 14px;
            }}
            .client-info {{
                background: #f7f7f7;
                border-left: 4px solid #667eea;
                padding: 12px;
                margin-bottom: 25px;
                border-radius: 4px;
            }}
            .client-info p {{
                margin: 5px 0;
                font-size: 13px;
                color: #555;
            }}
            .client-info strong {{
                color: #333;
            }}
            .form-group {{
                margin-bottom: 20px;
            }}
            label {{
                display: block;
                margin-bottom: 8px;
                color: #333;
                font-weight: 500;
                font-size: 14px;
            }}
            input[type="text"],
            input[type="password"] {{
                width: 100%;
                padding: 12px;
                border: 2px solid #e0e0e0;
                border-radius: 6px;
                font-size: 14px;
                box-sizing: border-box;
                transition: border-color 0.3s;
            }}
            input[type="text"]:focus,
            input[type="password"]:focus {{
                outline: none;
                border-color: #667eea;
            }}
            button {{
                width: 100%;
                padding: 14px;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                border: none;
                border-radius: 6px;
                font-size: 16px;
                font-weight: 600;
                cursor: pointer;
                transition: transform 0.2s, box-shadow 0.2s;
            }}
            button:hover {{
                transform: translateY(-2px);
                box-shadow: 0 5px 15px rgba(102, 126, 234, 0.4);
            }}
            button:active {{
                transform: translateY(0);
            }}
            .error {{
                background: #fee;
                border-left: 4px solid #f44;
                padding: 12px;
                margin-bottom: 20px;
                border-radius: 4px;
                color: #c33;
                font-size: 14px;
                display: none;
            }}
            .info {{
                text-align: center;
                margin-top: 20px;
                font-size: 12px;
                color: #666;
            }}
        </style>
    </head>
    <body>
        <div class="login-container">
            <h1>🔐 BV-BRC Login</h1>
            <p class="subtitle">Authorize access to your BV-BRC workspace</p>
            
            <div class="client-info">
                <p><strong>Application:</strong> {client.get('client_name', 'ChatGPT')}</p>
                <p><strong>Scopes:</strong> {scope or 'profile, token'}</p>
            </div>
            
            <div id="error-message" class="error"></div>
            
            <form id="login-form" method="POST" action="/oauth2/login">
                <input type="hidden" name="client_id" value="{client_id}">
                <input type="hidden" name="redirect_uri" value="{redirect_uri}">
                <input type="hidden" name="state" value="{state or ''}">
                <input type="hidden" name="code_challenge" value="{code_challenge or ''}">
                <input type="hidden" name="code_challenge_method" value="{code_challenge_method or ''}">
                <input type="hidden" name="scope" value="{scope}">
                
                <div class="form-group">
                    <label for="username">Username</label>
                    <input type="text" id="username" name="username" required autofocus placeholder="Enter your BV-BRC username">
                </div>
                
                <div class="form-group">
                    <label for="password">Password</label>
                    <input type="password" id="password" name="password" required placeholder="Enter your password">
                </div>
                
                <button type="submit">Login & Authorize</button>
            </form>
            
            <p class="info">
                By logging in, you authorize this application to access your BV-BRC workspace on your behalf.
            </p>
        </div>
        
        <script>
            document.getElementById('login-form').addEventListener('submit', async (e) => {{
                e.preventDefault();
                const form = e.target;
                const formData = new FormData(form);
                const errorDiv = document.getElementById('error-message');
                
                try {{
                    const response = await fetch(form.action, {{
                        method: 'POST',
                        body: formData
                    }});
                    
                    if (response.redirected) {{
                        window.location.href = response.url;
                        return;
                    }}
                    
                    const data = await response.json();
                    
                    if (response.ok) {{
                        // Should not reach here as success should redirect
                        window.location.href = data.redirect_url;
                    }} else {{
                        errorDiv.textContent = data.error_description || data.error || 'Login failed';
                        errorDiv.style.display = 'block';
                    }}
                }} catch (error) {{
                    errorDiv.textContent = 'Network error. Please try again. ' + (error && error.message ? error.message : error);
                    errorDiv.style.display = 'block';
                }}
        </script>
    </body>
    </html>
    """
    
    return HTMLResponse(content=login_html)

async def oauth2_login(request, authentication_url: str):
    """
    Handles the login form submission.
    Authenticates the user and generates an authorization code.
    Redirects back to ChatGPT's callback URL with the code.
    """
    try:
        # Parse form data
        form = await request.form()
        username = form.get("username")
        password = form.get("password")
        client_id = form.get("client_id")
        redirect_uri = form.get("redirect_uri")
        state = form.get("state")
        code_challenge = form.get("code_challenge")
        code_challenge_method = form.get("code_challenge_method")
        scope = form.get("scope")
        
        print(f"Login attempt for user: {username}")
        
        # Validate required fields
        if not username or not password:
            return JSONResponse(
                content={"error": "invalid_request", "error_description": "Username and password are required"},
                status_code=400
            )
        
        # Authenticate user with BV-BRC API
        # POST request to authentication endpoint
        try:
            response = requests.post(
                authentication_url,
                headers={
                    'Content-Type': 'application/x-www-form-urlencoded',
                },
                data={
                    'username': username,
                    'password': password
                },
                timeout=30
            )
            
            # Check if authentication was successful
            if response.status_code != 200:
                print(f"Authentication failed for user {username}: HTTP {response.status_code}")
                return JSONResponse(
                    content={"error": "access_denied", "error_description": "Invalid username or password"},
                    status_code=401
                )
            
            # Parse the response to get the token
            # The token should be in the response body
            user_token = response.text.strip()
            
            if not user_token:
                raise ValueError("No token received from authentication endpoint")
            
            print(f"Login successful for user: {username}")
            print(f"Token received (first 20 chars): {user_token[:20]}...")
                
        except requests.RequestException as e:
            print(f"Authentication request failed for user {username}: {e}")
            return JSONResponse(
                content={"error": "server_error", "error_description": "Authentication service unavailable"},
                status_code=503
            )
        except Exception as e:
            print(f"Authentication failed for user {username}: {e}")
            return JSONResponse(
                content={"error": "access_denied", "error_description": "Authentication failed"},
                status_code=401
            )
        
        # Generate authorization code (one-time use, short-lived)
        auth_code = secrets.token_urlsafe(32)
        
        # Store authorization code with associated data
        # Code expires in 10 minutes
        authorization_codes[auth_code] = {
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "code_challenge": code_challenge,
            "code_challenge_method": code_challenge_method,
            "scope": scope,
            "user_token": user_token,
            "username": username,
            "expires_at": time.time() + 600,  # 10 minutes
            "used": False
        }
        
        print(f"Generated authorization code for user {username}")
        print(f"Total active auth codes: {len(authorization_codes)}")
        
        # Build redirect URL with authorization code
        params = {
            "code": auth_code
        }
        if state:
            params["state"] = state
        
        redirect_url = f"{redirect_uri}?{urlencode(params)}"
        
        print(f"Redirecting to: {redirect_url}")
        
        # Redirect user back to ChatGPT with the authorization code
        return RedirectResponse(url=redirect_url, status_code=302)
        
    except Exception as e:
        print(f"Login error: {e}")
        return JSONResponse(
            content={
                "error": "server_error",
                "error_description": str(e)
            },
            status_code=500
        )

async def oauth2_token(request):
    """
    Handles the token request.
    Exchanges an authorization code for an access token.
    Retrieves the stored user token using the authorization code.
    """
    try:
        # Parse form data
        form = await request.form()
        code = form.get("code")
        client_id = form.get("client_id")
        client_secret = form.get("client_secret")
        redirect_uri = form.get("redirect_uri")
        grant_type = form.get("grant_type")
        code_verifier = form.get("code_verifier")  # For PKCE
        
        print(f"Token request received for code: {code[:20] if code else None}...")
        print(f"Client ID: {client_id}")

        if not code:
            return JSONResponse(
                content={"error": "invalid_request", "error_description": "code is required"},
                status_code=400
            )
        
        if not client_id:
            return JSONResponse(
                content={"error": "invalid_request", "error_description": "client_id is required"},
                status_code=400
            )
        
        if not redirect_uri:
            return JSONResponse(
                content={"error": "invalid_request", "error_description": "redirect_uri is required"},
                status_code=400
            )
        
        if not grant_type:
            return JSONResponse(
                content={"error": "invalid_request", "error_description": "grant_type is required"},
                status_code=400
            )
        
        if grant_type != "authorization_code":
            return JSONResponse(
                content={"error": "unsupported_grant_type", "error_description": "Only 'authorization_code' grant type is supported"},
                status_code=400
            )
        
        # Validate client exists
        client = get_registered_client(client_id)
        if not client:
            return JSONResponse(
                content={"error": "invalid_client", "error_description": "Client not found"},
                status_code=400
            )
        
        # Validate redirect_uri matches registered URIs
        if redirect_uri not in client.get("redirect_uris", []):
            return JSONResponse(
                content={"error": "invalid_request", "error_description": "redirect_uri does not match registered URIs"},
                status_code=400
            )
        
        # Validate authorization code exists
        if code not in authorization_codes:
            return JSONResponse(
                content={"error": "invalid_grant", "error_description": "Authorization code not found or invalid"},
                status_code=400
            )
        
        # Get stored authorization data
        auth_data = authorization_codes[code]
        
        # Validate authorization code is not used
        if auth_data["used"]:
            return JSONResponse(
                content={"error": "invalid_grant", "error_description": "Authorization code already used"},
                status_code=400
            )
        
        # Validate authorization code has not expired
        if auth_data["expires_at"] < time.time():
            return JSONResponse(
                content={"error": "invalid_grant", "error_description": "Authorization code expired"},
                status_code=400
            )
        
        # Validate client_id matches
        if auth_data["client_id"] != client_id:
            return JSONResponse(
                content={"error": "invalid_grant", "error_description": "Client ID mismatch"},
                status_code=400
            )
        
        # Validate redirect_uri matches
        if auth_data["redirect_uri"] != redirect_uri:
            return JSONResponse(
                content={"error": "invalid_grant", "error_description": "Redirect URI mismatch"},
                status_code=400
            )
        
        # Validate PKCE code verifier if code challenge was used
        if auth_data.get("code_challenge"):
            if not code_verifier:
                return JSONResponse(
                    content={"error": "invalid_grant", "error_description": "code_verifier is required for PKCE"},
                    status_code=400
                )
            
            # Verify code challenge (S256 method)
            computed_challenge = base64.urlsafe_b64encode(
                hashlib.sha256(code_verifier.encode()).digest()
            ).decode().rstrip('=')
            
            if computed_challenge != auth_data["code_challenge"]:
                return JSONResponse(
                    content={"error": "invalid_grant", "error_description": "Code verifier validation failed"},
                    status_code=400
                )
        
        # Mark authorization code as used
        auth_data["used"] = True
        
        # Retrieve the stored user token from the database (currently in-memory)
        user_token = auth_data["user_token"]
        stored_scope = auth_data.get("scope", "")
        
        print(f"Token exchange successful for user: {auth_data.get('username')}")
        print(f"Returning token (first 20 chars): {user_token[:20]}...")
        
        # Build token response with the stored user token
        token_response = {
            "access_token": user_token,
            "token_type": "Bearer",
            "expires_in": 3600,  # 1 hour
            "scope": stored_scope
        }

        return JSONResponse(content=token_response, status_code=200)
    except Exception as e:
        print(f"Token request error: {e}")
        import traceback
        traceback.print_exc()
        return JSONResponse(
            content={
                "error": "server_error",
                "error_description": str(e)
            },
            status_code=500
        )

