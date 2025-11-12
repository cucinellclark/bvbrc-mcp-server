import requests
import json
from typing import Any, Dict, Optional


class JsonRpcCaller:
    """A minimal, generic JSON-RPC caller class."""
    
    def __init__(self, service_url: str):
        """
        Initialize the JSON-RPC caller with service URL and authentication token.
        
        Args:
            service_url: The base URL for the service API
            token: Authentication token for API calls
        """
        self.service_url = service_url.rstrip('/')
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/jsonrpc+json'
        })
    
    def call(self, method: str, params: Optional[Dict[str, Any]] = None, request_id: int = 1, token: str = None) -> Dict[str, Any]:
        """
        Make a JSON-RPC call to the service API.
        
        Args:
            method: The RPC method name to call
            params: Optional parameters for the method (can be dict for workspace calls or list for service calls)
            request_id: Request ID for the JSON-RPC call
            token: Authentication token for API calls
        Returns:
            The response from the API call
            
        Raises:
            requests.RequestException: If the HTTP request fails
            ValueError: If the response contains an error
        """
        
        # Handle different param formats:
        # - Service functions pass params as a list: [app_name, params_dict, metadata_dict]
        # - Workspace functions pass params as a dict: {key: value, ...}
        print('params: ', params)
        if params is not None and isinstance(params, list) and len(params) >= 2 and isinstance(params[1], dict):
            # Service function format - add base_url to params dict if not present
            if 'base_url' not in params[1]:
                params[1]['base_url'] = 'https://www.patricbrc.org'
        print('here1')

        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "id": request_id,
            "params": params,
        }
        print('here2')
        if token:
            self.session.headers.update({
                'Authorization': f'{token}'
            })
        print('here3')
        try:
            response = self.session.post(
                self.service_url,
                data=json.dumps(payload),
                timeout=30
            )
            print('here4')
            response.raise_for_status()
            
            result = response.json()
            print('here5')
            # Check for JSON-RPC errors
            if "error" in result:
                raise ValueError(f"JSON-RPC error: {result['error']}")
            
            return result.get("result", {})
        

        except requests.RequestException as e:
            raise requests.RequestException(f"HTTP request failed: {e}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON response: {e}")
        except Exception as e:
            print(f"error: {e}")
            return []

    
    def close(self):
        """Close the HTTP session."""
        self.session.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

