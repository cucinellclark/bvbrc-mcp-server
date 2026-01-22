"""
Workflow manifest generation functions
"""
import os
import json
import sys
from typing import Dict, List, Any, Optional
from common.llm_client import LLMClient
from common.json_rpc import JsonRpcCaller
from functions.service_functions import enumerate_apps, get_service_info


def load_config_file(filename: str) -> Dict:
    """Load a JSON config file from the config directory."""
    script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(script_dir, 'config', filename)
    
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def load_prompt_file(filename: str) -> str:
    """Load a prompt file from the prompts directory."""
    script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    prompt_path = os.path.join(script_dir, 'prompts', filename)
    
    with open(prompt_path, 'r', encoding='utf-8') as f:
        return f.read()


def build_service_catalog(api: JsonRpcCaller, token: str, user_id: str = None) -> Dict[str, Any]:
    """
    Build a comprehensive service catalog with names, descriptions, and schemas.
    
    Args:
        api: JsonRpcCaller instance
        token: Authentication token
        user_id: User ID (optional)
        
    Returns:
        Dictionary with service information
    """
    # Get list of available services
    services_json = enumerate_apps(api, token, user_id)
    services_data = json.loads(services_json) if isinstance(services_json, str) else services_json
    
    # Load service name mapping
    service_mapping = load_config_file('service_mapping.json')
    friendly_to_api = service_mapping['friendly_to_api']
    api_to_friendly = {v: k for k, v in friendly_to_api.items()}
    
    # Extract service list
    if isinstance(services_data, list) and len(services_data) > 0:
        if isinstance(services_data[0], list):
            apps_list = services_data[0]
        else:
            apps_list = services_data
    else:
        apps_list = []
    
    # Build catalog
    catalog = {
        "services": [],
        "mapping": {
            "friendly_to_api": friendly_to_api,
            "api_to_friendly": api_to_friendly
        }
    }
    
    for app in apps_list:
        if not isinstance(app, dict) or 'id' not in app:
            continue
            
        api_name = app['id']
        friendly_name = api_to_friendly.get(api_name)
        
        if not friendly_name:
            continue
        
        # Get service description from prompt file
        try:
            description = get_service_info(friendly_name)
        except Exception:
            description = f"Service: {friendly_name}"
        
        catalog["services"].append({
            "friendly_name": friendly_name,
            "api_name": api_name,
            "description": description
        })
    
    return catalog


def generate_workflow_manifest_internal(
    user_query: str,
    api: JsonRpcCaller,
    token: str,
    user_id: str,
    llm_client: LLMClient
) -> str:
    """
    Generate a workflow manifest using LLM-based planning.
    
    Args:
        user_query: User's workflow description
        api: JsonRpcCaller instance
        token: Authentication token
        user_id: User ID for workspace paths
        llm_client: LLM client instance
        
    Returns:
        JSON string containing the workflow manifest
    """
    try:
        # Step 1: Build service catalog
        print("Building service catalog...", file=sys.stderr)
        catalog = build_service_catalog(api, token, user_id)
        
        # Load configuration files
        output_patterns = load_config_file('service_outputs.json')
        system_prompt = load_prompt_file('workflow_generation.txt')
        
        # Prepare service list and descriptions for the prompt
        service_info_list = []
        for service in catalog['services']:
            service_info_list.append({
                "friendly_name": service['friendly_name'],
                "api_name": service['api_name'],
                "description": service['description']
            })
        
        # Build the user prompt with all necessary information
        user_prompt = f"""Generate a workflow manifest for the following user request:

USER QUERY: {user_query}

AVAILABLE SERVICES:
{json.dumps(service_info_list, indent=2)}

SERVICE OUTPUT PATTERNS (use these exact patterns):
{json.dumps(output_patterns, indent=2)}

Generate a complete workflow manifest following the structure and rules in the system prompt. Return ONLY the JSON manifest with no additional text."""
        
        # Make the LLM call
        print("Generating workflow manifest...", file=sys.stderr)
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]
        
        response = llm_client.chat_completion(messages)
        print(f"LLM response (first 300 chars): {response[:300]}...", file=sys.stderr)
        
        # Parse the response
        try:
            # Extract JSON from response (handle markdown code blocks if present)
            response_text = response.strip()
            
            # Remove markdown code blocks if present
            if response_text.startswith("```"):
                lines = response_text.split("\n")
                # Remove first line (```json or ```)
                lines = lines[1:]
                # Remove last line if it's ```
                if lines and lines[-1].strip() == "```":
                    lines = lines[:-1]
                response_text = "\n".join(lines)
            
            # Parse JSON
            workflow_manifest = json.loads(response_text)
            
            # Update workspace_output_folder with actual user_id
            if 'base_context' in workflow_manifest:
                # Replace USERNAME placeholder with actual user_id in workspace_output_folder
                if 'workspace_output_folder' in workflow_manifest['base_context']:
                    workspace_path = workflow_manifest['base_context']['workspace_output_folder']
                    workflow_manifest['base_context']['workspace_output_folder'] = workspace_path.replace('/USERNAME/', f'/{user_id}/')
                else:
                    workflow_manifest['base_context']['workspace_output_folder'] = f"/{user_id}/home/WorkspaceOutputFolder"
                # Remove workspace_root if it exists (legacy field)
                if 'workspace_root' in workflow_manifest['base_context']:
                    del workflow_manifest['base_context']['workspace_root']
            else:
                workflow_manifest['base_context'] = {
                    "base_url": "https://www.bv-brc.org",
                    "workspace_output_folder": f"/{user_id}/home/WorkspaceOutputFolder"
                }
            
            return json.dumps(workflow_manifest, indent=2)
            
        except json.JSONDecodeError as e:
            print(f"Failed to parse LLM response as JSON: {e}", file=sys.stderr)
            return json.dumps({
                "error": f"Failed to parse LLM response: {str(e)}",
                "raw_response": response,
                "hint": "The LLM response was not valid JSON"
            }, indent=2)
    
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        print(f"Error in generate_workflow_manifest_internal: {error_trace}", file=sys.stderr)
        return json.dumps({
            "error": str(e),
            "traceback": error_trace
        }, indent=2)

