#!/usr/bin/env python3
"""
BV-BRC MVP Tools

This module contains MCP tools for querying MVP (Minimum Viable Product) data from BV-BRC.
"""

import json
from typing import Optional, Dict

from fastmcp import FastMCP

# Global variables to store configuration
_base_url = None
_token_provider = None

from functions.data_functions import (
    query_direct,
    lookup_parameters,
    query_info,
    list_solr_collections
)


def register_data_tools(mcp: FastMCP, base_url: str, token_provider=None):
    """
    Register all MVP-related MCP tools with the FastMCP server.
    
    Args:
        mcp: FastMCP server instance
        base_url: Base URL for BV-BRC API
        token_provider: TokenProvider instance for handling authentication tokens (optional)
    """
    global _base_url, _token_provider
    _base_url = base_url
    _token_provider = token_provider

    @mcp.tool()
    def query_collection(collection: str, filter_str: str = "",
                          select: Optional[str] = None, sort: Optional[str] = None,
                          cursorId: Optional[str] = None, countOnly: bool = False,
                          token: Optional[str] = None) -> str:
        """
        Query BV-BRC data directly using collection name and Solr query expression.
        
        Args:
            collection: The collection name (e.g., "genome", "genome_feature")
            filter_str: Solr query expression (e.g., "genome_id:123.45" or "species:\"Escherichia coli\"")
            select: Comma-separated list of fields to select (optional)
            sort: Field to sort by (optional)
            cursorId: Cursor ID for pagination (optional, use "*" or omit for first page)
            countOnly: If True, only return the total count without data (optional, default False)
            token: Authentication token for API access (optional, will be auto-detected if token_provider is configured)
        
        Note:
            When countOnly is True, use the minimum number of fields in the select parameter to reduce the number of fields returned.
            
        Returns:
            JSON string with query results:
            - If countOnly is True: {"count": <total_count>}
            - Otherwise: {"count": <batch_count>, "results": [...], "nextCursorId": <str|None>}
        """
        options = {}
        if select:
            options["select"] = select.split(",")
        if sort:
            options["sort"] = sort
        
        # Get authentication token and build headers
        headers: Optional[Dict[str, str]] = None
        if _token_provider:
            auth_token = _token_provider.get_token(token)
            if auth_token:
                headers = {"Authorization": auth_token}
        elif token:
            # Fallback: if token is provided directly and no token_provider, use it
            headers = {"Authorization": token}
        
        try:
            result = query_direct(collection, filter_str, options, _base_url, 
                                 headers=headers, cursorId=cursorId, countOnly=countOnly)
            return json.dumps(result, indent=2)
        except Exception as e:
            return json.dumps({
                "error": f"Error querying {collection}: {str(e)}"
            }, indent=2)
    
    @mcp.tool()
    def solr_collection_parameters(collection: str) -> str:
        """
        Get parameters for a given collection.
        
        Args:
            collection: The collection name (e.g., "genome")
        
        Returns:
            String with the parameters for the given collection
        """
        return lookup_parameters(collection)

    @mcp.tool()
    def solr_query_instructions() -> str:
        """
        Get general query instructions for all collections.
        
        Returns:
            String with general query instructions and formatting guidelines
        """
        return query_info()

    @mcp.tool()
    def solr_collections() -> str:
        """
        Get all available collections.
        
        Returns:
            String with the available collections
        """
        return list_solr_collections()

