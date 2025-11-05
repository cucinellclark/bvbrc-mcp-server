#!/usr/bin/env python3
"""
BV-BRC MVP Tools

This module contains MCP tools for querying MVP (Minimum Viable Product) data from BV-BRC.
"""

import json
from typing import Optional

from fastmcp import FastMCP

# Global variables to store configuration
_base_url = None

from functions.data_functions import (
    query_direct,
    lookup_parameters,
    query_info,
    list_solr_collections
)


def register_data_tools(mcp: FastMCP, base_url: str):
    """Register all MVP-related MCP tools with the FastMCP server."""
    global _base_url
    _base_url = base_url

    @mcp.tool()
    def query_collection(collection: str, filter_str: str = "",
                          select: Optional[str] = None, sort: Optional[str] = None,
                          cursorId: Optional[str] = None, countOnly: bool = False) -> str:
        """
        Query BV-BRC data directly using collection name and Solr query expression.
        
        Args:
            collection: The collection name (e.g., "genome", "genome_feature")
            filter_str: Solr query expression (e.g., "genome_id:123.45" or "species:\"Escherichia coli\"")
            select: Comma-separated list of fields to select (optional)
            sort: Field to sort by (optional)
            cursorId: Cursor ID for pagination (optional, use "*" or omit for first page)
            countOnly: If True, only return the total count without data (optional, default False)
        
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
        
        try:
            result = query_direct(collection, filter_str, options, _base_url, 
                                 cursorId=cursorId, countOnly=countOnly)
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

