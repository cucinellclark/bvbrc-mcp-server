#!/usr/bin/env python3
"""
BV-BRC MVP Tools

This module contains MCP tools for querying MVP (Minimum Viable Product) data from BV-BRC.
"""

import json
import subprocess
from typing import Optional, Dict, Any, List

from fastmcp import FastMCP

# Global variables to store configuration
_base_url = None
_token_provider = None

from functions.data_functions import (
    query_direct,
    lookup_parameters,
    query_info,
    list_solr_collections,
    normalize_select,
    normalize_sort,
    build_filter,
    get_collection_fields,
    validate_filter_fields
)


def convert_json_to_tsv(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Convert a list of JSON objects to TSV format using jq.
    
    Args:
        results: List of dictionaries to convert
        
    Returns:
        Dict with either:
        - {"tsv": <tsv_string>} on success
        - {"error": <error_message>} on failure
    """
    if not results:
        # Empty results - return empty TSV
        print("  TSV conversion: Empty results, returning empty TSV")
        return {"tsv": ""}
    
    print(f"  TSV conversion: Starting conversion of {len(results)} results to TSV format...")
    
    try:
        # Prepare jq command to convert JSON array to TSV
        # This extracts all unique keys from the first object as headers,
        # then converts each object to a TSV row
        jq_command = [
            "jq",
            "-r",
            "(.[0] | keys_unsorted) as $keys | $keys, (.[] | [.[$keys[]] | tostring]) | @tsv"
        ]
        
        # Convert results to JSON string
        json_input = json.dumps(results)
        print(f"  TSV conversion: Running jq command to convert JSON to TSV...")
        
        # Run jq command
        result = subprocess.run(
            jq_command,
            input=json_input,
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode != 0:
            print(f"  TSV conversion: FAILED - jq returned error code {result.returncode}")
            print(f"  TSV conversion error: {result.stderr}")
            return {
                "error": f"jq conversion failed: {result.stderr}",
                "hint": "Ensure jq is installed on your system"
            }
        
        tsv_lines = result.stdout.count('\n')
        print(f"  TSV conversion: SUCCESS - Converted to TSV with {tsv_lines} lines (including header)")
        return {"tsv": result.stdout}
        
    except FileNotFoundError:
        print("  TSV conversion: FAILED - jq command not found")
        return {
            "error": "jq command not found",
            "hint": "Please install jq to use TSV format. See https://jqlang.github.io/jq/download/"
        }
    except subprocess.TimeoutExpired:
        print("  TSV conversion: FAILED - Conversion timed out after 30 seconds")
        return {
            "error": "TSV conversion timed out (>30s)",
            "hint": "Try reducing the batch size"
        }
    except Exception as e:
        print(f"  TSV conversion: FAILED - Unexpected error: {str(e)}")
        return {
            "error": f"TSV conversion error: {str(e)}"
        }


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

    # New, clearer tool names
    @mcp.tool(annotations={"readOnlyHint": True})
    async def bvbrc_query_collection(collection: str,
                               filters: Optional[Dict[str, Any]] = None,
                               select: Optional[Any] = None,
                               sort: Optional[Any] = None,
                               cursorId: Optional[str] = None,
                               countOnly: bool = False,
                               batchSize: Optional[int] = None,
                               format: Optional[str] = "tsv",
                               token: Optional[str] = None) -> Dict[str, Any]:
        """
        Query BV-BRC data with structured filters; Solr syntax is handled for you.
        
        Returns a single page of results with cursor for pagination. For large result sets,
        make multiple calls using the returned nextCursorId until it becomes null.
        
        Args:
            collection: Collection name (e.g., "genome", "genome_feature").
            filters: Structured filter object describing conditions and grouping. Example:
                {
                  "logic": "and",
                  "filters": [
                    { "field": "genome_name", "op": "eq", "value": "Escherichia coli" },
                    { "logic": "or", "filters": [
                        { "field": "resistant_phenotype", "op": "eq", "value": "Resistant" },
                        { "field": "resistant_phenotype", "op": "eq", "value": "Intermediate" }
                      ]
                    }
                  ]
                }
            select: List of fields or comma-separated string (optional).
            sort: Sort string or list of field/direction dicts (optional).
            cursorId: Cursor ID for pagination. Use "*" or omit for first page.
                     Pass the returned nextCursorId to fetch subsequent pages.
                     When nextCursorId is null, you've reached the end.
            countOnly: If True, only return the total count without data.
            batchSize: Number of rows per page (defaults to 1000, range: 1-10000).
                      Use smaller values for faster initial response, larger for fewer round trips.
            format: Output format - "tsv" (default) or "json". 
                   TSV format requires jq to be installed on the system.
                   When format="tsv", results are converted to tab-separated values with headers.
            token: Authentication token (optional, auto-detected if token_provider is configured).
            
        Returns:
            Dict with structure:
            - If countOnly=True: {"numFound": <int>, "source": "bvbrc-mcp-data"}
            - returns: {
                "results": [...] | "tsv": <tsv_string>,           # Array of result objects or TSV string
                "count": <int>,             # Number of results in this page
                "numFound": <int>,          # Total results matching query
                "nextCursorId": <str|null>, # Pass this to get next page (null = no more pages)
                "source": "bvbrc-mcp-data"
              }
        """
        # Validate format parameter
        if format not in ["json", "tsv"]:
            return {
                "error": f"Invalid format: {format}. Must be 'json' or 'tsv'.",
                "source": "bvbrc-mcp-data"
            }
        
        mode_str = "count-only" if countOnly else "paginated-query"
        print(f"Querying collection: {collection}, mode: {mode_str}, format: {format}, cursorId: {cursorId or '*'}")
        
        options: Dict[str, Any] = {}
        select_fields = normalize_select(select)
        sort_expr = normalize_sort(sort)
        if select_fields:
            options["select"] = select_fields
        if sort_expr:
            options["sort"] = sort_expr
        
        # Validate filter fields against the collection's allowed fields
        allowed_fields = set(get_collection_fields(collection))
        invalid_fields = validate_filter_fields(filters, allowed_fields) if filters else []
        if invalid_fields:
            sample_fields = sorted(list(allowed_fields))[:25] if allowed_fields else []
            return {
                "error": f"Invalid field(s) for collection '{collection}': {', '.join(invalid_fields)}",
                "hint": "Call bvbrc_collection_fields_and_parameters to see valid fields.",
                "allowedFieldsSample": sample_fields,
                "source": "bvbrc-mcp-data"
            }

        # Build Solr query from structured filters
        filter_str = build_filter(filters)

        # Apply collection-specific defaults
        if collection == "genome_feature":
            auto = "patric_id:*"
            if filter_str and filter_str != "*:*":
                filter_str = f"({filter_str}) AND {auto}"
            else:
                filter_str = auto

        # Validate batchSize if provided
        if batchSize is not None:
            if batchSize < 1 or batchSize > 10000:
                return {
                    "error": f"Invalid batchSize: {batchSize}. Must be between 1 and 10000.",
                    "source": "bvbrc-mcp-data"
                }

        # Authentication headers
        headers: Optional[Dict[str, str]] = None
        if _token_provider:
            auth_token = _token_provider.get_token(token)
            if auth_token:
                headers = {"Authorization": auth_token}
        elif token:
            headers = {"Authorization": token}
        
        print(f"Filter is {filter_str}")
        
        try:
            result = await query_direct(
                collection, filter_str, options, _base_url, 
                headers=headers, cursorId=cursorId, countOnly=countOnly,
                batch_size=batchSize
            )
            
            # Prefer count for the returned page; fall back to numFound if needed
            observed_count = result.get("count", result.get("numFound"))
            if countOnly:
                print(f"Query found {observed_count} total results.")
            else:
                print(f"Query returned {observed_count} results for this page.")
                if result.get("nextCursorId"):
                    print(f"  More results available. Use nextCursorId to fetch next page.")
            
            # Convert to TSV if requested
            if format == "tsv" and not countOnly:
                print(f"  Converting {observed_count} results from JSON to TSV format...")
                conversion_result = convert_json_to_tsv(result.get("results", []))
                
                # Check if conversion was successful
                if "error" in conversion_result:
                    # Return error with original JSON as fallback
                    print(f"  TSV conversion failed, falling back to JSON format")
                    return {
                        "error": conversion_result["error"],
                        "hint": conversion_result.get("hint", ""),
                        "results": result.get("results", []),  # Fallback to JSON
                        "count": result.get("count"),
                        "numFound": result.get("numFound"),
                        "nextCursorId": result.get("nextCursorId"),
                        "source": "bvbrc-mcp-data"
                    }
                
                # Replace results with TSV string
                result["tsv"] = conversion_result["tsv"]
                del result["results"]  # Remove JSON results
                print(f"  TSV conversion complete. Results replaced with TSV string.")
            
            # Add 'source' field to the top-level response
            result['source'] = 'bvbrc-mcp-data'
            
            return result
            
        except Exception as e:
            return {
                "error": f"Error querying {collection}: {str(e)}",
                "source": "bvbrc-mcp-data"
            }

    @mcp.tool(annotations={"readOnlyHint": True})
    def bvbrc_collection_fields_and_parameters(collection: str) -> str:
        """
        Get fields and query parameters for a given BV-BRC collection.
        
        Args:
            collection: The collection name (e.g., "genome")
        
        Returns:
            String with the parameters for the given collection
        """
        return lookup_parameters(collection)

    @mcp.tool(annotations={"readOnlyHint": True})
    def bvbrc_query_examples_and_rules() -> str:
        """
        Get general query instructions and examples for all collections.
        
        Returns:
            String with general query instructions and formatting guidelines
        """
        print("Fetching general query instructions.")
        return query_info()

    @mcp.tool(annotations={"readOnlyHint": True})
    def bvbrc_list_collections() -> str:
        """
        List all available BV-BRC collections.
        
        Returns:
            String with the available collections
        """
        print("Fetching available collections.")
        return list_solr_collections()
