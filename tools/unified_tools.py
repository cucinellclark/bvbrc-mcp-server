"""Unified agent tools exposed as MCP tools.

Registers the same 23 tools that agents use internally as MCP tools,
so external MCP clients can use the same streamlined interface.

These tools are prefixed with ``agent_`` to distinguish them from
the existing MCP-native tools (which have more complex implementations
suited for direct external use).

The unified tools:
  - Use the same implementations from ``shared.tools.*``
  - Accept a ``bvbrc_token`` parameter for auth (since MCP clients
    don't have an agent config object)
  - Are async-compatible
"""

from __future__ import annotations

import sys
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastmcp import FastMCP

# Ensure the repo root is on sys.path so shared.tools is importable
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
for _subdir in ("", "agents", "config"):
    _p = str(_REPO_ROOT / _subdir) if _subdir else str(_REPO_ROOT)
    if _p not in sys.path:
        sys.path.insert(0, _p)

logger = logging.getLogger(__name__)


def register_unified_tools(mcp: FastMCP, token_provider: Any = None):
    """Register the 23 unified agent tools as MCP tools.

    These wrap the shared tool implementations so external MCP clients
    can use the same tools that agents use internally.

    Args:
        mcp: FastMCP server instance.
        token_provider: TokenProvider for resolving auth tokens.
    """
    from shared.tools.data import search_data, facet_query, probe_data
    from shared.tools.collections import list_collections, get_collection_fields
    from shared.tools.workspace import (
        workspace_browse,
        get_file_metadata,
        read_file_preview,
    )
    from shared.tools.gowe import (
        list_gowe_workflows,
        get_workflow_inputs,
        submit_gowe_job,
    )
    from shared.tools.groups import create_group
    from shared.tools.sra import get_sra_metadata
    from shared.tools.similar_genome import find_similar_genomes
    from shared.tools.literature import search_literature

    def _resolve_token(bvbrc_token: Optional[str] = None) -> Optional[str]:
        """Resolve BV-BRC auth token from explicit param or token provider."""
        if bvbrc_token:
            return bvbrc_token
        if token_provider:
            try:
                return token_provider.get_token()
            except Exception:
                return None
        return None

    def _build_headers(token: Optional[str]) -> Optional[Dict[str, str]]:
        """Build auth headers dict from a token."""
        if token:
            return {"Authorization": token}
        return None

    def _build_config(token: Optional[str] = None) -> Any:
        """Build a minimal config object for shared tools."""
        from shared.models import BaseAgentConfig
        kwargs: Dict[str, Any] = {}
        if token:
            kwargs["bvbrc_auth_token"] = token
        return BaseAgentConfig(**kwargs)

    # ---------------------------------------------------------------
    # DATA TOOLS
    # ---------------------------------------------------------------

    @mcp.tool()
    async def agent_search_data(
        collection: str,
        query: str,
        select: Optional[List[str]] = None,
        sort: Optional[str] = None,
        limit: int = 25,
        count_only: bool = False,
        bvbrc_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Search a BV-BRC Solr data collection. Returns matching records.

        Args:
            collection: Solr collection to query (e.g. 'genome', 'genome_feature').
            query: Solr query string (e.g. 'genus:Salmonella AND host_name:Human').
            select: Fields to return. Only request fields you need.
            sort: Sort order (e.g. 'genome_name asc').
            limit: Max records to return (default 25, max 1000).
            count_only: If true, return only the count.
            bvbrc_token: BV-BRC auth token.
        """
        token = _resolve_token(bvbrc_token)
        config = _build_config(token)
        headers = _build_headers(token)
        kwargs: Dict[str, Any] = {
            "collection": collection, "query": query,
            "config": config, "headers": headers,
        }
        if select is not None:
            kwargs["select"] = select
        if sort is not None:
            kwargs["sort"] = sort
        if limit != 25:
            kwargs["limit"] = limit
        if count_only:
            kwargs["count_only"] = count_only
        return await search_data(**kwargs)

    @mcp.tool()
    async def agent_facet_query(
        collection: str,
        query: str,
        facet_fields: List[str],
        facet_limit: int = 20,
        facet_mincount: int = 1,
        bvbrc_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get faceted counts (value distributions) for BV-BRC data fields.

        Args:
            collection: Solr collection to query.
            query: Solr query to filter records before faceting.
            facet_fields: Fields to get value distributions for.
            facet_limit: Max values per facet field (default 20).
            facet_mincount: Min count to include (default 1).
            bvbrc_token: BV-BRC auth token.
        """
        token = _resolve_token(bvbrc_token)
        config = _build_config(token)
        headers = _build_headers(token)
        return await facet_query(
            collection=collection, query=query, facet_fields=facet_fields,
            facet_limit=facet_limit, facet_mincount=facet_mincount,
            config=config, headers=headers,
        )

    @mcp.tool()
    async def agent_probe_data(
        collection: str,
        keywords: str,
        facet_fields: Optional[List[str]] = None,
        facet_limit: int = 20,
        bvbrc_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Keyword-based reconnaissance search against a BV-BRC collection.

        Args:
            collection: Collection to probe.
            keywords: Search keywords for full-text search.
            facet_fields: Fields to get value distributions for.
            facet_limit: Max values per facet field.
            bvbrc_token: BV-BRC auth token.
        """
        token = _resolve_token(bvbrc_token)
        config = _build_config(token)
        headers = _build_headers(token)
        kwargs: Dict[str, Any] = {
            "collection": collection, "keywords": keywords,
            "config": config, "headers": headers,
        }
        if facet_fields is not None:
            kwargs["facet_fields"] = facet_fields
        if facet_limit != 20:
            kwargs["facet_limit"] = facet_limit
        return await probe_data(**kwargs)

    @mcp.tool()
    async def agent_list_collections() -> Dict[str, Any]:
        """List all available BV-BRC Solr data collections with descriptions."""
        return await list_collections()

    @mcp.tool()
    async def agent_get_collection_fields(
        collection: str,
    ) -> Dict[str, Any]:
        """Get queryable fields and types for a BV-BRC Solr collection.

        Args:
            collection: The collection to inspect.
        """
        return await get_collection_fields(collection=collection)

    # ---------------------------------------------------------------
    # WORKSPACE TOOLS
    # ---------------------------------------------------------------

    @mcp.tool()
    async def agent_workspace_browse(
        path: Optional[str] = None,
        name_contains: Optional[List[str]] = None,
        file_extensions: Optional[List[str]] = None,
        workspace_types: Optional[List[str]] = None,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = None,
        num_results: int = 50,
        bvbrc_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Browse and search files in the user's BV-BRC workspace.

        Args:
            path: Workspace path to browse. Omit for home directory.
            name_contains: Filename substring filters (AND logic).
            file_extensions: File extension filters (OR logic).
            workspace_types: Workspace object type filters (OR logic).
            sort_by: Sort field (creation_time, name, size, type).
            sort_order: Sort direction (asc, desc).
            num_results: Max results (default 50).
            bvbrc_token: BV-BRC auth token.
        """
        token = _resolve_token(bvbrc_token)
        config = _build_config(token)
        headers = _build_headers(token)
        kwargs: Dict[str, Any] = {"config": config, "headers": headers}
        if path is not None:
            kwargs["path"] = path
        if name_contains is not None:
            kwargs["name_contains"] = name_contains
        if file_extensions is not None:
            kwargs["file_extensions"] = file_extensions
        if workspace_types is not None:
            kwargs["workspace_types"] = workspace_types
        if sort_by is not None:
            kwargs["sort_by"] = sort_by
        if sort_order is not None:
            kwargs["sort_order"] = sort_order
        if num_results != 50:
            kwargs["num_results"] = num_results
        return await workspace_browse(**kwargs)

    @mcp.tool()
    async def agent_get_file_metadata(
        path: str,
        bvbrc_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get metadata for a workspace file or folder.

        Args:
            path: Workspace path to the file or folder.
            bvbrc_token: BV-BRC auth token.
        """
        token = _resolve_token(bvbrc_token)
        config = _build_config(token)
        headers = _build_headers(token)
        return await get_file_metadata(path=path, config=config, headers=headers)

    @mcp.tool()
    async def agent_read_file_preview(
        path: str,
        max_bytes: int = 8192,
        bvbrc_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Read the first portion of a workspace file.

        Args:
            path: Workspace path to the file.
            max_bytes: Max bytes to read (default 8192).
            bvbrc_token: BV-BRC auth token.
        """
        token = _resolve_token(bvbrc_token)
        config = _build_config(token)
        headers = _build_headers(token)
        return await read_file_preview(
            path=path, max_bytes=max_bytes, config=config, headers=headers,
        )

    # ---------------------------------------------------------------
    # GOWE WORKFLOW TOOLS
    # ---------------------------------------------------------------

    @mcp.tool()
    async def agent_list_gowe_workflows(
        bvbrc_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """List all available GoWe workflows.

        Args:
            bvbrc_token: BV-BRC auth token.
        """
        token = _resolve_token(bvbrc_token)
        config = _build_config(token)
        headers = _build_headers(token)
        return await list_gowe_workflows(config=config, headers=headers)

    @mcp.tool()
    async def agent_get_workflow_inputs(
        workflow_id: str,
        bvbrc_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get the input schema for a GoWe workflow.

        Args:
            workflow_id: The GoWe workflow ID.
            bvbrc_token: BV-BRC auth token.
        """
        token = _resolve_token(bvbrc_token)
        config = _build_config(token)
        headers = _build_headers(token)
        return await get_workflow_inputs(
            workflow_id=workflow_id, config=config, headers=headers,
        )

    @mcp.tool()
    async def agent_submit_gowe_job(
        workflow_id: str,
        inputs: Dict[str, Any],
        bvbrc_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Submit a job to the GoWe workflow engine.

        Args:
            workflow_id: GoWe workflow ID to run.
            inputs: Input values matching the workflow's schema.
            bvbrc_token: BV-BRC auth token.
        """
        token = _resolve_token(bvbrc_token)
        config = _build_config(token)
        headers = _build_headers(token)
        return await submit_gowe_job(
            workflow_id=workflow_id, inputs=inputs, config=config, headers=headers,
        )

    # ---------------------------------------------------------------
    # GROUP TOOLS
    # ---------------------------------------------------------------

    @mcp.tool()
    async def agent_create_group(
        group_name: str,
        group_type: str,
        collection: str,
        query: str,
        limit: int = 500,
        bvbrc_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create a genome or feature group from a Solr query.

        Args:
            group_name: Name for the new group.
            group_type: 'genome_group' or 'feature_group'.
            collection: Solr collection to query for IDs.
            query: Solr query string.
            limit: Max IDs to include (default 500).
            bvbrc_token: BV-BRC auth token.
        """
        token = _resolve_token(bvbrc_token)
        config = _build_config(token)
        headers = _build_headers(token)
        return await create_group(
            group_name=group_name, group_type=group_type,
            collection=collection, query=query, limit=limit,
            config=config, headers=headers,
        )

    # ---------------------------------------------------------------
    # SRA TOOLS
    # ---------------------------------------------------------------

    @mcp.tool()
    async def agent_get_sra_metadata(
        sra_ids: List[str],
        bvbrc_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Retrieve metadata for SRA run accession IDs.

        Args:
            sra_ids: List of SRA IDs (e.g. ['SRR37956035']).
            bvbrc_token: BV-BRC auth token.
        """
        token = _resolve_token(bvbrc_token)
        config = _build_config(token)
        headers = _build_headers(token)
        return await get_sra_metadata(
            sra_ids=sra_ids, config=config, headers=headers,
        )

    # ---------------------------------------------------------------
    # GENOME SIMILARITY
    # ---------------------------------------------------------------

    @mcp.tool()
    async def agent_find_similar_genomes(
        genome_id: Optional[str] = None,
        fasta_file: Optional[str] = None,
        max_pvalue: Optional[float] = None,
        max_distance: Optional[float] = None,
        max_hits: Optional[int] = None,
        scope: Optional[str] = None,
        include_bacterial: Optional[bool] = None,
        include_viral: Optional[bool] = None,
        bvbrc_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Find similar genomes using Mash/MinHash distance.

        Provide exactly one of genome_id or fasta_file.

        Args:
            genome_id: BV-BRC genome ID (e.g. '83332.12').
            fasta_file: Full workspace path to a FASTA file.
            max_pvalue: Max p-value threshold.
            max_distance: Max Mash distance.
            max_hits: Max results to return.
            scope: 'reference' or 'all'.
            include_bacterial: Include bacterial genomes.
            include_viral: Include viral genomes.
            bvbrc_token: BV-BRC auth token.
        """
        token = _resolve_token(bvbrc_token)
        config = _build_config(token)
        headers = _build_headers(token)
        kwargs: Dict[str, Any] = {"config": config, "headers": headers}
        if genome_id is not None:
            kwargs["genome_id"] = genome_id
        if fasta_file is not None:
            kwargs["fasta_file"] = fasta_file
        if max_pvalue is not None:
            kwargs["max_pvalue"] = max_pvalue
        if max_distance is not None:
            kwargs["max_distance"] = max_distance
        if max_hits is not None:
            kwargs["max_hits"] = max_hits
        if scope is not None:
            kwargs["scope"] = scope
        if include_bacterial is not None:
            kwargs["include_bacterial"] = include_bacterial
        if include_viral is not None:
            kwargs["include_viral"] = include_viral
        return await find_similar_genomes(**kwargs)

    # ---------------------------------------------------------------
    # LITERATURE
    # ---------------------------------------------------------------

    @mcp.tool()
    async def agent_search_literature(
        query: str,
        top_k: int = 10,
        use_graph: bool = False,
        bvbrc_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Search scientific literature using the RAG service.

        Args:
            query: Natural-language query.
            top_k: Max sources to return (default 10).
            use_graph: Enable knowledge-graph retrieval.
            bvbrc_token: BV-BRC auth token.
        """
        token = _resolve_token(bvbrc_token)
        config = _build_config(token)
        headers = _build_headers(token)
        return await search_literature(
            query=query, top_k=top_k, use_graph=use_graph,
            config=config, headers=headers,
        )

    logger.info("Registered %d unified agent tools (agent_* prefix)", 15)
