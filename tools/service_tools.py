"""
BVBRC Service MCP Tools

This module contains all the MCP tool functions for the BVBRC Service MCP Server.
All tools are registered with the FastMCP server instance.

Refactored to use a unified service submission pattern similar to data_tools.py
"""
import sys
import json
import time
import os
import traceback
from fastmcp import FastMCP
from common.json_rpc import JsonRpcCaller
from common.gowe_client import GoWeClient, GoWeError
from functions.service_functions import (
    enumerate_apps, list_jobs, get_service_info,
    start_date_app, start_genome_annotation_app,
    start_genome_assembly_app, start_comprehensive_genome_analysis_app, start_blast_app,
    start_primer_design_app, start_variation_app, start_tnseq_app, start_bacterial_genome_tree_app,
    start_gene_tree_app, start_core_genome_mlst_app, start_whole_genome_snp_app,
    start_taxonomic_classification_app, start_metagenomic_binning_app, start_metagenomic_read_mapping_app,
    start_rnaseq_app, start_expression_import_app, start_sars_wastewater_analysis_app,
    start_sequence_submission_app, start_influenza_ha_subtype_conversion_app,
    start_subspecies_classification_app, start_viral_assembly_app, start_fastq_utils_app,
    start_genome_alignment_app, start_sars_genome_analysis_app, start_msa_snp_analysis_app,
    start_metacats_app, start_proteome_comparison_app, start_comparative_systems_app,
    start_docking_app, start_similar_genome_finder_app,
)
from typing import Any, List, Dict, Optional, Union


def extract_userid_from_token(token: str = None) -> str:
    """
    Extract user ID from JWT token.
    Returns a default user ID if token is None or invalid.
    """
    if not token:
        return None

    try:
        user_id = token.split('|')[0].replace('un=','')
        return user_id

    except Exception as e:
        print(f"Error extracting user ID from token: {e}")
        return None


def register_service_tools(mcp: FastMCP, api: JsonRpcCaller, similar_genome_finder_api: JsonRpcCaller, token_provider):
    """
    Register all MCP tools with the FastMCP server instance.

    Args:
        mcp: FastMCP server instance
        api: Main service API caller
        similar_genome_finder_api: Similar genome finder API caller
        token_provider: TokenProvider instance for handling authentication tokens
    """

    # Mapping between BV-BRC API service names and user-friendly names
    # API_NAME -> user_friendly_name
    BVBRC_TO_FRIENDLY = {
        'Date': 'date',
        'GenomeAssembly2': 'genome_assembly',
        'GenomeAnnotation': 'genome_annotation',
        'ComprehensiveGenomeAnalysis': 'comprehensive_genome_analysis',
        'Homology': 'blast',
        'PrimerDesign': 'primer_design',
        'Variation': 'variation',
        'TnSeq': 'tnseq',
        'CodonTree': 'bacterial_genome_tree',
        'GeneTree': 'gene_tree',
        'CoreGenomeMLST': 'core_genome_mlst',
        'WholeGenomeSNPAnalysis': 'whole_genome_snp',
        'TaxonomicClassification': 'taxonomic_classification',
        'MetagenomeBinning': 'metagenomic_binning',
        'MetagenomicReadMapping': 'metagenomic_read_mapping',
        'RNASeq': 'rnaseq',
        'ExpressionImport': 'expression_import',
        'SARSWastewaterAnalysis': 'sars_wastewater_analysis',
        'SequenceSubmission': 'sequence_submission',
        'InfluenzaHASubtypeConversion': 'influenza_ha_subtype_conversion',
        'HASubtypeNumberingConversion': 'influenza_ha_subtype_conversion',
        'SubspeciesClassification': 'subspecies_classification',
        'ViralAssembly': 'viral_assembly',
        'GenomeAlignment': 'genome_alignment',
        'SARS2Assembly': 'sars_genome_analysis',
        'MSA': 'msa_snp_analysis',
        'MetaCATS': 'metacats',
        'GenomeComparison': 'proteome_comparison',
        'ComparativeSystems': 'comparative_systems',
        'Docking': 'docking',
        'SimilarGenomeFinder': 'similar_genome_finder',
        'FastqUtils': 'fastqutils',
    }

    # Reverse mapping: user_friendly_name -> API_NAME
    FRIENDLY_TO_BVBRC = {v: k for k, v in BVBRC_TO_FRIENDLY.items()}

    # Service mapping: maps user-friendly service names to their handler functions
    SERVICE_MAP = {
        # Basic Services
        'date': start_date_app,

        # Genomics Analysis Services
        'genome_assembly': start_genome_assembly_app,
        'genome_annotation': start_genome_annotation_app,
        'comprehensive_genome_analysis': start_comprehensive_genome_analysis_app,
        'blast': start_blast_app,
        'primer_design': start_primer_design_app,
        'variation': start_variation_app,
        'tnseq': start_tnseq_app,

        # Phylogenomics Services
        'bacterial_genome_tree': start_bacterial_genome_tree_app,
        'gene_tree': start_gene_tree_app,
        'core_genome_mlst': start_core_genome_mlst_app,
        'whole_genome_snp': start_whole_genome_snp_app,

        # Metagenomics Services
        'taxonomic_classification': start_taxonomic_classification_app,
        'metagenomic_binning': start_metagenomic_binning_app,
        'metagenomic_read_mapping': start_metagenomic_read_mapping_app,

        # Transcriptomics Services
        'rnaseq': start_rnaseq_app,
        'expression_import': start_expression_import_app,

        # Viral Services
        'sars_wastewater_analysis': start_sars_wastewater_analysis_app,
        'sequence_submission': start_sequence_submission_app,
        'influenza_ha_subtype_conversion': start_influenza_ha_subtype_conversion_app,
        'subspecies_classification': start_subspecies_classification_app,
        'viral_assembly': start_viral_assembly_app,

        # Additional Services
        'genome_alignment': start_genome_alignment_app,
        'sars_genome_analysis': start_sars_genome_analysis_app,
        'msa_snp_analysis': start_msa_snp_analysis_app,
        'metacats': start_metacats_app,
        'proteome_comparison': start_proteome_comparison_app,
        'comparative_systems': start_comparative_systems_app,
        'docking': start_docking_app,
        'fastqutils': start_fastq_utils_app,
    }

    # Special case for similar_genome_finder which uses a different API
    SPECIAL_API_SERVICES = {
        'similar_genome_finder': (similar_genome_finder_api, start_similar_genome_finder_app),
    }

    # Helper Tools

    @mcp.tool(name="list_service_apps", annotations={"readOnlyHint": True})
    async def service_enumerate_apps(token: Optional[str] = None) -> str:
        """
        List available BV-BRC service apps that can be submitted.

        USE THIS TOOL FOR:
        - Discovering available service names
        - Validating whether a service exists before schema lookup/submission

        DO NOT USE THIS TOOL FOR:
        - Getting input parameter definitions (use get_service_submission_schema)
        - How-to guidance for choosing/using a service (use helpdesk_service_usage)

        Returns:
            JSON array of user-friendly service names (e.g., ["blast", "genome_assembly", "rnaseq", ...])
        """
        auth_token = token_provider.get_token(token)
        if not auth_token:
            return "Error: No authentication token available"

        user_id = extract_userid_from_token(auth_token)
        result = await enumerate_apps(api, auth_token, user_id=user_id)

        # Parse the result and extract only the service IDs
        try:
            # Result is a JSON string, parse it
            apps_data = json.loads(result) if isinstance(result, str) else result

            # Extract IDs from the apps list
            # The structure is typically [[app1, app2, ...]] or [app1, app2, ...]
            if isinstance(apps_data, list) and len(apps_data) > 0:
                # If it's nested (list of lists), flatten it
                if isinstance(apps_data[0], list):
                    apps_list = apps_data[0]
                else:
                    apps_list = apps_data

                # Extract the 'id' field from each app and map to user-friendly names
                bvbrc_service_ids = [app.get('id') for app in apps_list if isinstance(app, dict) and 'id' in app]

                # Convert BV-BRC service names to user-friendly names
                # Only include services we have mappings for
                friendly_names = []
                for bvbrc_id in bvbrc_service_ids:
                    if bvbrc_id in BVBRC_TO_FRIENDLY:
                        friendly_names.append(BVBRC_TO_FRIENDLY[bvbrc_id])

                return {
                    "services": sorted(friendly_names),
                    "count": len(friendly_names),
                    "source": "bvbrc-service"
                }

            return {
                "services": [],
                "count": 0,
                "source": "bvbrc-service"
            }
        except Exception as e:
            print(f"Error parsing service list: {e}", file=sys.stderr)
            return {
                "error": f"Error parsing service list: {str(e)}",
                "errorType": "API_ERROR",
                "source": "bvbrc-service"
            }

    @mcp.tool(name="list_jobs", annotations={"readOnlyHint": True})
    async def service_list_jobs(
        token: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
        sort_by: str = "submit_time",
        sort_dir: str = "desc",
        status: Optional[str] = None,
        service: Optional[str] = None,
        search: Optional[str] = None,
        include_archived: bool = False
    ) -> Dict[str, Any]:
        """
        List jobs (summary view) with sorting/filtering support.

        USE THIS TOOL FOR:
        - Finding jobs by status/service/search term
        - Getting paginated recent-job summaries

        DO NOT USE THIS TOOL FOR:
        - Deep inspection or troubleshooting for specific jobs (use get_job_details)

        Args:
            token: Authentication token (optional)
            limit: Maximum number of jobs to return
            offset: Number of jobs to skip (for pagination)
            sort_by: Field to sort by (e.g., "submit_time", "status")
            sort_dir: Sort direction ("asc" or "desc")
            status: Filter by job status (e.g., "completed", "running", "failed")
            service: Filter by service name. Valid service names include:
                bacterial_genome_tree, blast, comparative_systems, comprehensive_genome_analysis,
                core_genome_mlst, date, docking, expression_import, fastqutils, gene_tree,
                genome_alignment, genome_annotation, genome_assembly, influenza_ha_subtype_conversion,
                metacats, metagenomic_binning, metagenomic_read_mapping, msa_snp_analysis,
                primer_design, proteome_comparison, rnaseq, sars_genome_analysis, sars_wastewater_analysis,
                sequence_submission, similar_genome_finder, subspecies_classification, taxonomic_classification,
                tnseq, variation, viral_assembly, whole_genome_snp
            search: Search term to filter jobs by name or description
            include_archived: Whether to include archived jobs
        """
        auth_token = token_provider.get_token(token)
        if not auth_token:
            return {
                "error": "No authentication token available",
                "errorType": "AUTHENTICATION_FAILED",
                "source": "bvbrc-service"
            }

        # Convert friendly service name to BVBRC API name if needed
        if service and service in FRIENDLY_TO_BVBRC:
            service = FRIENDLY_TO_BVBRC[service]

        user_id = extract_userid_from_token(auth_token)
        return await list_jobs(
            api=api,
            token=auth_token,
            user_id=user_id,
            limit=limit,
            offset=offset,
            sort_by=sort_by,
            sort_dir=sort_dir,
            status=status,
            service=service,
            search=search,
            include_archived=include_archived
        )

    @mcp.tool(name="get_service_submission_schema", annotations={"readOnlyHint": True})
    def service_get_service_submission_schema(service_name: str = None, token: Optional[str] = None) -> str:
        """
        Fetch the authoritative submission schema for one service.
        Use the helpdesk tool for any other guidance or questions about which service to run.

        USE THIS TOOL FOR:
        - Exact required/optional parameters before submit_workflow/service execution

        DO NOT USE THIS TOOL FOR:
        - Listing available services (use list_service_apps)
        - Troubleshooting existing jobs (use get_job_details)

        Args:
            service_name: Name of the service to get submission schema for (e.g., 'genome_assembly', 'blast', 'primer_design')
            token: Authentication token (optional - will use default if not provided)

        Returns:
            Structured parameter/schema details required for submission
        """
        if not service_name:
            return "Error: service_name parameter is required"

        auth_token = token_provider.get_token(token)
        if not auth_token:
            return "Error: No authentication token available"

        # Convert BV-BRC name to friendly name if needed
        if service_name in BVBRC_TO_FRIENDLY:
            service_name = BVBRC_TO_FRIENDLY[service_name]

        try:
            return get_service_info(service_name=service_name)
        except Exception as e:
            available = sorted(list(SERVICE_MAP.keys()) + list(SPECIAL_API_SERVICES.keys()))
            return f"Error getting service submission schema: {str(e)}\n\nAvailable services: {', '.join(available)}"


    # ---------------------------------------------------------------
    # Service-Specific Plan Tools (hybrid: LLM params + deterministic validation)
    # ---------------------------------------------------------------

    @mcp.tool(name="submit_workflow")
    async def submit_workflow(
        workflow_id: Optional[str] = None,
        workflow_json: Optional[Dict[str, Any]] = None,
        token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Submit an already planned workflow for execution.

        This tool submits an already-planned workflow by workflow_id. The workflow
        should come from plan_workflow(), which assigns workflow_id and persists
        the workflow as status='planned'. Validation is performed during submission.

        DO NOT USE THIS TOOL FOR:
        - Creating a new plan from natural language (use plan_workflow first)

        Args:
            workflow_id: Planned workflow ID (preferred)
            workflow_json: Optional workflow payload containing workflow_id
            token: Authentication token (optional - will use default if not provided)

        Returns:
            Dictionary with:
            - On success:
              {
                "workflow_id": "wf_123...",
                "status": "pending",
                "submitted_at": "2026-02-04T10:30:00Z",
                "message": "Workflow submitted for execution",
                "status_url": "http://.../workflows/wf_123/status"
              }

            - On error:
              {
                "error": "Error description",
                "errorType": "SUBMISSION_FAILED | VALIDATION_FAILED | ENGINE_UNAVAILABLE",
                "hint": "Helpful suggestion"
              }

        Notes:
            - The workflow engine must be running and configured for execution
            - The workflow must already be planned/registered before submission
            - Use workflow monitoring tools to track execution progress
            - The workflow_id can be used to query status and retrieve results

        Example:
            # First, plan the workflow
            plan = plan_workflow(user_query="Assemble and annotate genome")

            # Review the plan, then submit it
            result = submit_workflow(workflow_id=plan["workflow_id"])
        """
        if not workflow_id and not workflow_json:
            return {
                "error": "workflow_id or workflow_json parameter is required",
                "errorType": "INVALID_PARAMETERS",
                "hint": "Provide a planned workflow_id (preferred) or a workflow payload that already contains workflow_id",
                "example": "submit_workflow(workflow_id='wf_...')",
                "source": "bvbrc-service"
            }

        if not workflow_id and isinstance(workflow_json, dict):
            if isinstance(workflow_json.get("workflow_id"), str) and workflow_json.get("workflow_id"):
                workflow_id = workflow_json["workflow_id"]
            elif isinstance(workflow_json.get("workflow_json"), dict):
                nested = workflow_json.get("workflow_json", {})
                if isinstance(nested.get("workflow_id"), str) and nested.get("workflow_id"):
                    workflow_id = nested["workflow_id"]

        if not workflow_id:
            return {
                "error": "submit_workflow requires a registered workflow_id",
                "errorType": "INVALID_PARAMETERS",
                "hint": "Run plan_workflow() first and pass the returned workflow_id",
                "source": "bvbrc-service"
            }

        # Get authentication token
        auth_token = token_provider.get_token(token)
        if not auth_token:
            return {
                "error": "No authentication token available",
                "errorType": "AUTHENTICATION_FAILED",
                "hint": "Please provide a valid authentication token",
                "source": "bvbrc-service"
            }

        # Extract user ID
        user_id = extract_userid_from_token(auth_token)
        if not user_id:
            return {
                "error": "Could not extract user ID from token",
                "errorType": "AUTHENTICATION_FAILED",
                "hint": "The provided token is invalid or malformed",
                "source": "bvbrc-service"
            }

        try:
            # Load configuration for GoWe URL
            config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', 'config.json')

            with open(config_path, 'r') as f:
                config = json.load(f)

            # Get GoWe configuration
            gowe_config = config.get('gowe', {})
            gowe_url = gowe_config.get('url', 'https://gowe.software-smithy.org')
            gowe_timeout = gowe_config.get('timeout', 60)

            # Check if GoWe is enabled
            if not gowe_config.get('enabled', True):
                return {
                    "error": "GoWe workflow engine is disabled in configuration",
                    "errorType": "ENGINE_UNAVAILABLE",
                    "hint": "Enable gowe in config.json to submit workflows for execution",
                    "source": "bvbrc-service"
                }

            client = GoWeClient(base_url=gowe_url, timeout=gowe_timeout)

            # Check if GoWe is healthy
            print("Checking GoWe workflow engine health...", file=sys.stderr)
            is_healthy = await client.is_healthy()
            if not is_healthy:
                print("GoWe health check failed", file=sys.stderr)
                return {
                    "error": "GoWe workflow engine is not available",
                    "errorType": "ENGINE_UNAVAILABLE",
                    "hint": f"Ensure GoWe is running at {gowe_url}",
                    "source": "bvbrc-service"
                }

            # GoWe submission requires inputs. For the MCP tool path,
            # we submit with empty inputs (the workflow was already
            # registered with its CWL definition). If the workflow
            # requires inputs, they should have been provided at
            # registration time via the service agent pipeline.
            result = await client.create_submission(
                workflow_id=workflow_id,
                inputs={},
                auth_token=auth_token,
            )
            submission_id = result.get('id', '')
            print(f"Workflow submitted via GoWe: submission_id={submission_id}", file=sys.stderr)

            return {
                "workflow_id": workflow_id,
                "submission_id": submission_id,
                "status": result.get('state', 'PENDING'),
                "submitted_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "message": "Workflow submitted for execution via GoWe",
                "call": {
                    "tool": "submit_workflow",
                    "arguments_executed": {
                        "workflow_id": workflow_id
                    },
                    "replayable": True
                },
                "source": "bvbrc-service"
            }

        except GoWeError as e:
            print(f"GoWe error: {e}", file=sys.stderr)
            return {
                "error": str(e),
                "errorType": e.error_type if hasattr(e, 'error_type') else "SUBMISSION_FAILED",
                "hint": "GoWe rejected the submission. Check the error message for details.",
                "source": "bvbrc-service"
            }
        except FileNotFoundError as e:
            return {
                "error": f"Configuration file not found: {str(e)}",
                "errorType": "CONFIGURATION_ERROR",
                "hint": "Ensure config/config.json exists with 'gowe' section",
                "source": "bvbrc-service"
            }
        except Exception as e:
            error_trace = traceback.format_exc()
            print(f"Error in submit_workflow: {error_trace}", file=sys.stderr)
            return {
                "error": str(e),
                "errorType": "UNKNOWN_ERROR",
                "traceback": error_trace,
                "source": "bvbrc-service"
            }
