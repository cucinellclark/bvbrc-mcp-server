"""
RAG Database Functions

This module provides functions for querying and managing RAG (Retrieval-Augmented Generation) databases.
"""

from typing import Dict, Any, Optional, List
import time
import requests


def query_rag_helpdesk_func(
    query: str,
    top_k: Optional[int] = 5,
    config: Optional[dict] = None,
    summarize: bool = True,
) -> Dict[str, Any]:
    """
    Query the RAG helpdesk index to retrieve relevant support and helpdesk documents.

    This function is intended to back the `query_rag_helpdesk` MCP tool.
    It wraps the RAG_API service to query the helpdesk database.

    Args:
        query: The search query string, typically a support or troubleshooting question.
        top_k: Number of top results to return.
        config: Configuration dictionary with RAG / index settings.
                Can include 'database_name' (default: 'helpdesk') and 'score_threshold' (default: 0.0).
        summarize: If True (default), call the summarization LLM on retrieved
                   documents. Set to False to return raw documents only (useful
                   when the caller's own LLM will synthesize the answer).

    Returns:
        Dictionary with query results containing:
        - results: List of retrieved documents with content, score, and metadata
        - count: Number of results returned
        - query: The original query string
        - index: The database name queried
        - summary: (only when summarize=True) LLM-generated summary
    """
    if config is None:
        config = {}

    # Get database name from config, default to 'helpdesk'
    database_name = config.get("database_name", "bvbrc_helpdesk")
    score_threshold = config.get("score_threshold", 0.0)
    rag_api_base_url = config.get("rag_api_base_url", "http://127.0.0.1:8000")
    rag_api_timeout_seconds = config.get("rag_api_timeout_seconds", 45)

    try:
        query_payload = {
            "query": query,
            "top_k": top_k,
            "score_threshold": score_threshold,
        }
        query_url = f"{rag_api_base_url.rstrip('/')}/query/{database_name}"
        rag_response = requests.post(
            query_url,
            json=query_payload,
            timeout=rag_api_timeout_seconds,
        )
        rag_response.raise_for_status()
        search_result = rag_response.json()

        # Transform the RAG service response to match expected format
        documents = search_result.get("documents", [])
        results = [
            {
                "content": doc.get("content", ""),
                "score": doc.get("score", 0.0),
                "metadata": doc.get("metadata", {}),
            }
            for doc in documents
        ]

        result: Dict[str, Any] = {
            "results": results,
            "count": len(results),
            "query": query,
            "index": database_name,
            "source": "bvbrc-rag-api",
        }

        # Optionally summarize retrieved documents
        if summarize:
            documents_text = [
                doc.get("content", "") for doc in results if doc.get("content")
            ]
            summary_output = summarize_helpdesk_documents(
                query=query,
                documents=documents_text,
                model_config=config.get("summarization_model", {}),
            )
            result["summary"] = summary_output.get("summary", "")
            if summary_output.get("error"):
                result["summary_error"] = summary_output["error"]

        return result

    except requests.RequestException as e:
        return {
            "results": [],
            "count": 0,
            "query": query,
            "index": database_name,
            "summary": "",
            "used_documents": [],
            "error": f"RAG API request failed: {str(e)}",
            "errorType": "RAG_API_REQUEST_ERROR",
            "source": "bvbrc-rag-api",
        }
    except Exception as e:
        # Return error result in expected format
        return {
            "results": [],
            "count": 0,
            "query": query,
            "index": database_name,
            "summary": "",
            "used_documents": [],
            "error": str(e),
            "errorType": "API_ERROR",
            "source": "bvbrc-rag-api",
        }


def summarize_helpdesk_documents(
    query: str,
    documents: List[str],
    model_config: Optional[dict] = None,
) -> Dict[str, Any]:
    """
    Summarize helpdesk documents to answer the query using an LLM orchestrator.

    Args:
        query: Original user query.
        documents: List of document text snippets to summarize.
        model_config: Configuration for the summarization model (endpoint, model, apiKey, max_tokens).

    Returns:
        Dict containing:
        - summary: Summarized text response (empty string if unavailable)
        - used_documents: The documents that were summarized (echo of input)
        - error: Optional error message if summarization failed
    """
    if model_config is None:
        model_config = {}

    if not documents:
        return {"summary": "", "used_documents": [], "error": None}

    endpoint = model_config.get("endpoint")
    model = model_config.get("model")
    api_key = model_config.get("apiKey")
    max_tokens = model_config.get("max_tokens", 2048)

    if not endpoint or not model:
        return {
            "summary": "",
            "used_documents": documents,
            "error": "Summarization model configuration is missing endpoint or model",
        }

    # Prepare request payload for the orchestrator (OpenAI-compatible chat/completions)
    prompt_documents = "\n\n".join(
        [f"Document {idx + 1}:\n{doc}" for idx, doc in enumerate(documents)]
    )
    messages = [
        {
            "role": "system",
            "content": (
                "You are a BV-BRC helpdesk assistant. Answer the user's question "
                "using only the provided documents. Be highly descriptive and "
                "specific about the data, features, parameters, outputs, and workflow details "
                "found in those documents, and explain how each relevant detail "
                "addresses the user's question. If information is insufficient, "
                "state what is missing."
            ),
        },
        {
            "role": "user",
            "content": (
                f"User question: {query}\n\n"
                f"Context documents:\n{prompt_documents}\n\n"
                "Write a detailed grounded summary that directly answers the user's "
                "question and explains the most relevant document details. Prefer "
                "clear bullet points when helpful, include concrete values/examples "
                "from the documents when available, and avoid generic statements."
            ),
        },
    ]

    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    payload = {
        "model": model,
        "messages": messages,
        "temperature": 0.7,
        "max_tokens": max_tokens,
    }

    try:
        response = requests.post(endpoint, json=payload, headers=headers, timeout=60)
        response.raise_for_status()
        data = response.json()
        summary = (
            data.get("choices", [{}])[0].get("message", {}).get("content", "").strip()
        )

        return {
            "summary": summary,
            "used_documents": documents,
            "error": None,
        }
    except Exception as e:
        return {
            "summary": "",
            "used_documents": documents,
            "error": f"Summarization failed: {str(e)}",
        }


def literature_rag_retrieve_func(
    query: str,
    top_k: int = 10,
    use_graph: bool = False,
    config: Optional[dict] = None,
    auth_token: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Query the literature RAG retrieval service for relevant scientific publications.

    Proxies to the Copilot API gateway's /copilot-api/rag/retrieve endpoint,
    which forwards to the Coconut vector-search service.  Returns raw document
    chunks with content, scores, and metadata (title, DOI, year, citations).

    Args:
        query: Natural-language search query (organism, gene, topic, etc.).
        top_k: Maximum number of source documents to return (default 10).
        use_graph: Whether to enable knowledge-graph-augmented retrieval.
        config: Configuration dict.  Expected keys:
                - ``literature_rag_url`` (default ``http://ash.cels.anl.gov:12006``)
                - ``literature_rag_timeout_seconds`` (default 45)
        auth_token: BV-BRC authorization token (``un=...|tokenid=...``).

    Returns:
        Dictionary with:
        - sources: list of dicts with ``content``, ``score``, ``metadata``
        - count: number of sources returned
        - query: echo of the original query
        On error, returns an ``error`` key with a description.
    """
    if config is None:
        config = {}

    base_url = config.get("literature_rag_url", "http://ash.cels.anl.gov:12006").rstrip(
        "/"
    )
    timeout = config.get("literature_rag_timeout_seconds", 45)
    retrieve_url = f"{base_url}/copilot-api/rag/retrieve"

    headers: Dict[str, str] = {"Content-Type": "application/json"}
    if auth_token:
        headers["Authorization"] = auth_token

    payload: Dict[str, Any] = {
        "query": query,
        "top_k": top_k,
        "use_graph": use_graph,
    }

    try:
        response = requests.post(
            retrieve_url,
            json=payload,
            headers=headers,
            timeout=timeout,
        )
        response.raise_for_status()
        data = response.json()

        # The gateway returns the Coconut response as-is.
        # The Literature.js frontend expects { sources: [...] }.
        sources = data.get("sources", [])

        return {
            "sources": sources,
            "count": len(sources),
            "query": query,
            "source": "literature-rag",
        }

    except requests.RequestException as e:
        status_code = getattr(getattr(e, "response", None), "status_code", None)
        detail = ""
        if hasattr(e, "response") and e.response is not None:
            try:
                detail = e.response.text[:500]
            except Exception:
                pass
        return {
            "sources": [],
            "count": 0,
            "query": query,
            "error": f"Literature RAG request failed: {str(e)}",
            "errorType": "LITERATURE_RAG_REQUEST_ERROR",
            "status_code": status_code,
            "detail": detail,
            "source": "literature-rag",
        }
    except Exception as e:
        return {
            "sources": [],
            "count": 0,
            "query": query,
            "error": f"Literature RAG unexpected error: {str(e)}",
            "errorType": "LITERATURE_RAG_ERROR",
            "source": "literature-rag",
        }


def list_publication_datasets_func(
    query: str,
    top_k: Optional[int] = 5,
    config: Optional[dict] = None,
) -> Dict[str, Any]:
    """
    List publication datasets relevant to a query using a RAG-backed dataset index.

    This function is intended to back the `list_publication_datasets` MCP tool.

    Args:
        query: The search query string describing the desired data or analysis.
        top_k: Maximum number of datasets to return.
        config: Configuration dictionary with dataset index / RAG settings.

    Returns:
        Dictionary with dataset listing results.
    """
    if config is None:
        config = {}

    # TODO: Implement publication dataset lookup logic.
    # Example steps:
    # 1. Encode the query.
    # 2. Search a publication / dataset index (e.g., vector DB + metadata filters).
    # 3. Return a structured list of datasets that match.

    _ = time.time()  # Placeholder for potential timing / metrics later.

    result: Dict[str, Any] = {
        "results": [],
        "count": 0,
        "query": query,
        "index": "publication_datasets",
        "source": "bvbrc-rag",
    }

    return result
