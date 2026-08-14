#!/usr/bin/env python3
"""
SRA Tools

MCP-native SRA tools have been removed. Use the unified agent tool
``agent_get_sra_metadata`` (unified_tools.py) or the shared tool
``shared.tools.sra.get_sra_metadata`` instead.
"""

import sys
from fastmcp import FastMCP


def register_sra_tools(mcp: FastMCP, config: dict = None):
    """No-op — SRA tools are now provided by the unified agent toolbox."""
    pass

