"""
Connects to the Airflow MCP server (the `airflow-mcp-server` PyPI package)
as an MCP client, giving the LangGraph agent real tool-calling access to
Airflow instead of hand-rolled REST calls.

Design choice: spawned as a short-lived stdio subprocess per call, rather
than run as an always-on sidecar container. Airflow 3's API only supports
JWT auth, and JWTs expire — a long-running sidecar would need its own
token-refresh logic. Minting a fresh token right before each subprocess
launch sidesteps that entirely, at the cost of a small startup overhead
per call. Fine for an incident-response agent that runs occasionally,
not fine for a high-throughput service.

NOTE: tool names below are discovered by keyword match (e.g. "log",
"clear" + "task"), not hard-coded, because the exact tool names exposed
by airflow-mcp-server for your Airflow version haven't been confirmed
against a live instance yet. Once you run this and see the real tool
names (print them once via list_airflow_mcp_tools()), replace the
keyword search in agent.py with exact names for reliability.
"""

from langchain_mcp_adapters.client import MultiServerMCPClient
from app.services.airflow_api import get_auth_token, AIRFLOW_URL


async def get_airflow_mcp_tools():
    """Spawns airflow-mcp-server with a fresh JWT and returns its tools."""
    token = get_auth_token()
    client = MultiServerMCPClient(
        {
            "airflow": {
                "command": "airflow-mcp-server",
                "args": [
                    "--base-url", AIRFLOW_URL,
                    "--auth-token", token,
                    "--static-tools",  # stable named tools instead of dynamic discovery
                ],
                "transport": "stdio",
            }
        }
    )
    return await client.get_tools()


def find_tool(tools, *keywords):
    """Best-effort lookup: first tool whose name contains all given keywords."""
    for tool in tools:
        name = tool.name.lower()
        if all(k in name for k in keywords):
            return tool
    return None


async def list_airflow_mcp_tools():
    """
    Debug helper: run this once against your live stack to see the real
    tool names, then hard-code exact names in agent.py instead of the
    keyword search. E.g. from a shell inside the airflow-agent container:
        python -c "import asyncio; from app.core.mcp_client import list_airflow_mcp_tools; asyncio.run(list_airflow_mcp_tools())"
    """
    tools = await get_airflow_mcp_tools()
    for t in tools:
        print(f"- {t.name}: {t.description}")
    return tools
