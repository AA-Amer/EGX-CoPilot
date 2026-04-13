"""
Orchestrator agent — routes user queries to the appropriate sub-agent
and synthesizes a final response.
TODO: implement with Claude claude-sonnet-4-5 tool use.
"""


class OrchestratorAgent:
    """Routes queries and synthesises responses from sub-agents."""

    def __init__(self, config: dict):
        self.config = config

    async def handle(self, message: str, context: dict) -> str:
        raise NotImplementedError
