"""
MCP Client for communicating with MCP servers.
Provides async interface to dental practice intelligence servers.
"""

import asyncio
import json
from typing import Dict, Any, Optional, List
from datetime import datetime
import aiohttp
from etl_pipeline.core.logger import get_logger
from etl_pipeline.config.settings import settings

logger = get_logger(__name__)

class MCPClient:
    """Client for communicating with MCP servers."""
    
    def __init__(self, server_config: Optional[Dict[str, Any]] = None):
        """Initialize MCP client with server configuration."""
        self.server_config = server_config or self._load_default_config()
        self.session: Optional[aiohttp.ClientSession] = None
        self.servers_available = {}
    
    def _load_default_config(self) -> Dict[str, Any]:
        """Load default MCP server configuration."""
        return {
            'dental_quality_server': {
                'url': 'http://localhost:8001',
                'timeout': 30,
                'enabled': True
            },
            'model_generator_server': {
                'url': 'http://localhost:8002', 
                'timeout': 60,
                'enabled': True
            },
            'orchestration_server': {
                'url': 'http://localhost:8003',
                'timeout': 45,
                'enabled': True
            },
            'business_intelligence_server': {
                'url': 'http://localhost:8004',
                'timeout': 90,
                'enabled': True
            }
        }
    
    async def __aenter__(self):
        """Async context manager entry."""
        self.session = aiohttp.ClientSession()
        await self._check_server_availability()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()
    
    async def _check_server_availability(self):
        """Check which MCP servers are available."""
        for server_name, config in self.server_config.items():
            if not config.get('enabled', False):
                self.servers_available[server_name] = False
                continue
                
            try:
                async with self.session.get(
                    f"{config['url']}/health",
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as response:
                    self.servers_available[server_name] = response.status == 200
                    if response.status == 200:
                        logger.info(f"MCP server {server_name} is available")
                    else:
                        logger.warning(f"MCP server {server_name} returned status {response.status}")
            except Exception as e:
                logger.warning(f"MCP server {server_name} unavailable: {str(e)}")
                self.servers_available[server_name] = False
    
    async def call_tool(self, server_name: str, tool_name: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Call a tool on an MCP server.
        
        Args:
            server_name: Name of the MCP server
            tool_name: Name of the tool to call
            params: Parameters to pass to the tool
            
        Returns:
            Dict containing the tool response
        """
        if not self.servers_available.get(server_name, False):
            logger.warning(f"MCP server {server_name} not available, returning fallback response")
            return self._get_fallback_response(tool_name)
        
        config = self.server_config[server_name]
        url = f"{config['url']}/tools/{tool_name}"
        
        try:
            async with self.session.post(
                url,
                json=params,
                timeout=aiohttp.ClientTimeout(total=config.get('timeout', 30))
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    logger.info(f"MCP tool {tool_name} executed successfully")
                    return result
                else:
                    logger.error(f"MCP tool {tool_name} failed with status {response.status}")
                    return self._get_fallback_response(tool_name)
        
        except Exception as e:
            logger.error(f"Error calling MCP tool {tool_name}: {str(e)}")
            return self._get_fallback_response(tool_name)
    
    def _get_fallback_response(self, tool_name: str) -> Dict[str, Any]:
        """Get fallback response when MCP server is unavailable."""
        return {
            'success': False,
            'tool_name': tool_name,
            'message': 'MCP server unavailable - using fallback response',
            'timestamp': datetime.now().isoformat(),
            'fallback': True
        }
    
    async def validate_patient_record_integrity(self, patient_id: str) -> Dict[str, Any]:
        """Validate patient record integrity across systems."""
        return await self.call_tool(
            'dental_quality_server',
            'validate_patient_record_integrity',
            {'patient_id': patient_id}
        )
    
    async def audit_dental_workflow_compliance(self, date_range: str) -> Dict[str, Any]:
        """Audit dental workflow compliance."""
        return await self.call_tool(
            'dental_quality_server', 
            'audit_dental_workflow_compliance',
            {'date_range': date_range}
        )
    
    async def generate_staging_model(self, table_name: str, business_context: str) -> Dict[str, Any]:
        """Generate dbt staging model for a table."""
        return await self.call_tool(
            'model_generator_server',
            'generate_staging_model',
            {'table_name': table_name, 'business_context': business_context}
        )
    
    async def optimize_pipeline_schedule(self, clinic_schedule: Dict, data_priorities: List) -> Dict[str, Any]:
        """Optimize pipeline scheduling for clinic operations."""
        return await self.call_tool(
            'orchestration_server',
            'optimize_pipeline_schedule', 
            {'clinic_schedule': clinic_schedule, 'data_priorities': data_priorities}
        )
    
    async def analyze_practice_performance(self, metrics: Dict, benchmark_data: Dict) -> Dict[str, Any]:
        """Analyze dental practice performance and provide recommendations."""
        return await self.call_tool(
            'business_intelligence_server',
            'analyze_practice_performance',
            {'metrics': metrics, 'benchmark_data': benchmark_data}
        )

# Convenience function for one-off calls
async def call_mcp_tool(server_name: str, tool_name: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """Convenience function for making single MCP tool calls."""
    async with MCPClient() as client:
        return await client.call_tool(server_name, tool_name, params) 