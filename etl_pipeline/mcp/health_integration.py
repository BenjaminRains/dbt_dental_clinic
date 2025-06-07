"""
MCP-Enhanced Health Checker
Integrates MCP servers with existing health monitoring to provide
dental-specific intelligence on top of technical database health.
"""

import asyncio
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from etl_pipeline.monitoring.health_checks import health_checker
from etl_pipeline.mcp.client import MCPClient
from etl_pipeline.core.logger import get_logger
from etl_pipeline.config.settings import settings

logger = get_logger(__name__)

class MCPEnhancedHealthChecker:
    """Enhanced health checker that combines technical and business health monitoring."""
    
    def __init__(self, mcp_client: Optional[MCPClient] = None):
        """Initialize with optional MCP client."""
        self.mcp_client = mcp_client
        self.basic_health_checker = health_checker
    
    async def check_comprehensive_health(self) -> Dict[str, Any]:
        """
        Perform comprehensive health check combining:
        1. Technical database health (existing health_checker)
        2. Dental business process health (MCP servers)
        """
        logger.info("Starting comprehensive health check")
        
        # 1. Technical foundation (your existing health checks)
        basic_health = self._get_basic_health()
        
        # 2. Enhanced dental intelligence (MCP layer)
        dental_health = await self._get_dental_health()
        
        # 3. Combined assessment
        overall_health = self._assess_overall_health(basic_health, dental_health)
        
        return {
            'timestamp': datetime.now().isoformat(),
            'basic_health': basic_health,
            'dental_health': dental_health,
            'overall_assessment': overall_health,
            'recommendations': await self._generate_recommendations(basic_health, dental_health)
        }
    
    def _get_basic_health(self) -> Dict[str, Any]:
        """Get basic technical health using existing health_checker."""
        try:
            # Use your existing health checker
            basic_results = self.basic_health_checker.check_all()
            
            # Enhanced formatting
            return {
                'connection_health': basic_results,
                'summary': self._summarize_basic_health(basic_results),
                'technical_score': self._calculate_technical_score(basic_results)
            }
        except Exception as e:
            logger.error(f"Basic health check failed: {str(e)}")
            return {
                'connection_health': {},
                'summary': 'Health check failed',
                'technical_score': 0,
                'error': str(e)
            }
    
    async def _get_dental_health(self) -> Dict[str, Any]:
        """Get dental-specific health using MCP servers."""
        if not self.mcp_client:
            return {
                'patient_integrity': {'status': 'not_available', 'score': None},
                'clinical_workflows': {'status': 'not_available', 'score': None},
                'revenue_cycle': {'status': 'not_available', 'score': None},
                'compliance': {'status': 'not_available', 'score': None}
            }
        
        try:
            # Patient data integrity validation
            patient_health = await self.mcp_client.call_tool(
                'dental_quality_server',
                'validate_patient_record_integrity',
                {'scope': 'comprehensive'}
            )
            
            # Clinical workflow compliance
            workflow_health = await self.mcp_client.call_tool(
                'dental_quality_server',
                'audit_dental_workflow_compliance',
                {'date_range': 'last_7_days'}
            )
            
            # Revenue cycle health
            revenue_health = await self.mcp_client.call_tool(
                'business_intelligence_server',
                'analyze_revenue_cycle_health',
                {'include_aging': True}
            )
            
            # HIPAA compliance
            compliance_health = await self.mcp_client.call_tool(
                'dental_quality_server',
                'validate_hipaa_compliance',
                {'include_recommendations': True}
            )
            
            return {
                'patient_integrity': patient_health,
                'clinical_workflows': workflow_health,
                'revenue_cycle': revenue_health,
                'compliance': compliance_health,
                'dental_score': self._calculate_dental_score({
                    'patient': patient_health,
                    'workflow': workflow_health,
                    'revenue': revenue_health,
                    'compliance': compliance_health
                })
            }
            
        except Exception as e:
            logger.error(f"Dental health check failed: {str(e)}")
            return {
                'patient_integrity': {'status': 'error', 'message': str(e)},
                'clinical_workflows': {'status': 'error', 'message': str(e)},
                'revenue_cycle': {'status': 'error', 'message': str(e)},
                'compliance': {'status': 'error', 'message': str(e)},
                'dental_score': 0
            }
    
    def _summarize_basic_health(self, basic_results: Dict) -> str:
        """Summarize basic health results."""
        if not basic_results:
            return "Unable to assess technical health"
        
        total_connections = len(basic_results)
        healthy_connections = sum(1 for db_health in basic_results.values() 
                                if db_health.get('connection', False))
        
        if healthy_connections == total_connections:
            return "All database connections healthy"
        elif healthy_connections > 0:
            return f"{healthy_connections}/{total_connections} database connections healthy"
        else:
            return "Critical: No database connections available"
    
    def _calculate_technical_score(self, basic_results: Dict) -> float:
        """Calculate technical health score (0-100)."""
        if not basic_results:
            return 0.0
        
        total_score = 0
        total_checks = 0
        
        for db_type, db_health in basic_results.items():
            # Connection check (50% weight)
            if db_health.get('connection', False):
                total_score += 50
            total_checks += 50
            
            # Performance check (50% weight)
            performance = db_health.get('performance', {})
            if performance and performance.get('query_time', 999) < 1.0:
                total_score += 50
            total_checks += 50
        
        return (total_score / total_checks * 100) if total_checks > 0 else 0.0
    
    def _calculate_dental_score(self, dental_results: Dict) -> float:
        """Calculate dental business health score (0-100)."""
        scores = []
        
        for check_name, result in dental_results.items():
            if isinstance(result, dict) and 'score' in result:
                scores.append(result['score'])
            elif isinstance(result, dict) and result.get('success', False):
                scores.append(100.0)
            elif isinstance(result, dict) and not result.get('fallback', False):
                scores.append(50.0)  # Partial success
        
        return sum(scores) / len(scores) if scores else 0.0
    
    def _assess_overall_health(self, basic_health: Dict, dental_health: Dict) -> Dict[str, Any]:
        """Assess overall system health combining technical and business factors."""
        technical_score = basic_health.get('technical_score', 0)
        dental_score = dental_health.get('dental_score', 0)
        
        # Weighted average: 40% technical, 60% business
        overall_score = (technical_score * 0.4) + (dental_score * 0.6)
        
        if overall_score >= 90:
            status = "EXCELLENT"
            color = "green"
        elif overall_score >= 75:
            status = "GOOD"
            color = "green"
        elif overall_score >= 60:
            status = "WARNING"
            color = "yellow"
        elif overall_score >= 40:
            status = "CRITICAL"
            color = "orange"
        else:
            status = "EMERGENCY"
            color = "red"
        
        return {
            'overall_score': round(overall_score, 1),
            'status': status,
            'color': color,
            'technical_weight': 40,
            'business_weight': 60,
            'breakdown': {
                'technical_score': technical_score,
                'dental_score': dental_score
            }
        }
    
    async def _generate_recommendations(self, basic_health: Dict, dental_health: Dict) -> List[Dict[str, Any]]:
        """Generate actionable recommendations based on health assessment."""
        recommendations = []
        
        # Technical recommendations
        if basic_health.get('technical_score', 0) < 80:
            recommendations.append({
                'type': 'technical',
                'priority': 'high',
                'title': 'Database Performance Issues',
                'description': 'Some database connections showing performance degradation',
                'action': 'Review database performance metrics and consider optimization'
            })
        
        # Dental business recommendations
        dental_score = dental_health.get('dental_score', 0)
        if dental_score < 90:
            # Get specific recommendations from MCP if available
            if self.mcp_client:
                try:
                    mcp_recommendations = await self.mcp_client.call_tool(
                        'business_intelligence_server',
                        'generate_health_recommendations',
                        {
                            'technical_score': basic_health.get('technical_score', 0),
                            'dental_score': dental_score,
                            'health_data': dental_health
                        }
                    )
                    
                    if mcp_recommendations.get('success', False):
                        recommendations.extend(mcp_recommendations.get('recommendations', []))
                except Exception as e:
                    logger.warning(f"Failed to get MCP recommendations: {str(e)}")
            
            # Fallback recommendations
            if dental_score < 70:
                recommendations.append({
                    'type': 'business',
                    'priority': 'medium',
                    'title': 'Dental Workflow Review Needed',
                    'description': 'Business process health below optimal levels',
                    'action': 'Review patient scheduling, billing, and clinical workflows'
                })
        
        return recommendations

# Convenience function for CLI integration
async def run_enhanced_health_check() -> Dict[str, Any]:
    """Run enhanced health check and return results."""
    async with MCPClient() as mcp_client:
        enhanced_checker = MCPEnhancedHealthChecker(mcp_client)
        return await enhanced_checker.check_comprehensive_health() 