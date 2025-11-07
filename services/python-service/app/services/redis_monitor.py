"""
Redis Connection Pool Monitor
Monitors Redis connection pool usage and provides alerts when approaching limits
"""
import asyncio
import logging
import time
from typing import Dict, Any
from ..config.redis_config import redis_cluster, redis_pubsub_client

logger = logging.getLogger(__name__)

class RedisConnectionMonitor:
    """Monitor Redis connection pool usage and health"""
    
    def __init__(self):
        self.stats = {
            'cluster_pool_size': 0,
            'cluster_available': 0,
            'cluster_in_use': 0,
            'pubsub_pool_size': 0,
            'pubsub_available': 0,
            'pubsub_in_use': 0,
            'last_check': None,
            'warnings_sent': 0,
            'errors_detected': 0
        }
        self.warning_threshold = 0.8  # Warn when 80% of connections are in use
        self.critical_threshold = 0.95  # Critical when 95% of connections are in use
        
    async def check_connection_pools(self) -> Dict[str, Any]:
        """Check current connection pool status"""
        try:
            current_time = time.time()
            
            # Get cluster connection pool stats
            cluster_stats = await self._get_cluster_pool_stats()
            
            # Get pubsub connection pool stats  
            pubsub_stats = await self._get_pubsub_pool_stats()
            
            # Update internal stats
            self.stats.update({
                **cluster_stats,
                **pubsub_stats,
                'last_check': current_time
            })
            
            # Check for warnings/alerts
            await self._check_thresholds()
            
            return self.stats
            
        except Exception as e:
            logger.error(f"Error checking Redis connection pools: {e}")
            self.stats['errors_detected'] += 1
            return self.stats
    
    async def _get_cluster_pool_stats(self) -> Dict[str, Any]:
        """Get Redis cluster connection pool statistics"""
        try:
            # Access the connection pool from the cluster
            pool_stats = {}
            
            # Try to get pool information from cluster nodes
            if hasattr(redis_cluster, '_nodes_manager'):
                nodes_manager = redis_cluster._nodes_manager
                if hasattr(nodes_manager, 'nodes_cache'):
                    total_connections = 0
                    total_available = 0
                    
                    for node_name, node in nodes_manager.nodes_cache.items():
                        if hasattr(node, 'connection_pool'):
                            pool = node.connection_pool
                            if hasattr(pool, '_available_connections') and hasattr(pool, '_created_connections'):
                                available = len(pool._available_connections)
                                created = pool._created_connections
                                total_connections += created
                                total_available += available
                    
                    pool_stats = {
                        'cluster_pool_size': total_connections,
                        'cluster_available': total_available,
                        'cluster_in_use': total_connections - total_available
                    }
            
            # Fallback if we can't get detailed stats
            if not pool_stats:
                pool_stats = {
                    'cluster_pool_size': 500,  # Max configured
                    'cluster_available': -1,   # Unknown
                    'cluster_in_use': -1       # Unknown
                }
                
            return pool_stats
            
        except Exception as e:
            logger.debug(f"Could not get cluster pool stats: {e}")
            return {
                'cluster_pool_size': 500,
                'cluster_available': -1,
                'cluster_in_use': -1
            }
    
    async def _get_pubsub_pool_stats(self) -> Dict[str, Any]:
        """Get Redis pubsub connection pool statistics"""
        try:
            pool_stats = {}
            
            # Try to get pool information from pubsub client
            if hasattr(redis_pubsub_client, 'connection_pool'):
                pool = redis_pubsub_client.connection_pool
                if hasattr(pool, '_available_connections') and hasattr(pool, '_created_connections'):
                    available = len(pool._available_connections)
                    created = pool._created_connections
                    
                    pool_stats = {
                        'pubsub_pool_size': created,
                        'pubsub_available': available,
                        'pubsub_in_use': created - available
                    }
            
            # Fallback if we can't get detailed stats
            if not pool_stats:
                pool_stats = {
                    'pubsub_pool_size': 50,  # Max configured
                    'pubsub_available': -1,  # Unknown
                    'pubsub_in_use': -1      # Unknown
                }
                
            return pool_stats
            
        except Exception as e:
            logger.debug(f"Could not get pubsub pool stats: {e}")
            return {
                'pubsub_pool_size': 50,
                'pubsub_available': -1,
                'pubsub_in_use': -1
            }
    
    async def _check_thresholds(self):
        """Check if connection usage exceeds warning/critical thresholds"""
        try:
            # Check cluster pool
            if self.stats['cluster_pool_size'] > 0 and self.stats['cluster_in_use'] >= 0:
                cluster_usage = self.stats['cluster_in_use'] / self.stats['cluster_pool_size']
                
                if cluster_usage >= self.critical_threshold:
                    logger.error(f"🚨 CRITICAL: Redis cluster connection pool at {cluster_usage:.1%} "
                               f"({self.stats['cluster_in_use']}/{self.stats['cluster_pool_size']})")
                    self.stats['warnings_sent'] += 1
                elif cluster_usage >= self.warning_threshold:
                    logger.warning(f"⚠️ WARNING: Redis cluster connection pool at {cluster_usage:.1%} "
                                 f"({self.stats['cluster_in_use']}/{self.stats['cluster_pool_size']})")
                    self.stats['warnings_sent'] += 1
            
            # Check pubsub pool
            if self.stats['pubsub_pool_size'] > 0 and self.stats['pubsub_in_use'] >= 0:
                pubsub_usage = self.stats['pubsub_in_use'] / self.stats['pubsub_pool_size']
                
                if pubsub_usage >= self.critical_threshold:
                    logger.error(f"🚨 CRITICAL: Redis pubsub connection pool at {pubsub_usage:.1%} "
                               f"({self.stats['pubsub_in_use']}/{self.stats['pubsub_pool_size']})")
                    self.stats['warnings_sent'] += 1
                elif pubsub_usage >= self.warning_threshold:
                    logger.warning(f"⚠️ WARNING: Redis pubsub connection pool at {pubsub_usage:.1%} "
                                 f"({self.stats['pubsub_in_use']}/{self.stats['pubsub_pool_size']})")
                    self.stats['warnings_sent'] += 1
                    
        except Exception as e:
            logger.error(f"Error checking connection thresholds: {e}")
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get current health status for monitoring APIs"""
        try:
            cluster_healthy = True
            pubsub_healthy = True
            
            if self.stats['cluster_pool_size'] > 0 and self.stats['cluster_in_use'] >= 0:
                cluster_usage = self.stats['cluster_in_use'] / self.stats['cluster_pool_size']
                cluster_healthy = cluster_usage < self.critical_threshold
            
            if self.stats['pubsub_pool_size'] > 0 and self.stats['pubsub_in_use'] >= 0:
                pubsub_usage = self.stats['pubsub_in_use'] / self.stats['pubsub_pool_size']
                pubsub_healthy = pubsub_usage < self.critical_threshold
            
            overall_healthy = cluster_healthy and pubsub_healthy
            
            return {
                'healthy': overall_healthy,
                'cluster_healthy': cluster_healthy,
                'pubsub_healthy': pubsub_healthy,
                'stats': self.stats,
                'last_check': self.stats.get('last_check'),
                'warnings_sent': self.stats.get('warnings_sent', 0),
                'errors_detected': self.stats.get('errors_detected', 0)
            }
            
        except Exception as e:
            logger.error(f"Error getting health status: {e}")
            return {
                'healthy': False,
                'error': str(e),
                'stats': self.stats
            }

# Global monitor instance
redis_monitor = RedisConnectionMonitor()

async def start_redis_monitoring(interval_seconds: int = 30):
    """Start Redis connection monitoring in background"""
    logger.info(f"Starting Redis connection monitoring (interval: {interval_seconds}s)")
    
    while True:
        try:
            await redis_monitor.check_connection_pools()
            await asyncio.sleep(interval_seconds)
        except Exception as e:
            logger.error(f"Redis monitoring error: {e}")
            await asyncio.sleep(interval_seconds)

def get_redis_health() -> Dict[str, Any]:
    """Get current Redis health status (sync version for APIs)"""
    return redis_monitor.get_health_status()
