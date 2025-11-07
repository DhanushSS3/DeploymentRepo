import os
from pathlib import Path
from dotenv import load_dotenv
from redis.asyncio.cluster import RedisCluster
from redis.asyncio.cluster import ClusterNode
import redis.asyncio as redis

# Load environment variables from root .env file
# Path: services/python-service/app/config/redis_config.py -> root
# __file__ = .../services/python-service/app/config/redis_config.py
# .parent = .../services/python-service/app/config
# .parent.parent = .../services/python-service/app
# .parent.parent.parent = .../services/python-service
# .parent.parent.parent.parent = .../services
# We need one more .parent to get to root
root_dir = Path(__file__).parent.parent.parent.parent.parent
env_path = root_dir / '.env'
load_dotenv(env_path)

# Debug logging
import logging
logger = logging.getLogger(__name__)
logger.info("Redis config loading .env from: %s", env_path.resolve())
logger.info("Redis config .env file exists: %s", env_path.exists())
logger.info("REDIS_PASSWORD loaded: %s", "Yes" if os.getenv("REDIS_PASSWORD") else "No")

class RedisConfig:
    """Redis Cluster configuration for market data storage"""
    
    def __init__(self):
        # Redis cluster nodes from environment
        redis_hosts_env = os.getenv("REDIS_HOSTS") or "127.0.0.1:7001"
        redis_hosts = redis_hosts_env.split(",")
        
        # Get password from environment
        redis_password = os.getenv("REDIS_PASSWORD") or "admin@livefxhub@123"

        # Parse all hosts for startup nodes using ClusterNode objects
        startup_nodes = []
        for host in redis_hosts:
            host_ip, host_port = host.strip().split(":")
            startup_nodes.append(ClusterNode(host_ip, int(host_port)))

        # Address mapping function for local development
        def address_remap(address):
            host, port = address
            mapping = {
                "172.28.0.2": ("127.0.0.1", 7001),
                "172.28.0.3": ("127.0.0.1", 7002),  
                "172.28.0.4": ("127.0.0.1", 7003),
                "172.28.0.5": ("127.0.0.1", 7004),
                "172.28.0.6": ("127.0.0.1", 7005),
                "172.28.0.7": ("127.0.0.1", 7006),
                "172.28.0.8": ("127.0.0.1", 7007),
                "172.28.0.9": ("127.0.0.1", 7008),
                "172.28.0.10": ("127.0.0.1", 7009),
            }
            return mapping.get(host, (host, port))

        # Create optimized configuration for 8-core CPU with high-frequency data
        # Using only supported parameters for redis-py compatibility
        self.cluster_config = {
            "startup_nodes": startup_nodes,
            "decode_responses": True,
            "password": redis_password,
            "health_check_interval": 30,
            "socket_connect_timeout": 5,    # Faster connection timeout
            "socket_timeout": 5,            # Faster socket timeout
            "max_connections": 500,         # Increased from 200 to handle high-frequency operations
            "address_remap": address_remap,
        }
    
    def get_cluster(self):
        """Get async Redis cluster connection"""
        return RedisCluster(**self.cluster_config)

# Global async Redis cluster instance
redis_cluster = RedisConfig().get_cluster()

# Optimized Redis connection pool for pub/sub operations
redis_password = os.getenv("REDIS_PASSWORD") or "admin@livefxhub@123"
redis_pubsub_client = redis.Redis(
    host='127.0.0.1', 
    port=7001, 
    decode_responses=True,
    password=redis_password,
    max_connections=50,          # Increased from 10 to handle high-frequency pub/sub operations
    socket_connect_timeout=5,    # Connection timeout
    socket_timeout=None,         # Disable read timeout for long-lived pub/sub listen
    health_check_interval=30     # Health check every 30s
)