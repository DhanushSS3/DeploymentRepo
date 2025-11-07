"""
Redis Circuit Breaker
Implements circuit breaker pattern for Redis operations to handle connection exhaustion gracefully
"""
import asyncio
import logging
import time
from enum import Enum
from typing import Any, Callable, Optional, Dict
from redis.exceptions import MaxConnectionsError, ConnectionError, TimeoutError

logger = logging.getLogger(__name__)

class CircuitState(Enum):
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Circuit is open, failing fast
    HALF_OPEN = "half_open"  # Testing if service is back

class RedisCircuitBreaker:
    """Circuit breaker for Redis operations"""
    
    def __init__(self, 
                 failure_threshold: int = 5,
                 recovery_timeout: int = 60,
                 expected_exception: tuple = (MaxConnectionsError, ConnectionError, TimeoutError)):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        
        self.failure_count = 0
        self.last_failure_time = None
        self.state = CircuitState.CLOSED
        self.stats = {
            'total_calls': 0,
            'successful_calls': 0,
            'failed_calls': 0,
            'circuit_open_count': 0,
            'last_failure': None,
            'last_success': None
        }
    
    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection"""
        self.stats['total_calls'] += 1
        
        # Check if circuit should transition from OPEN to HALF_OPEN
        if self.state == CircuitState.OPEN:
            if self._should_attempt_reset():
                self.state = CircuitState.HALF_OPEN
                logger.info("Redis circuit breaker transitioning to HALF_OPEN state")
            else:
                # Circuit is still open, fail fast
                self.stats['failed_calls'] += 1
                raise MaxConnectionsError("Redis circuit breaker is OPEN - failing fast")
        
        try:
            # Execute the function
            result = await func(*args, **kwargs)
            
            # Success - reset failure count and close circuit if needed
            self._on_success()
            return result
            
        except self.expected_exception as e:
            # Expected failure - increment failure count
            self._on_failure(e)
            raise
        except Exception as e:
            # Unexpected exception - don't count towards circuit breaker
            logger.error(f"Unexpected exception in circuit breaker: {e}")
            raise
    
    def _on_success(self):
        """Handle successful operation"""
        self.failure_count = 0
        self.stats['successful_calls'] += 1
        self.stats['last_success'] = time.time()
        
        if self.state == CircuitState.HALF_OPEN:
            self.state = CircuitState.CLOSED
            logger.info("Redis circuit breaker reset to CLOSED state after successful operation")
    
    def _on_failure(self, exception: Exception):
        """Handle failed operation"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        self.stats['failed_calls'] += 1
        self.stats['last_failure'] = time.time()
        
        logger.warning(f"Redis operation failed ({self.failure_count}/{self.failure_threshold}): {exception}")
        
        if self.failure_count >= self.failure_threshold:
            self.state = CircuitState.OPEN
            self.stats['circuit_open_count'] += 1
            logger.error(f"Redis circuit breaker OPENED after {self.failure_count} failures")
    
    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt reset"""
        if self.last_failure_time is None:
            return True
        return time.time() - self.last_failure_time >= self.recovery_timeout
    
    def get_state(self) -> Dict[str, Any]:
        """Get current circuit breaker state and statistics"""
        return {
            'state': self.state.value,
            'failure_count': self.failure_count,
            'failure_threshold': self.failure_threshold,
            'last_failure_time': self.last_failure_time,
            'recovery_timeout': self.recovery_timeout,
            'stats': self.stats.copy()
        }
    
    def is_available(self) -> bool:
        """Check if circuit breaker allows operations"""
        return self.state != CircuitState.OPEN
    
    def reset(self):
        """Manually reset the circuit breaker"""
        self.failure_count = 0
        self.last_failure_time = None
        self.state = CircuitState.CLOSED
        logger.info("Redis circuit breaker manually reset")

# Global circuit breaker instances
redis_cluster_breaker = RedisCircuitBreaker(
    failure_threshold=5,
    recovery_timeout=30,
    expected_exception=(MaxConnectionsError, ConnectionError, TimeoutError)
)

redis_pubsub_breaker = RedisCircuitBreaker(
    failure_threshold=3,
    recovery_timeout=15,
    expected_exception=(MaxConnectionsError, ConnectionError, TimeoutError)
)

async def safe_redis_operation(func: Callable, *args, use_pubsub: bool = False, **kwargs) -> Any:
    """
    Execute Redis operation with circuit breaker protection
    
    Args:
        func: Redis operation function to execute
        *args: Arguments for the function
        use_pubsub: Whether to use pubsub circuit breaker (default: cluster breaker)
        **kwargs: Keyword arguments for the function
    
    Returns:
        Result of the Redis operation
        
    Raises:
        MaxConnectionsError: If circuit breaker is open
        Other Redis exceptions: If operation fails
    """
    breaker = redis_pubsub_breaker if use_pubsub else redis_cluster_breaker
    return await breaker.call(func, *args, **kwargs)

def get_circuit_breaker_status() -> Dict[str, Any]:
    """Get status of all circuit breakers"""
    return {
        'cluster_breaker': redis_cluster_breaker.get_state(),
        'pubsub_breaker': redis_pubsub_breaker.get_state(),
        'timestamp': time.time()
    }

def reset_circuit_breakers():
    """Reset all circuit breakers"""
    redis_cluster_breaker.reset()
    redis_pubsub_breaker.reset()
    logger.info("All Redis circuit breakers reset")
