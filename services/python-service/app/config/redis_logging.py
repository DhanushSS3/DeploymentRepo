"""
Dedicated Redis Pool Usage Logger
Creates separate log files for Redis connection pool monitoring and warnings
"""
import logging
import os
from pathlib import Path
from logging.handlers import RotatingFileHandler
import time

# Create logs directory if it doesn't exist
logs_dir = Path(__file__).parent.parent.parent / "logs"
logs_dir.mkdir(exist_ok=True)

# Redis Pool Usage Logger
redis_pool_logger = logging.getLogger('redis_pool')
redis_pool_logger.setLevel(logging.DEBUG)
redis_pool_logger.propagate = False  # Don't propagate to root logger

# Create rotating file handler for pool usage (50MB max, 5 backups)
pool_handler = RotatingFileHandler(
    logs_dir / "redis_pool_usage.log",
    maxBytes=50*1024*1024,  # 50MB
    backupCount=5,
    encoding='utf-8'
)
pool_handler.setLevel(logging.DEBUG)

# Create formatter for pool usage logs
pool_formatter = logging.Formatter(
    '%(asctime)s - %(levelname)s - POOL - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
pool_handler.setFormatter(pool_formatter)
redis_pool_logger.addHandler(pool_handler)

# Redis Connection Warnings Logger
redis_warnings_logger = logging.getLogger('redis_warnings')
redis_warnings_logger.setLevel(logging.WARNING)
redis_warnings_logger.propagate = False  # Don't propagate to root logger

# Create rotating file handler for warnings (10MB max, 3 backups)
warnings_handler = RotatingFileHandler(
    logs_dir / "redis_warnings.log",
    maxBytes=10*1024*1024,  # 10MB
    backupCount=3,
    encoding='utf-8'
)
warnings_handler.setLevel(logging.WARNING)

# Create formatter for warning logs
warnings_formatter = logging.Formatter(
    '%(asctime)s - %(levelname)s - %(name)s - %(funcName)s:%(lineno)d - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
warnings_handler.setFormatter(warnings_formatter)
redis_warnings_logger.addHandler(warnings_handler)

# Redis Connection Trace Logger (for detailed connection tracking)
redis_trace_logger = logging.getLogger('redis_trace')
redis_trace_logger.setLevel(logging.DEBUG)
redis_trace_logger.propagate = False

# Create rotating file handler for connection traces (100MB max, 3 backups)
trace_handler = RotatingFileHandler(
    logs_dir / "redis_connection_trace.log",
    maxBytes=100*1024*1024,  # 100MB
    backupCount=3,
    encoding='utf-8'
)
trace_handler.setLevel(logging.DEBUG)

# Create detailed formatter for trace logs
trace_formatter = logging.Formatter(
    '%(asctime)s.%(msecs)03d - %(levelname)s - %(name)s - %(funcName)s:%(lineno)d - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
trace_handler.setFormatter(trace_formatter)
redis_trace_logger.addHandler(trace_handler)

def log_pool_usage(pool_type: str, total: int, available: int, in_use: int, operation: str = ""):
    """Log Redis pool usage statistics"""
    usage_pct = (in_use / total * 100) if total > 0 else 0
    redis_pool_logger.info(
        f"{pool_type.upper()}_POOL | Total: {total} | Available: {available} | "
        f"In-Use: {in_use} | Usage: {usage_pct:.1f}% | Operation: {operation}"
    )

def log_pool_warning(pool_type: str, total: int, in_use: int, threshold: float, operation: str = ""):
    """Log Redis pool usage warning"""
    usage_pct = (in_use / total * 100) if total > 0 else 0
    redis_warnings_logger.warning(
        f"HIGH_POOL_USAGE | {pool_type.upper()}_POOL | "
        f"Usage: {usage_pct:.1f}% ({in_use}/{total}) | "
        f"Threshold: {threshold:.1f}% | Operation: {operation}"
    )

def log_pool_critical(pool_type: str, total: int, in_use: int, operation: str = ""):
    """Log Redis pool critical usage"""
    usage_pct = (in_use / total * 100) if total > 0 else 0
    redis_warnings_logger.error(
        f"CRITICAL_POOL_USAGE | {pool_type.upper()}_POOL | "
        f"Usage: {usage_pct:.1f}% ({in_use}/{total}) | Operation: {operation}"
    )

def log_connection_acquire(pool_type: str, operation: str, duration_ms: float = None):
    """Log connection acquisition"""
    duration_str = f" | Duration: {duration_ms:.2f}ms" if duration_ms else ""
    redis_trace_logger.debug(f"CONN_ACQUIRE | {pool_type.upper()} | {operation}{duration_str}")

def log_connection_release(pool_type: str, operation: str, duration_ms: float = None):
    """Log connection release"""
    duration_str = f" | Duration: {duration_ms:.2f}ms" if duration_ms else ""
    redis_trace_logger.debug(f"CONN_RELEASE | {pool_type.upper()} | {operation}{duration_str}")

def log_connection_error(pool_type: str, operation: str, error: str, retry_attempt: int = None):
    """Log connection errors"""
    retry_str = f" | Retry: {retry_attempt}" if retry_attempt else ""
    redis_warnings_logger.error(f"CONN_ERROR | {pool_type.upper()} | {operation} | {error}{retry_str}")

def log_pipeline_operation(pool_type: str, operation: str, commands_count: int, duration_ms: float = None):
    """Log pipeline operations"""
    duration_str = f" | Duration: {duration_ms:.2f}ms" if duration_ms else ""
    redis_trace_logger.debug(
        f"PIPELINE | {pool_type.upper()} | {operation} | Commands: {commands_count}{duration_str}"
    )

def log_connection_leak_detection(pool_type: str, expected_available: int, actual_available: int):
    """Log potential connection leaks"""
    if expected_available != actual_available:
        redis_warnings_logger.warning(
            f"POTENTIAL_LEAK | {pool_type.upper()}_POOL | "
            f"Expected Available: {expected_available} | Actual Available: {actual_available} | "
            f"Potential Leak: {expected_available - actual_available}"
        )

class RedisConnectionTracker:
    """Track Redis connections for leak detection"""
    
    def __init__(self):
        self.active_connections = {}
        self.operation_stats = {
            'total_operations': 0,
            'active_operations': 0,
            'completed_operations': 0,
            'failed_operations': 0
        }
    
    def start_operation(self, operation_id: str, pool_type: str, operation: str):
        """Track start of Redis operation"""
        self.active_connections[operation_id] = {
            'pool_type': pool_type,
            'operation': operation,
            'start_time': time.time(),
            'status': 'active'
        }
        self.operation_stats['total_operations'] += 1
        self.operation_stats['active_operations'] += 1
        
        redis_trace_logger.debug(
            f"OP_START | {operation_id} | {pool_type.upper()} | {operation} | "
            f"Active: {self.operation_stats['active_operations']}"
        )
    
    def end_operation(self, operation_id: str, success: bool = True, error: str = None):
        """Track end of Redis operation"""
        if operation_id in self.active_connections:
            conn_info = self.active_connections[operation_id]
            duration = (time.time() - conn_info['start_time']) * 1000  # ms
            
            if success:
                self.operation_stats['completed_operations'] += 1
                status = "SUCCESS"
            else:
                self.operation_stats['failed_operations'] += 1
                status = f"FAILED - {error}" if error else "FAILED"
            
            self.operation_stats['active_operations'] -= 1
            
            redis_trace_logger.debug(
                f"OP_END | {operation_id} | {conn_info['pool_type'].upper()} | "
                f"{conn_info['operation']} | {status} | Duration: {duration:.2f}ms | "
                f"Active: {self.operation_stats['active_operations']}"
            )
            
            del self.active_connections[operation_id]
    
    def get_stats(self):
        """Get connection tracking statistics"""
        return {
            'active_operations': len(self.active_connections),
            'stats': self.operation_stats.copy(),
            'active_connections': list(self.active_connections.keys())
        }
    
    def check_for_leaks(self):
        """Check for potential connection leaks (long-running operations)"""
        current_time = time.time()
        leak_threshold = 30  # 30 seconds
        
        for op_id, conn_info in self.active_connections.items():
            duration = current_time - conn_info['start_time']
            if duration > leak_threshold:
                redis_warnings_logger.warning(
                    f"LONG_RUNNING_OP | {op_id} | {conn_info['pool_type'].upper()} | "
                    f"{conn_info['operation']} | Duration: {duration:.1f}s"
                )

# Global connection tracker
connection_tracker = RedisConnectionTracker()

def get_redis_log_stats():
    """Get Redis logging statistics"""
    return {
        'pool_log_file': str(logs_dir / "redis_pool_usage.log"),
        'warnings_log_file': str(logs_dir / "redis_warnings.log"),
        'trace_log_file': str(logs_dir / "redis_connection_trace.log"),
        'connection_tracker_stats': connection_tracker.get_stats(),
        'logs_directory': str(logs_dir)
    }
