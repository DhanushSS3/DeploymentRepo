"""
Redis Connection Pool Capacity Analysis
Analyzes if current connection pool settings can handle 500 market ticks per second
"""
import asyncio
import time
from typing import Dict, Any

class RedisCapacityAnalyzer:
    """Analyze Redis connection pool capacity for high-frequency operations"""
    
    def __init__(self):
        self.cluster_pool_size = 500
        self.pubsub_pool_size = 50
        
    def analyze_market_listener_capacity(self, ticks_per_second: int = 500) -> Dict[str, Any]:
        """
        Analyze market listener capacity for handling market ticks
        
        Market Listener Operations per Tick:
        1. Cluster Pipeline: HSET for market data (1 connection)
        2. PubSub Pipeline: PUBLISH notifications (1 connection)
        
        Total: 2 connections per tick
        """
        
        # Market Listener Connection Usage
        cluster_connections_per_tick = 1  # Pipeline for market data storage
        pubsub_connections_per_tick = 1   # Pipeline for notifications
        
        # Calculate connections needed per second
        cluster_connections_per_second = ticks_per_second * cluster_connections_per_tick
        pubsub_connections_per_second = ticks_per_second * pubsub_connections_per_tick
        
        # Estimate connection hold time (pipeline execution time)
        avg_pipeline_duration_ms = 2.0  # 2ms average based on Redis cluster performance
        connection_hold_time_seconds = avg_pipeline_duration_ms / 1000
        
        # Calculate concurrent connections needed (Little's Law: N = λ × W)
        # N = number of concurrent connections
        # λ = arrival rate (operations per second)
        # W = average time each connection is held
        
        concurrent_cluster_connections = cluster_connections_per_second * connection_hold_time_seconds
        concurrent_pubsub_connections = pubsub_connections_per_second * connection_hold_time_seconds
        
        # Add safety margin (50% buffer for spikes and retries)
        safety_margin = 1.5
        required_cluster_connections = concurrent_cluster_connections * safety_margin
        required_pubsub_connections = concurrent_pubsub_connections * safety_margin
        
        # Calculate utilization
        cluster_utilization = required_cluster_connections / self.cluster_pool_size
        pubsub_utilization = required_pubsub_connections / self.pubsub_pool_size
        
        return {
            'ticks_per_second': ticks_per_second,
            'market_listener_analysis': {
                'cluster_pool': {
                    'pool_size': self.cluster_pool_size,
                    'connections_per_second': cluster_connections_per_second,
                    'concurrent_connections_needed': concurrent_cluster_connections,
                    'with_safety_margin': required_cluster_connections,
                    'utilization_percent': cluster_utilization * 100,
                    'sufficient': cluster_utilization < 0.8  # 80% threshold
                },
                'pubsub_pool': {
                    'pool_size': self.pubsub_pool_size,
                    'connections_per_second': pubsub_connections_per_second,
                    'concurrent_connections_needed': concurrent_pubsub_connections,
                    'with_safety_margin': required_pubsub_connections,
                    'utilization_percent': pubsub_utilization * 100,
                    'sufficient': pubsub_utilization < 0.8  # 80% threshold
                }
            }
        }
    
    def analyze_portfolio_calculator_capacity(self, 
                                            users_per_symbol: int = 100,
                                            symbols_per_tick: int = 10,
                                            ticks_per_second: int = 500) -> Dict[str, Any]:
        """
        Analyze portfolio calculator capacity
        
        Portfolio Calculator Operations per Symbol Update:
        1. Fetch market prices for symbols (1 cluster connection)
        2. Update user portfolios (N cluster connections, where N = users affected)
        3. Publish portfolio notifications (N pubsub connections)
        """
        
        # Calculate operations triggered by market ticks
        symbol_updates_per_second = symbols_per_tick * ticks_per_second
        users_affected_per_second = symbol_updates_per_second * users_per_symbol
        
        # Portfolio Calculator Connection Usage
        # Market price fetching (batched per symbol)
        price_fetch_connections_per_second = symbol_updates_per_second
        
        # Portfolio updates (one per affected user, but throttled to 200ms intervals)
        # Throttling reduces the effective rate
        throttle_interval = 0.2  # 200ms
        max_portfolio_updates_per_second = 1 / throttle_interval * users_per_symbol  # 500 users/sec max
        actual_portfolio_updates_per_second = min(users_affected_per_second, max_portfolio_updates_per_second)
        
        # Portfolio notifications (one per portfolio update)
        notification_connections_per_second = actual_portfolio_updates_per_second
        
        # Total connections per second
        total_cluster_per_second = price_fetch_connections_per_second + actual_portfolio_updates_per_second
        total_pubsub_per_second = notification_connections_per_second
        
        # Connection hold time
        avg_operation_duration_ms = 3.0  # 3ms average for portfolio operations
        connection_hold_time_seconds = avg_operation_duration_ms / 1000
        
        # Concurrent connections needed
        concurrent_cluster_connections = total_cluster_per_second * connection_hold_time_seconds
        concurrent_pubsub_connections = total_pubsub_per_second * connection_hold_time_seconds
        
        # Safety margin
        safety_margin = 2.0  # Higher margin for portfolio calculator due to complexity
        required_cluster_connections = concurrent_cluster_connections * safety_margin
        required_pubsub_connections = concurrent_pubsub_connections * safety_margin
        
        # Utilization
        cluster_utilization = required_cluster_connections / self.cluster_pool_size
        pubsub_utilization = required_pubsub_connections / self.pubsub_pool_size
        
        return {
            'users_per_symbol': users_per_symbol,
            'symbols_per_tick': symbols_per_tick,
            'ticks_per_second': ticks_per_second,
            'portfolio_calculator_analysis': {
                'symbol_updates_per_second': symbol_updates_per_second,
                'users_affected_per_second': users_affected_per_second,
                'throttled_updates_per_second': actual_portfolio_updates_per_second,
                'cluster_pool': {
                    'pool_size': self.cluster_pool_size,
                    'price_fetches_per_second': price_fetch_connections_per_second,
                    'portfolio_updates_per_second': actual_portfolio_updates_per_second,
                    'total_connections_per_second': total_cluster_per_second,
                    'concurrent_connections_needed': concurrent_cluster_connections,
                    'with_safety_margin': required_cluster_connections,
                    'utilization_percent': cluster_utilization * 100,
                    'sufficient': cluster_utilization < 0.8
                },
                'pubsub_pool': {
                    'pool_size': self.pubsub_pool_size,
                    'notifications_per_second': notification_connections_per_second,
                    'concurrent_connections_needed': concurrent_pubsub_connections,
                    'with_safety_margin': required_pubsub_connections,
                    'utilization_percent': pubsub_utilization * 100,
                    'sufficient': pubsub_utilization < 0.8
                }
            }
        }
    
    def analyze_other_services_capacity(self) -> Dict[str, Any]:
        """
        Analyze capacity for other Redis-using services
        
        Other Services:
        1. Provider Pending Monitor: ~10 operations/second
        2. Order Processing: ~50 operations/second
        3. User Management: ~20 operations/second
        4. Health Checks: ~5 operations/second
        """
        
        # Estimate other services usage
        other_services_cluster_per_second = 85  # Total estimated
        other_services_pubsub_per_second = 10   # Minimal pubsub usage
        
        # Connection hold time
        avg_operation_duration_ms = 2.0
        connection_hold_time_seconds = avg_operation_duration_ms / 1000
        
        # Concurrent connections
        concurrent_cluster_connections = other_services_cluster_per_second * connection_hold_time_seconds
        concurrent_pubsub_connections = other_services_pubsub_per_second * connection_hold_time_seconds
        
        # Safety margin
        safety_margin = 1.5
        required_cluster_connections = concurrent_cluster_connections * safety_margin
        required_pubsub_connections = concurrent_pubsub_connections * safety_margin
        
        # Utilization
        cluster_utilization = required_cluster_connections / self.cluster_pool_size
        pubsub_utilization = required_pubsub_connections / self.pubsub_pool_size
        
        return {
            'other_services_analysis': {
                'cluster_pool': {
                    'operations_per_second': other_services_cluster_per_second,
                    'concurrent_connections_needed': concurrent_cluster_connections,
                    'with_safety_margin': required_cluster_connections,
                    'utilization_percent': cluster_utilization * 100,
                    'sufficient': cluster_utilization < 0.8
                },
                'pubsub_pool': {
                    'operations_per_second': other_services_pubsub_per_second,
                    'concurrent_connections_needed': concurrent_pubsub_connections,
                    'with_safety_margin': required_pubsub_connections,
                    'utilization_percent': pubsub_utilization * 100,
                    'sufficient': pubsub_utilization < 0.8
                }
            }
        }
    
    def generate_comprehensive_analysis(self, ticks_per_second: int = 500) -> Dict[str, Any]:
        """Generate comprehensive capacity analysis"""
        
        market_analysis = self.analyze_market_listener_capacity(ticks_per_second)
        portfolio_analysis = self.analyze_portfolio_calculator_capacity(ticks_per_second=ticks_per_second)
        other_analysis = self.analyze_other_services_capacity()
        
        # Combine all utilizations
        total_cluster_utilization = (
            market_analysis['market_listener_analysis']['cluster_pool']['utilization_percent'] +
            portfolio_analysis['portfolio_calculator_analysis']['cluster_pool']['utilization_percent'] +
            other_analysis['other_services_analysis']['cluster_pool']['utilization_percent']
        )
        
        total_pubsub_utilization = (
            market_analysis['market_listener_analysis']['pubsub_pool']['utilization_percent'] +
            portfolio_analysis['portfolio_calculator_analysis']['pubsub_pool']['utilization_percent'] +
            other_analysis['other_services_analysis']['pubsub_pool']['utilization_percent']
        )
        
        # Recommendations
        recommendations = []
        
        if total_cluster_utilization > 80:
            recommendations.append(f"CRITICAL: Cluster pool utilization at {total_cluster_utilization:.1f}% - increase pool size")
        elif total_cluster_utilization > 60:
            recommendations.append(f"WARNING: Cluster pool utilization at {total_cluster_utilization:.1f}% - monitor closely")
        
        if total_pubsub_utilization > 80:
            recommendations.append(f"CRITICAL: PubSub pool utilization at {total_pubsub_utilization:.1f}% - increase pool size")
        elif total_pubsub_utilization > 60:
            recommendations.append(f"WARNING: PubSub pool utilization at {total_pubsub_utilization:.1f}% - monitor closely")
        
        if total_cluster_utilization <= 60 and total_pubsub_utilization <= 60:
            recommendations.append("GOOD: Current pool sizes are sufficient for the expected load")
        
        return {
            'analysis_timestamp': time.time(),
            'ticks_per_second': ticks_per_second,
            'current_pool_sizes': {
                'cluster_pool': self.cluster_pool_size,
                'pubsub_pool': self.pubsub_pool_size
            },
            'total_utilization': {
                'cluster_pool_percent': total_cluster_utilization,
                'pubsub_pool_percent': total_pubsub_utilization,
                'cluster_sufficient': total_cluster_utilization < 80,
                'pubsub_sufficient': total_pubsub_utilization < 80
            },
            'component_analyses': {
                'market_listener': market_analysis,
                'portfolio_calculator': portfolio_analysis,
                'other_services': other_analysis
            },
            'recommendations': recommendations,
            'capacity_summary': {
                'can_handle_500_tps': total_cluster_utilization < 80 and total_pubsub_utilization < 80,
                'max_sustainable_tps': int(500 * (80 / max(total_cluster_utilization, total_pubsub_utilization))),
                'bottleneck': 'cluster_pool' if total_cluster_utilization > total_pubsub_utilization else 'pubsub_pool'
            }
        }

def run_capacity_analysis():
    """Run and print capacity analysis"""
    analyzer = RedisCapacityAnalyzer()
    analysis = analyzer.generate_comprehensive_analysis(500)
    
    print("=" * 80)
    print("REDIS CONNECTION POOL CAPACITY ANALYSIS")
    print("=" * 80)
    print(f"Analysis for {analysis['ticks_per_second']} ticks per second")
    print(f"Current Pool Sizes: Cluster={analysis['current_pool_sizes']['cluster_pool']}, PubSub={analysis['current_pool_sizes']['pubsub_pool']}")
    print()
    
    print("TOTAL UTILIZATION:")
    print(f"  Cluster Pool: {analysis['total_utilization']['cluster_pool_percent']:.1f}%")
    print(f"  PubSub Pool:  {analysis['total_utilization']['pubsub_pool_percent']:.1f}%")
    print()
    
    print("CAPACITY SUMMARY:")
    print(f"  Can handle 500 TPS: {'YES' if analysis['capacity_summary']['can_handle_500_tps'] else 'NO'}")
    print(f"  Max sustainable TPS: {analysis['capacity_summary']['max_sustainable_tps']}")
    print(f"  Bottleneck: {analysis['capacity_summary']['bottleneck']}")
    print()
    
    print("RECOMMENDATIONS:")
    for rec in analysis['recommendations']:
        print(f"  • {rec}")
    print()
    
    return analysis

if __name__ == "__main__":
    run_capacity_analysis()
