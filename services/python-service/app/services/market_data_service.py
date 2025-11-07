import orjson
import time
import asyncio
from typing import Dict, Any, Optional
from ..config.redis_config import redis_cluster, redis_pubsub_client
from ..services.logging.execution_price_logger import (
    log_market_processing, log_redis_issue, log_price_inconsistency, 
    log_missing_price_data
)
import logging
import math

logger = logging.getLogger(__name__)

class MarketDataService:
    """Service for processing and storing market price data in Redis"""
    
    def __init__(self):
        self.redis = redis_cluster
        self.pubsub_redis = redis_pubsub_client
        self.staleness_threshold = 5  # 5 seconds
    
    async def process_market_feed(self, feed_data: Dict[str, Any]) -> bool:
        """
        Process bulk market feed data and store in Redis with pipeline sharding
        Handles partial price updates (buy or sell only)
        
        Args:
            feed_data: Dictionary containing 'market_prices' with symbol price data
            
        Returns:
            bool: True if processing successful, False otherwise
        """
        start_time = time.time()
        
        try:
            market_prices = feed_data.get('market_prices', {})
            # logger.info(f"🔍 MARKET_SERVICE: Processing {len(market_prices)} symbols")
            # logger.info(f"🔍 MARKET_SERVICE: Feed data keys: {list(feed_data.keys())}")
            
            if not market_prices:
                logger.error(f"❌ MARKET_SERVICE: No market_prices found in market feed. Feed data: {feed_data}")
                return False
            
            current_timestamp = int(time.time() * 1000)  # epoch milliseconds
            
            # Batch validate and prepare data with partial update support
            valid_updates = []
            for symbol, price_data in market_prices.items():
                processed_data = await self._validate_and_parse_partial_price(symbol, price_data, current_timestamp)
                if processed_data:
                    valid_updates.append(processed_data)
            
            if not valid_updates:
                logger.error(f"❌ MARKET_SERVICE: No valid price updates to process from {len(market_prices)} symbols")
                return False
            
            # logger.info(f"✅ MARKET_SERVICE: Processing {len(valid_updates)} valid updates to Redis")
            
            # Process partial updates with Redis merge logic
            await self._process_partial_updates_sharded(valid_updates)
            
            # Publish price update notifications for Portfolio Calculator
            await self._publish_price_update_notifications(valid_updates)
            
            # Log market processing metrics
            processing_time_ms = (time.time() - start_time) * 1000
            log_market_processing(
                symbols_processed=len(valid_updates),
                processing_time_ms=processing_time_ms,
                batch_size=len(market_prices),
                success=True,
                total_symbols_received=len(market_prices),
                valid_symbols=len(valid_updates)
            )
            
            logger.debug(f"Processed {len(market_prices)} symbol updates in {processing_time_ms:.2f}ms")
            return True
            
        except Exception as e:
            processing_time_ms = (time.time() - start_time) * 1000
            log_market_processing(
                symbols_processed=0,
                processing_time_ms=processing_time_ms,
                batch_size=len(market_prices) if 'market_prices' in locals() else 0,
                success=False,
                error=str(e)
            )
            logger.error(f"Failed to process market feed: {e}")
            return False
    
    async def _process_partial_updates_sharded(self, valid_updates: list, shard_size: int = 1000):
        """
        Process partial price updates using Redis pipelines - ZERO TICK LOSS optimized
        
        Args:
            valid_updates: List of validated partial price update tuples
            shard_size: Large shard size for maximum throughput
        """
        if not valid_updates:
            return
            
        # Use larger shards for maximum throughput - process more symbols per pipeline
        shards = [valid_updates[i:i + shard_size] for i in range(0, len(valid_updates), shard_size)]
        
        # Process shards concurrently with higher concurrency
        tasks = []
        for shard in shards:
            task = self._process_partial_update_shard(shard)
            tasks.append(task)
        
        # Use asyncio.gather with return_exceptions to handle partial failures
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Log any shard processing failures
        failed_shards = sum(1 for result in results if isinstance(result, Exception))
        if failed_shards > 0:
            logger.warning(f"Failed to process {failed_shards}/{len(shards)} shards")
    
    async def _process_partial_update_shard(self, update_shard: list):
        """
        Process a single shard of partial price updates with Redis merge logic
        
        Args:
            update_shard: List of partial price update tuples for this shard
        """
        max_retries = 3  # Increased retries for better reliability
        retry_delay = 0.01  # 10ms - ultra-fast retry for zero tick loss
        
        for attempt in range(max_retries):
            try:
                # Use async context manager to ensure proper connection cleanup
                async with self.redis.pipeline() as pipe:
                    # Batch all operations in pipeline for better performance
                    for symbol, update_fields, timestamp in update_shard:
                        key = f"market:{symbol}"
                        update_fields['ts'] = timestamp
                        
                        # Use HSET with mapping for atomic field updates
                        pipe.hset(key, mapping=update_fields)
                        
                        # Set expiration to prevent stale data accumulation (5 minutes)
                        pipe.expire(key, 300)
                    
                    # Execute all operations atomically
                    results = await pipe.execute()
                    # logger.info(f"✅ REDIS_STORAGE: Successfully stored {len(update_shard)} symbols to Redis")
                    # logger.info(f"✅ REDIS_STORAGE: Pipeline results: {len(results)} operations completed")
                    return  # Success, exit retry loop
                    
            except (ConnectionError, TimeoutError, OSError) as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Redis connection error on attempt {attempt + 1}/{max_retries}: {e}. Retrying in {retry_delay}s...")
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    logger.error(f"Failed to process partial update shard after {max_retries} attempts: {e}")
                    # Continue processing other shards even if this one fails
                    
            except Exception as e:
                logger.error(f"Unexpected error processing partial update shard: {e}")
                break  # Don't retry for unexpected errors
    
    async def _validate_and_parse_partial_price(self, symbol: str, price_data: Dict[str, str], timestamp: int) -> Optional[tuple]:
        """
        Parse individual symbol partial price data (no bid/ask relational validation)
        Handles cases where only 'buy' or 'sell' is provided. Any provided side is accepted
        and written as-is after float parsing.
        
        Price mapping: buy -> ask (price users pay to buy), sell -> bid (price users get when selling)
        
        Args:
            symbol: Trading symbol (e.g., 'EURUSD')
            price_data: Dict with optional 'buy' and/or 'sell' price strings
            timestamp: Current timestamp in milliseconds
            
        Returns:
            tuple: (symbol, update_fields_dict, timestamp) if valid, None otherwise
        """
        try:
            # Extract price data (partial updates allowed)
            buy_str = price_data.get('buy')
            sell_str = price_data.get('sell')

            if buy_str is None and sell_str is None:
                log_missing_price_data(symbol, ["buy", "sell"], source="websocket_feed")
                logger.debug(f"No price data provided for {symbol}")
                return None

            update_fields = {}
            parse_errors = []

            # Parse buy price if provided (buy -> ask)
            if buy_str is not None:
                try:
                    ask_price = float(buy_str)
                    update_fields['ask'] = ask_price
                except (ValueError, TypeError) as e:
                    parse_errors.append(f"buy: {e}")
                    logger.debug(f"Failed to parse buy price for {symbol}: {e}")

            # Parse sell price if provided (sell -> bid)
            if sell_str is not None:
                try:
                    bid_price = float(sell_str)
                    update_fields['bid'] = bid_price
                except (ValueError, TypeError) as e:
                    parse_errors.append(f"sell: {e}")
                    logger.debug(f"Failed to parse sell price for {symbol}: {e}")

            # If neither side parsed successfully, skip
            if not update_fields:
                log_missing_price_data(symbol, parse_errors, source="websocket_feed", 
                                     raw_data=price_data)
                return None

            # Check for price inconsistency if we have both bid and ask
            if 'bid' in update_fields and 'ask' in update_fields:
                bid = update_fields['bid']
                ask = update_fields['ask']
                if ask < bid:
                    log_price_inconsistency(symbol, bid, ask, source="websocket_feed",
                                          raw_data=price_data)
                    # Still process the data but log the issue
                    logger.warning(f"Price inconsistency detected for {symbol}: bid={bid} ask={ask}")

            return (symbol, update_fields, timestamp)

        except Exception as e:
            logger.error(f"Unexpected error processing partial price for {symbol}: {e}")
            log_missing_price_data(symbol, ["parsing_error"], source="websocket_feed",
                                 error=str(e), raw_data=price_data)
            return None
    
    async def _get_existing_price_for_validation(self, symbol: str) -> Optional[Dict[str, float]]:
        """
        Get existing price from Redis for validation purposes
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Dict with existing bid/ask prices or None
        """
        try:
            key = f"market:{symbol}"
            price_data = await self.redis.hmget(key, ["bid", "ask"])
            
            if price_data and any(price_data):
                result = {}
                if price_data[0]:  # bid exists
                    result['bid'] = float(price_data[0])
                if price_data[1]:  # ask exists
                    result['ask'] = float(price_data[1])
                return result
            
            return None
            
        except Exception as e:
            logger.debug(f"Could not get existing price for {symbol}: {e}")
            return None
    
    async def get_symbol_price(self, symbol: str) -> Optional[Dict[str, float]]:
        """
        Get current price for a symbol with staleness check.
        As of Oct 2025, DO NOT reject stale data for order placement/close. Staleness only affects monitoring, not endpoint logic.
        """
        try:
            key = f"market:{symbol}"
            price_data = await self.redis.hmget(key, ["bid", "ask", "ts"])
            if not all(price_data):
                logger.debug(f"No price data found for {symbol}")
                return None
            bid, ask, ts = price_data
            timestamp = int(ts)
            current_time = int(time.time() * 1000)
            if current_time - timestamp > (self.staleness_threshold * 1000):
                logger.warning(f"Stale price data for {symbol}: {(current_time - timestamp) / 1000}s old (proceeding anyway)")
            return {
                "bid": float(bid),
                "ask": float(ask),
                "ts": timestamp
            }
        except Exception as e:
            logger.error(f"Failed to get price for {symbol}: {e}")
            return None
    
    async def get_multiple_prices(self, symbols: list) -> Dict[str, Dict[str, float]]:
        """
        Get prices for multiple symbols efficiently
        
        Args:
            symbols: List of trading symbols
            
        Returns:
            Dict mapping symbol to price data
        """
        try:
            async with self.redis.pipeline() as pipe:
                # Batch fetch all symbols
                for symbol in symbols:
                    key = f"market:{symbol}"
                    pipe.hmget(key, ["bid", "ask", "ts"])
                
                results = await pipe.execute()
            
            current_time = int(time.time() * 1000)
            
            prices = {}
            for i, symbol in enumerate(symbols):
                price_data = results[i]
                if all(price_data):
                    bid, ask, ts = price_data
                    timestamp = int(ts)
                    
                    # Check staleness
                    if current_time - timestamp <= (self.staleness_threshold * 1000):
                        prices[symbol] = {
                            "bid": float(bid),
                            "ask": float(ask),
                            "ts": timestamp
                        }
            
            return prices
            
        except Exception as e:
            logger.error(f"Failed to get multiple prices: {e}")
            return {}
    
    async def get_all_prices_snapshot(self) -> Dict[str, Any]:
        """
        Get complete market snapshot for monitoring/dashboards
        Build from structured hashes instead of separate JSON storage
        
        Returns:
            Dict with all current prices
        """
        try:
            # Scan for all market: keys to get symbols (handle hash-tagged keys)
            symbols = []
            async for key in self.redis.scan_iter(match="market:*", count=1000):
                if key.startswith("market:"):
                    # Extract symbol from key: market:EURUSD -> EURUSD
                    symbol = key[7:]  # Remove "market:" prefix
                    
                    if symbol and symbol != "prices":  # Skip market:prices if it exists
                        symbols.append(symbol)
            
            if not symbols:
                return {"timestamp": int(time.time() * 1000), "total_symbols": 0, "prices": {}}
            
            # Batch fetch all symbol prices
            async with self.redis.pipeline() as pipe:
                for symbol in symbols:
                    key = f"market:{symbol}"
                    pipe.hmget(key, ["bid", "ask", "ts"])
                
                results = await pipe.execute()
            
            # Build snapshot with staleness check
            current_time = int(time.time() * 1000)
            valid_prices = {}
            
            for i, symbol in enumerate(symbols):
                price_data = results[i]
                if all(price_data):
                    bid, ask, ts = price_data
                    timestamp = int(ts)
                    
                    # Check staleness
                    if current_time - timestamp <= (self.staleness_threshold * 1000):
                        valid_prices[symbol] = {
                            "bid": float(bid),
                            "ask": float(ask),
                            "ts": timestamp
                        }
            
            return {
                "timestamp": current_time,
                "total_symbols": len(valid_prices),
                "prices": valid_prices
            }
            
        except Exception as e:
            logger.error(f"Failed to get price snapshot: {e}")
            return {"timestamp": int(time.time() * 1000), "total_symbols": 0, "prices": {}}
    
    async def _publish_price_update_notifications(self, valid_updates: list):
        """
        Publish symbol notifications to Redis Pub/Sub for Portfolio Calculator (optimized for high-frequency)
        
        Args:
            valid_updates: List of validated price update tuples (symbol, update_fields, timestamp)
        """
        if not valid_updates:
            return
        
        # Extract unique symbols from valid updates (avoid duplicates)
        symbols_in_message = list(set(update[0] for update in valid_updates))
        
        if not symbols_in_message:
            return
            
        logger.debug(f"Publishing price update notifications for {len(symbols_in_message)} unique symbols")
        
        # Batch publish notifications for better performance
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Use async context manager for pipeline to ensure proper connection cleanup
                async with self.pubsub_redis.pipeline() as pipe:
                    for symbol in symbols_in_message:
                        pipe.publish("market_price_updates", symbol)
                    
                    # Execute all publications at once
                    await pipe.execute()
                    
                logger.debug(f"Batch published {len(symbols_in_message)} symbol notifications")
                return  # Success, exit retry loop
                
            except (ConnectionError, TimeoutError, OSError) as e:
                if attempt < max_retries - 1:
                    logger.warning(f"PubSub connection error on attempt {attempt + 1}/{max_retries}: {e}. Retrying...")
                    await asyncio.sleep(0.01 * (2 ** attempt))  # Exponential backoff
                else:
                    logger.error(f"Failed to batch publish notifications after {max_retries} attempts: {e}")
                    # Fallback to individual publishing
                    for symbol in symbols_in_message:
                        try:
                            await self.pubsub_redis.publish("market_price_updates", symbol)
                        except Exception as symbol_error:
                            logger.error(f"Failed to publish symbol {symbol}: {symbol_error}")
            except Exception as e:
                logger.error(f"Unexpected error in batch publish: {e}")
                break
    
    def is_price_stale(self, timestamp: int) -> bool:
        """Check if price timestamp is stale (>5s old)"""
        current_time = int(time.time() * 1000)
        return (current_time - timestamp) > (self.staleness_threshold * 1000)
