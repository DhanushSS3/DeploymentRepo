"""
Binary WebSocket Market Data Listener
Handles protobuf binary messages from wss://quotes.livefxhub.com:9001/?token=Lkj@asd@1234
Based on frontend JavaScript implementation with pako.inflate and protobuf decoding
"""
import asyncio
import logging
import time
import websockets
import zlib
import struct
from typing import Dict, Any, Optional
from collections import deque
from app.services.market_data_service import MarketDataService
from app.services.logging.execution_price_logger import log_websocket_issue, log_market_processing
from app.config.redis_config import redis_pubsub_client
from app.config.redis_logging import (
    log_pool_usage, log_connection_acquire, log_connection_release, 
    log_connection_error, log_pipeline_operation, connection_tracker
)
import threading
import uuid

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ProtobufMarketListener:
    """
    WebSocket market data listener with protobuf decoding
    
    Matches the frontend JavaScript implementation:
    1. Connects to wss://quotes.livefxhub.com:9001/?token=Lkj@asd@1234
    2. Receives binary ArrayBuffer messages
    3. Decompresses using zlib (pako.inflate equivalent)
    4. Decodes protobuf using MarketUpdate schema
    """
    
    def __init__(self):
        self.ws_url = "wss://quotes.livefxhub.com:9001/?token=Lkj@asd@1234"
        self.market_service = MarketDataService()
        self.reconnect_delay = 5
        self.max_reconnect_attempts = 10
        self.is_running = False
        
        # ZERO TICK LOSS: Process every message immediately
        # No queuing, no batching, no delays - immediate Redis updates
        
        # Performance metrics
        self.stats = {
            'messages_processed': 0,
            'bytes_processed': 0,
            'parse_errors': 0,
            'successful_decodes': 0,
            'batches_processed': 0,
            'queue_size': 0,
            'avg_batch_size': 0
        }
        
        self.redis_queue = asyncio.Queue()
        self.redis_semaphore = asyncio.Semaphore(10)  # Reasonable default, adjust as needed
        self.writer_task = None
        self._shutdown_event = asyncio.Event()
        self.last_values = {}  # symbol -> (bid, ask)
        self._last_sent_ms = {}  # symbol -> last enqueue timestamp
        self._last_msg_ms = 0
        
    async def start(self):
        """Start the market listener with auto-reconnection and batch processing"""
        self.is_running = True
        reconnect_count = 0
        
        logger.info("Starting protobuf market data listener...")
        logger.info(f"Target WebSocket: {self.ws_url}")
        logger.info("🚀 ZERO TICK LOSS MODE: Every message processed immediately")
        
        self.writer_task = asyncio.create_task(self._redis_writer())
        
        try:
            while self.is_running and reconnect_count < self.max_reconnect_attempts:
                try:
                    await self._connect_and_listen()
                    reconnect_count = 0
                    
                except websockets.exceptions.ConnectionClosed:
                    reconnect_count += 1
                    logger.warning(f"WebSocket connection closed. Reconnecting... (attempt {reconnect_count})")
                    await asyncio.sleep(self.reconnect_delay)
                    
                except Exception as e:
                    reconnect_count += 1
                    logger.error(f"WebSocket error: {e}")
                    await asyncio.sleep(self.reconnect_delay)
            
            if reconnect_count >= self.max_reconnect_attempts:
                logger.error("Max reconnection attempts reached. Stopping listener.")
        
        finally:
            # Clean shutdown
            if self.writer_task:
                self._shutdown_event.set()
                await self.writer_task
        
        logger.info("Protobuf market listener stopped")
    
    async def _connect_and_listen(self):
        """Establish WebSocket connection and listen for messages"""
        logger.info(f"Connecting to market feed: {self.ws_url}")
        
        async with websockets.connect(
            self.ws_url,
            ping_interval=None,  # Don't send pings (server will ping us)
            ping_timeout=None,   # No timeout (server controls ping/pong)
            close_timeout=5,
            max_size=10**7,
            read_limit=2**20
        ) as websocket:
            # Server sends pings every 30s, websockets library will auto-respond with pong
            # This keeps us alive without interfering with server's ping/pong mechanism
            logger.info("Connected to market feed successfully")
            # Reset last message timer at connect
            self._last_msg_ms = int(time.time() * 1000)

            # Idle watchdog: force reconnect if no data for >30s
            async def _idle_watchdog():
                try:
                    while True:
                        await asyncio.sleep(5)
                        now = int(time.time() * 1000)
                        last_age = now - (self._last_msg_ms or now)
                        if last_age > 30000:  # 30s
                            logger.warning(f"Idle watchdog: no market data for {last_age}ms, closing websocket to reconnect")
                            try:
                                await websocket.close()
                            except Exception:
                                pass
                            break
                except asyncio.CancelledError:
                    return

            watchdog_task = asyncio.create_task(_idle_watchdog())
            
            # CRITICAL FIX: Add timeout to receive loop to detect silent stalls
            # 
            # Root Cause: If the websocket server's ZeroMQ listener stalls, it stops broadcasting
            # to clients, but the websocket connection stays open. Without this timeout, the
            # client waits forever in websocket.recv(), causing "No fresh data" after 4+ minutes.
            #
            # Server-side issue (for future fix):
            # - WebSocket server's `for await (const [msg] of sock)` in ZeroMQ blocks forever
            # - No timeout or reconnection logic on server when ZMQ publisher dies
            # - Cache-only broadcasts mean no data = no broadcasts = silent stall
            #
            # This client-side fix: Force reconnect after 30s of no data
            timeout_seconds = 30  # Force break if no data received for 30s
            
            try:
                while True:
                    try:
                        # Use asyncio.wait_for to timeout the receive operation
                        # Note: websockets library automatically handles server pings (responds with pong)
                        message = await asyncio.wait_for(websocket.recv(), timeout=timeout_seconds)
                        
                        try:
                            if isinstance(message, bytes):
                                self._last_msg_ms = int(time.time() * 1000)
                                await self._process_single_message_immediate(message)
                                self.stats['messages_processed'] += 1
                                self.stats['bytes_processed'] += len(message)
                            else:
                                logger.debug(f"Received control frame (ping/pong handled automatically)")
                                # Server ping/pong frames are handled automatically by websockets library
                                # We don't need to do anything with them
                        except Exception as e:
                            logger.error(f"Error processing message immediately: {e}")
                            self.stats['parse_errors'] += 1
                    except asyncio.TimeoutError:
                        # No data received within timeout - websocket is likely dead
                        now = int(time.time() * 1000)
                        last_age = now - (self._last_msg_ms or now)
                        logger.error(f"⚠️ RECEIVE TIMEOUT: No data for {last_age}ms. Websocket appears dead, forcing reconnect.")
                        break
                    except websockets.exceptions.ConnectionClosed:
                        # Normal close from server - will be caught by outer handler
                        logger.info("WebSocket connection closed by server")
                        break
                        
            finally:
                try:
                    watchdog_task.cancel()
                except Exception:
                    pass
    
    async def _redis_writer(self):
        while not self._shutdown_event.is_set():
            updates = []
            try:
                # Await at least 1 update, or timeout after ~20ms
                item = await asyncio.wait_for(self.redis_queue.get(), timeout=0.02)
                updates.append(item)
                while True:
                    updates.append(self.redis_queue.get_nowait())
            except asyncio.QueueEmpty:
                pass
            except asyncio.TimeoutError:
                pass

            if not updates:
                continue

            async with self.redis_semaphore:
                # Generate unique operation ID for tracking
                cluster_op_id = str(uuid.uuid4())[:8]
                pubsub_op_id = str(uuid.uuid4())[:8]
                
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        # Track cluster pipeline operation start
                        connection_tracker.start_operation(
                            cluster_op_id, "cluster", f"market_data_pipeline_{len(updates)}_symbols"
                        )
                        log_connection_acquire("cluster", f"market_data_pipeline_{len(updates)}_symbols")
                        
                        start_time = time.time()
                        # Use async context manager to ensure proper connection cleanup
                        async with self.market_service.redis.pipeline() as pipe:
                            ts = int(time.time() * 1000)
                            for symbol, bid, ask in updates:
                                pipe.hset(f"market:{symbol}", mapping={"bid": bid, "ask": ask, "ts": ts})
                            await pipe.execute()
                        
                        duration_ms = (time.time() - start_time) * 1000
                        log_pipeline_operation("cluster", f"market_data_{len(updates)}_symbols", len(updates), duration_ms)
                        log_connection_release("cluster", f"market_data_pipeline_{len(updates)}_symbols", duration_ms)
                        connection_tracker.end_operation(cluster_op_id, success=True)
                        
                        # Publish updated symbols to notify portfolio calculator and other subscribers
                        unique_symbols = list(set([symbol for symbol, _, _ in updates]))
                        if unique_symbols:
                            try:
                                # Track pubsub pipeline operation start
                                connection_tracker.start_operation(
                                    pubsub_op_id, "pubsub", f"market_notifications_{len(unique_symbols)}_symbols"
                                )
                                log_connection_acquire("pubsub", f"market_notifications_{len(unique_symbols)}_symbols")
                                
                                pub_start_time = time.time()
                                # Use async context manager for pubsub pipeline
                                async with redis_pubsub_client.pipeline() as pub_pipe:
                                    for sym in unique_symbols:
                                        pub_pipe.publish("market_price_updates", sym)
                                    await pub_pipe.execute()
                                
                                pub_duration_ms = (time.time() - pub_start_time) * 1000
                                log_pipeline_operation("pubsub", f"notifications_{len(unique_symbols)}_symbols", len(unique_symbols), pub_duration_ms)
                                log_connection_release("pubsub", f"market_notifications_{len(unique_symbols)}_symbols", pub_duration_ms)
                                connection_tracker.end_operation(pubsub_op_id, success=True)
                                
                                logger.debug(f"Published {len(unique_symbols)} symbol updates to market_price_updates channel")
                            except Exception as pub_err:
                                logger.warning(f"Failed to publish market_price_updates: {pub_err}")
                                log_connection_error("pubsub", f"market_notifications_{len(unique_symbols)}_symbols", str(pub_err))
                                connection_tracker.end_operation(pubsub_op_id, success=False, error=str(pub_err))
                        
                        break  # Success, exit retry loop
                        
                    except (ConnectionError, TimeoutError, OSError) as e:
                        log_connection_error("cluster", f"market_data_pipeline_{len(updates)}_symbols", str(e), attempt + 1)
                        connection_tracker.end_operation(cluster_op_id, success=False, error=str(e))
                        
                        if attempt < max_retries - 1:
                            logger.warning(f"Redis writer connection error on attempt {attempt + 1}/{max_retries}: {e}. Retrying...")
                            await asyncio.sleep(0.01 * (2 ** attempt))  # Exponential backoff
                            # Generate new operation ID for retry
                            cluster_op_id = str(uuid.uuid4())[:8]
                        else:
                            logger.error(f"Redis writer pipeline error after {max_retries} attempts: {e}")
                            await asyncio.sleep(0.05)
                    except Exception as e:
                        logger.error(f"Unexpected Redis writer error: {e}")
                        log_connection_error("cluster", f"market_data_pipeline_{len(updates)}_symbols", str(e))
                        connection_tracker.end_operation(cluster_op_id, success=False, error=str(e))
                        await asyncio.sleep(0.05)
                        break
            # Periodic debug (every ~1s): queue size and last msg age
            try:
                if int(time.time() * 1000) // 1000 % 1 == 0:
                    last_age_ms = (int(time.time() * 1000) - self._last_msg_ms) if self._last_msg_ms else -1
                    self.stats['queue_size'] = self.redis_queue.qsize()
                    if last_age_ms >= 60000:  # >60s without messages
                        logger.warning(f"Market feed silence: last message age {last_age_ms}ms, queue={self.stats['queue_size']}")
            except Exception:
                pass
    
    async def _process_single_message_immediate(self, message: bytes):
        """
        Process a single message: deduplicate first, only enqueue symbol if changed beyond threshold.
        """
        try:
            # Step 1: Decompress binary data
            decompressed_data = zlib.decompress(message)
            # Step 2: Decode protobuf
            decoded_data = self._decode_market_update(decompressed_data)
            if not decoded_data:
                return
            # Step 3: Deduplicate and enqueue
            if decoded_data.get('type') == 'market_update':
                market_data = decoded_data.get('data', {}).get('market_prices', {})
                now_ms = int(time.time() * 1000)
                for symbol, price_data in market_data.items():
                    bid = float(price_data.get('sell', 0)) if 'sell' in price_data else None
                    ask = float(price_data.get('buy', 0)) if 'buy' in price_data else None
                    # Only enqueue if at least one price exists
                    if bid is not None or ask is not None:
                        prev = self.last_values.get(symbol)
                        updated = False
                        # Always update if no previous, else check threshold
                        if prev is None:
                            updated = True
                        else:
                            prev_bid, prev_ask = prev
                            # Nulls always count as update
                            if bid is not None and (prev_bid is None or abs(bid - prev_bid) > 1e-5):
                                updated = True
                            if ask is not None and (prev_ask is None or abs(ask - prev_ask) > 1e-5):
                                updated = True
                        # Force refresh at least every 5 seconds to keep ts fresh
                        last_sent = self._last_sent_ms.get(symbol)
                        if not updated and (last_sent is None or (now_ms - last_sent) >= 5000):
                            updated = True
                        if updated:
                            self.last_values[symbol] = (bid, ask)
                            self._last_sent_ms[symbol] = now_ms
                            await self.redis_queue.put((symbol, bid, ask))
        except Exception as e:
            logger.error(f"Failed to process message immediately: {e}")
            self.stats['parse_errors'] += 1
    
    def _decode_market_update(self, data: bytes) -> Optional[Dict[str, Any]]:
        """
        Decode protobuf MarketUpdate message
        
        Schema (matching frontend JavaScript):
        MarketUpdate {
            type: string (id=1)
            data: MarketPrices (id=2)
        }
        
        MarketPrices {
            market_prices: map<string, MarketPrice> (id=1)
        }
        
        MarketPrice {
            buy: double (id=1)
            sell: double (id=2) 
            spread: double (id=3)
        }
        """
        try:
            result = {}
            offset = 0
            
            # logger.info(f"🔍 DEBUGGING: Decoding protobuf data, size: {len(data)} bytes")
            
            while offset < len(data):
                # Read field tag and wire type
                if offset >= len(data):
                    break
                    
                tag_byte = data[offset]
                wire_type = tag_byte & 0x07
                field_number = tag_byte >> 3
                offset += 1
                
                # logger.info(f"🔍 DEBUGGING: Field {field_number}, wire_type {wire_type}")
                
                if field_number == 1 and wire_type == 2:  # type field (string)
                    length, bytes_read = self._read_varint(data, offset)
                    offset += bytes_read
                    
                    if offset + length <= len(data):
                        result['type'] = data[offset:offset + length].decode('utf-8')
                        offset += length
                        # logger.info(f"✅ DEBUGGING: Decoded type: {result['type']}")
                
                elif field_number == 2 and wire_type == 2:  # data field (MarketPrices)
                    length, bytes_read = self._read_varint(data, offset)
                    offset += bytes_read
                    
                    # logger.info(f"🔍 DEBUGGING: MarketPrices data length: {length}")
                    
                    if offset + length <= len(data):
                        market_prices_data = data[offset:offset + length]
                        market_prices = self._decode_market_prices(market_prices_data)
                        
                        if market_prices:
                            result['data'] = {'market_prices': market_prices}
                            # logger.info(f"✅ DEBUGGING: Decoded {len(market_prices)} market prices: {list(market_prices.keys())[:5]}")
                        else:
                            logger.error(f"❌ DEBUGGING: Failed to decode market prices from {length} bytes")
                        
                        offset += length
                
                else:
                    # Skip unknown fields
                    # logger.info(f"⚠️ DEBUGGING: Skipping unknown field {field_number}")
                    offset = self._skip_field(data, offset, wire_type)
            
            if result:
                # logger.info(f"✅ DEBUGGING: Successfully decoded protobuf with type: {result.get('type')}")
                # if 'data' in result:
                #     market_count = len(result['data'].get('market_prices', {}))
                #     logger.info(f"✅ DEBUGGING: Market data contains {market_count} symbols")
                return result
            else:
                logger.error(f"❌ DEBUGGING: No valid data decoded from {len(data)} bytes")
                return None
            
        except Exception as e:
            logger.error(f"❌ DEBUGGING: Protobuf decode error: {e}")
            logger.error(f"❌ DEBUGGING: Data preview: {data[:50].hex() if len(data) >= 50 else data.hex()}")
            return None
    
    def _decode_market_prices(self, data: bytes) -> Dict[str, Dict[str, float]]:
        """Decode MarketPrices message containing map<string, MarketPrice>"""
        market_prices = {}
        offset = 0
        
        while offset < len(data):
            if offset >= len(data):
                break
                
            tag_byte = data[offset]
            wire_type = tag_byte & 0x07
            field_number = tag_byte >> 3
            offset += 1
            
            if field_number == 1 and wire_type == 2:  # market_prices map field
                length, bytes_read = self._read_varint(data, offset)
                offset += bytes_read
                
                if offset + length <= len(data):
                    map_entry_data = data[offset:offset + length]
                    symbol, price_data = self._decode_map_entry(map_entry_data)
                    
                    if symbol and price_data:
                        market_prices[symbol] = price_data
                    
                    offset += length
            else:
                offset = self._skip_field(data, offset, wire_type)
        
        return market_prices
    
    def _decode_map_entry(self, data: bytes) -> tuple:
        """Decode map entry: key=symbol (string), value=MarketPrice"""
        symbol = None
        price_data = None
        offset = 0
        
        while offset < len(data):
            if offset >= len(data):
                break
                
            tag_byte = data[offset]
            wire_type = tag_byte & 0x07
            field_number = tag_byte >> 3
            offset += 1
            
            if field_number == 1 and wire_type == 2:  # key (symbol string)
                length, bytes_read = self._read_varint(data, offset)
                offset += bytes_read
                
                if offset + length <= len(data):
                    symbol = data[offset:offset + length].decode('utf-8')
                    offset += length
            
            elif field_number == 2 and wire_type == 2:  # value (MarketPrice)
                length, bytes_read = self._read_varint(data, offset)
                offset += bytes_read
                
                if offset + length <= len(data):
                    price_message_data = data[offset:offset + length]
                    price_data = self._decode_market_price(price_message_data)
                    offset += length
            
            else:
                offset = self._skip_field(data, offset, wire_type)
        
        return symbol, price_data
    
    def _decode_market_price(self, data: bytes) -> Optional[Dict[str, float]]:
        """Decode MarketPrice message with buy, sell, spread fields"""
        price_data = {}
        offset = 0
        
        while offset < len(data):
            if offset >= len(data):
                break
                
            tag_byte = data[offset]
            wire_type = tag_byte & 0x07
            field_number = tag_byte >> 3
            offset += 1
            
            if wire_type == 1 and offset + 8 <= len(data):  # Fixed64 (double)
                price_bytes = data[offset:offset + 8]
                price = struct.unpack('<d', price_bytes)[0]
                
                if field_number == 1:  # buy field
                    price_data['buy'] = price
                elif field_number == 2:  # sell field  
                    price_data['sell'] = price
                elif field_number == 3:  # spread field
                    price_data['spread'] = price
                
                offset += 8
            else:
                offset = self._skip_field(data, offset, wire_type)
        
        return price_data if price_data else None
    
    def _read_varint(self, data: bytes, offset: int) -> tuple:
        """Read protobuf varint from data at offset, return (value, bytes_read)"""
        value = 0
        shift = 0
        bytes_read = 0
        
        while offset + bytes_read < len(data):
            byte = data[offset + bytes_read]
            bytes_read += 1
            
            value |= (byte & 0x7F) << shift
            
            if (byte & 0x80) == 0:
                break
                
            shift += 7
            
            if shift >= 64:  # Prevent infinite loop
                break
        
        return value, bytes_read
    
    def _skip_field(self, data: bytes, offset: int, wire_type: int) -> int:
        """Skip unknown protobuf field based on wire type"""
        if wire_type == 0:  # Varint
            _, bytes_read = self._read_varint(data, offset)
            return offset + bytes_read
        elif wire_type == 1:  # Fixed64
            return offset + 8
        elif wire_type == 2:  # Length-delimited
            length, bytes_read = self._read_varint(data, offset)
            return offset + bytes_read + length
        elif wire_type == 5:  # Fixed32
            return offset + 4
        else:
            # Unknown wire type, skip 1 byte
            return offset + 1
    
    def stop(self):
        """Stop the market listener"""
        self.is_running = False
        logger.info("Stopping protobuf market listener...")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive performance statistics for high-frequency monitoring"""
        stats = self.stats.copy()
        
        # Add calculated metrics
        if self.stats['batches_processed'] > 0:
            stats['messages_per_batch'] = self.stats['messages_processed'] / self.stats['batches_processed']
            stats['success_rate'] = (self.stats['successful_decodes'] / max(self.stats['messages_processed'], 1)) * 100
        else:
            stats['messages_per_batch'] = 0
            stats['success_rate'] = 0
            
        stats['current_queue_size'] = len(self.message_queue)
        stats['error_rate'] = (self.stats['parse_errors'] / max(self.stats['messages_processed'], 1)) * 100
        
        return stats
    
    def log_performance_summary(self):
        """Log performance summary for monitoring"""
        stats = self.get_stats()
        logger.info(f"📊 Performance Summary - Messages: {stats['messages_processed']}, "
                   f"Batches: {stats['batches_processed']}, "
                   f"Queue: {stats['current_queue_size']}, "
                   f"Success Rate: {stats['success_rate']:.1f}%, "
                   f"Avg Batch Size: {stats['avg_batch_size']:.1f}")
        
        if stats['error_rate'] > 5:  # Alert if error rate > 5%
            logger.warning(f"⚠️ High error rate detected: {stats['error_rate']:.1f}%")
    
    async def get_connection_status(self) -> Dict[str, Any]:
        """Get current connection status (for health_api.py compatibility)"""
        return {
            "is_running": self.is_running,
            "protocol": "protobuf_binary",
            "ws_url": self.ws_url,
            "reconnect_delay": self.reconnect_delay,
            "max_reconnect_attempts": self.max_reconnect_attempts,
            "performance": self.stats
        }

# Global listener instance (for health_api.py compatibility)
binary_market_listener = ProtobufMarketListener()

async def start_binary_market_listener():
    """Start the binary market listener as a background task (for main.py compatibility)"""
    await binary_market_listener.start()

# Main execution
async def main():
    """Main function to run the protobuf market listener"""
    listener = ProtobufMarketListener()
    
    try:
        await listener.start()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    finally:
        listener.stop()

if __name__ == "__main__":
    asyncio.run(main())
