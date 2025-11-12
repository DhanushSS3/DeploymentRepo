"""
Portfolio Calculator Service - Step 1: Market Price Update Listener

This service implements the first step of the Portfolio Calculator:
- Listens to market_price_updates Redis pub/sub channel
- Fetches affected users from symbol_holders Redis sets
- Maintains dirty_user_ids in-memory sets for throttled processing
- Follows SOLID principles with single responsibility and dependency inversion
"""

import asyncio
import logging
from typing import Set, Dict, Tuple, List, Optional
from threading import Lock
import time
import os

from ..config.redis_config import redis_cluster, redis_pubsub_client
from app.services.portfolio.margin_calculator import compute_single_order_margin
from app.services.portfolio.symbol_margin_aggregator import compute_symbol_margin
from app.services.portfolio.conversion_utils import convert_to_usd as portfolio_convert_to_usd
from app.services.portfolio.user_margin_service import compute_user_total_margin
from app.services.orders.order_repository import fetch_user_config as repo_fetch_user_config
from app.config.redis_logging import (
    log_pool_usage, log_connection_acquire, log_connection_release, 
    log_connection_error, log_pipeline_operation, connection_tracker
)
import uuid

# Env-driven strict mode
STRICT_MODE = os.getenv("PORTFOLIO_STRICT_MODE", "true").strip().lower() in ("1", "true", "yes", "on")


class PortfolioCalculatorListener:
    """
    Single Responsibility: Listen to market updates and collect dirty users
    Open/Closed: Can be extended with new user types without modification
    Dependency Inversion: Depends on Redis abstractions, not concrete implementations
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # In-memory dirty user sets (thread-safe)
        self._dirty_users_lock = Lock()
        self._dirty_users: Dict[str, Set[str]] = {
            'live': set(),
            'demo': set(),
            'strategy_provider': set(),
            'copy_follower': set()
        }
        
        # Statistics for monitoring
        self._stats = {
            'symbols_processed': 0,
            'users_affected_total': 0,
            'last_update_time': None,
            'start_time': time.time(),
            'users_processed': 0,
            'calculation_errors': 0,
            # Telemetry counters
            'calc.fatal_errors': 0,
            'calc.degraded': 0,
            'calc.orders_skipped': 0,
            'calc.users_processed': 0,
        }
        
        # Strict mode toggle
        self.strict_mode = STRICT_MODE

        # In-memory FX cache with short TTL to reduce Redis calls for conversion pairs
        self._fx_cache = {
            'ttl': 1.0,  # seconds
            'rates': {}  # mapping: symbol -> {'ts': epoch_seconds, 'bid': float, 'ask': float}
        }
        
        # Redis pub/sub subscription
        self._pubsub = None
        self._running = False
        
        # Throttled calculation loop task
        self._calculation_task = None

    async def start_listener(self):
        """Start the market price update listener and calculation loop"""
        self.logger.info("Starting Portfolio Calculator Listener...")
        try:
            self._pubsub = redis_pubsub_client.pubsub()
            await self._pubsub.subscribe('market_price_updates')
            self._running = True
            self.logger.info("Successfully subscribed to market_price_updates channel")

            # Start throttled calculation loop as a background task
            loop = asyncio.get_event_loop()
            self._calculation_task = loop.create_task(self._throttled_calculation_loop())

            # Start the main listening loop
            await self._listen_loop()
            
        except Exception as e:
            self.logger.error(f"Failed to start portfolio calculator listener: {e}")
            raise
    
    async def stop_listener(self):
        """Stop the listener and calculation loop gracefully"""
        self.logger.info("Stopping Portfolio Calculator Listener...")
        self._running = False
        if self._pubsub:
            await self._pubsub.unsubscribe('market_price_updates')
            await self._pubsub.close()
        if self._calculation_task:
            self._calculation_task.cancel()
        self.logger.info("Portfolio Calculator Listener stopped")

    async def _throttled_calculation_loop(self):
        """
        Throttled batch calculation loop (every 200ms):
        - Copies and clears dirty user sets
        - For each user, fetches all open orders, market prices, calculates portfolio, writes to Redis
        """
        self.logger.info("Starting throttled portfolio calculation loop (200ms interval)")
        while self._running:
            await asyncio.sleep(0.2)
            batch = {'live': set(), 'demo': set(), 'strategy_provider': set(), 'copy_follower': set()}
            with self._dirty_users_lock:
                for user_type in ('live', 'demo', 'strategy_provider', 'copy_follower'):
                    batch[user_type] = set(self._dirty_users[user_type])
                    self._dirty_users[user_type].clear()
            for user_type in ('live', 'demo', 'strategy_provider', 'copy_follower'):
                if not batch[user_type]:
                    continue
                await self._process_dirty_users_batch(batch[user_type], user_type)

    async def _process_dirty_users_batch(self, user_ids: Set[str], user_type: str):
        """
        For each dirty user, fetch all open orders, fetch prices, calculate, and update Redis
        """
        # if user_ids:
            # self.logger.info(f"⚙️ Portfolio calc: Processing {len(user_ids)} dirty {user_type} users")
        for user_key in user_ids:
            try:
                if not user_key.startswith(f"{user_type}:"):
                    self.logger.warning(f"Invalid user_key format: {user_key}")
                    continue

                user_id = user_key.split(":", 1)[1]
                user_ctx = f"{user_type}:{user_id}"

                # Fetch orders first; if none, still count as processed
                orders = await self._fetch_user_orders(user_type, user_id)
                symbols = list({o.get('symbol') for o in orders if o.get('symbol')}) if orders else []

                # Fetch user config (strict: no silent defaults for balance)
                user_cfg = await self._fetch_user_config(user_type, user_id)

                # Missing or unparsable balance → fatal
                if user_cfg.get('balance') is None:
                    self.logger.error(f"Portfolio calc fatal: missing_balance for user={user_ctx}")
                    await self._update_user_portfolio_status(user_type, user_id, calc_status="error", error_codes="missing_balance")
                    self._stats['calc.fatal_errors'] += 1
                    self._stats['calc.users_processed'] += 1
                    self._stats['users_processed'] += 1
                    continue

                # Fetch prices & group data only if there are orders
                prices = await self._fetch_market_prices(symbols) if symbols else {}
                group_data = await self._fetch_group_data_batch(symbols, user_cfg.get('group', 'Standard')) if symbols else {}

                # Validate inputs
                fatal_errors, order_skips, warnings = await self._validate_user_inputs(user_cfg, orders or [], prices, group_data)
                for w in warnings:
                    self.logger.warning(f"validation_warning user={user_ctx} msg={w}")

                if fatal_errors:
                    err_codes = ",".join(sorted(set(fatal_errors)))
                    self.logger.error(f"Portfolio calc fatal for user={user_ctx} errors={err_codes}")
                    await self._update_user_portfolio_status(user_type, user_id, calc_status="error", error_codes=err_codes)
                    self._stats['calc.fatal_errors'] += 1
                    self._stats['calc.users_processed'] += 1
                    self._stats['users_processed'] += 1
                    continue

                # Calculate metrics on valid orders only; capture meta for degraded status
                portfolio, meta = await self._calculate_portfolio_metrics(
                    orders or [], prices, group_data, user_cfg['balance'], (user_cfg.get('leverage') or 0.0), order_skips, user_ctx
                )

                # Determine calc_status and degraded_fields
                calc_status = "ok"
                degraded_fields: List[str] = []
                if meta.get('orders_skipped', 0) > 0 or any(flag in meta.get('degraded_flags', set()) for flag in ("missing_conversion",)):
                    calc_status = "degraded"
                    degraded_fields.append("orders_skipped")
                    flags = meta.get('degraded_flags', set())
                    for field in ("missing_group_data", "missing_prices", "missing_conversion"):
                        if field in flags:
                            degraded_fields.append(field)
                    self._stats['calc.degraded'] += 1
                # Update portfolio snapshot with status + metrics
                portfolio.update({
                    'calc_status': calc_status,
                    'degraded_fields': ",".join(degraded_fields) if degraded_fields else "",
                })
                await self._update_user_portfolio(user_type, user_id, portfolio)

                # Telemetry increments
                self._stats['calc.orders_skipped'] += int(meta.get('orders_skipped', 0))
                self._stats['calc.users_processed'] += 1
                self._stats['users_processed'] += 1
            except Exception as e:
                self.logger.error(f"Portfolio calculation error for {user_key}: {e}")
                self._stats['calculation_errors'] += 1

    async def _fetch_user_orders(self, user_type: str, user_id: str) -> list:
        """
        Fetch all open order hashes for a user from Redis: user_holdings:{{{user_type:user_id}}}:{order_id}
        Returns a list of order dicts.
        """
        hash_tag = f"{user_type}:{user_id}"
        pattern = f"user_holdings:{{{hash_tag}}}:*"
        try:
            # Prefer the indexed set if available to avoid cluster-wide SCAN
            index_key = f"user_orders_index:{{{hash_tag}}}"
            indexed_ids = await redis_cluster.smembers(index_key)
            order_keys = []
            if indexed_ids:
                order_keys = [f"user_holdings:{{{hash_tag}}}:{oid}" for oid in indexed_ids]
            else:
                # Fallback to SCAN; handle possible cluster-structured responses
                cursor = 0
                raw_keys = []
                while True:
                    try:
                        batch_result = await redis_cluster.scan(cursor=cursor, match=pattern, count=50)
                    except Exception as e:
                        self.logger.error(f"SCAN error for pattern {pattern}: {e}")
                        break
                    # batch_result may be (cursor, list) or dict mapping node->(cursor, list)
                    if isinstance(batch_result, tuple) and len(batch_result) == 2:
                        cursor, batch = batch_result
                        if isinstance(batch, dict):
                            for _, v in batch.items():
                                if isinstance(v, tuple) and len(v) == 2:
                                    _, lst = v
                                    raw_keys.extend(lst or [])
                                elif isinstance(v, (list, set, tuple)):
                                    raw_keys.extend(list(v))
                        elif isinstance(batch, (list, set, tuple)):
                            raw_keys.extend(list(batch))
                    elif isinstance(batch_result, dict):
                        # Map of node -> (cursor, keys)
                        for _, v in batch_result.items():
                            if isinstance(v, tuple) and len(v) == 2:
                                _, lst = v
                                raw_keys.extend(lst or [])
                        cursor = 0
                    else:
                        # Unknown structure; stop
                        cursor = 0
                    if cursor == 0:
                        break
                # Sanitize keys to strings
                for k in raw_keys:
                    try:
                        if isinstance(k, (bytes, bytearray)):
                            order_keys.append(k.decode())
                        else:
                            order_keys.append(str(k))
                    except Exception:
                        order_keys.append(str(k))

            if not order_keys:
                return []
            # Pipeline hgetall for all orders using sanitized string keys
            orders = await asyncio.gather(*(redis_cluster.hgetall(k) for k in order_keys))
            # Attach order_id and key; symbol is expected in fields; if missing, it will be validated later
            enriched = []
            for i, k in enumerate(order_keys):
                try:
                    key_str = k  # already sanitized to str above
                except Exception:
                    key_str = str(k)
                order_id = key_str.rsplit(":", 1)[-1]
                od = orders[i] or {}
                od['order_id'] = od.get('order_id') or order_id
                od['order_key'] = key_str
                enriched.append(od)
            return enriched
        except Exception as e:
            self.logger.error(f"Error fetching orders for {user_type}:{user_id}: {e}")
            return []

    async def _fetch_market_prices(self, symbols: list) -> dict:
        """
        Fetch latest market prices for all given symbols from Redis hashes market:{symbol}
        Returns dict: {symbol: {'bid':..., 'ask':...}}
        """
        prices = {}
        try:
            if not symbols:
                return prices
            # Pipeline HMGET for all symbols to reduce round trips and pool usage
            op_id = str(uuid.uuid4())[:8]
            connection_tracker.start_operation(op_id, "cluster", f"fetch_market_prices_{len(symbols)}_symbols")
            log_connection_acquire("cluster", f"fetch_market_prices_{len(symbols)}_symbols")
            
            start_time = time.time()
            try:
                async with redis_cluster.pipeline() as pipe:
                    keys = [f"market:{symbol}" for symbol in symbols]
                    for k in keys:
                        pipe.hmget(k, ["bid", "ask"])  # [bid, ask]
                    results = await pipe.execute()
                
                duration_ms = (time.time() - start_time) * 1000
                log_pipeline_operation("cluster", f"market_prices_{len(symbols)}_symbols", len(symbols), duration_ms)
                log_connection_release("cluster", f"fetch_market_prices_{len(symbols)}_symbols", duration_ms)
                connection_tracker.end_operation(op_id, success=True)
            except Exception as e:
                duration_ms = (time.time() - start_time) * 1000
                log_connection_error("cluster", f"fetch_market_prices_{len(symbols)}_symbols", str(e))
                connection_tracker.end_operation(op_id, success=False, error=str(e))
                raise
            for i, symbol in enumerate(symbols):
                try:
                    vals = results[i]
                except Exception:
                    vals = None
                if vals and len(vals) >= 2:
                    try:
                        bid = float(vals[0]) if vals[0] is not None else None
                    except Exception:
                        bid = None
                    try:
                        ask = float(vals[1]) if vals[1] is not None else None
                    except Exception:
                        ask = None
                    # Only include if any price exists
                    if (bid is not None) or (ask is not None):
                        prices[symbol] = {"bid": bid, "ask": ask}
            return prices
        except Exception as e:
            self.logger.error(f"Error fetching market prices for symbols {symbols}: {e}")
            return prices

    async def _fetch_user_config(self, user_type: str, user_id: str) -> Dict:
        """
        Fetch user's config via repository to centralize key handling (hash-tagged first, legacy fallback).
        Returns dict: {'balance': Optional[float], 'leverage': float, 'group': str}
        """
        try:
            cfg = await repo_fetch_user_config(user_type, user_id)
        except Exception as e:
            self.logger.error(f"Error fetching user config via repository for {user_type}:{user_id}: {e}")
            cfg = {}

        # Map repository fields to calculator expectations
        balance: Optional[float] = None
        try:
            if cfg and (cfg.get('wallet_balance') is not None):
                balance = float(cfg.get('wallet_balance'))
        except (TypeError, ValueError):
            balance = None

        leverage: float = 0.0
        try:
            if cfg and (cfg.get('leverage') is not None):
                lev = float(cfg.get('leverage'))
                if lev > 0:
                    leverage = lev
        except (TypeError, ValueError):
            leverage = 0.0

        group = (cfg.get('group') or 'Standard') if cfg else 'Standard'
        return {'balance': balance, 'leverage': leverage, 'group': group}

    async def _validate_user_inputs(self, user_cfg: Dict, orders: List[Dict], prices: Dict, group_data: Dict) -> Tuple[List[str], Dict[str, str], List[str]]:
        """
        Validate inputs strictly. Returns (fatal_errors, order_skips, warnings)

        Fatal if:
        - Missing wallet_balance
        - For any order: margin<=0 AND leverage<=0 AND missing contract_size (from both groups).

        order_skips reasons may include:
        - missing_symbol, missing_prices, missing_group_data, missing_profit_currency,
          missing_margin_and_leverage_or_contract_size
        """
        fatal_errors: List[str] = []
        order_skips: Dict[str, str] = {}
        warnings: List[str] = []

        # Fatal: missing balance
        if user_cfg.get('balance') is None:
            fatal_errors.append('missing_balance')

        leverage = float(user_cfg.get('leverage') or 0.0)

        for od in orders:
            order_id = od.get('order_id') or 'unknown'
            symbol = od.get('symbol')

            # Missing symbol
            if not symbol:
                order_skips[order_id] = 'missing_symbol'
                continue

            # Missing price data
            if symbol not in prices:
                order_skips[order_id] = 'missing_prices'
                continue

            # Group data checks
            g = group_data.get(symbol)
            contract_size = None if not g else self._safe_float(g.get('contract_size'))
            profit = None if not g else (g.get('profit') or None)
            if (g is None) or (contract_size is None):
                # Potentially fatal if also no leverage and margin missing/<=0
                try:
                    margin_val = float(od.get('margin') or 0)
                except (TypeError, ValueError):
                    margin_val = 0.0
                if self.strict_mode and margin_val <= 0 and leverage <= 0 and contract_size is None:
                    fatal_errors.append('missing_contract_size_no_leverage_and_margin')
                else:
                    order_skips[order_id] = 'missing_group_data'
                continue

            # Profit currency presence checked during calculation step as well
            if profit is None:
                order_skips[order_id] = 'missing_profit_currency'

        return fatal_errors, order_skips, warnings

    async def _fetch_user_balance_and_leverage(self, user_type: str, user_id: str):
        """
        Fetch user's wallet balance, leverage, and group from Redis:
        Key: user:{{user_type:user_id}}:config (Hash)
        Note: Legacy method. Prefer _fetch_user_config for strict mode. Kept for backward compatibility.
        """
        try:
            key = f"user:{{{user_type}:{user_id}}}:config"
            data = await redis_cluster.hgetall(key)
            if not data:
                return 0.0, 100.0, "Standard"
            # Safe parsing
            balance_raw = data.get('wallet_balance', 0)
            leverage_raw = data.get('leverage', 100)
            group = data.get('group', 'Standard') or 'Standard'
            try:
                balance = float(balance_raw)
            except (TypeError, ValueError):
                balance = 0.0
            try:
                leverage = float(leverage_raw)
            except (TypeError, ValueError):
                leverage = 100.0
            if leverage <= 0:
                leverage = 100.0
            return balance, leverage, group
        except Exception as e:
            self.logger.error(f"Error fetching user config for {user_type}:{user_id}: {e}")
            return 0.0, 100.0, "Standard"

    async def _fetch_group_data_batch(self, symbols: list, group: str) -> dict:
        """
        Fetch contract_size and profit currency for each symbol from groups:{group}:{symbol},
        with fallback to groups:{Standard}:{symbol} if missing.
        Returns dict: {symbol: {'contract_size': Optional[float], 'profit': Optional[str], 'type': int, 'crypto_margin_factor': Optional[float]}}
        """
        group_data = {}
        try:
            # First try user-specific group keys in parallel
            grp_keys = [f"groups:{{{group}}}:{symbol}" for symbol in symbols]
            grp_results = await asyncio.gather(*(redis_cluster.hgetall(k) for k in grp_keys))

            # Collect missing symbols to fallback to Standard
            missing_indices = [i for i, data in enumerate(grp_results) if not data]
            std_results_map = {}
            if missing_indices:
                std_keys = [f"groups:{{Standard}}:{symbols[i]}" for i in missing_indices]
                std_results = await asyncio.gather(*(redis_cluster.hgetall(k) for k in std_keys))
                for idx, data in zip(missing_indices, std_results):
                    std_results_map[idx] = data

            for i, symbol in enumerate(symbols):
                data = grp_results[i] if grp_results[i] else std_results_map.get(i, None)
                if data:
                    try:
                        contract_size = float(data.get('contract_size')) if (data.get('contract_size') is not None) else None
                    except (TypeError, ValueError):
                        contract_size = None
                    profit = data.get('profit') or None
                    try:
                        itype = int(data.get('type')) if (data.get('type') is not None) else 1
                    except (TypeError, ValueError):
                        itype = 1
                    try:
                        cmf = float(data.get('crypto_margin_factor')) if (data.get('crypto_margin_factor') is not None) else None
                    except (TypeError, ValueError):
                        cmf = None
                else:
                    contract_size = None
                    profit = None
                    itype = 1
                    cmf = None
                group_data[symbol] = {
                    'contract_size': contract_size,
                    'profit': profit,
                    'type': itype,
                    'crypto_margin_factor': cmf,
                }
            return group_data
        except Exception as e:
            self.logger.error(f"Error fetching group data for symbols {symbols}: {e}")
            return group_data

    async def _calculate_portfolio_metrics(
        self,
        orders: List[Dict],
        prices: Dict,
        group_data: Dict,
        balance: float,
        leverage: float,
        skip_orders: Dict[str, str],
        user_ctx: str = "",
    ) -> Tuple[Dict, Dict]:
        """
        Calculate PnL, equity, used margin, free margin, margin level, etc., over valid orders only.
        - Keeps commission and swap defaults at 0.
        - Skips orders present in skip_orders or failing runtime checks (conversion, margin calc).
        Returns (portfolio_dict, meta) where meta contains {'orders_skipped': int, 'degraded_flags': set}
        """
        total_pl_usd = 0.0
        used_margin = 0.0
        skipped = 0
        degraded_flags: Set[str] = set()
        
        # Determine if there are queued orders; if so we will prefer used_margin_all from cache
        has_queued = False
        for _od in (orders or []):
            try:
                if (str((_od.get('order_status') or '')).upper() == 'QUEUED') or (str((_od.get('execution_status') or '')).upper() == 'QUEUED'):
                    has_queued = True
                    break
            except Exception:
                pass
        
        # Fetch cached margin fields from user_portfolio
        cached_used_executed: Optional[float] = None
        cached_used_all: Optional[float] = None
        try:
            if user_ctx and ':' in user_ctx:
                _ut, _uid = user_ctx.split(':', 1)
                pf_key = f"user_portfolio:{{{_ut}:{_uid}}}"
                pf = await redis_cluster.hgetall(pf_key)
                if pf:
                    try:
                        cached_used_executed = float(pf.get('used_margin_executed')) if pf.get('used_margin_executed') is not None else None
                    except (TypeError, ValueError):
                        cached_used_executed = None
                    try:
                        cached_used_all = float(pf.get('used_margin_all')) if pf.get('used_margin_all') is not None else None
                    except (TypeError, ValueError):
                        cached_used_all = None
        except Exception as e:
            self.logger.error(f"failed_to_fetch_cached_margins user={user_ctx} err={e}")

        for order in orders:
            try:
                order_id = order.get('order_id') or 'unknown'
                symbol = order.get('symbol')

                # Respect pre-validated skips
                if order_id in skip_orders:
                    reason = skip_orders[order_id]
                    skipped += 1
                    degraded_flags.add('orders_skipped')
                    if reason in ('missing_group_data',):
                        degraded_flags.add('missing_group_data')
                    if reason in ('missing_prices',):
                        degraded_flags.add('missing_prices')
                    self.logger.warning(f"order_skip user={user_ctx} symbol={symbol} reason={reason}")
                    continue

                # Validate availability at runtime
                if (not symbol) or (symbol not in prices) or (symbol not in group_data):
                    skipped += 1
                    degraded_flags.update({'orders_skipped', 'missing_group_data' if (symbol not in group_data) else 'missing_prices'})
                    self.logger.warning(f"order_skip user={user_ctx} symbol={symbol} reason={'missing_group_data' if (symbol not in group_data) else 'missing_prices'}")
                    continue

                g = group_data.get(symbol) or {}
                contract_size = self._safe_float(g.get('contract_size'))
                profit_currency = g.get('profit') or None

                if (contract_size is None) or (profit_currency is None):
                    skipped += 1
                    degraded_flags.add('orders_skipped')
                    degraded_flags.add('missing_group_data' if contract_size is None else 'missing_profit_currency')
                    self.logger.warning(f"order_skip user={user_ctx} symbol={symbol} reason={'missing_group_data' if contract_size is None else 'missing_profit_currency'}")
                    continue

                order_type = (order.get('order_type') or '').upper()
                # Safe float parsing from Redis string values
                entry_price = self._safe_float(order.get('order_price')) or 0.0
                quantity = self._safe_float(order.get('order_quantity')) or 0.0
                margin_val = self._safe_float(order.get('margin')) or 0.0
                swap = self._safe_float(order.get('swap')) or 0.0
                commission = self._safe_float(order.get('commission')) or 0.0

                # Use latest market prices
                market_bid = self._safe_float(prices[symbol].get('bid')) or 0.0
                market_ask = self._safe_float(prices[symbol].get('ask')) or 0.0

                if order_type == 'BUY':
                    pnl = (market_bid - entry_price) * quantity * contract_size
                elif order_type == 'SELL':
                    pnl = (entry_price - market_ask) * quantity * contract_size
                else:
                    pnl = 0.0

                pnl += swap - commission

                # Strict conversion: skip if no conversion pair
                pnl_usd = await self.convert_to_usd(pnl, profit_currency, symbol, prices, strict=self.strict_mode)
                if pnl_usd is None:
                    skipped += 1
                    degraded_flags.update({'orders_skipped', 'missing_conversion'})
                    self.logger.warning(f"order_skip user={user_ctx} symbol={symbol} reason=missing_conversion")
                    continue
                total_pl_usd += pnl_usd

                # Skip per-order margin computation on market ticks; use cached portfolio margins instead
                # PnL is still computed above; margin values will be sourced from cached fields below
                pass

            except Exception as e:
                self.logger.error(f"Error calculating order PnL: {order}: {e}")
                continue

        # Choose used_margin from cached portfolio fields
        # Prefer used_margin_all when there are queued orders; otherwise use executed only.
        chosen_used: Optional[float] = None
        if has_queued and (cached_used_all is not None):
            chosen_used = float(cached_used_all)
        elif cached_used_executed is not None:
            chosen_used = float(cached_used_executed)
        
        # Fallback: compute executed and (optionally) total if cache missing
        if chosen_used is None:
            try:
                exec_margin, total_margin, _ = await compute_user_total_margin(
                    user_type=user_ctx.split(':', 1)[0] if user_ctx and ':' in user_ctx else 'live',
                    user_id=user_ctx.split(':', 1)[1] if user_ctx and ':' in user_ctx else '0',
                    orders=orders,
                    prices_cache=prices,
                    strict=False,
                    include_queued=True,
                )
                chosen_used = float(total_margin) if has_queued and (total_margin is not None) else (float(exec_margin) if exec_margin is not None else 0.0)
            except Exception as e:
                self.logger.error(f"fallback_margin_compute_failed user={user_ctx} err={e}")
                chosen_used = 0.0
        used_margin = float(chosen_used or 0.0)

        equity = (balance or 0.0) + total_pl_usd
        free_margin = equity - used_margin
        margin_level = (equity / used_margin * 100) if used_margin > 0 else 0.0
        portfolio = {
            'equity': round(equity, 2),
            'balance': round(balance or 0.0, 2),
            'free_margin': round(free_margin, 2),
            'used_margin': round(used_margin, 2),
            'margin_level': round(margin_level, 2),
            'open_pnl': round(total_pl_usd, 2),
            'total_pl': round(total_pl_usd, 2),
            'ts': self._now_ms(),
        }
        meta = {'orders_skipped': skipped, 'degraded_flags': degraded_flags}
        return portfolio, meta

    async def convert_to_usd(self, amount: float, from_currency: str, symbol: str, prices: dict = None, strict: Optional[bool] = None) -> Optional[float]:
        """
        Convert amount from from_currency to USD using portfolio helper layer (ask price).
        Delegates to app.services.portfolio.conversion_utils.convert_to_usd.
        """
        try:
            if strict is None:
                strict = self.strict_mode
            return await portfolio_convert_to_usd(amount, from_currency, prices_cache=prices or {}, strict=strict)
        except Exception as e:
            self.logger.error(f"Error converting {amount} {from_currency} to USD via helper: {e}")
            return None if strict else amount

    def _safe_float(self, v) -> Optional[float]:
        try:
            if v is None:
                return None
            return float(v)
        except (TypeError, ValueError):
            return None

    def _now_ms(self) -> int:
        return int(time.time() * 1000)

    async def _update_user_portfolio_status(self, user_type: str, user_id: str, calc_status: str, error_codes: Optional[str] = None, degraded_fields: Optional[str] = None):
        """
        Update only status fields for user's portfolio snapshot, preserving existing metrics.
        """
        redis_key = f"user_portfolio:{{{user_type}:{user_id}}}"
        mapping = {
            'calc_status': calc_status,
            'ts': self._now_ms(),
        }
        if error_codes is not None:
            mapping['error_codes'] = error_codes
        if degraded_fields is not None:
            mapping['degraded_fields'] = degraded_fields
        try:
            await redis_cluster.hset(redis_key, mapping=mapping)
        except Exception as e:
            self.logger.error(f"Error updating status for {redis_key}: {e}")

    async def _update_user_portfolio(self, user_type: str, user_id: str, portfolio: dict):
        """
        Write portfolio metrics to Redis hash user_portfolio:{{user_type:user_id}}
        Optimization:
        - Do not recompute margins on every tick.
        - Use cached used_margin_executed/used_margin_all when present.
        - Only compute margins if missing, or to backfill when queued orders exist.
        """
        redis_key = f"user_portfolio:{{{user_type}:{user_id}}}"
        try:
            # Fetch orders and detect if there are queued orders
            orders = await self._fetch_user_orders(user_type, user_id)
            has_queued = False
            for _od in (orders or []):
                try:
                    if (str((_od.get('order_status') or '')).upper() == 'QUEUED') or (str((_od.get('execution_status') or '')).upper() == 'QUEUED'):
                        has_queued = True
                        break
                except Exception:
                    pass

            # Read existing portfolio margin fields
            existing_pf = await redis_cluster.hgetall(redis_key)
            existing_exe = None
            existing_all = None
            try:
                if existing_pf and (existing_pf.get('used_margin_executed') is not None):
                    existing_exe = float(existing_pf.get('used_margin_executed'))
            except (TypeError, ValueError):
                existing_exe = None
            try:
                if existing_pf and (existing_pf.get('used_margin_all') is not None):
                    existing_all = float(existing_pf.get('used_margin_all'))
            except (TypeError, ValueError):
                existing_all = None

            executed_margin = existing_exe
            total_margin = existing_all

            # Compute only if necessary
            if has_queued:
                if (executed_margin is None) or (total_margin is None):
                    executed_margin, total_margin, _ = await compute_user_total_margin(
                        user_type=user_type,
                        user_id=user_id,
                        orders=orders,
                        prices_cache=None,
                        strict=False,
                        include_queued=True,
                    )
            else:
                if executed_margin is None:
                    executed_margin, _, _ = await compute_user_total_margin(
                        user_type=user_type,
                        user_id=user_id,
                        orders=orders,
                        prices_cache=None,
                        strict=False,
                        include_queued=False,
                    )
                total_margin = executed_margin if executed_margin is not None else total_margin

            # Add margin fields to portfolio (as strings for Redis)
            if executed_margin is not None:
                portfolio['used_margin_executed'] = str(float(executed_margin))
            if total_margin is not None:
                portfolio['used_margin_all'] = str(float(total_margin))

            # Legacy field reflects the margin used for current calculations view
            chosen_used = (float(total_margin) if (has_queued and (total_margin is not None)) else (float(executed_margin) if executed_margin is not None else 0.0))
            portfolio['used_margin'] = str(round(chosen_used, 2))

            # Track portfolio update operation
            update_op_id = str(uuid.uuid4())[:8]
            connection_tracker.start_operation(update_op_id, "cluster", f"portfolio_update_{user_type}_{user_id}")
            log_connection_acquire("cluster", f"portfolio_update_{user_type}_{user_id}")
            
            start_time = time.time()
            try:
                await redis_cluster.hset(redis_key, mapping=portfolio)
                duration_ms = (time.time() - start_time) * 1000
                log_connection_release("cluster", f"portfolio_update_{user_type}_{user_id}", duration_ms)
                connection_tracker.end_operation(update_op_id, success=True)
                
                # self.logger.info(f"✅ Portfolio calc: WROTE portfolio to Redis key={redis_key} equity={portfolio.get('equity')} margin_level={portfolio.get('margin_level')}")
            except Exception as e:
                duration_ms = (time.time() - start_time) * 1000
                log_connection_error("cluster", f"portfolio_update_{user_type}_{user_id}", str(e))
                connection_tracker.end_operation(update_op_id, success=False, error=str(e))
                raise
            
            # Publish a lightweight notification for watchers (AutoCutoff, dashboards, etc.)
            pub_op_id = str(uuid.uuid4())[:8]
            try:
                connection_tracker.start_operation(pub_op_id, "pubsub", f"portfolio_notification_{user_type}_{user_id}")
                log_connection_acquire("pubsub", f"portfolio_notification_{user_type}_{user_id}")
                
                pub_start_time = time.time()
                await redis_pubsub_client.publish('portfolio_updates', f"{user_type}:{user_id}")
                pub_duration_ms = (time.time() - pub_start_time) * 1000
                
                log_connection_release("pubsub", f"portfolio_notification_{user_type}_{user_id}", pub_duration_ms)
                connection_tracker.end_operation(pub_op_id, success=True)
                self.logger.debug(f"📢 Portfolio calc: Published portfolio_updates for {user_type}:{user_id}")
            except Exception as pub_err:
                log_connection_error("pubsub", f"portfolio_notification_{user_type}_{user_id}", str(pub_err))
                connection_tracker.end_operation(pub_op_id, success=False, error=str(pub_err))
                self.logger.warning(f"Failed to publish portfolio update for {user_type}:{user_id}: {pub_err}")
        except Exception as e:
            self.logger.error(f"Error updating portfolio for {redis_key}: {e}")

    
    async def _listen_loop(self):
        """Main listening loop for market price updates"""
        self.logger.info("Portfolio Calculator Listener is now active and listening for market_price_updates")
        
        try:
            async for message in self._pubsub.listen():
                if not self._running:
                    break
                
                if message['type'] == 'message':
                    symbol = str(message.get('data', ''))
                    # self.logger.info(f"🎯 Portfolio calc: Received market_price_updates message for symbol={symbol}")
                    await self._process_symbol_update(symbol)
                    
        except Exception as e:
            self.logger.error(f"Error in portfolio calculator listen loop: {e}")
            if self._running:
                # Attempt to reconnect after a delay
                await asyncio.sleep(5)
                await self.start_listener()
    
    async def _process_symbol_update(self, symbol: str):
        """
        Process a single symbol update by fetching affected users
        Optimized: Batch fetch all user types in single pipeline operation to reduce connections
        """
        try:
            # self.logger.info(f"🔔 Portfolio calc: Processing symbol update: {symbol}")
            
            # Batch fetch all user types in single pipeline operation to prevent connection exhaustion
            live_users, demo_users, strategy_provider_users, copy_follower_users = await self._fetch_all_symbol_holders_batch(symbol)
            
            # self.logger.info(f"📊 Portfolio calc: Found holders - live:{len(live_users)} demo:{len(demo_users)} strategy_provider:{len(strategy_provider_users)} copy_follower:{len(copy_follower_users)}")
            
            # Add to dirty user sets
            users_added = 0
            if live_users:
                users_added += self._add_to_dirty_users(live_users, 'live')
            if demo_users:
                users_added += self._add_to_dirty_users(demo_users, 'demo')
            if strategy_provider_users:
                users_added += self._add_to_dirty_users(strategy_provider_users, 'strategy_provider')
            if copy_follower_users:
                users_added += self._add_to_dirty_users(copy_follower_users, 'copy_follower')
            
            # Update statistics
            self._update_stats(symbol, users_added)
            
            if users_added > 0:
                self.logger.debug(
                    f"Symbol {symbol}: Added {users_added} users to dirty sets "
                    f"(live: {len(live_users)}, demo: {len(demo_users)}, "
                    f"strategy_provider: {len(strategy_provider_users)}, copy_follower: {len(copy_follower_users)})"
                )
            
        except Exception as e:
            self.logger.error(f"Error processing symbol update for {symbol}: {e}")
    
    async def _fetch_symbol_holders(self, symbol: str, user_type: str) -> Set[str]:
        """
        Fetch all users holding positions in a specific symbol with proper connection management
        
        Args:
            symbol: The trading symbol (e.g., 'EURUSD')
            user_type: Type of user ('live', 'demo', 'strategy_provider', 'copy_follower')
            
        Returns:
            Set of user identifiers in format 'user_type:user_id'
        """
        redis_key = f"symbol_holders:{symbol}:{user_type}"
        
        try:
            # Use proper connection management with pipeline to prevent connection leaks
            async with redis_cluster.pipeline() as pipe:
                pipe.smembers(redis_key)
                results = await pipe.execute()
                user_ids = results[0] if results else set()
            
            if user_ids:
                # Convert to set and ensure proper format
                formatted_users = {
                    f"{user_type}:{user_id}" if not user_id.startswith(f"{user_type}:") 
                    else user_id 
                    for user_id in user_ids
                }
                
                self.logger.debug(
                    f"Found {len(formatted_users)} {user_type} users for symbol {symbol}"
                )
                return formatted_users
            
            return set()
            
        except Exception as e:
            # Detailed error logging to identify exact failure points
            import traceback
            error_details = {
                "symbol": symbol,
                "user_type": user_type,
                "redis_key": redis_key,
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": traceback.format_exc()
            }
            
            self.logger.error(
                f"❌ PORTFOLIO_CALC: Failed to fetch symbol holders - "
                f"Symbol: {symbol}, UserType: {user_type}, "
                f"RedisKey: {redis_key}, "
                f"ErrorType: {type(e).__name__}, "
                f"ErrorMsg: {str(e)}"
            )
            self.logger.debug(f"Full traceback for symbol_holders error: {traceback.format_exc()}")
            
            return set()
    
    async def _fetch_all_symbol_holders_batch(self, symbol: str) -> Tuple[Set[str], Set[str], Set[str], Set[str]]:
        """
        Batch fetch all user types for a symbol in single pipeline operation
        This prevents connection pool exhaustion by using only one connection
        
        Args:
            symbol: The trading symbol (e.g., 'EURUSD')
            
        Returns:
            Tuple of (live_users, demo_users, strategy_provider_users, copy_follower_users)
        """
        user_types = ['live', 'demo', 'strategy_provider', 'copy_follower']
        redis_keys = [f"symbol_holders:{symbol}:{user_type}" for user_type in user_types]
        
        try:
            # Single pipeline operation to fetch all user types at once
            async with redis_cluster.pipeline() as pipe:
                for redis_key in redis_keys:
                    pipe.smembers(redis_key)
                results = await pipe.execute()
            
            # Process results for each user type
            all_users = []
            for i, user_type in enumerate(user_types):
                user_ids = results[i] if i < len(results) and results[i] else set()
                
                if user_ids:
                    # Convert to set and ensure proper format
                    formatted_users = {
                        f"{user_type}:{user_id}" if not user_id.startswith(f"{user_type}:") 
                        else user_id 
                        for user_id in user_ids
                    }
                    all_users.append(formatted_users)
                    
                    self.logger.debug(
                        f"Found {len(formatted_users)} {user_type} users for symbol {symbol}"
                    )
                else:
                    all_users.append(set())
            
            return tuple(all_users)
            
        except Exception as e:
            # Detailed error logging for batch operation
            import traceback
            self.logger.error(
                f"❌ PORTFOLIO_CALC: Failed to batch fetch symbol holders - "
                f"Symbol: {symbol}, "
                f"RedisKeys: {redis_keys}, "
                f"ErrorType: {type(e).__name__}, "
                f"ErrorMsg: {str(e)}"
            )
            self.logger.debug(f"Full traceback for batch symbol_holders error: {traceback.format_exc()}")
            
            # Return empty sets for all user types on error
            return set(), set(), set(), set()
    
    def _add_to_dirty_users(self, user_ids: Set[str], user_type: str) -> int:
        """
        Add user IDs to the dirty users set for a specific user type
        Thread-safe operation with automatic deduplication
        
        Args:
            user_ids: Set of user identifiers to add
            user_type: 'live' or 'demo'
            
        Returns:
            Number of new users added (excluding duplicates)
        """
        if not user_ids or user_type not in self._dirty_users:
            return 0
        
        with self._dirty_users_lock:
            # Get current size before adding
            before_size = len(self._dirty_users[user_type])
            
            # Add all user IDs (set automatically deduplicates)
            self._dirty_users[user_type].update(user_ids)
            
            # Calculate how many new users were actually added
            after_size = len(self._dirty_users[user_type])
            new_users_added = after_size - before_size
            
            return new_users_added
    
    def _update_stats(self, symbol: str, users_affected: int):
        """Update internal statistics for monitoring"""
        self._stats['symbols_processed'] += 1
        self._stats['users_affected_total'] += users_affected
        self._stats['last_update_time'] = time.time()
        
        # Log statistics periodically
        if self._stats['symbols_processed'] % 100 == 0:
            self._log_statistics()
    
    def _log_statistics(self):
        """Log current statistics for monitoring"""
        uptime = time.time() - self._stats['start_time']
        
        with self._dirty_users_lock:
            live_dirty = len(self._dirty_users['live'])
            demo_dirty = len(self._dirty_users['demo'])
            strategy_provider_dirty = len(self._dirty_users['strategy_provider'])
            copy_follower_dirty = len(self._dirty_users['copy_follower'])
        
        self.logger.debug(
            f"Portfolio Calculator Stats - "
            f"Symbols processed: {self._stats['symbols_processed']}, "
            f"Total users affected: {self._stats['users_affected_total']}, "
            f"Current dirty users (live: {live_dirty}, demo: {demo_dirty}, "
            f"strategy_provider: {strategy_provider_dirty}, copy_follower: {copy_follower_dirty}), "
            f"Uptime: {uptime:.1f}s"
        )
    
    def get_dirty_users(self, user_type: str) -> Set[str]:
        """
        Get current dirty users for a specific user type (read-only)
        Thread-safe operation
        
        Args:
            user_type: 'live', 'demo', 'strategy_provider', or 'copy_follower'
            
        Returns:
            Copy of current dirty users set
        """
        with self._dirty_users_lock:
            return self._dirty_users.get(user_type, set()).copy()
    
    def get_and_clear_dirty_users(self, user_type: str) -> Set[str]:
        """
        Get and clear dirty users for a specific user type
        Thread-safe operation for throttled processing
        
        Args:
            user_type: 'live', 'demo', 'strategy_provider', or 'copy_follower'
            
        Returns:
            Set of dirty users that were cleared
        """
        with self._dirty_users_lock:
            if user_type not in self._dirty_users:
                return set()
            
            dirty_users = self._dirty_users[user_type].copy()
            self._dirty_users[user_type].clear()
            
            if dirty_users:
                self.logger.debug(
                    f"Cleared {len(dirty_users)} dirty {user_type} users for processing"
                )
            
            return dirty_users
    
    def get_statistics(self) -> Dict:
        """Get current listener statistics"""
        uptime = time.time() - self._stats['start_time']
        
        with self._dirty_users_lock:
            current_stats = {
                **self._stats,
                'uptime_seconds': uptime,
                'dirty_users_live': len(self._dirty_users['live']),
                'dirty_users_demo': len(self._dirty_users['demo']),
                'dirty_users_strategy_provider': len(self._dirty_users['strategy_provider']),
                'dirty_users_copy_follower': len(self._dirty_users['copy_follower']),
                'is_running': self._running
            }
        
        return current_stats


# Global instance for the application
portfolio_listener = PortfolioCalculatorListener()


async def start_portfolio_listener():
    """Start the portfolio calculator listener as a background task"""
    await portfolio_listener.start_listener()


async def stop_portfolio_listener():
    """Stop the portfolio calculator listener"""
    await portfolio_listener.stop_listener()
