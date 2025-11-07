import os
import asyncio
import logging
from typing import Dict, Any, Optional

from app.config.redis_config import redis_cluster
from app.services.orders.order_repository import fetch_group_data, fetch_user_config, fetch_user_portfolio
from app.services.portfolio.margin_calculator import compute_single_order_margin
from app.services.orders.service_provider_client import send_provider_order

logger = logging.getLogger(__name__)

SET_ACTIVE = "provider_pending_active"
HKEY_PREFIX = "provider_pending:"


def _hkey(order_id: str) -> str:
    return f"{HKEY_PREFIX}{order_id}"


async def register_provider_pending(info: Dict[str, Any]) -> None:
    """
    Register a provider pending order for continuous margin monitoring.
    Expected fields in info: order_id, symbol, order_type, order_quantity, user_id, user_type, group
    """
    try:
        order_id = str(info.get("order_id"))
        symbol = str(info.get("symbol") or "").upper()
        order_type = str(info.get("order_type") or "").upper()
        order_qty = str(info.get("order_quantity"))
        user_id = str(info.get("user_id"))
        user_type = str(info.get("user_type") or "").lower()
        group = str(info.get("group") or "Standard")
        if not order_id:
            return
        async with redis_cluster.pipeline() as p:
            p.sadd(SET_ACTIVE, order_id)
            p.hset(_hkey(order_id), mapping={
                "symbol": symbol,
                "order_type": order_type,
                "order_quantity": order_qty,
                "user_id": user_id,
                "user_type": user_type,
                "group": group,
                "created_at": str(int(asyncio.get_event_loop().time() * 1000)),
            })
            await p.execute()
    except Exception:
        logger.exception("register_provider_pending failed for %s", info.get("order_id"))


async def _compute_half_spread(group: str, symbol: str) -> Optional[float]:
    try:
        g = await fetch_group_data(symbol, group)
        if g:
            try:
                spread = float(g.get("spread")) if g.get("spread") is not None else None
                spread_pip = float(g.get("spread_pip")) if g.get("spread_pip") is not None else None
            except (TypeError, ValueError):
                spread = None
                spread_pip = None
            if spread is not None and spread_pip is not None:
                return (spread * spread_pip) / 2.0
    except Exception as e:
        logger.warning("compute_half_spread failed %s:%s: %s", group, symbol, e)
    return None


async def _get_ask(symbol: str) -> Optional[float]:
    try:
        arr = await redis_cluster.hmget(f"market:{symbol}", ["bid", "ask"])  # [bid, ask]
        if arr and arr[1] is not None:
            return float(arr[1])
    except Exception as e:
        logger.warning("get_ask failed for %s: %s", symbol, e)
    return None


async def _validate_margin(user_type: str, user_id: str, group: str, symbol: str, qty: float, ask: float) -> bool:
    try:
        cfg = await fetch_user_config(user_type, user_id)
        leverage = float(cfg.get("leverage") or 0.0)
        balance = float(cfg.get("wallet_balance") or 0.0)
        if leverage <= 0:
            return False
        # group data
        g = await fetch_group_data(symbol, group)
        try:
            contract_size = float(g.get("contract_size")) if g.get("contract_size") is not None else None
        except (TypeError, ValueError):
            contract_size = None
        profit_currency = g.get("profit") or None
        try:
            instrument_type = int(g.get("type")) if g.get("type") is not None else 1
        except (TypeError, ValueError):
            instrument_type = 1
        half_spread = await _compute_half_spread(group, symbol)
        if contract_size is None or profit_currency is None or half_spread is None:
            return False
        # exec price for margin preview = ask + half_spread
        exec_price_user = float(ask) + float(half_spread)
        single_margin = await compute_single_order_margin(
            contract_size=contract_size,
            order_quantity=float(qty),
            execution_price=float(exec_price_user),
            profit_currency=(str(profit_currency).upper() if profit_currency else None),
            symbol=symbol,
            leverage=float(leverage),
            instrument_type=int(instrument_type),
            prices_cache={},
            crypto_margin_factor=None,
            strict=True,
        )
        if single_margin is None:
            return False
        port = await fetch_user_portfolio(user_type, user_id)
        try:
            used_all = float(port.get("used_margin_all")) if port and port.get("used_margin_all") is not None else 0.0
        except (TypeError, ValueError):
            used_all = 0.0
        free = balance - used_all
        return free >= float(single_margin)
    except Exception:
        logger.exception("_validate_margin failed for %s:%s %s", user_type, user_id, symbol)
        return False


async def _send_cancel(order_id: str, cancel_id: str, order_type: str) -> bool:
    try:
        payload = {
            "original_id": str(order_id),
            "cancel_id": str(cancel_id),
            "order_type": str(order_type).upper(),
            "status": "CANCELLED",
        }
        ok, via = await send_provider_order(payload)
        if not ok:
            logger.warning("provider cancel send failed %s via=%s", order_id, via)
            return False
        return True
    except Exception:
        logger.exception("_send_cancel failed for %s", order_id)
        return False


async def _process_one(order_id: str) -> None:
    try:
        hk = _hkey(order_id)
        meta = await redis_cluster.hgetall(hk)
        if not meta:
            await redis_cluster.srem(SET_ACTIVE, order_id)
            return
        symbol = str(meta.get("symbol") or "").upper()
        order_type = str(meta.get("order_type") or "").upper()
        user_id = str(meta.get("user_id") or "")
        user_type = str(meta.get("user_type") or "").lower()
        group = str(meta.get("group") or "Standard")
        try:
            qty = float(meta.get("order_quantity")) if meta.get("order_quantity") is not None else 0.0
        except Exception:
            qty = 0.0
        # If already executed/cancelled, stop monitoring
        try:
            od = await redis_cluster.hgetall(f"order_data:{order_id}")
            st = (od.get("order_status") or "").upper() if od else ""
            if st in ("OPEN", "CANCELLED", "REJECTED"):
                await redis_cluster.srem(SET_ACTIVE, order_id)
                await redis_cluster.delete(hk)
                return
        except Exception:
            pass
        ask = await _get_ask(symbol)
        if not (ask and ask > 0):
            return
        ok_margin = await _validate_margin(user_type, user_id, group, symbol, qty, ask)
        if ok_margin:
            return
        # Need to cancel: fetch or generate cancel_id
        cancel_id = None
        try:
            od = await redis_cluster.hgetall(f"order_data:{order_id}")
            cancel_id = od.get("cancel_id") if od else None
        except Exception:
            cancel_id = None
        
        # If no cancel_id exists, generate one and store it
        if not cancel_id:
            from app.services.orders.id_generator import generate_cancel_id
            cancel_id = generate_cancel_id()
            logger.info("Generated cancel_id=%s for provider pending order %s", cancel_id, order_id)
            
            # Store the cancel_id in order_data for future reference
            try:
                await redis_cluster.hset(f"order_data:{order_id}", "cancel_id", cancel_id)
                # Also register in lifecycle service for tracking
                from app.services.orders.order_registry import add_lifecycle_id
                await add_lifecycle_id(order_id, cancel_id, "cancel_id")
            except Exception as e:
                logger.warning("Failed to store cancel_id for order %s: %s", order_id, e)
        # Avoid duplicate cancel requests
        sent_key = f"provider_pending_cancel_sent:{order_id}"
        nx = await redis_cluster.set(sent_key, "1", ex=300, nx=True)
        if not nx:
            return
        ok = await _send_cancel(order_id, cancel_id, order_type)
        if ok:
            await redis_cluster.srem(SET_ACTIVE, order_id)
            await redis_cluster.delete(hk)
    except Exception:
        logger.exception("_process_one failed for %s", order_id)


async def start_provider_pending_monitor():
    async def _loop():
        while True:
            try:
                ids = await redis_cluster.smembers(SET_ACTIVE)
                tasks = []
                for oid in ids or []:
                    if not oid:
                        continue
                    tasks.append(_process_one(str(oid)))
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
            except Exception:
                logger.exception("provider pending monitor loop error")
            await asyncio.sleep(float(os.getenv("PROVIDER_PENDING_TICK_SEC", "0.5")))
    asyncio.create_task(_loop())
