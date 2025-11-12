import asyncio
import logging
from typing import Optional

from app.config.redis_config import redis_cluster, redis_pubsub_client
from .notifier import EmailNotifier
from .liquidation import LiquidationEngine

logger = logging.getLogger(__name__)

ALERT_TTL_SEC = 10800  # 3 hours (3 * 60 * 60)
SEM_LIMIT = 50


async def _get_margin_level(user_type: str, user_id: str) -> float:
    try:
        # Fetch only required fields to reduce payload
        pf = await redis_cluster.hmget(f"user_portfolio:{{{user_type}:{user_id}}}", ["margin_level", "used_margin"])
        if pf and pf[0] is not None:
            try:
                margin_level = float(pf[0])
                used_margin = float(pf[1] or 0)
                if used_margin == 0:
                    logger.debug("AutoCutoffWatcher: User %s:%s has no used margin (%.2f), treating as safe", user_type, user_id, used_margin)
                    return 999.0
                return margin_level
            except (ValueError, TypeError) as e:
                logger.warning("AutoCutoffWatcher: Failed to parse margin_level for %s:%s: %s (raw=%s)", user_type, user_id, e, pf)
                return 0.0
    except Exception as e:
        logger.warning("AutoCutoffWatcher: Failed to get margin level for %s:%s: %s", user_type, user_id, e)
    logger.debug("AutoCutoffWatcher: No margin data for %s:%s, returning 0.0", user_type, user_id)
    return 0.0


async def _get_user_email(user_type: str, user_id: str) -> Optional[str]:
    try:
        key = f"user:{{{user_type}:{user_id}}}:config"
        data = await redis_cluster.hgetall(key)
        em = (data or {}).get("email")
        if em:
            return str(em)
    except Exception:
        pass
    return None


async def _clear_flags(user_type: str, user_id: str):
    try:
        pipe = redis_cluster.pipeline()
        pipe.delete(f"autocutoff:liquidating:{user_type}:{user_id}")
        pipe.delete(f"autocutoff:alert_sent:{user_type}:{user_id}")
        await pipe.execute()
    except Exception:
        pass


async def _handle_user(user_type: str, user_id: str, notifier: EmailNotifier, liq: LiquidationEngine):
    ml = await _get_margin_level(user_type, user_id)
    # logger.info("AutoCutoffWatcher: checking user %s:%s margin_level=%.2f", user_type, user_id, ml)
    
    if ml >= 50.0:
        logger.debug("AutoCutoffWatcher: user %s:%s is safe (margin_level=%.2f)", user_type, user_id, ml)
        await _clear_flags(user_type, user_id)
        return

    # Alert zone
    if 10.0 <= ml < 50.0:
        logger.warning("AutoCutoffWatcher: ALERT TRIGGERED for user %s:%s (margin_level=%.2f < 50.0)", user_type, user_id, ml)
        # rate-limit via Redis TTL flag with atomic check-and-set
        alert_key = f"autocutoff:alert_sent:{user_type}:{user_id}"
        try:
            # Use SET with NX (only if not exists) to prevent race conditions
            # TTL is 3 hours to limit alerts to once every 3 hours
            already_set = await redis_cluster.set(alert_key, "1", ex=ALERT_TTL_SEC, nx=True)
            if not already_set:
                # Alert already sent within TTL period
                logger.info("AutoCutoffWatcher: alert already sent for %s:%s (within 3h TTL)", user_type, user_id)
                return
        except Exception as e:
            logger.error("AutoCutoffWatcher: error checking alert flag for %s:%s: %s", user_type, user_id, e)
            # Fallback to simple check if SET NX fails
            try:
                already = await redis_cluster.get(alert_key)
                if already:
                    logger.info("AutoCutoffWatcher: alert already sent for %s:%s (fallback check)", user_type, user_id)
                    return
            except Exception:
                pass
        
        email = await _get_user_email(user_type, user_id)
        logger.info("AutoCutoffWatcher: sending alert email to %s for %s:%s", email, user_type, user_id)
        ok = await notifier.send_alert(user_type=user_type, user_id=user_id, email=email, margin_level=ml, threshold=50.0)
        if ok:
            logger.info("AutoCutoffWatcher: alert email sent successfully to %s for %s:%s", email, user_type, user_id)
        else:
            logger.error("AutoCutoffWatcher: alert email FAILED for %s:%s", user_type, user_id)
            # If email failed, remove the TTL key so we can retry later
            try:
                await redis_cluster.delete(alert_key)
            except Exception:
                pass
        return

    # Liquidation zone
    if ml < 10.0:
        logger.warning("AutoCutoffWatcher: LIQUIDATION TRIGGERED for user %s:%s (margin_level=%.2f < 10.0)", user_type, user_id, ml)
        liq_key = f"autocutoff:liquidating:{user_type}:{user_id}"
        try:
            got = await redis_cluster.set(liq_key, "1", nx=True)
        except Exception as e:
            logger.error("AutoCutoffWatcher: failed to set liquidation flag for %s:%s: %s", user_type, user_id, e)
            got = None
        if not got:
            # already running
            logger.info("AutoCutoffWatcher: liquidation already in progress for %s:%s", user_type, user_id)
            return
        try:
            logger.info("AutoCutoffWatcher: starting liquidation for %s:%s", user_type, user_id)
            await liq.run(user_type=user_type, user_id=user_id)
            logger.info("AutoCutoffWatcher: completed liquidation for %s:%s", user_type, user_id)
        finally:
            try:
                await redis_cluster.delete(liq_key)
            except Exception:
                pass


async def _handle_user_limited(user_type: str, user_id: str, notifier: EmailNotifier, liq: LiquidationEngine, sem: asyncio.Semaphore):
    async with sem:
        await _handle_user(user_type, user_id, notifier, liq)


async def _watch_loop():
    notifier = EmailNotifier()
    liq = LiquidationEngine()
    sem = asyncio.Semaphore(SEM_LIMIT)

    # Optional: log pool usage once at startup
    try:
        pool = redis_cluster.connection_pool
        logger.info("AutoCutoffWatcher: Redis pool max=%s", getattr(pool, "max_connections", None))
    except Exception:
        pass

    # Subscribe to portfolio updates
    pubsub = redis_pubsub_client.pubsub()
    await pubsub.subscribe("portfolio_updates")
    logger.info("AutoCutoffWatcher subscribed to portfolio_updates")

    try:
        async for message in pubsub.listen():
            try:
                if message.get("type") != "message":
                    continue
                data = str(message.get("data") or "")
                if ":" not in data:
                    continue
                user_type, user_id = data.split(":", 1)
                user_type = user_type.strip().lower()
                user_id = user_id.strip()
                logger.debug("AutoCutoffWatcher: received update for %s:%s", user_type, user_id)
                # Fire-and-forget per-user handler with concurrency limit
                asyncio.create_task(_handle_user_limited(user_type, user_id, notifier, liq, sem))
            except Exception as e:
                logger.error("AutoCutoffWatcher message processing error: %s", e)
    except asyncio.CancelledError:
        logger.info("AutoCutoffWatcher cancelled")
    except Exception as e:
        logger.exception("AutoCutoffWatcher error: %s", e)
        # Attempt to reconnect on timeout or transient errors
        try:
            await pubsub.unsubscribe("portfolio_updates")
            await pubsub.close()
        except Exception:
            pass
        await asyncio.sleep(2)
        asyncio.create_task(_watch_loop())
    finally:
        try:
            await pubsub.unsubscribe("portfolio_updates")
            await pubsub.close()
        except Exception:
            pass


async def start_autocutoff_watcher():
    logger.info("Starting AutoCutoffWatcher...")
    asyncio.create_task(_watch_loop())
