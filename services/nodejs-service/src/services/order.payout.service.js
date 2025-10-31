const sequelize = require('../config/db');
const { Transaction } = require('sequelize');
const { LiveUser, DemoUser } = require('../models');
const StrategyProviderAccount = require('../models/strategyProviderAccount.model');
const CopyFollowerAccount = require('../models/copyFollowerAccount.model');
const UserTransaction = require('../models/userTransaction.model');
const idGenerator = require('./idGenerator.service');
const logger = require('./logger.service');
const { redisCluster } = require('../../config/redis');
const { logOrderClosureSwap } = require('../utils/swap.logger');

function getUserModel(userType) {
  switch (userType) {
    case 'live':
      return LiveUser;
    case 'demo':
      return DemoUser;
    case 'strategy_provider':
      return StrategyProviderAccount;
    case 'copy_follower':
      return CopyFollowerAccount;
    default:
      return LiveUser; // Default fallback
  }
}

function toNum(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

/**
 * Apply order close payout atomically:
 * - Update user's wallet_balance by netProfit
 * - Create two transaction records: commission (debit) and profit/loss (credit/debit)
 *
 * IMPORTANT: This function does NOT implement idempotency by itself.
 * Callers must guard with an external idempotency key (e.g., Redis NX) per order_id.
 */
async function applyOrderClosePayout({
  userType,
  userId,
  orderPk = null,              // numeric SQL PK of the order row if available
  orderIdStr = null,           // string order_id like ord_YYYYMMDD_XXX (for metadata)
  netProfit = 0,
  commission = 0,
  profitUsd = 0,
  swap = 0,
  symbol = null,
  orderType = null,
}) {
  const np = toNum(netProfit);
  const com = Math.max(0, toNum(commission));
  const sw = toNum(swap);
  const pUsd = toNum(profitUsd);

  // Profit/Loss transaction amount aggregates profit_usd and swap
  // so that (profit_loss + (-commission)) == net_profit
  const profitLossAmount = np + com; // equals profit_usd + swap (if np = profit_usd - com + swap)
  const profitLossType = profitLossAmount >= 0 ? 'profit' : 'loss';

  const UserModel = getUserModel(String(userType));

  const txResult = await sequelize.transaction({ isolationLevel: Transaction.ISOLATION_LEVELS.READ_COMMITTED }, async (t) => {
    // Lock the user row for update to serialize wallet updates
    const user = await UserModel.findByPk(parseInt(String(userId), 10), {
      transaction: t,
      lock: t.LOCK.UPDATE,
    });
    if (!user) {
      throw new Error(`${String(userType)} user not found for payout`);
    }

    const balanceBefore = toNum(user.wallet_balance);
    const finalBalance = balanceBefore + np;

    // Running balance for audit trail across the two transactions
    let runningBalance = balanceBefore;

    // 1) Commission record (debit) if any
    if (com > 0) {
      const txnId = await idGenerator.generateTransactionId();
      const amount = -Math.abs(com);
      const after = runningBalance + amount;
      await UserTransaction.create({
        transaction_id: txnId,
        user_id: parseInt(String(userId), 10),
        user_type: String(userType),
        order_id: orderPk || null,
        type: 'commission',
        amount: amount,
        balance_before: runningBalance,
        balance_after: after,
        status: 'completed',
        notes: `Commission for order ${orderIdStr || orderPk || ''}`,
        metadata: {
          order_id: orderIdStr,
          order_pk: orderPk,
          symbol,
          order_type: orderType,
          commission,
          profit_usd: pUsd,
          net_profit: np,
          swap: sw,
        },
      }, { transaction: t });
      runningBalance = after;
    }

    // 2) Profit/Loss record
    {
      const txnId = await idGenerator.generateTransactionId();
      const amount = profitLossType === 'profit' ? Math.abs(profitLossAmount) : -Math.abs(profitLossAmount);
      const after = finalBalance; // ensure records reconcile to final wallet
      await UserTransaction.create({
        transaction_id: txnId,
        user_id: parseInt(String(userId), 10),
        user_type: String(userType),
        order_id: orderPk || null,
        type: profitLossType,
        amount: amount,
        balance_before: runningBalance,
        balance_after: after,
        status: 'completed',
        notes: `${profitLossType === 'profit' ? 'Profit' : 'Loss'} from order ${orderIdStr || orderPk || ''}`,
        metadata: {
          order_id: orderIdStr,
          order_pk: orderPk,
          symbol,
          order_type: orderType,
          commission,
          profit_usd: pUsd,
          net_profit: np,
          swap: sw,
        },
      }, { transaction: t });
      runningBalance = after;
    }

    // Update user's wallet to final balance
    await user.update({ wallet_balance: finalBalance }, { transaction: t });

    logger.info('Applied order close payout', {
      userId: String(userId),
      userType: String(userType),
      orderPk,
      orderIdStr,
      balanceBefore,
      finalBalance,
      netProfit: np,
      commission: com,
      profitLossAmount,
      swap: sw,
    });

    // Log swap closure details if swap amount exists
    if (sw !== 0) {
      logOrderClosureSwap({
        order_id: orderIdStr || orderPk?.toString() || 'unknown',
        user_id: parseInt(String(userId), 10),
        user_type: String(userType),
        symbol: symbol || 'unknown',
        group_name: 'unknown', // Would need to be passed from caller
        order_type: orderType || 'unknown',
        order_quantity: 0, // Would need to be passed from caller
        order_duration_days: 0, // Would need to be calculated from order creation date
        total_swap_accumulated: sw,
        final_swap_transaction_id: null, // Swap is included in profit/loss transaction
        closure_date: new Date().toISOString(),
        net_profit_before_swap: pUsd,
        net_profit_after_swap: np
      });
    }

    return { ok: true, balance_before: balanceBefore, balance_after: finalBalance };
  });

  // After successful commit, sync Redis user config wallet_balance (best-effort)
  try {
    const key = `user:{${String(userType)}:${String(userId)}}:config`;
    await redisCluster.hset(key, { wallet_balance: String(txResult.balance_after) });
    
    // For strategy providers and copy followers, also update their account-specific cache
    if (String(userType) === 'strategy_provider' || String(userType) === 'copy_follower') {
      logger.info('Updated wallet balance for copy trading account', {
        userType: String(userType),
        userId: String(userId),
        balanceAfter: txResult.balance_after,
        netProfit: np,
        commission: com
      });
    }
  } catch (e) {
    logger.warn('Failed to sync Redis wallet_balance after payout', { error: e.message, userId: String(userId), userType: String(userType) });
  }

  return txResult;
}

module.exports = { applyOrderClosePayout };
