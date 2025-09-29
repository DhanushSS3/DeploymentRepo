const { CryptoPayment, UserTransaction, LiveUser } = require('../models');
const sequelize = require('../config/db');
const crypto = require('crypto');
const axios = require('axios');
const logger = require('./logger.service');
const redisUserCache = require('./redis.user.cache.service');
const idGenerator = require('./idGenerator.service');
const { cryptoPaymentLogger } = require('./logging');

class CryptoPaymentService {
  constructor() {
    this.tyltApiUrl = 'https://api.tylt.money/transactions/merchant/createPayinRequest';
    this.apiKey = process.env.TLP_API_KEY;
    this.apiSecret = process.env.TLP_API_SECRET;
    this.callbackUrl = process.env.TLP_CALLBACK_URL || 'https://livefxhubv1.livefxhub.com/api/crypto-payments/webhook';
  }

  /**
   * Create HMAC SHA-256 signature for Tylt API
   * @param {string} secret - API secret key
   * @param {string} data - JSON stringified request body
   * @returns {string} HMAC signature
   */
  createSignature(secret, data) {
    return crypto.createHmac('sha256', secret)
                 .update(data)
                 .digest('hex');
  }

  /**
   * Validate HMAC signature from Tylt webhook
   * @param {string} signature - Signature from X-TLP-SIGNATURE header
   * @param {string|Object} body - Raw request body string or parsed object
   * @returns {boolean} True if signature is valid
   */
  validateWebhookSignature(signature, body) {
    try {
      // Ensure we're using the correct format for HMAC calculation
      const bodyString = typeof body === 'string' ? body : JSON.stringify(body);
      const calculatedSignature = this.createSignature(this.apiSecret, bodyString);
      
      logger.info('HMAC Signature Validation', {
        receivedSignature: signature,
        calculatedSignature: calculatedSignature,
        bodyLength: bodyString.length,
        match: calculatedSignature === signature
      });
      
      return calculatedSignature === signature;
    } catch (error) {
      logger.error('Error validating webhook signature', { 
        error: error.message,
        signature,
        bodyType: typeof body
      });
      return false;
    }
  }

  /**
   * Create a new crypto payment deposit request
   * @param {Object} paymentData - Payment request data
   * @param {string} paymentData.user_id - User ID
   * @param {string} paymentData.baseAmount - Amount to be paid
   * @param {string} paymentData.baseCurrency - Base currency
   * @param {string} paymentData.settledCurrency - Settlement currency
   * @param {string} paymentData.networkSymbol - Network symbol
   * @param {string} paymentData.customerName - Optional customer name
   * @param {string} paymentData.comments - Optional comments
   * @returns {Object} Payment response with URL and order details
   */
  async createDepositRequest(paymentData) {
    const flowId = `flow_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    const startTime = Date.now();
    
    try {
      const { user_id, baseAmount, baseCurrency, settledCurrency, networkSymbol, customerName, comments, _ip, _userAgent } = paymentData;

      // Validate required fields
      if (!user_id || !baseAmount || !baseCurrency || !settledCurrency || !networkSymbol) {
        throw new Error('Missing required payment fields');
      }

      // Generate unique merchant order ID
      const merchantOrderId = CryptoPayment.generateMerchantOrderId();

      // Prepare request body for Tylt API
      const requestBody = {
        merchantOrderId,
        baseAmount: parseFloat(baseAmount),
        baseCurrency,
        settledCurrency,
        networkSymbol,
        callBackUrl: this.callbackUrl,
        settleUnderpayment: 1, // Auto-settle underpayments
        ...(customerName && { customerName }),
        ...(comments && { comments })
      };

      // Convert to JSON string for signing
      const raw = JSON.stringify(requestBody);

      // Generate HMAC signature
      const signature = this.createSignature(this.apiSecret, raw);

      // Prepare headers
      const headers = {
        'X-TLP-APIKEY': this.apiKey,
        'X-TLP-SIGNATURE': signature,
        'Content-Type': 'application/json',
        'User-Agent': 'LiveFXHub-CryptoGateway/1.0'
      };

      logger.info('Creating Tylt payment request', { 
        merchantOrderId, 
        userId: user_id, 
        amount: baseAmount, 
        currency: baseCurrency 
      });

      // Log outgoing request details
      const apiRequestTimestamp = new Date().toISOString();
      cryptoPaymentLogger.logOutgoingRequest(
        'POST',
        this.tyltApiUrl,
        headers,
        requestBody,
        merchantOrderId,
        parseInt(user_id)
      );

      // Call Tylt API with timing
      const apiStartTime = Date.now();
      const response = await axios.post(this.tyltApiUrl, raw, { headers });
      const responseTime = Date.now() - apiStartTime;

      // Log incoming response details
      const apiResponseTimestamp = new Date().toISOString();
      cryptoPaymentLogger.logIncomingResponse(
        response.status,
        response.headers,
        response.data,
        merchantOrderId,
        parseInt(user_id),
        responseTime
      );

      if (!response.data || !response.data.data) {
        throw new Error('Invalid response from Tylt API');
      }

      const tyltData = response.data.data;

      // Save payment record to database
      const cryptoPayment = await CryptoPayment.create({
        userId: parseInt(user_id),
        merchantOrderId,
        orderId: tyltData.orderId, // Store Tylt's order ID
        baseAmount: parseFloat(baseAmount),
        baseCurrency,
        settledCurrency,
        networkSymbol,
        status: 'PENDING',
        transactionDetails: {
          tyltResponse: tyltData,
          depositAddress: tyltData.depositAddress,
          paymentURL: tyltData.paymentURL,
          expiresAt: tyltData.expiresAt,
          commission: tyltData.commission
        },
        settledAmountRequested: tyltData.settledAmountRequested,
        commission: tyltData.commission
      });

      logger.info('Crypto payment record created', { 
        id: cryptoPayment.id, 
        merchantOrderId, 
        userId: user_id 
      });

      // Log complete WebSocket flow
      const totalFlowTime = Date.now() - startTime;
      const userResponseTimestamp = new Date().toISOString();
      
      cryptoPaymentLogger.logWebSocketFlow({
        flowId,
        userId: parseInt(user_id),
        merchantOrderId,
        userRequestedAmount: parseFloat(baseAmount),
        userRequestedCurrency: baseCurrency,
        networkSymbol,
        userRequestTimestamp: new Date(startTime).toISOString(),
        apiRequestTimestamp,
        apiResponseTimestamp,
        userResponseTimestamp,
        apiEndpoint: this.tyltApiUrl,
        apiStatusCode: response.status,
        responseTime,
        requestSize: raw.length,
        responseSize: JSON.stringify(response.data).length,
        apiSuccess: !!tyltData.paymentURL,
        paymentUrl: tyltData.paymentURL,
        expiresAt: tyltData.expiresAt,
        totalFlowTime,
        success: true,
        ip: _ip || 'unknown',
        userAgent: _userAgent || 'unknown'
      });

      // Return success response
      return {
        status: true,
        message: 'PaymentUrl Generated Successfully',
        data: {
          paymentUrl: tyltData.paymentURL,
          merchantOrderId,
          expiresAt: tyltData.expiresAt
        }
      };

    } catch (error) {
      // Log WebSocket error
      cryptoPaymentLogger.logWebSocketError('payment_creation', error, {
        userId: paymentData.user_id,
        merchantOrderId: paymentData.merchantOrderId,
        requestedAmount: paymentData.baseAmount
      });

      logger.error('Error creating crypto payment deposit', { 
        error: error.message, 
        userId: paymentData.user_id,
        stack: error.stack 
      });

      if (error.response) {
        // Tylt API error
        logger.error('Tylt API error response', { 
          status: error.response.status, 
          data: error.response.data 
        });
        throw new Error(`Payment gateway error: ${error.response.data?.msg || error.response.statusText}`);
      }

      throw new Error(`Failed to create payment request: ${error.message}`);
    }
  }

  /**
   * Get crypto payment by merchant order ID
   * @param {string} merchantOrderId - Merchant order ID
   * @returns {Object} Crypto payment record
   */
  async getPaymentByMerchantOrderId(merchantOrderId) {
    try {
      const payment = await CryptoPayment.findOne({
        where: { merchantOrderId }
      });

      if (!payment) {
        throw new Error('Payment not found');
      }

      return payment;
    } catch (error) {
      logger.error('Error fetching payment by merchant order ID', { 
        merchantOrderId, 
        error: error.message 
      });
      throw error;
    }
  }

  /**
   * Get crypto payments by user ID
   * @param {number} userId - User ID
   * @param {Object} options - Query options
   * @returns {Array} List of crypto payments
   */
  async getPaymentsByUserId(userId, options = {}) {
    try {
      const { limit = 50, offset = 0, status } = options;
      
      const whereClause = { userId };
      if (status) {
        whereClause.status = status;
      }

      const payments = await CryptoPayment.findAll({
        where: whereClause,
        limit,
        offset,
        order: [['created_at', 'DESC']]
      });

      return payments;
    } catch (error) {
      logger.error('Error fetching payments by user ID', { 
        userId, 
        error: error.message 
      });
      throw error;
    }
  }

  /**
   * Update payment status and details from webhook
   * @param {string} merchantOrderId - Merchant order ID
   * @param {Object} webhookData - Complete webhook data from Tylt
   * @returns {Object} Updated payment record
   */
  async updatePaymentFromWebhook(merchantOrderId, webhookData) {
    const transaction = await sequelize.transaction();
    const processingStartTime = Date.now();
    
    try {
      const payment = await CryptoPayment.findOne({
        where: { merchantOrderId },
        transaction
      });

      if (!payment) {
        throw new Error('Payment not found for webhook update');
      }

      const { status, baseAmountReceived, settledAmountReceived, settledAmountCredited, commission } = webhookData;
      
      // Map Tylt status to internal status
      const internalStatus = this.mapTyltStatusToInternal(status);
      
      // Get user's current wallet balance for logging
      const user = await LiveUser.findByPk(payment.userId, { transaction });
      const previousWalletBalance = user ? parseFloat(user.wallet_balance) || 0 : 0;
      
      // Update payment with webhook data
      const updatedPayment = await payment.update({
        status: internalStatus,
        baseAmountReceived: baseAmountReceived ? parseFloat(baseAmountReceived) : payment.baseAmountReceived,
        settledAmountReceived: settledAmountReceived ? parseFloat(settledAmountReceived) : payment.settledAmountReceived,
        settledAmountCredited: settledAmountCredited ? parseFloat(settledAmountCredited) : payment.settledAmountCredited,
        commission: commission ? parseFloat(commission) : payment.commission,
        transactionDetails: {
          ...payment.transactionDetails,
          webhookData,
          lastUpdated: new Date().toISOString()
        }
      }, { transaction });

      let walletCreditSuccess = false;
      let walletCreditAmount = 0;
      let newWalletBalance = previousWalletBalance;
      let transactionId = null;

      // Credit user wallet if payment is successful (completed, underpayment, or overpayment)
      logger.info('Wallet credit check', {
        merchantOrderId,
        internalStatus,
        baseAmountReceived,
        shouldCredit: ['COMPLETED', 'UNDERPAYMENT', 'OVERPAYMENT'].includes(internalStatus) && baseAmountReceived
      });

      if (['COMPLETED', 'UNDERPAYMENT', 'OVERPAYMENT'].includes(internalStatus) && baseAmountReceived) {
        try {
          logger.info('Attempting wallet credit', {
            merchantOrderId,
            userId: payment.userId,
            amount: baseAmountReceived,
            internalStatus
          });

          const creditResult = await this.creditUserWallet(payment.userId, parseFloat(baseAmountReceived), webhookData, transaction);
          walletCreditSuccess = true;
          walletCreditAmount = parseFloat(baseAmountReceived);
          newWalletBalance = previousWalletBalance + walletCreditAmount;
          transactionId = creditResult.transactionId;

          logger.info('Wallet credit successful', {
            merchantOrderId,
            userId: payment.userId,
            amount: walletCreditAmount,
            transactionId,
            newBalance: newWalletBalance
          });
        } catch (creditError) {
          logger.error('Failed to credit user wallet', { 
            merchantOrderId, 
            userId: payment.userId,
            amount: baseAmountReceived,
            error: creditError.message,
            stack: creditError.stack
          });
          // Don't fail the entire webhook processing if wallet credit fails
        }
      } else {
        logger.info('Wallet credit skipped', {
          merchantOrderId,
          internalStatus,
          baseAmountReceived,
          reason: !['COMPLETED', 'UNDERPAYMENT', 'OVERPAYMENT'].includes(internalStatus) 
            ? 'Status not eligible for credit' 
            : 'No baseAmountReceived'
        });
      }

      await transaction.commit();
      
      const processingTime = Date.now() - processingStartTime;

      // Log detailed webhook processing
      cryptoPaymentLogger.logWebhookProcessing(webhookData, {
        receivedAt: new Date().toISOString(),
        ip: webhookData._ip || 'unknown',
        userAgent: webhookData._userAgent || 'unknown',
        signature: webhookData._signature || 'unknown',
        signatureValid: webhookData._signatureValid || false,
        dbUpdateSuccess: true,
        walletCreditSuccess,
        walletCreditAmount,
        previousWalletBalance,
        newWalletBalance,
        transactionId,
        processingTime
      });
      
      logger.info('Payment updated from webhook', { 
        merchantOrderId, 
        status: internalStatus,
        paymentId: payment.id,
        baseAmountReceived,
        walletCredited: walletCreditSuccess
      });

      // Return updated payment with previous status for logging
      return {
        ...updatedPayment.toJSON(),
        previousStatus: payment.status
      };
    } catch (error) {
      await transaction.rollback();
      
      const processingTime = Date.now() - processingStartTime;
      
      // Log webhook processing failure
      cryptoPaymentLogger.logWebhookProcessing(webhookData, {
        receivedAt: new Date().toISOString(),
        ip: webhookData._ip || 'unknown',
        userAgent: webhookData._userAgent || 'unknown',
        signature: webhookData._signature || 'unknown',
        signatureValid: webhookData._signatureValid || false,
        dbUpdateSuccess: false,
        walletCreditSuccess: false,
        walletCreditAmount: 0,
        previousWalletBalance: 0,
        newWalletBalance: 0,
        transactionId: null,
        processingTime,
        error: error.message
      });
      
      logger.error('Error updating payment from webhook', { 
        merchantOrderId, 
        error: error.message,
        stack: error.stack 
      });
      throw error;
    }
  }

  /**
   * Map Tylt payment status to internal status (case-insensitive)
   * @param {string} tyltStatus - Status from Tylt
   * @returns {string} Internal status
   */
  mapTyltStatusToInternal(tyltStatus) {
    const statusMap = {
      'pending': 'PENDING',
      'waiting': 'PENDING',
      'processing': 'PROCESSING',
      'confirming': 'PROCESSING',
      'completed': 'COMPLETED',
      'paid': 'COMPLETED',
      'success': 'COMPLETED',
      'underpayment': 'UNDERPAYMENT',
      'under payment': 'UNDERPAYMENT',  // Added mapping for "Under Payment"
      'overpayment': 'OVERPAYMENT',
      'over payment': 'OVERPAYMENT',    // Added mapping for "Over Payment"
      'failed': 'FAILED',
      'cancelled': 'CANCELLED',
      'expired': 'FAILED'
    };

    const normalizedStatus = tyltStatus ? tyltStatus.toLowerCase() : '';
    return statusMap[normalizedStatus] || 'PENDING';
  }

  /**
   * Credit user wallet and create transaction record (Live users only)
   * @param {number} userId - User ID
   * @param {number} amount - Amount to credit (baseAmountReceived)
   * @param {Object} webhookData - Webhook data for reference
   * @param {Object} transaction - Database transaction
   */
  async creditUserWallet(userId, amount, webhookData, transaction) {
    try {
      // Find live user (crypto deposits only for live users)
      const user = await LiveUser.findByPk(userId, { transaction });
      const userType = 'live';
      
      if (!user) {
        throw new Error(`Live user not found with ID: ${userId}`);
      }

      // Get current wallet balance
      const currentBalance = parseFloat(user.wallet_balance) || 0;
      const newBalance = currentBalance + amount;
      
      // Update user wallet balance
      await user.update({
        wallet_balance: newBalance
      }, { transaction });

      // Generate unique transaction ID (Redis-backed, atomic)
      const transactionId = await idGenerator.generateTransactionId();
      
      // Create transaction record
      await UserTransaction.create({
        transaction_id: transactionId,
        user_id: userId,
        user_type: userType,
        type: 'deposit',
        amount: amount,
        balance_before: currentBalance,
        balance_after: newBalance,
        status: 'completed',
        reference_id: webhookData.merchantOrderId,
        notes: `Crypto deposit via Tylt - ${webhookData.status}`,
        metadata: {
          paymentGateway: 'tylt',
          orderId: webhookData.orderId,
          merchantOrderId: webhookData.merchantOrderId,
          baseCurrency: webhookData.baseCurrency,
          settledCurrency: webhookData.settledCurrency,
          network: webhookData.network,
          depositAddress: webhookData.depositAddress,
          transactionHash: webhookData.transactions?.[0]?.transactionHash,
          commission: webhookData.commission,
          webhookReceivedAt: new Date().toISOString()
        }
      }, { transaction });

      // Update Redis user cache with new wallet balance
      try {
        await redisUserCache.updateUser('live', userId, {
          wallet_balance: newBalance
        });
        logger.info('Redis user cache updated after wallet credit', {
          userId,
          userType,
          previousBalance: currentBalance,
          newBalance
        });
      } catch (cacheError) {
        // Log cache update failure but don't fail the transaction
        logger.error('Failed to update Redis cache after wallet credit', {
          userId,
          userType,
          amount,
          error: cacheError.message
        });
      }

      logger.info('User wallet credited successfully', {
        userId,
        userType,
        amount,
        previousBalance: currentBalance,
        newBalance,
        transactionId,
        merchantOrderId: webhookData.merchantOrderId
      });

      return { transactionId, previousBalance: currentBalance, newBalance };

    } catch (error) {
      logger.error('Error crediting user wallet', {
        userId,
        amount,
        error: error.message,
        stack: error.stack
      });
      throw error;
    }
  }

  // Transaction ID generation is delegated to idGenerator (Redis-backed)
}

module.exports = new CryptoPaymentService();
