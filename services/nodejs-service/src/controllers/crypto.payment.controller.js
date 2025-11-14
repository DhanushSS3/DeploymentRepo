const cryptoPaymentService = require('../services/crypto.payment.service');
const logger = require('../utils/logger');
const { cryptoPaymentLogger } = require('../services/logging');

class CryptoPaymentController {
  /**
   * Create a new crypto payment deposit request
   * @route POST /api/crypto-payments/deposit
   */
  async createDeposit(req, res) {
    try {
      const { baseAmount, baseCurrency, settledCurrency, networkSymbol, user_id, customerName, comments } = req.body;
      
      // Always use authenticated user's ID for security (ignore user_id from request body)
      const userId = req.user && (req.user.sub || req.user.user_id);

      // Log the deposit request from frontend
      cryptoPaymentLogger.logDepositRequest(
        userId, 
        { baseAmount, baseCurrency, settledCurrency, networkSymbol, customerName, comments },
        req.ip,
        req.get('User-Agent')
      );

      // Validate required fields
      if (!baseAmount || !baseCurrency || !settledCurrency || !networkSymbol) {
        return res.status(400).json({
          status: false,
          message: 'Missing required fields: baseAmount, baseCurrency, settledCurrency, networkSymbol'
        });
      }
      
      // Ensure user is authenticated
      if (!userId) {
          return res.status(401).json({
            status: false,
            message: 'Authentication required. Please log in to create a deposit.'
          });
        }

      // Validate amount is positive number
      const amount = parseFloat(baseAmount);
      if (isNaN(amount) || amount <= 0) {
        return res.status(400).json({
          status: false,
          message: 'baseAmount must be a positive number'
        });
      }

      // Validate user_id is positive integer
      const userIdInt = parseInt(userId);
      if (isNaN(userIdInt) || userIdInt <= 0) {
        return res.status(400).json({
          status: false,
          message: 'user_id must be a positive integer'
        });
      }

      logger.info('Creating crypto deposit request', { 
        userId: userIdInt, 
        amount: baseAmount, 
        currency: baseCurrency,
        network: networkSymbol 
      });

      const result = await cryptoPaymentService.createDepositRequest({
        user_id: userIdInt.toString(),
        baseAmount,
        baseCurrency,
        settledCurrency,
        networkSymbol,
        customerName,
        comments,
        // Add request context for logging
        _ip: req.ip,
        _userAgent: req.get('User-Agent')
      });

      // Log the Tylt API response
      cryptoPaymentLogger.logTyltResponse(
        userIdInt,
        result.data?.merchantOrderId,
        result.data || result
      );

      res.status(200).json(result);

    } catch (error) {
      logger.error('Error in createDeposit controller', { 
        error: error.message,
        body: req.body,
        stack: error.stack 
      });

      res.status(500).json({
        status: false,
        message: error.message || 'Failed to create deposit request'
      });
    }
  }

  /**
   * Get payment details by merchant order ID
   * @route GET /api/crypto-payments/:merchantOrderId
   */
  async getPaymentByOrderId(req, res) {
    try {
      const { merchantOrderId } = req.params;

      if (!merchantOrderId) {
        return res.status(400).json({
          status: false,
          message: 'merchantOrderId is required'
        });
      }

      const payment = await cryptoPaymentService.getPaymentByMerchantOrderId(merchantOrderId);

      res.status(200).json({
        status: true,
        message: 'Payment retrieved successfully',
        data: payment
      });

    } catch (error) {
      logger.error('Error in getPaymentByOrderId controller', { 
        merchantOrderId: req.params.merchantOrderId,
        error: error.message 
      });

      if (error.message === 'Payment not found') {
        return res.status(404).json({
          status: false,
          message: 'Payment not found'
        });
      }

      res.status(500).json({
        status: false,
        message: 'Failed to retrieve payment'
      });
    }
  }

  /**
   * Get payments by user ID
   * @route GET /api/crypto-payments/user/:userId
   */
  async getPaymentsByUserId(req, res) {
    try {
      const { userId } = req.params;
      const { limit, offset, status } = req.query;

      if (!userId) {
        return res.status(400).json({
          status: false,
          message: 'userId is required'
        });
      }

      const userIdInt = parseInt(userId);
      if (isNaN(userIdInt) || userIdInt <= 0) {
        return res.status(400).json({
          status: false,
          message: 'userId must be a positive integer'
        });
      }

      const options = {};
      if (limit) options.limit = parseInt(limit);
      if (offset) options.offset = parseInt(offset);
      if (status) options.status = status;

      const payments = await cryptoPaymentService.getPaymentsByUserId(userIdInt, options);

      res.status(200).json({
        status: true,
        message: 'Payments retrieved successfully',
        data: payments,
        count: payments.length
      });

    } catch (error) {
      logger.error('Error in getPaymentsByUserId controller', { 
        userId: req.params.userId,
        error: error.message 
      });

      res.status(500).json({
        status: false,
        message: 'Failed to retrieve payments'
      });
    }
  }

  /**
   * Handle webhook from Tylt payment gateway
   * @route POST /api/crypto-payments/webhook
   */
  async handleWebhook(req, res) {
    try {
      logger.info('Received Tylt webhook', { 
        body: req.body,
        headers: req.headers 
      });

      // Validate HMAC signature
      const signature = req.headers['x-tlp-signature'];
      const rawBody = req.rawBody || JSON.stringify(req.body);
      
      logger.info('Webhook signature validation details', {
        hasSignature: !!signature,
        hasRawBody: !!req.rawBody,
        bodyType: typeof req.body,
        signatureHeader: signature ? 'present' : 'missing',
        bodyLength: rawBody ? rawBody.length : 0
      });
      
      if (!signature) {
        logger.warn('Webhook received without signature', {
          headers: req.headers,
          body: req.body
        });
        return res.status(400).send('Missing X-TLP-SIGNATURE header');
      }

      const isValidSignature = cryptoPaymentService.validateWebhookSignature(signature, rawBody);
      
      if (!isValidSignature) {
        // Log signature validation failure with expected signature for development
        const expectedSignature = cryptoPaymentService.createSignature(process.env.TLP_API_SECRET, rawBody);
        
        // Debug: Write expected HMAC signature to application.log
        logger.error('🔐 HMAC Signature Validation Failed', {
          receivedSignature: signature,
          expectedSignature: expectedSignature,
          rawBodyLength: rawBody ? rawBody.length : 0,
          ip: req.ip,
          userAgent: req.get('User-Agent')
        });
        
        cryptoPaymentLogger.logSignatureValidationFailure(
          signature,
          expectedSignature,
          rawBody,
          req.ip,
          req.get('User-Agent')
        );
        
        logger.error('Invalid webhook signature', { signature });
        return res.status(400).send('Invalid HMAC signature');
      }

      // Extract webhook data - handle nested data structure
      const webhookData = req.body.data || req.body;
      const { merchantOrderId } = webhookData;

      // Add request context to webhook data for detailed logging
      webhookData._ip = req.ip;
      webhookData._userAgent = req.get('User-Agent');
      webhookData._signature = signature;
      webhookData._signatureValid = isValidSignature;

      // Log webhook callback
      cryptoPaymentLogger.logWebhookCallback(
        webhookData,
        signature,
        isValidSignature,
        req.ip,
        req.get('User-Agent')
      );

      if (!merchantOrderId) {
        return res.status(400).json({
          status: false,
          message: 'merchantOrderId is required in webhook'
        });
      }

      // Update payment record with complete webhook data
      const updatedPayment = await cryptoPaymentService.updatePaymentFromWebhook(merchantOrderId, webhookData);

      // Log payment status update
      cryptoPaymentLogger.logPaymentUpdate(
        merchantOrderId,
        updatedPayment.previousStatus,
        webhookData.status,
        updatedPayment.id
      );

      logger.info('Payment updated from webhook', { 
        merchantOrderId, 
        status: webhookData.status,
        paymentId: updatedPayment.id 
      });

      // Respond with "ok" as required by Tylt
      res.status(200).send('ok');

    } catch (error) {
      logger.error('Error processing webhook', { 
        error: error.message,
        body: req.body,
        stack: error.stack 
      });

      res.status(500).send('Failed to process webhook');
    }
  }

}

module.exports = new CryptoPaymentController();
