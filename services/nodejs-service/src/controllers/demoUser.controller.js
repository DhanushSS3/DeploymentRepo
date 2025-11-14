const DemoUser = require('../models/demoUser.model');
const { generateAccountNumber } = require('../services/accountNumber.service');
const { hashPassword } = require('../services/password.service');
const { validationResult } = require('express-validator');
const { Op } = require('sequelize');
const TransactionService = require('../services/transaction.service');
const logger = require('../services/logger.service');
const ErrorResponse = require('../utils/errorResponse.util');
const { IdempotencyService } = require('../services/idempotency.service');
const { comparePassword } = require('../services/password.service');
const jwt = require('jsonwebtoken');
const { logDemoUserLogin } = require('../services/loginLogger');
const redisUserCache = require('../services/redis.user.cache.service');

/**
 * Demo User Signup with transaction handling and deadlock prevention
 */
async function signup(req, res) {
  const operationId = `demo_signup_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

  try {
    // Validate request
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return ErrorResponse.validationError(req, res, errors.array(), 'demo user signup');
    }

    const {
      name, phone_number, email, password, city, state, country, pincode,
      security_question, security_answer, is_active,
      ...optionalFields
    } = req.body;

    // Generate idempotency key
    const idempotencyKey = IdempotencyService.generateKey(req, 'demo_signup');
    const { isExisting, record } = await IdempotencyService.checkIdempotency(idempotencyKey);

    if (isExisting) {
      if (record.status === 'completed') {
        logger.info('Returning cached response for duplicate demo signup', {
          idempotencyKey,
          operationId
        });
        return res.status(201).json(record.response);
      } else if (record.status === 'processing') {
        return res.status(409).json({
          success: false,
          message: 'Request is already being processed'
        });
      }
    }

    logger.transactionStart('demo_user_signup', {
      operationId,
      email,
      phone_number
    });

    // Execute signup within transaction
    const result = await TransactionService.executeWithRetry(async (transaction) => {
      // Check uniqueness within transaction
      const existing = await DemoUser.findOne({
        where: { [Op.or]: [{ email }, { phone_number }] },
        transaction
      });

      if (existing) {
        throw new Error('Email or phone number already exists');
      }

      // Generate unique account number
      let account_number;
      let unique = false;
      let attempts = 0;
      const maxAttempts = 10;

      while (!unique && attempts < maxAttempts) {
        account_number = generateAccountNumber('DEMO');
        const exists = await DemoUser.findOne({
          where: { account_number },
          transaction
        });
        if (!exists) {
          unique = true;
        }
        attempts++;
      }

      if (!unique) {
        throw new Error('Unable to generate unique account number after maximum attempts');
      }

      // Hash password
      const hashedPassword = await hashPassword(password);

      // Lookup country_id from countries table (case-insensitive)
      const Country = require('../models/country.model');
      let country_id = null;
      if (country) {
        const Sequelize = require('sequelize');
        const countryRecord = await Country.findOne({
          where: Sequelize.where(
            Sequelize.fn('LOWER', Sequelize.col('name')),
            country.toLowerCase()
          ),
          transaction
        });
        if (countryRecord) {
          country_id = countryRecord.id;
        }
      }
      // Create user with default values
      const user = await DemoUser.create({
        name,
        phone_number,
        email,
        password: hashedPassword,
        city,
        state,
        country, // keep string
        country_id, // map to id
        pincode,
        security_question,
        security_answer,
        is_active,
        account_number,
        user_type: 'demo',
        // Set default values for demo users
        wallet_balance: 10000,
        leverage: 100,
        ...optionalFields,
        // Force group to 'Group B' for all demo users (override any request value)
        group: 'Group B'
      }, { transaction });

      logger.financial('demo_user_created', {
        operationId,
        userId: user.id,
        account_number: user.account_number,
        email: user.email
      });

      // Add user to Redis cache after successful creation
      try {
        const userData = {
          id: user.id,
          user_type: 'demo',
          email: user.email,
          wallet_balance: parseFloat(user.wallet_balance) || 0,
          leverage: user.leverage || 0,
          margin: parseFloat(user.margin) || 0,
          account_number: user.account_number,
          group: user.group,
          status: user.status,
          is_active: user.is_active,
          country_id: user.country_id
        };
        await redisUserCache.updateUser('demo', user.id, userData);
        logger.info(`Added new demo user ${user.id} to Redis cache`);
      } catch (cacheError) {
        logger.error('Failed to add new demo user to Redis cache:', cacheError);
        // Don't fail the signup if cache update fails
      }

      return {
        success: true,
        message: 'Demo user signup successful',
        user: {
          id: user.id,
          account_number: user.account_number,
          email: user.email,
          name: user.name,
          is_active: user.is_active
        }
      };
    });

    // Mark idempotency as completed
    await IdempotencyService.markCompleted(idempotencyKey, result);

    logger.transactionSuccess('demo_user_signup', {
      operationId,
      userId: result.user.id
    });

    return res.status(201).json(result);

  } catch (error) {
    // Mark idempotency as failed if we have the key
    try {
      const idempotencyKey = IdempotencyService.generateKey(req, 'demo_signup');
      await IdempotencyService.markFailed(idempotencyKey, error);
    } catch (idempotencyError) {
      logger.error('Failed to mark idempotency as failed', { 
        error: idempotencyError.message 
      });
    }

    // Handle specific error types with clear messages for user action
    if (error.message === 'Email or phone number already exists') {
      return ErrorResponse.duplicateError(req, res, 'Email or phone number already exists', 'demo user signup');
    }

    if (error.message.includes('Unable to generate unique')) {
      return ErrorResponse.serviceUnavailableError(req, res, 'Account generation service');
    }

    // Handle all other errors with generic response and detailed logging
    return ErrorResponse.serverError(req, res, error, 'demo user signup');
  }
}

async function login(req, res) {
  const { email, password } = req.body;
  const ip = req.ip;
  const { checkAndIncrementRateLimit, resetRateLimit, storeSession } = require('../utils/redisSession.util');

  try {
    // Rate limiting
    const isRateLimited = await checkAndIncrementRateLimit({ email, ip, userType: 'demo' });
    if (isRateLimited) {
      return res.status(429).json({ success: false, message: 'Too many login attempts. Please try again later.' });
    }

    const user = await DemoUser.findOne({ where: { email } });
    if (!user) {
      return res.status(401).json({ success: false, message: 'Invalid credentials' });
    }

    // Check if user account is active
    if (!user.is_active) {
      return res.status(401).json({ success: false, message: 'User account is inactive' });
    }

    const valid = await comparePassword(password, user.password);
    if (!valid) {
      return res.status(401).json({ success: false, message: 'Invalid credentials' });
    }

    // Reset rate limit on successful login
    await resetRateLimit({ email, ip, userType: 'demo' });

    const JWT_SECRET = process.env.JWT_SECRET || 'your_jwt_secret';
    const { v4: uuidv4 } = require('uuid');
    const sessionId = uuidv4();

    const jwtPayload = {
      sub: user.id,
      user_id: user.id,
      user_type: user.user_type,
      account_number: user.account_number,
      group: user.group,
      status: user.status,
      is_active: user.is_active,
      account_type: 'demo',
      role: 'trader',
      session_id: sessionId
    };

    const token = jwt.sign(jwtPayload, JWT_SECRET, { expiresIn: '7d', jwtid: sessionId });
    const refreshToken = jwt.sign(
      { userId: user.id, sessionId },
      JWT_SECRET + '_REFRESH',
      { expiresIn: '7d' }
    );

    // Store session in Redis with refresh token (enforces 3 session limit)
    const sessionResult = await storeSession(
      user.id,
      sessionId,
      {
        ...jwtPayload,
        jwt: token,
        refresh_token: refreshToken
      },
      'demo',
      refreshToken
    );

    // Log if any sessions were revoked due to limit
    if (sessionResult.revokedSessions && sessionResult.revokedSessions.length > 0) {
      logger.info('Revoked old sessions due to concurrent session limit', {
        userId: user.id,
        userType: 'demo',
        revokedSessions: sessionResult.revokedSessions,
        newSessionId: sessionId
      });
    }

    // Log successful login for demo users
    logDemoUserLogin({
      email: user.email,
      account_number: user.account_number,
      ip: req.ip,
      userAgent: req.get('User-Agent'),
      timestamp: new Date().toISOString()
    });


    return res.status(200).json({
      success: true,
      message: 'Login successful',
      access_token: token,
      refresh_token: refreshToken,
      expires_in: 604800, // 7 days in seconds
      token_type: 'Bearer',
      session_id: sessionId
    });
  } catch (err) {
    logger.error('Demo user login failed', { error: err.message });
    return res.status(500).json({ success: false, message: 'Internal server error' });
  }
}

async function refreshToken(req, res) {
  const { refresh_token: refreshToken } = req.body;
  const { validateRefreshToken, deleteRefreshToken, storeSession } = require('../utils/redisSession.util');
  
  if (!refreshToken) {
    return res.status(400).json({ 
      success: false, 
      message: 'Refresh token is required' 
    });
  }

  try {
    const JWT_SECRET = process.env.JWT_SECRET || 'your_jwt_secret';
    const decoded = jwt.verify(refreshToken, JWT_SECRET + '_REFRESH');
    
    const tokenData = await validateRefreshToken(refreshToken);
    if (!tokenData || tokenData.userId !== decoded.userId || tokenData.sessionId !== decoded.sessionId || tokenData.userType !== 'demo') {
      await deleteRefreshToken(refreshToken);
      return res.status(401).json({ 
        success: false, 
        message: 'Invalid or expired refresh token'
      });
    }

    const user = await DemoUser.findByPk(decoded.userId);
    if (!user) {
      await deleteRefreshToken(refreshToken);
      return res.status(404).json({ 
        success: false, 
        message: 'User not found' 
      });
    }

    const sessionId = tokenData.sessionId;
    const jwtPayload = {
      sub: user.id,
      user_id: user.id,
      user_type: user.user_type,
      account_number: user.account_number,
      group: user.group,
      status: user.status,
      is_active: user.is_active,
      account_type: 'demo',
      role: 'trader',
      session_id: sessionId
    };
    
    const newAccessToken = jwt.sign(jwtPayload, JWT_SECRET, { 
      expiresIn: '7d', 
      jwtid: sessionId 
    });

    const newRefreshToken = jwt.sign(
      { userId: user.id, sessionId },
      JWT_SECRET + '_REFRESH',
      { expiresIn: '7d' }
    );

    await storeSession(
      user.id,
      sessionId,
      {
        ...jwtPayload,
        jwt: newAccessToken,
        refresh_token: newRefreshToken
      },
      'demo',
      newRefreshToken
    );

    await deleteRefreshToken(refreshToken);

    return res.status(200).json({
      success: true,
      message: 'Token refreshed successfully',
      access_token: newAccessToken,
      refresh_token: newRefreshToken,
      expires_in: 604800, // 7 days in seconds
      token_type: 'Bearer',
      session_id: sessionId
    });
  } catch (error) {
    if (error.name === 'TokenExpiredError') {
      await deleteRefreshToken(refreshToken);
      return res.status(401).json({ 
        success: false, 
        message: 'Refresh token has expired' 
      });
    }
    logger.error('Failed to refresh demo user token', { error: error.message });
    return res.status(500).json({ 
      success: false, 
      message: 'Failed to refresh token' 
    });
  }
}

async function logout(req, res) {
  const { userId, sessionId } = req.user;
  const { refresh_token: refreshToken } = req.body;

  try {
    const { deleteSession } = require('../utils/redisSession.util');
    await deleteSession(userId, sessionId, 'demo', refreshToken);

    return res.status(200).json({ 
      success: true, 
      message: 'Logout successful' 
    });
  } catch (error) {
    logger.error('Demo user logout failed', { error: error.message });
    return res.status(500).json({ 
      success: false, 
      message: 'Failed to logout' 
    });
  }
}

/**
 * Get authenticated demo user information
 * GET /api/demo-users/me
 */
async function getUserInfo(req, res) {
  try {
    const userId = req.user.sub || req.user.user_id || req.user.id;

    const user = await DemoUser.findByPk(userId, {
      attributes: [
        'id', 'name', 'email', 'phone_number', 'user_type',
        'wallet_balance', 'leverage', 'margin', 'net_profit',
        'account_number', 'group', 'city', 'state', 'pincode',
        'country', 'created_at'
      ]
    });

    if (!user) {
      // Error response remains unchanged for clarity
      return res.status(404).json({
        success: false,
        message: 'User not found'
      });
    }

    // Construct the plain user object from the database result
    const userInfo = {
      id: user.id,
      name: user.name,
      email: user.email,
      phone_number: user.phone_number,
      user_type: user.user_type,
      wallet_balance: parseFloat(user.wallet_balance) || 0,
      leverage: user.leverage,
      margin: parseFloat(user.margin) || 0,
      net_profit: parseFloat(user.net_profit) || 0,
      account_number: user.account_number,
      group: user.group,
      city: user.city,
      state: user.state,
      pincode: user.pincode,
      country: user.country,
      created_at: user.created_at
    };

    // Return the user object directly as the JSON response
    return res.status(200).json(userInfo);

  } catch (error) {
    logger.error('Failed to get demo user info', {
      error: error.message,
      userId: req.user.sub || req.user.user_id || req.user.id
    });
    // Error response remains unchanged
    return res.status(500).json({
      success: false,
      message: 'Internal server error'
    });
  }
}

module.exports = { signup, login, refreshToken, logout, getUserInfo };

