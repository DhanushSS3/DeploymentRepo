const LiveUser = require('../models/liveUser.model');
const { generateAccountNumber } = require('../services/accountNumber.service');
const { hashPassword, generateViewPassword, hashViewPassword, compareViewPassword } = require('../services/password.service');
const LiveUserAuthService = require('../services/liveUser.auth.service');
const { generateReferralCode } = require('../services/referralCode.service');
const { validationResult } = require('express-validator');
const { Op } = require('sequelize');
const TransactionService = require('../services/transaction.service');
const logger = require('../services/logger.service');
const ErrorResponse = require('../utils/errorResponse.util');
const { IdempotencyService } = require('../services/idempotency.service');
const jwt = require('jsonwebtoken');
const { comparePassword } = require('../services/password.service');
const redisUserCache = require('../services/redis.user.cache.service');

/**
 * Live User Signup with transaction handling and deadlock prevention
 */
async function signup(req, res) {
  const operationId = `live_signup_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  
  try {
    // Validate request
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return ErrorResponse.validationError(req, res, errors.array(), 'live user signup');
    }

    const {
      name, phone_number, email, password, city, state, country, pincode, group,
      bank_ifsc_code, bank_account_number, bank_holder_name, bank_branch_name,
      security_question, security_answer, address_proof, is_self_trading, 
      id_proof, address_proof_image, id_proof_image, book,
      ...optionalFields
    } = req.body;

    // Handle field name variations (frontend may send camelCase)
    const is_active = req.body.is_active || req.body.isActive;

    // Generate idempotency key
    const idempotencyKey = IdempotencyService.generateKey(req, 'live_signup');
    const { isExisting, record } = await IdempotencyService.checkIdempotency(idempotencyKey);

    if (isExisting) {
      if (record.status === 'completed') {
        logger.info('Returning cached response for duplicate live signup', { 
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

    logger.transactionStart('live_user_signup', { 
      operationId, 
      email, 
      phone_number 
    });

    // Execute signup within transaction
    const result = await TransactionService.executeWithRetry(async (transaction) => {
      // Check uniqueness within transaction
      const existing = await LiveUser.findOne({
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
        account_number = generateAccountNumber('LIVE');
        const exists = await LiveUser.findOne({ 
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

      // Generate and hash view password
      const plainViewPassword = generateViewPassword(14);
      const hashedViewPassword = await hashViewPassword(plainViewPassword);

      // Handle file uploads
      let address_proof_image = req.files && req.files.address_proof_image 
        ? `/uploads/${req.files.address_proof_image[0].filename}` 
        : null;
      let id_proof_image = req.files && req.files.id_proof_image 
        ? `/uploads/${req.files.id_proof_image[0].filename}` 
        : null;

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
      const user = await LiveUser.create({
        name, 
        phone_number, 
        email, 
        password: hashedPassword, 
        city, 
        state, 
        country, // keep string
        country_id, // map to id
        pincode, 
        group,
        bank_ifsc_code: bank_ifsc_code || null, 
        bank_account_number: bank_account_number || null, 
        bank_holder_name: bank_holder_name || null, 
        bank_branch_name: bank_branch_name || null,
        security_question, 
        security_answer, 
        address_proof, 
        address_proof_image,
        id_proof,
        id_proof_image, 
        is_self_trading, 
        is_active,
        account_number,
        user_type: 'live',
        view_password: hashedViewPassword,
        book: book || null,
        // Set default values for live users
        sending_orders: 'barclays',
        leverage: 100,
        ...optionalFields
      }, { transaction });

      // Generate unique referral code
      let referral_code;
      let referralUnique = false;
      let referralAttempts = 0;

      while (!referralUnique && referralAttempts < maxAttempts) {
        referral_code = generateReferralCode();
        const exists = await LiveUser.findOne({ 
          where: { referral_code },
          transaction
        });
        if (!exists) {
          referralUnique = true;
        }
        referralAttempts++;
      }

      if (!referralUnique) {
        throw new Error('Unable to generate unique referral code after maximum attempts');
      }

      // Update user with referral code
      await user.update({ referral_code }, { transaction });

      logger.financial('live_user_created', {
        operationId,
        userId: user.id,
        account_number: user.account_number,
        email: user.email,
        bank_account_number: user.bank_account_number,
        group: user.group
      });

      // Add user to Redis cache after successful creation
      try {
        const userData = {
          id: user.id,
          user_type: 'live',
          email: user.email,
          wallet_balance: parseFloat(user.wallet_balance) || 0,
          leverage: user.leverage || 0,
          margin: parseFloat(user.margin) || 0,
          account_number: user.account_number,
          group: user.group,
          status: user.status,
          is_active: user.is_active,
          country_id: user.country_id,
          mam_id: user.mam_id,
          mam_status: user.mam_status,
          pam_id: user.pam_id,
          pam_status: user.pam_status,
          copy_trading_wallet: parseFloat(user.copy_trading_wallet) || 0,
          copytrader_id: user.copytrader_id,
          copytrading_status: user.copytrading_status,
          copytrading_alloted_time: user.copytrading_alloted_time ? user.copytrading_alloted_time.toISOString() : null,
          sending_orders: user.sending_orders || 'rock'
        };
        await redisUserCache.updateUser('live', user.id, userData);
        logger.info(`Added new live user ${user.id} to Redis cache`);
      } catch (cacheError) {
        logger.error('Failed to add new live user to Redis cache:', cacheError);
        // Don't fail the signup if cache update fails
      }

      return {
        success: true,
        message: 'Live user signup successful',
        user: {
          id: user.id,
          account_number: user.account_number,
          email: user.email,
          referral_code: user.referral_code,
          name: user.name,
          is_active: user.is_active,
          group: user.group,
          view_password: plainViewPassword // Return plain password only once
        }
      };
    });

    // Mark idempotency as completed
    await IdempotencyService.markCompleted(idempotencyKey, result);

    logger.transactionSuccess('live_user_signup', { 
      operationId, 
      userId: result.user.id 
    });

    return res.status(201).json(result);

  } catch (error) {
    // Mark idempotency as failed if we have the key
    try {
      const idempotencyKey = IdempotencyService.generateKey(req, 'live_signup');
      await IdempotencyService.markFailed(idempotencyKey, error);
    } catch (idempotencyError) {
      logger.error('Failed to mark idempotency as failed', { 
        error: idempotencyError.message 
      });
    }

    // Handle specific error types with clear messages for user action
    if (error.message === 'Email or phone number already exists') {
      return ErrorResponse.duplicateError(req, res, 'Email or phone number already exists', 'live user signup');
    }

    if (error.message.includes('Unable to generate unique')) {
      return ErrorResponse.serviceUnavailableError(req, res, 'Account generation service');
    }

    // Handle all other errors with generic response and detailed logging
    return ErrorResponse.serverError(req, res, error, 'live user signup');
  }
}

/**
 * Live User Login - returns JWT on success
 * Supports both master password and view_password authentication
 */
async function login(req, res, next) {
  const { email, password } = req.body;
  const ip = req.ip;
  const { checkAndIncrementRateLimit, resetRateLimit, storeSession } = require('../utils/redisSession.util');
  try {
    // Rate limiting: block if too many attempts
    const isRateLimited = await checkAndIncrementRateLimit({ email, ip, userType: 'live' });
    if (isRateLimited) {
      return res.status(429).json({ success: false, message: 'Too many login attempts. Please try again later.' });
    }

    const user = await LiveUser.findOne({ where: { email } });
    if (!user) {
      return res.status(401).json({ success: false, message: 'Invalid credentials' });
    }
    
    // Check if user account is active
    if (!user.is_active) {
      return res.status(401).json({ success: false, message: 'User account is inactive' });
    }
    
    // Validate credentials using auth service
    const { isValid, loginType } = await LiveUserAuthService.validateCredentials(password, user);
    if (!isValid) {
      return res.status(401).json({ success: false, message: 'Invalid credentials' });
    }
    // Passed authentication: reset rate limit
    await resetRateLimit({ email, ip, userType: 'live' });

    const JWT_SECRET = process.env.JWT_SECRET || 'your_jwt_secret';
    const jwt = require('jsonwebtoken');
    const { v4: uuidv4 } = require('uuid');
    const sessionId = uuidv4();
    // Generate JWT payload using auth service
    const jwtPayload = LiveUserAuthService.generateJWTPayload(user, loginType, sessionId);
    // Generate access token (7 days expiry)
    const token = jwt.sign(jwtPayload, JWT_SECRET, { expiresIn: '7d', jwtid: sessionId });
    
    // Generate refresh token (7 days expiry)
    const refreshToken = jwt.sign(
      { userId: user.id, sessionId },
      JWT_SECRET + '_REFRESH', // Different secret for refresh tokens
      { expiresIn: '7d' }
    );

    // Store session in Redis with refresh token (enforces 3 session limit)
    const sessionResult = await storeSession(
      user.id, 
      sessionId, 
      {
        ...jwtPayload,
        jwt: token,
        refresh_token: refreshToken // Store refresh token in session for reference
      },
      'live',
      refreshToken // Pass refresh token to be stored separately
    );

    // Log if any sessions were revoked due to limit
    if (sessionResult.revokedSessions && sessionResult.revokedSessions.length > 0) {
      logger.info('Revoked old sessions due to concurrent session limit', {
        userId: user.id,
        userType: 'live',
        revokedSessions: sessionResult.revokedSessions,
        newSessionId: sessionId
      });
    }

    // Log successful login for live users
    const { logLiveUserLogin } = require('../services/loginLogger');
    logLiveUserLogin({
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
  } catch (error) {
    return ErrorResponse.serverError(req, res, error, 'live user login');
  }
}


/**
 * Refresh access token using a valid refresh token
 */
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
    // Verify the refresh token
    const JWT_SECRET = process.env.JWT_SECRET || 'your_jwt_secret';
    const decoded = jwt.verify(refreshToken, JWT_SECRET + '_REFRESH');
    
    // Check if the refresh token exists in Redis and is valid
    const tokenData = await validateRefreshToken(refreshToken);
    if (!tokenData || tokenData.userId !== decoded.userId || tokenData.sessionId !== decoded.sessionId) {
      // If invalid, clean up any existing refresh token
      await deleteRefreshToken(refreshToken);
      return res.status(401).json({ 
        success: false, 
        message: 'Invalid or expired refresh token' 
      });
    }

    // Get user data
    const user = await LiveUser.findByPk(decoded.userId);
    if (!user) {
      await deleteRefreshToken(refreshToken);
      return res.status(404).json({ 
        success: false, 
        message: 'User not found' 
      });
    }

    // Generate new access token
    const sessionId = tokenData.sessionId; // Keep the same session
    
    // Determine login type from stored session data (fallback to 'master' for existing sessions)
    const storedRole = tokenData.role || 'trader';
    const loginType = storedRole === 'viewer' ? 'view' : 'master';
    
    // Generate JWT payload using auth service
    const jwtPayload = LiveUserAuthService.generateJWTPayload(user, loginType, sessionId);
    
    const newAccessToken = jwt.sign(jwtPayload, JWT_SECRET, { 
      expiresIn: '7d', 
      jwtid: sessionId 
    });

    // Generate new refresh token (rotate refresh token)
    const newRefreshToken = jwt.sign(
      { userId: user.id, sessionId },
      JWT_SECRET + '_REFRESH',
      { expiresIn: '7d' }
    );

    // Update session in Redis with new access token
    await storeSession(
      user.id,
      sessionId,
      {
        ...jwtPayload,
        jwt: newAccessToken,
        refresh_token: newRefreshToken
      },
      'live',
      newRefreshToken
    );

    // Delete the old refresh token in a separate operation
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
      // Clean up expired refresh token
      await deleteRefreshToken(refreshToken);
      return ErrorResponse.authenticationError(req, res, 'Your session has expired. Please login again.');
    }
    return ErrorResponse.serverError(req, res, error, 'token refresh');
  }
}

/**
 * Secure user logout
 */
async function logout(req, res) {
  const { userId, sessionId } = req.user; // from authenticateJWT middleware
  const { refresh_token: refreshToken } = req.body;

  try {
    // Invalidate the session and refresh token in Redis
    const { deleteSession } = require('../utils/redisSession.util');
    await deleteSession(userId, sessionId, 'live', refreshToken);

    return res.status(200).json({ 
      success: true, 
      message: 'Logout successful' 
    });
  } catch (error) {
    return ErrorResponse.serverError(req, res, error, 'user logout');
  }
}

/**
 * Regenerate view password for a live user
 * POST /users/{id}/regenerate-view-password
 */
async function regenerateViewPassword(req, res) {
  const { id } = req.params;
  const operationId = `regenerate_view_password_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

  try {
    // Validate that the user exists
    const user = await LiveUser.findByPk(id);
    if (!user) {
      return res.status(404).json({
        success: false,
        message: 'User not found'
      });
    }

    // Execute regeneration within transaction
    const result = await TransactionService.executeWithRetry(async (transaction) => {
      // Generate new view password
      const plainViewPassword = generateViewPassword(14);
      const hashedViewPassword = await hashViewPassword(plainViewPassword);

      // Update user with new view password
      await user.update({ 
        view_password: hashedViewPassword 
      }, { transaction });

      logger.financial('view_password_regenerated', {
        operationId,
        userId: user.id,
        account_number: user.account_number,
        email: user.email
      });

      return {
        success: true,
        message: 'View password regenerated successfully',
        data: {
          view_password: plainViewPassword // Return plain password only once
        }
      };
    });

    logger.transactionSuccess('regenerate_view_password', { 
      operationId, 
      userId: user.id 
    });

    return res.status(200).json(result);

  } catch (error) {
    return ErrorResponse.serverError(req, res, error, 'regenerate view password');
  }
}

/**
 * Get authenticated live user information
 * GET /api/live-users/me
 */
async function getUserInfo(req, res) {
  try {
    const userId = req.user.sub || req.user.user_id || req.user.id;

    const user = await LiveUser.findByPk(userId, {
      attributes: [
        'id', 'name', 'email', 'phone_number', 'user_type',
        'wallet_balance', 'leverage', 'margin', 'net_profit',
        'account_number', 'group', 'city', 'state', 'pincode',
        'country', 'bank_ifsc_code', 'bank_holder_name',
        'bank_account_number', 'referral_code', 'is_self_trading',
        'created_at'
      ]
    });

    if (!user) {
      // Error case remains the same for clarity
      return res.status(404).json({
        success: false,
        message: 'User not found'
      });
    }

    // Construct the plain user object
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
      bank_ifsc_code: user.bank_ifsc_code,
      bank_holder_name: user.bank_holder_name,
      bank_account_number: user.bank_account_number,
      referral_code: user.referral_code,
      is_self_trading: user.is_self_trading,
      created_at: user.created_at
    };

    // Return the user object directly
    return res.status(200).json(userInfo);

  } catch (error) {
    return ErrorResponse.serverError(req, res, error, 'get user info');
  }
}

/**
 * Get user's active sessions (for debugging/admin purposes)
 * GET /api/live-users/sessions
 */
async function getUserSessions(req, res) {
  try {
    const userId = req.user.sub || req.user.user_id || req.user.id;
    const userType = req.user.account_type === 'strategy_provider' ? 'strategy_provider' : 'live';
    
    const { getUserActiveSessions } = require('../utils/redisSession.util');
    const activeSessions = await getUserActiveSessions(userId, userType);
    
    return res.status(200).json({
      success: true,
      message: 'Active sessions retrieved successfully',
      data: {
        userId,
        userType,
        activeSessions: activeSessions.map(session => ({
          sessionId: session.sessionId,
          createdAt: new Date(session.timestamp).toISOString(),
          isCurrentSession: session.sessionId === req.user.session_id
        })),
        totalSessions: activeSessions.length,
        maxAllowed: 3
      }
    });
  } catch (error) {
    logger.error('Failed to get user sessions', {
      userId: req.user?.sub || req.user?.user_id || req.user?.id,
      error: error.message
    });
    
    return res.status(500).json({
      success: false,
      message: 'Internal server error while retrieving sessions'
    });
  }
}

module.exports = { signup, login, refreshToken, logout, regenerateViewPassword, getUserInfo, getUserSessions };