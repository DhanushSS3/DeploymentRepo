const express = require('express');
const { signup, regenerateViewPassword, getUserInfo } = require('../controllers/liveUser.controller');
const { body, param } = require('express-validator');
const upload = require('../middlewares/upload.middleware');
const { handleValidationErrors } = require('../middlewares/error.middleware');
const { authenticateJWT } = require('../middlewares/auth.middleware');

const router = express.Router();

/**
 * @swagger
 * /api/live-users/signup:
 *   post:
 *     summary: Register a new live user
 *     tags: [Live Users]
 *     requestBody:
 *       required: true
 *       content:
 *         multipart/form-data:
 *           schema:
 *             $ref: '#/components/schemas/LiveUserSignup'
 *     responses:
 *       201:
 *         description: User registered successfully
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/SuccessResponse'
 *       400:
 *         description: Validation error
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Error'
 *       409:
 *         description: Email or phone number already exists
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Error'
 *       500:
 *         description: Internal server error
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Error'
 */
router.post('/signup',
  upload.fields([
    { name: 'address_proof_image', maxCount: 1 },
    { name: 'id_proof_image', maxCount: 1 }
  ]),
  [
    body('name').notEmpty(),
    body('phone_number').notEmpty(),
    body('email').isEmail(),
    body('password').isLength({ min: 6 }),
    body('city').notEmpty(),
    body('state').notEmpty(),
    body('country').notEmpty(),
    body('pincode').notEmpty(),
    body('group').notEmpty(),
    body('bank_ifsc_code').notEmpty(),
    body('bank_account_number').notEmpty(),
    body('bank_holder_name').notEmpty(),
    body('bank_branch_name').notEmpty(),
    body('security_question').notEmpty(),
    body('security_answer').notEmpty(),
    body('address_proof').notEmpty(),
    body('address_proof_image').optional(),
    body('id_proof').notEmpty(),
    body('id_proof_image').optional(),
    body('is_self_trading').notEmpty().isInt({ min: 0, max: 1 }).withMessage('is_self_trading must be 0 or 1'),
    body('is_active').notEmpty().isInt({ min: 0, max: 1 }).withMessage('is_active must be 0 or 1'),
    body('book').optional().isLength({ max: 5 }).withMessage('book must be maximum 5 characters').isAlphanumeric().withMessage('book must contain only alphanumeric characters'),
  ],
  signup
);

/**
 * @swagger
 * /api/live-users/login:
 *   post:
 *     summary: Login for live user
 *     tags: [Live Users]
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               email:
 *                 type: string
 *               password:
 *                 type: string
 *     responses:
 *       200:
 *         description: Login successful
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 success:
 *                   type: boolean
 *                 message:
 *                   type: string
 *                 token:
 *                   type: string
 *       401:
 *         description: Invalid credentials
 *       500:
 *         description: Internal server error
 */
router.post('/login',
  [
    body('email').isEmail(),
    body('password').isLength({ min: 6 })
  ],
  handleValidationErrors,
  require('../controllers/liveUser.controller').login
);

/**
 * @swagger
 * /api/live-users/refresh-token:
 *   post:
 *     summary: Refresh access token using a refresh token
 *     tags: [Live Users]
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required:
 *               - refresh_token
 *             properties:
 *               refresh_token:
 *                 type: string
 *                 description: Valid refresh token received during login
 *     responses:
 *       200:
 *         description: New access and refresh tokens
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 success:
 *                   type: boolean
 *                 message:
 *                   type: string
 *                 data:
 *                   type: object
 *                   properties:
 *                     access_token:
 *                       type: string
 *                     refresh_token:
 *                       type: string
 *                     expires_in:
 *                       type: number
 *                     token_type:
 *                       type: string
 *                     session_id:
 *                       type: string
 *       400:
 *         description: Missing refresh token
 *       401:
 *         description: Invalid or expired refresh token
 *       500:
 *         description: Internal server error
 */
router.post('/refresh-token',
  [
    body('refresh_token').notEmpty().withMessage('Refresh token is required')
  ],
  handleValidationErrors,
  require('../controllers/liveUser.controller').refreshToken
);

/**
 * @swagger
 * /api/live-users/logout:
 *   post:
 *     summary: Logout for live user
 *     tags: [Live Users]
 *     security:
 *       - bearerAuth: []
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               refresh_token:
 *                 type: string
 *                 description: The refresh token to invalidate.
 *     responses:
 *       200:
 *         description: Logout successful
 *       401:
 *         description: Unauthorized
 *       500:
 *         description: Internal server error
 */
router.post('/logout', authenticateJWT, require('../controllers/liveUser.controller').logout);

/**
 * @swagger
 * /api/live-users/{id}/regenerate-view-password:
 *   post:
 *     summary: Regenerate view password for a live user
 *     tags: [Live Users]
 *     security:
 *       - bearerAuth: []
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: integer
 *         description: Live user ID
 *     responses:
 *       200:
 *         description: View password regenerated successfully
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 success:
 *                   type: boolean
 *                 message:
 *                   type: string
 *                 data:
 *                   type: object
 *                   properties:
 *                     view_password:
 *                       type: string
 *                       description: New plain text view password (returned only once)
 *       401:
 *         description: Unauthorized
 *       404:
 *         description: User not found
 *       500:
 *         description: Internal server error
 */
router.post('/:id/regenerate-view-password',
  authenticateJWT,
  [
    param('id').isInt({ min: 1 }).withMessage('User ID must be a positive integer')
  ],
  handleValidationErrors,
  regenerateViewPassword
);

/**
 * @swagger
 * /api/live-users/me:
 *   get:
 *     summary: Get authenticated live user information
 *     tags: [Live Users]
 *     security:
 *       - bearerAuth: []
 *     description: Retrieve current user's profile information using JWT authentication
 *     responses:
 *       200:
 *         description: User information retrieved successfully
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 success:
 *                   type: boolean
 *                   example: true
 *                 message:
 *                   type: string
 *                   example: "User information retrieved successfully"
 *                 data:
 *                   type: object
 *                   properties:
 *                     user:
 *                       type: object
 *                       properties:
 *                         id:
 *                           type: integer
 *                         name:
 *                           type: string
 *                         email:
 *                           type: string
 *                         phone_number:
 *                           type: string
 *                         user_type:
 *                           type: string
 *                         wallet_balance:
 *                           type: number
 *                         leverage:
 *                           type: integer
 *                         margin:
 *                           type: number
 *                         net_profit:
 *                           type: number
 *                         account_number:
 *                           type: string
 *                         group:
 *                           type: string
 *                         city:
 *                           type: string
 *                         state:
 *                           type: string
 *                         pincode:
 *                           type: string
 *                         country:
 *                           type: string
 *                         bank_ifsc_code:
 *                           type: string
 *                         bank_holder_name:
 *                           type: string
 *                         bank_account_number:
 *                           type: string
 *                         referral_code:
 *                           type: string
 *                         is_self_trading:
 *                           type: integer
 *                         created_at:
 *                           type: string
 *                           format: date-time
 *       401:
 *         description: Unauthorized
 *       404:
 *         description: User not found
 *       500:
 *         description: Internal server error
 */
router.get('/me', authenticateJWT, getUserInfo);

module.exports = router;