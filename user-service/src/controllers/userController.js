const userService = require('../services/userService');
const logger = require('../utils/logger');

/**
 * Get current user profile
 * GET /api/users/me
 */
const getCurrentUser = async (req, res) => {
  try {
    if (!req.user || !req.user.authId) {
      return res.status(401).json({
        success: false,
        message: 'User authentication information missing',
      });
    }

    const authId = req.user.authId;
    const user = await userService.getUserByAuthId(authId);

    res.status(200).json({
      success: true,
      data: user,
    });
  } catch (error) {
    logger.error('Error in getCurrentUser:', error);
    res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

/**
 * Get user by ID
 * GET /api/users/:id
 */
const getUserById = async (req, res) => {
  try {
    const userId = req.params.id;
    
    if (!userId || userId.trim() === '') {
      return res.status(400).json({
        success: false,
        message: 'User ID is required',
      });
    }

    const user = await userService.getUserById(userId);

    res.status(200).json({
      success: true,
      data: user,
    });
  } catch (error) {
    logger.error('Error in getUserById:', error);
    res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

/**
 * Get user by authId
 * GET /api/users/auth/:authId
 */
const getUserByAuthId = async (req, res) => {
  try {
    const authId = req.params.authId;
    
    if (!authId || authId.trim() === '') {
      return res.status(400).json({
        success: false,
        message: 'Auth ID is required',
      });
    }

    const user = await userService.getUserByAuthId(authId);

    res.status(200).json({
      success: true,
      data: user,
    });
  } catch (error) {
    logger.error('Error in getUserByAuthId:', error);
    res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

/**
 * Get user by email
 * GET /api/users/email/:email
 */
const getUserByEmail = async (req, res) => {
  try {
    const email = req.params.email;
    
    if (!email || email.trim() === '') {
      return res.status(400).json({
        success: false,
        message: 'Email is required',
      });
    }

    // Basic email format validation
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(email)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid email format',
      });
    }

    const user = await userService.getUserByEmail(email);

    res.status(200).json({
      success: true,
      data: user,
    });
  } catch (error) {
    logger.error('Error in getUserByEmail:', error);
    res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

/**
 * Update current user profile
 * PUT /api/users/me
 */
const updateCurrentUser = async (req, res) => {
  try {
    if (!req.user || !req.user.authId) {
      return res.status(401).json({
        success: false,
        message: 'User authentication information missing',
      });
    }

    if (!req.body || Object.keys(req.body).length === 0) {
      return res.status(400).json({
        success: false,
        message: 'Update data is required',
      });
    }

    const authId = req.user.authId;
    const user = await userService.getUserByAuthId(authId);

    // Email is immutable for self-service profile updates.
    // If the frontend sends email (even from a disabled input), allow it only if unchanged.
    const updateData = { ...req.body };
    if (Object.prototype.hasOwnProperty.call(updateData, 'email')) {
      const incomingEmail = updateData.email;
      if (incomingEmail && String(incomingEmail).toLowerCase() !== String(user.email).toLowerCase()) {
        return res.status(400).json({
          success: false,
          message: 'Email cannot be updated',
        });
      }
      delete updateData.email;
    }

    const updatedUser = await userService.updateUser(user.id, updateData);

    res.status(200).json({
      success: true,
      message: 'Profile updated successfully',
      data: updatedUser,
    });
  } catch (error) {
    logger.error('Error in updateCurrentUser:', error);
    res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

/**
 * Update user by ID (Admin only)
 * PUT /api/users/:id
 */
const updateUser = async (req, res) => {
  try {
    const userId = req.params.id;
    
    if (!userId || userId.trim() === '') {
      return res.status(400).json({
        success: false,
        message: 'User ID is required',
      });
    }

    if (!req.body || Object.keys(req.body).length === 0) {
      return res.status(400).json({
        success: false,
        message: 'Update data is required',
      });
    }

    const updatedUser = await userService.updateUser(userId, req.body);

    res.status(200).json({
      success: true,
      message: 'User updated successfully',
      data: updatedUser,
    });
  } catch (error) {
    logger.error('Error in updateUser:', error);
    res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

/**
 * Delete user (soft delete)
 * DELETE /api/users/:id
 */
const deleteUser = async (req, res) => {
  try {
    const userId = req.params.id;
    
    if (!userId || userId.trim() === '') {
      return res.status(400).json({
        success: false,
        message: 'User ID is required',
      });
    }

    const result = await userService.deleteUser(userId);

    res.status(200).json({
      success: true,
      message: result.message,
    });
  } catch (error) {
    logger.error('Error in deleteUser:', error);
    res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

/**
 * Get all users with pagination
 * GET /api/users
 */
const getAllUsers = async (req, res) => {
  try {
    let { page = 1, limit = 10, role, profileIsComplete, search } = req.query;

    // Validate and sanitize pagination parameters
    page = parseInt(page, 10);
    limit = parseInt(limit, 10);

    if (isNaN(page) || page < 1) {
      page = 1;
    }

    if (isNaN(limit) || limit < 1) {
      limit = 10;
    }

    if (limit > 100) {
      limit = 100; // Cap maximum limit
    }

    const filters = {};
    if (role) {
      // Validate role
      const validRoles = ['USER', 'VENDOR', 'ADMIN', 'MANAGER'];
      if (validRoles.includes(role.toUpperCase())) {
        filters.role = role;
      }
    }
    if (profileIsComplete) filters.profileIsComplete = profileIsComplete;
    if (search && search.trim() !== '') {
      filters.search = search.trim();
    }

    const result = await userService.getAllUsers(filters, page, limit);

    res.status(200).json({
      success: true,
      data: result.users,
      pagination: result.pagination,
    });
  } catch (error) {
    logger.error('Error in getAllUsers:', error);
    res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

/**
 * Get platform users with account status from auth-service
 * GET /api/users/platform-users
 */
const getPlatformUsers = async (req, res) => {
  try {
    let { page = 1, limit = 10, role, search } = req.query;

    page = parseInt(page, 10);
    limit = parseInt(limit, 10);

    if (isNaN(page) || page < 1) {
      page = 1;
    }

    if (isNaN(limit) || limit < 1) {
      limit = 10;
    }

    if (limit > 100) {
      limit = 100;
    }

    const filters = {};
    if (role) {
      const validRoles = ['USER', 'VENDOR', 'ADMIN', 'MANAGER'];
      if (validRoles.includes(role.toUpperCase())) {
        filters.role = role;
      }
    }

    if (search && search.trim() !== '') {
      filters.search = search.trim();
    }

    const result = await userService.getPlatformUsers(filters, page, limit);

    res.status(200).json({
      success: true,
      data: result.users,
      pagination: result.pagination,
    });
  } catch (error) {
    logger.error('Error in getPlatformUsers:', error);
    res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

/**
 * Get user statistics
 * GET /api/users/stats
 */
const getUserStats = async (req, res) => {
  try {
    const stats = await userService.getUserStats();

    res.status(200).json({
      success: true,
      data: stats,
    });
  } catch (error) {
    logger.error('Error in getUserStats:', error);
    res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

/**
 * Get team access data (Admin only)
 * GET /api/users/team-access
 */
const getTeamAccess = async (req, res) => {
  try {
    let { page = 1, limit = 10, search = '' } = req.query;

    page = parseInt(page, 10);
    limit = parseInt(limit, 10);

    if (isNaN(page) || page < 1) page = 1;
    if (isNaN(limit) || limit < 1) limit = 10;
    if (limit > 50) limit = 50;

    const result = await userService.getTeamAccessData({
      search,
      page,
      limit,
    });

    res.status(200).json({
      success: true,
      data: result,
    });
  } catch (error) {
    logger.error('Error in getTeamAccess:', error);
    res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

/**
 * Block team member (Admin only)
 * PATCH /api/users/team-access/:authId/block
 */
const blockTeamMember = async (req, res) => {
  try {
    const { authId } = req.params;

    if (!req.user?.authId) {
      return res.status(401).json({
        success: false,
        message: 'User authentication information missing',
      });
    }

    const user = await userService.blockTeamMember({
      authId,
      requestedByAuthId: req.user.authId,
    });

    res.status(200).json({
      success: true,
      message: 'Team member blocked successfully',
      data: {
        authId: user.authId,
        email: user.email,
        role: user.role,
        status: user.isActive ? 'ACTIVE' : 'BLOCKED',
      },
    });
  } catch (error) {
    logger.error('Error in blockTeamMember:', error);
    res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

/**
 * Unblock team member (Admin only)
 * PATCH /api/users/team-access/:authId/unblock
 */
const unblockTeamMember = async (req, res) => {
  try {
    const { authId } = req.params;

    if (!req.user?.authId) {
      return res.status(401).json({
        success: false,
        message: 'User authentication information missing',
      });
    }

    const user = await userService.unblockTeamMember({
      authId,
      requestedByAuthId: req.user.authId,
    });

    res.status(200).json({
      success: true,
      message: 'Team member unblocked successfully',
      data: {
        authId: user.authId,
        email: user.email,
        role: user.role,
        status: user.isActive ? 'ACTIVE' : 'BLOCKED',
      },
    });
  } catch (error) {
    logger.error('Error in unblockTeamMember:', error);
    res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

/**
 * Update last login
 * POST /api/users/login
 */
const updateLastLogin = async (req, res) => {
  try {
    if (!req.user || !req.user.authId) {
      return res.status(401).json({
        success: false,
        message: 'User authentication information missing',
      });
    }

    const authId = req.user.authId;
    const user = await userService.updateLastLogin(authId);

    res.status(200).json({
      success: true,
      message: 'Last login updated',
      data: user,
    });
  } catch (error) {
    logger.error('Error in updateLastLogin:', error);
    res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

/**
 * Health check
 * GET /api/users/health
 */
const healthCheck = (req, res) => {
  res.status(200).json({
    success: true,
    message: 'User service is running',
    timestamp: new Date().toISOString(),
  });
};

module.exports = {
  getCurrentUser,
  getUserById,
  getUserByAuthId,
  getUserByEmail,
  updateCurrentUser,
  updateUser,
  deleteUser,
  getAllUsers,
  getPlatformUsers,
  getTeamAccess,
  blockTeamMember,
  unblockTeamMember,
  getUserStats,
  updateLastLogin,
  healthCheck,
};
