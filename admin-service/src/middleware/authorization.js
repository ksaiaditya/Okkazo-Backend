const logger = require('../utils/logger');
const { createApiError } = require('../utils/ApiError');

const authorizeRoles = (allowedRoles) => {
  return (req, res, next) => {
    try {
      if (!req.user) {
        throw createApiError(401, 'Authentication required');
      }

      const userRole = req.user.role;

      if (!allowedRoles.includes(userRole)) {
        logger.warn('Authorization failed', {
          authId: req.user.authId,
          userRole,
          allowedRoles,
        });

        throw createApiError(403, 'Access denied. Insufficient permissions.');
      }

      logger.debug('User authorized', {
        authId: req.user.authId,
        role: userRole,
      });

      next();
    } catch (error) {
      return res.status(error.statusCode || 403).json({
        success: false,
        message: error.message,
      });
    }
  };
};

const isAdmin = (req, res, next) => {
  return authorizeRoles(['ADMIN'])(req, res, next);
};

const isAdminOrManager = (req, res, next) => {
  return authorizeRoles(['ADMIN', 'MANAGER'])(req, res, next);
};

module.exports = {
  authorizeRoles,
  isAdmin,
  isAdminOrManager,
};
