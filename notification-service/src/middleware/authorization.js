const createApiError = require('../utils/ApiError');

const authorizeRoles = (allowedRoles) => {
  return (req, res, next) => {
    try {
      if (!req.user) {
        throw createApiError(401, 'Authentication required');
      }

      const role = String(req.user.role || '').trim().toUpperCase();
      if (!allowedRoles.includes(role)) {
        throw createApiError(403, 'Access denied. Insufficient permissions.');
      }

      next();
    } catch (error) {
      return res.status(error.statusCode || 403).json({
        success: false,
        message: error.message,
      });
    }
  };
};

module.exports = { authorizeRoles };
