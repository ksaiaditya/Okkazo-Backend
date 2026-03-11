const adminEventProducer = require('../kafka/adminEventProducer');
const logger = require('../utils/logger');
const createApiError = require('../utils/ApiError');
const axios = require('axios');

const VALID_DEPARTMENTS = ['Public Event', 'Private Event', 'Core Operation'];
const VALID_ROLES = ['Senior Event Manager', 'Junior Manager', 'Event Coordinator'];
const DEPARTMENT_ROLE_MAP = {
  'Public Event': ['Senior Event Manager', 'Junior Manager'],
  'Private Event': ['Senior Event Manager', 'Junior Manager'],
  'Core Operation': ['Event Coordinator'],
};

const createManager = async (managerData, createdByAuthId) => {
  try {
    if (!managerData.name || !managerData.email || !managerData.department || !managerData.assignedRole) {
      throw createApiError(400, 'Name, email, department, and assigned role are required');
    }

    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(managerData.email)) {
      throw createApiError(400, 'Invalid email format');
    }

    const department = managerData.department.trim();
    const assignedRole = managerData.assignedRole.trim();

    if (!VALID_DEPARTMENTS.includes(department)) {
      throw createApiError(400, `Invalid department. Must be one of: ${VALID_DEPARTMENTS.join(', ')}`);
    }

    if (!VALID_ROLES.includes(assignedRole)) {
      throw createApiError(400, `Invalid assigned role. Must be one of: ${VALID_ROLES.join(', ')}`);
    }

    const allowedRoles = DEPARTMENT_ROLE_MAP[department];
    if (!allowedRoles.includes(assignedRole)) {
      throw createApiError(400, `Role '${assignedRole}' is not valid for department '${department}'. Allowed: ${allowedRoles.join(', ')}`);
    }

    // Check if email already exists in auth-service
    const authServiceUrl = process.env.AUTH_SERVICE_URL || 'http://auth-service:8081';
    try {
      const emailCheckResponse = await axios.get(`${authServiceUrl}/check-email`, {
        params: { email: managerData.email.trim().toLowerCase() },
        timeout: 5000
      });

      if (emailCheckResponse.data.exists) {
        throw createApiError(400, `Email already exists with role: ${emailCheckResponse.data.role}`);
      }
    } catch (error) {
      // If it's our API error (email exists), rethrow it
      if (error.statusCode === 400) {
        throw error;
      }
      // Log other errors but don't block manager creation
      logger.error('Error checking email existence:', error.message);
      logger.warn('Proceeding with manager creation despite email check failure');
    }

    const eventData = {
      name: managerData.name.trim(),
      email: managerData.email.trim().toLowerCase(),
      department,
      assignedRole,
      createdBy: createdByAuthId,
    };

    await adminEventProducer.publishManagerCreated(eventData);

    logger.info('Manager creation initiated', {
      email: eventData.email,
      department: eventData.department,
      createdBy: createdByAuthId,
    });

    return {
      email: eventData.email,
      name: eventData.name,
      department: eventData.department,
      assignedRole: eventData.assignedRole,
    };
  } catch (error) {
    if (error.statusCode) throw error;
    logger.error('Error creating manager:', error);
    throw createApiError(500, 'Failed to create manager');
  }
};

module.exports = {
  createManager,
};
