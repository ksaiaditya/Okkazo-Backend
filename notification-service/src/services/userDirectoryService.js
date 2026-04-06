const axios = require('axios');
const logger = require('../utils/logger');

const userServiceUrl = process.env.USER_SERVICE_URL || 'http://user-service:8082';
const timeout = Number(process.env.UPSTREAM_HTTP_TIMEOUT_MS || 10000);

const systemHeaders = () => ({
  'x-auth-id': 'notification-service',
  'x-user-id': 'notification-service',
  'x-user-email': process.env.SYSTEM_EMAIL || 'noreply@okkazo.com',
  'x-user-username': 'notification-service',
  'x-user-role': 'ADMIN',
});

const isUuid = (value) => /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(String(value || '').trim());

const fetchUserByAuthId = async (authId) => {
  const id = String(authId || '').trim();
  if (!id) return null;

  try {
    const response = await axios.get(`${userServiceUrl}/auth/${encodeURIComponent(id)}`, {
      timeout,
      headers: systemHeaders(),
    });
    return response?.data?.data || null;
  } catch (error) {
    logger.warn('Failed to fetch user by authId', {
      authId: id,
      message: error?.response?.data?.message || error?.message,
    });
    return null;
  }
};

const fetchUserById = async (userId) => {
  const id = String(userId || '').trim();
  if (!id) return null;

  try {
    const response = await axios.get(`${userServiceUrl}/${encodeURIComponent(id)}`, {
      timeout,
      headers: systemHeaders(),
    });
    return response?.data?.data || null;
  } catch (error) {
    logger.warn('Failed to fetch user by id', {
      userId: id,
      message: error?.response?.data?.message || error?.message,
    });
    return null;
  }
};

const resolveAuthIdFromIdentifier = async (identifier) => {
  const value = String(identifier || '').trim();
  if (!value) return null;

  if (isUuid(value)) {
    return value;
  }

  const byId = await fetchUserById(value);
  if (byId?.authId) return String(byId.authId).trim();

  return null;
};

const fetchUsersByRole = async (role, perPage = 100) => {
  const normalizedRole = String(role || '').trim().toUpperCase();
  if (!normalizedRole) return [];

  const users = [];
  let page = 1;
  let totalPages = 1;

  do {
    try {
      const response = await axios.get(`${userServiceUrl}/platform-users`, {
        timeout,
        headers: systemHeaders(),
        params: {
          role: normalizedRole,
          page,
          limit: perPage,
        },
      });

      const rows = Array.isArray(response?.data?.data) ? response.data.data : [];
      users.push(...rows);
      totalPages = Number(response?.data?.pagination?.totalPages || 1);
      page += 1;
    } catch (error) {
      logger.warn('Failed to fetch users by role', {
        role: normalizedRole,
        page,
        message: error?.response?.data?.message || error?.message,
      });
      break;
    }
  } while (page <= totalPages);

  return users;
};

module.exports = {
  fetchUserByAuthId,
  fetchUserById,
  resolveAuthIdFromIdentifier,
  fetchUsersByRole,
};
