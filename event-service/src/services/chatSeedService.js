const axios = require('axios');
const logger = require('../utils/logger');

const resolveChatServiceBaseUrl = () => {
  const explicit = process.env.CHAT_SERVICE_URL;
  if (explicit && String(explicit).trim()) return String(explicit).trim().replace(/\/$/, '');

  // Default for docker-compose networking.
  return 'http://chat-service:8089';
};

const postJson = async (url, body, headers = {}) => {
  const res = await axios.post(url, body || {}, {
    headers: {
      'Content-Type': 'application/json',
      ...headers,
    },
    timeout: 5000,
    validateStatus: () => true,
  });

  const json = res?.data;
  if (res.status < 200 || res.status >= 300 || !json?.success) {
    const msg = json?.message || `HTTP ${res.status}`;
    throw new Error(msg);
  }

  return json.data;
};

/**
 * Ensure the event conversation exists and includes both the event owner (user) and assigned manager.
 *
 * This uses chat-service's existing header-based auth (x-auth-id/x-user-role).
 * It is best-effort and must never block assignment flows.
 */
const ensureEventChatSeeded = async ({ eventId, userAuthId, managerAuthId }) => {
  const safeEventId = String(eventId || '').trim();
  const safeUserAuthId = String(userAuthId || '').trim();
  const safeManagerAuthId = String(managerAuthId || '').trim();

  if (!safeEventId || !safeUserAuthId || !safeManagerAuthId) return;

  const baseUrl = resolveChatServiceBaseUrl();
  const url = `${baseUrl}/api/chat/conversations/event/${encodeURIComponent(safeEventId)}/ensure`;

  try {
    // Ensure user is a participant
    await postJson(url, {}, { 'x-auth-id': safeUserAuthId, 'x-user-role': 'USER' });

    // Ensure manager is a participant
    await postJson(url, {}, { 'x-auth-id': safeManagerAuthId, 'x-user-role': 'MANAGER' });
  } catch (err) {
    logger.error('Failed to seed event chat conversation', {
      eventId: safeEventId,
      message: err?.message || String(err),
    });
  }
};

module.exports = {
  ensureEventChatSeeded,
};
