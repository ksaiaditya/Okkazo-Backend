const axios = require('axios');
const logger = require('../utils/logger');

const resolveChatServiceBaseUrl = () => {
  const explicit = process.env.CHAT_SERVICE_URL;
  if (explicit && String(explicit).trim()) return String(explicit).trim().replace(/\/$/, '');
  return 'http://chat-service:8089';
};

const postJson = async ({ url, body, headers }) => {
  const res = await axios.post(url, body || {}, {
    headers: {
      'Content-Type': 'application/json',
      ...(headers || {}),
    },
    timeout: 8000,
    validateStatus: () => true,
  });

  const json = res?.data;
  if (res.status < 200 || res.status >= 300 || !json?.success) {
    const msg = json?.message || `HTTP ${res.status}`;
    throw new Error(msg);
  }

  return json.data;
};

const ensureEventConversation = async ({ eventId, senderAuthId, senderRole }) => {
  const safeEventId = String(eventId || '').trim();
  const safeAuthId = String(senderAuthId || '').trim();
  const safeRole = String(senderRole || '').trim();

  if (!safeEventId || !safeAuthId || !safeRole) {
    throw new Error('eventId, senderAuthId, senderRole are required');
  }

  const baseUrl = resolveChatServiceBaseUrl();
  const url = `${baseUrl}/api/chat/conversations/event/${encodeURIComponent(safeEventId)}/ensure`;

  return postJson({
    url,
    body: {},
    headers: { 'x-auth-id': safeAuthId, 'x-user-role': safeRole },
  });
};

const sendConversationMessage = async ({ conversationId, senderAuthId, senderRole, text }) => {
  const convoId = String(conversationId || '').trim();
  const safeAuthId = String(senderAuthId || '').trim();
  const safeRole = String(senderRole || '').trim();
  const safeText = text != null ? String(text) : '';

  if (!convoId || !safeAuthId || !safeRole) {
    throw new Error('conversationId, senderAuthId, senderRole are required');
  }

  const baseUrl = resolveChatServiceBaseUrl();
  const url = `${baseUrl}/api/chat/conversations/${encodeURIComponent(convoId)}/messages`;

  return postJson({
    url,
    body: { text: safeText },
    headers: { 'x-auth-id': safeAuthId, 'x-user-role': safeRole },
  });
};

const sendEventConversationMessage = async ({ eventId, senderAuthId, senderRole, text }) => {
  const convo = await ensureEventConversation({ eventId, senderAuthId, senderRole });
  const conversationId = String(convo?._id || convo?.id || '').trim();
  if (!conversationId) throw new Error('Failed to resolve conversationId');

  try {
    return await sendConversationMessage({ conversationId, senderAuthId, senderRole, text });
  } catch (err) {
    logger.error('Failed to send chat message to event conversation', {
      eventId: String(eventId || '').trim(),
      conversationId,
      senderRole,
      message: err?.message || String(err),
    });
    throw err;
  }
};

module.exports = {
  ensureEventConversation,
  sendConversationMessage,
  sendEventConversationMessage,
};
