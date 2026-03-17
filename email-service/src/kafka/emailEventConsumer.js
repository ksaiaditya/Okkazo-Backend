const kafkaConfig = require('../config/kafka');
const emailService = require('../services/emailService');
const logger = require('../utils/logger');
const axios = require('axios');

// Module state
let consumer = null;

// Topics
const authTopic = process.env.KAFKA_AUTH_TOPIC || process.env.KAFKA_TOPIC || 'auth_events';
const paymentTopic = process.env.KAFKA_PAYMENT_TOPIC || 'payment_events';

// Upstream service URLs (Docker defaults)
const userServiceUrl = process.env.USER_SERVICE_URL || 'http://user-service:8082';
const eventServiceUrl = process.env.EVENT_SERVICE_URL || 'http://event-service:8086';

// HTTP + dedupe settings
const httpTimeoutMs = parseInt(process.env.UPSTREAM_HTTP_TIMEOUT_MS || '10000', 10);
const dedupeTtlMs = parseInt(process.env.PAYMENT_EMAIL_DEDUPE_TTL_MS || '3600000', 10);
const sentPaymentEmails = new Map();

const pruneSentCache = () => {
  const now = Date.now();
  for (const [key, ts] of sentPaymentEmails.entries()) {
    if (now - ts > dedupeTtlMs) {
      sentPaymentEmails.delete(key);
    }
  }
};

const initialize = async () => {
  const maxRetries = 5;
  const retryDelay = 3000; // 3 seconds

  for (let attempt = 1; attempt <= maxRetries; attempt += 1) {
    try {
      consumer = await kafkaConfig.createConsumer();

      // Allow metadata to sync
      await new Promise((resolve) => setTimeout(resolve, 1500));

      await consumer.subscribe({ topic: authTopic, fromBeginning: false });
      if (paymentTopic && paymentTopic !== authTopic) {
        await consumer.subscribe({ topic: paymentTopic, fromBeginning: false });
      }

      logger.info('Subscribed to Kafka topics', { authTopic, paymentTopic });
      return;
    } catch (error) {
      logger.error(`Error initializing Kafka consumer (attempt ${attempt}/${maxRetries}):`, error.message);
      if (attempt < maxRetries) {
        await new Promise((resolve) => setTimeout(resolve, retryDelay));
      } else {
        throw error;
      }
    }
  }
};

const fetchUserByAuthId = async (authId) => {
  const response = await axios.get(`${userServiceUrl}/auth/${encodeURIComponent(authId)}`, {
    timeout: httpTimeoutMs,
  });

  return response.data?.data;
};

const fetchPlanningByEventIdForUser = async (eventId, user) => {
  const response = await axios.get(`${eventServiceUrl}/planning/${encodeURIComponent(eventId)}`, {
    timeout: httpTimeoutMs,
    headers: {
      'x-auth-id': user?.authId || '',
      'x-user-id': user?._id?.toString?.() || user?.id || '',
      'x-user-email': user?.email || '',
      'x-user-username': user?.name || '',
      'x-user-role': user?.role || 'USER',
    },
  });

  return response.data?.data;
};

const fetchPromoteByEventIdForUser = async (eventId, user) => {
  const response = await axios.get(`${eventServiceUrl}/promote/${encodeURIComponent(eventId)}`, {
    timeout: httpTimeoutMs,
    headers: {
      'x-auth-id': user?.authId || '',
      'x-user-id': user?._id?.toString?.() || user?.id || '',
      'x-user-email': user?.email || '',
      'x-user-username': user?.name || '',
      'x-user-role': user?.role || 'USER',
    },
  });

  return response.data?.data;
};

const handlePaymentSuccessEvent = async (payload) => {
  const { eventId, authId, razorpayPaymentId, transactionId, paidAt, amount, currency, orderType } = payload || {};

  if (!eventId || !authId) {
    logger.error('PAYMENT_SUCCESS event missing required fields', { eventId, authId });
    return;
  }

  const dedupeKey = razorpayPaymentId || transactionId || payload.paymentOrderId || `${eventId}:${paidAt || ''}`;
  pruneSentCache();

  const existingTs = sentPaymentEmails.get(dedupeKey);
  if (existingTs && Date.now() - existingTs < dedupeTtlMs) {
    logger.info('Skipping duplicate PAYMENT_SUCCESS email', { eventId, authId, dedupeKey });
    return;
  }

  const user = await fetchUserByAuthId(authId);
  const recipientEmail = user?.email;
  if (!recipientEmail) {
    logger.error('Unable to send payment email: user email not found', { authId, eventId });
    return;
  }

  const recipientName = user?.name || user?.fullName || 'there';

  let eventTitle = 'your event';
  let eventLocation = 'TBA';
  let eventStatus = 'CONFIRMED';

  if ((orderType || '').toUpperCase() === 'PROMOTE EVENT') {
    const promote = await fetchPromoteByEventIdForUser(eventId, user);
    if (!promote) {
      logger.error('Unable to send payment email: promote record not found', { authId, eventId, orderType });
      return;
    }

    eventTitle = promote?.eventTitle || eventTitle;
    eventLocation = promote?.venue?.locationName || eventLocation;
    eventStatus = promote?.eventStatus || eventStatus;
  } else {
    const planning = await fetchPlanningByEventIdForUser(eventId, user);
    if (!planning) {
      logger.error('Unable to send payment email: planning not found', { authId, eventId, orderType });
      return;
    }

    eventTitle = planning?.eventTitle || eventTitle;
    eventLocation = planning?.location?.name || planning?.location || eventLocation;
    eventStatus = planning?.status || eventStatus;
  }

  await emailService.sendPaymentSuccessEmail(recipientEmail, {
    recipientName,
    eventId,
    eventTitle,
    eventLocation,
    eventStatus,
    amount,
    currency,
    transactionId,
    paidAt,
  });

  sentPaymentEmails.set(dedupeKey, Date.now());
  logger.info('Payment success email sent successfully', { authId, eventId, to: recipientEmail });
};

const handleEvent = async (eventType, payload, topic) => {
  if (!eventType) {
    logger.warn('Received Kafka event without type', { topic });
    return;
  }

  switch (eventType) {
    case 'USER_REGISTERED':
      await handleUserRegistered(payload);
      break;
    case 'PASSWORD_RESET_REQUESTED':
      await handlePasswordResetRequested(payload);
      break;
    case 'EMAIL_VERIFICATION_RESEND':
      await handleEmailVerificationResend(payload);
      break;
    case 'VENDOR_ACCOUNT_CREATED':
      await handleVendorAccountCreated(payload);
      break;
    case 'MANAGER_ACCOUNT_CREATED':
      await handleManagerAccountCreated(payload);
      break;
    case 'PAYMENT_SUCCESS':
      if (topic === paymentTopic) {
        await handlePaymentSuccessEvent(payload);
      }
      break;
    // Payment events that are useful for audit/analytics but do not trigger emails here
    case 'PAYMENT_ORDER_CREATED':
    case 'PAYMENT_TRANSACTION_UPDATED':
    case 'PAYMENT_REFUND_SUCCESS':
      break;
    default:
      logger.warn(`Unknown event type: ${eventType}`);
  }
};

const startConsuming = async () => {
  if (!consumer) {
    throw new Error('Consumer not initialized. Call initialize() first.');
  }

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const key = message.key ? message.key.toString() : null;
        const rawValue = message.value.toString();

        logger.debug('Raw Kafka message', { topic, partition, key, rawValue });

        const payload = JSON.parse(rawValue);
        const eventType = payload.type || payload.eventType;

        logger.info(`Received Kafka message: ${eventType}`, {
          topic,
          partition,
          offset: message.offset,
          key,
        });

        await handleEvent(eventType, payload, topic);
      } catch (error) {
        logger.error('Error processing Kafka message:', error);
      }
    },
  });

  logger.info('Kafka consumer started successfully');
};

// Existing auth / account lifecycle handlers

const handleUserRegistered = async (event) => {
  try {
    const { authId, email, verificationToken } = event;

    if (!authId || !email || !verificationToken) {
      logger.error('USER_REGISTERED event missing required fields', { event });
      return;
    }

    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(email)) {
      logger.error('USER_REGISTERED event has invalid email format', { email });
      return;
    }

    logger.info('Processing USER_REGISTERED event', { authId, email });
    await emailService.sendVerificationEmail(email, verificationToken, authId);
    logger.info('Verification email sent successfully', { authId, email });
  } catch (error) {
    logger.error('Error handling USER_REGISTERED event:', error);
    throw error;
  }
};

const handlePasswordResetRequested = async (event) => {
  try {
    const { authId, email, resetToken } = event;

    if (!authId || !email || !resetToken) {
      logger.error('PASSWORD_RESET_REQUESTED event missing required fields', { event });
      return;
    }

    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(email)) {
      logger.error('PASSWORD_RESET_REQUESTED event has invalid email format', { email });
      return;
    }

    logger.info('Processing PASSWORD_RESET_REQUESTED event', { authId, email });
    await emailService.sendPasswordResetEmail(email, resetToken, authId);
    logger.info('Password reset email sent successfully', { authId, email });
  } catch (error) {
    logger.error('Error handling PASSWORD_RESET_REQUESTED event:', error);
    throw error;
  }
};

const handleEmailVerificationResend = async (event) => {
  try {
    const { authId, email, verificationToken } = event;

    if (!authId || !email || !verificationToken) {
      logger.error('EMAIL_VERIFICATION_RESEND event missing required fields', { event });
      return;
    }

    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(email)) {
      logger.error('EMAIL_VERIFICATION_RESEND event has invalid email format', { email });
      return;
    }

    logger.info('Processing EMAIL_VERIFICATION_RESEND event', { authId, email });
    await emailService.sendVerificationEmail(email, verificationToken, authId);
    logger.info('Verification email resent successfully', { authId, email });
  } catch (error) {
    logger.error('Error handling EMAIL_VERIFICATION_RESEND event:', error);
    throw error;
  }
};

const handleVendorAccountCreated = async (event) => {
  try {
    const { authId, email, passwordResetToken, businessName, applicationId } = event;

    if (!authId || !email || !passwordResetToken || !businessName || !applicationId) {
      logger.error('VENDOR_ACCOUNT_CREATED event missing required fields', { event });
      return;
    }

    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(email)) {
      logger.error('VENDOR_ACCOUNT_CREATED event has invalid email format', { email });
      return;
    }

    logger.info('Processing VENDOR_ACCOUNT_CREATED event', { authId, email, businessName, applicationId });
    await emailService.sendVendorAccountCreatedEmail(email, passwordResetToken, businessName, applicationId, authId);
    logger.info('Vendor account created email sent successfully', { authId, email, applicationId });
  } catch (error) {
    logger.error('Error handling VENDOR_ACCOUNT_CREATED event:', error);
    throw error;
  }
};

const handleManagerAccountCreated = async (event) => {
  try {
    const { authId, email, passwordResetToken, name, department, assignedRole } = event;

    if (!authId || !email || !passwordResetToken || !name) {
      logger.error('MANAGER_ACCOUNT_CREATED event missing required fields', { event });
      return;
    }

    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(email)) {
      logger.error('MANAGER_ACCOUNT_CREATED event has invalid email format', { email });
      return;
    }

    logger.info('Processing MANAGER_ACCOUNT_CREATED event', { authId, email, name, department });
    await emailService.sendManagerAccountCreatedEmail(email, passwordResetToken, name, department, assignedRole, authId);
    logger.info('Manager account created email sent successfully', { authId, email });
  } catch (error) {
    logger.error('Error handling MANAGER_ACCOUNT_CREATED event:', error);
    throw error;
  }
};

const shutdown = async () => {
  try {
    if (consumer) {
      await consumer.disconnect();
      logger.info('Kafka consumer shut down gracefully');
    }
  } catch (error) {
    logger.error('Error shutting down Kafka consumer:', error);
  }
};

module.exports = {
  initialize,
  startConsuming,
  shutdown,
};
