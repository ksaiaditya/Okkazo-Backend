const kafkaConfig = require('../config/kafka');
const emailService = require('../services/emailService');
const logger = require('../utils/logger');
const axios = require('axios');

// Module state
let consumer = null;

// Topics
const authTopic = process.env.KAFKA_AUTH_TOPIC || process.env.KAFKA_TOPIC || 'auth_events';
const paymentTopic = process.env.KAFKA_PAYMENT_TOPIC || 'payment_events';
const eventTopic = process.env.KAFKA_EVENT_TOPIC || 'event_events';

// Upstream service URLs (Docker defaults)
const userServiceUrl = process.env.USER_SERVICE_URL || 'http://user-service:8082';
const eventServiceUrl = process.env.EVENT_SERVICE_URL || 'http://event-service:8086';

// HTTP + dedupe settings
const httpTimeoutMs = parseInt(process.env.UPSTREAM_HTTP_TIMEOUT_MS || '10000', 10);
const dedupeTtlMs = parseInt(process.env.PAYMENT_EMAIL_DEDUPE_TTL_MS || '3600000', 10);
const sentPaymentEmails = new Map();

const alternativesDedupeTtlMs = parseInt(process.env.ALTERNATIVES_EMAIL_DEDUPE_TTL_MS || '3600000', 10);
const sentAlternativesEmails = new Map();

const quoteDedupeTtlMs = parseInt(process.env.QUOTE_EMAIL_DEDUPE_TTL_MS || '86400000', 10);
const sentQuoteEmails = new Map();

const pruneSentCache = () => {
  const now = Date.now();
  for (const [key, ts] of sentPaymentEmails.entries()) {
    if (now - ts > dedupeTtlMs) {
      sentPaymentEmails.delete(key);
    }
  }
};

const pruneAlternativesSentCache = () => {
  const now = Date.now();
  for (const [key, ts] of sentAlternativesEmails.entries()) {
    if (now - ts > alternativesDedupeTtlMs) {
      sentAlternativesEmails.delete(key);
    }
  }
};

const pruneQuoteSentCache = () => {
  const now = Date.now();
  for (const [key, ts] of sentQuoteEmails.entries()) {
    if (now - ts > quoteDedupeTtlMs) {
      sentQuoteEmails.delete(key);
    }
  }
};

const formatMoneyRangeFromPaise = (minPaise, maxPaise) => {
  const min = Number(minPaise);
  const max = Number(maxPaise);
  if (!Number.isFinite(min) || min <= 0) return '—';
  const safeMax = Number.isFinite(max) && max > 0 ? max : min;
  const toInr = (p) => (Math.round(Number(p || 0)) / 100);
  const fmt = (n) => `₹${Number(n || 0).toLocaleString('en-IN', { maximumFractionDigits: 2 })}`;
  return `${fmt(toInr(min))} - ${fmt(toInr(safeMax))}`;
};

const formatMoneyFromPaise = (paise) => {
  const n = Number(paise);
  if (!Number.isFinite(n) || n <= 0) return '—';
  const inr = Math.round(n) / 100;
  return `₹${inr.toLocaleString('en-IN', { maximumFractionDigits: 2 })}`;
};

const fetchPlanningQuoteLatestForUser = async (eventId, user) => {
  const response = await axios.get(`${eventServiceUrl}/planning/${encodeURIComponent(eventId)}/quote/latest`, {
    timeout: httpTimeoutMs,
    headers: {
      'x-auth-id': user?.authId || '',
      'x-user-id': user?._id?.toString?.() || user?.id || '',
      'x-user-email': user?.email || '',
      'x-user-username': user?.name || user?.username || '',
      'x-user-role': user?.role || 'USER',
    },
  });

  return response.data?.data;
};

const handlePlanningQuoteLocked = async (event) => {
  const { eventId, authId, version } = event || {};

  if (!eventId || !authId) {
    logger.error('PLANNING_QUOTE_LOCKED missing required fields', { event });
    return;
  }

  pruneQuoteSentCache();

  const userDedupeKey = `${String(eventId)}:${String(version || 'latest')}:USER`;
  if (sentQuoteEmails.has(userDedupeKey)) {
    logger.info('Skipping duplicate quote email (user)', { eventId, version, userDedupeKey });
    return;
  }

  const owner = await fetchUserByAuthId(authId);
  if (!owner?.email) {
    logger.error('Unable to send quote email: owner email not found', { authId, eventId });
    return;
  }

  let planning = null;
  try {
    planning = await fetchPlanningByEventIdForUser(eventId, { ...owner, role: owner?.role || 'USER' });
  } catch (e) {
    logger.warn('Failed to fetch planning for quote email (non-blocking)', { eventId, authId, message: e?.message || String(e) });
  }

  let quote = null;
  try {
    quote = await fetchPlanningQuoteLatestForUser(eventId, { ...owner, role: owner?.role || 'USER' });
  } catch (e) {
    logger.error('Failed to fetch quote for PLANNING_QUOTE_LOCKED', { eventId, authId, message: e?.message || String(e) });
    return;
  }

  const eventTitle = planning?.eventTitle || planning?.eventName || 'Event';
  const eventDate = planning?.eventDate || planning?.schedule?.startAt || quote?.eventStartAt || null;
  const eventLocation = planning?.location?.name || planning?.location?.location || null;

  const vendorCache = new Map();
  const resolveVendorName = async (vendorAuthId) => {
    const key = String(vendorAuthId || '').trim();
    if (!key) return '—';
    if (vendorCache.has(key)) return vendorCache.get(key);
    const u = await fetchUserByAuthId(key);
    const name = u?.businessName || u?.vendorName || u?.name || u?.username || 'Vendor';
    vendorCache.set(key, name);
    return name;
  };

  const quoteItems = Array.isArray(quote?.items) ? quote.items : [];
  const userItems = [];
  for (const it of quoteItems) {
    const vendorName = await resolveVendorName(it?.vendorAuthId);
    userItems.push({
      service: it?.service || 'Service',
      vendorName,
      clientRange: formatMoneyRangeFromPaise(it?.clientTotal?.minPaise, it?.clientTotal?.maxPaise),
    });
  }

  const promotionsTotal = quote?.promotionsTotal?.minPaise
    ? formatMoneyFromPaise(quote.promotionsTotal.minPaise)
    : null;
  const grandTotal = formatMoneyRangeFromPaise(quote?.clientGrandTotal?.minPaise, quote?.clientGrandTotal?.maxPaise);

  await emailService.sendPlanningQuoteLockedUserEmail(owner.email, {
    recipientName: owner?.name || owner?.username || 'there',
    eventId,
    eventTitle,
    eventDate,
    eventLocation,
    version: quote?.version || version || 1,
    items: userItems,
    promotionsTotal,
    grandTotal,
  });

  sentQuoteEmails.set(userDedupeKey, Date.now());

  // Vendor emails (per vendorAuthId)
  const items = quoteItems;
  const byVendor = new Map();
  for (const it of items) {
    const vendorAuth = (it?.vendorAuthId || '').toString().trim();
    if (!vendorAuth) continue;
    if (!byVendor.has(vendorAuth)) byVendor.set(vendorAuth, []);
    byVendor.get(vendorAuth).push(it);
  }

  for (const [vendorAuthId, vendorItemsRaw] of byVendor.entries()) {
    const vendorDedupeKey = `${String(eventId)}:${String(quote?.version || version || 'latest')}:VENDOR:${vendorAuthId}`;
    pruneQuoteSentCache();
    if (sentQuoteEmails.has(vendorDedupeKey)) {
      logger.info('Skipping duplicate quote email (vendor)', { eventId, version, vendorAuthId });
      continue;
    }

    const vendorUser = await fetchUserByAuthId(vendorAuthId);
    const vendorEmail = vendorUser?.email;
    if (!vendorEmail) {
      logger.warn('Skipping vendor quote email: vendor email not found', { vendorAuthId, eventId });
      continue;
    }

    let vendorMin = 0;
    let vendorMax = 0;
    let chargeMin = 0;
    let chargeMax = 0;
    let clientMin = 0;
    let clientMax = 0;

    const vendorItems = vendorItemsRaw.map((it) => {
      const vMin = Number(it?.vendorTotal?.minPaise || 0);
      const vMax = Number(it?.vendorTotal?.maxPaise || 0);
      const cMin = Number(it?.serviceCharge?.minPaise || 0);
      const cMax = Number(it?.serviceCharge?.maxPaise || 0);
      const clMin = Number(it?.clientTotal?.minPaise || 0);
      const clMax = Number(it?.clientTotal?.maxPaise || 0);

      if (Number.isFinite(vMin)) vendorMin += vMin;
      if (Number.isFinite(vMax)) vendorMax += vMax;
      if (Number.isFinite(cMin)) chargeMin += cMin;
      if (Number.isFinite(cMax)) chargeMax += cMax;
      if (Number.isFinite(clMin)) clientMin += clMin;
      if (Number.isFinite(clMax)) clientMax += clMax;

      return {
        service: it?.service || 'Service',
        vendorRange: formatMoneyRangeFromPaise(it?.vendorTotal?.minPaise, it?.vendorTotal?.maxPaise),
        chargeRange: formatMoneyRangeFromPaise(it?.serviceCharge?.minPaise, it?.serviceCharge?.maxPaise),
        clientRange: formatMoneyRangeFromPaise(it?.clientTotal?.minPaise, it?.clientTotal?.maxPaise),
        commissionPercent: Number(it?.commissionPercent || 0),
      };
    });

    await emailService.sendPlanningQuoteLockedVendorEmail(vendorEmail, {
      recipientName: vendorUser?.name || vendorUser?.username || 'there',
      eventId,
      eventTitle,
      eventDate,
      eventLocation,
      version: quote?.version || version || 1,
      items: vendorItems,
      vendorSubtotal: formatMoneyRangeFromPaise(vendorMin, vendorMax),
      serviceChargeTotal: formatMoneyRangeFromPaise(chargeMin, chargeMax),
      clientTotal: formatMoneyRangeFromPaise(clientMin, clientMax),
    });

    sentQuoteEmails.set(vendorDedupeKey, Date.now());
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

      if (eventTopic && eventTopic !== authTopic && eventTopic !== paymentTopic) {
        await consumer.subscribe({ topic: eventTopic, fromBeginning: false });
      }

      logger.info('Subscribed to Kafka topics', { authTopic, paymentTopic, eventTopic });
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

const formatMoneyRangeFromBasePrice = (basePrice) => {
  const p = basePrice == null ? null : Number(basePrice);
  if (!Number.isFinite(p) || p <= 0) return '—';
  const min = Math.round(p);
  const max = Math.ceil(min * 1.5);
  return `₹${min.toLocaleString()} - ₹${max.toLocaleString()}`;
};

const formatDistance = (km) => {
  const n = km == null ? null : Number(km);
  if (!Number.isFinite(n)) return '—';
  if (n < 1) return `${Math.round(n * 1000)} m`;
  return `${n.toFixed(1)} km`;
};

const handleVendorRequestRejectedAlternatives = async (event) => {
  const { eventId, authId, service, rejectionReason, options, radiusKm } = event || {};

  if (!eventId || !authId || !service) {
    logger.error('VENDOR_REQUEST_REJECTED_ALTERNATIVES missing required fields', { event });
    return;
  }

  const opts = Array.isArray(options) ? options : [];
  if (opts.length === 0) {
    logger.info('No alternatives in VENDOR_REQUEST_REJECTED_ALTERNATIVES; skipping email', { eventId, authId, service });
    return;
  }

  // Dedupe (Kafka retries / replays)
  const dedupeKey = `${String(eventId)}:${String(service)}`;
  pruneAlternativesSentCache();
  const existingTs = sentAlternativesEmails.get(dedupeKey);
  if (existingTs && Date.now() - existingTs < alternativesDedupeTtlMs) {
    logger.info('Skipping duplicate alternatives email', { eventId, authId, service, dedupeKey });
    return;
  }

  const user = await fetchUserByAuthId(authId);
  const recipientEmail = user?.email;
  if (!recipientEmail) {
    logger.error('Unable to send alternatives email: user email not found', { authId, eventId });
    return;
  }

  let planning = null;
  try {
    planning = await fetchPlanningByEventIdForUser(eventId, user);
  } catch (e) {
    logger.warn('Failed to fetch planning for alternatives email (non-blocking)', { eventId, authId, message: e?.message || String(e) });
  }

  const safeEventTitle = planning?.eventTitle || planning?.eventName || 'Event';
  const safeEventDate = planning?.eventDate || planning?.schedule?.startAt || null;
  const safeLocation = planning?.location?.name || planning?.location?.location || null;

  const topOptions = opts.slice(0, 8).map((o) => ({
    businessName: o?.businessName || 'Vendor',
    tier: o?.tier || null,
    serviceCategory: o?.serviceCategory || service,
    location: o?.location || safeLocation || null,
    country: o?.country || null,
    priceRange: formatMoneyRangeFromBasePrice(o?.price),
    distanceText: o?.distanceText || formatDistance(o?.distanceKm),
  }));

  await emailService.sendVendorRejectedAlternativesEmail(recipientEmail, {
    recipientName: user?.name || user?.username || 'there',
    eventId,
    eventTitle: safeEventTitle,
    eventDate: safeEventDate,
    eventLocation: safeLocation,
    serviceLabel: service,
    rejectionReason: rejectionReason || null,
    radiusKm: Number.isFinite(Number(radiusKm)) ? Number(radiusKm) : null,
    options: topOptions,
  });

  sentAlternativesEmails.set(dedupeKey, Date.now());
};

const fetchUserByAuthId = async (authId) => {
  try {
    const response = await axios.get(`${userServiceUrl}/auth/${encodeURIComponent(authId)}`, {
      timeout: httpTimeoutMs,
    });

    return response.data?.data;
  } catch (e) {
    const status = e?.response?.status;
    logger.warn('Failed to fetch user from user-service', {
      authId,
      status,
      message: e?.message || String(e),
    });
    return null;
  }
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
    case 'VENDOR_REQUEST_REJECTED_ALTERNATIVES':
      if (topic === eventTopic) {
        await handleVendorRequestRejectedAlternatives(payload);
      }
      break;
    case 'PLANNING_QUOTE_LOCKED':
      if (topic === eventTopic) {
        await handlePlanningQuoteLocked(payload);
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
        logger.error('Error processing Kafka message', {
          topic,
          partition,
          offset: message?.offset,
          message: error?.message || String(error),
          stack: error?.stack,
        });
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
