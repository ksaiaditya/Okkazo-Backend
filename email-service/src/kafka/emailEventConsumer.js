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

const paymentRefundEmailDedupeTtlMs = parseInt(
  process.env.PAYMENT_REFUND_EMAIL_DEDUPE_TTL_MS || String(dedupeTtlMs),
  10
);
const sentPaymentRefundEmails = new Map();

const alternativesDedupeTtlMs = parseInt(process.env.ALTERNATIVES_EMAIL_DEDUPE_TTL_MS || '3600000', 10);
const sentAlternativesEmails = new Map();

const quoteDedupeTtlMs = parseInt(process.env.QUOTE_EMAIL_DEDUPE_TTL_MS || '86400000', 10);
const sentQuoteEmails = new Map();

const settlementDedupeTtlMs = parseInt(process.env.SETTLEMENT_EMAIL_DEDUPE_TTL_MS || '86400000', 10);
const sentSettlementEmails = new Map();

const userRevenuePayoutEmailDedupeTtlMs = parseInt(
  process.env.USER_REVENUE_PAYOUT_EMAIL_DEDUPE_TTL_MS || String(dedupeTtlMs),
  10
);
const sentUserRevenuePayoutEmails = new Map();

const promotionEmailBlastDedupeTtlMs = parseInt(
  process.env.PROMOTION_EMAIL_BLAST_DEDUPE_TTL_MS || '900000',
  10
);
const sentPromotionEmailBlastRequests = new Map();

const ticketReminderEmailDedupeTtlMs = parseInt(
  process.env.TICKET_REMINDER_EMAIL_DEDUPE_TTL_MS || '172800000',
  10
);
const sentTicketReminderEmails = new Map();

const planningCancellationEmailDedupeTtlMs = parseInt(
  process.env.PLANNING_CANCELLATION_EMAIL_DEDUPE_TTL_MS || '604800000',
  10
);
const sentPlanningCancellationEmails = new Map();

const ticketCancellationEmailDedupeTtlMs = parseInt(
  process.env.TICKET_CANCELLATION_EMAIL_DEDUPE_TTL_MS || '604800000',
  10
);
const sentTicketCancellationEmails = new Map();

const pruneSentCache = () => {
  const now = Date.now();
  for (const [key, ts] of sentPaymentEmails.entries()) {
    if (now - ts > dedupeTtlMs) {
      sentPaymentEmails.delete(key);
    }
  }
};

const prunePaymentRefundSentCache = () => {
  const now = Date.now();
  for (const [key, ts] of sentPaymentRefundEmails.entries()) {
    if (now - ts > paymentRefundEmailDedupeTtlMs) {
      sentPaymentRefundEmails.delete(key);
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

const pruneSettlementSentCache = () => {
  const now = Date.now();
  for (const [key, ts] of sentSettlementEmails.entries()) {
    if (now - ts > settlementDedupeTtlMs) {
      sentSettlementEmails.delete(key);
    }
  }
};

const pruneUserRevenuePayoutSentCache = () => {
  const now = Date.now();
  for (const [key, ts] of sentUserRevenuePayoutEmails.entries()) {
    if (now - ts > userRevenuePayoutEmailDedupeTtlMs) {
      sentUserRevenuePayoutEmails.delete(key);
    }
  }
};

const prunePromotionEmailBlastSentCache = () => {
  const now = Date.now();
  for (const [key, ts] of sentPromotionEmailBlastRequests.entries()) {
    if (now - ts > promotionEmailBlastDedupeTtlMs) {
      sentPromotionEmailBlastRequests.delete(key);
    }
  }
};

const pruneTicketReminderSentCache = () => {
  const now = Date.now();
  for (const [key, ts] of sentTicketReminderEmails.entries()) {
    if (now - ts > ticketReminderEmailDedupeTtlMs) {
      sentTicketReminderEmails.delete(key);
    }
  }
};

const prunePlanningCancellationSentCache = () => {
  const now = Date.now();
  for (const [key, ts] of sentPlanningCancellationEmails.entries()) {
    if (now - ts > planningCancellationEmailDedupeTtlMs) {
      sentPlanningCancellationEmails.delete(key);
    }
  }
};

const pruneTicketCancellationSentCache = () => {
  const now = Date.now();
  for (const [key, ts] of sentTicketCancellationEmails.entries()) {
    if (now - ts > ticketCancellationEmailDedupeTtlMs) {
      sentTicketCancellationEmails.delete(key);
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

const buildSystemHeaders = () => ({
  'x-auth-id': 'email-service',
  'x-user-id': 'email-service',
  'x-user-email': process.env.FROM_EMAIL || 'noreply@okkazo.com',
  'x-user-username': 'email-service',
  'x-user-role': 'ADMIN',
});

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

const fetchVendorSelectionForUser = async (eventId, user) => {
  const response = await axios.get(`${eventServiceUrl}/vendor-selection/${encodeURIComponent(eventId)}`, {
    timeout: httpTimeoutMs,
    params: {
      includeVendors: 'true',
    },
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
  const { eventId, authId, version, forceSend, emailAudience } = event || {};
  const normalizedAudience = String(emailAudience || '').trim().toUpperCase();
  const sendUserEmail = normalizedAudience !== 'VENDOR_ONLY';
  const sendVendorEmail = normalizedAudience !== 'USER_ONLY';
  const bypassDedupe = Boolean(forceSend);

  if (!eventId || !authId) {
    logger.error('PLANNING_QUOTE_LOCKED missing required fields', { event });
    return;
  }

  pruneQuoteSentCache();

  const userDedupeKey = `${String(eventId)}:${String(version || 'latest')}:USER`;
  if (sendUserEmail && !bypassDedupe && sentQuoteEmails.has(userDedupeKey)) {
    logger.info('Skipping duplicate quote email (user)', { eventId, version, userDedupeKey });
    return;
  }

  const owner = await fetchUserByAuthId(authId);
  if (sendUserEmail && !owner?.email) {
    logger.error('Unable to send quote email: owner email not found', { authId, eventId });
    return;
  }

  const requestUser = {
    ...owner,
    authId: String(authId || '').trim(),
    role: owner?.role || 'USER',
  };

  let planning = null;
  try {
    planning = await fetchPlanningByEventIdForUser(eventId, requestUser);
  } catch (e) {
    logger.warn('Failed to fetch planning for quote email (non-blocking)', { eventId, authId, message: e?.message || String(e) });
  }

  let quote = null;
  try {
    quote = await fetchPlanningQuoteLatestForUser(eventId, requestUser);
  } catch (e) {
    logger.error('Failed to fetch quote for PLANNING_QUOTE_LOCKED', { eventId, authId, message: e?.message || String(e) });
    return;
  }

  let selection = null;
  try {
    selection = await fetchVendorSelectionForUser(eventId, requestUser);
  } catch (e) {
    logger.warn('Failed to fetch vendor selection for quote email (non-blocking)', {
      eventId,
      authId,
      message: e?.message || String(e),
    });
  }

  const eventTitle = planning?.eventTitle || planning?.eventName || 'Event';
  const eventDate = planning?.eventDate || planning?.schedule?.startAt || quote?.eventStartAt || null;
  const eventLocation = planning?.location?.name || planning?.location?.location || null;

  const normalizeLookup = (value) => String(value || '').trim().toLowerCase();
  const toItemLookupKey = (vendorAuthId, service) => {
    const vendorKey = normalizeLookup(vendorAuthId);
    const serviceKey = normalizeLookup(service);
    if (!vendorKey || !serviceKey) return null;
    return `${vendorKey}::${serviceKey}`;
  };

  const vendorProfiles = Array.isArray(selection?.vendorProfiles) ? selection.vendorProfiles : [];
  const vendorProfileByAuthId = new Map();
  for (const profile of vendorProfiles) {
    const authKeys = [profile?.authId, profile?.vendorAuthId, profile?.userAuthId]
      .map((v) => String(v || '').trim())
      .filter(Boolean);
    for (const authKey of authKeys) {
      if (!vendorProfileByAuthId.has(authKey)) {
        vendorProfileByAuthId.set(authKey, profile);
      }
    }
  }

  const selectionMetaByVendorService = new Map();
  const selectionPricingByVendorService = new Map();
  const selectionVendors = Array.isArray(selection?.vendors) ? selection.vendors : [];
  for (const row of selectionVendors) {
    const vendorAuthId = String(row?.vendorAuthId || '').trim();
    const serviceName = String(row?.serviceName || row?.service || '').trim();
    const key = toItemLookupKey(vendorAuthId, row?.service);
    if (!key) continue;

    const pricingQuantityRaw = Number(row?.pricingQuantity);
    const pricingQuantity = Number.isFinite(pricingQuantityRaw) && pricingQuantityRaw > 0
      ? pricingQuantityRaw
      : null;
    const quantityUnit = String(row?.pricingQuantityUnit || row?.pricingUnit || '').trim();
    const quantity = pricingQuantity != null
      ? `${pricingQuantity}${quantityUnit ? ` ${quantityUnit}` : ''}`
      : (quantityUnit || '—');

    const profile = vendorProfileByAuthId.get(vendorAuthId) || null;
    const businessName = String(
      profile?.businessName ||
      profile?.name ||
      row?.businessName ||
      ''
    ).trim();

    selectionMetaByVendorService.set(key, {
      serviceName: serviceName || null,
      quantity,
      businessName: businessName || null,
    });

    const quotedInrRaw = Number(row?.vendorQuotedPrice);
    const quotedInr = Number.isFinite(quotedInrRaw) && quotedInrRaw > 0 ? quotedInrRaw : 0;
    const isLocked = Boolean(row?.priceLocked) && quotedInr > 0;
    const lockedTotalPaise = isLocked
      ? Math.max(0, Math.round(quotedInr * 100))
      : 0;

    selectionPricingByVendorService.set(key, {
      isLocked,
      lockedTotalPaise,
    });
  }

  const vendorCache = new Map();
  const resolveVendorName = async (vendorAuthId) => {
    const key = String(vendorAuthId || '').trim();
    if (!key) return '—';
    if (vendorCache.has(key)) return vendorCache.get(key);

    const profile = vendorProfileByAuthId.get(key);
    const profileName = String(profile?.businessName || profile?.name || '').trim();
    if (profileName) {
      vendorCache.set(key, profileName);
      return profileName;
    }

    const u = await fetchUserByAuthId(key);
    const name = u?.businessName || u?.vendorName || u?.name || u?.username || 'Vendor';
    vendorCache.set(key, name);
    return name;
  };

  const quoteItems = Array.isArray(quote?.items) ? quote.items : [];
  const userItems = [];
  let userItemsTotalPaise = 0;
  for (const it of quoteItems) {
    const vendorAuthId = String(it?.vendorAuthId || '').trim();
    const metaKey = toItemLookupKey(vendorAuthId, it?.service);
    const selectionMeta = metaKey ? selectionMetaByVendorService.get(metaKey) : null;
    const selectionPricing = metaKey ? selectionPricingByVendorService.get(metaKey) : null;
    const vendorName = selectionMeta?.businessName || await resolveVendorName(vendorAuthId);
    const fallbackClientTotalPaise = Number(
      it?.clientTotal?.minPaise ??
      it?.clientTotal?.maxPaise ??
      it?.vendorTotal?.minPaise ??
      it?.vendorTotal?.maxPaise ??
      0
    );
    const itemTotalPaise = (selectionPricing?.isLocked && Number(selectionPricing?.lockedTotalPaise) > 0)
      ? Number(selectionPricing.lockedTotalPaise)
      : fallbackClientTotalPaise;

    if (Number.isFinite(itemTotalPaise) && itemTotalPaise > 0) {
      userItemsTotalPaise += itemTotalPaise;
    }

    userItems.push({
      service: selectionMeta?.serviceName || it?.service || 'Service',
      vendorName,
      quantity: selectionMeta?.quantity || '—',
      clientTotal: formatMoneyFromPaise(itemTotalPaise),
    });
  }

  const promotions = (Array.isArray(quote?.promotions) ? quote.promotions : [])
    .map((p) => ({
      name: String(p?.value || '').trim() || 'Promotion',
      price: formatMoneyFromPaise(p?.feePaise),
    }))
    .filter((p) => p.name);

  const promotionsTotalPaise = Number(quote?.promotionsTotal?.minPaise ?? quote?.promotionsTotal?.maxPaise ?? 0);
  const promotionsTotal = promotionsTotalPaise > 0
    ? formatMoneyFromPaise(promotionsTotalPaise)
    : null;

  const snapshotClientTotalPaise = Number(quote?.clientGrandTotal?.minPaise ?? quote?.clientGrandTotal?.maxPaise ?? 0);
  const totalAmountPaise = userItemsTotalPaise > 0
    ? Math.max(0, userItemsTotalPaise) + Math.max(0, promotionsTotalPaise)
    : snapshotClientTotalPaise;
  const totalAmount = formatMoneyFromPaise(totalAmountPaise);

  if (sendUserEmail) {
    await emailService.sendPlanningQuoteLockedUserEmail(owner.email, {
      recipientName: owner?.name || owner?.username || 'there',
      eventId,
      eventTitle,
      eventDate,
      eventLocation,
      version: quote?.version || version || 1,
      items: userItems,
      promotions,
      promotionsTotal,
      totalAmount,
    });

    sentQuoteEmails.set(userDedupeKey, Date.now());
  }

  if (!sendVendorEmail) {
    return;
  }

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
    if (!bypassDedupe && sentQuoteEmails.has(vendorDedupeKey)) {
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

const handlePlanningRemainingPaymentConfirmed = async (event) => {
  const { eventId, authId, razorpayPaymentId, paymentOrderId, paidAt } = event || {};
  if (!eventId || !authId) {
    logger.error('PLANNING_REMAINING_PAYMENT_CONFIRMED missing required fields', { event });
    return;
  }

  pruneSettlementSentCache();
  const dedupeKey = `${String(eventId)}:${String(razorpayPaymentId || paymentOrderId || paidAt || 'remaining-payment')}`;
  if (sentSettlementEmails.has(dedupeKey)) {
    logger.info('Skipping duplicate settlement thank-you email', { eventId, dedupeKey });
    return;
  }

  const owner = await fetchUserByAuthId(authId);
  if (!owner?.email) {
    logger.error('Unable to send settlement thank-you email: user email not found', { authId, eventId });
    return;
  }

  const requestUser = {
    ...owner,
    authId: String(authId || '').trim(),
    role: owner?.role || 'USER',
  };

  const planning = await fetchPlanningByEventIdForUser(eventId, requestUser);
  if (!planning) {
    logger.error('Unable to send settlement thank-you email: planning not found', { authId, eventId });
    return;
  }

  let quote = null;
  try {
    quote = await fetchPlanningQuoteLatestForUser(eventId, requestUser);
  } catch (e) {
    logger.warn('Failed to fetch latest quote for settlement thank-you email', {
      eventId,
      authId,
      message: e?.message || String(e),
    });
  }

  let selection = null;
  try {
    selection = await fetchVendorSelectionForUser(eventId, requestUser);
  } catch (e) {
    logger.warn('Failed to fetch vendor selection for settlement thank-you email', {
      eventId,
      authId,
      message: e?.message || String(e),
    });
  }

  const normalizeLookup = (value) => String(value || '').trim().toLowerCase();
  const toItemLookupKey = (vendorAuthId, service) => {
    const vendorKey = normalizeLookup(vendorAuthId);
    const serviceKey = normalizeLookup(service);
    if (!vendorKey || !serviceKey) return null;
    return `${vendorKey}::${serviceKey}`;
  };

  const vendorProfiles = Array.isArray(selection?.vendorProfiles) ? selection.vendorProfiles : [];
  const vendorProfileByAuthId = new Map();
  for (const profile of vendorProfiles) {
    const authKeys = [profile?.authId, profile?.vendorAuthId, profile?.userAuthId]
      .map((v) => String(v || '').trim())
      .filter(Boolean);
    for (const authKey of authKeys) {
      if (!vendorProfileByAuthId.has(authKey)) {
        vendorProfileByAuthId.set(authKey, profile);
      }
    }
  }

  const selectionMetaByVendorService = new Map();
  const selectionPricingByVendorService = new Map();
  const selectionVendors = Array.isArray(selection?.vendors) ? selection.vendors : [];
  for (const row of selectionVendors) {
    const vendorAuthId = String(row?.vendorAuthId || '').trim();
    const key = toItemLookupKey(vendorAuthId, row?.service);
    if (!key) continue;

    const profile = vendorProfileByAuthId.get(vendorAuthId) || null;
    const businessName = String(
      profile?.businessName ||
      profile?.name ||
      row?.businessName ||
      ''
    ).trim();

    selectionMetaByVendorService.set(key, {
      serviceName: String(row?.serviceName || row?.service || '').trim() || null,
      businessName: businessName || null,
    });

    const quotedInrRaw = Number(row?.vendorQuotedPrice);
    const quotedInr = Number.isFinite(quotedInrRaw) && quotedInrRaw > 0 ? quotedInrRaw : 0;
    const isLocked = Boolean(row?.priceLocked) && quotedInr > 0;
    const lockedTotalPaise = isLocked
      ? Math.max(0, Math.round(quotedInr * 100))
      : 0;

    selectionPricingByVendorService.set(key, {
      isLocked,
      lockedTotalPaise,
    });
  }

  const quoteItems = Array.isArray(quote?.items) ? quote.items : [];
  const items = [];
  let itemsTotalPaise = 0;
  for (const it of quoteItems) {
    const vendorAuthId = String(it?.vendorAuthId || '').trim();
    const key = toItemLookupKey(vendorAuthId, it?.service);
    const selectionMeta = key ? selectionMetaByVendorService.get(key) : null;
    const selectionPricing = key ? selectionPricingByVendorService.get(key) : null;

    const fallbackClientTotalPaise = Number(
      it?.clientTotal?.minPaise ??
      it?.clientTotal?.maxPaise ??
      it?.vendorTotal?.minPaise ??
      it?.vendorTotal?.maxPaise ??
      0
    );
    const itemTotalPaise = (selectionPricing?.isLocked && Number(selectionPricing?.lockedTotalPaise) > 0)
      ? Number(selectionPricing.lockedTotalPaise)
      : fallbackClientTotalPaise;

    itemsTotalPaise += Number.isFinite(itemTotalPaise) && itemTotalPaise > 0 ? itemTotalPaise : 0;

    items.push({
      service: selectionMeta?.serviceName || it?.service || 'Service',
      vendorName: selectionMeta?.businessName || 'Vendor',
      amount: formatMoneyFromPaise(itemTotalPaise),
    });
  }

  const promotions = (Array.isArray(quote?.promotions) ? quote.promotions : [])
    .map((p) => ({
      name: String(p?.value || '').trim() || 'Promotion',
      amount: formatMoneyFromPaise(p?.feePaise),
    }))
    .filter((p) => p.name);

  const promotionsTotalPaise = Number(quote?.promotionsTotal?.minPaise ?? quote?.promotionsTotal?.maxPaise ?? 0);
  const totalAmountPaise = Number(quote?.clientGrandTotal?.minPaise ?? quote?.clientGrandTotal?.maxPaise ?? 0)
    || Math.max(0, itemsTotalPaise + Math.max(0, promotionsTotalPaise));

  const depositPaidAmountPaise = Math.max(0, Number(planning?.depositPaidAmountPaise || 0));
  const vendorConfirmationPaidAmountPaise = Math.max(0, Number(planning?.vendorConfirmationPaidAmountPaise || 0));
  const remainingPaymentPaidAmountPaise = Math.max(0, Number(planning?.remainingPaymentPaidAmountPaise || 0));
  const totalPaidPaise = depositPaidAmountPaise + vendorConfirmationPaidAmountPaise + remainingPaymentPaidAmountPaise;

  const frontendBaseUrl = String(process.env.FRONTEND_URL || '').replace(/\/$/, '');
  const feedbackUrl = frontendBaseUrl
    ? `${frontendBaseUrl}/user/event-management/${encodeURIComponent(String(eventId))}`
    : null;

  await emailService.sendPlanningFinalSettlementThankYouEmail(owner.email, {
    recipientName: owner?.name || owner?.username || 'there',
    eventId,
    eventTitle: planning?.eventTitle || 'Event',
    eventDate: planning?.eventDate || planning?.schedule?.startAt || null,
    eventLocation: planning?.location?.name || planning?.location?.location || null,
    items,
    promotions,
    totalAmount: formatMoneyFromPaise(totalAmountPaise || totalPaidPaise),
    depositPaid: formatMoneyFromPaise(depositPaidAmountPaise),
    vendorConfirmationPaid: formatMoneyFromPaise(vendorConfirmationPaidAmountPaise),
    remainingPaid: formatMoneyFromPaise(remainingPaymentPaidAmountPaise),
    totalPaid: formatMoneyFromPaise(totalPaidPaise),
    feedbackUrl,
  });

  sentSettlementEmails.set(dedupeKey, Date.now());
  logger.info('Planning final settlement thank-you email sent successfully', {
    eventId,
    authId,
    to: owner.email,
  });
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

const formatReminderLeadText = (hours) => {
  const n = Number(hours || 0);
  if (!Number.isFinite(n) || n <= 0) return 'soon';
  return `${n} hour${n === 1 ? '' : 's'}`;
};

const handleTicketEventReminderEmailRequested = async (payload) => {
  const eventId = String(payload?.eventId || '').trim();
  const authId = String(payload?.authId || '').trim();
  const eventTitle = String(payload?.eventTitle || 'Your event').trim() || 'Your event';
  const leadHours = Number(payload?.reminderOffsetHours || payload?.metadata?.offsetHours || 0);
  const dedupeKey = String(payload?.dedupeKey || `${eventId}:${authId}:${leadHours || 'na'}`).trim();

  if (!eventId || !authId) {
    logger.error('TICKET_EVENT_REMINDER_EMAIL_REQUESTED missing required fields', { payload });
    return;
  }

  pruneTicketReminderSentCache();
  if (dedupeKey && sentTicketReminderEmails.has(dedupeKey)) {
    logger.info('Skipping duplicate ticket reminder email request', { eventId, authId, dedupeKey });
    return;
  }

  const user = await fetchUserByAuthId(authId);
  const recipientEmail = String(user?.email || '').trim();
  if (!recipientEmail) {
    logger.warn('Unable to send ticket reminder email: user email not found', { eventId, authId });
    return;
  }

  await emailService.sendTicketEventReminderEmail(recipientEmail, {
    recipientName: user?.name || user?.fullName || user?.username || 'there',
    eventId,
    eventTitle,
    eventStartAt: payload?.eventStartAt || payload?.metadata?.eventStartAt || null,
    leadHours,
    actionUrl: payload?.actionUrl || '/user/ticket-management',
  });

  if (dedupeKey) {
    sentTicketReminderEmails.set(dedupeKey, Date.now());
  }

  logger.info('Ticket reminder email sent', {
    eventId,
    authId,
    leadTime: formatReminderLeadText(leadHours),
    to: recipientEmail,
  });
};

const handleTicketCancelledByUserEmailRequested = async (payload) => {
  const eventId = String(payload?.eventId || '').trim();
  const authId = String(payload?.authId || '').trim();
  const ticketId = String(payload?.ticketId || '').trim();
  const dedupeKey = String(payload?.dedupeKey || `${eventId}:${ticketId || 'na'}:${authId}`).trim();

  if (!eventId || !authId) {
    logger.error('TICKET_CANCELLED_BY_USER missing required fields for email', { payload });
    return;
  }

  pruneTicketCancellationSentCache();
  if (dedupeKey && sentTicketCancellationEmails.has(dedupeKey)) {
    logger.info('Skipping duplicate ticket cancellation email request', { eventId, authId, ticketId, dedupeKey });
    return;
  }

  const user = await fetchUserByAuthId(authId);
  const recipientEmail = String(user?.email || '').trim();
  if (!recipientEmail) {
    logger.warn('Unable to send ticket cancellation email: user email not found', { eventId, authId, ticketId });
    return;
  }

  await emailService.sendTicketCancellationUserEmail(recipientEmail, {
    recipientName: user?.name || user?.fullName || user?.username || 'there',
    eventId,
    eventTitle: payload?.eventTitle || 'Event',
    selectedDay: payload?.selectedDay || null,
    eventStartAt: payload?.eventStartAt || null,
    cancelledAt: payload?.cancelledAt || null,
    cancellationReason: payload?.cancellationReason || null,
    refundAmountInInr: payload?.refundAmountInInr || 0,
    refundPercent: payload?.refundPercent || 0,
    timelineLabel: payload?.timelineLabel || null,
    actionUrl: payload?.actionUrl || '/user/my-events',
  });

  if (dedupeKey) {
    sentTicketCancellationEmails.set(dedupeKey, Date.now());
  }

  logger.info('Ticket cancellation email sent', {
    eventId,
    authId,
    ticketId,
    to: recipientEmail,
  });
};

const handlePlanningEventCancelledForGuests = async (payload) => {
  const eventId = String(payload?.eventId || '').trim();
  const eventTitle = String(payload?.eventTitle || '').trim() || 'Event';
  const recipientAuthIds = Array.isArray(payload?.recipientAuthIds)
    ? Array.from(new Set(payload.recipientAuthIds.map((id) => String(id || '').trim()).filter(Boolean)))
    : [];

  if (!eventId || recipientAuthIds.length === 0) {
    logger.warn('PLANNING_EVENT_CANCELLED_FOR_GUESTS missing required fields', {
      eventId,
      recipients: recipientAuthIds.length,
    });
    return;
  }

  prunePlanningCancellationSentCache();

  let sentCount = 0;
  let skippedCount = 0;
  let failedCount = 0;

  for (const authId of recipientAuthIds) {
    const dedupeKey = `${eventId}:${authId}`;
    if (sentPlanningCancellationEmails.has(dedupeKey)) {
      skippedCount += 1;
      continue;
    }

    try {
      const user = await fetchUserByAuthId(authId);
      const recipientEmail = String(user?.email || '').trim();
      if (!recipientEmail) {
        failedCount += 1;
        logger.warn('Skipping cancellation email because recipient email was not found', { eventId, authId });
        continue;
      }

      await emailService.sendPlanningEventCancelledGuestEmail(recipientEmail, {
        recipientName: user?.name || user?.fullName || user?.username || 'there',
        eventId,
        eventTitle,
        eventDate: payload?.eventDate || null,
        eventLocation: payload?.eventLocation || null,
        cancellationReason: payload?.cancellationReason || null,
        refundTimelineLabel: payload?.refundTimelineLabel || null,
        actionUrl: payload?.actionUrl || null,
      });

      sentPlanningCancellationEmails.set(dedupeKey, Date.now());
      sentCount += 1;
    } catch (error) {
      failedCount += 1;
      logger.warn('Failed to send planning cancellation email to guest', {
        eventId,
        authId,
        message: error?.message || String(error),
      });
    }
  }

  logger.info('Processed PLANNING_EVENT_CANCELLED_FOR_GUESTS email batch', {
    eventId,
    requestedRecipients: recipientAuthIds.length,
    sentCount,
    skippedCount,
    failedCount,
  });
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
    name: o?.name || null,
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

const fetchUsersByRole = async ({ role = 'USER', pageSize = 100 } = {}) => {
  const users = [];
  const maxPages = 200;
  const normalizedRole = String(role || 'USER').trim().toUpperCase();
  const safePageSize = Math.max(1, Math.min(100, Number(pageSize) || 100));

  for (let page = 1; page <= maxPages; page += 1) {
    const response = await axios.get(`${userServiceUrl}/`, {
      timeout: httpTimeoutMs,
      headers: buildSystemHeaders(),
      params: {
        role: normalizedRole,
        page,
        limit: safePageSize,
      },
    });

    const rows = Array.isArray(response?.data?.data) ? response.data.data : [];
    users.push(...rows);

    const totalPagesRaw = Number(response?.data?.pagination?.totalPages || page);
    const totalPages = Number.isFinite(totalPagesRaw) && totalPagesRaw > 0 ? totalPagesRaw : page;

    if (rows.length === 0 || page >= totalPages) break;
  }

  return users;
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

const fetchTicketByIdForUser = async (ticketId, user) => {
  const response = await axios.get(`${eventServiceUrl}/tickets/my/${encodeURIComponent(ticketId)}`, {
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
  const normalizedOrderType = String(orderType || '').trim().toUpperCase();
  const notes = payload?.notes || {};

  if (normalizedOrderType === 'PLANNING EVENT REMAINING FEE') {
    logger.info('Skipping generic payment-success email for remaining fee payment', {
      eventId,
      authId,
      orderType: normalizedOrderType,
    });
    return;
  }

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
  let ticketId = String(notes?.ticketId || payload?.ticketId || '').trim() || null;
  let ticketQuantity = null;
  let ticketTierSummary = null;
  let ticketLink = null;

  if (normalizedOrderType === 'PROMOTE EVENT') {
    const promote = await fetchPromoteByEventIdForUser(eventId, user);
    if (!promote) {
      logger.error('Unable to send payment email: promote record not found', { authId, eventId, orderType });
      return;
    }

    eventTitle = promote?.eventTitle || eventTitle;
    eventLocation = promote?.venue?.locationName || eventLocation;
    eventStatus = promote?.eventStatus || eventStatus;
  } else if (normalizedOrderType === 'TICKET SALE') {
    const frontendBaseUrl = String(process.env.FRONTEND_URL || '').replace(/\/$/, '');
    if (ticketId) {
      try {
        const ticket = await fetchTicketByIdForUser(ticketId, user);
        if (ticket) {
          eventTitle = ticket?.eventTitle || eventTitle;
          eventLocation = ticket?.venue?.locationName || eventLocation;
          eventStatus = ticket?.ticketStatus || 'TICKET CONFIRMED';
          ticketQuantity = Number(ticket?.tickets?.noOfTickets || 0) || null;
          const tiers = Array.isArray(ticket?.tickets?.tiers) ? ticket.tickets.tiers : [];
          ticketTierSummary = tiers.length
            ? tiers.map((tier) => `${tier?.name || 'Tier'} x${Number(tier?.noOfTickets || 0)}`).join(', ')
            : null;
        }
      } catch (error) {
        logger.warn('Unable to fetch ticket details for payment email', {
          ticketId,
          eventId,
          authId,
          message: error?.message || String(error),
        });
      }
    }

    eventTitle = notes?.eventTitle || eventTitle;
    eventLocation = notes?.eventLocation || eventLocation;
    ticketQuantity = ticketQuantity || Number(notes?.ticketQuantity || 0) || null;

    if (!ticketTierSummary && notes?.ticketTiers) {
      try {
        const parsedTiers = JSON.parse(notes.ticketTiers);
        if (Array.isArray(parsedTiers) && parsedTiers.length > 0) {
          ticketTierSummary = parsedTiers
            .map((tier) => `${tier?.name || 'Tier'} x${Number(tier?.noOfTickets || tier?.quantity || 0)}`)
            .join(', ');
        }
      } catch (_) {
        ticketTierSummary = null;
      }
    }

    ticketLink = notes?.ticketLink || (ticketId && frontendBaseUrl
      ? `${frontendBaseUrl}/user/ticket/${encodeURIComponent(ticketId)}`
      : null);
    eventStatus = 'TICKET CONFIRMED';
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
    ticketId,
    ticketQuantity,
    ticketTierSummary,
    ticketLink,
  });

  sentPaymentEmails.set(dedupeKey, Date.now());
  logger.info('Payment success email sent successfully', { authId, eventId, to: recipientEmail });
};

const handlePaymentRefundSuccessEvent = async (payload) => {
  const {
    eventId,
    authId,
    paymentOrderId,
    transactionId,
    razorpayRefundId,
    amount,
    currency,
    refundedAt,
    orderType,
  } = payload || {};

  if (!eventId || !authId) {
    logger.error('PAYMENT_REFUND_SUCCESS event missing required fields', { eventId, authId });
    return;
  }

  const dedupeKey = razorpayRefundId || paymentOrderId || `${eventId}:${authId}:${transactionId || refundedAt || ''}`;
  prunePaymentRefundSentCache();
  if (sentPaymentRefundEmails.has(dedupeKey)) {
    logger.info('Skipping duplicate PAYMENT_REFUND_SUCCESS email', { eventId, authId, dedupeKey });
    return;
  }

  const user = await fetchUserByAuthId(authId);
  const recipientEmail = user?.email;
  if (!recipientEmail) {
    logger.error('Unable to send refund email: user email not found', { authId, eventId });
    return;
  }

  const recipientName = user?.name || user?.fullName || 'there';
  let eventTitle = 'your event';
  let eventLocation = 'TBA';
  let eventStatus = 'REFUNDED';

  try {
    const promote = await fetchPromoteByEventIdForUser(eventId, user);
    if (promote) {
      eventTitle = promote?.eventTitle || eventTitle;
      eventLocation = promote?.venue?.locationName || eventLocation;
      eventStatus = promote?.eventStatus || eventStatus;
    }
  } catch (error) {
    if (Number(error?.response?.status) !== 404) {
      logger.warn('Unable to fetch promote details for refund email', {
        eventId,
        authId,
        message: error?.message || String(error),
      });
    }
  }

  if (eventTitle === 'your event') {
    try {
      const planning = await fetchPlanningByEventIdForUser(eventId, user);
      if (planning) {
        eventTitle = planning?.eventTitle || eventTitle;
        eventLocation = planning?.location?.name || planning?.location || eventLocation;
        eventStatus = planning?.status || eventStatus;
      }
    } catch (error) {
      if (Number(error?.response?.status) !== 404) {
        logger.warn('Unable to fetch planning details for refund email', {
          eventId,
          authId,
          message: error?.message || String(error),
        });
      }
    }
  }

  await emailService.sendPaymentRefundSuccessEmail(recipientEmail, {
    recipientName,
    eventId,
    eventTitle,
    eventLocation,
    eventStatus,
    amount,
    currency,
    transactionId,
    refundedAt,
    razorpayRefundId,
    orderType,
  });

  sentPaymentRefundEmails.set(dedupeKey, Date.now());
  logger.info('Payment refund success email sent successfully', {
    authId,
    eventId,
    to: recipientEmail,
    dedupeKey,
  });
};

const handleUserRevenuePayoutSuccessEvent = async (payload) => {
  const eventId = String(payload?.eventId || '').trim();
  const userAuthId = String(payload?.userAuthId || payload?.authId || '').trim();
  const payoutAmountPaiseRaw = Number(payload?.amount ?? payload?.payoutAmountPaise ?? 0);
  const payoutAmountPaise = Number.isFinite(payoutAmountPaiseRaw) && payoutAmountPaiseRaw > 0
    ? Math.round(payoutAmountPaiseRaw)
    : 0;

  if (!eventId || !userAuthId || payoutAmountPaise <= 0) {
    logger.error('USER_REVENUE_PAYOUT_SUCCESS missing required fields', {
      eventId,
      userAuthId,
      payoutAmountPaise,
    });
    return;
  }

  pruneUserRevenuePayoutSentCache();
  const dedupeKey = String(
    payload?.payoutId
    || payload?.razorpayTransferId
    || payload?.transactionRef
    || `${eventId}:${userAuthId}:${payload?.paidAt || payoutAmountPaise}`
  ).trim();
  if (sentUserRevenuePayoutEmails.has(dedupeKey)) {
    logger.info('Skipping duplicate user revenue payout email', { eventId, userAuthId, dedupeKey });
    return;
  }

  const user = await fetchUserByAuthId(userAuthId);
  const recipientEmail = user?.email;
  if (!recipientEmail) {
    logger.error('Unable to send generated revenue payout email: user email not found', { userAuthId, eventId });
    return;
  }

  const requestUser = {
    ...user,
    authId: userAuthId,
    role: user?.role || 'USER',
  };

  let eventTitle = 'Event';
  let eventLocation = 'TBA';

  try {
    const promote = await fetchPromoteByEventIdForUser(eventId, requestUser);
    if (promote) {
      eventTitle = promote?.eventTitle || eventTitle;
      eventLocation = promote?.venue?.locationName || eventLocation;
    }
  } catch (error) {
    if (Number(error?.response?.status) !== 404) {
      logger.warn('Unable to fetch promote details for payout email', {
        eventId,
        userAuthId,
        message: error?.message || String(error),
      });
    }
  }

  if (eventTitle === 'Event') {
    try {
      const planning = await fetchPlanningByEventIdForUser(eventId, requestUser);
      if (planning) {
        eventTitle = planning?.eventTitle || eventTitle;
        eventLocation = planning?.location?.name || planning?.location || eventLocation;
      }
    } catch (error) {
      if (Number(error?.response?.status) !== 404) {
        logger.warn('Unable to fetch planning details for payout email', {
          eventId,
          userAuthId,
          message: error?.message || String(error),
        });
      }
    }
  }

  await emailService.sendUserGeneratedRevenueReceivedEmail(recipientEmail, {
    recipientName: user?.name || user?.username || 'there',
    eventId,
    eventTitle,
    eventLocation,
    payoutAmountPaise,
    currency: payload?.currency || 'INR',
    payoutMode: payload?.payoutMode || null,
    paidAt: payload?.paidAt || null,
    transactionRef: payload?.razorpayTransferId || payload?.transactionRef || payload?.payoutId || null,
  });

  sentUserRevenuePayoutEmails.set(dedupeKey, Date.now());
  logger.info('Generated revenue payout email sent successfully', {
    eventId,
    userAuthId,
    to: recipientEmail,
    dedupeKey,
  });
};

const handlePromotionEmailBlastRequested = async (payload) => {
  const eventId = String(payload?.eventId || '').trim();
  if (!eventId) {
    logger.error('PROMOTION_EMAIL_BLAST_REQUESTED missing eventId', { payload });
    return;
  }

  const eventType = String(payload?.eventType || '').trim().toLowerCase();
  const requestId = String(payload?.requestId || '').trim();
  const dedupeKey = requestId || `${eventType || 'unknown'}:${eventId}:${String(payload?.requestedAt || '')}`;

  prunePromotionEmailBlastSentCache();
  if (sentPromotionEmailBlastRequests.has(dedupeKey)) {
    logger.info('Skipping duplicate promotion email blast request', { dedupeKey, eventId, eventType });
    return;
  }

  const systemRequester = {
    authId: 'email-service',
    id: 'email-service',
    email: process.env.FROM_EMAIL || 'noreply@okkazo.com',
    name: 'email-service',
    role: 'MANAGER',
  };

  let eventTitle = String(payload?.eventTitle || '').trim() || 'Event';
  let eventDescription = String(payload?.eventDescription || '').trim() || '';
  let eventLocation = String(payload?.eventLocation || '').trim() || 'TBA';
  let eventDate = payload?.eventDate || null;
  let eventBannerUrl = String(payload?.eventBannerUrl || '').trim() || null;

  try {
    if (eventType === 'planning') {
      const planning = await fetchPlanningByEventIdForUser(eventId, systemRequester);
      if (planning) {
        eventTitle = planning?.eventTitle || eventTitle;
        eventDescription = planning?.eventDescription || eventDescription;
        eventLocation = planning?.location?.name || planning?.location || eventLocation;
        eventDate = planning?.schedule?.startAt || planning?.eventDate || eventDate;
        eventBannerUrl = planning?.eventBanner?.url || planning?.eventBanner || eventBannerUrl;
      }
    } else if (eventType === 'promote') {
      const promote = await fetchPromoteByEventIdForUser(eventId, systemRequester);
      if (promote) {
        eventTitle = promote?.eventTitle || eventTitle;
        eventDescription = promote?.eventDescription || eventDescription;
        eventLocation = promote?.venue?.locationName || eventLocation;
        eventDate = promote?.schedule?.startAt || eventDate;
        eventBannerUrl = promote?.eventBanner?.url || promote?.eventBanner || eventBannerUrl;
      }
    }
  } catch (error) {
    logger.warn('Failed to hydrate event details for promotion email blast', {
      eventId,
      eventType,
      message: error?.message || String(error),
    });
  }

  const allUsers = await fetchUsersByRole({ role: 'USER', pageSize: 100 });

  const seenEmails = new Set();
  const recipients = [];
  for (const row of allUsers) {
    if (row?.isActive === false) continue;

    const email = String(row?.email || '').trim();
    if (!email) continue;

    const emailKey = email.toLowerCase();
    if (seenEmails.has(emailKey)) continue;
    seenEmails.add(emailKey);

    recipients.push({
      email,
      name: String(row?.name || row?.fullName || row?.username || '').trim() || 'there',
    });
  }

  if (recipients.length === 0) {
    logger.warn('No USER recipients found for promotion email blast', { eventId, eventType, dedupeKey });
    sentPromotionEmailBlastRequests.set(dedupeKey, Date.now());
    return;
  }

  const frontendBaseUrl = String(process.env.FRONTEND_URL || '').replace(/\/$/, '');
  const eventUrl = frontendBaseUrl
    ? `${frontendBaseUrl}/user/event/${encodeURIComponent(eventId)}`
    : null;

  const batchSize = Math.max(1, Number(process.env.PROMOTION_EMAIL_BLAST_BATCH_SIZE || 25));
  let sentCount = 0;
  let failedCount = 0;

  for (let idx = 0; idx < recipients.length; idx += batchSize) {
    const chunk = recipients.slice(idx, idx + batchSize);
    const chunkResult = await Promise.allSettled(
      chunk.map((recipient) => emailService.sendPromotionEmailBlastEmail(recipient.email, {
        recipientName: recipient.name,
        eventId,
        eventTitle,
        eventDescription,
        eventLocation,
        eventDate,
        eventUrl,
        eventBannerUrl,
      }))
    );

    for (const result of chunkResult) {
      if (result.status === 'fulfilled') sentCount += 1;
      else failedCount += 1;
    }
  }

  sentPromotionEmailBlastRequests.set(dedupeKey, Date.now());
  logger.info('Promotion email blast completed', {
    eventId,
    eventType,
    dedupeKey,
    recipients: recipients.length,
    sentCount,
    failedCount,
  });
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
    case 'PASSWORD_CHANGED':
      await handlePasswordChanged(payload);
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
    case 'PAYMENT_REFUND_SUCCESS':
      if (topic === paymentTopic) {
        await handlePaymentRefundSuccessEvent(payload);
      }
      break;
    case 'USER_REVENUE_PAYOUT_SUCCESS':
      if (topic === paymentTopic || topic === eventTopic) {
        await handleUserRevenuePayoutSuccessEvent(payload);
      }
      break;
    case 'PROMOTION_EMAIL_BLAST_REQUESTED':
      if (topic === eventTopic) {
        await handlePromotionEmailBlastRequested(payload);
      }
      break;
    case 'TICKET_EVENT_REMINDER_EMAIL_REQUESTED':
      if (topic === eventTopic) {
        await handleTicketEventReminderEmailRequested(payload);
      }
      break;
    case 'TICKET_CANCELLED_BY_USER':
      if (topic === eventTopic) {
        await handleTicketCancelledByUserEmailRequested(payload);
      }
      break;
    case 'PLANNING_EVENT_CANCELLED_FOR_GUESTS':
      if (topic === eventTopic) {
        await handlePlanningEventCancelledForGuests(payload);
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
    case 'PLANNING_REMAINING_PAYMENT_CONFIRMED':
      if (topic === eventTopic) {
        await handlePlanningRemainingPaymentConfirmed(payload);
      }
      break;
    // Payment events that are useful for audit/analytics but do not trigger emails here
    case 'PAYMENT_ORDER_CREATED':
    case 'PAYMENT_TRANSACTION_UPDATED':
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

const handlePasswordChanged = async (event) => {
  try {
    const { authId, email, username, changedAt } = event;

    if (!authId || !email) {
      logger.error('PASSWORD_CHANGED event missing required fields', { event });
      return;
    }

    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(email)) {
      logger.error('PASSWORD_CHANGED event has invalid email format', { email });
      return;
    }

    logger.info('Processing PASSWORD_CHANGED event', { authId, email });
    await emailService.sendPasswordChangedEmail(email, {
      recipientName: username || 'there',
      changedAt: changedAt || null,
      authId,
    });
    logger.info('Password changed confirmation email sent successfully', { authId, email });
  } catch (error) {
    logger.error('Error handling PASSWORD_CHANGED event:', error);
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
