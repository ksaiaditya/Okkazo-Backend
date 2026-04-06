const Planning = require('../models/Planning');
const Promote = require('../models/Promote');
const UserEventTicket = require('../models/UserEventTicket');
const axios = require('axios');
const { CATEGORY, STATUS } = require('../utils/planningConstants');
const { ADMIN_DECISION_STATUS, PROMOTE_STATUS } = require('../utils/promoteConstants');
const { USER_TICKET_STATUS, USER_TICKET_VERIFICATION_STATUS } = require('../utils/ticketConstants');
const createApiError = require('../utils/ApiError');
const logger = require('../utils/logger');
const { fetchUserByAuthId } = require('./userServiceClient');
const { signTicketQrToken, verifyTicketQrToken } = require('../utils/ticketQrToken');

const ORDER_SERVICE_URL = (process.env.ORDER_SERVICE_URL || 'http://order-service:8087').replace(/\/$/, '');
const DAY_KEY_RE = /^\d{4}-\d{2}-\d{2}$/;
const MAX_SCAN_HISTORY = 200;

const toPositiveInt = (value, fallback) => {
  const n = Number.parseInt(value, 10);
  if (!Number.isFinite(n) || n < 1) return fallback;
  return n;
};

const roundToPaise = (amountInInr) => {
  const n = Number(amountInInr || 0);
  if (!Number.isFinite(n) || n < 0) return 0;
  return Math.round(n * 100);
};

const normalizeTierNameKey = (value) => String(value || '').trim().toLowerCase().replace(/\s+/g, ' ');

const TIER_NAME_ALIASES = new Map([
  ['general admission', 'general'],
  ['general', 'general'],
]);

const getTierLookupKeys = (name) => {
  const base = normalizeTierNameKey(name);
  if (!base) return [];

  const alias = TIER_NAME_ALIASES.get(base);
  if (!alias || alias === base) return [base];
  return [base, alias];
};

const resolveRequestedTier = (tierMap, requestedName) => {
  const keys = getTierLookupKeys(requestedName);
  for (const key of keys) {
    const matched = tierMap.get(key);
    if (matched) return matched;
  }
  return null;
};

const normalizeRequestedTiers = (tiers) => {
  if (!Array.isArray(tiers)) return [];

  return tiers
    .map((tier) => ({
      name: String(tier?.name || '').trim(),
      quantity: Number(tier?.quantity || 0),
    }))
    .filter((tier) => tier.name && Number.isFinite(tier.quantity) && tier.quantity > 0)
    .map((tier) => ({ ...tier, quantity: Math.floor(tier.quantity) }));
};

const normalizePromotionTypes = (promotionType) => {
  if (!Array.isArray(promotionType)) return [];

  return promotionType
    .map((promo) => String(promo || '').trim())
    .filter(Boolean);
};

const normalizeDayKey = (value) => {
  if (value == null) return '';

  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) return '';
    return value.toISOString().slice(0, 10);
  }

  const raw = String(value || '').trim();
  if (!raw) return '';
  const day = raw.includes('T') ? raw.slice(0, 10) : raw;
  return DAY_KEY_RE.test(day) ? day : '';
};

const resolveSelectedDayFromSchedule = (schedule) => normalizeDayKey(schedule?.startAt);

const resolveTicketSelectedDay = ({ selectedDay, schedule } = {}) => {
  const explicit = normalizeDayKey(selectedDay);
  if (explicit) return explicit;
  return resolveSelectedDayFromSchedule(schedule) || null;
};

const appendTicketScanHistory = (existing, entry) => {
  const rows = Array.isArray(existing) ? [...existing] : [];
  rows.push(entry);
  if (rows.length > MAX_SCAN_HISTORY) {
    return rows.slice(rows.length - MAX_SCAN_HISTORY);
  }
  return rows;
};

const mapScanHistoryForApi = (scanHistory) => {
  if (!Array.isArray(scanHistory)) return [];
  return scanHistory
    .map((row) => {
      const scannedAt = row?.scannedAt ? new Date(row.scannedAt) : null;
      return {
        scannedAt: scannedAt && !Number.isNaN(scannedAt.getTime()) ? scannedAt.toISOString() : null,
        scannedByAuthId: String(row?.scannedByAuthId || '').trim() || null,
        scannedByRole: String(row?.scannedByRole || '').trim() || null,
        outcome: String(row?.outcome || '').trim() || null,
      };
    })
    .filter((row) => row.scannedAt && row.outcome);
};

const toDisplayTicketStatus = (ticket) => {
  const ticketStatus = String(ticket?.ticketStatus || '').trim().toUpperCase();
  const verificationStatus = String(ticket?.verification?.status || '').trim().toUpperCase();

  if (ticketStatus === USER_TICKET_STATUS.SUCCESS) {
    if (verificationStatus === USER_TICKET_VERIFICATION_STATUS.VERIFIED) {
      return 'Checked In';
    }
    return 'Confirmed';
  }

  if (ticketStatus === USER_TICKET_STATUS.CANCELED || ticketStatus === USER_TICKET_STATUS.EXPIRED) {
    return 'Cancelled';
  }

  return 'Pending';
};

const resolveTicketTypeLabel = (ticket) => {
  const tiers = Array.isArray(ticket?.tickets?.tiers) ? ticket.tickets.tiers : [];
  const tierNames = tiers
    .map((tier) => String(tier?.name || '').trim())
    .filter(Boolean);

  if (tierNames.length > 0) {
    return tierNames.join(', ');
  }

  const ticketType = String(ticket?.tickets?.ticketType || '').trim().toLowerCase();
  if (ticketType === 'free') return 'General Admission';
  return 'Ticket';
};

const normalizeGuestName = (user, authId) => {
  const candidates = [
    user?.name,
    user?.fullName,
    user?.username,
  ];

  for (const value of candidates) {
    const text = String(value || '').trim();
    if (text) return text;
  }

  const suffix = String(authId || '').trim();
  if (!suffix) return 'Guest';
  return `Guest ${suffix.slice(0, 6)}`;
};

const normalizeGuestEmail = (user) => {
  const email = String(user?.email || user?.mail || '').trim();
  return email || null;
};

const normalizePlanningDayWiseAllocations = ({ tickets, tiers } = {}) => {
  const rows = Array.isArray(tickets?.dayWiseAllocations) ? tickets.dayWiseAllocations : [];
  if (rows.length === 0) return [];

  const priceByTierName = new Map(
    (Array.isArray(tiers) ? tiers : [])
      .map((tier) => [normalizeTierNameKey(tier?.name), Number(tier?.price || 0)])
      .filter(([name]) => Boolean(name))
  );

  return rows
    .map((row) => {
      const day = normalizeDayKey(row?.day);
      if (!day) return null;

      const ticketCountRaw = Number(row?.ticketCount || 0);
      const ticketCount = Number.isFinite(ticketCountRaw) && ticketCountRaw > 0 ? ticketCountRaw : 0;

      const tierBreakdown = (Array.isArray(row?.tierBreakdown) ? row.tierBreakdown : [])
        .map((tierRow) => {
          const name = String(tierRow?.tierName || tierRow?.name || '').trim();
          if (!name) return null;

          const availableRaw = Number(tierRow?.ticketCount || tierRow?.quantity || 0);
          const available = Number.isFinite(availableRaw) && availableRaw > 0 ? availableRaw : 0;
          const price = Number(priceByTierName.get(normalizeTierNameKey(name)) || 0);

          return {
            name,
            available,
            price,
          };
        })
        .filter((tier) => tier && tier.available > 0);

      return {
        day,
        ticketCount,
        tierBreakdown,
      };
    })
    .filter(Boolean)
    .sort((a, b) => String(a.day).localeCompare(String(b.day)));
};

const ensureTicketSalesWindow = (ticketAvailability) => {
  const now = new Date();
  const startAt = ticketAvailability?.startAt ? new Date(ticketAvailability.startAt) : null;
  const endAt = ticketAvailability?.endAt ? new Date(ticketAvailability.endAt) : null;

  if (!startAt || Number.isNaN(startAt.getTime()) || !endAt || Number.isNaN(endAt.getTime())) {
    throw createApiError(409, 'Ticket availability window is not configured for this event');
  }

  if (now < startAt) {
    throw createApiError(409, 'Ticket sales have not started yet for this event');
  }

  if (now > endAt) {
    throw createApiError(409, 'Ticket sales have ended for this event');
  }
};

const resolveEventForPurchase = async (eventId) => {
  const trimmedEventId = String(eventId || '').trim();
  if (!trimmedEventId) {
    throw createApiError(400, 'Event ID is required');
  }

  const now = new Date();

  const planning = await Planning.findOne({
    eventId: trimmedEventId,
    category: CATEGORY.PUBLIC,
    platformFeePaid: true,
    status: STATUS.CONFIRMED,
    'ticketAvailability.startAt': { $lte: now },
    'ticketAvailability.endAt': { $gte: now },
  }).lean();

  if (planning) {
    const planningTiers = (Array.isArray(planning?.tickets?.tiers) ? planning.tickets.tiers : [])
      .map((tier) => ({
        name: String(tier?.tierName || '').trim(),
        available: Number(tier?.ticketCount || 0),
        price: Number(tier?.ticketPrice || 0),
      }))
      .filter((tier) => tier.name && tier.available > 0);

    const planningDayWiseAllocations = normalizePlanningDayWiseAllocations({
      tickets: planning?.tickets,
      tiers: planningTiers,
    });

    return {
      source: 'planning-public',
      event: planning,
      ticketType: String(planning?.tickets?.ticketType || 'free').toLowerCase() === 'paid' ? 'paid' : 'free',
      totalAvailable: Number(planning?.tickets?.totalTickets || 0),
      tiers: planningTiers,
      dayWiseAllocations: planningDayWiseAllocations,
      venue: {
        locationName: planning?.location?.name || 'TBA',
        latitude: planning?.location?.latitude ?? null,
        longitude: planning?.location?.longitude ?? null,
      },
      schedule: planning?.schedule || null,
      ticketAvailability: planning?.ticketAvailability || null,
      eventTitle: planning?.eventTitle || 'Event',
      eventDescription: planning?.eventDescription || '',
      eventField: planning?.eventField || null,
      eventBanner: planning?.eventBanner || null,
      selectedPromotions: normalizePromotionTypes(planning?.promotionType),
    };
  }

  const promote = await Promote.findOne({
    eventId: trimmedEventId,
    platformFeePaid: true,
    eventStatus: { $nin: [PROMOTE_STATUS.PAYMENT_REQUIRED, PROMOTE_STATUS.COMPLETE] },
    'adminDecision.status': ADMIN_DECISION_STATUS.APPROVED,
    'ticketAvailability.startAt': { $lte: now },
    'ticketAvailability.endAt': { $gte: now },
  }).lean();

  if (promote) {
    return {
      source: 'promote',
      event: promote,
      ticketType: String(promote?.tickets?.ticketType || 'free').toLowerCase() === 'paid' ? 'paid' : 'free',
      totalAvailable: Number(promote?.tickets?.noOfTickets || 0),
      tiers: (Array.isArray(promote?.tickets?.tiers) ? promote.tickets.tiers : [])
        .map((tier) => ({
          name: String(tier?.name || '').trim(),
          available: Number(tier?.quantity || 0),
          price: Number(tier?.price || 0),
        }))
        .filter((tier) => tier.name && tier.available > 0),
      venue: {
        locationName: promote?.venue?.locationName || 'TBA',
        latitude: promote?.venue?.latitude ?? null,
        longitude: promote?.venue?.longitude ?? null,
      },
      schedule: promote?.schedule || null,
      ticketAvailability: promote?.ticketAvailability || null,
      eventTitle: promote?.eventTitle || 'Event',
      eventDescription: promote?.eventDescription || '',
      eventField: promote?.eventField || null,
      eventBanner: promote?.eventBanner || null,
    };
  }

  throw createApiError(404, 'Ticket event not found or not available for sale');
};

const computeSelection = ({ ticketType, totalAvailable, tiers, requestedTiers, selectedDay, dayWiseAllocations }) => {
  const normalizedRequested = normalizeRequestedTiers(requestedTiers);
  if (!normalizedRequested.length) {
    throw createApiError(400, 'Select at least one ticket tier with quantity');
  }

  const normalizedType = String(ticketType || '').trim().toLowerCase();
  const baseTiers = Array.isArray(tiers) ? tiers : [];
  const dayRows = Array.isArray(dayWiseAllocations) ? dayWiseAllocations : [];

  const normalizedSelectedDay = normalizeDayKey(selectedDay);

  let effectiveTotalAvailable = Number(totalAvailable || 0);
  let effectiveTiers = [...baseTiers];

  if (dayRows.length > 0) {
    if (!normalizedSelectedDay) {
      throw createApiError(400, 'Please select an event date before booking tickets');
    }

    const selectedDayRow = dayRows.find((row) => normalizeDayKey(row?.day) === normalizedSelectedDay);
    if (!selectedDayRow) {
      throw createApiError(400, 'Selected date is not available for ticket booking');
    }

    effectiveTotalAvailable = Number(selectedDayRow?.ticketCount || 0);

    if (Array.isArray(selectedDayRow?.tierBreakdown) && selectedDayRow.tierBreakdown.length > 0) {
      const priceByTierName = new Map(
        baseTiers
          .map((tier) => [normalizeTierNameKey(tier?.name), Number(tier?.price || 0)])
          .filter(([name]) => Boolean(name))
      );

      effectiveTiers = selectedDayRow.tierBreakdown
        .map((tier) => {
          const name = String(tier?.name || tier?.tierName || '').trim();
          if (!name) return null;

          const availableRaw = Number(tier?.available ?? tier?.ticketCount ?? tier?.quantity ?? 0);
          const available = Number.isFinite(availableRaw) && availableRaw > 0 ? availableRaw : 0;
          const price = Number(priceByTierName.get(normalizeTierNameKey(name)) || tier?.price || 0);

          return {
            name,
            available,
            price,
          };
        })
        .filter((tier) => tier && tier.name && tier.available > 0);
    }
  }

  // Free events can be configured without explicit tiers in DB.
  // In that case, treat requests as selecting the implicit "General" tier.
  const tiersForMatching =
    normalizedType === 'free' && effectiveTiers.length === 0
      ? [{ name: 'General', available: Number(effectiveTotalAvailable || 0), price: 0 }]
      : effectiveTiers;

  const tierMap = new Map();
  for (const tier of tiersForMatching) {
    for (const key of getTierLookupKeys(tier?.name)) {
      if (!tierMap.has(key)) {
        tierMap.set(key, tier);
      }
    }
  }

  const selectedTiers = [];

  let totalQuantity = 0;
  let totalAmountInPaise = 0;

  for (const requestTier of normalizedRequested) {
    const matchedTier = resolveRequestedTier(tierMap, requestTier.name);

    if (!matchedTier) {
      throw createApiError(400, `Ticket tier not found: ${requestTier.name}`);
    }

    if (requestTier.quantity > Number(matchedTier.available || 0)) {
      throw createApiError(409, `Only ${matchedTier.available} tickets left in tier ${matchedTier.name}`);
    }

    const unitPrice = ticketType === 'paid' ? Number(matchedTier.price || 0) : 0;
    const lineAmountInPaise = roundToPaise(unitPrice) * requestTier.quantity;

    selectedTiers.push({
      name: matchedTier.name,
      noOfTickets: requestTier.quantity,
      price: unitPrice,
    });

    totalQuantity += requestTier.quantity;
    totalAmountInPaise += lineAmountInPaise;
  }

  if (totalQuantity < 1) {
    throw createApiError(400, 'Ticket quantity must be at least 1');
  }

  if (totalQuantity > Number(effectiveTotalAvailable || 0)) {
    throw createApiError(409, 'Not enough tickets available for this event');
  }

  return {
    selectedTiers,
    totalQuantity,
    totalAmountInPaise,
    selectedDay: normalizedSelectedDay || null,
  };
};

const mapTicketForFrontend = (ticket) => {
  if (!ticket) return null;

  const verificationStatus = String(ticket?.verification?.status || '').trim().toUpperCase() === USER_TICKET_VERIFICATION_STATUS.VERIFIED
    ? USER_TICKET_VERIFICATION_STATUS.VERIFIED
    : USER_TICKET_VERIFICATION_STATUS.PENDING;

  const selectedDay = resolveTicketSelectedDay({
    selectedDay: ticket?.tickets?.selectedDay,
    schedule: ticket?.schedule,
  });

  const normalizedTickets = {
    ...(ticket?.tickets || {}),
    selectedDay: selectedDay || null,
  };

  const qrToken = signTicketQrToken({
    ticketId: ticket.ticketId,
    eventId: ticket.eventId,
    userAuthId: ticket.userAuthId,
  });

  return {
    ticketId: ticket.ticketId,
    eventId: ticket.eventId,
    eventSource: ticket.eventSource,
    eventTitle: ticket.eventTitle,
    eventDescription: ticket.eventDescription,
    eventField: ticket.eventField,
    eventBanner: ticket.eventBanner,
    venue: ticket.venue,
    schedule: ticket.schedule,
    ticketAvailability: ticket.ticketAvailability,
    tickets: normalizedTickets,
    selectedDay,
    isPaid: Boolean(ticket.isPaid),
    ticketStatus: ticket.ticketStatus,
    verification: {
      status: verificationStatus,
      verifiedAt: ticket?.verification?.verifiedAt || null,
      verifiedByAuthId: ticket?.verification?.verifiedByAuthId || null,
      lastScannedAt: ticket?.verification?.lastScannedAt || null,
      scanCount: Number(ticket?.verification?.scanCount || 0),
      scanHistory: mapScanHistoryForApi(ticket?.verification?.scanHistory),
    },
    paidAt: ticket.paidAt,
    createdAt: ticket.createdAt,
    qrToken,
    // Kept for compatibility with existing frontend code paths.
    qrPayload: qrToken,
  };
};

const normalizePlanningTickets = (tickets) => {
  const noOfTickets = Number(tickets?.totalTickets || 0);
  const ticketType = String(tickets?.ticketType || '').trim().toLowerCase() === 'paid' ? 'paid' : 'free';
  const tiers = Array.isArray(tickets?.tiers)
    ? tickets.tiers
      .map((tier) => ({
        name: String(tier?.tierName || '').trim(),
        noOfTickets: Number(tier?.ticketCount || 0),
        price: Number(tier?.ticketPrice || 0),
      }))
      .filter((tier) => tier.name && tier.noOfTickets > 0)
    : [];

  const dayWiseAllocations = normalizePlanningDayWiseAllocations({
    tickets,
    tiers,
  });

  return {
    noOfTickets,
    ticketType,
    tiers,
    dayWiseAllocations,
  };
};

const normalizePromoteTickets = (tickets) => {
  const noOfTickets = Number(tickets?.noOfTickets || 0);
  const ticketType = String(tickets?.ticketType || '').trim().toLowerCase() === 'paid' ? 'paid' : 'free';
  const tiers = Array.isArray(tickets?.tiers)
    ? tickets.tiers
      .map((tier) => ({
        name: String(tier?.name || '').trim(),
        noOfTickets: Number(tier?.quantity || 0),
        price: Number(tier?.price || 0),
      }))
      .filter((tier) => tier.name && tier.noOfTickets > 0)
    : [];

  return {
    noOfTickets,
    ticketType,
    tiers,
  };
};

const mapPlanningEvent = (event, soldCount = 0) => {
  const totalTickets = Number(event?.tickets?.totalTickets || 0);
  const safeTotal = totalTickets > 0 ? totalTickets : 1;
  const trendingScore = Number((Number(soldCount || 0) / safeTotal).toFixed(4));

  return {
  source: 'planning-public',
  eventId: event.eventId,
  ticketAvailabilityEndAt: event?.ticketAvailability?.endAt || null,
  eventTitle: event.eventTitle || '',
  eventDescription: event.eventDescription || '',
  eventScheduled: {
    startAt: event?.schedule?.startAt || null,
    endAt: event?.schedule?.endAt || null,
  },
  venue: {
    locationName: event?.location?.name || '',
    latitude: event?.location?.latitude ?? null,
    longitude: event?.location?.longitude ?? null,
  },
  tickets: normalizePlanningTickets(event?.tickets),
  ticketsSold: Number(soldCount || 0),
  trendingScore,
  eventField: event?.eventField || null,
  eventBanner: event?.eventBanner || null,
  selectedPromotions: normalizePromotionTypes(event?.promotionType),
  };
};

const mapPromoteEvent = (event, soldCount = 0) => {
  const modelSold = Number(event?.ticketAnalytics?.ticketsSold || 0);
  const effectiveSold = Math.max(modelSold, Number(soldCount || 0));
  const totalTickets = Number(event?.tickets?.noOfTickets || 0);
  const safeTotal = totalTickets > 0 ? totalTickets : 1;
  const trendingScore = Number((effectiveSold / safeTotal).toFixed(4));

  return {
  source: 'promote',
  eventId: event.eventId,
  ticketAvailabilityEndAt: event?.ticketAvailability?.endAt || null,
  eventTitle: event.eventTitle || '',
  eventDescription: event.eventDescription || '',
  eventScheduled: {
    startAt: event?.schedule?.startAt || null,
    endAt: event?.schedule?.endAt || null,
  },
  venue: {
    locationName: event?.venue?.locationName || '',
    latitude: event?.venue?.latitude ?? null,
    longitude: event?.venue?.longitude ?? null,
  },
  tickets: normalizePromoteTickets(event?.tickets),
  ticketsSold: effectiveSold,
  trendingScore,
  eventField: event?.eventField || null,
  eventBanner: event?.eventBanner || null,
  };
};

const getTicketMarketplaceEvents = async ({ page = 1, limit = 20 } = {}) => {
  const safePage = toPositiveInt(page, 1);
  const safeLimit = Math.min(100, toPositiveInt(limit, 20));
  const now = new Date();

  const planningQuery = {
    category: CATEGORY.PUBLIC,
    platformFeePaid: true,
    status: STATUS.CONFIRMED,
    'ticketAvailability.startAt': { $lte: now },
    'ticketAvailability.endAt': { $gte: now },
  };

  const promoteQuery = {
    platformFeePaid: true,
    eventStatus: { $nin: [PROMOTE_STATUS.PAYMENT_REQUIRED, PROMOTE_STATUS.COMPLETE] },
    'adminDecision.status': ADMIN_DECISION_STATUS.APPROVED,
    'ticketAvailability.startAt': { $lte: now },
    'ticketAvailability.endAt': { $gte: now },
  };

  const planningSelect = [
    'eventId',
    'eventTitle',
    'eventDescription',
    'eventField',
    'eventBanner',
    'location',
    'schedule',
    'ticketAvailability',
    'tickets',
    'promotionType',
    'updatedAt',
  ].join(' ');

  const promoteSelect = [
    'eventId',
    'eventTitle',
    'eventDescription',
    'eventField',
    'eventBanner',
    'venue',
    'schedule',
    'ticketAvailability',
    'tickets',
    'ticketAnalytics',
    'updatedAt',
  ].join(' ');

  const [planningEvents, promoteEvents] = await Promise.all([
    Planning.find(planningQuery).select(planningSelect).lean(),
    Promote.find(promoteQuery).select(promoteSelect).lean(),
  ]);

  const eventIds = [
    ...(planningEvents || []).map((e) => String(e?.eventId || '').trim()).filter(Boolean),
    ...(promoteEvents || []).map((e) => String(e?.eventId || '').trim()).filter(Boolean),
  ];

  const soldAgg = eventIds.length
    ? await UserEventTicket.aggregate([
      {
        $match: {
          eventId: { $in: eventIds },
          ticketStatus: USER_TICKET_STATUS.SUCCESS,
        },
      },
      {
        $group: {
          _id: '$eventId',
          sold: { $sum: '$tickets.noOfTickets' },
        },
      },
    ])
    : [];

  const soldMap = new Map((soldAgg || []).map((row) => [String(row?._id || ''), Number(row?.sold || 0)]));

  const unified = [
    ...(planningEvents || []).map((e) => mapPlanningEvent(e, soldMap.get(String(e?.eventId || '')) || 0)),
    ...(promoteEvents || []).map((e) => mapPromoteEvent(e, soldMap.get(String(e?.eventId || '')) || 0)),
  ].sort((a, b) => {
    const diff = Number(b?.trendingScore || 0) - Number(a?.trendingScore || 0);
    if (diff !== 0) return diff;
    const aSold = Number(b?.ticketsSold || 0) - Number(a?.ticketsSold || 0);
    if (aSold !== 0) return aSold;
    const aEnd = new Date(a?.ticketAvailabilityEndAt || 0).getTime();
    const bEnd = new Date(b?.ticketAvailabilityEndAt || 0).getTime();
    return aEnd - bEnd;
  });

  const total = unified.length;
  const skip = (safePage - 1) * safeLimit;
  const events = unified.slice(skip, skip + safeLimit);

  return {
    events,
    pagination: {
      currentPage: safePage,
      totalPages: Math.max(1, Math.ceil(total / safeLimit)),
      total,
      limit: safeLimit,
    },
    serverTime: now.toISOString(),
  };
};

const getMyTicketInterests = async ({ userAuthId } = {}) => {
  const authId = String(userAuthId || '').trim();
  if (!authId) {
    return {
      fields: [],
      totalPurchasedTickets: 0,
    };
  }

  const purchased = await UserEventTicket.find({
    userAuthId: authId,
    ticketStatus: USER_TICKET_STATUS.SUCCESS,
  })
    .select('eventField tickets.noOfTickets')
    .lean();

  const fieldCount = new Map();
  for (const row of purchased || []) {
    const field = String(row?.eventField || '').trim();
    if (!field) continue;
    fieldCount.set(field, (fieldCount.get(field) || 0) + 1);
  }

  const fields = Array.from(fieldCount.entries())
    .sort((a, b) => b[1] - a[1])
    .map(([field]) => field);

  const totalPurchasedTickets = (purchased || []).reduce(
    (sum, row) => sum + Number(row?.tickets?.noOfTickets || 0),
    0
  );

  return {
    fields,
    totalPurchasedTickets,
  };
};

const prepareTicketPurchase = async ({ eventId, userAuthId, userId, tiers, selectedDay } = {}) => {
  const authId = String(userAuthId || '').trim();
  if (!authId) {
    throw createApiError(401, 'Authentication required');
  }

  const resolved = await resolveEventForPurchase(eventId);
  ensureTicketSalesWindow(resolved.ticketAvailability);

  const { selectedTiers, totalQuantity, totalAmountInPaise, selectedDay: normalizedSelectedDay } = computeSelection({
    ticketType: resolved.ticketType,
    totalAvailable: resolved.totalAvailable,
    tiers: resolved.tiers,
    requestedTiers: tiers,
    selectedDay,
    dayWiseAllocations: resolved.dayWiseAllocations,
  });

  const finalSelectedDay = resolveTicketSelectedDay({
    selectedDay: normalizedSelectedDay,
    schedule: resolved.schedule,
  });

  if (resolved.ticketType === 'paid' && totalAmountInPaise <= 0) {
    throw createApiError(400, 'Computed ticket amount is invalid');
  }

  // Pricing rule: Service fee 20% of ticket subtotal + Processing fee 20% of ticket subtotal.
  const subtotalInPaise = totalAmountInPaise;
  const serviceFeeInPaise = subtotalInPaise > 0 ? Math.round(subtotalInPaise * 0.2) : 0;
  const processingFeeInPaise = subtotalInPaise > 0 ? Math.round(subtotalInPaise * 0.2) : 0;
  const payableAmountInPaise = subtotalInPaise + serviceFeeInPaise + processingFeeInPaise;

  const avgUnitPrice = totalQuantity > 0
    ? Number((totalAmountInPaise / 100 / totalQuantity).toFixed(2))
    : 0;

  const expiresAt = new Date(Date.now() + 15 * 60 * 1000);

  const normalizedEventId = String(eventId).trim();
  let latestPendingTicket = await UserEventTicket.findOne({
    eventId: normalizedEventId,
    userAuthId: authId,
    ticketStatus: USER_TICKET_STATUS.PAYMENT_REQUIRED,
    isPaid: false,
  }).sort({ createdAt: -1 });

  if (latestPendingTicket) {
    const maybePaid = await reconcilePendingTicketFromOrderService({
      ticket: latestPendingTicket.toObject(),
    });

    if (maybePaid) {
      latestPendingTicket = null;
    }
  }

  const isPendingExpired = latestPendingTicket?.expiresAt
    ? new Date(latestPendingTicket.expiresAt).getTime() < Date.now()
    : false;

  let ticket;
  if (latestPendingTicket && !isPendingExpired) {
    latestPendingTicket.eventSource = resolved.source;
    latestPendingTicket.userId = userId || latestPendingTicket.userId || null;
    latestPendingTicket.eventTitle = resolved.eventTitle;
    latestPendingTicket.eventDescription = resolved.eventDescription;
    latestPendingTicket.eventField = resolved.eventField;
    latestPendingTicket.eventBanner = resolved.eventBanner;
    latestPendingTicket.venue = resolved.venue;
    latestPendingTicket.schedule = resolved.schedule;
    latestPendingTicket.ticketAvailability = resolved.ticketAvailability;
    latestPendingTicket.tickets = {
      noOfTickets: totalQuantity,
      ticketType: resolved.ticketType,
      tiers: selectedTiers,
      selectedDay: finalSelectedDay,
      unitPrice: avgUnitPrice,
      totalAmount: Number((subtotalInPaise / 100).toFixed(2)),
      currency: 'INR',
    };
    latestPendingTicket.expiresAt = expiresAt;
    ticket = await latestPendingTicket.save();
  } else {
    ticket = await UserEventTicket.create({
      eventId: normalizedEventId,
      eventSource: resolved.source,
      userId: userId || null,
      userAuthId: authId,
      eventTitle: resolved.eventTitle,
      eventDescription: resolved.eventDescription,
      eventField: resolved.eventField,
      eventBanner: resolved.eventBanner,
      venue: resolved.venue,
      schedule: resolved.schedule,
      ticketAvailability: resolved.ticketAvailability,
      tickets: {
        noOfTickets: totalQuantity,
        ticketType: resolved.ticketType,
        tiers: selectedTiers,
        selectedDay: finalSelectedDay,
        unitPrice: avgUnitPrice,
        totalAmount: Number((subtotalInPaise / 100).toFixed(2)),
        currency: 'INR',
      },
      isPaid: false,
      ticketStatus: USER_TICKET_STATUS.PAYMENT_REQUIRED,
      expiresAt,
    });
  }

  const frontendBaseUrl = (process.env.FRONTEND_URL || 'http://localhost:5173').replace(/\/$/, '');

  return {
    ticketId: ticket.ticketId,
    eventId: ticket.eventId,
    eventSource: ticket.eventSource,
    eventTitle: ticket.eventTitle,
    eventLocation: ticket?.venue?.locationName || 'TBA',
    ticketType: ticket?.tickets?.ticketType,
    tiers: ticket?.tickets?.tiers || [],
    selectedDay: resolveTicketSelectedDay({
      selectedDay: ticket?.tickets?.selectedDay,
      schedule: ticket?.schedule,
    }) || null,
    quantity: ticket?.tickets?.noOfTickets || 0,
    subtotalInPaise,
    subtotalInInr: Number((subtotalInPaise / 100).toFixed(2)),
    serviceFeeInPaise,
    serviceFeeInInr: Number((serviceFeeInPaise / 100).toFixed(2)),
    processingFeeInPaise,
    processingFeeInInr: Number((processingFeeInPaise / 100).toFixed(2)),
    amountInPaise: payableAmountInPaise,
    amountInInr: Number((payableAmountInPaise / 100).toFixed(2)),
    qrToken: signTicketQrToken({
      ticketId: ticket.ticketId,
      eventId: ticket.eventId,
      userAuthId: ticket.userAuthId,
    }),
    currency: 'INR',
    checkoutLink: `${frontendBaseUrl}/user/ticket/${encodeURIComponent(ticket.ticketId)}`,
    ticketStatus: ticket.ticketStatus,
    expiresAt: ticket.expiresAt,
  };
};

const confirmFreeTicketPurchase = async ({ eventId, ticketId, userAuthId } = {}) => {
  const authId = String(userAuthId || '').trim();
  if (!authId) {
    throw createApiError(401, 'Authentication required');
  }

  const normalizedEventId = String(eventId || '').trim();
  if (!normalizedEventId) {
    throw createApiError(400, 'Event ID is required');
  }

  const normalizedTicketId = String(ticketId || '').trim();

  const query = {
    eventId: normalizedEventId,
    userAuthId: authId,
    ticketStatus: USER_TICKET_STATUS.PAYMENT_REQUIRED,
    isPaid: false,
  };

  if (normalizedTicketId) {
    query.ticketId = normalizedTicketId;
  }

  let pendingTicket = await UserEventTicket.findOne(query).sort({ createdAt: -1 }).lean();
  if (!pendingTicket && normalizedTicketId) {
    // If already confirmed, return the ticket idempotently.
    const existing = await UserEventTicket.findOne({
      eventId: normalizedEventId,
      ticketId: normalizedTicketId,
      userAuthId: authId,
      ticketStatus: USER_TICKET_STATUS.SUCCESS,
    }).lean();

    if (existing) {
      return mapTicketForFrontend(existing);
    }
  }

  if (!pendingTicket) {
    throw createApiError(404, 'Pending ticket not found for confirmation');
  }

  const ticketType = String(pendingTicket?.tickets?.ticketType || '').trim().toLowerCase();
  const totalAmount = Number(pendingTicket?.tickets?.totalAmount || 0);
  if (!(ticketType === 'free' || totalAmount <= 0)) {
    throw createApiError(409, 'Only free tickets can be confirmed without payment');
  }

  const confirmed = await markTicketSalePaid({
    eventId: normalizedEventId,
    authId,
    ticketId: pendingTicket.ticketId,
    paidAt: new Date().toISOString(),
    notes: {
      ticketId: pendingTicket.ticketId,
    },
  });

  if (!confirmed) {
    throw createApiError(500, 'Failed to confirm free ticket');
  }

  return confirmed;
};

const verifyTicketQr = async ({ token, scannedByAuthId, scannedByRole } = {}) => {
  const payload = verifyTicketQrToken(token);

  const ticket = await UserEventTicket.findOne({
    ticketId: String(payload.ticketId).trim(),
    eventId: String(payload.eventId).trim(),
    userAuthId: String(payload.userAuthId).trim(),
  });

  if (!ticket) {
    throw createApiError(404, 'Ticket not found for this QR token');
  }

  const isPaid = Boolean(ticket?.isPaid);
  const ticketStatus = String(ticket?.ticketStatus || '').trim().toUpperCase();
  if (!isPaid || ticketStatus !== USER_TICKET_STATUS.SUCCESS) {
    if (!isPaid || ticketStatus === USER_TICKET_STATUS.PAYMENT_REQUIRED) {
      throw createApiError(409, 'Ticket payment is pending');
    }
    if (ticketStatus === USER_TICKET_STATUS.CANCELED) {
      throw createApiError(409, 'Ticket is canceled and cannot be used for entry');
    }
    if (ticketStatus === USER_TICKET_STATUS.EXPIRED) {
      throw createApiError(409, 'Ticket has expired');
    }
    throw createApiError(409, 'Ticket is not valid for entry');
  }

  const now = new Date();

  const startAt = ticket?.schedule?.startAt ? new Date(ticket.schedule.startAt) : null;
  if (startAt && !Number.isNaN(startAt.getTime()) && now < startAt) {
    throw createApiError(409, 'Event check-in has not started yet');
  }

  const endAt = ticket?.schedule?.endAt ? new Date(ticket.schedule.endAt) : null;
  if (endAt && !Number.isNaN(endAt.getTime()) && now > endAt) {
    throw createApiError(409, 'Event has ended and check-in is closed');
  }

  const verificationStatus = String(ticket?.verification?.status || '').trim().toUpperCase() === USER_TICKET_VERIFICATION_STATUS.VERIFIED
    ? USER_TICKET_VERIFICATION_STATUS.VERIFIED
    : USER_TICKET_VERIFICATION_STATUS.PENDING;

  const previousScanCount = Number(ticket?.verification?.scanCount || 0);
  const scannerAuthId = String(scannedByAuthId || '').trim() || null;
  const scannerRole = String(scannedByRole || '').trim().toUpperCase() || null;
  const selectedDay = resolveTicketSelectedDay({
    selectedDay: ticket?.tickets?.selectedDay,
    schedule: ticket?.schedule,
  });

  if (!ticket.tickets) ticket.tickets = {};
  if (selectedDay && ticket?.tickets?.selectedDay !== selectedDay) {
    ticket.tickets.selectedDay = selectedDay;
  }

  if (verificationStatus === USER_TICKET_VERIFICATION_STATUS.VERIFIED) {
    const scanHistory = appendTicketScanHistory(ticket?.verification?.scanHistory, {
      scannedAt: now,
      scannedByAuthId: scannerAuthId,
      scannedByRole: scannerRole,
      outcome: 'ALREADY_SCANNED',
    });

    ticket.verification = {
      ...(ticket.verification?.toObject ? ticket.verification.toObject() : ticket.verification || {}),
      status: USER_TICKET_VERIFICATION_STATUS.VERIFIED,
      verifiedAt: ticket?.verification?.verifiedAt || now,
      verifiedByAuthId: ticket?.verification?.verifiedByAuthId || scannerAuthId,
      lastScannedAt: now,
      scanCount: previousScanCount + 1,
      scanHistory,
    };

    await ticket.save({ validateBeforeSave: false });

    return {
      valid: true,
      alreadyScanned: true,
      message: "It's already scanned",
      ticketId: ticket.ticketId,
      eventId: ticket.eventId,
      userAuthId: ticket.userAuthId,
      eventTitle: ticket.eventTitle,
      eventSource: ticket.eventSource,
      ticketStatus: ticket.ticketStatus,
      verificationStatus: USER_TICKET_VERIFICATION_STATUS.VERIFIED,
      quantity: Number(ticket?.tickets?.noOfTickets || 0),
      tiers: Array.isArray(ticket?.tickets?.tiers) ? ticket.tickets.tiers : [],
      selectedDay: ticket?.tickets?.selectedDay || selectedDay || null,
      paidAt: ticket.paidAt || null,
      verifiedAt: ticket?.verification?.verifiedAt || null,
      lastScannedAt: now.toISOString(),
      scanCount: previousScanCount + 1,
      scannedByAuthId: ticket?.verification?.verifiedByAuthId || null,
      scannedByRole: scannerRole,
      scanHistory: mapScanHistoryForApi(scanHistory),
    };
  }

  const scanHistory = appendTicketScanHistory(ticket?.verification?.scanHistory, {
    scannedAt: now,
    scannedByAuthId: scannerAuthId,
    scannedByRole: scannerRole,
    outcome: 'VERIFIED',
  });

  ticket.verification = {
    ...(ticket.verification?.toObject ? ticket.verification.toObject() : ticket.verification || {}),
    status: USER_TICKET_VERIFICATION_STATUS.VERIFIED,
    verifiedAt: now,
    verifiedByAuthId: scannerAuthId,
    lastScannedAt: now,
    scanCount: previousScanCount + 1,
    scanHistory,
  };

  await ticket.save({ validateBeforeSave: false });

  return {
    valid: true,
    alreadyScanned: false,
    message: 'Ticket verified successfully',
    ticketId: ticket.ticketId,
    eventId: ticket.eventId,
    userAuthId: ticket.userAuthId,
    eventTitle: ticket.eventTitle,
    eventSource: ticket.eventSource,
    ticketStatus: ticket.ticketStatus,
    verificationStatus: USER_TICKET_VERIFICATION_STATUS.VERIFIED,
    quantity: Number(ticket?.tickets?.noOfTickets || 0),
    tiers: Array.isArray(ticket?.tickets?.tiers) ? ticket.tickets.tiers : [],
    selectedDay: ticket?.tickets?.selectedDay || selectedDay || null,
    paidAt: ticket.paidAt || null,
    verifiedAt: now.toISOString(),
    lastScannedAt: now.toISOString(),
    scanCount: previousScanCount + 1,
    scannedByAuthId: scannerAuthId,
    scannedByRole: scannerRole,
    scanHistory: mapScanHistoryForApi(scanHistory),
  };
};

const getMyTicketByTicketId = async ({ ticketId, userAuthId } = {}) => {
  const authId = String(userAuthId || '').trim();
  if (!authId) {
    throw createApiError(401, 'Authentication required');
  }

  const normalizedTicketId = String(ticketId || '').trim();
  if (!normalizedTicketId) {
    throw createApiError(400, 'Ticket ID is required');
  }

  const ticket = await UserEventTicket.findOne({
    ticketId: normalizedTicketId,
    userAuthId: authId,
  }).lean();

  if (!ticket) {
    throw createApiError(404, 'Ticket not found');
  }

  return mapTicketForFrontend(ticket);
};

const reconcilePendingTicketFromOrderService = async ({ ticket }) => {
  if (!ticket?.eventId || !ticket?.ticketId || !ticket?.userAuthId) return false;

  try {
    const response = await axios.get(
      `${ORDER_SERVICE_URL}/orders/${encodeURIComponent(String(ticket.eventId).trim())}`,
      {
        headers: {
          'x-auth-id': String(ticket.userAuthId || '').trim(),
          'x-user-id': String(ticket.userId || '').trim(),
          'x-user-role': 'USER',
        },
        timeout: 6000,
      }
    );

    const order = response?.data?.data;
    if (!order) return false;

    const paid = String(order?.status || '').toUpperCase() === 'PAID';
    const isTicketSale = String(order?.orderType || '').toUpperCase() === 'TICKET SALE';
    const samePaymentOrder = ticket?.paymentOrderId
      && String(order?.paymentOrderId || '').trim() === String(ticket.paymentOrderId).trim();
    const sameTicketFromNotes = String(order?.notes?.ticketId || '').trim() === String(ticket.ticketId).trim();

    if (!paid || !isTicketSale || (!samePaymentOrder && !sameTicketFromNotes)) {
      return false;
    }

    const patched = await markTicketSalePaid({
      eventId: ticket.eventId,
      authId: ticket.userAuthId,
      ticketId: ticket.ticketId,
      paymentOrderId: order.paymentOrderId,
      transactionId: order.transactionId,
      paidAt: order.paidAt || new Date().toISOString(),
      notes: order.notes || {},
    });

    return Boolean(patched);
  } catch (error) {
    logger.warn('Failed to reconcile pending ticket from order-service', {
      ticketId: ticket?.ticketId,
      eventId: ticket?.eventId,
      message: error?.response?.data?.message || error?.message,
    });
    return false;
  }
};

const getMyTickets = async ({ userAuthId } = {}) => {
  const authId = String(userAuthId || '').trim();
  if (!authId) {
    throw createApiError(401, 'Authentication required');
  }

  const pendingRows = await UserEventTicket.find({
    userAuthId: authId,
    ticketStatus: USER_TICKET_STATUS.PAYMENT_REQUIRED,
  })
    .sort({ createdAt: -1 })
    .limit(10)
    .lean();

  if (Array.isArray(pendingRows) && pendingRows.length > 0) {
    for (const pending of pendingRows) {
      await reconcilePendingTicketFromOrderService({ ticket: pending });
    }
  }

  const rows = await UserEventTicket.find({
    userAuthId: authId,
    ticketStatus: USER_TICKET_STATUS.SUCCESS,
  })
    .sort({ paidAt: -1, createdAt: -1 })
    .lean();

  return {
    tickets: (rows || []).map((row) => mapTicketForFrontend(row)),
    total: (rows || []).length,
  };
};

const getEventTicketGuests = async ({ eventId, page = 1, limit = 20, query = '' } = {}) => {
  const normalizedEventId = String(eventId || '').trim();
  if (!normalizedEventId) {
    throw createApiError(400, 'Event ID is required');
  }

  const normalizedPage = toPositiveInt(page, 1);
  const normalizedLimit = Math.min(100, toPositiveInt(limit, 20));
  const normalizedQuery = String(query || '').trim().toLowerCase();

  const rows = await UserEventTicket.find({ eventId: normalizedEventId })
    .sort({ paidAt: -1, createdAt: -1 })
    .lean();

  if (!Array.isArray(rows) || rows.length === 0) {
    return {
      guests: [],
      total: 0,
      page: normalizedPage,
      limit: normalizedLimit,
      totalPages: 0,
    };
  }

  const uniqueAuthIds = Array.from(
    new Set(
      rows
        .map((row) => String(row?.userAuthId || '').trim())
        .filter(Boolean)
    )
  );

  const userByAuthId = new Map();
  await Promise.all(uniqueAuthIds.map(async (authId) => {
    try {
      const user = await fetchUserByAuthId(authId);
      if (user) userByAuthId.set(authId, user);
    } catch (error) {
      logger.warn('Failed to resolve guest user from user-service', {
        authId,
        eventId: normalizedEventId,
        message: error?.response?.data?.message || error?.message,
      });
    }
  }));

  const mapped = rows.map((ticket) => {
    const userAuthId = String(ticket?.userAuthId || '').trim();
    const user = userByAuthId.get(userAuthId) || null;

    const quantityRaw = Number(ticket?.tickets?.noOfTickets || 0);
    const quantity = Number.isFinite(quantityRaw) && quantityRaw > 0 ? quantityRaw : 0;

    const paidAmountRaw = Number(ticket?.tickets?.totalAmount || 0);
    const paidAmount = Number.isFinite(paidAmountRaw) && paidAmountRaw >= 0 ? paidAmountRaw : 0;

    return {
      ticketId: String(ticket?.ticketId || '').trim() || null,
      userAuthId,
      registrant: {
        name: normalizeGuestName(user, userAuthId),
        email: normalizeGuestEmail(user),
      },
      ticketType: resolveTicketTypeLabel(ticket),
      quantity,
      status: toDisplayTicketStatus(ticket),
      paidAmount,
      currency: String(ticket?.tickets?.currency || 'INR').trim() || 'INR',
      paidAt: ticket?.paidAt || null,
      createdAt: ticket?.createdAt || null,
    };
  });

  const filtered = normalizedQuery
    ? mapped.filter((row) => {
      const haystack = [
        row?.registrant?.name,
        row?.registrant?.email,
        row?.ticketType,
        row?.ticketId,
        row?.status,
      ]
        .map((value) => String(value || '').toLowerCase())
        .join(' ');

      return haystack.includes(normalizedQuery);
    })
    : mapped;

  const total = filtered.length;
  const totalPages = total === 0 ? 0 : Math.ceil(total / normalizedLimit);
  const safePage = totalPages === 0 ? 1 : Math.min(normalizedPage, totalPages);
  const start = (safePage - 1) * normalizedLimit;
  const guests = filtered.slice(start, start + normalizedLimit);

  return {
    guests,
    total,
    page: safePage,
    limit: normalizedLimit,
    totalPages,
  };
};

const markTicketSalePaid = async (payload = {}) => {
  const eventId = String(payload?.eventId || '').trim();
  const authId = String(payload?.authId || '').trim();
  const ticketId = String(payload?.notes?.ticketId || payload?.ticketId || '').trim();

  if (!eventId || !authId || !ticketId) {
    logger.error('PAYMENT_SUCCESS for TICKET SALE missing required fields', {
      eventId,
      authId,
      ticketId,
    });
    return null;
  }

  const ticket = await UserEventTicket.findOne({
    ticketId,
    eventId,
    userAuthId: authId,
  });

  if (!ticket) {
    logger.error('Ticket purchase record not found while confirming payment', { eventId, authId, ticketId });
    return null;
  }

  if (ticket.ticketStatus === USER_TICKET_STATUS.SUCCESS) {
    return mapTicketForFrontend(ticket.toObject());
  }

  const selectedTiers = Array.isArray(ticket?.tickets?.tiers) ? ticket.tickets.tiers : [];
  const requestedTotal = Number(ticket?.tickets?.noOfTickets || 0);

  if (requestedTotal < 1) {
    logger.error('Ticket purchase record has invalid quantity', { eventId, authId, ticketId, requestedTotal });
    ticket.ticketStatus = USER_TICKET_STATUS.CANCELED;
    await ticket.save();
    return null;
  }

  if (ticket.eventSource === 'planning-public') {
    const planning = await Planning.findOne({ eventId });
    if (!planning) {
      logger.error('Planning event missing while confirming ticket payment', { eventId, ticketId });
      return null;
    }

    const totalAvailable = Number(planning?.tickets?.totalTickets || 0);
    if (totalAvailable < requestedTotal) {
      logger.error('Insufficient planning tickets during payment confirmation', {
        eventId,
        ticketId,
        requestedTotal,
        totalAvailable,
      });
      ticket.ticketStatus = USER_TICKET_STATUS.CANCELED;
      await ticket.save();
      return null;
    }

    planning.tickets.totalTickets = totalAvailable - requestedTotal;

    if (Array.isArray(planning?.tickets?.tiers) && planning.tickets.tiers.length > 0) {
      for (const selectedTier of selectedTiers) {
        const index = planning.tickets.tiers.findIndex(
          (tier) => String(tier?.tierName || '').toLowerCase() === String(selectedTier?.name || '').toLowerCase()
        );

        if (index < 0) {
          logger.error('Planning tier missing during payment confirmation', { eventId, ticketId, tier: selectedTier?.name });
          ticket.ticketStatus = USER_TICKET_STATUS.CANCELED;
          await ticket.save();
          return null;
        }

        const available = Number(planning.tickets.tiers[index]?.ticketCount || 0);
        const requested = Number(selectedTier?.noOfTickets || 0);
        if (available < requested) {
          logger.error('Insufficient planning tier inventory during payment confirmation', {
            eventId,
            ticketId,
            tier: selectedTier?.name,
            requested,
            available,
          });
          ticket.ticketStatus = USER_TICKET_STATUS.CANCELED;
          await ticket.save();
          return null;
        }

        planning.tickets.tiers[index].ticketCount = available - requested;
      }
    }

    const selectedDay = normalizeDayKey(ticket?.tickets?.selectedDay);
    if (selectedDay && Array.isArray(planning?.tickets?.dayWiseAllocations) && planning.tickets.dayWiseAllocations.length > 0) {
      const dayIndex = planning.tickets.dayWiseAllocations.findIndex(
        (row) => normalizeDayKey(row?.day) === selectedDay
      );

      if (dayIndex < 0) {
        logger.error('Planning day allocation missing during payment confirmation', { eventId, ticketId, selectedDay });
        ticket.ticketStatus = USER_TICKET_STATUS.CANCELED;
        await ticket.save();
        return null;
      }

      const dayAvailable = Number(planning.tickets.dayWiseAllocations[dayIndex]?.ticketCount || 0);
      if (dayAvailable < requestedTotal) {
        logger.error('Insufficient planning day inventory during payment confirmation', {
          eventId,
          ticketId,
          selectedDay,
          requestedTotal,
          dayAvailable,
        });
        ticket.ticketStatus = USER_TICKET_STATUS.CANCELED;
        await ticket.save();
        return null;
      }

      planning.tickets.dayWiseAllocations[dayIndex].ticketCount = dayAvailable - requestedTotal;

      const dayTiers = Array.isArray(planning.tickets.dayWiseAllocations[dayIndex]?.tierBreakdown)
        ? planning.tickets.dayWiseAllocations[dayIndex].tierBreakdown
        : [];

      if (dayTiers.length > 0) {
        for (const selectedTier of selectedTiers) {
          const tierIndex = dayTiers.findIndex(
            (tier) => normalizeTierNameKey(tier?.tierName || tier?.name) === normalizeTierNameKey(selectedTier?.name)
          );

          if (tierIndex < 0) {
            logger.error('Planning day tier missing during payment confirmation', {
              eventId,
              ticketId,
              selectedDay,
              tier: selectedTier?.name,
            });
            ticket.ticketStatus = USER_TICKET_STATUS.CANCELED;
            await ticket.save();
            return null;
          }

          const dayTierAvailable = Number(dayTiers[tierIndex]?.ticketCount || 0);
          const dayTierRequested = Number(selectedTier?.noOfTickets || 0);
          if (dayTierAvailable < dayTierRequested) {
            logger.error('Insufficient planning day tier inventory during payment confirmation', {
              eventId,
              ticketId,
              selectedDay,
              tier: selectedTier?.name,
              requested: dayTierRequested,
              available: dayTierAvailable,
            });
            ticket.ticketStatus = USER_TICKET_STATUS.CANCELED;
            await ticket.save();
            return null;
          }

          dayTiers[tierIndex].ticketCount = dayTierAvailable - dayTierRequested;
        }
      }
    }

    await planning.save({ validateBeforeSave: false });
  } else if (ticket.eventSource === 'promote') {
    const promote = await Promote.findOne({ eventId });
    if (!promote) {
      logger.error('Promote event missing while confirming ticket payment', { eventId, ticketId });
      return null;
    }

    const totalAvailable = Number(promote?.tickets?.noOfTickets || 0);
    if (totalAvailable < requestedTotal) {
      logger.error('Insufficient promote tickets during payment confirmation', {
        eventId,
        ticketId,
        requestedTotal,
        totalAvailable,
      });
      ticket.ticketStatus = USER_TICKET_STATUS.CANCELED;
      await ticket.save();
      return null;
    }

    promote.tickets.noOfTickets = totalAvailable - requestedTotal;

    if (Array.isArray(promote?.tickets?.tiers) && promote.tickets.tiers.length > 0) {
      for (const selectedTier of selectedTiers) {
        const index = promote.tickets.tiers.findIndex(
          (tier) => String(tier?.name || '').toLowerCase() === String(selectedTier?.name || '').toLowerCase()
        );

        if (index < 0) {
          logger.error('Promote tier missing during payment confirmation', { eventId, ticketId, tier: selectedTier?.name });
          ticket.ticketStatus = USER_TICKET_STATUS.CANCELED;
          await ticket.save();
          return null;
        }

        const available = Number(promote.tickets.tiers[index]?.quantity || 0);
        const requested = Number(selectedTier?.noOfTickets || 0);
        if (available < requested) {
          logger.error('Insufficient promote tier inventory during payment confirmation', {
            eventId,
            ticketId,
            tier: selectedTier?.name,
            requested,
            available,
          });
          ticket.ticketStatus = USER_TICKET_STATUS.CANCELED;
          await ticket.save();
          return null;
        }

        promote.tickets.tiers[index].quantity = available - requested;
      }
    }

    const currentSold = Number(promote?.ticketAnalytics?.ticketsSold || 0);
    promote.ticketAnalytics = {
      ...(promote.ticketAnalytics?.toObject ? promote.ticketAnalytics.toObject() : promote.ticketAnalytics || {}),
      ticketsSold: currentSold + requestedTotal,
      ticketsYetToSell: Math.max(0, (Number(promote.tickets?.noOfTickets || 0))),
    };

    await promote.save({ validateBeforeSave: false });
  }

  ticket.isPaid = true;
  ticket.ticketStatus = USER_TICKET_STATUS.SUCCESS;
  ticket.paidAt = payload?.paidAt ? new Date(payload.paidAt) : new Date();
  ticket.expiresAt = null;
  await ticket.save();

  return mapTicketForFrontend(ticket.toObject());
};

module.exports = {
  getTicketMarketplaceEvents,
  getMyTicketInterests,
  prepareTicketPurchase,
  confirmFreeTicketPurchase,
  getMyTickets,
  getMyTicketByTicketId,
  getEventTicketGuests,
  markTicketSalePaid,
  verifyTicketQr,
};
