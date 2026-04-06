const Promote = require('../models/Promote');
const Planning = require('../models/Planning');
const UserEventTicket = require('../models/UserEventTicket');
const axios = require('axios');
const createApiError = require('../utils/ApiError');
const logger = require('../utils/logger');
const { PROMOTE_STATUS, ADMIN_DECISION_STATUS } = require('../utils/promoteConstants');
const { TERMINAL_STATUSES: PLANNING_TERMINAL_STATUSES } = require('../utils/planningConstants');
const { USER_TICKET_STATUS } = require('../utils/ticketConstants');
const promoteConfigService = require('./promoteConfigService');
const mongoose = require('mongoose');
const { fetchUserById, fetchActiveManagers } = require('./userServiceClient');
const { ensureEventChatSeeded } = require('./chatSeedService');
const { publishEvent } = require('../kafka/eventProducer');

const defaultOrderServiceUrl = process.env.SERVICE_HOST
  ? 'http://order-service:8087'
  : 'http://localhost:8087';
const orderServiceUrl = process.env.ORDER_SERVICE_URL || defaultOrderServiceUrl;
const upstreamTimeoutMs = parseInt(process.env.UPSTREAM_HTTP_TIMEOUT_MS || '10000', 10);

const buildInternalOrderServiceHeaders = () => ({
  'x-auth-id': 'event-service',
  'x-user-id': '',
  'x-user-email': '',
  'x-user-username': 'event-service',
  'x-user-role': 'MANAGER',
});

const normalizeId = (value) => String(value || '').trim();

const REQUIRED_PROMOTE_MANAGER_DEPARTMENT = 'Public Event';

const normalizeLoose = (value) => String(value || '').trim().toLowerCase();

const normalizePromotionToken = (value) => String(value || '')
  .trim()
  .toLowerCase()
  .replace(/[_-]+/g, ' ')
  .replace(/\s+/g, ' ');

const hasPromotionSelected = (rows, promotionName) => {
  const target = normalizePromotionToken(promotionName);
  if (!target) return false;

  const list = Array.isArray(rows) ? rows : [];
  return list.some((row) => normalizePromotionToken(row) === target);
};

const isEligibleManagerRole = (user) => normalizeLoose(user?.role) === 'manager';

const isDepartmentMatch = (userDepartment, requiredDepartment) => {
  if (!requiredDepartment) return true;
  return normalizeLoose(userDepartment) === normalizeLoose(requiredDepartment);
};

// Accept both junior/senior manager roles. If assignedRole is null, allow it.
const isAssignedRoleEligible = (assignedRole) => {
  if (!assignedRole) return false;
  const role = normalizeLoose(assignedRole);
  return role.includes('junior') || role.includes('senior');
};

const toDateOrNull = (value) => {
  if (!value) return null;
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? null : d;
};

const normalizeRange = (range) => {
  const start = toDateOrNull(range?.start);
  const end = toDateOrNull(range?.end) || start;
  if (!start || !end) return null;
  if (end < start) return { start, end: start };
  return { start, end };
};

const rangesOverlap = (a, b) => {
  const ra = normalizeRange(a);
  const rb = normalizeRange(b);

  // If either side is malformed/missing schedule, keep conservative behavior and block.
  if (!ra || !rb) return true;
  return ra.start <= rb.end && rb.start <= ra.end;
};

const promoteToRange = (promote) => ({
  start: promote?.schedule?.startAt,
  end: promote?.schedule?.endAt,
});

const toNonNegativeNumber = (value) => {
  const n = Number(value || 0);
  if (!Number.isFinite(n) || n < 0) return 0;
  return n;
};

const buildPromoteTicketSalesStats = async (promote, feesConfig = {}) => {
  const eventId = String(promote?.eventId || '').trim();
  const remainingTickets = toNonNegativeNumber(promote?.tickets?.noOfTickets);
  const configuredPlatformFeeInr = toNonNegativeNumber(feesConfig?.platformFee);
  const cfgServiceChargePercentRaw = Number(feesConfig?.serviceChargePercent);
  const serviceChargePercent = Number.isFinite(cfgServiceChargePercentRaw)
    ? Math.max(0, Math.min(100, cfgServiceChargePercentRaw))
    : 0;

  if (!eventId) {
    return {
      totalTickets: remainingTickets,
      ticketsSold: 0,
      ticketsRemaining: remainingTickets,
      conversionRatePercent: 0,
      ticketSubtotalInr: 0,
      serviceChargePercent,
      serviceChargeInr: 0,
      serviceFeeInr: 0,
      processingFeeInr: 0,
      grossRevenueInr: 0,
      platformFeeInr: configuredPlatformFeeInr,
      totalFeesInr: configuredPlatformFeeInr,
      netPnlInr: 0,
      currency: 'INR',
    };
  }

  const [generatedRows, successfulAgg] = await Promise.all([
    UserEventTicket.find({
      eventId,
      eventSource: 'promote',
      ticketStatus: { $nin: [USER_TICKET_STATUS.CANCELED, USER_TICKET_STATUS.EXPIRED] },
    })
      .select('tickets.noOfTickets tickets.totalAmount')
      .lean(),
    UserEventTicket.aggregate([
      {
        $match: {
          eventId,
          eventSource: 'promote',
          ticketStatus: USER_TICKET_STATUS.SUCCESS,
        },
      },
      {
        $group: {
          _id: '$eventId',
          successfulTicketsSold: { $sum: '$tickets.noOfTickets' },
        },
      },
    ]),
  ]);

  const toPaise = (inr) => {
    const value = Number(inr || 0);
    if (!Number.isFinite(value) || value <= 0) return 0;
    return Math.round(value * 100);
  };

  let generatedTickets = 0;
  let generatedSubtotalPaise = 0;

  for (const row of generatedRows || []) {
    const quantityRaw = Number(row?.tickets?.noOfTickets || 0);
    const quantity = Number.isFinite(quantityRaw) && quantityRaw > 0 ? Math.floor(quantityRaw) : 0;
    generatedTickets += quantity;

    const lineSubtotalPaise = toPaise(row?.tickets?.totalAmount);
    generatedSubtotalPaise += lineSubtotalPaise;
  }

  const ticketsSold = Math.max(0, generatedTickets);
  const successfulTicketsSold = toNonNegativeNumber(successfulAgg?.[0]?.successfulTicketsSold);
  const totalTickets = Math.max(remainingTickets + ticketsSold, ticketsSold);
  const ticketsRemaining = Math.max(0, totalTickets - ticketsSold);

  // Revenue rule: ticket price * generated tickets (derived from generated ticket rows).
  const grossRevenuePaise = generatedSubtotalPaise;
  const serviceChargePaise = Math.round(grossRevenuePaise * (serviceChargePercent / 100));
  const platformFeePaise = toPaise(configuredPlatformFeeInr);
  const totalFeesPaise = platformFeePaise + serviceChargePaise;

  const ticketSubtotalInr = Number((grossRevenuePaise / 100).toFixed(2));
  const serviceChargeInr = Number((serviceChargePaise / 100).toFixed(2));
  const serviceFeeInr = serviceChargeInr;
  const processingFeeInr = 0;
  const grossRevenueInr = Number((grossRevenuePaise / 100).toFixed(2));
  const platformFeeInr = Number((platformFeePaise / 100).toFixed(2));
  const totalFeesInr = Number((totalFeesPaise / 100).toFixed(2));
  const netPnlInr = Number(((grossRevenuePaise - totalFeesPaise) / 100).toFixed(2));
  const conversionRatePercent = totalTickets > 0
    ? Number(((ticketsSold / totalTickets) * 100).toFixed(2))
    : 0;

  return {
    totalTickets,
    ticketsSold,
    successfulTicketsSold,
    ticketsRemaining,
    conversionRatePercent,
    ticketSubtotalInr,
    serviceChargePercent,
    serviceChargeInr,
    serviceFeeInr,
    processingFeeInr,
    grossRevenueInr,
    platformFeeInr,
    totalFeesInr,
    netPnlInr,
    currency: 'INR',
  };
};

let immediateAutoAssignCursor = 0;

const rotateListFromCursor = (ids = []) => {
  const list = Array.isArray(ids) ? ids : [];
  if (list.length <= 1) return list;

  const start = Math.max(0, Number(immediateAutoAssignCursor || 0)) % list.length;
  immediateAutoAssignCursor = (start + 1) % list.length;

  return [...list.slice(start), ...list.slice(0, start)];
};

const getEventRangeByEventId = async (eventId) => {
  const normalizedEventId = String(eventId || '').trim();
  if (!normalizedEventId) return null;

  const [promote, planning] = await Promise.all([
    Promote.findOne({ eventId: normalizedEventId }).select('eventId schedule').lean(),
    Planning.findOne({ eventId: normalizedEventId }).select('eventId category schedule eventDate').lean(),
  ]);

  if (promote) return promoteToRange(promote);
  if (planning) return planningToRange(planning);
  return null;
};

const planningToRange = (planning) => {
  const category = String(planning?.category || '').trim().toLowerCase();
  if (category === 'public') {
    return {
      start: planning?.schedule?.startAt,
      end: planning?.schedule?.endAt,
    };
  }

  return {
    start: planning?.eventDate,
    end: planning?.eventDate,
  };
};

const autoAssignManagerImmediately = async (promote, { decidedByAuthId = null } = {}) => {
  if (!promote?.eventId) return { assigned: false, managerId: null };
  if (promote.assignedManagerId) return { assigned: false, managerId: String(promote.assignedManagerId) };

  let managers;
  try {
    const limit = Math.min(2000, Math.max(1, Number(process.env.MANAGER_AUTOASSIGN_MANAGER_FETCH_LIMIT || 500)));
    managers = await fetchActiveManagers({ limit });
  } catch (error) {
    logger.warn(`Immediate auto-assign failed to fetch managers: ${error.message}`);
    return { assigned: false, managerId: null };
  }

  const candidateIds = Array.from(
    new Set(
      (managers || [])
        .filter((m) => isEligibleManagerRole(m))
        .filter((m) => m?.isActive !== false)
        .filter((m) => isDepartmentMatch(m?.department, REQUIRED_PROMOTE_MANAGER_DEPARTMENT))
        .filter((m) => isAssignedRoleEligible(m?.assignedRole))
        .map((m) => normalizeId(m?._id || m?.id))
        .filter(Boolean)
    )
  ).sort();

  const candidateOrder = rotateListFromCursor(candidateIds);

  for (const managerId of candidateOrder) {
    try {
      const result = await tryAutoAssignManager(promote.eventId, managerId, {
        assignedByAuthId: decidedByAuthId || 'system:autoassign',
      });
      if (result?.assigned) return { assigned: true, managerId };
    } catch (error) {
      // Continue trying other managers (availability conflicts, etc.)
      continue;
    }
  }

  return { assigned: false, managerId: null };
};

const assertManagerEligibleForPromote = async ({ managerId } = {}) => {
  const user = await fetchUserById(managerId);
  if (!user) throw createApiError(404, 'Manager not found in user-service');

  if (!isEligibleManagerRole(user)) {
    throw createApiError(400, 'Provided user is not a MANAGER');
  }

  if (!isDepartmentMatch(user.department, REQUIRED_PROMOTE_MANAGER_DEPARTMENT)) {
    throw createApiError(400, `Manager department must be ${REQUIRED_PROMOTE_MANAGER_DEPARTMENT}`);
  }

  if (!isAssignedRoleEligible(user.assignedRole)) {
    throw createApiError(400, 'Manager assignedRole must be JUNIOR or SENIOR');
  }

  if (user.isActive === false) {
    throw createApiError(400, 'Manager is not active');
  }
};

const assertManagerAvailable = async ({ managerId, eventIdToExclude } = {}) => {
  const normalizedManagerId = normalizeId(managerId);
  if (!normalizedManagerId) throw createApiError(400, 'managerId is required');

  let targetRange = null;
  if (eventIdToExclude) {
    const targetPromote = await Promote.findOne({ eventId: String(eventIdToExclude).trim() })
      .select('schedule')
      .lean();
    targetRange = targetPromote ? promoteToRange(targetPromote) : null;
  }

  const activeAssignmentQuery = {
    assignedManagerId: normalizedManagerId,
    eventStatus: { $ne: PROMOTE_STATUS.COMPLETE },
    'adminDecision.status': { $ne: ADMIN_DECISION_STATUS.REJECTED },
  };
  if (eventIdToExclude) {
    activeAssignmentQuery.eventId = { $ne: String(eventIdToExclude).trim() };
  }

  const existingPromotes = await Promote.find(activeAssignmentQuery)
    .select('eventId schedule')
    .lean();

  if (!targetRange) {
    if ((existingPromotes || []).length > 0) {
      throw createApiError(409, 'Manager is already assigned to another event');
    }
  } else {
    const hasPromoteConflict = (existingPromotes || []).some((row) => rangesOverlap(targetRange, promoteToRange(row)));
    if (hasPromoteConflict) {
      throw createApiError(409, 'Manager is already assigned to another event for overlapping dates');
    }
  }

  const existingPlannings = await Planning.find({
    assignedManagerId: normalizedManagerId,
    status: { $nin: PLANNING_TERMINAL_STATUSES },
  })
    .select('eventId category schedule eventDate')
    .lean();

  if (!targetRange) {
    if ((existingPlannings || []).length > 0) {
      throw createApiError(409, 'Manager is already assigned to another event');
    }
  } else {
    const hasPlanningConflict = (existingPlannings || []).some((row) => rangesOverlap(targetRange, planningToRange(row)));
    if (hasPlanningConflict) {
      throw createApiError(409, 'Manager is already assigned to another event for overlapping dates');
    }
  }
};

// ─── Create ───────────────────────────────────────────────────────────────────

/**
 * Create a new promote record.
 * The eventBanner and authenticityProofs Cloudinary results are merged in
 * by the controller BEFORE calling this function.
 */
const createPromote = async (payload) => {
  // Snapshot the current fees config onto the promote record
  if (
    payload.platformFee === undefined ||
    payload.platformFee === null ||
    payload.serviceChargePercent === undefined ||
    payload.serviceChargePercent === null
  ) {
    const cfg = await promoteConfigService.getFees();
    if (payload.platformFee === undefined || payload.platformFee === null) {
      payload.platformFee = cfg.platformFee;
    }
    if (payload.serviceChargePercent === undefined || payload.serviceChargePercent === null) {
      payload.serviceChargePercent = cfg.serviceChargePercent;
    }
  }

  const promote = new Promote(payload);
  const saved = await promote.save();

  return {
    promoteId: saved.promoteId,
    eventId: saved.eventId,
    eventTitle: saved.eventTitle,
    eventCategory: saved.eventCategory,
    eventStatus: saved.eventStatus,
    platformFeePaid: saved.platformFeePaid,
    platformFee: saved.platformFee,
    serviceChargePercent: saved.serviceChargePercent,
    totalAmount: saved.totalAmount,
    serviceCharge: saved.serviceCharge,
    estimatedNetRevenue: saved.estimatedNetRevenue,
    ticketAnalytics: saved.ticketAnalytics,
    schedule: saved.schedule,
  };
};

// ─── Read (own) ───────────────────────────────────────────────────────────────

const getMyPromotes = async (authId, page = 1, limit = 10) => {
  if (!authId?.trim()) throw createApiError(400, 'Auth ID is required');

  const skip = (page - 1) * limit;

  const [promotes, total, cfg] = await Promise.all([
    Promote.find({ authId: authId.trim() })
      .sort({ createdAt: -1 })
      .skip(skip)
      .limit(Number(limit))
      .lean(),
    Promote.countDocuments({ authId: authId.trim() }),
    promoteConfigService.getFees(),
  ]);

  const platformFeeFallback = cfg.platformFee;
  const serviceChargePercentFallback = cfg.serviceChargePercent;
  const hydratedPromotes = (promotes || []).map((p) => ({
    ...p,
    platformFee: (p.platformFee === undefined || p.platformFee === null) ? platformFeeFallback : p.platformFee,
    serviceChargePercent: (p.serviceChargePercent === undefined || p.serviceChargePercent === null)
      ? serviceChargePercentFallback
      : p.serviceChargePercent,
  }));

  return {
    promotes: hydratedPromotes,
    pagination: {
      currentPage: Number(page),
      totalPages: Math.ceil(total / limit),
      total,
      limit: Number(limit),
    },
  };
};

// ─── Read (single by promoteId or eventId) ────────────────────────────────────

const getPromoteByEventId = async (eventId) => {
  if (!eventId?.trim()) throw createApiError(400, 'Event ID is required');

  const [promote, cfg] = await Promise.all([
    Promote.findOne({ eventId: eventId.trim() }).lean(),
    promoteConfigService.getFees(),
  ]);
  if (!promote) throw createApiError(404, 'Promote record not found');

  const normalized = {
    ...promote,
    platformFee: (promote.platformFee === undefined || promote.platformFee === null) ? cfg.platformFee : promote.platformFee,
    serviceChargePercent: (promote.serviceChargePercent === undefined || promote.serviceChargePercent === null)
      ? cfg.serviceChargePercent
      : promote.serviceChargePercent,
  };

  try {
    normalized.ticketSalesStats = await buildPromoteTicketSalesStats(normalized, cfg);
  } catch (error) {
    logger.warn('Failed to compute promote ticket sales stats', {
      eventId: String(normalized?.eventId || '').trim(),
      message: error?.message || String(error),
    });

    const fallbackPlatformFeeInr = toNonNegativeNumber(normalized?.platformFee ?? cfg?.platformFee);
    const fallbackServiceChargePercentRaw = Number(normalized?.serviceChargePercent ?? cfg?.serviceChargePercent);
    const fallbackServiceChargePercent = Number.isFinite(fallbackServiceChargePercentRaw)
      ? Math.max(0, Math.min(100, fallbackServiceChargePercentRaw))
      : 0;

    normalized.ticketSalesStats = {
      totalTickets: toNonNegativeNumber(normalized?.tickets?.noOfTickets),
      ticketsSold: 0,
      successfulTicketsSold: 0,
      ticketsRemaining: toNonNegativeNumber(normalized?.tickets?.noOfTickets),
      conversionRatePercent: 0,
      ticketSubtotalInr: 0,
      serviceChargePercent: fallbackServiceChargePercent,
      serviceChargeInr: 0,
      serviceFeeInr: 0,
      processingFeeInr: 0,
      grossRevenueInr: 0,
      platformFeeInr: fallbackPlatformFeeInr,
      totalFeesInr: fallbackPlatformFeeInr,
      netPnlInr: 0,
      currency: 'INR',
    };
  }

  return normalized;
};

/**
 * Add a CORE staff member to a promote event (Manager/Admin)
 */
const addPromoteCoreStaff = async ({ eventId, staffId, actorRole, actorManagerId } = {}) => {
  const trimmedEventId = String(eventId || '').trim();
  const trimmedStaffId = String(staffId || '').trim();
  if (!trimmedEventId) throw createApiError(400, 'eventId is required');
  if (!trimmedStaffId) throw createApiError(400, 'staffId is required');

  if (String(actorRole || '').toUpperCase() !== 'MANAGER') {
    throw createApiError(403, 'Only MANAGER can assign staff');
  }

  const promote = await Promote.findOne({ eventId: trimmedEventId });
  if (!promote) throw createApiError(404, 'Promote record not found');

  if (String(promote?.adminDecision?.status || '').trim() !== ADMIN_DECISION_STATUS.APPROVED) {
    throw createApiError(409, 'Staff can only be assigned when promote status is APPROVED');
  }

  const normalizedActorId = String(actorManagerId || '').trim();
  if (!normalizedActorId) throw createApiError(403, 'Manager identity is required');
  if (String(promote.assignedManagerId || '').trim() !== normalizedActorId) {
    throw createApiError(403, 'You are not assigned to this event');
  }

  // Enforce availability: staff cannot be assigned to other active events.
  const [conflictPromote, conflictPlanning] = await Promise.all([
    Promote.findOne({
      eventId: { $ne: trimmedEventId },
      eventStatus: { $ne: PROMOTE_STATUS.COMPLETE },
      'adminDecision.status': { $ne: ADMIN_DECISION_STATUS.REJECTED },
      $or: [
        { assignedManagerId: trimmedStaffId },
        { coreStaffIds: trimmedStaffId },
      ],
    })
      .select('eventId')
      .lean(),
    Planning.findOne({
      eventId: { $ne: trimmedEventId },
      status: { $nin: PLANNING_TERMINAL_STATUSES },
      $or: [
        { assignedManagerId: trimmedStaffId },
        { coreStaffIds: trimmedStaffId },
      ],
    })
      .select('eventId')
      .lean(),
  ]);

  if (conflictPromote || conflictPlanning) {
    throw createApiError(409, 'Staff is already assigned to another active event');
  }

  const existing = Array.isArray(promote.coreStaffIds) ? promote.coreStaffIds.map(String) : [];
  if (!existing.includes(trimmedStaffId)) {
    promote.coreStaffIds = [...existing, trimmedStaffId];
    await promote.save();
  }

  const cfg = await promoteConfigService.getFees();
  const json = promote.toJSON();
  return {
    ...json,
    platformFee: (json.platformFee === undefined || json.platformFee === null) ? cfg.platformFee : json.platformFee,
    serviceChargePercent: (json.serviceChargePercent === undefined || json.serviceChargePercent === null)
      ? cfg.serviceChargePercent
      : json.serviceChargePercent,
  };
};

/**
 * Remove a CORE staff member from a promote event (Manager/Admin)
 */
const removePromoteCoreStaff = async ({ eventId, staffId, actorRole, actorManagerId } = {}) => {
  const trimmedEventId = String(eventId || '').trim();
  const trimmedStaffId = String(staffId || '').trim();
  if (!trimmedEventId) throw createApiError(400, 'eventId is required');
  if (!trimmedStaffId) throw createApiError(400, 'staffId is required');

  if (String(actorRole || '').toUpperCase() !== 'MANAGER') {
    throw createApiError(403, 'Only MANAGER can remove staff');
  }

  const promote = await Promote.findOne({ eventId: trimmedEventId });
  if (!promote) throw createApiError(404, 'Promote record not found');

  if (String(promote?.adminDecision?.status || '').trim() !== ADMIN_DECISION_STATUS.APPROVED) {
    throw createApiError(409, 'Staff can only be removed when promote status is APPROVED');
  }

  const normalizedActorId = String(actorManagerId || '').trim();
  if (!normalizedActorId) throw createApiError(403, 'Manager identity is required');
  if (String(promote.assignedManagerId || '').trim() !== normalizedActorId) {
    throw createApiError(403, 'You are not assigned to this event');
  }

  const existing = Array.isArray(promote.coreStaffIds) ? promote.coreStaffIds.map(String) : [];
  promote.coreStaffIds = existing.filter((x) => x !== trimmedStaffId);
  await promote.save({ validateBeforeSave: false });

  const cfg = await promoteConfigService.getFees();
  const json = promote.toJSON();
  return {
    ...json,
    platformFee: (json.platformFee === undefined || json.platformFee === null) ? cfg.platformFee : json.platformFee,
    serviceChargePercent: (json.serviceChargePercent === undefined || json.serviceChargePercent === null)
      ? cfg.serviceChargePercent
      : json.serviceChargePercent,
  };
};

/**
 * Release generated ticket revenue to user (demo flow) for promote events.
 */
const releasePromoteGeneratedRevenuePayout = async ({ eventId, actorRole, actorAuthId, actorManagerId, mode = 'DEMO' } = {}) => {
  const trimmedEventId = String(eventId || '').trim();
  if (!trimmedEventId) throw createApiError(400, 'eventId is required');

  const normalizedRole = String(actorRole || '').trim().toUpperCase();
  if (!['MANAGER', 'ADMIN'].includes(normalizedRole)) {
    throw createApiError(403, 'Only MANAGER or ADMIN can release generated revenue payout');
  }

  const normalizedMode = String(mode || 'DEMO').trim().toUpperCase();
  if (!['DEMO', 'RAZORPAY'].includes(normalizedMode)) {
    throw createApiError(400, 'mode must be either DEMO or RAZORPAY');
  }

  const promote = await Promote.findOne({ eventId: trimmedEventId });
  if (!promote) throw createApiError(404, 'Promote record not found');
  const ownerAuthId = String(promote.authId || '').trim();

  if (normalizedRole === 'MANAGER') {
    const normalizedActorManagerId = String(actorManagerId || '').trim();
    if (!normalizedActorManagerId) throw createApiError(403, 'Manager identity is required');
    if (String(promote.assignedManagerId || '').trim() !== normalizedActorManagerId) {
      throw createApiError(403, 'You are not assigned to this event');
    }
  }

  const existingStatus = String(promote?.generatedRevenuePayout?.status || '').trim().toUpperCase();
  if (existingStatus === 'SUCCESS') {
    const existing = await getPromoteByEventId(trimmedEventId);
    const generatedRevenueInr = toNonNegativeNumber(existing?.ticketSalesStats?.grossRevenueInr);
    const totalFeesInr = toNonNegativeNumber(existing?.ticketSalesStats?.totalFeesInr ?? existing?.ticketSalesStats?.platformFeeInr);
    return {
      ...existing,
      generatedRevenuePayoutSummary: {
        generatedRevenueInr,
        totalVendorCostInr: 0,
        totalFeesInr,
        payoutAmountInr: Number((Number(promote?.generatedRevenuePayout?.amountPaise || 0) / 100).toFixed(2)),
        mode: normalizedMode,
        alreadyProcessed: true,
      },
    };
  }

  const promoteWithStats = await getPromoteByEventId(trimmedEventId);
  const generatedRevenueInr = toNonNegativeNumber(promoteWithStats?.ticketSalesStats?.grossRevenueInr);
  const totalFeesInr = toNonNegativeNumber(promoteWithStats?.ticketSalesStats?.totalFeesInr ?? promoteWithStats?.ticketSalesStats?.platformFeeInr);
  const payoutAmountInr = Number(Math.max(0, generatedRevenueInr - totalFeesInr).toFixed(2));
  const payoutAmountPaise = Math.round(payoutAmountInr * 100);

  if (payoutAmountPaise <= 0) {
    throw createApiError(409, 'Generated revenue payout amount must be greater than zero');
  }

  let payoutRecord = null;
  if (normalizedMode === 'RAZORPAY') {
    if (!ownerAuthId) {
      throw createApiError(409, 'Cannot release payout because event owner authId is missing');
    }

    try {
      const payoutRes = await axios.post(
        `${orderServiceUrl}/orders/user-payouts/release`,
        {
          eventId: trimmedEventId,
          userAuthId: ownerAuthId,
          payoutAmountPaise,
          generatedRevenuePaise: Math.round(generatedRevenueInr * 100),
          totalVendorCostPaise: 0,
          totalFeesPaise: Math.round(totalFeesInr * 100),
          currency: 'INR',
        },
        {
          timeout: upstreamTimeoutMs,
          headers: buildInternalOrderServiceHeaders(),
        }
      );

      payoutRecord = payoutRes?.data?.data?.payout || null;
    } catch (error) {
      const statusCode = Number(error?.response?.status || error?.statusCode || 502);
      const message = String(error?.response?.data?.message || error?.message || 'Failed to release payout in Razorpay mode');
      throw createApiError(statusCode, message);
    }
  }

  const paidAt = normalizedMode === 'RAZORPAY'
    ? (payoutRecord?.paidAt ? new Date(payoutRecord.paidAt) : new Date())
    : new Date();

  const transactionRef = normalizedMode === 'RAZORPAY'
    ? String(payoutRecord?.razorpayTransferId || payoutRecord?.payoutId || `RAZORPAY-GRP-${trimmedEventId}-${Date.now()}`).trim()
    : `DEMO-GRP-${trimmedEventId}-${Date.now()}`;

  promote.generatedRevenuePayout = {
    mode: normalizedMode,
    status: 'SUCCESS',
    amountPaise: payoutAmountPaise,
    currency: 'INR',
    paidAt,
    paidByAuthId: String(actorAuthId || '').trim() || null,
    transactionRef,
    notes: normalizedMode === 'RAZORPAY'
      ? 'Generated revenue payout transferred via Razorpay Route'
      : 'Generated revenue payout sent to user in DEMO mode',
  };

  await promote.save({ validateBeforeSave: false });

  if (normalizedMode === 'DEMO' && ownerAuthId) {
    try {
      await publishEvent('USER_REVENUE_PAYOUT_SUCCESS', {
        eventId: trimmedEventId,
        userAuthId: ownerAuthId,
        amount: payoutAmountPaise,
        currency: 'INR',
        managerAuthId: String(actorAuthId || '').trim() || null,
        transactionRef,
        payoutMode: 'DEMO',
        paidAt: paidAt?.toISOString?.() || new Date().toISOString(),
      });
    } catch (kafkaError) {
      logger.error('Failed to publish USER_REVENUE_PAYOUT_SUCCESS from promote payout:', kafkaError);
    }
  }

  const updated = await getPromoteByEventId(trimmedEventId);
  return {
    ...updated,
    generatedRevenuePayoutSummary: {
      generatedRevenueInr,
      totalVendorCostInr: 0,
      totalFeesInr,
      payoutAmountInr,
      mode: normalizedMode,
      alreadyProcessed: false,
    },
  };
};

/**
 * Trigger EMAIL BLAST promotion action for promote events.
 */
const triggerPromoteEmailBlastPromotionAction = async ({ eventId, actorRole, actorAuthId, actorManagerId } = {}) => {
  const trimmedEventId = String(eventId || '').trim();
  if (!trimmedEventId) throw createApiError(400, 'eventId is required');

  const normalizedRole = String(actorRole || '').trim().toUpperCase();
  if (!['MANAGER', 'ADMIN'].includes(normalizedRole)) {
    throw createApiError(403, 'Only MANAGER or ADMIN can trigger email blast');
  }

  const promote = await Promote.findOne({ eventId: trimmedEventId });
  if (!promote) throw createApiError(404, 'Promote record not found');

  if (normalizedRole === 'MANAGER') {
    const normalizedActorManagerId = String(actorManagerId || '').trim();
    if (!normalizedActorManagerId) throw createApiError(403, 'Manager identity is required');
    if (String(promote.assignedManagerId || '').trim() !== normalizedActorManagerId) {
      throw createApiError(403, 'You are not assigned to this event');
    }
  }

  if (!hasPromotionSelected(promote.promotion, 'Email Blast')) {
    throw createApiError(409, 'Email Blast is not selected for this promote event');
  }

  const requestId = `PEB-${trimmedEventId}-${Date.now()}`;
  const requestedAt = new Date().toISOString();
  const eventDate = promote?.schedule?.startAt || null;
  const parsedEventDate = eventDate ? new Date(eventDate) : null;
  const eventDateIso = parsedEventDate && !Number.isNaN(parsedEventDate.getTime())
    ? parsedEventDate.toISOString()
    : null;
  const eventLocation = promote?.venue?.locationName || null;

  await publishEvent('PROMOTION_EMAIL_BLAST_REQUESTED', {
    requestId,
    eventId: trimmedEventId,
    eventType: 'promote',
    promotionType: 'EMAIL_BLAST',
    requestedByAuthId: String(actorAuthId || '').trim() || null,
    requestedByRole: normalizedRole,
    requestedAt,
    eventTitle: String(promote?.eventTitle || '').trim() || null,
    eventDescription: String(promote?.eventDescription || '').trim() || null,
    eventDate: eventDateIso,
    eventLocation: eventLocation ? String(eventLocation).trim() : null,
    eventBannerUrl: String(promote?.eventBanner?.url || promote?.eventBanner || '').trim() || null,
  });

  return {
    requestId,
    eventId: trimmedEventId,
    eventType: 'promote',
    promotionType: 'EMAIL_BLAST',
    requestedAt,
  };
};

// ─── Read all (admin / manager) ──────────────────────────────────────────────

const getAllPromotes = async (filters = {}, page = 1, limit = 10) => {
  const query = {};

  if (filters.eventStatus) query.eventStatus = filters.eventStatus;
  if (filters.platformFeePaid !== undefined) {
    query.platformFeePaid = filters.platformFeePaid === 'true';
  }
  if (filters.authId) query.authId = filters.authId;
  if (filters.search?.trim()) {
    query.$or = [
      { eventTitle: new RegExp(filters.search, 'i') },
      { eventId: new RegExp(filters.search, 'i') },
    ];
  }

  const skip = (page - 1) * limit;

  const [promotes, total, cfg] = await Promise.all([
    Promote.find(query)
      .sort({ createdAt: -1 })
      .skip(skip)
      .limit(Number(limit))
      .lean(),
    Promote.countDocuments(query),
    promoteConfigService.getFees(),
  ]);

  const platformFeeFallback = cfg.platformFee;
  const serviceChargePercentFallback = cfg.serviceChargePercent;
  const hydratedPromotes = (promotes || []).map((p) => ({
    ...p,
    platformFee: (p.platformFee === undefined || p.platformFee === null) ? platformFeeFallback : p.platformFee,
    serviceChargePercent: (p.serviceChargePercent === undefined || p.serviceChargePercent === null)
      ? serviceChargePercentFallback
      : p.serviceChargePercent,
  }));

  return {
    promotes: hydratedPromotes,
    pagination: {
      currentPage: Number(page),
      totalPages: Math.ceil(total / limit),
      total,
      limit: Number(limit),
    },
  };
};

// ─── Mark as paid (called by Kafka payment_events consumer) ──────────────────

const markPromotePaid = async (eventId) => {
  if (!eventId?.trim()) throw createApiError(400, 'Event ID is required');

  const promote = await Promote.findOne({ eventId: eventId.trim() });
  if (!promote) throw createApiError(404, 'Promote record not found');

  if (!promote.platformFeePaid) {
    promote.platformFeePaid = true;
    await promote.save(); // pre-validate hook recalculates status
    logger.info(`Promote marked as paid: ${eventId}`);
  }

  return promote;
};

// ─── Update status (manager / admin) ─────────────────────────────────────────

const updatePromoteStatus = async (eventId, eventStatus, assignedManagerId = null) => {
  if (!eventId) throw createApiError(400, 'Event ID is required');

  const promote = await Promote.findOne({ eventId: eventId.trim() });
  if (!promote) throw createApiError(404, 'Promote record not found');

  const allowedTransitions = [PROMOTE_STATUS.LIVE, PROMOTE_STATUS.COMPLETE];
  if (!allowedTransitions.includes(eventStatus)) {
    throw createApiError(400, `eventStatus must be one of: ${allowedTransitions.join(', ')}`);
  }

  promote.eventStatus = eventStatus;
  if (assignedManagerId) {
    await assertManagerEligibleForPromote({ managerId: assignedManagerId });
    await assertManagerAvailable({ managerId: assignedManagerId, eventIdToExclude: promote.eventId });

    const now = new Date();
    promote.assignedManagerId = assignedManagerId;
    promote.managerAssignment = {
      assignedAt: now,
      assignedByAuthId: null,
      autoAssigned: false,
    };

    // Backward-compatible behavior: assigning a manager implies approval.
    if (promote.adminDecision?.status !== ADMIN_DECISION_STATUS.APPROVED) {
      promote.adminDecision = {
        status: ADMIN_DECISION_STATUS.APPROVED,
        decidedAt: now,
        decidedByAuthId: null,
        rejectionReason: null,
      };
    }
  }

  await promote.save();
  logger.info(`Promote status updated: ${eventId} → ${eventStatus}`);
  return promote;
};

// ─── Assign manager (admin only) ─────────────────────────────────────────────

const assignManager = async (eventId, managerId) => {
  return assignManagerWithMetadata(eventId, managerId, { assignedByAuthId: null, autoAssigned: false });
};

const assignManagerWithMetadata = async (
  eventId,
  managerId,
  { assignedByAuthId = null, autoAssigned = false } = {}
) => {
  if (!eventId) throw createApiError(400, 'Event ID is required');
  if (!managerId) throw createApiError(400, 'Manager ID is required');

  const promote = await Promote.findOne({ eventId: String(eventId).trim() });
  if (!promote) throw createApiError(404, 'Promote record not found');

  if (promote.adminDecision?.status === ADMIN_DECISION_STATUS.REJECTED) {
    throw createApiError(400, 'Cannot assign a manager to a rejected event');
  }

  await assertManagerEligibleForPromote({ managerId });

  await assertManagerAvailable({ managerId, eventIdToExclude: promote.eventId });

  const now = new Date();
  promote.assignedManagerId = managerId;
  promote.managerAssignment = {
    assignedAt: now,
    assignedByAuthId: assignedByAuthId || null,
    autoAssigned: Boolean(autoAssigned),
  };

  if (promote.adminDecision?.status !== ADMIN_DECISION_STATUS.APPROVED) {
    promote.adminDecision = {
      status: ADMIN_DECISION_STATUS.APPROVED,
      decidedAt: now,
      decidedByAuthId: assignedByAuthId || null,
      rejectionReason: null,
    };
  }

  await promote.save();
  logger.info(`Manager ${managerId} assigned to promote ${eventId} (auto=${Boolean(autoAssigned)})`);

  // Best-effort: seed event chat between user + assigned manager.
  ensureEventChatSeeded({
    eventId: promote.eventId,
    userAuthId: promote.authId,
    managerAuthId: managerId,
  });

  return promote;
};

/**
 * Auto-assign helper used by the background job.
 * - Idempotent: will NOT overwrite an existing assignment.
 * - Does NOT call user-service (eligibility is enforced by the job's manager cache).
 */
const tryAutoAssignManager = async (
  eventId,
  managerId,
  { assignedByAuthId = 'system:autoassign' } = {}
) => {
  if (!eventId) throw createApiError(400, 'Event ID is required');
  if (!managerId) throw createApiError(400, 'Manager ID is required');

  await assertManagerAvailable({ managerId, eventIdToExclude: String(eventId).trim() });

  const now = new Date();
  const updateResult = await Promote.updateOne(
    {
      eventId: String(eventId).trim(),
      assignedManagerId: null,
      eventStatus: { $ne: PROMOTE_STATUS.COMPLETE },
      'adminDecision.status': ADMIN_DECISION_STATUS.APPROVED,
      'adminDecision.decidedAt': { $ne: null },
    },
    {
      $set: {
        assignedManagerId: managerId,
        managerAssignment: {
          assignedAt: now,
          assignedByAuthId: assignedByAuthId || null,
          autoAssigned: true,
        },
      },
    }
  );

  // NOTE: updateOne() bypasses Mongoose validation hooks, so PromoteSchema.pre('validate')
  // will NOT recompute eventStatus. Keep DB consistent manually.
  if (updateResult?.modifiedCount === 1) {
    // Best-effort: seed event chat between user + assigned manager.
    try {
      const promoteForChat = await Promote.findOne({ eventId: String(eventId).trim() })
        .select('eventId authId assignedManagerId')
        .lean();

      if (promoteForChat?.eventId && promoteForChat?.authId && promoteForChat?.assignedManagerId) {
        ensureEventChatSeeded({
          eventId: promoteForChat.eventId,
          userAuthId: promoteForChat.authId,
          managerAuthId: promoteForChat.assignedManagerId,
        });
      }
    } catch (error) {
      logger.warn(`Failed to seed promote chat after auto-assign for ${String(eventId).trim()}: ${error.message}`);
    }

    try {
      const promote = await Promote.findOne({ eventId: String(eventId).trim() }).select('platformFeePaid assignedManagerId eventStatus').lean();
      if (promote && ![PROMOTE_STATUS.LIVE, PROMOTE_STATUS.COMPLETE].includes(promote.eventStatus)) {
        const computedStatus = !promote.platformFeePaid
          ? PROMOTE_STATUS.PAYMENT_REQUIRED
          : (!promote.assignedManagerId ? PROMOTE_STATUS.MANAGER_UNASSIGNED : PROMOTE_STATUS.IN_REVIEW);

        if (computedStatus !== promote.eventStatus) {
          await Promote.updateOne(
            {
              eventId: String(eventId).trim(),
              eventStatus: { $nin: [PROMOTE_STATUS.LIVE, PROMOTE_STATUS.COMPLETE] },
            },
            { $set: { eventStatus: computedStatus } }
          );
        }
      }
    } catch (error) {
      logger.warn(`Failed to sync promote eventStatus after auto-assign for ${String(eventId).trim()}: ${error.message}`);
    }
  }

  return {
    assigned: updateResult?.modifiedCount === 1,
  };
};

const unassignPromoteManager = async (eventId, { unassignedByAuthId = null } = {}) => {
  if (!eventId?.trim()) throw createApiError(400, 'Event ID is required');

  const promote = await Promote.findOne({ eventId: String(eventId).trim() });
  if (!promote) throw createApiError(404, 'Promote record not found');

  promote.assignedManagerId = null;
  promote.managerAssignment = {
    assignedAt: null,
    assignedByAuthId: unassignedByAuthId || null,
    autoAssigned: false,
  };

  await promote.save();
  logger.info(`Promote manager unassigned: ${promote.eventId}`);
  return promote;
};

const decidePromote = async (
  eventId,
  {
    decision,
    rejectionReason = null,
    managerId = null,
    decidedByAuthId = null,
  } = {}
) => {
  if (!eventId?.trim()) throw createApiError(400, 'Event ID is required');
  if (!decision) throw createApiError(400, 'decision is required');

  const normalizedDecision = String(decision).trim().toUpperCase();
  const now = new Date();

  const promote = await Promote.findOne({ eventId: String(eventId).trim() });
  if (!promote) throw createApiError(404, 'Promote record not found');

  if (normalizedDecision === 'REJECT') {
    promote.adminDecision = {
      status: ADMIN_DECISION_STATUS.REJECTED,
      decidedAt: now,
      decidedByAuthId: decidedByAuthId || null,
      rejectionReason: rejectionReason ? String(rejectionReason).trim().slice(0, 500) : null,
    };
    promote.assignedManagerId = null;
    promote.managerAssignment = {
      assignedAt: null,
      assignedByAuthId: null,
      autoAssigned: false,
    };

    await promote.save();
    logger.info(`Promote ${eventId} rejected by ${decidedByAuthId || 'admin'}`);
    return promote;
  }

  if (normalizedDecision !== 'APPROVE') {
    throw createApiError(400, 'decision must be APPROVE or REJECT');
  }

  promote.adminDecision = {
    status: ADMIN_DECISION_STATUS.APPROVED,
    decidedAt: now,
    decidedByAuthId: decidedByAuthId || null,
    rejectionReason: null,
  };
  await promote.save();

  if (managerId) {
    return assignManagerWithMetadata(promote.eventId, managerId, {
      assignedByAuthId: decidedByAuthId || null,
      autoAssigned: false,
    });
  }

  // Requirement: approving a promote event should immediately auto-assign a manager.
  const auto = await autoAssignManagerImmediately(promote, { decidedByAuthId });
  if (auto.assigned) {
    const refreshed = await Promote.findOne({ eventId: String(promote.eventId).trim() });
    if (refreshed) {
      logger.info(`Promote ${eventId} approved and auto-assigned manager ${auto.managerId}`);
      return refreshed;
    }
  }

  logger.info(`Promote ${eventId} approved by ${decidedByAuthId || 'admin'}`);
  return promote;
};

const getUnavailableManagerIds = async ({ eventId = null } = {}) => {
  const normalizedEventId = String(eventId || '').trim();

  const [existingPromotes, existingPlannings, targetRange] = await Promise.all([
    Promote.find({
      assignedManagerId: { $ne: null },
      eventStatus: { $ne: PROMOTE_STATUS.COMPLETE },
      'adminDecision.status': { $ne: ADMIN_DECISION_STATUS.REJECTED },
      ...(normalizedEventId ? { eventId: { $ne: normalizedEventId } } : {}),
    })
      .select('assignedManagerId schedule')
      .lean(),
    Planning.find({
      assignedManagerId: { $ne: null },
      status: { $nin: PLANNING_TERMINAL_STATUSES },
      ...(normalizedEventId ? { eventId: { $ne: normalizedEventId } } : {}),
    })
      .select('assignedManagerId category schedule eventDate')
      .lean(),
    normalizedEventId ? getEventRangeByEventId(normalizedEventId) : null,
  ]);

  const unavailable = new Set();

  for (const row of existingPromotes || []) {
    const managerId = normalizeId(row?.assignedManagerId);
    if (!managerId) continue;

    if (!targetRange || rangesOverlap(targetRange, promoteToRange(row))) {
      unavailable.add(managerId);
    }
  }

  for (const row of existingPlannings || []) {
    const managerId = normalizeId(row?.assignedManagerId);
    if (!managerId) continue;

    if (!targetRange || rangesOverlap(targetRange, planningToRange(row))) {
      unavailable.add(managerId);
    }
  }

  return Array.from(unavailable);
};

const getAdminDashboard = async ({ limit = 200 } = {}) => {
  const safeLimit = Math.min(500, Math.max(1, Number(limit) || 200));

  const baseSelect =
    'eventId eventTitle eventCategory customCategory eventField eventBanner schedule createdAt authId assignedManagerId adminDecision managerAssignment eventStatus platformFeePaid';

  const [assigned, applications, rejected] = await Promise.all([
    Promote.find({
      assignedManagerId: { $ne: null },
      'adminDecision.status': { $ne: ADMIN_DECISION_STATUS.REJECTED },
    })
      .sort({ createdAt: -1 })
      .limit(safeLimit)
      .select(baseSelect)
      .lean(),
    Promote.find({
      assignedManagerId: null,
      'adminDecision.status': { $ne: ADMIN_DECISION_STATUS.REJECTED },
    })
      .sort({ createdAt: -1 })
      .limit(safeLimit)
      .select(baseSelect)
      .lean(),
    Promote.find({
      'adminDecision.status': ADMIN_DECISION_STATUS.REJECTED,
    })
      .sort({ createdAt: -1 })
      .limit(safeLimit)
      .select(baseSelect)
      .lean(),
  ]);

  return {
    assigned: assigned || [],
    applications: applications || [],
    rejected: rejected || [],
  };
};

// ─── Manager dashboard helpers ──────────────────────────────────────────────

const getPromotesForManager = async ({ managerId, limit = 200 } = {}) => {
  if (!managerId || !String(managerId).trim()) {
    throw createApiError(400, 'managerId is required');
  }

  const safeLimit = Math.min(500, Math.max(1, Number(limit) || 200));

  const baseSelect =
    'eventId eventTitle eventCategory customCategory eventField eventBanner schedule ticketAvailability tickets venue createdAt authId assignedManagerId adminDecision managerAssignment eventStatus platformFeePaid totalAmount serviceCharge estimatedNetRevenue ticketAnalytics';

  const promotes = await Promote.find({
    assignedManagerId: String(managerId).trim(),
    'adminDecision.status': { $ne: ADMIN_DECISION_STATUS.REJECTED },
  })
    .sort({ createdAt: -1 })
    .limit(safeLimit)
    .select(baseSelect)
    .lean();

  return promotes || [];
};

/**
 * Update promote core details (Manager/Admin)
 */
const updatePromoteDetails = async ({ eventId, updates = {}, actorRole, actorManagerId } = {}) => {
  const trimmedEventId = String(eventId || '').trim();
  if (!trimmedEventId) throw createApiError(400, 'eventId is required');

  const promote = await Promote.findOne({ eventId: trimmedEventId });
  if (!promote) throw createApiError(404, 'Promote not found');

  const isAdmin = String(actorRole || '').toUpperCase() === 'ADMIN';
  if (!isAdmin) {
    const normalizedActorId = normalizeId(actorManagerId);
    if (!normalizedActorId) throw createApiError(403, 'Manager identity is required');
    if (normalizeId(promote.assignedManagerId) !== normalizedActorId) {
      throw createApiError(403, 'You are not assigned to this promote');
    }
  }

  const nextTitle = updates.eventTitle;
  const nextDescription = updates.eventDescription;
  const nextLocationName = updates.locationName;

  if (typeof nextTitle === 'string' && nextTitle.trim()) {
    promote.eventTitle = nextTitle.trim();
  }
  if (typeof nextDescription === 'string' && nextDescription.trim()) {
    promote.eventDescription = nextDescription.trim();
  }
  if (typeof nextLocationName === 'string' && nextLocationName.trim()) {
    promote.venue = promote.venue || {};
    promote.venue.locationName = nextLocationName.trim();
  }

  await promote.save();
  return promote.toJSON();
};

// ─── Delete ───────────────────────────────────────────────────────────────────

const deletePromote = async (eventId) => {
  if (!eventId) throw createApiError(400, 'Event ID is required');

  const promote = await Promote.findOneAndDelete({ eventId: eventId.trim() });
  if (!promote) throw createApiError(404, 'Promote record not found');

  logger.info(`Promote deleted: ${eventId}`);
  return { message: 'Promote record deleted successfully', promote };
};

module.exports = {
  createPromote,
  getMyPromotes,
  getPromoteByEventId,
  releasePromoteGeneratedRevenuePayout,
  triggerPromoteEmailBlastPromotionAction,
  updatePromoteDetails,
  addPromoteCoreStaff,
  removePromoteCoreStaff,
  getAllPromotes,
  getPromotesForManager,
  markPromotePaid,
  updatePromoteStatus,
  assignManager,
  assignManagerWithMetadata,
  tryAutoAssignManager,
  unassignPromoteManager,
  decidePromote,
  getUnavailableManagerIds,
  getAdminDashboard,
  deletePromote,
};
