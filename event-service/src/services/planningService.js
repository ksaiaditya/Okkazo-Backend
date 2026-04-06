const Planning = require('../models/Planning');
const Promote = require('../models/Promote');
const UserEventTicket = require('../models/UserEventTicket');
const VendorSelection = require('../models/VendorSelection');
const axios = require('axios');
const logger = require('../utils/logger');
const createApiError = require('../utils/ApiError');
const {
  STATUS,
  STATUS_VALUES,
  CATEGORY,
  TERMINAL_STATUSES,
  USER_HIDDEN_STATUSES,
} = require('../utils/planningConstants');
const { PROMOTE_STATUS, ADMIN_DECISION_STATUS } = require('../utils/promoteConstants');
const { USER_TICKET_STATUS } = require('../utils/ticketConstants');
const vendorSelectionService = require('./vendorSelectionService');
const vendorReservationService = require('./vendorReservationService');
const promoteConfigService = require('./promoteConfigService');
const planningQuoteService = require('./planningQuoteService');
const mongoose = require('mongoose');
const { fetchUserById, resolveUserServiceIdFromAuthId } = require('./userServiceClient');
const { ensureEventChatSeeded } = require('./chatSeedService');
const { sendEventDmConversationMessage } = require('./chatServiceClient');
const { publishEvent } = require('../kafka/eventProducer');
const {
  parseIstDayStart,
  shiftDateKeepingIstTime,
  toIstDayString,
} = require('../utils/istDateTime');

const REQUIRED_DEPARTMENT_BY_PLANNING_CATEGORY = {
  [CATEGORY.PUBLIC]: 'Public Event',
  [CATEGORY.PRIVATE]: 'Private Event',
};

const defaultOrderServiceUrl = process.env.SERVICE_HOST
  ? 'http://order-service:8087'
  : 'http://localhost:8087';
const orderServiceUrl = process.env.ORDER_SERVICE_URL || defaultOrderServiceUrl;
const upstreamTimeoutMs = parseInt(process.env.UPSTREAM_HTTP_TIMEOUT_MS || '10000', 10);

const resolveFrontendBaseUrl = () => {
  const fromEnv = String(process.env.FRONTEND_URL || process.env.FRONTEND_URL_FALLBACK || '').trim();
  if (fromEnv) return fromEnv.replace(/\/$/, '');
  return 'http://localhost:5173';
};

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

const buildInternalOrderServiceHeaders = () => ({
  'x-auth-id': 'event-service',
  'x-user-id': '',
  'x-user-email': '',
  'x-user-username': 'event-service',
  'x-user-role': 'MANAGER',
});

const fetchVendorPayoutsForEventFromOrderService = async (eventId) => {
  const normalizedEventId = String(eventId || '').trim();
  if (!normalizedEventId) return [];

  const response = await axios.get(
    `${orderServiceUrl}/orders/vendor-payouts/event/${encodeURIComponent(normalizedEventId)}`,
    {
      timeout: upstreamTimeoutMs,
      headers: buildInternalOrderServiceHeaders(),
    }
  );

  return Array.isArray(response?.data?.data?.payouts) ? response.data.data.payouts : [];
};

const toVendorServicePayoutKey = ({ vendorAuthId, service }) => {
  const vendorKey = String(vendorAuthId || '').trim().toLowerCase();
  const serviceKey = String(service || '').trim().toLowerCase();
  if (!vendorKey || !serviceKey) return null;
  return `${vendorKey}::${serviceKey}`;
};

const ensureStickyVendorReservationsForPlanning = async (planning) => {
  const eid = String(planning?.eventId || '').trim();
  const uid = String(planning?.authId || '').trim();
  if (!eid || !uid) return { attempted: 0, claimed: 0, conflicts: 0, skipped: true, reason: 'missing eventId/authId' };

  const reservationDays = vendorReservationService.planningToReservationDays(planning);
  if (reservationDays.length === 0) return { attempted: 0, claimed: 0, conflicts: 0, skipped: true, reason: 'missing planning day' };

  const selection = await VendorSelection.findOne({ eventId: eid })
    .select('vendors')
    .lean();

  const vendorItems = Array.isArray(selection?.vendors) ? selection.vendors : [];
  if (!selection || vendorItems.length === 0) {
    return { attempted: 0, claimed: 0, conflicts: 0, skipped: true, reason: 'missing vendor selection' };
  }

  let attempted = 0;
  let claimed = 0;
  let conflicts = 0;

  for (const item of vendorItems) {
    const vendorAuthId = item?.vendorAuthId != null ? String(item.vendorAuthId).trim() : '';
    if (!vendorAuthId) continue;

    attempted += 1;
    try {
      await vendorReservationService.claimForDays({
        vendorAuthId,
        days: reservationDays,
        eventId: eid,
        authId: uid,
        service: item?.service || null,
        serviceId: item?.serviceId || null,
      });
      claimed += 1;
    } catch (error) {
      if (error?.statusCode === 409 || error?.status === 409) {
        conflicts += 1;
        logger.warn('Sticky vendor reservation claim conflict after payment', {
          eventId: eid,
          days: reservationDays,
          vendorAuthId,
          service: item?.service || null,
          serviceId: item?.serviceId || null,
          message: error?.message,
        });
        continue;
      }

      logger.error('Failed to claim sticky vendor reservation after payment', {
        eventId: eid,
        day,
        vendorAuthId,
        service: item?.service || null,
        serviceId: item?.serviceId || null,
        message: error?.message || String(error),
      });
    }
  }

  return { attempted, claimed, conflicts, skipped: false, days: reservationDays };
};

const isAssignedRoleEligible = (assignedRole) => {
  if (!assignedRole) return false;
  const role = normalizeLoose(assignedRole);
  return role.includes('junior') || role.includes('senior');
};

const isCoordinatorAssignedRole = (assignedRole) => {
  if (!assignedRole) return false;
  const role = normalizeLoose(assignedRole);
  return role.includes('coordinator');
};

const normalizeRangeToIstDaySpan = (range) => {
  const startDay = toIstDayString(range?.start);
  const endDay = toIstDayString(range?.end || range?.start);
  if (!startDay || !endDay) return null;

  const start = parseIstDayStart(startDay);
  const end = parseIstDayStart(endDay);
  if (!start || !end) return null;
  if (end < start) return { start, end: start };
  return { start, end };
};

const rangesOverlap = (a, b) => {
  const ra = normalizeRangeToIstDaySpan(a);
  const rb = normalizeRangeToIstDaySpan(b);

  // If either side is malformed/missing schedule, keep conservative behavior and block.
  if (!ra || !rb) return true;
  return ra.start <= rb.end && rb.start <= ra.end;
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

const promoteToRange = (promote) => ({
  start: promote?.schedule?.startAt,
  end: promote?.schedule?.endAt,
});

const assertCoreStaffEligibleForPlanning = async ({ staffId } = {}) => {
  const normalizedStaffId = String(staffId || '').trim();
  if (!normalizedStaffId) throw createApiError(400, 'staffId is required');

  const user = await fetchUserById(normalizedStaffId);
  if (!user) throw createApiError(404, 'Staff member not found in user-service');

  if (normalizeLoose(user?.role) !== 'manager') {
    throw createApiError(400, 'Selected staff member is not a MANAGER');
  }

  if (!isCoordinatorAssignedRole(user?.assignedRole)) {
    throw createApiError(400, 'Only MANAGER with assignedRole COORDINATOR can be added as team staff');
  }

  if (user?.isActive === false) {
    throw createApiError(400, 'Selected staff member is not active');
  }

  return user;
};

const assertCoreStaffAvailableAcrossEvents = async ({ staffId, targetRange, planningEventIdToExclude } = {}) => {
  const normalizedStaffId = String(staffId || '').trim();
  if (!normalizedStaffId) throw createApiError(400, 'staffId is required');

  const normalizedExcludeEventId = String(planningEventIdToExclude || '').trim();

  const [existingPlannings, existingPromotes] = await Promise.all([
    Planning.find({
      status: { $nin: TERMINAL_STATUSES },
      ...(normalizedExcludeEventId ? { eventId: { $ne: normalizedExcludeEventId } } : {}),
      $or: [
        { assignedManagerId: normalizedStaffId },
        { coreStaffIds: normalizedStaffId },
      ],
    })
      .select('eventId category schedule eventDate')
      .lean(),
    Promote.find({
      eventStatus: { $ne: PROMOTE_STATUS.COMPLETE },
      'adminDecision.status': { $ne: ADMIN_DECISION_STATUS.REJECTED },
      ...(normalizedExcludeEventId ? { eventId: { $ne: normalizedExcludeEventId } } : {}),
      $or: [
        { assignedManagerId: normalizedStaffId },
        { coreStaffIds: normalizedStaffId },
      ],
    })
      .select('eventId schedule')
      .lean(),
  ]);

  const hasPlanningConflict = (existingPlannings || []).some((row) => {
    if (!targetRange) return true;
    return rangesOverlap(targetRange, planningToRange(row));
  });

  if (hasPlanningConflict) {
    throw createApiError(409, 'Coordinator is already assigned to another event for overlapping dates');
  }

  const hasPromoteConflict = (existingPromotes || []).some((row) => {
    if (!targetRange) return true;
    return rangesOverlap(targetRange, promoteToRange(row));
  });

  if (hasPromoteConflict) {
    throw createApiError(409, 'Coordinator is already assigned to another event for overlapping dates');
  }
};

const assertManagerEligibleForPlanning = async ({ managerId, planningCategory } = {}) => {
  const user = await fetchUserById(managerId);
  if (!user) throw createApiError(404, 'Manager not found in user-service');

  if (normalizeLoose(user?.role) !== 'manager') {
    throw createApiError(400, 'Provided user is not a MANAGER');
  }

  const requiredDepartment = REQUIRED_DEPARTMENT_BY_PLANNING_CATEGORY[planningCategory] || null;
  if (requiredDepartment && normalizeLoose(user?.department) !== normalizeLoose(requiredDepartment)) {
    throw createApiError(400, `Manager department must be ${requiredDepartment}`);
  }

  if (!isAssignedRoleEligible(user?.assignedRole)) {
    throw createApiError(400, 'Manager assignedRole must be JUNIOR or SENIOR');
  }

  if (user.isActive === false) {
    throw createApiError(400, 'Manager is not active');
  }
};

const assertManagerAvailableAcrossEvents = async ({ managerId, planningEventIdToExclude } = {}) => {
  if (!String(managerId || '').trim()) {
    throw createApiError(400, 'assignedManagerId is required');
  }

  let targetRange = null;
  if (planningEventIdToExclude) {
    const targetPlanning = await Planning.findOne({ eventId: String(planningEventIdToExclude).trim() })
      .select('category schedule eventDate')
      .lean();
    targetRange = targetPlanning ? planningToRange(targetPlanning) : null;
  }

  const existingPromotes = await Promote.find({
    assignedManagerId: String(managerId).trim(),
    eventStatus: { $ne: PROMOTE_STATUS.COMPLETE },
    'adminDecision.status': { $ne: ADMIN_DECISION_STATUS.REJECTED },
  })
    .select('eventId schedule')
    .lean();

  if (!targetRange) {
    if ((existingPromotes || []).length > 0) throw createApiError(409, 'Manager is already assigned to another event');
  } else {
    const hasPromoteConflict = (existingPromotes || []).some((row) => rangesOverlap(targetRange, promoteToRange(row)));
    if (hasPromoteConflict) throw createApiError(409, 'Manager is already assigned to another event for overlapping dates');
  }

  const planningQuery = {
    assignedManagerId: String(managerId).trim(),
    status: { $nin: TERMINAL_STATUSES },
  };
  if (planningEventIdToExclude) {
    planningQuery.eventId = { $ne: String(planningEventIdToExclude).trim() };
  }

  const existingPlannings = await Planning.find(planningQuery)
    .select('eventId category schedule eventDate')
    .lean();

  if (!targetRange) {
    if ((existingPlannings || []).length > 0) throw createApiError(409, 'Manager is already assigned to another event');
  } else {
    const hasPlanningConflict = (existingPlannings || []).some((row) => rangesOverlap(targetRange, planningToRange(row)));
    if (hasPlanningConflict) throw createApiError(409, 'Manager is already assigned to another event for overlapping dates');
  }
};

const normalizePlanningForApi = (planning, platformFeeFallback, viewerRole = null) => {
  if (!planning) return planning;

  const normalized = {
    ...planning,
    platformFeePaid: Boolean(planning.platformFeePaid) || Boolean(planning.isPaid),
    depositPaid: Boolean(planning.depositPaid),
    vendorConfirmationPaid: Boolean(planning.vendorConfirmationPaid),
    remainingPaymentPaid: Boolean(planning.remainingPaymentPaid),
    remainingPaymentPaidAmountPaise: Number(planning.remainingPaymentPaidAmountPaise || 0),
    remainingPaymentPaidCurrency: planning.remainingPaymentPaidCurrency || 'INR',
    remainingPaymentPaidAt: planning.remainingPaymentPaidAt || null,
    fullPaymentPaid: Boolean(planning.fullPaymentPaid),
  };

  // Ensure platformFee is always populated for UI/clients.
  if (normalized.platformFee === undefined || normalized.platformFee === null) {
    normalized.platformFee = platformFeeFallback;
  }

  const normalizedRole = String(viewerRole || '').trim().toUpperCase();
  if (normalizedRole === 'USER' && USER_HIDDEN_STATUSES.includes(normalized.status)) {
    normalized.status = STATUS.COMPLETED;
  }

  // Hide legacy name.
  delete normalized.isPaid;

  return normalized;
};

const toNonNegativeNumber = (value) => {
  const n = Number(value || 0);
  return Number.isFinite(n) && n > 0 ? n : 0;
};

const computePlanningVendorCostInr = async (eventId) => {
  const normalizedEventId = String(eventId || '').trim();
  if (!normalizedEventId) return 0;

  const selection = await VendorSelection.findOne({ eventId: normalizedEventId })
    .select('vendors')
    .lean();

  const rows = Array.isArray(selection?.vendors) ? selection.vendors : [];
  return rows.reduce((sum, row) => {
    const isLocked = Boolean(row?.priceLocked) && Number(row?.vendorQuotedPrice || 0) > 0;
    if (!isLocked) return sum;

    const lockedPrice = toNonNegativeNumber(row?.vendorQuotedPrice);
    const commissionAmount = toNonNegativeNumber(row?.commissionAmount);
    const vendorPayout = Math.max(0, lockedPrice - commissionAmount);
    return sum + vendorPayout;
  }, 0);
};

const resolvePlanningTicketCapacity = (planning) => {
  const topLevelCount = toNonNegativeNumber(planning?.tickets?.totalTickets);
  if (topLevelCount > 0) return topLevelCount;

  const dayWise = Array.isArray(planning?.tickets?.dayWiseAllocations)
    ? planning.tickets.dayWiseAllocations
    : [];

  const dayWiseCount = dayWise.reduce((sum, row) => {
    const count = Number(row?.ticketCount || 0);
    return sum + (Number.isFinite(count) && count > 0 ? count : 0);
  }, 0);
  if (dayWiseCount > 0) return dayWiseCount;

  const tiers = Array.isArray(planning?.tickets?.tiers) ? planning.tickets.tiers : [];
  const tierCount = tiers.reduce((sum, tier) => {
    const count = Number(tier?.ticketCount || 0);
    return sum + (Number.isFinite(count) && count > 0 ? count : 0);
  }, 0);
  if (tierCount > 0) return tierCount;

  return toNonNegativeNumber(planning?.tickets?.totalTickets);
};

const buildPlanningTicketSalesStats = async (planning, feesConfig = {}) => {
  const eventId = String(planning?.eventId || '').trim();
  const remainingTickets = resolvePlanningTicketCapacity(planning);
  const configuredPlatformFeeInr = toNonNegativeNumber(
    planning?.platformFee ?? feesConfig?.platformFee
  );
  const serviceChargePercentRaw = Number(feesConfig?.serviceChargePercent);
  const serviceChargePercent = Number.isFinite(serviceChargePercentRaw)
    ? Math.max(0, Math.min(100, serviceChargePercentRaw))
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

  const [soldRows, generatedAgg] = await Promise.all([
    UserEventTicket.find({
      eventId,
      ticketStatus: USER_TICKET_STATUS.SUCCESS,
    })
      .select('tickets.noOfTickets tickets.totalAmount')
      .lean(),
    UserEventTicket.aggregate([
      {
        $match: {
          eventId,
          ticketStatus: { $nin: [USER_TICKET_STATUS.CANCELED, USER_TICKET_STATUS.EXPIRED] },
        },
      },
      {
        $group: {
          _id: '$eventId',
          generatedTickets: { $sum: '$tickets.noOfTickets' },
        },
      },
    ]),
  ]);

  const toPaise = (inr) => {
    const value = Number(inr || 0);
    if (!Number.isFinite(value) || value <= 0) return 0;
    return Math.round(value * 100);
  };

  let successfulTicketsSold = 0;
  let subtotalPaise = 0;

  for (const row of soldRows || []) {
    const quantityRaw = Number(row?.tickets?.noOfTickets || 0);
    const quantity = Number.isFinite(quantityRaw) && quantityRaw > 0 ? Math.floor(quantityRaw) : 0;
    successfulTicketsSold += quantity;

    const lineSubtotalPaise = toPaise(row?.tickets?.totalAmount);
    subtotalPaise += lineSubtotalPaise;
  }

  const generatedRow = Array.isArray(generatedAgg) && generatedAgg.length > 0 ? generatedAgg[0] : null;
  const generatedTickets = toNonNegativeNumber(generatedRow?.generatedTickets);

  // KPI consistency rule:
  // total capacity = current remaining in planning + tickets generated for this event.
  const ticketsSold = generatedTickets;
  const totalTickets = Math.max(remainingTickets, remainingTickets + ticketsSold);

  const grossRevenuePaise = subtotalPaise;
  const serviceChargePaise = Math.round(subtotalPaise * (serviceChargePercent / 100));
  const platformFeePaise = toPaise(configuredPlatformFeeInr);
  const totalFeesPaise = platformFeePaise + serviceChargePaise;

  const ticketSubtotalInr = Number((subtotalPaise / 100).toFixed(2));
  const serviceChargeInr = Number((serviceChargePaise / 100).toFixed(2));
  const serviceFeeInr = serviceChargeInr;
  const processingFeeInr = 0;
  const grossRevenueInr = Number((grossRevenuePaise / 100).toFixed(2));
  const platformFeeInr = Number((platformFeePaise / 100).toFixed(2));
  const totalFeesInr = Number((totalFeesPaise / 100).toFixed(2));
  const netPnlInr = Number(((grossRevenuePaise - totalFeesPaise) / 100).toFixed(2));
  const ticketsRemaining = Math.max(0, totalTickets - ticketsSold);
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

/**
 * Create a new planning event
 */
const createPlanning = async (payload) => {
  if (payload.platformFee === undefined || payload.platformFee === null) {
    const cfg = await promoteConfigService.getFees();
    payload.platformFee = cfg.platformFee;
  }

  const planning = new Planning(payload);
  const saved = await planning.save();

  const eventScheduleDate =
    saved.category === "public" ? saved.schedule?.startAt : saved.eventDate;

  return {
    eventId: saved.eventId,
    title: saved.eventTitle,
    eventScheduleDate,
    location: saved.location,
    selectedServices: saved.selectedServices,
    status: saved.status,
  };
};

/**
 * Get all plannings for a specific user
 */
const getPlanningsByAuthId = async (authId, page = 1, limit = 10, viewerRole = null) => {
  if (!authId || authId.trim() === "") {
    throw createApiError(400, "Auth ID is required");
  }

  const skip = (page - 1) * limit;

  const [plannings, total, cfg] = await Promise.all([
    Planning.find({ authId: authId.trim() })
      .sort({ createdAt: -1 })
      .skip(skip)
      .limit(Number(limit))
      .lean(),
    Planning.countDocuments({ authId: authId.trim() }),
    promoteConfigService.getFees(),
  ]);

  const hydrated = (plannings || []).map((p) => normalizePlanningForApi(p, cfg.platformFee, viewerRole));

  return {
    plannings: hydrated,
    pagination: {
      currentPage: Number(page),
      totalPages: Math.ceil(total / limit),
      totalPlannings: total,
      limit: Number(limit),
    },
  };
};

/**
 * Get a single planning by eventId
 */
const getPlanningByEventId = async (eventId, viewerRole = null) => {
  if (!eventId || eventId.trim() === "") {
    throw createApiError(400, "Event ID is required");
  }

  const [planning, cfg] = await Promise.all([
    Planning.findOne({ eventId: eventId.trim() }).lean(),
    promoteConfigService.getFees(),
  ]);
  if (!planning) {
    throw createApiError(404, "Planning not found");
  }

  const normalized = normalizePlanningForApi(planning, cfg.platformFee, viewerRole);

  if (String(normalized?.category || '').trim() === CATEGORY.PUBLIC) {
    try {
      normalized.ticketSalesStats = await buildPlanningTicketSalesStats(normalized, cfg);
    } catch (error) {
      logger.warn('Failed to compute planning ticket sales stats', {
        eventId: String(normalized?.eventId || '').trim(),
        message: error?.message || String(error),
      });
      const fallbackPlatformFeeInr = toNonNegativeNumber(normalized?.platformFee ?? cfg?.platformFee);
      const fallbackServiceChargePercentRaw = Number(cfg?.serviceChargePercent);
      const fallbackServiceChargePercent = Number.isFinite(fallbackServiceChargePercentRaw)
        ? Math.max(0, Math.min(100, fallbackServiceChargePercentRaw))
        : 0;
      normalized.ticketSalesStats = {
        totalTickets: resolvePlanningTicketCapacity(normalized),
        ticketsSold: 0,
        successfulTicketsSold: 0,
        ticketsRemaining: resolvePlanningTicketCapacity(normalized),
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
  }

  return normalized;
};

/**
 * Get all plannings with pagination and filters (Admin/Manager)
 */
const getAllPlannings = async (filters = {}, page = 1, limit = 10, viewerRole = null) => {
  const query = {};

  if (filters.category) {
    query.category = filters.category;
  }
  if (filters.status) {
    query.status = filters.status;
  }
  if (filters.isUrgent !== undefined) {
    query.isUrgent = filters.isUrgent === "true";
  }
  if (filters.search) {
    query.$or = [
      { eventTitle: new RegExp(filters.search, "i") },
      { eventId: new RegExp(filters.search, "i") },
    ];
  }

  const skip = (page - 1) * limit;

  const [plannings, total, cfg] = await Promise.all([
    Planning.find(query)
      .sort({ createdAt: -1 })
      .skip(skip)
      .limit(Number(limit))
      .lean(),
    Planning.countDocuments(query),
    promoteConfigService.getFees(),
  ]);

  const hydrated = (plannings || []).map((p) => normalizePlanningForApi(p, cfg.platformFee, viewerRole));

  return {
    plannings: hydrated,
    pagination: {
      currentPage: Number(page),
      totalPages: Math.ceil(total / limit),
      totalPlannings: total,
      limit: Number(limit),
    },
  };
};

/**
 * Update a planning status (Manager/Admin)
 */
const updatePlanningStatus = async (
  eventId,
  status,
  assignedManagerId = null,
) => {
  if (!eventId) {
    throw createApiError(400, "Event ID is required");
  }

  if (!status || !String(status).trim()) {
    throw createApiError(400, 'Status is required');
  }

  const normalizedStatus = String(status).trim();
  if (!STATUS_VALUES.includes(normalizedStatus)) {
    throw createApiError(400, `Invalid planning status: ${normalizedStatus}`);
  }

  const planning = await Planning.findOne({ eventId: eventId.trim() });
  if (!planning) {
    throw createApiError(404, "Planning not found");
  }

  planning.status = normalizedStatus;
  if (assignedManagerId) {
    await assertManagerEligibleForPlanning({ managerId: assignedManagerId, planningCategory: planning.category });
    await assertManagerAvailableAcrossEvents({ managerId: assignedManagerId, planningEventIdToExclude: planning.eventId });
    planning.assignedManagerId = assignedManagerId;
  }

  await planning.save();
  logger.info(`Planning status updated: ${eventId} -> ${status}`);

  if (planning.status === STATUS.APPROVED) {
    try {
      await planningQuoteService.lockQuoteAtApproved({ eventId: planning.eventId, lockedByAuthId: assignedManagerId || null });
    } catch (err) {
      logger.warn('Failed to lock quote after planning status APPROVED', {
        eventId: planning.eventId,
        message: err?.message,
      });
    }
  }

  // Best-effort: seed event chat between user + assigned manager.
  if (assignedManagerId) {
    ensureEventChatSeeded({
      eventId: planning.eventId,
      userAuthId: planning.authId,
      managerAuthId: assignedManagerId,
    });
  }

  // Keep VendorSelection manager sync consistent whenever assignedManagerId changes.
  // (VendorSelection may already exist even before IMMEDIATE_ACTION.)
  if (assignedManagerId != null) {
    try {
      await vendorSelectionService.ensureForPlanning(planning);
    } catch (err) {
      logger.error('Failed to sync VendorSelection after planning status/manager update', {
        eventId: planning.eventId,
        message: err.message,
      });
    }
  }

  if (planning.status === STATUS.IMMEDIATE_ACTION) {
    try {
      await vendorSelectionService.ensureForPlanning(planning);
    } catch (err) {
      logger.error('Failed to ensure VendorSelection after status update', {
        eventId: planning.eventId,
        message: err.message,
      });
    }
  }
  return planning;
};

/**
 * Mark a private planning event as COMPLETED.
 * Allowed actors:
 * - planning owner (USER)
 * - assigned manager (MANAGER)
 */
const markPlanningAsComplete = async ({ eventId, actorAuthId, actorUserId, actorRole } = {}) => {
  const normalizedEventId = String(eventId || '').trim();
  if (!normalizedEventId) {
    throw createApiError(400, 'Event ID is required');
  }

  const normalizedActorAuthId = String(actorAuthId || '').trim();
  const normalizedActorUserId = String(actorUserId || '').trim();
  if (!normalizedActorAuthId && !normalizedActorUserId) {
    throw createApiError(401, 'Actor identity is required');
  }

  const planning = await Planning.findOne({ eventId: normalizedEventId });
  if (!planning) {
    throw createApiError(404, 'Planning not found');
  }

  const normalizedRole = String(actorRole || '').trim().toUpperCase();
  const isOwner = String(planning.authId || '').trim() === normalizedActorAuthId;
  const assignedManagerId = String(planning.assignedManagerId || '').trim();

  let resolvedManagerUserId = normalizedActorUserId;
  if (!resolvedManagerUserId && normalizedRole === 'MANAGER' && normalizedActorAuthId) {
    try {
      resolvedManagerUserId = String(await resolveUserServiceIdFromAuthId(normalizedActorAuthId) || '').trim();
    } catch (error) {
      logger.warn('Failed to resolve manager user id for mark-complete authorization', {
        eventId: normalizedEventId,
        actorAuthId: normalizedActorAuthId,
        message: error?.message,
      });
    }
  }

  let assignedManagerAuthId = '';
  if (normalizedRole === 'MANAGER' && assignedManagerId && normalizedActorAuthId) {
    try {
      const assignedManagerUser = await fetchUserById(assignedManagerId);
      assignedManagerAuthId = String(assignedManagerUser?.authId || '').trim();
    } catch (error) {
      logger.warn('Failed to resolve assigned manager auth id for mark-complete authorization', {
        eventId: normalizedEventId,
        assignedManagerId,
        message: error?.message,
      });
    }
  }

  const isAssignedManager = normalizedRole === 'MANAGER' && (
    (assignedManagerId && normalizedActorAuthId && assignedManagerId === normalizedActorAuthId)
    || (assignedManagerId && resolvedManagerUserId && assignedManagerId === resolvedManagerUserId)
    || (assignedManagerAuthId && normalizedActorAuthId && assignedManagerAuthId === normalizedActorAuthId)
  );

  if (!isOwner && !isAssignedManager) {
    logger.warn('Mark-complete authorization rejected', {
      eventId: normalizedEventId,
      actorRole: normalizedRole,
      actorAuthId: normalizedActorAuthId || null,
      actorUserId: normalizedActorUserId || null,
      assignedManagerId: assignedManagerId || null,
      resolvedManagerUserId: resolvedManagerUserId || null,
      assignedManagerAuthId: assignedManagerAuthId || null,
      planningOwnerAuthId: String(planning.authId || '').trim() || null,
    });
    throw createApiError(403, 'Only the event owner or assigned manager can mark this event as complete');
  }

  if (String(planning.category || '').trim().toLowerCase() !== CATEGORY.PRIVATE) {
    throw createApiError(409, 'Mark as complete is currently supported only for private planning events');
  }

  const currentStatus = String(planning.status || '').trim();
  if (currentStatus === STATUS.COMPLETED) {
    return planning;
  }

  if (currentStatus !== STATUS.CONFIRMED) {
    throw createApiError(409, 'Only CONFIRMED events can be marked as complete');
  }

  planning.status = STATUS.COMPLETED;
  await planning.save({ validateBeforeSave: false });
  logger.info(`Planning marked as completed: ${normalizedEventId}`);

  // When manager marks a private planning event as complete, notify the owner in DM
  // to proceed with remaining payment using the event management link.
  if (normalizedRole === 'MANAGER') {
    const ownerAuthId = String(planning.authId || '').trim();
    if (ownerAuthId && normalizedActorAuthId) {
      const frontendBaseUrl = resolveFrontendBaseUrl();
      const userEventManagementLink = `${frontendBaseUrl}/user/event-management/${encodeURIComponent(normalizedEventId)}`;
      const eventTitle = String(planning.eventTitle || '').trim() || `Event ${normalizedEventId}`;
      const completionMessage = [
        `Your event \"${eventTitle}\" has been marked as completed successfully.`,
        'Please pay the remaining amount to close the event.',
        `Pay now: ${userEventManagementLink}`,
      ].join('\n');

      try {
        await sendEventDmConversationMessage({
          eventId: normalizedEventId,
          otherAuthId: ownerAuthId,
          senderAuthId: normalizedActorAuthId,
          senderRole: 'MANAGER',
          text: completionMessage,
        });
      } catch (error) {
        logger.warn('Failed to send mark-complete remaining-payment reminder message', {
          eventId: normalizedEventId,
          managerAuthId: normalizedActorAuthId,
          ownerAuthId,
          message: error?.message || String(error),
        });
      }
    }
  }

  return planning;
};

/**
 * Assign a manager to a planning without changing status.
 * This supports admin/manual assignment and auto-assign flows that should not be coupled to an "approval" step.
 */
const assignPlanningManager = async (eventId, assignedManagerId) => {
  if (!eventId || !String(eventId).trim()) {
    throw createApiError(400, 'Event ID is required');
  }
  if (!assignedManagerId) {
    throw createApiError(400, 'assignedManagerId is required');
  }

  const planning = await Planning.findOne({ eventId: String(eventId).trim() });
  if (!planning) {
    throw createApiError(404, 'Planning not found');
  }

  await assertManagerEligibleForPlanning({ managerId: assignedManagerId, planningCategory: planning.category });
  await assertManagerAvailableAcrossEvents({ managerId: assignedManagerId, planningEventIdToExclude: planning.eventId });

  planning.assignedManagerId = assignedManagerId;
  await planning.save();
  logger.info(`Planning manager assigned: ${planning.eventId} -> ${assignedManagerId}`);

  // Best-effort: seed event chat between user + assigned manager.
  ensureEventChatSeeded({
    eventId: planning.eventId,
    userAuthId: planning.authId,
    managerAuthId: assignedManagerId,
  });

  // Ensure VendorSelection exists + sync managerId.
  try {
    await vendorSelectionService.ensureForPlanning(planning);
  } catch (err) {
    logger.error('Failed to sync VendorSelection after manager assignment', {
      eventId: planning.eventId,
      message: err.message,
    });
  }

  if (planning.status === STATUS.IMMEDIATE_ACTION) {
    try {
      await vendorSelectionService.ensureForPlanning(planning);
    } catch (err) {
      logger.error('Failed to ensure VendorSelection after manager assignment', {
        eventId: planning.eventId,
        message: err.message,
      });
    }
  }

  return planning;
};

/**
 * Auto-assign helper used by the background job.
 * - Idempotent: will NOT overwrite an existing assignment.
 * - Does NOT call user-service (eligibility is enforced by the job's manager cache).
 */
const tryAutoAssignPlanningManager = async (eventId, assignedManagerId) => {
  if (!eventId || !String(eventId).trim()) {
    throw createApiError(400, 'Event ID is required');
  }
  if (!assignedManagerId) {
    throw createApiError(400, 'assignedManagerId is required');
  }

  await assertManagerAvailableAcrossEvents({
    managerId: assignedManagerId,
    planningEventIdToExclude: String(eventId).trim(),
  });

  const updateResult = await Planning.updateOne(
    {
      eventId: String(eventId).trim(),
      assignedManagerId: null,
      status: { $nin: TERMINAL_STATUSES },
      $or: [{ vendorSelectionId: { $ne: null } }, { platformFeePaid: true }, { isPaid: true }],
    },
    {
      $set: {
        assignedManagerId,
      },
    }
  );

  if (updateResult?.modifiedCount === 1) {
    logger.info(`Planning manager auto-assigned: ${String(eventId).trim()} -> ${assignedManagerId}`);

    // Best-effort sync vendor selection manager fields.
    try {
      const planning = await Planning.findOne({ eventId: String(eventId).trim() });
      if (planning) {
        await vendorSelectionService.ensureForPlanning(planning);

        // Best-effort: seed event chat between user + assigned manager.
        ensureEventChatSeeded({
          eventId: planning.eventId,
          userAuthId: planning.authId,
          managerAuthId: assignedManagerId,
        });
      }
    } catch (err) {
      logger.error('Failed to sync VendorSelection after auto-assign', {
        eventId: String(eventId).trim(),
        message: err.message,
      });
    }
  }

  return {
    assigned: updateResult?.modifiedCount === 1,
  };
};

const unassignPlanningManager = async (eventId) => {
  if (!eventId || !String(eventId).trim()) {
    throw createApiError(400, 'Event ID is required');
  }

  const planning = await Planning.findOne({ eventId: String(eventId).trim() });
  if (!planning) {
    throw createApiError(404, 'Planning not found');
  }

  planning.assignedManagerId = null;
  // Business rule: unassigning a manager should revert the planning back to an approval state,
  // not to IMMEDIATE ACTION (which can be inferred from payment flags in model hooks).
  planning.status = STATUS.PENDING_APPROVAL;

  // Bypass validate hooks so the model's status auto-recompute doesn't override the explicit status.
  await planning.save({ validateBeforeSave: false });
  logger.info(`Planning manager unassigned: ${planning.eventId} -> ${planning.status}`);

  // Best-effort: keep VendorSelection cleared and status recomputed.
  try {
    await vendorSelectionService.ensureForPlanning(planning);
  } catch (err) {
    logger.error('Failed to sync VendorSelection after manager unassignment', {
      eventId: planning.eventId,
      message: err.message,
    });
  }
  return planning;
};

/**
 * Manager dashboard: list plannings assigned to a specific manager.
 */
const getPlanningsForManager = async ({ managerId, limit = 200 } = {}) => {
  if (!managerId || !String(managerId).trim()) {
    throw createApiError(400, 'managerId is required');
  }

  const safeLimit = Math.min(500, Math.max(1, Number(limit) || 200));

  const baseSelect =
    'eventId eventTitle category eventType customEventType eventField eventBanner schedule eventDate createdAt authId assignedManagerId status isUrgent platformFeePaid isPaid depositPaid depositPaidAmountPaise depositPaidCurrency depositPaidAt vendorConfirmationPaid vendorConfirmationPaidAmountPaise vendorConfirmationPaidCurrency vendorConfirmationPaidAt remainingPaymentPaid remainingPaymentPaidAmountPaise remainingPaymentPaidCurrency remainingPaymentPaidAt fullPaymentPaid vendorSelectionId selectedServices selectedVendors tickets platformFee totalAmount';

  const plannings = await Planning.find({
    assignedManagerId: String(managerId).trim(),
  })
    .sort({ createdAt: -1 })
    .limit(safeLimit)
    .select(baseSelect)
    .lean();

  const cfg = await promoteConfigService.getFees();
  return (plannings || []).map((p) => normalizePlanningForApi(p, cfg.platformFee));
};

/**
 * Manager dashboard: list assigned planning applications awaiting approval.
 */
const getPlanningApplicationsForManager = async ({ managerId, limit = 200 } = {}) => {
  if (!managerId || !String(managerId).trim()) {
    throw createApiError(400, 'managerId is required');
  }

  const safeLimit = Math.min(500, Math.max(1, Number(limit) || 200));

  const baseSelect =
    'eventId eventTitle category eventType customEventType eventField eventBanner schedule eventDate createdAt authId assignedManagerId status isUrgent platformFeePaid isPaid depositPaid depositPaidAmountPaise depositPaidCurrency depositPaidAt vendorConfirmationPaid vendorConfirmationPaidAmountPaise vendorConfirmationPaidCurrency vendorConfirmationPaidAt remainingPaymentPaid remainingPaymentPaidAmountPaise remainingPaymentPaidCurrency remainingPaymentPaidAt fullPaymentPaid vendorSelectionId selectedServices selectedVendors tickets platformFee totalAmount';

  const plannings = await Planning.find({
    assignedManagerId: String(managerId).trim(),
    status: STATUS.PENDING_APPROVAL,
  })
    .sort({ createdAt: -1 })
    .limit(safeLimit)
    .select(baseSelect)
    .lean();

  const cfg = await promoteConfigService.getFees();
  return (plannings || []).map((p) => normalizePlanningForApi(p, cfg.platformFee));
};

/**
 * Update planning core details (Manager/Admin)
 */
const updatePlanningDetails = async ({ eventId, updates = {}, actorRole, actorManagerId } = {}) => {
  const trimmedEventId = String(eventId || '').trim();
  if (!trimmedEventId) throw createApiError(400, 'eventId is required');

  const planning = await Planning.findOne({ eventId: trimmedEventId });
  if (!planning) throw createApiError(404, 'Planning not found');

  const isAdmin = String(actorRole || '').toUpperCase() === 'ADMIN';
  if (!isAdmin) {
    const normalizedActorId = String(actorManagerId || '').trim();
    if (!normalizedActorId) throw createApiError(403, 'Manager identity is required');
    if (String(planning.assignedManagerId || '').trim() !== normalizedActorId) {
      throw createApiError(403, 'You are not assigned to this planning');
    }
  }

  const nextTitle = updates.eventTitle;
  const nextDescription = updates.eventDescription;
  const nextLocationName = updates.locationName;
  const setUpdates = {};

  if (typeof nextTitle === 'string' && nextTitle.trim()) {
    setUpdates.eventTitle = nextTitle.trim();
  }
  if (typeof nextDescription === 'string' && nextDescription.trim()) {
    setUpdates.eventDescription = nextDescription.trim();
  }
  if (typeof nextLocationName === 'string' && nextLocationName.trim()) {
    setUpdates['location.name'] = nextLocationName.trim();
  }

  if (Object.keys(setUpdates).length > 0) {
    await Planning.updateOne(
      { _id: planning._id },
      { $set: setUpdates }
    );
  }

  const refreshed = await Planning.findById(planning._id);
  if (!refreshed) throw createApiError(404, 'Planning not found');

  const cfg = await promoteConfigService.getFees();
  return normalizePlanningForApi(refreshed.toJSON(), cfg.platformFee);
};

/**
 * Sync reservation day for a planning owner.
 * This keeps planning.eventDate/schedule day aligned with vendor selection reservation day.
 */
const updatePlanningReservationDayForOwner = async ({ eventId, authId, day } = {}) => {
  const trimmedEventId = String(eventId || '').trim();
  const trimmedAuthId = String(authId || '').trim();
  const parsedDay = parseIstDayStart(day);

  if (!trimmedEventId) throw createApiError(400, 'eventId is required');
  if (!trimmedAuthId) throw createApiError(400, 'authId is required');
  if (!parsedDay) throw createApiError(400, 'day must be in YYYY-MM-DD format');

  const planning = await Planning.findOne({ eventId: trimmedEventId, authId: trimmedAuthId });
  if (!planning) throw createApiError(404, 'Planning not found');

  const blockedStatuses = new Set([STATUS.CONFIRMED, ...TERMINAL_STATUSES]);
  const currentStatus = String(planning.status || '').trim();
  if (blockedStatuses.has(currentStatus)) {
    throw createApiError(409, 'Cannot change date for finalized planning status');
  }

  const previousReservationDay = String(planning.category || '').trim() === CATEGORY.PUBLIC
    ? toIstDayString(planning?.schedule?.startAt)
    : toIstDayString(planning?.eventDate);
  const nextReservationDay = toIstDayString(parsedDay);

  if (String(planning.category || '').trim() === CATEGORY.PUBLIC) {
    const currentStart = toDateOrNull(planning?.schedule?.startAt);
    const currentEnd = toDateOrNull(planning?.schedule?.endAt);

    if (!currentStart) {
      throw createApiError(409, 'Public event schedule is missing start date');
    }

    const nextStart = shiftDateKeepingIstTime(nextReservationDay, currentStart);
    const durationMs = currentEnd ? Math.max(0, currentEnd.getTime() - currentStart.getTime()) : 0;
    const nextEnd = currentEnd ? new Date(nextStart.getTime() + durationMs) : null;

    const ticketStart = toDateOrNull(planning?.ticketAvailability?.startAt);
    const ticketEnd = toDateOrNull(planning?.ticketAvailability?.endAt);

    planning.schedule = {
      ...(planning.schedule || {}),
      startAt: nextStart,
      ...(nextEnd ? { endAt: nextEnd } : {}),
    };

    if (ticketStart || ticketEnd) {
      const nextTicketStart = ticketStart
        ? new Date(nextStart.getTime() + (ticketStart.getTime() - currentStart.getTime()))
        : null;
      const nextTicketEnd = ticketEnd
        ? new Date(nextStart.getTime() + (ticketEnd.getTime() - currentStart.getTime()))
        : null;

      planning.ticketAvailability = {
        ...(planning.ticketAvailability || {}),
        ...(nextTicketStart ? { startAt: nextTicketStart } : {}),
        ...(nextTicketEnd ? { endAt: nextTicketEnd } : {}),
      };
    }
  } else {
    planning.eventDate = parsedDay;
  }

  await planning.save();

  if (previousReservationDay && nextReservationDay && previousReservationDay !== nextReservationDay) {
    await vendorReservationService.reassignEventReservationsDay({
      eventId: trimmedEventId,
      fromDay: previousReservationDay,
      toDay: nextReservationDay,
    });
  }

  const cfg = await promoteConfigService.getFees();
  return normalizePlanningForApi(planning.toJSON(), cfg.platformFee);
};

/**
 * Add a CORE staff member to a planning event (Manager/Admin)
 */
const addPlanningCoreStaff = async ({ eventId, staffId, actorRole, actorManagerId } = {}) => {
  const trimmedEventId = String(eventId || '').trim();
  const trimmedStaffId = String(staffId || '').trim();
  if (!trimmedEventId) throw createApiError(400, 'eventId is required');
  if (!trimmedStaffId) throw createApiError(400, 'staffId is required');

  if (String(actorRole || '').toUpperCase() !== 'MANAGER') {
    throw createApiError(403, 'Only MANAGER can assign staff');
  }

  const planning = await Planning.findOne({ eventId: trimmedEventId });
  if (!planning) throw createApiError(404, 'Planning not found');

  const planningStatus = String(planning.status || '').trim();
  if (TERMINAL_STATUSES.includes(planningStatus)) {
    throw createApiError(409, 'Staff cannot be assigned for completed or closed planning events');
  }

  const normalizedActorId = String(actorManagerId || '').trim();
  if (!normalizedActorId) throw createApiError(403, 'Manager identity is required');
  if (String(planning.assignedManagerId || '').trim() !== normalizedActorId) {
    throw createApiError(403, 'You are not assigned to this planning');
  }

  const staffUser = await assertCoreStaffEligibleForPlanning({ staffId: trimmedStaffId });
  const targetRange = planningToRange(planning);

  await assertCoreStaffAvailableAcrossEvents({
    staffId: trimmedStaffId,
    targetRange,
    planningEventIdToExclude: trimmedEventId,
  });

  const existing = Array.isArray(planning.coreStaffIds) ? planning.coreStaffIds.map(String) : [];
  if (!existing.includes(trimmedStaffId)) {
    planning.coreStaffIds = [...existing, trimmedStaffId];
    await planning.save({ validateBeforeSave: false });
  }

  const staffAuthId = String(staffUser?.authId || '').trim();
  if (staffAuthId) {
    ensureEventChatSeeded({
      eventId: planning.eventId,
      userAuthId: planning.authId,
      managerAuthId: staffAuthId,
    });
  }

  const cfg = await promoteConfigService.getFees();
  return normalizePlanningForApi(planning.toJSON(), cfg.platformFee);
};

/**
 * Remove a CORE staff member from a planning event (Manager/Admin)
 */
const removePlanningCoreStaff = async ({ eventId, staffId, actorRole, actorManagerId } = {}) => {
  const trimmedEventId = String(eventId || '').trim();
  const trimmedStaffId = String(staffId || '').trim();
  if (!trimmedEventId) throw createApiError(400, 'eventId is required');
  if (!trimmedStaffId) throw createApiError(400, 'staffId is required');

  if (String(actorRole || '').toUpperCase() !== 'MANAGER') {
    throw createApiError(403, 'Only MANAGER can remove staff');
  }

  const planning = await Planning.findOne({ eventId: trimmedEventId });
  if (!planning) throw createApiError(404, 'Planning not found');

  const planningStatus = String(planning.status || '').trim();
  if (TERMINAL_STATUSES.includes(planningStatus)) {
    throw createApiError(409, 'Staff cannot be removed for completed or closed planning events');
  }

  const normalizedActorId = String(actorManagerId || '').trim();
  if (!normalizedActorId) throw createApiError(403, 'Manager identity is required');
  if (String(planning.assignedManagerId || '').trim() !== normalizedActorId) {
    throw createApiError(403, 'You are not assigned to this planning');
  }

  const existing = Array.isArray(planning.coreStaffIds) ? planning.coreStaffIds.map(String) : [];
  const next = existing.filter((x) => x !== trimmedStaffId);
  planning.coreStaffIds = next;
  await planning.save({ validateBeforeSave: false });

  const cfg = await promoteConfigService.getFees();
  return normalizePlanningForApi(planning.toJSON(), cfg.platformFee);
};

/**
 * Release generated ticket revenue to user (demo flow) for public planning events.
 */
const releasePlanningGeneratedRevenuePayout = async ({ eventId, actorRole, actorAuthId, actorManagerId, mode = 'DEMO' } = {}) => {
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

  const planning = await Planning.findOne({ eventId: trimmedEventId });
  if (!planning) throw createApiError(404, 'Planning not found');
  const ownerAuthId = String(planning.authId || '').trim();

  const category = String(planning.category || '').trim().toLowerCase();
  if (category !== CATEGORY.PUBLIC) {
    throw createApiError(409, 'Generated revenue payout is supported only for public planning events');
  }

  if (normalizedRole === 'MANAGER') {
    const normalizedActorManagerId = String(actorManagerId || '').trim();
    if (!normalizedActorManagerId) throw createApiError(403, 'Manager identity is required');
    if (String(planning.assignedManagerId || '').trim() !== normalizedActorManagerId) {
      throw createApiError(403, 'You are not assigned to this planning');
    }
  }

  const existingStatus = String(planning?.generatedRevenuePayout?.status || '').trim().toUpperCase();
  if (existingStatus === 'SUCCESS') {
    const cfg = await promoteConfigService.getFees();
    const normalizedExisting = normalizePlanningForApi(planning.toJSON(), cfg.platformFee);
    normalizedExisting.ticketSalesStats = await buildPlanningTicketSalesStats(normalizedExisting, cfg);
    normalizedExisting.generatedRevenuePayoutSummary = {
      generatedRevenueInr: toNonNegativeNumber(normalizedExisting?.ticketSalesStats?.grossRevenueInr),
      totalVendorCostInr: await computePlanningVendorCostInr(trimmedEventId),
      totalFeesInr: toNonNegativeNumber(normalizedExisting?.ticketSalesStats?.totalFeesInr),
      payoutAmountInr: Number((Number(planning?.generatedRevenuePayout?.amountPaise || 0) / 100).toFixed(2)),
      mode: normalizedMode,
      alreadyProcessed: true,
    };
    return normalizedExisting;
  }

  const cfg = await promoteConfigService.getFees();
  const normalizedPlanning = normalizePlanningForApi(planning.toJSON(), cfg.platformFee);
  const ticketSalesStats = await buildPlanningTicketSalesStats(normalizedPlanning, cfg);

  const generatedRevenueInr = toNonNegativeNumber(ticketSalesStats?.grossRevenueInr);
  const totalVendorCostInr = await computePlanningVendorCostInr(trimmedEventId);
  const totalFeesInr = toNonNegativeNumber(ticketSalesStats?.totalFeesInr ?? ticketSalesStats?.platformFeeInr);
  const payoutAmountInr = Number(Math.max(0, generatedRevenueInr - totalVendorCostInr - totalFeesInr).toFixed(2));
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
          totalVendorCostPaise: Math.round(totalVendorCostInr * 100),
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

  planning.generatedRevenuePayout = {
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

  await planning.save({ validateBeforeSave: false });

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
      logger.error('Failed to publish USER_REVENUE_PAYOUT_SUCCESS from planning payout:', kafkaError);
    }
  }

  const normalizedUpdated = normalizePlanningForApi(planning.toJSON(), cfg.platformFee);
  normalizedUpdated.ticketSalesStats = ticketSalesStats;
  normalizedUpdated.generatedRevenuePayoutSummary = {
    generatedRevenueInr,
    totalVendorCostInr,
    totalFeesInr,
    payoutAmountInr,
    mode: normalizedMode,
    alreadyProcessed: false,
  };

  return normalizedUpdated;
};

/**
 * Trigger EMAIL BLAST promotion action for public planning events.
 */
const triggerPlanningEmailBlastPromotionAction = async ({ eventId, actorRole, actorAuthId, actorManagerId } = {}) => {
  const trimmedEventId = String(eventId || '').trim();
  if (!trimmedEventId) throw createApiError(400, 'eventId is required');

  const normalizedRole = String(actorRole || '').trim().toUpperCase();
  if (!['MANAGER', 'ADMIN'].includes(normalizedRole)) {
    throw createApiError(403, 'Only MANAGER or ADMIN can trigger email blast');
  }

  const planning = await Planning.findOne({ eventId: trimmedEventId });
  if (!planning) throw createApiError(404, 'Planning not found');

  const category = String(planning.category || '').trim().toLowerCase();
  if (category !== CATEGORY.PUBLIC) {
    throw createApiError(409, 'Email blast promotion is supported only for public planning events');
  }

  if (normalizedRole === 'MANAGER') {
    const normalizedActorManagerId = String(actorManagerId || '').trim();
    if (!normalizedActorManagerId) throw createApiError(403, 'Manager identity is required');
    if (String(planning.assignedManagerId || '').trim() !== normalizedActorManagerId) {
      throw createApiError(403, 'You are not assigned to this planning');
    }
  }

  if (!hasPromotionSelected(planning.promotionType, 'Email Blast')) {
    throw createApiError(409, 'Email Blast is not selected for this planning event');
  }

  const requestId = `PEB-${trimmedEventId}-${Date.now()}`;
  const requestedAt = new Date().toISOString();
  const eventDate = planning?.schedule?.startAt || planning?.eventDate || null;
  const parsedEventDate = eventDate ? new Date(eventDate) : null;
  const eventDateIso = parsedEventDate && !Number.isNaN(parsedEventDate.getTime())
    ? parsedEventDate.toISOString()
    : null;
  const eventLocation = planning?.location?.name || planning?.location || null;

  await publishEvent('PROMOTION_EMAIL_BLAST_REQUESTED', {
    requestId,
    eventId: trimmedEventId,
    eventType: 'planning',
    promotionType: 'EMAIL_BLAST',
    requestedByAuthId: String(actorAuthId || '').trim() || null,
    requestedByRole: normalizedRole,
    requestedAt,
    eventTitle: String(planning?.eventTitle || '').trim() || null,
    eventDescription: String(planning?.eventDescription || '').trim() || null,
    eventDate: eventDateIso,
    eventLocation: eventLocation ? String(eventLocation).trim() : null,
    eventBannerUrl: String(planning?.eventBanner?.url || planning?.eventBanner || '').trim() || null,
  });

  return {
    requestId,
    eventId: trimmedEventId,
    eventType: 'planning',
    promotionType: 'EMAIL_BLAST',
    requestedAt,
  };
};

/**
 * Submit post-completion feedback for planning event (Owner)
 */
const submitPlanningFeedback = async ({ eventId, authId, platformFeedback, vendorFeedback } = {}) => {
  const trimmedEventId = String(eventId || '').trim();
  const trimmedAuthId = String(authId || '').trim();

  if (!trimmedEventId) throw createApiError(400, 'eventId is required');
  if (!trimmedAuthId) throw createApiError(401, 'Authentication required');

  const planning = await Planning.findOne({ eventId: trimmedEventId });
  if (!planning) throw createApiError(404, 'Planning not found');

  if (String(planning.authId || '').trim() !== trimmedAuthId) {
    throw createApiError(403, 'Only the event owner can submit feedback');
  }

  const lifecycleStatus = String(planning.status || '').trim();
  const canSubmitFeedbackStatus = new Set([
    STATUS.COMPLETED,
    STATUS.VENDOR_PAYMENT_PENDING,
    STATUS.CLOSED,
  ]);
  if (!canSubmitFeedbackStatus.has(lifecycleStatus)) {
    throw createApiError(409, 'Feedback can be submitted only after event completion');
  }

  if (!planning.remainingPaymentPaid && lifecycleStatus !== STATUS.CLOSED) {
    throw createApiError(409, 'Feedback can be submitted after the final payment is completed');
  }

  const existingPlatformRating = Number(planning?.feedback?.platform?.rating || 0);
  const existingVendorFeedback = Array.isArray(planning?.feedback?.vendors) ? planning.feedback.vendors : [];
  if ((Number.isFinite(existingPlatformRating) && existingPlatformRating > 0) || existingVendorFeedback.length > 0) {
    throw createApiError(409, 'Feedback has already been submitted and cannot be updated');
  }

  const platformRatingRaw = Number(platformFeedback?.rating);
  const platformRating = Number.isFinite(platformRatingRaw) ? Math.round(platformRatingRaw) : NaN;
  if (!Number.isFinite(platformRating) || platformRating < 1 || platformRating > 5) {
    throw createApiError(400, 'platformFeedback.rating must be between 1 and 5');
  }

  const platformReview = String(platformFeedback?.review || '').trim();
  if (!platformReview) {
    throw createApiError(400, 'platformFeedback.review is required');
  }

  const normalizedPlatformFeedback = {
    rating: platformRating,
    review: platformReview,
    submittedAt: new Date(),
  };

  const allowedPairs = new Set(
    (Array.isArray(planning.selectedVendors) ? planning.selectedVendors : [])
      .map((row) => {
        const vendorAuthId = String(row?.vendorAuthId || '').trim();
        const service = String(row?.service || '').trim();
        if (!vendorAuthId || !service) return null;
        return `${vendorAuthId.toLowerCase()}::${service.toLowerCase()}`;
      })
      .filter(Boolean)
  );

  if (allowedPairs.size === 0) {
    throw createApiError(409, 'No opted vendors are available for feedback');
  }

  const incomingVendorFeedback = Array.isArray(vendorFeedback) ? vendorFeedback : [];
  if (incomingVendorFeedback.length !== allowedPairs.size) {
    throw createApiError(400, 'Feedback for every opted vendor is required');
  }

  const normalizedVendorFeedback = [];
  const seenPairs = new Set();
  for (const row of incomingVendorFeedback) {
    const vendorAuthId = String(row?.vendorAuthId || '').trim();
    const service = String(row?.service || '').trim();
    const pairKey = `${vendorAuthId.toLowerCase()}::${service.toLowerCase()}`;
    if (!vendorAuthId || !service || !allowedPairs.has(pairKey)) {
      throw createApiError(400, 'vendorFeedback contains vendor/service that is not part of opted vendors');
    }
    if (seenPairs.has(pairKey)) {
      throw createApiError(400, 'Duplicate vendor feedback entries are not allowed');
    }
    seenPairs.add(pairKey);

    const ratingRaw = Number(row?.rating);
    const rating = Number.isFinite(ratingRaw) ? Math.round(ratingRaw) : NaN;
    if (!Number.isFinite(rating) || rating < 1 || rating > 5) {
      throw createApiError(400, 'Each vendor feedback rating must be between 1 and 5');
    }

    const review = String(row?.review || '').trim();
    if (!review) {
      throw createApiError(400, 'Each vendor feedback review is required');
    }

    normalizedVendorFeedback.push({
      vendorAuthId,
      service,
      rating,
      review,
      submittedAt: new Date(),
    });
  }

  if (seenPairs.size !== allowedPairs.size) {
    throw createApiError(400, 'Feedback for every opted vendor is required');
  }

  planning.feedback = {
    platform: normalizedPlatformFeedback,
    vendors: normalizedVendorFeedback,
  };

  await planning.save({ validateBeforeSave: false });

  const cfg = await promoteConfigService.getFees();
  return normalizePlanningForApi(planning.toJSON(), cfg.platformFee, 'USER');
};

/**
 * Delete a planning by eventId (Owner or Admin)
 */
const deletePlanning = async (eventId) => {
  if (!eventId) {
    throw createApiError(400, "Event ID is required");
  }

  const planning = await Planning.findOneAndDelete({ eventId: eventId.trim() });
  if (!planning) {
    throw createApiError(404, "Planning not found");
  }

  logger.info(`Planning deleted: ${eventId}`);
  return { message: "Planning deleted successfully" };
};

/**
 * Get planning statistics (Admin/Manager)
 */
const getPlanningStats = async () => {
  const [total, byCategory, byStatus, urgentCount] = await Promise.all([
    Planning.countDocuments(),
    Planning.aggregate([{ $group: { _id: "$category", count: { $sum: 1 } } }]),
    Planning.aggregate([{ $group: { _id: "$status", count: { $sum: 1 } } }]),
    Planning.countDocuments({ isUrgent: true }),
  ]);

  return {
    total,
    urgent: urgentCount,
    byCategory: byCategory.reduce((acc, item) => {
      acc[item._id] = item.count;
      return acc;
    }, {}),
    byStatus: byStatus.reduce((acc, item) => {
      acc[item._id] = item.count;
      return acc;
    }, {}),
  };
};

/**
 * Mark planning as paid after verified payment event
 */
const markPlanningPaid = async (eventId) => {
  if (!eventId || eventId.trim() === '') {
    throw createApiError(400, 'Event ID is required');
  }

  const planning = await Planning.findOne({ eventId: eventId.trim() });
  if (!planning) {
    throw createApiError(404, 'Planning not found');
  }

  const alreadyPaid = Boolean(planning.platformFeePaid) || Boolean(planning.isPaid);
  if (!alreadyPaid) {
    planning.platformFeePaid = true;
    // Keep legacy flag in sync for any older consumers / DB rows.
    planning.isPaid = true;
    await planning.save();
    logger.info(`Planning marked as paid: ${eventId}`);
  }

  if (planning.status === STATUS.IMMEDIATE_ACTION) {
    try {
      await vendorSelectionService.ensureForPlanning(planning);
    } catch (err) {
      logger.error('Failed to ensure VendorSelection after markPlanningPaid', {
        eventId: planning.eventId,
        message: err.message,
      });
    }
  }

  return planning;
};

/**
 * Mark planning deposit as paid after verified deposit payment event.
 */
const markPlanningDepositPaid = async (eventId, { amountPaise = null, currency = null, paidAt = null } = {}) => {
  if (!eventId || eventId.trim() === '') {
    throw createApiError(400, 'Event ID is required');
  }

  const planning = await Planning.findOne({ eventId: eventId.trim() });
  if (!planning) {
    throw createApiError(404, 'Planning not found');
  }

  const nextAmount = amountPaise != null ? Number(amountPaise) : null;
  const nextCurrency = currency != null ? String(currency).trim() : null;
  const nextPaidAt = paidAt ? new Date(paidAt) : null;

  if (!planning.depositPaid) {
    planning.depositPaid = true;
  }

  if (nextAmount != null && Number.isFinite(nextAmount) && nextAmount >= 0) {
    if (planning.depositPaidAmountPaise == null) planning.depositPaidAmountPaise = Math.round(nextAmount);
  }
  if (nextCurrency && planning.depositPaidCurrency == null) {
    planning.depositPaidCurrency = nextCurrency;
  }
  if (nextPaidAt && planning.depositPaidAt == null && !Number.isNaN(nextPaidAt.getTime())) {
    planning.depositPaidAt = nextPaidAt;
  }

  await planning.save({ validateBeforeSave: false });
  logger.info(`Planning deposit marked as paid: ${eventId}`);

  // Once payment progress begins, reservations must become sticky (non-expiring).
  // Best-effort: claim/refresh reservations for each selected vendor on the planning day.
  try {
    await vendorSelectionService.ensureForPlanning(planning);
  } catch (err) {
    logger.warn('Failed to ensure VendorSelection while making reservations sticky (deposit)', {
      eventId: planning.eventId,
      message: err?.message,
    });
  }

  try {
    await ensureStickyVendorReservationsForPlanning(planning);
  } catch (err) {
    logger.error('Failed to refresh sticky vendor reservations after deposit payment', {
      eventId: planning.eventId,
      message: err?.message,
    });
  }

  return planning;
};

/**
 * Mark planning vendor confirmation payment as paid and transition to CONFIRMED.
 */
const markPlanningVendorConfirmationPaid = async (
  eventId,
  { amountPaise = null, currency = null, paidAt = null } = {}
) => {
  if (!eventId || eventId.trim() === '') {
    throw createApiError(400, 'Event ID is required');
  }

  const planning = await Planning.findOne({ eventId: eventId.trim() });
  if (!planning) {
    throw createApiError(404, 'Planning not found');
  }

  const nextAmount = amountPaise != null ? Number(amountPaise) : null;
  const nextCurrency = currency != null ? String(currency).trim() : null;
  const nextPaidAt = paidAt ? new Date(paidAt) : null;

  if (!planning.vendorConfirmationPaid) {
    planning.vendorConfirmationPaid = true;
  }

  if (nextAmount != null && Number.isFinite(nextAmount) && nextAmount >= 0) {
    if (planning.vendorConfirmationPaidAmountPaise == null) {
      planning.vendorConfirmationPaidAmountPaise = Math.round(nextAmount);
    }
  }
  if (nextCurrency && planning.vendorConfirmationPaidCurrency == null) {
    planning.vendorConfirmationPaidCurrency = nextCurrency;
  }
  if (nextPaidAt && planning.vendorConfirmationPaidAt == null && !Number.isNaN(nextPaidAt.getTime())) {
    planning.vendorConfirmationPaidAt = nextPaidAt;
  }

  if (String(planning.status || '').trim() !== STATUS.CONFIRMED) {
    planning.status = STATUS.CONFIRMED;
  }

  await planning.save({ validateBeforeSave: false });
  logger.info(`Planning vendor confirmation marked as paid: ${eventId} -> ${planning.status}`);

  // Vendor confirmation payment locks vendors for the date permanently.
  try {
    await vendorSelectionService.ensureForPlanning(planning);
  } catch (err) {
    logger.warn('Failed to ensure VendorSelection while making reservations sticky (vendor confirmation)', {
      eventId: planning.eventId,
      message: err?.message,
    });
  }

  try {
    await ensureStickyVendorReservationsForPlanning(planning);
  } catch (err) {
    logger.error('Failed to refresh sticky vendor reservations after vendor confirmation payment', {
      eventId: planning.eventId,
      message: err?.message,
    });
  }

  return planning;
};

/**
 * Mark private planning remaining payment as paid and move to VENDOR_PAYMENT_PENDING.
 */
const markPlanningRemainingPaymentPaid = async (
  eventId,
  { amountPaise = null, currency = null, paidAt = null } = {}
) => {
  if (!eventId || eventId.trim() === '') {
    throw createApiError(400, 'Event ID is required');
  }

  const planning = await Planning.findOne({ eventId: eventId.trim() });
  if (!planning) {
    throw createApiError(404, 'Planning not found');
  }

  if (String(planning.category || '').trim().toLowerCase() !== CATEGORY.PRIVATE) {
    throw createApiError(409, 'Remaining payment flow is currently supported only for private planning events');
  }

  const currentStatus = String(planning.status || '').trim();
  if (currentStatus !== STATUS.COMPLETED && currentStatus !== STATUS.VENDOR_PAYMENT_PENDING) {
    throw createApiError(409, 'Remaining payment can only be completed after event status is COMPLETED');
  }

  const nextAmount = amountPaise != null ? Number(amountPaise) : null;
  const nextCurrency = currency != null ? String(currency).trim() : null;
  const nextPaidAt = paidAt ? new Date(paidAt) : null;

  planning.remainingPaymentPaid = true;

  if (nextAmount != null && Number.isFinite(nextAmount) && nextAmount >= 0) {
    planning.remainingPaymentPaidAmountPaise = Math.round(nextAmount);
  }
  if (nextCurrency) {
    planning.remainingPaymentPaidCurrency = nextCurrency;
  }
  if (nextPaidAt && !Number.isNaN(nextPaidAt.getTime())) {
    planning.remainingPaymentPaidAt = nextPaidAt;
  }

  if (currentStatus !== STATUS.VENDOR_PAYMENT_PENDING) {
    planning.status = STATUS.VENDOR_PAYMENT_PENDING;
  }

  await planning.save({ validateBeforeSave: false });
  logger.info(`Planning remaining payment marked as paid: ${eventId} -> ${planning.status}`);

  return planning;
};

/**
 * Sync planning payment completion status from vendor payout records.
 * - If all accepted+locked vendor services are paid: CLOSED
 * - If partially paid while confirmed: VENDOR_PAYMENT_PENDING
 */
const syncPlanningStatusAfterVendorPayout = async (eventId) => {
  const normalizedEventId = String(eventId || '').trim();
  if (!normalizedEventId) {
    throw createApiError(400, 'Event ID is required');
  }

  const planning = await Planning.findOne({ eventId: normalizedEventId });
  if (!planning) {
    throw createApiError(404, 'Planning not found');
  }

  const selection = await VendorSelection.findOne({ eventId: normalizedEventId })
    .select('vendors')
    .lean();

  const vendors = Array.isArray(selection?.vendors) ? selection.vendors : [];
  const requiredPayoutKeys = vendors
    .filter((row) => String(row?.status || '').trim().toUpperCase() === 'ACCEPTED')
    .filter((row) => Boolean(row?.priceLocked))
    .map((row) => toVendorServicePayoutKey({ vendorAuthId: row?.vendorAuthId, service: row?.service }))
    .filter(Boolean);

  if (requiredPayoutKeys.length === 0) {
    return planning;
  }

  let eventPayoutRows = [];
  try {
    eventPayoutRows = await fetchVendorPayoutsForEventFromOrderService(normalizedEventId);
  } catch (error) {
    logger.warn('Failed to sync planning payout status from order-service', {
      eventId: normalizedEventId,
      message: error?.message || String(error),
    });
    return planning;
  }

  const successfulPayoutKeys = new Set(
    (Array.isArray(eventPayoutRows) ? eventPayoutRows : [])
      .filter((row) => String(row?.status || '').trim().toUpperCase() === 'SUCCESS')
      .map((row) => toVendorServicePayoutKey({ vendorAuthId: row?.vendorAuthId, service: row?.service }))
      .filter(Boolean)
  );

  const allVendorsPaid = requiredPayoutKeys.every((key) => successfulPayoutKeys.has(key));
  const currentStatus = String(planning.status || '').trim();

  if (allVendorsPaid) {
    if (currentStatus !== STATUS.CLOSED || !planning.fullPaymentPaid) {
      planning.status = STATUS.CLOSED;
      planning.fullPaymentPaid = true;
      await planning.save({ validateBeforeSave: false });
      logger.info('Planning moved to CLOSED after all vendor payouts completed', {
        eventId: normalizedEventId,
        requiredPayoutCount: requiredPayoutKeys.length,
      });
    }
    return planning;
  }

  if (currentStatus === STATUS.CONFIRMED) {
    planning.status = STATUS.VENDOR_PAYMENT_PENDING;
    await planning.save({ validateBeforeSave: false });
    logger.info('Planning moved to VENDOR_PAYMENT_PENDING after partial vendor payouts', {
      eventId: normalizedEventId,
      requiredPayoutCount: requiredPayoutKeys.length,
      successfulPayoutCount: successfulPayoutKeys.size,
    });
  }

  return planning;
};

/**
 * Confirm a planning selection (Owner)
 * - Sets planning.status to PENDING_APPROVAL
 * - Ensures VendorSelection exists and snapshots selected vendors onto planning.selectedVendors
 *
 * Uses validateBeforeSave=false to avoid blocking confirmation on legacy/partial public fields
 * (e.g., ticketAvailability date rules) when we're not modifying those fields.
 */
const confirmPlanning = async ({ eventId, authId }) => {
  if (!eventId || !String(eventId).trim()) {
    throw createApiError(400, 'Event ID is required');
  }
  if (!authId || !String(authId).trim()) {
    throw createApiError(400, 'Auth ID is required');
  }

  const planning = await Planning.findOne({ eventId: String(eventId).trim(), authId: String(authId).trim() });
  if (!planning) {
    throw createApiError(404, 'Planning not found');
  }

  // Ensure vendorSelectionId exists on planning
  await vendorSelectionService.ensureForPlanning(planning);

  // Recompute VendorSelection totals/status at confirm time.
  // This ensures totalMinAmount/totalMaxAmount reflect any latest per-service pricing.
  const selectionDoc = await VendorSelection.findOne({ eventId: planning.eventId });
  if (selectionDoc) {
    await selectionDoc.save();
  }

  const selection = selectionDoc ? selectionDoc.toObject() : null;
  const selectedVendors = Array.isArray(selection?.vendors)
    ? selection.vendors
        .filter((v) => v?.vendorAuthId)
        .map((v) => ({
          service: String(v.service || '').trim(),
          vendorAuthId: String(v.vendorAuthId || '').trim(),
        }))
        .filter((v) => v.service && v.vendorAuthId)
    : [];

  planning.status = STATUS.PENDING_APPROVAL;
  planning.selectedVendors = selectedVendors;

  await planning.save({ validateBeforeSave: false });
  logger.info(`Planning confirmed: ${planning.eventId} -> ${planning.status}`);
  return planning;
};

/**
 * Admin dashboard lists for Planning requests.
 * - assigned: manager assigned, not rejected
 * - applications: manager not assigned, not completed/rejected, and has progressed beyond draft
 * - rejected: explicitly rejected
 */
const getAdminDashboard = async ({ limit = 200 } = {}) => {
  const safeLimit = Math.min(500, Math.max(1, Number(limit) || 200));

  const progressedGate = { $or: [{ vendorSelectionId: { $ne: null } }, { platformFeePaid: true }, { isPaid: true }] };
  const baseSelect =
    'eventId eventTitle category eventType customEventType eventField eventBanner schedule eventDate createdAt authId assignedManagerId status isUrgent platformFeePaid isPaid depositPaid fullPaymentPaid vendorSelectionId';

  const [assigned, applications, rejected] = await Promise.all([
    Planning.find({
      assignedManagerId: { $ne: null },
      status: { $ne: STATUS.REJECTED },
    })
      .sort({ createdAt: -1 })
      .limit(safeLimit)
      .select(baseSelect)
      .lean(),
    Planning.find({
      assignedManagerId: null,
      status: { $nin: TERMINAL_STATUSES },
      ...progressedGate,
    })
      .sort({ createdAt: -1 })
      .limit(safeLimit)
      .select(baseSelect)
      .lean(),
    Planning.find({
      status: STATUS.REJECTED,
    })
      .sort({ createdAt: -1 })
      .limit(safeLimit)
      .select(baseSelect)
      .lean(),
  ]);

  const cfg = await promoteConfigService.getFees();

  const normalizeList = (items) => (items || []).map((p) => normalizePlanningForApi(p, cfg.platformFee));

  return {
    assigned: normalizeList(assigned),
    applications: normalizeList(applications),
    rejected: normalizeList(rejected),
  };
};

module.exports = {
  createPlanning,
  getPlanningsByAuthId,
  getPlanningByEventId,
  getAllPlannings,
  updatePlanningStatus,
  markPlanningAsComplete,
  assignPlanningManager,
  tryAutoAssignPlanningManager,
  unassignPlanningManager,
  deletePlanning,
  getPlanningStats,
  markPlanningPaid,
  markPlanningDepositPaid,
  markPlanningVendorConfirmationPaid,
  markPlanningRemainingPaymentPaid,
  syncPlanningStatusAfterVendorPayout,
  confirmPlanning,
  getAdminDashboard,
  getPlanningsForManager,
  getPlanningApplicationsForManager,
  updatePlanningDetails,
  updatePlanningReservationDayForOwner,
  addPlanningCoreStaff,
  removePlanningCoreStaff,
  releasePlanningGeneratedRevenuePayout,
  triggerPlanningEmailBlastPromotionAction,
  submitPlanningFeedback,
};
