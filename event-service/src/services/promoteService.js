const Promote = require('../models/Promote');
const Planning = require('../models/Planning');
const UserEventTicket = require('../models/UserEventTicket');
const axios = require('axios');
const createApiError = require('../utils/ApiError');
const logger = require('../utils/logger');
const { PROMOTE_STATUS, ADMIN_DECISION_STATUS } = require('../utils/promoteConstants');
const { TERMINAL_STATUSES: PLANNING_TERMINAL_STATUSES } = require('../utils/planningConstants');
const { USER_TICKET_STATUS, USER_TICKET_VERIFICATION_STATUS } = require('../utils/ticketConstants');
const promoteConfigService = require('./promoteConfigService');
const mongoose = require('mongoose');
const { fetchUserById, fetchActiveManagers, fetchUserByAuthId } = require('./userServiceClient');
const { ensureEventChatSeeded } = require('./chatSeedService');
const { publishEvent } = require('../kafka/eventProducer');

const defaultOrderServiceUrl = process.env.SERVICE_HOST
  ? 'http://order-service:8087'
  : 'http://localhost:8087';
const orderServiceUrl = process.env.ORDER_SERVICE_URL || defaultOrderServiceUrl;
const notificationServiceUrl = (
  process.env.NOTIFICATION_SERVICE_URL
  || (process.env.SERVICE_HOST ? 'http://notification-service:8088' : 'http://localhost:8088')
).replace(/\/$/, '');
const upstreamTimeoutMs = parseInt(process.env.UPSTREAM_HTTP_TIMEOUT_MS || '10000', 10);

const buildInternalOrderServiceHeaders = () => ({
  'x-auth-id': 'event-service',
  'x-user-id': '',
  'x-user-email': '',
  'x-user-username': 'event-service',
  'x-user-role': 'MANAGER',
});

const DEFAULT_REFUND_TIMELINE_LABEL = '5-7 working days';
const PROMOTE_REFUND_REQUEST_STATUSES = {
  PENDING_REVIEW: 'PENDING_REVIEW',
  APPROVED: 'APPROVED',
  REJECTED: 'REJECTED',
  REFUNDED: 'REFUNDED',
};

const PROMOTE_REFUND_REASON_CODES = new Set([
  'CLIENT_CANCELLED',
  'VENDOR_UNAVAILABLE',
  'OKKAZO_FAILURE',
  'FORCE_MAJEURE',
]);

const PROMOTE_REFUND_SCENARIO_CODES = {
  WITHIN_24_HOURS: 'WITHIN_24_HOURS',
  BEFORE_TICKET_SALES: 'BEFORE_TICKET_SALES',
  AFTER_TICKET_SALES: 'AFTER_TICKET_SALES',
  OKKAZO_FAILURE: 'OKKAZO_FAILURE',
};

const PROMOTE_REFUND_SCENARIO_LABELS = {
  [PROMOTE_REFUND_SCENARIO_CODES.WITHIN_24_HOURS]: 'Cancellation within 24 hours',
  [PROMOTE_REFUND_SCENARIO_CODES.BEFORE_TICKET_SALES]: 'Cancellation before ticket sales',
  [PROMOTE_REFUND_SCENARIO_CODES.AFTER_TICKET_SALES]: 'Cancellation after ticket sales',
  [PROMOTE_REFUND_SCENARIO_CODES.OKKAZO_FAILURE]: 'Cancellation due to Okkazo failure',
};

const PROMOTE_LIABILITY_ORDER_PURPOSE = 'PROMOTE_LIABILITY_RECOVERY';

const REVENUE_OPS_ASSIGNED_ROLES = new Set([
  'REVENUE OPERATIONS SPECIALIST',
  'REVENUE OPERATION SPECIALIST',
  'REVENUE OPERATIONS SPECIALISTS',
  'REVENUE OPERATION SPECIALISTS',
]);

const resolveFrontendBaseUrl = () => {
  const fromEnv = String(process.env.FRONTEND_URL || process.env.FRONTEND_URL_FALLBACK || '').trim();
  if (fromEnv) return fromEnv.replace(/\/$/, '');
  return 'http://localhost:5173';
};

const buildSystemNotificationHeaders = () => {
  const authId = process.env.EVENT_SERVICE_SYSTEM_AUTH_ID || 'system:event-service';
  return {
    'x-auth-id': authId,
    'x-user-id': authId,
    'x-user-email': 'system@okkazo.local',
    'x-user-username': 'event-service',
    'x-user-role': 'ADMIN',
  };
};

let promoteRefundRoundRobinCursor = 0;

const normalizeId = (value) => String(value || '').trim();

const REQUIRED_PROMOTE_MANAGER_DEPARTMENT = 'Public Event';

const normalizeLoose = (value) => String(value || '').trim().toLowerCase();

const normalizeRoleToken = (value) => String(value || '')
  .trim()
  .toUpperCase()
  .replace(/[_-]+/g, ' ')
  .replace(/\s+/g, ' ');

const isRevenueOpsAssignedRole = (assignedRole) => REVENUE_OPS_ASSIGNED_ROLES.has(normalizeRoleToken(assignedRole));

const normalizePromoteRefundReasonCode = (value) => {
  const code = String(value || '').trim().toUpperCase();
  if (!PROMOTE_REFUND_REASON_CODES.has(code)) return 'CLIENT_CANCELLED';
  return code;
};

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

const isPromoteAdminApproved = (promote) => {
  return String(promote?.adminDecision?.status || '').trim().toUpperCase() === ADMIN_DECISION_STATUS.APPROVED;
};

const hasTicketAvailabilityStartedBy = (promote, at = new Date()) => {
  const ticketStart = promote?.ticketAvailability?.startAt ? new Date(promote.ticketAvailability.startAt) : null;
  if (!ticketStart || Number.isNaN(ticketStart.getTime())) return false;

  const atDate = at instanceof Date && !Number.isNaN(at.getTime()) ? at : new Date();
  return atDate.getTime() >= ticketStart.getTime();
};

const isPromoteLiabilityApplicableForCancellation = ({ promote, cancelledAt = new Date() } = {}) => {
  return isPromoteAdminApproved(promote) && hasTicketAvailabilityStartedBy(promote, cancelledAt);
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

const getPromoteGuestTicketRecipients = async ({ eventId } = {}) => {
  const normalizedEventId = String(eventId || '').trim();
  if (!normalizedEventId) return [];

  const rows = await UserEventTicket.find({
    eventId: normalizedEventId,
    eventSource: 'promote',
    ticketStatus: USER_TICKET_STATUS.SUCCESS,
  })
    .select('userAuthId')
    .lean();

  return Array.from(
    new Set(
      (Array.isArray(rows) ? rows : [])
        .map((row) => String(row?.userAuthId || '').trim())
        .filter(Boolean)
    )
  );
};

const sendPromoteGuestCancellationNotifications = async ({
  promote,
  recipientAuthIds = [],
  refundTimelineLabel = DEFAULT_REFUND_TIMELINE_LABEL,
} = {}) => {
  const normalizedRecipients = Array.isArray(recipientAuthIds)
    ? recipientAuthIds.map((id) => String(id || '').trim()).filter(Boolean)
    : [];

  if (normalizedRecipients.length === 0) {
    return {
      targetedGuests: 0,
      delivered: 0,
      failed: 0,
      failedRecipients: [],
    };
  }

  const normalizedEventId = String(promote?.eventId || '').trim();
  const eventTitle = String(promote?.eventTitle || '').trim() || `Event ${normalizedEventId}`;
  const actionUrl = `${resolveFrontendBaseUrl()}/user/ticket-management`;
  const title = `Event Cancelled & Refunded: ${eventTitle}`;
  const message = [
    `The event "${eventTitle}" has been cancelled by the organizer.`,
    `Your ticket refund has been initiated and is usually processed within ${refundTimelineLabel}.`,
  ].join(' ');

  const headers = buildSystemNotificationHeaders();
  const requests = normalizedRecipients.map((recipientAuthId) => axios.post(
    `${notificationServiceUrl}/system/send-to-user`,
    {
      recipientAuthId,
      recipientRole: 'USER',
      title,
      message,
      actionUrl,
      category: 'EVENT',
      type: 'EVENT_CANCELLED',
      metadata: {
        eventId: normalizedEventId,
        eventType: 'promote',
        eventTitle,
        source: 'event-service:promote-refund',
      },
    },
    {
      headers,
      timeout: 10_000,
    }
  ));

  const settled = await Promise.allSettled(requests);
  const failedRecipients = [];

  settled.forEach((result, index) => {
    if (result.status === 'fulfilled') return;
    const failedRecipient = normalizedRecipients[index];
    if (failedRecipient) failedRecipients.push(failedRecipient);

    logger.warn('Failed to send promote cancellation notification to guest', {
      eventId: normalizedEventId,
      recipientAuthId: failedRecipient || null,
      message: result?.reason?.response?.data?.message || result?.reason?.message,
    });
  });

  return {
    targetedGuests: normalizedRecipients.length,
    delivered: normalizedRecipients.length - failedRecipients.length,
    failed: failedRecipients.length,
    failedRecipients,
  };
};

const publishPromoteGuestCancellationEmailEvent = async ({
  promote,
  recipientAuthIds = [],
  refundTimelineLabel = DEFAULT_REFUND_TIMELINE_LABEL,
  cancellationReason = null,
} = {}) => {
  const normalizedRecipients = Array.isArray(recipientAuthIds)
    ? recipientAuthIds.map((id) => String(id || '').trim()).filter(Boolean)
    : [];

  if (normalizedRecipients.length === 0) {
    return {
      queued: false,
      queuedRecipients: 0,
    };
  }

  const eventId = String(promote?.eventId || '').trim();
  const eventTitle = String(promote?.eventTitle || '').trim() || `Event ${eventId}`;
  const eventDate = promote?.schedule?.startAt || null;
  const eventLocation = String(promote?.venue?.locationName || '').trim() || null;

  try {
    await publishEvent('PLANNING_EVENT_CANCELLED_FOR_GUESTS', {
      eventId,
      eventType: 'promote',
      eventTitle,
      eventDate,
      eventLocation,
      cancellationReason: String(cancellationReason || '').trim() || null,
      refundTimelineLabel: String(refundTimelineLabel || '').trim() || DEFAULT_REFUND_TIMELINE_LABEL,
      recipientAuthIds: normalizedRecipients,
      actionUrl: `${resolveFrontendBaseUrl()}/user/ticket-management`,
      cancelledAt: new Date().toISOString(),
      source: 'promote-refund-request',
    });

    return {
      queued: true,
      queuedRecipients: normalizedRecipients.length,
    };
  } catch (error) {
    logger.warn('Failed to queue promote cancellation guest email event', {
      eventId,
      recipients: normalizedRecipients.length,
      message: error?.message || String(error),
    });

    return {
      queued: false,
      queuedRecipients: 0,
      error: error?.message || String(error),
    };
  }
};

const fetchOrdersForEventFromOrderService = async (eventId) => {
  const normalizedEventId = String(eventId || '').trim();
  if (!normalizedEventId) return [];

  try {
    const response = await axios.get(
      `${orderServiceUrl}/orders/admin/${encodeURIComponent(normalizedEventId)}`,
      {
        timeout: upstreamTimeoutMs,
        headers: buildInternalOrderServiceHeaders(),
      }
    );

    const rows = response?.data?.data?.orders;
    return Array.isArray(rows) ? rows : [];
  } catch (error) {
    logger.warn('Unable to fetch payment orders from order-service while building promote refund result', {
      eventId: normalizedEventId,
      message: error?.response?.data?.message || error?.message || String(error),
    });
    return [];
  }
};

const resolvePromotePaymentSummary = ({ orders = [], promote } = {}) => {
  const normalizedOrders = Array.isArray(orders) ? orders : [];

  const promotePaymentOrder = normalizedOrders.find((row) => {
    const orderType = String(row?.orderType || '').trim().toUpperCase();
    const status = String(row?.status || '').trim().toUpperCase();
    return orderType === 'PROMOTE EVENT' && (status === 'PAID' || status === 'REFUNDED');
  }) || null;

  const rawPromotionUsedPaise = Number(promotePaymentOrder?.amount || 0);
  const promotionUsedAmountPaise = Number.isFinite(rawPromotionUsedPaise) && rawPromotionUsedPaise > 0
    ? Math.round(rawPromotionUsedPaise)
    : 0;

  const paidAtCandidate = promotePaymentOrder?.paidAt
    || promotePaymentOrder?.createdAt
    || promote?.createdAt
    || null;
  const paidAt = paidAtCandidate ? new Date(paidAtCandidate) : null;

  return {
    paymentOrder: promotePaymentOrder,
    promotionUsedAmountPaise,
    paidAt: paidAt && !Number.isNaN(paidAt.getTime()) ? paidAt : null,
  };
};

const determinePromoteRefundScenarioCode = ({
  reasonCode,
  within24Hours,
  ticketsSold,
} = {}) => {
  if (String(reasonCode || '').trim().toUpperCase() === 'OKKAZO_FAILURE') {
    return PROMOTE_REFUND_SCENARIO_CODES.OKKAZO_FAILURE;
  }

  if (Number(ticketsSold || 0) > 0) {
    return PROMOTE_REFUND_SCENARIO_CODES.AFTER_TICKET_SALES;
  }

  if (Boolean(within24Hours)) {
    return PROMOTE_REFUND_SCENARIO_CODES.WITHIN_24_HOURS;
  }

  return PROMOTE_REFUND_SCENARIO_CODES.BEFORE_TICKET_SALES;
};

const buildPromoteRefundResult = ({
  promote,
  reasonCode = 'CLIENT_CANCELLED',
  salesStats,
  promotionUsedAmountPaise = 0,
  paymentPaidAt = null,
  now = new Date(),
} = {}) => {
  const normalizedSalesStats = salesStats && typeof salesStats === 'object' ? salesStats : {};
  const grossRevenueInr = Number(normalizedSalesStats?.grossRevenueInr || 0);
  const grossRevenuePaise = Number.isFinite(grossRevenueInr) && grossRevenueInr > 0
    ? Math.round(grossRevenueInr * 100)
    : 0;

  const platformFeePercent = 2.5;
  const platformFeeAmountPaise = Math.max(0, Math.round(grossRevenuePaise * (platformFeePercent / 100)));
  const normalizedPromotionUsedPaise = Math.max(0, Math.round(Number(promotionUsedAmountPaise || 0)));

  const ticketsSold = Math.max(0, Number(normalizedSalesStats?.ticketsSold || 0));
  const ticketStart = promote?.ticketAvailability?.startAt ? new Date(promote.ticketAvailability.startAt) : null;
  const ticketSalesStarted = Boolean(ticketsSold > 0)
    || (ticketStart && !Number.isNaN(ticketStart.getTime()) ? now.getTime() >= ticketStart.getTime() : false);

  const paidAt = paymentPaidAt instanceof Date && !Number.isNaN(paymentPaidAt.getTime())
    ? paymentPaidAt
    : (promote?.createdAt ? new Date(promote.createdAt) : null);
  const within24Hours = paidAt && !Number.isNaN(paidAt.getTime())
    ? (now.getTime() - paidAt.getTime()) <= 24 * 60 * 60 * 1000
    : false;

  const normalizedReasonCode = normalizePromoteRefundReasonCode(reasonCode);
  const scenarioCode = determinePromoteRefundScenarioCode({
    reasonCode: normalizedReasonCode,
    within24Hours,
    ticketsSold,
  });
  const scenarioLabel = PROMOTE_REFUND_SCENARIO_LABELS[scenarioCode] || scenarioCode;

  let userRefundAmountPaise = 0;
  if (
    scenarioCode === PROMOTE_REFUND_SCENARIO_CODES.WITHIN_24_HOURS
    || scenarioCode === PROMOTE_REFUND_SCENARIO_CODES.BEFORE_TICKET_SALES
    || scenarioCode === PROMOTE_REFUND_SCENARIO_CODES.OKKAZO_FAILURE
  ) {
    userRefundAmountPaise = normalizedPromotionUsedPaise;
  }

  const totalFeesInr = toNonNegativeNumber(
    salesStats?.totalFeesInr
    ?? (toNonNegativeNumber(salesStats?.platformFeeInr) + toNonNegativeNumber(salesStats?.serviceChargeInr))
  );
  const totalFeesPaise = Math.round(totalFeesInr * 100);

  const liabilityApplicable = isPromoteLiabilityApplicableForCancellation({
    promote,
    cancelledAt: now,
  });

  let promoterLiabilityAmountPaise = liabilityApplicable && ticketsSold > 0 ? totalFeesPaise : 0;
  let platformLiabilityAmountPaise = 0;

  if (scenarioCode === PROMOTE_REFUND_SCENARIO_CODES.OKKAZO_FAILURE) {
    promoterLiabilityAmountPaise = 0;
    platformLiabilityAmountPaise = userRefundAmountPaise + grossRevenuePaise + totalFeesPaise;
  }

  return {
    scenarioCode,
    scenarioLabel,
    reasonCode: normalizedReasonCode,
    ticketsSold,
    ticketSalesStarted,
    within24Hours,
    grossRevenuePaise,
    platformFeePercent,
    platformFeeAmountPaise,
    promotionUsedAmountPaise: normalizedPromotionUsedPaise,
    grossPaidAmountPaise: userRefundAmountPaise,
    refundAmountPaise: userRefundAmountPaise,
    deductionAmountPaise: Math.max(0, normalizedPromotionUsedPaise - userRefundAmountPaise),
    deductionPercent: normalizedPromotionUsedPaise > 0
      ? Number((((normalizedPromotionUsedPaise - userRefundAmountPaise) / normalizedPromotionUsedPaise) * 100).toFixed(2))
      : 0,
    userRefundAmountPaise,
    promoterLiabilityAmountPaise,
    platformLiabilityAmountPaise,
    timelineLabel: DEFAULT_REFUND_TIMELINE_LABEL,
    currency: 'INR',
  };
};

const pickRevenueOpsAssignee = async () => {
  const managers = await fetchActiveManagers({ limit: 1000 });
  const eligibleManagers = (Array.isArray(managers) ? managers : [])
    .filter((manager) => normalizeLoose(manager?.role) === 'manager')
    .filter((manager) => manager?.isActive !== false)
    .filter((manager) => isRevenueOpsAssignedRole(manager?.assignedRole))
    .map((manager) => ({
      id: String(manager?._id || manager?.id || '').trim(),
      authId: String(manager?.authId || '').trim(),
      assignedRole: String(manager?.assignedRole || '').trim(),
      name: String(manager?.name || manager?.fullName || manager?.username || '').trim(),
    }))
    .filter((manager) => manager.id);

  if (eligibleManagers.length === 0) {
    throw createApiError(409, 'No active Revenue Operations Specialist is available right now');
  }

  const rankedManagers = [...eligibleManagers].sort((a, b) => {
    const nameCmp = String(a.name || '').localeCompare(String(b.name || ''));
    if (nameCmp !== 0) return nameCmp;
    return a.id.localeCompare(b.id);
  });

  const selectedIndex = promoteRefundRoundRobinCursor % rankedManagers.length;
  promoteRefundRoundRobinCursor = (selectedIndex + 1) % rankedManagers.length;
  return rankedManagers[selectedIndex];
};

const resolveRevenueOpsManagerContext = async ({ authId, role }) => {
  const normalizedRole = String(role || '').trim().toUpperCase();
  if (normalizedRole === 'ADMIN') {
    return {
      isAdmin: true,
      managerId: null,
      authId: String(authId || '').trim() || null,
      assignedRole: 'ADMIN',
    };
  }

  if (normalizedRole !== 'MANAGER') {
    throw createApiError(403, 'Only manager or admin users can access refund requests');
  }

  const normalizedAuthId = String(authId || '').trim();
  if (!normalizedAuthId) {
    throw createApiError(401, 'Manager authentication information missing');
  }

  const user = await fetchUserByAuthId(normalizedAuthId);
  if (!user) {
    throw createApiError(404, 'Manager profile not found');
  }

  const managerId = String(user?._id || user?.id || '').trim();
  if (!managerId) {
    throw createApiError(404, 'Manager profile not found');
  }

  if (!isRevenueOpsAssignedRole(user?.assignedRole)) {
    throw createApiError(403, 'Only Revenue Operations Specialist managers can access refund requests');
  }

  return {
    isAdmin: false,
    managerId,
    authId: normalizedAuthId,
    assignedRole: String(user?.assignedRole || '').trim() || null,
  };
};

const processPromoteGuestTicketBulkRefund = async ({
  eventId,
  recipientAuthIds = [],
  reasonCode = 'CLIENT_CANCELLED',
  managerAuthId = null,
  managerNotes = null,
  scenarioCode = null,
} = {}) => {
  const normalizedEventId = String(eventId || '').trim();
  const normalizedRecipients = Array.isArray(recipientAuthIds)
    ? recipientAuthIds.map((id) => String(id || '').trim()).filter(Boolean)
    : [];

  if (!normalizedEventId) {
    return {
      skipped: true,
      reason: 'Missing event id for ticket refund',
      totalOrders: 0,
      refundedCount: 0,
      failedCount: 0,
    };
  }

  const payload = {
    eventId: normalizedEventId,
    reasonCode: normalizePromoteRefundReasonCode(reasonCode),
    notes: {
      source: 'promote-cancellation-auto-bulk-refund',
      managerAuthId: String(managerAuthId || '').trim() || null,
      managerNotes: String(managerNotes || '').trim() || null,
      scenarioCode: String(scenarioCode || '').trim() || null,
    },
  };

  if (normalizedRecipients.length > 0) {
    payload.authIds = normalizedRecipients;
  }

  try {
    const response = await axios.post(
      `${orderServiceUrl}/orders/refund/event-ticket-sales`,
      payload,
      {
        timeout: upstreamTimeoutMs,
        headers: buildInternalOrderServiceHeaders(),
      }
    );

    return response?.data?.data || {
      skipped: false,
      totalOrders: 0,
      refundedCount: 0,
      failedCount: 0,
    };
  } catch (error) {
    const message = String(error?.response?.data?.message || error?.message || 'Failed to trigger promote bulk ticket refund');
    return {
      skipped: false,
      totalOrders: 0,
      refundedCount: 0,
      failedCount: Math.max(1, normalizedRecipients.length),
      error: message,
    };
  }
};

const processPromoteOwnerRefund = async ({
  eventId,
  ownerAuthId,
  refundAmountPaise,
  reasonCode = 'CLIENT_CANCELLED',
  initiatedByAuthId = null,
  managerNotes = null,
  scenarioCode = null,
} = {}) => {
  const normalizedEventId = String(eventId || '').trim();
  const normalizedOwnerAuthId = String(ownerAuthId || '').trim();
  const normalizedReasonCode = normalizePromoteRefundReasonCode(reasonCode);

  if (!normalizedEventId || !normalizedOwnerAuthId) {
    return {
      skipped: true,
      reason: 'Missing owner refund identifiers',
      refundedAmount: 0,
      refundId: null,
      transactionId: null,
      refundedAt: null,
    };
  }

  const amountPaise = Math.max(0, Math.round(Number(refundAmountPaise || 0)));
  if (amountPaise <= 0) {
    return {
      skipped: true,
      reason: 'Owner refund amount is zero',
      refundedAmount: 0,
      refundId: null,
      transactionId: null,
      refundedAt: null,
    };
  }

  try {
    const response = await axios.post(
      `${orderServiceUrl}/orders/refund`,
      {
        eventId: normalizedEventId,
        authId: normalizedOwnerAuthId,
        amount: Number((amountPaise / 100).toFixed(2)),
        reasonCode: normalizedReasonCode,
        notes: {
          source: 'promote-cancellation-owner-refund',
          initiatedByAuthId: String(initiatedByAuthId || '').trim() || null,
          managerNotes: String(managerNotes || '').trim() || null,
          scenarioCode: String(scenarioCode || '').trim() || null,
        },
      },
      {
        timeout: upstreamTimeoutMs,
        headers: buildInternalOrderServiceHeaders(),
      }
    );

    const payload = response?.data?.data || {};
    return {
      skipped: false,
      refundedAmount: Math.max(0, Number(payload?.refundedAmount || 0)),
      refundId: String(payload?.refundId || '').trim() || null,
      transactionId: String(payload?.transactionId || '').trim() || null,
      refundedAt: payload?.refundedAt || null,
    };
  } catch (error) {
    const message = String(error?.response?.data?.message || error?.message || 'Failed to process owner refund');
    return {
      skipped: false,
      refundedAmount: 0,
      refundId: null,
      transactionId: null,
      refundedAt: null,
      error: message,
    };
  }
};

const toPaiseFromInr = (value) => {
  const amount = Number(value || 0);
  if (!Number.isFinite(amount) || amount <= 0) return 0;
  return Math.round(amount * 100);
};

const resolvePromoteLiabilityAmountPaise = ({ promote, fallbackSalesStats = null } = {}) => {
  const reasonCode = normalizePromoteRefundReasonCode(promote?.refundRequest?.reasonCode);
  if (reasonCode === 'OKKAZO_FAILURE') return 0;

  const requestedAtRaw = promote?.refundRequest?.requestedAt;
  const requestedAt = requestedAtRaw ? new Date(requestedAtRaw) : new Date();
  if (!isPromoteLiabilityApplicableForCancellation({ promote, cancelledAt: requestedAt })) {
    return 0;
  }

  const hasResultLiabilityField = Boolean(
    promote?.refundRequest?.result
    && Object.prototype.hasOwnProperty.call(promote.refundRequest.result, 'promoterLiabilityAmountPaise')
  );
  if (hasResultLiabilityField) {
    return Math.max(0, Number(promote?.refundRequest?.result?.promoterLiabilityAmountPaise || 0));
  }

  const ticketSalesStats = fallbackSalesStats || promote?.ticketSalesStats || null;
  const ticketsSold = toNonNegativeNumber(ticketSalesStats?.ticketsSold);
  if (ticketsSold <= 0) return 0;

  const totalFeesInr = toNonNegativeNumber(
    ticketSalesStats?.totalFeesInr
    ?? (toNonNegativeNumber(ticketSalesStats?.platformFeeInr) + toNonNegativeNumber(ticketSalesStats?.serviceChargeInr))
  );
  return toPaiseFromInr(totalFeesInr);
};

const processPromoteLiabilityRecoveryOrder = async ({
  eventId,
  ownerAuthId,
  amountPaise,
  initiatedByAuthId = null,
  reasonCode = 'CLIENT_CANCELLED',
  scenarioCode = null,
} = {}) => {
  const normalizedEventId = String(eventId || '').trim();
  const normalizedOwnerAuthId = String(ownerAuthId || '').trim();
  const normalizedReasonCode = normalizePromoteRefundReasonCode(reasonCode);
  const recoveryAmountPaise = Math.max(0, Math.round(Number(amountPaise || 0)));

  if (!normalizedEventId || !normalizedOwnerAuthId) {
    return {
      skipped: true,
      reason: 'Missing liability recovery identifiers',
      amountPaise: 0,
      paymentOrderId: null,
      razorpayOrderId: null,
      transactionId: null,
      keyId: null,
      currency: 'INR',
    };
  }

  if (recoveryAmountPaise <= 0) {
    return {
      skipped: true,
      reason: 'Liability recovery amount is zero',
      amountPaise: 0,
      paymentOrderId: null,
      razorpayOrderId: null,
      transactionId: null,
      keyId: null,
      currency: 'INR',
    };
  }

  try {
    const response = await axios.post(
      `${orderServiceUrl}/orders/create`,
      {
        eventId: normalizedEventId,
        authId: normalizedOwnerAuthId,
        orderType: 'PROMOTE EVENT',
        amount: Number((recoveryAmountPaise / 100).toFixed(2)),
        currency: 'INR',
        notes: {
          source: 'promote-cancellation-liability-recovery',
          orderPurpose: PROMOTE_LIABILITY_ORDER_PURPOSE,
          liabilityRecovery: true,
          initiatedByAuthId: String(initiatedByAuthId || '').trim() || null,
          reasonCode: normalizedReasonCode,
          scenarioCode: String(scenarioCode || '').trim() || null,
        },
      },
      {
        timeout: upstreamTimeoutMs,
        headers: buildInternalOrderServiceHeaders(),
      }
    );

    const payload = response?.data?.data || {};
    return {
      skipped: false,
      amountPaise: Math.max(0, Number(payload?.amount || recoveryAmountPaise)),
      paymentOrderId: String(payload?.orderId || '').trim() || null,
      razorpayOrderId: String(payload?.razorpayOrderId || '').trim() || null,
      transactionId: String(payload?.transactionId || '').trim() || null,
      keyId: String(payload?.keyId || '').trim() || null,
      currency: String(payload?.currency || 'INR').trim() || 'INR',
    };
  } catch (error) {
    return {
      skipped: false,
      amountPaise: recoveryAmountPaise,
      paymentOrderId: null,
      razorpayOrderId: null,
      transactionId: null,
      keyId: null,
      currency: 'INR',
      error: String(error?.response?.data?.message || error?.message || 'Failed to create liability recovery order'),
    };
  }
};

const sendPromoteOwnerLiabilityRecoveryNotification = async ({
  promote,
  ownerAuthId,
  amountPaise,
} = {}) => {
  const normalizedOwnerAuthId = String(ownerAuthId || '').trim();
  const eventId = String(promote?.eventId || '').trim();
  if (!normalizedOwnerAuthId || !eventId) {
    return { sent: false, reason: 'Missing liability notification context' };
  }

  const title = String(promote?.eventTitle || '').trim() || `Event ${eventId}`;
  const amountInr = Number((Math.max(0, Number(amountPaise || 0)) / 100).toFixed(2));
  const actionUrl = `${resolveFrontendBaseUrl()}/user/event-management`;

  try {
    await axios.post(
      `${notificationServiceUrl}/system/send-to-user`,
      {
        recipientAuthId: normalizedOwnerAuthId,
        recipientRole: 'USER',
        title: `Liability Payment Required: ${title}`,
        message: `Your event was cancelled and guest refunds were processed. Please pay the liability amount of INR ${amountInr.toFixed(2)} to settle platform fees.`,
        actionUrl,
        category: 'EVENT',
        type: 'LIABILITY_RECOVERY',
        metadata: {
          eventId,
          eventType: 'promote',
          amountPaise: Math.max(0, Number(amountPaise || 0)),
          source: 'event-service:promote-liability-recovery',
        },
      },
      {
        headers: buildSystemNotificationHeaders(),
        timeout: 10_000,
      }
    );

    return { sent: true };
  } catch (error) {
    logger.warn('Failed to send promote liability recovery notification to owner', {
      eventId,
      ownerAuthId: normalizedOwnerAuthId,
      message: error?.response?.data?.message || error?.message || String(error),
    });

    return { sent: false, error: error?.message || String(error) };
  }
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
    eventStatus: { $nin: [PROMOTE_STATUS.COMPLETE, PROMOTE_STATUS.CANCELLED, PROMOTE_STATUS.CLOSED] },
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
      eventStatus: { $nin: [PROMOTE_STATUS.COMPLETE, PROMOTE_STATUS.CANCELLED, PROMOTE_STATUS.CLOSED] },
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

  const promoteEventStatus = String(promote?.eventStatus || '').trim().toUpperCase();
  const promoteRefundStatus = String(promote?.refundRequest?.status || '').trim().toUpperCase();
  const cancelledStatuses = new Set(['CANCELLED', 'CANCELED', 'REFUNDED']);
  const refundBlockedStatuses = new Set([
    PROMOTE_REFUND_REQUEST_STATUSES.PENDING_REVIEW,
    PROMOTE_REFUND_REQUEST_STATUSES.APPROVED,
    PROMOTE_REFUND_REQUEST_STATUSES.REFUNDED,
  ]);

  if (cancelledStatuses.has(promoteEventStatus) || refundBlockedStatuses.has(promoteRefundStatus)) {
    throw createApiError(409, 'Generated revenue payout is disabled for cancelled promote events. Collect platform liability from the event creator.');
  }

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

const recoverPromoteCancellationLiability = async ({ eventId, actorRole, actorAuthId, actorManagerId } = {}) => {
  const trimmedEventId = String(eventId || '').trim();
  if (!trimmedEventId) throw createApiError(400, 'eventId is required');

  const normalizedRole = String(actorRole || '').trim().toUpperCase();
  if (!['MANAGER', 'ADMIN'].includes(normalizedRole)) {
    throw createApiError(403, 'Only MANAGER or ADMIN can recover promote cancellation liability');
  }

  const promote = await Promote.findOne({ eventId: trimmedEventId });
  if (!promote) throw createApiError(404, 'Promote record not found');
  if (!promote.refundRequest) {
    throw createApiError(409, 'Cancellation refund request not found for this promote event');
  }

  if (normalizedRole === 'MANAGER') {
    const normalizedActorManagerId = String(actorManagerId || '').trim();
    if (!normalizedActorManagerId) throw createApiError(403, 'Manager identity is required');
    if (String(promote.assignedManagerId || '').trim() !== normalizedActorManagerId) {
      throw createApiError(403, 'You are not assigned to this event');
    }
  }

  const promoteEventStatus = String(promote?.eventStatus || '').trim().toUpperCase();
  const promoteRefundStatus = String(promote?.refundRequest?.status || '').trim().toUpperCase();
  const cancelledStatuses = new Set(['CANCELLED', 'CANCELED', 'REFUNDED']);
  const refundRelevantStatuses = new Set([
    PROMOTE_REFUND_REQUEST_STATUSES.PENDING_REVIEW,
    PROMOTE_REFUND_REQUEST_STATUSES.APPROVED,
    PROMOTE_REFUND_REQUEST_STATUSES.REFUNDED,
  ]);

  if (!cancelledStatuses.has(promoteEventStatus) && !refundRelevantStatuses.has(promoteRefundStatus)) {
    throw createApiError(409, 'Liability recovery is available only for cancelled promote events');
  }

  const existingRecovery = promote?.refundRequest?.liabilityRecovery || null;
  const existingStatus = String(existingRecovery?.status || '').trim().toUpperCase();
  if (existingStatus === 'PAID') {
    const existing = await getPromoteByEventId(trimmedEventId);
    return {
      ...existing,
      liabilityRecoverySummary: {
        alreadyRecovered: true,
      },
    };
  }

  if (existingStatus === 'PENDING_PAYMENT' && String(existingRecovery?.paymentOrderId || '').trim()) {
    const existing = await getPromoteByEventId(trimmedEventId);
    return {
      ...existing,
      liabilityRecoverySummary: {
        alreadyPending: true,
      },
    };
  }

  const feesConfig = await promoteConfigService.getFees();
  const salesStats = await buildPromoteTicketSalesStats(promote.toJSON(), feesConfig);
  const liabilityAmountPaise = resolvePromoteLiabilityAmountPaise({
    promote,
    fallbackSalesStats: salesStats,
  });

  if (liabilityAmountPaise <= 0) {
    promote.refundRequest = promote.refundRequest || {};
    promote.refundRequest.liabilityRecovery = {
      status: 'NOT_REQUIRED',
      amountPaise: 0,
      currency: 'INR',
      ownerAuthId: String(promote?.authId || '').trim() || null,
      paymentOrderId: null,
      razorpayOrderId: null,
      transactionId: null,
      initiatedByAuthId: String(actorAuthId || '').trim() || null,
      initiatedAt: new Date(),
      paidAt: null,
      lastError: null,
    };
    await promote.save({ validateBeforeSave: false });

    const updatedNoRecovery = await getPromoteByEventId(trimmedEventId);
    return {
      ...updatedNoRecovery,
      liabilityRecoverySummary: {
        alreadyRecovered: false,
        alreadyPending: false,
        notRequired: true,
      },
    };
  }

  const reasonCode = normalizePromoteRefundReasonCode(promote?.refundRequest?.reasonCode);
  const scenarioCode = String(promote?.refundRequest?.result?.scenarioCode || '').trim() || null;
  const ownerAuthId = String(promote?.authId || '').trim();
  const initiatedByAuthId = String(actorAuthId || '').trim() || null;

  const liabilityOrder = await processPromoteLiabilityRecoveryOrder({
    eventId: trimmedEventId,
    ownerAuthId,
    amountPaise: liabilityAmountPaise,
    initiatedByAuthId,
    reasonCode,
    scenarioCode,
  });

  promote.refundRequest = promote.refundRequest || {};
  if (liabilityOrder?.error) {
    promote.refundRequest.liabilityRecovery = {
      status: 'FAILED',
      amountPaise: liabilityAmountPaise,
      currency: liabilityOrder?.currency || 'INR',
      ownerAuthId: ownerAuthId || null,
      paymentOrderId: null,
      razorpayOrderId: null,
      transactionId: null,
      initiatedByAuthId,
      initiatedAt: new Date(),
      paidAt: null,
      lastError: liabilityOrder.error,
    };
    await promote.save({ validateBeforeSave: false });
    throw createApiError(502, liabilityOrder.error);
  }

  promote.refundRequest.liabilityRecovery = {
    status: 'PENDING_PAYMENT',
    amountPaise: Math.max(0, Number(liabilityOrder?.amountPaise || liabilityAmountPaise)),
    currency: liabilityOrder?.currency || 'INR',
    ownerAuthId: ownerAuthId || null,
    paymentOrderId: liabilityOrder?.paymentOrderId || null,
    razorpayOrderId: liabilityOrder?.razorpayOrderId || null,
    transactionId: liabilityOrder?.transactionId || null,
    initiatedByAuthId,
    initiatedAt: new Date(),
    paidAt: null,
    lastError: null,
  };

  await promote.save({ validateBeforeSave: false });

  await sendPromoteOwnerLiabilityRecoveryNotification({
    promote,
    ownerAuthId,
    amountPaise: promote.refundRequest.liabilityRecovery.amountPaise,
  });

  const updated = await getPromoteByEventId(trimmedEventId);
  return {
    ...updated,
    liabilityRecoverySummary: {
      alreadyRecovered: false,
      alreadyPending: false,
      notRequired: false,
    },
  };
};

const markPromoteLiabilityRecovered = async ({
  eventId,
  paymentOrderId = null,
  razorpayOrderId = null,
  transactionId = null,
  paidAt = null,
} = {}) => {
  const trimmedEventId = String(eventId || '').trim();
  if (!trimmedEventId) throw createApiError(400, 'eventId is required');

  const promote = await Promote.findOne({ eventId: trimmedEventId });
  if (!promote) throw createApiError(404, 'Promote record not found');

  const currentRecovery = promote?.refundRequest?.liabilityRecovery || null;
  const currentStatus = String(currentRecovery?.status || '').trim().toUpperCase();
  if (currentStatus === 'PAID') return promote;

  const parsedPaidAt = paidAt ? new Date(paidAt) : new Date();
  const effectivePaidAt = Number.isNaN(parsedPaidAt.getTime()) ? new Date() : parsedPaidAt;

  const amountPaise = resolvePromoteLiabilityAmountPaise({ promote });
  promote.refundRequest = promote.refundRequest || {};
  promote.refundRequest.liabilityRecovery = {
    status: 'PAID',
    amountPaise: Math.max(0, Number(currentRecovery?.amountPaise || amountPaise || 0)),
    currency: String(currentRecovery?.currency || 'INR').trim() || 'INR',
    ownerAuthId: String(currentRecovery?.ownerAuthId || promote?.authId || '').trim() || null,
    paymentOrderId: String(paymentOrderId || currentRecovery?.paymentOrderId || '').trim() || null,
    razorpayOrderId: String(razorpayOrderId || currentRecovery?.razorpayOrderId || '').trim() || null,
    transactionId: String(transactionId || currentRecovery?.transactionId || '').trim() || null,
    initiatedByAuthId: String(currentRecovery?.initiatedByAuthId || '').trim() || null,
    initiatedAt: currentRecovery?.initiatedAt || new Date(),
    paidAt: effectivePaidAt,
    lastError: null,
  };

  await promote.save({ validateBeforeSave: false });
  return promote;
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

const updatePromoteStatus = async (
  eventId,
  eventStatus,
  assignedManagerId = null,
  { updatedByAuthId = null, updatedByRole = null, updatedByManagerId = null } = {}
) => {
  if (!eventId) throw createApiError(400, 'Event ID is required');

  const promote = await Promote.findOne({ eventId: eventId.trim() });
  if (!promote) throw createApiError(404, 'Promote record not found');

  const allowedTransitions = [PROMOTE_STATUS.LIVE, PROMOTE_STATUS.COMPLETE, PROMOTE_STATUS.CANCELLED, PROMOTE_STATUS.CLOSED];
  if (!allowedTransitions.includes(eventStatus)) {
    throw createApiError(400, `eventStatus must be one of: ${allowedTransitions.join(', ')}`);
  }

  if (eventStatus === PROMOTE_STATUS.CANCELLED) {
    return createPromoteRefundRequest({
      eventId: promote.eventId,
      authId: String(promote.authId || '').trim(),
      cancellationReason: String(updatedByRole || '').trim().toUpperCase() === 'ADMIN'
        ? 'Cancelled by platform (admin action)'
        : 'Cancelled by platform (manager action)',
      reasonCode: 'OKKAZO_FAILURE',
      initiatedByAuthId: String(updatedByAuthId || '').trim() || null,
    });
  }

  if (eventStatus === PROMOTE_STATUS.COMPLETE || eventStatus === PROMOTE_STATUS.CLOSED) {
    const normalizedRole = String(updatedByRole || '').trim().toUpperCase();

    if (normalizedRole === 'MANAGER') {
      const normalizedActorManagerId = String(updatedByManagerId || '').trim();
      if (!normalizedActorManagerId) {
        throw createApiError(403, 'Manager identity is required to close promote event');
      }
      if (String(promote.assignedManagerId || '').trim() !== normalizedActorManagerId) {
        throw createApiError(403, 'Only the assigned manager can close this promote event');
      }
    }

    const promoteEventStatus = String(promote?.eventStatus || '').trim().toUpperCase();
    const promoteRefundStatus = String(promote?.refundRequest?.status || '').trim().toUpperCase();
    const cancelledStatuses = new Set(['CANCELLED', 'CANCELED', 'REFUNDED']);
    const refundRelevantStatuses = new Set([
      PROMOTE_REFUND_REQUEST_STATUSES.PENDING_REVIEW,
      PROMOTE_REFUND_REQUEST_STATUSES.APPROVED,
      PROMOTE_REFUND_REQUEST_STATUSES.REFUNDED,
    ]);

    const cancellationFlowActive =
      cancelledStatuses.has(promoteEventStatus) || refundRelevantStatuses.has(promoteRefundStatus);

    if (cancellationFlowActive) {
      const liabilityRecoveryStatus = String(promote?.refundRequest?.liabilityRecovery?.status || '').trim().toUpperCase();
      const liabilitySettledByStatus = liabilityRecoveryStatus === 'PAID' || liabilityRecoveryStatus === 'NOT_REQUIRED';

      if (!liabilitySettledByStatus) {
        const feesConfig = await promoteConfigService.getFees();
        const salesStats = await buildPromoteTicketSalesStats(promote.toJSON(), feesConfig);
        const liabilityAmountPaise = resolvePromoteLiabilityAmountPaise({
          promote,
          fallbackSalesStats: salesStats,
        });

        if (liabilityAmountPaise > 0) {
          throw createApiError(409, 'Cannot close promote event until creator liability is paid');
        }

        promote.refundRequest = promote.refundRequest || {};
        promote.refundRequest.liabilityRecovery = {
          ...(promote.refundRequest.liabilityRecovery || {}),
          status: 'NOT_REQUIRED',
          amountPaise: 0,
          currency: 'INR',
          ownerAuthId: String(promote?.authId || '').trim() || null,
          paymentOrderId: null,
          razorpayOrderId: null,
          transactionId: null,
          initiatedByAuthId: String(updatedByAuthId || '').trim() || null,
          initiatedAt: promote?.refundRequest?.liabilityRecovery?.initiatedAt || new Date(),
          paidAt: null,
          lastError: null,
        };
      }
    }
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

  await promote.save({ validateBeforeSave: false });
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

  promote.eventStatus = PROMOTE_STATUS.CONFIRMED;

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
      eventStatus: { $nin: [PROMOTE_STATUS.COMPLETE, PROMOTE_STATUS.CANCELLED, PROMOTE_STATUS.CLOSED] },
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
        eventStatus: PROMOTE_STATUS.CONFIRMED,
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
      const promote = await Promote.findOne({ eventId: String(eventId).trim() })
        .select('platformFeePaid assignedManagerId eventStatus adminDecision.status')
        .lean();
      if (promote && ![PROMOTE_STATUS.LIVE, PROMOTE_STATUS.COMPLETE, PROMOTE_STATUS.CANCELLED, PROMOTE_STATUS.CLOSED].includes(promote.eventStatus)) {
        const computedStatus = !promote.platformFeePaid
          ? PROMOTE_STATUS.PAYMENT_REQUIRED
          : (!promote.assignedManagerId
            ? PROMOTE_STATUS.MANAGER_UNASSIGNED
            : (String(promote?.adminDecision?.status || '').trim().toUpperCase() === 'APPROVED'
              ? PROMOTE_STATUS.CONFIRMED
              : PROMOTE_STATUS.IN_REVIEW));

        if (computedStatus !== promote.eventStatus) {
          await Promote.updateOne(
            {
              eventId: String(eventId).trim(),
              eventStatus: { $nin: [PROMOTE_STATUS.LIVE, PROMOTE_STATUS.COMPLETE, PROMOTE_STATUS.CANCELLED, PROMOTE_STATUS.CLOSED] },
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
  promote.eventStatus = PROMOTE_STATUS.CONFIRMED;
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
      eventStatus: { $nin: [PROMOTE_STATUS.COMPLETE, PROMOTE_STATUS.CANCELLED, PROMOTE_STATUS.CLOSED] },
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

  const normalizedManagerId = String(managerId).trim();

  const safeLimit = Math.min(500, Math.max(1, Number(limit) || 200));

  const baseSelect =
    'eventId eventTitle eventCategory customCategory eventField eventBanner schedule ticketAvailability tickets venue createdAt authId assignedManagerId coreStaffIds adminDecision managerAssignment eventStatus platformFeePaid totalAmount serviceCharge estimatedNetRevenue ticketAnalytics';

  const promotes = await Promote.find({
    $or: [
      { assignedManagerId: normalizedManagerId },
      { coreStaffIds: normalizedManagerId },
    ],
    'adminDecision.status': { $ne: ADMIN_DECISION_STATUS.REJECTED },
  })
    .sort({ createdAt: -1 })
    .limit(safeLimit)
    .select(baseSelect)
    .lean();

  return promotes || [];
};

const createPromoteRefundRequest = async ({
  eventId,
  authId,
  cancellationReason = null,
  reasonCode = null,
  initiatedByAuthId = null,
} = {}) => {
  const normalizedEventId = String(eventId || '').trim();
  const normalizedAuthId = String(authId || '').trim();
  const normalizedInitiatorAuthId = String(initiatedByAuthId || authId || '').trim();
  const normalizedReason = String(cancellationReason || '').trim();
  const normalizedReasonCode = normalizePromoteRefundReasonCode(reasonCode);

  if (!normalizedEventId) throw createApiError(400, 'eventId is required');
  if (!normalizedAuthId) throw createApiError(401, 'Authentication required');

  const promote = await Promote.findOne({ eventId: normalizedEventId });
  if (!promote) throw createApiError(404, 'Promote not found');

  if (String(promote.authId || '').trim() !== normalizedAuthId) {
    throw createApiError(403, 'Only the event owner can request cancellation refund');
  }

  const lifecycleStatus = String(promote.eventStatus || '').trim().toUpperCase();
  if (lifecycleStatus === PROMOTE_STATUS.COMPLETE || lifecycleStatus === PROMOTE_STATUS.CLOSED || lifecycleStatus === PROMOTE_STATUS.REJECTED) {
    throw createApiError(409, 'Refund request cannot be raised for this promote status');
  }

  const existingStatus = String(promote?.refundRequest?.status || '').trim().toUpperCase();
  if (
    existingStatus === PROMOTE_REFUND_REQUEST_STATUSES.PENDING_REVIEW
    || existingStatus === PROMOTE_REFUND_REQUEST_STATUSES.APPROVED
    || existingStatus === PROMOTE_REFUND_REQUEST_STATUSES.REFUNDED
  ) {
    throw createApiError(409, 'A refund request already exists for this event');
  }

  const [feesConfig, orders] = await Promise.all([
    promoteConfigService.getFees(),
    fetchOrdersForEventFromOrderService(normalizedEventId),
  ]);
  const salesStats = await buildPromoteTicketSalesStats(promote.toJSON(), feesConfig);
  const paymentSummary = resolvePromotePaymentSummary({ orders, promote });
  const refundResult = buildPromoteRefundResult({
    promote,
    reasonCode: normalizedReasonCode,
    salesStats,
    promotionUsedAmountPaise: paymentSummary.promotionUsedAmountPaise,
    paymentPaidAt: paymentSummary.paidAt,
    now: new Date(),
  });

  const now = new Date();

  const nextRefundRequest = {
    status: PROMOTE_REFUND_REQUEST_STATUSES.REFUNDED,
    requestedByAuthId: normalizedAuthId,
    requestedAt: now,
    reasonCode: normalizedReasonCode,
    cancellationReason: normalizedReason || null,
    assignedManagerId: null,
    assignedManagerRole: null,
    managerReviewedByAuthId: normalizedInitiatorAuthId || normalizedAuthId,
    managerReviewedAt: now,
    managerNotes: normalizedReason || null,
    result: refundResult,
    refundedAt: now,
    refundTransactionRef: null,
    bulkRefundSummary: null,
  };

  const createResult = await Promote.updateOne(
    {
      _id: promote._id,
      authId: normalizedAuthId,
      $or: [
        { refundRequest: null },
        { refundRequest: { $exists: false } },
        { 'refundRequest.status': PROMOTE_REFUND_REQUEST_STATUSES.REJECTED },
      ],
    },
    {
      $set: {
        refundRequest: nextRefundRequest,
        eventStatus: PROMOTE_STATUS.CANCELLED,
      },
    }
  );

  if (Number(createResult?.modifiedCount || 0) !== 1) {
    throw createApiError(409, 'A refund request already exists for this event');
  }

  const updatedPromote = await Promote.findById(promote._id);
  if (!updatedPromote) {
    throw createApiError(404, 'Promote not found');
  }

  const recipientAuthIds = await getPromoteGuestTicketRecipients({ eventId: normalizedEventId });
  const ownerRefundAmountPaise = Math.max(0, Number(updatedPromote?.refundRequest?.result?.userRefundAmountPaise || 0));
  const scenarioCode = String(updatedPromote?.refundRequest?.result?.scenarioCode || '').trim() || null;
  const timelineLabel = String(updatedPromote?.refundRequest?.result?.timelineLabel || '').trim() || DEFAULT_REFUND_TIMELINE_LABEL;
  const cancelledAt = new Date();

  let bulkResult = {
    skipped: true,
    reason: 'No successful ticket guests found for this event',
    totalOrders: 0,
    refundedCount: 0,
    failedCount: 0,
  };

  if (recipientAuthIds.length > 0) {
    bulkResult = await processPromoteGuestTicketBulkRefund({
      eventId: normalizedEventId,
      recipientAuthIds,
      reasonCode: normalizedReasonCode,
      managerAuthId: normalizedInitiatorAuthId || normalizedAuthId,
      managerNotes: normalizedReason,
      scenarioCode,
    });

    const failedCount = Math.max(0, Number(bulkResult?.failedCount || 0));
    if (failedCount > 0) {
      throw createApiError(502, `Failed to process ${failedCount} guest ticket refunds`);
    }
  }

  let ownerRefund = {
    skipped: true,
    reason: 'Owner refund amount is zero',
    refundedAmount: 0,
    refundId: null,
    transactionId: null,
    refundedAt: null,
  };
  if (ownerRefundAmountPaise > 0) {
    ownerRefund = await processPromoteOwnerRefund({
      eventId: normalizedEventId,
      ownerAuthId: normalizedAuthId,
      refundAmountPaise: ownerRefundAmountPaise,
      reasonCode: normalizedReasonCode,
      initiatedByAuthId: normalizedInitiatorAuthId || normalizedAuthId,
      managerNotes: normalizedReason,
      scenarioCode,
    });

    if (ownerRefund?.error) {
      throw createApiError(502, ownerRefund.error);
    }
  }

  const liabilityAmountPaise = resolvePromoteLiabilityAmountPaise({
    promote: updatedPromote,
    fallbackSalesStats: salesStats,
  });

  let liabilityRecoveryState = {
    status: 'NOT_REQUIRED',
    amountPaise: 0,
    currency: 'INR',
    ownerAuthId: normalizedAuthId,
    paymentOrderId: null,
    razorpayOrderId: null,
    transactionId: null,
    initiatedByAuthId: normalizedInitiatorAuthId || normalizedAuthId || null,
    initiatedAt: new Date(),
    paidAt: null,
    lastError: null,
  };

  if (liabilityAmountPaise > 0) {
    const liabilityOrder = await processPromoteLiabilityRecoveryOrder({
      eventId: normalizedEventId,
      ownerAuthId: normalizedAuthId,
      amountPaise: liabilityAmountPaise,
      initiatedByAuthId: normalizedInitiatorAuthId || normalizedAuthId,
      reasonCode: normalizedReasonCode,
      scenarioCode,
    });

    if (liabilityOrder?.error) {
      liabilityRecoveryState = {
        status: 'FAILED',
        amountPaise: liabilityAmountPaise,
        currency: liabilityOrder?.currency || 'INR',
        ownerAuthId: normalizedAuthId,
        paymentOrderId: null,
        razorpayOrderId: null,
        transactionId: null,
        initiatedByAuthId: normalizedInitiatorAuthId || normalizedAuthId || null,
        initiatedAt: new Date(),
        paidAt: null,
        lastError: liabilityOrder.error,
      };
    } else {
      liabilityRecoveryState = {
        status: 'PENDING_PAYMENT',
        amountPaise: Math.max(0, Number(liabilityOrder?.amountPaise || liabilityAmountPaise)),
        currency: liabilityOrder?.currency || 'INR',
        ownerAuthId: normalizedAuthId,
        paymentOrderId: liabilityOrder?.paymentOrderId || null,
        razorpayOrderId: liabilityOrder?.razorpayOrderId || null,
        transactionId: liabilityOrder?.transactionId || null,
        initiatedByAuthId: normalizedInitiatorAuthId || normalizedAuthId || null,
        initiatedAt: new Date(),
        paidAt: null,
        lastError: null,
      };

      await sendPromoteOwnerLiabilityRecoveryNotification({
        promote: updatedPromote,
        ownerAuthId: normalizedAuthId,
        amountPaise: liabilityRecoveryState.amountPaise,
      });
    }
  }

  updatedPromote.refundRequest.bulkRefundSummary = bulkResult;
  updatedPromote.refundRequest.refundTransactionRef = ownerRefund?.refundId
    || ownerRefund?.transactionId
    || `PROMOTE-CANCEL-${Date.now()}`;
  updatedPromote.refundRequest.refundedAt = ownerRefund?.refundedAt
    ? new Date(ownerRefund.refundedAt)
    : cancelledAt;
  updatedPromote.refundRequest.liabilityRecovery = liabilityRecoveryState;

  const cancellationReasonText = `Event cancelled: ${String(updatedPromote?.refundRequest?.cancellationReason || 'Promoter requested cancellation').trim()}`;
  const ticketUpdateResult = await UserEventTicket.updateMany(
    {
      eventId: normalizedEventId,
      eventSource: 'promote',
      ticketStatus: USER_TICKET_STATUS.SUCCESS,
    },
    {
      $set: {
        ticketStatus: USER_TICKET_STATUS.CANCELED,
        'verification.status': USER_TICKET_VERIFICATION_STATUS.PENDING,
        'verification.verifiedAt': null,
        'verification.verifiedByAuthId': null,
        'verification.lastScannedAt': null,
        'cancellation.requestId': String(updatedPromote?.refundRequest?.requestId || '').trim() || null,
        'cancellation.cancelledAt': cancelledAt,
        'cancellation.reason': cancellationReasonText,
        'cancellation.reasonCode': normalizedReasonCode,
        'cancellation.flags.eventCancelled': true,
        'cancellation.flags.okkazoFailure': normalizedReasonCode === 'OKKAZO_FAILURE',
        'cancellation.timelineLabel': timelineLabel,
        'cancellation.refundPaymentOrderId': String(ownerRefund?.refundId || '').trim()
          || String(updatedPromote?.refundRequest?.refundTransactionRef || '').trim()
          || null,
        'cancellation.refundedAt': cancelledAt,
      },
    }
  );

  if (ticketUpdateResult && ticketUpdateResult.acknowledged === false) {
    throw createApiError(502, 'Failed to update promote guest ticket cancellation status');
  }

  try {
    await Promise.all([
      sendPromoteGuestCancellationNotifications({
        promote: updatedPromote,
        recipientAuthIds,
        refundTimelineLabel: timelineLabel,
      }),
      publishPromoteGuestCancellationEmailEvent({
        promote: updatedPromote,
        recipientAuthIds,
        refundTimelineLabel: timelineLabel,
        cancellationReason: updatedPromote?.refundRequest?.cancellationReason || null,
      }),
    ]);
  } catch (guestCommsError) {
    logger.warn('Promote guest cancellation communications failed after user cancellation request', {
      eventId: normalizedEventId,
      message: guestCommsError?.message || String(guestCommsError),
    });
  }

  await updatedPromote.save({ validateBeforeSave: false });

  return getPromoteByEventId(normalizedEventId);
};

const getPromoteRefundRequestByEventId = async ({ eventId } = {}) => {
  const normalizedEventId = String(eventId || '').trim();
  if (!normalizedEventId) throw createApiError(400, 'eventId is required');

  return getPromoteByEventId(normalizedEventId);
};

const getPromoteRefundRequestsForManager = async ({ managerId, limit = 200, statuses = [] } = {}) => {
  const safeLimit = Math.min(500, Math.max(1, Number(limit) || 200));
  const normalizedManagerId = String(managerId || '').trim();

  const normalizedStatuses = Array.isArray(statuses)
    ? statuses
      .map((status) => String(status || '').trim().toUpperCase())
      .filter(Boolean)
      .filter((status) => Object.values(PROMOTE_REFUND_REQUEST_STATUSES).includes(status))
    : [];

  const query = {
    refundRequest: { $ne: null },
  };

  if (normalizedManagerId) {
    query['refundRequest.assignedManagerId'] = normalizedManagerId;
  }

  if (normalizedStatuses.length > 0) {
    query['refundRequest.status'] = { $in: normalizedStatuses };
  }

  const rows = await Promote.find(query)
    .sort({ 'refundRequest.requestedAt': -1, createdAt: -1 })
    .limit(safeLimit)
    .select([
      'eventId',
      'eventTitle',
      'eventStatus',
      'createdAt',
      'authId',
      'assignedManagerId',
      'platformFee',
      'serviceChargePercent',
      'ticketAnalytics',
      'ticketAvailability',
      'refundRequest',
    ].join(' '))
    .lean();

  const cfg = await promoteConfigService.getFees();
  const fallbackPlatformFee = cfg.platformFee;
  const fallbackServiceChargePercent = cfg.serviceChargePercent;

  return (rows || []).map((row) => ({
    ...row,
    platformFee: (row.platformFee === undefined || row.platformFee === null) ? fallbackPlatformFee : row.platformFee,
    serviceChargePercent: (row.serviceChargePercent === undefined || row.serviceChargePercent === null)
      ? fallbackServiceChargePercent
      : row.serviceChargePercent,
  }));
};

const reviewPromoteRefundRequest = async ({
  eventId,
  managerId,
  managerAuthId,
  nextStatus,
  managerNotes = null,
  refundTransactionRef = null,
  isAdmin = false,
} = {}) => {
  const normalizedEventId = String(eventId || '').trim();
  const normalizedManagerId = String(managerId || '').trim();
  const normalizedManagerAuthId = String(managerAuthId || '').trim();
  const normalizedNextStatus = String(nextStatus || '').trim().toUpperCase();
  const normalizedNotes = String(managerNotes || '').trim();
  const normalizedRefundRef = String(refundTransactionRef || '').trim();

  if (!normalizedEventId) throw createApiError(400, 'eventId is required');
  if (!Object.values(PROMOTE_REFUND_REQUEST_STATUSES).includes(normalizedNextStatus)) {
    throw createApiError(400, 'Invalid refund request status');
  }

  const promote = await Promote.findOne({ eventId: normalizedEventId });
  if (!promote) throw createApiError(404, 'Promote not found');
  if (!promote.refundRequest) throw createApiError(404, 'Refund request not found');

  const assignedManagerId = String(promote?.refundRequest?.assignedManagerId || '').trim();
  if (!isAdmin && assignedManagerId && normalizedManagerId && assignedManagerId !== normalizedManagerId) {
    throw createApiError(403, 'Only assigned Revenue Operations Specialist can review this request');
  }

  const currentStatus = String(promote?.refundRequest?.status || '').trim().toUpperCase();
  if (currentStatus === PROMOTE_REFUND_REQUEST_STATUSES.REFUNDED) {
    throw createApiError(409, 'Refund request is already finalized');
  }

  if (
    currentStatus === PROMOTE_REFUND_REQUEST_STATUSES.PENDING_REVIEW
    && ![
      PROMOTE_REFUND_REQUEST_STATUSES.APPROVED,
      PROMOTE_REFUND_REQUEST_STATUSES.REJECTED,
      PROMOTE_REFUND_REQUEST_STATUSES.REFUNDED,
    ].includes(normalizedNextStatus)
  ) {
    throw createApiError(409, 'Pending requests can only be approved, rejected, or marked refunded');
  }

  if (
    currentStatus === PROMOTE_REFUND_REQUEST_STATUSES.APPROVED
    && ![
      PROMOTE_REFUND_REQUEST_STATUSES.REFUNDED,
      PROMOTE_REFUND_REQUEST_STATUSES.REJECTED,
    ].includes(normalizedNextStatus)
  ) {
    throw createApiError(409, 'Approved requests can only be marked refunded or rejected');
  }

  promote.refundRequest.status = normalizedNextStatus;
  promote.refundRequest.managerReviewedByAuthId = normalizedManagerAuthId || null;
  promote.refundRequest.managerReviewedAt = new Date();
  promote.refundRequest.managerNotes = normalizedNotes || null;

  if (normalizedNextStatus === PROMOTE_REFUND_REQUEST_STATUSES.REFUNDED) {
    const ownerRefundAmountPaise = Math.max(0, Number(promote?.refundRequest?.result?.userRefundAmountPaise || 0));
    const reasonCode = normalizePromoteRefundReasonCode(promote?.refundRequest?.reasonCode);
    const scenarioCode = String(promote?.refundRequest?.result?.scenarioCode || '').trim() || null;
    const recipientAuthIds = await getPromoteGuestTicketRecipients({ eventId: normalizedEventId });

    if (!normalizedRefundRef && recipientAuthIds.length > 0) {
      const bulkResult = await processPromoteGuestTicketBulkRefund({
        eventId: normalizedEventId,
        recipientAuthIds,
        reasonCode,
        managerAuthId: normalizedManagerAuthId,
        managerNotes: normalizedNotes,
        scenarioCode,
      });

      const failedCount = Math.max(0, Number(bulkResult?.failedCount || 0));
      if (failedCount > 0) {
        throw createApiError(502, `Failed to process ${failedCount} guest ticket refunds`);
      }

      promote.refundRequest.bulkRefundSummary = bulkResult;
    } else if (!normalizedRefundRef) {
      promote.refundRequest.bulkRefundSummary = {
        skipped: true,
        reason: 'No successful ticket guests found for this event',
        totalOrders: 0,
        refundedCount: 0,
        failedCount: 0,
      };
    } else {
      promote.refundRequest.bulkRefundSummary = null;
    }

    let ownerRefundRef = normalizedRefundRef || null;
    if (!ownerRefundRef && ownerRefundAmountPaise > 0) {
      const ownerRefund = await processPromoteOwnerRefund({
        eventId: normalizedEventId,
        ownerAuthId: String(promote?.authId || '').trim(),
        refundAmountPaise: ownerRefundAmountPaise,
        reasonCode,
        initiatedByAuthId: normalizedManagerAuthId,
        managerNotes: normalizedNotes,
        scenarioCode,
      });

      if (ownerRefund?.error) {
        throw createApiError(502, ownerRefund.error);
      }

      ownerRefundRef = ownerRefund?.refundId || ownerRefund?.transactionId || null;
      promote.refundRequest.refundedAt = ownerRefund?.refundedAt ? new Date(ownerRefund.refundedAt) : new Date();
    } else {
      promote.refundRequest.refundedAt = new Date();
    }

    let liabilitySalesStats = null;
    try {
      const feesConfig = await promoteConfigService.getFees();
      liabilitySalesStats = await buildPromoteTicketSalesStats(promote.toJSON(), feesConfig);
    } catch (liabilityStatsError) {
      logger.warn('Failed to compute promote liability ticket sales stats during refund finalization', {
        eventId: normalizedEventId,
        message: liabilityStatsError?.message || String(liabilityStatsError),
      });
    }

    const liabilityAmountPaise = resolvePromoteLiabilityAmountPaise({
      promote,
      fallbackSalesStats: liabilitySalesStats,
    });

    let liabilityRecoveryState = {
      status: 'NOT_REQUIRED',
      amountPaise: 0,
      currency: 'INR',
      ownerAuthId: String(promote?.authId || '').trim() || null,
      paymentOrderId: null,
      razorpayOrderId: null,
      transactionId: null,
      initiatedByAuthId: normalizedManagerAuthId || null,
      initiatedAt: new Date(),
      paidAt: null,
      lastError: null,
    };

    if (liabilityAmountPaise > 0) {
      const liabilityOrder = await processPromoteLiabilityRecoveryOrder({
        eventId: normalizedEventId,
        ownerAuthId: String(promote?.authId || '').trim(),
        amountPaise: liabilityAmountPaise,
        initiatedByAuthId: normalizedManagerAuthId,
        reasonCode,
        scenarioCode,
      });

      if (liabilityOrder?.error) {
        liabilityRecoveryState = {
          status: 'FAILED',
          amountPaise: liabilityAmountPaise,
          currency: liabilityOrder?.currency || 'INR',
          ownerAuthId: String(promote?.authId || '').trim() || null,
          paymentOrderId: null,
          razorpayOrderId: null,
          transactionId: null,
          initiatedByAuthId: normalizedManagerAuthId || null,
          initiatedAt: new Date(),
          paidAt: null,
          lastError: liabilityOrder.error,
        };
      } else {
        liabilityRecoveryState = {
          status: 'PENDING_PAYMENT',
          amountPaise: Math.max(0, Number(liabilityOrder?.amountPaise || liabilityAmountPaise)),
          currency: liabilityOrder?.currency || 'INR',
          ownerAuthId: String(promote?.authId || '').trim() || null,
          paymentOrderId: liabilityOrder?.paymentOrderId || null,
          razorpayOrderId: liabilityOrder?.razorpayOrderId || null,
          transactionId: liabilityOrder?.transactionId || null,
          initiatedByAuthId: normalizedManagerAuthId || null,
          initiatedAt: new Date(),
          paidAt: null,
          lastError: null,
        };

        await sendPromoteOwnerLiabilityRecoveryNotification({
          promote,
          ownerAuthId: String(promote?.authId || '').trim(),
          amountPaise: liabilityRecoveryState.amountPaise,
        });
      }
    }

    promote.refundRequest.refundTransactionRef = ownerRefundRef || `PROMOTE-CANCEL-${Date.now()}`;
    promote.refundRequest.liabilityRecovery = liabilityRecoveryState;

    const cancelledAt = new Date();
    const cancellationReasonText = `Event cancelled: ${String(promote?.refundRequest?.cancellationReason || 'Promoter requested cancellation').trim()}`;
    const timelineLabel = String(promote?.refundRequest?.result?.timelineLabel || '').trim() || DEFAULT_REFUND_TIMELINE_LABEL;

    const ticketUpdateResult = await UserEventTicket.updateMany(
      {
        eventId: normalizedEventId,
        eventSource: 'promote',
        ticketStatus: USER_TICKET_STATUS.SUCCESS,
      },
      {
        $set: {
          ticketStatus: USER_TICKET_STATUS.CANCELED,
          'verification.status': USER_TICKET_VERIFICATION_STATUS.PENDING,
          'verification.verifiedAt': null,
          'verification.verifiedByAuthId': null,
          'verification.lastScannedAt': null,
          'cancellation.requestId': String(promote?.refundRequest?.requestId || '').trim() || null,
          'cancellation.cancelledAt': cancelledAt,
          'cancellation.reason': cancellationReasonText,
          'cancellation.reasonCode': reasonCode,
          'cancellation.flags.eventCancelled': true,
          'cancellation.flags.okkazoFailure': reasonCode === 'OKKAZO_FAILURE',
          'cancellation.timelineLabel': timelineLabel,
          'cancellation.refundPaymentOrderId': String(promote?.refundRequest?.refundTransactionRef || '').trim() || null,
          'cancellation.refundedAt': cancelledAt,
        },
      }
    );

    if (ticketUpdateResult && ticketUpdateResult.acknowledged === false) {
      throw createApiError(502, 'Failed to update promote guest ticket cancellation status');
    }

    try {
      await Promise.all([
        sendPromoteGuestCancellationNotifications({
          promote,
          recipientAuthIds,
          refundTimelineLabel: timelineLabel,
        }),
        publishPromoteGuestCancellationEmailEvent({
          promote,
          recipientAuthIds,
          refundTimelineLabel: timelineLabel,
          cancellationReason: promote?.refundRequest?.cancellationReason || null,
        }),
      ]);
    } catch (guestCommsError) {
      logger.warn('Promote guest cancellation communications failed after refund finalization', {
        eventId: normalizedEventId,
        message: guestCommsError?.message || String(guestCommsError),
      });
    }
  }

  await promote.save({ validateBeforeSave: false });

  return getPromoteByEventId(normalizedEventId);
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
  createPromoteRefundRequest,
  getPromoteRefundRequestByEventId,
  getPromoteRefundRequestsForManager,
  reviewPromoteRefundRequest,
  resolveRevenueOpsManagerContext,
  releasePromoteGeneratedRevenuePayout,
  recoverPromoteCancellationLiability,
  triggerPromoteEmailBlastPromotionAction,
  updatePromoteDetails,
  addPromoteCoreStaff,
  removePromoteCoreStaff,
  getAllPromotes,
  getPromotesForManager,
  markPromotePaid,
  markPromoteLiabilityRecovered,
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
