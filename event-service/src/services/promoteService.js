const Promote = require('../models/Promote');
const createApiError = require('../utils/ApiError');
const logger = require('../utils/logger');
const { PROMOTE_STATUS } = require('../utils/promoteConstants');
const promoteConfigService = require('./promoteConfigService');

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

  return {
    ...promote,
    platformFee: (promote.platformFee === undefined || promote.platformFee === null) ? cfg.platformFee : promote.platformFee,
    serviceChargePercent: (promote.serviceChargePercent === undefined || promote.serviceChargePercent === null)
      ? cfg.serviceChargePercent
      : promote.serviceChargePercent,
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
  if (assignedManagerId) promote.assignedManagerId = assignedManagerId;

  await promote.save();
  logger.info(`Promote status updated: ${eventId} → ${eventStatus}`);
  return promote;
};

// ─── Assign manager (admin only) ─────────────────────────────────────────────

const assignManager = async (eventId, managerId) => {
  if (!eventId) throw createApiError(400, 'Event ID is required');
  if (!managerId) throw createApiError(400, 'Manager ID is required');

  const promote = await Promote.findOne({ eventId: eventId.trim() });
  if (!promote) throw createApiError(404, 'Promote record not found');

  promote.assignedManagerId = managerId;
  await promote.save(); // status recalculated in pre-validate
  logger.info(`Manager ${managerId} assigned to promote ${eventId}`);
  return promote;
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
  getAllPromotes,
  markPromotePaid,
  updatePromoteStatus,
  assignManager,
  deletePromote,
};
