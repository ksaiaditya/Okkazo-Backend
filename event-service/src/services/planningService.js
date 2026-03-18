const Planning = require("../models/Planning");
const VendorSelection = require('../models/VendorSelection');
const logger = require("../utils/logger");
const createApiError = require("../utils/ApiError");
const { STATUS } = require('../utils/planningConstants');
const vendorSelectionService = require('./vendorSelectionService');
const promoteConfigService = require('./promoteConfigService');

const hydratePlanningFees = (planning, platformFeeFallback) => {
  if (!planning) return planning;
  if (planning.platformFee === undefined || planning.platformFee === null) {
    return { ...planning, platformFee: platformFeeFallback };
  }
  return planning;
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
    status: saved.selectedServices,
  };
};

/**
 * Get all plannings for a specific user
 */
const getPlanningsByAuthId = async (authId, page = 1, limit = 10) => {
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

  const hydrated = (plannings || []).map((p) => hydratePlanningFees(p, cfg.platformFee));

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
const getPlanningByEventId = async (eventId) => {
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

  return hydratePlanningFees(planning, cfg.platformFee);
};

/**
 * Get all plannings with pagination and filters (Admin/Manager)
 */
const getAllPlannings = async (filters = {}, page = 1, limit = 10) => {
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

  const hydrated = (plannings || []).map((p) => hydratePlanningFees(p, cfg.platformFee));

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

  const planning = await Planning.findOne({ eventId: eventId.trim() });
  if (!planning) {
    throw createApiError(404, "Planning not found");
  }

  planning.status = status;
  if (assignedManagerId) {
    planning.assignedManagerId = assignedManagerId;
  }

  await planning.save();
  logger.info(`Planning status updated: ${eventId} -> ${status}`);

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

  if (!planning.isPaid) {
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

module.exports = {
  createPlanning,
  getPlanningsByAuthId,
  getPlanningByEventId,
  getAllPlannings,
  updatePlanningStatus,
  deletePlanning,
  getPlanningStats,
  markPlanningPaid,
  confirmPlanning,
};
