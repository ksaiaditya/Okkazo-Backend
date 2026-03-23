const Planning = require('../models/Planning');
const Promote = require('../models/Promote');
const VendorSelection = require('../models/VendorSelection');
const logger = require('../utils/logger');
const createApiError = require('../utils/ApiError');
const { STATUS, STATUS_VALUES, CATEGORY } = require('../utils/planningConstants');
const { PROMOTE_STATUS, ADMIN_DECISION_STATUS } = require('../utils/promoteConstants');
const vendorSelectionService = require('./vendorSelectionService');
const promoteConfigService = require('./promoteConfigService');
const planningQuoteService = require('./planningQuoteService');
const mongoose = require('mongoose');
const { fetchUserById } = require('./userServiceClient');
const { ensureEventChatSeeded } = require('./chatSeedService');

const REQUIRED_DEPARTMENT_BY_PLANNING_CATEGORY = {
  [CATEGORY.PUBLIC]: 'Public Event',
  [CATEGORY.PRIVATE]: 'Private Event',
};

const normalizeLoose = (value) => String(value || '').trim().toLowerCase();

const isAssignedRoleEligible = (assignedRole) => {
  if (!assignedRole) return false;
  const role = normalizeLoose(assignedRole);
  return role.includes('junior') || role.includes('senior');
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

  const existingPromote = await Promote.findOne({
    assignedManagerId: String(managerId).trim(),
    eventStatus: { $ne: PROMOTE_STATUS.COMPLETE },
    'adminDecision.status': { $ne: ADMIN_DECISION_STATUS.REJECTED },
  })
    .select('eventId')
    .lean();
  if (existingPromote) throw createApiError(409, 'Manager is already assigned to another event');

  const planningQuery = {
    assignedManagerId: String(managerId).trim(),
    status: { $nin: [STATUS.COMPLETED, STATUS.REJECTED] },
  };
  if (planningEventIdToExclude) {
    planningQuery.eventId = { $ne: String(planningEventIdToExclude).trim() };
  }

  const existingPlanning = await Planning.findOne(planningQuery).select('eventId').lean();
  if (existingPlanning) throw createApiError(409, 'Manager is already assigned to another event');
};

const normalizePlanningForApi = (planning, platformFeeFallback) => {
  if (!planning) return planning;

  const normalized = {
    ...planning,
    platformFeePaid: Boolean(planning.platformFeePaid) || Boolean(planning.isPaid),
    depositPaid: Boolean(planning.depositPaid),
    vendorConfirmationPaid: Boolean(planning.vendorConfirmationPaid),
    fullPaymentPaid: Boolean(planning.fullPaymentPaid),
  };

  // Ensure platformFee is always populated for UI/clients.
  if (normalized.platformFee === undefined || normalized.platformFee === null) {
    normalized.platformFee = platformFeeFallback;
  }

  // Hide legacy name.
  delete normalized.isPaid;

  return normalized;
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

  const hydrated = (plannings || []).map((p) => normalizePlanningForApi(p, cfg.platformFee));

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

  return normalizePlanningForApi(planning, cfg.platformFee);
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

  const hydrated = (plannings || []).map((p) => normalizePlanningForApi(p, cfg.platformFee));

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
      status: { $nin: [STATUS.COMPLETED, STATUS.REJECTED] },
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
    'eventId eventTitle category eventType customEventType eventField eventBanner schedule eventDate createdAt authId assignedManagerId status isUrgent platformFeePaid isPaid depositPaid fullPaymentPaid vendorSelectionId selectedServices selectedVendors tickets platformFee';

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
    'eventId eventTitle category eventType customEventType eventField eventBanner schedule eventDate createdAt authId assignedManagerId status isUrgent platformFeePaid isPaid depositPaid fullPaymentPaid vendorSelectionId selectedServices selectedVendors tickets platformFee';

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

  if (typeof nextTitle === 'string' && nextTitle.trim()) {
    planning.eventTitle = nextTitle.trim();
  }
  if (typeof nextDescription === 'string' && nextDescription.trim()) {
    planning.eventDescription = nextDescription.trim();
  }
  if (typeof nextLocationName === 'string' && nextLocationName.trim()) {
    planning.location = planning.location || {};
    planning.location.name = nextLocationName.trim();
  }

  await planning.save();

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

  if (String(planning.status || '').trim() !== STATUS.CONFIRMED) {
    throw createApiError(409, 'Staff can only be assigned when planning status is CONFIRMED');
  }

  const normalizedActorId = String(actorManagerId || '').trim();
  if (!normalizedActorId) throw createApiError(403, 'Manager identity is required');
  if (String(planning.assignedManagerId || '').trim() !== normalizedActorId) {
    throw createApiError(403, 'You are not assigned to this planning');
  }

  // Enforce availability: staff cannot be assigned to other active events.
  const [conflictPlanning, conflictPromote] = await Promise.all([
    Planning.findOne({
      eventId: { $ne: trimmedEventId },
      status: { $nin: [STATUS.COMPLETED, STATUS.REJECTED] },
      $or: [
        { assignedManagerId: trimmedStaffId },
        { coreStaffIds: trimmedStaffId },
      ],
    })
      .select('eventId')
      .lean(),
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
  ]);

  if (conflictPlanning || conflictPromote) {
    throw createApiError(409, 'Staff is already assigned to another active event');
  }

  const existing = Array.isArray(planning.coreStaffIds) ? planning.coreStaffIds.map(String) : [];
  if (!existing.includes(trimmedStaffId)) {
    planning.coreStaffIds = [...existing, trimmedStaffId];
    await planning.save({ validateBeforeSave: false });
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

  if (String(planning.status || '').trim() !== STATUS.CONFIRMED) {
    throw createApiError(409, 'Staff can only be removed when planning status is CONFIRMED');
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
      status: { $nin: [STATUS.COMPLETED, STATUS.REJECTED] },
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
  assignPlanningManager,
  tryAutoAssignPlanningManager,
  unassignPlanningManager,
  deletePlanning,
  getPlanningStats,
  markPlanningPaid,
  markPlanningDepositPaid,
  markPlanningVendorConfirmationPaid,
  confirmPlanning,
  getAdminDashboard,
  getPlanningsForManager,
  getPlanningApplicationsForManager,
  updatePlanningDetails,
  addPlanningCoreStaff,
  removePlanningCoreStaff,
};
