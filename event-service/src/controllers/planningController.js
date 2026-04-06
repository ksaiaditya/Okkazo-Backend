const planningService = require('../services/planningService');
const planningQuoteService = require('../services/planningQuoteService');
const promoteConfigService = require('../services/promoteConfigService');
const VendorSelection = require('../models/VendorSelection');
const { resolveUserServiceIdFromAuthId, fetchUserById } = require('../services/userServiceClient');
const bannerUploadService = require('../services/bannerUploadService');
const { publishEvent } = require('../kafka/eventProducer');
const logger = require('../utils/logger');
const axios = require('axios');
const vendorReservationService = require('../services/vendorReservationService');
const {
  toIstDayString,
  normalizeIstDayInput,
  parseIstDayStart,
  startOfIstDay,
} = require('../utils/istDateTime');

const defaultVendorServiceUrl = process.env.SERVICE_HOST
  ? 'http://vendor-service:8084' // docker-compose service name
  : 'http://localhost:8084';
const vendorServiceUrl = process.env.VENDOR_SERVICE_URL || defaultVendorServiceUrl;
const upstreamTimeoutMs = parseInt(process.env.UPSTREAM_HTTP_TIMEOUT_MS || '10000', 10);
const HIGH_DEMAND_START_DAYS = 6;
const HIGH_DEMAND_END_DAYS = 20;
const PLANNING_LIFECYCLE_STATUSES = new Set(['CONFIRMED', 'LIVE', 'COMPLETED', 'COMPLETE', 'CLOSED']);

const listAcceptedVendorAuthIdsForEvent = async (eventId) => {
  const normalizedEventId = String(eventId || '').trim();
  if (!normalizedEventId) return [];

  const selection = await VendorSelection.findOne({ eventId: normalizedEventId })
    .select('vendors.vendorAuthId vendors.status')
    .lean();

  const vendors = Array.isArray(selection?.vendors) ? selection.vendors : [];
  return Array.from(
    new Set(
      vendors
        .filter((row) => String(row?.status || '').trim().toUpperCase() === 'ACCEPTED')
        .map((row) => String(row?.vendorAuthId || '').trim())
        .filter(Boolean)
    )
  );
};

const publishPlanningLifecycleEventIfNeeded = async ({ planning, updatedBy = null }) => {
  const status = String(planning?.status || '').trim().toUpperCase();
  if (!PLANNING_LIFECYCLE_STATUSES.has(status)) return;

  let vendorAuthIds = [];
  if (status === 'CONFIRMED' || status === 'CLOSED') {
    vendorAuthIds = await listAcceptedVendorAuthIdsForEvent(planning?.eventId);
  }

  await publishEvent('EVENT_LIFECYCLE_STATUS_UPDATED', {
    eventId: String(planning?.eventId || '').trim() || null,
    authId: String(planning?.authId || '').trim() || null,
    status,
    eventType: 'planning',
    assignedManagerId: String(planning?.assignedManagerId || '').trim() || null,
    vendorAuthIds,
    eventTitle: String(planning?.eventTitle || '').trim() || null,
    updatedBy: updatedBy ? String(updatedBy).trim() : null,
    occurredAt: new Date().toISOString(),
  });
};

const toNumber = (value, fallback = null) => {
  if (value == null) return fallback;
  if (typeof value === 'string' && value.trim() === '') return fallback;
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
};

const haversineKm = ({ lat1, lon1, lat2, lon2 }) => {
  const R = 6371;
  const toRad = (deg) => (deg * Math.PI) / 180;

  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);

  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) * Math.sin(dLon / 2);

  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
};

const normalizeSortKey = (value) => {
  const v = String(value || '').trim().toLowerCase();
  if (!v) return 'recommended';
  if (v === 'nearest' || v.includes('nearest')) return 'nearest';
  if (v === 'toprated' || v.includes('top')) return 'topRated';
  if (v === 'trending' || v.includes('trend')) return 'trending';
  if (v.includes('price') && v.includes('low')) return 'priceLow';
  if (v.includes('price') && v.includes('high')) return 'priceHigh';
  return v;
};

const extractRating = (service) => {
  const direct = toNumber(service?.rating, null);
  if (direct != null) return direct;

  const fromDetails = toNumber(service?.details?.rating, null);
  if (fromDetails != null) return fromDetails;

  const fromAvg = toNumber(service?.details?.avgRating, null);
  if (fromAvg != null) return fromAvg;

  return 0;
};

const buildMapsUrl = ({ latitude, longitude }) => {
  if (latitude == null || longitude == null) return null;
  return `https://www.google.com/maps?q=${encodeURIComponent(String(latitude))},${encodeURIComponent(String(longitude))}`;
};

const toValidMultiplier = (value, fallback) => {
  const n = toNumber(value, fallback);
  return Number.isFinite(n) && n > 0 ? n : fallback;
};

const parseDemandMultipliers = (feesConfig) => {
  const normalMin = toValidMultiplier(feesConfig?.demandPricingMultipliers?.normal?.min, 1);
  const normalMax = toValidMultiplier(feesConfig?.demandPricingMultipliers?.normal?.max, 1);
  const highDemandMin = toValidMultiplier(feesConfig?.demandPricingMultipliers?.highDemand?.min, 1.5);
  const highDemandMax = toValidMultiplier(feesConfig?.demandPricingMultipliers?.highDemand?.max, 2.25);

  return {
    normal: {
      min: normalMin,
      max: normalMax >= normalMin ? normalMax : normalMin,
    },
    highDemand: {
      min: highDemandMin,
      max: highDemandMax >= highDemandMin ? highDemandMax : highDemandMin,
    },
  };
};

const getDemandTierForDay = (day) => {
  const d = normalizeIstDayInput(day);
  if (!d) return 'NORMAL';

  const selectedDate = parseIstDayStart(d);
  if (!selectedDate || Number.isNaN(selectedDate.getTime())) return 'NORMAL';

  const today = startOfIstDay(new Date());
  if (!today) return 'NORMAL';

  const diffTime = selectedDate.getTime() - today.getTime();
  const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));

  return diffDays >= HIGH_DEMAND_START_DAYS && diffDays <= HIGH_DEMAND_END_DAYS
    ? 'HIGH_DEMAND'
    : 'NORMAL';
};

const ensureAccessToPlanning = async ({ eventId, user }) => {
  const planning = await planningService.getPlanningByEventId(eventId, user?.role || null);

  if (
    user?.role !== 'ADMIN' &&
    user?.role !== 'MANAGER' &&
    planning.authId !== user?.authId
  ) {
    const err = new Error('Access denied. You can only view your own plannings.');
    err.statusCode = 403;
    throw err;
  }

  return planning;
};

const buildManagerProfile = (user) => {
  if (!user || typeof user !== 'object') return null;

  return {
    id: user._id || user.id || null,
    authId: user.authId || null,
    name: user.name || user.fullName || user.username || 'Event Manager',
    assignedRole: user.assignedRole || user.role || user.department || 'Manager',
    department: user.department || null,
  };
};

const enrichPlanningWithManagerProfile = async (planning) => {
  if (!planning || typeof planning !== 'object') return planning;

  const assignedManagerId = String(planning.assignedManagerId || '').trim();
  if (!assignedManagerId) {
    return {
      ...planning,
      managerProfile: null,
    };
  }

  try {
    const manager = await fetchUserById(assignedManagerId);
    return {
      ...planning,
      managerProfile: buildManagerProfile(manager),
    };
  } catch (error) {
    logger.warn('Failed to enrich planning manager profile', {
      eventId: planning.eventId,
      assignedManagerId,
      message: error?.message,
    });

    return {
      ...planning,
      managerProfile: null,
    };
  }
};

/**
 * Unassign planning manager (Admin only)
 * PATCH /planning/:eventId/unassign-manager
 */
const unassignPlanningManager = async (req, res) => {
  try {
    const { eventId } = req.params;

    if (!eventId || eventId.trim() === '') {
      return res.status(400).json({
        success: false,
        message: 'Event ID is required',
      });
    }

    const planning = await planningService.unassignPlanningManager(eventId);

    try {
      await publishEvent('PLANNING_STATUS_UPDATED', {
        eventId: planning.eventId,
        authId: planning.authId,
        status: planning.status,
        assignedManagerId: planning.assignedManagerId,
        updatedBy: req.user?.authId || null,
      });

      await publishPlanningLifecycleEventIfNeeded({
        planning,
        updatedBy: req.user?.authId || null,
      });
    } catch (kafkaError) {
      logger.error('Failed to publish PLANNING_STATUS_UPDATED event:', kafkaError);
    }

    return res.status(200).json({
      success: true,
      message: 'Manager unassigned successfully',
      data: planning,
    });
  } catch (error) {
    logger.error('Error in unassignPlanningManager:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

const fetchAllVendorsBasedOnService = async ({
  serviceCategory,
  latitude,
  longitude,
  radiusKm,
  limit,
  skip,
  businessName,
  enableGeo,
  day,
  from,
  to,
  preferAvailabilityEndpoint = true,
}) => {
  const params = {
    serviceCategory,
    limit,
    skip,
    ...(businessName ? { businessName } : {}),
  };

  const hasAvailabilityRange =
    (day != null && String(day).trim()) ||
    ((from != null && String(from).trim()) && (to != null && String(to).trim()));

  // Only include geo params when explicitly enabled AND coordinates are valid.
  // This avoids unintentionally filtering out all vendors for distant events.
  if (enableGeo && latitude != null && longitude != null) {
    params.latitude = latitude;
    params.longitude = longitude;
    if (radiusKm != null) params.radiusKm = radiusKm;
  }

  if (day != null && String(day).trim()) params.day = String(day).trim();
  if (from != null && String(from).trim()) params.from = String(from).trim();
  if (to != null && String(to).trim()) params.to = String(to).trim();

  const upstreamPath = (preferAvailabilityEndpoint && hasAvailabilityRange)
    ? '/api/vendor/services/available'
    : '/api/vendor/services/search';

  const response = await axios.get(`${vendorServiceUrl}${upstreamPath}`, {
    timeout: upstreamTimeoutMs,
    params,
  });

  const services = response.data?.data?.services;
  return Array.isArray(services) ? services : [];
};

const fetchPublicVendorsByAuthIds = async (authIds) => {
  if (!Array.isArray(authIds) || authIds.length === 0) return [];

  const response = await axios.get(`${vendorServiceUrl}/api/vendor/public/vendors`, {
    timeout: upstreamTimeoutMs,
    params: {
      authIds: authIds.join(','),
    },
  });

  const vendors = response.data?.data?.vendors;
  return Array.isArray(vendors) ? vendors : [];
};

/**
 * Create a new planning event
 * POST /planning
 *
 * For public events, the request is multipart/form-data so that
 * the eventBanner image file can be uploaded alongside JSON fields.
 * For private events, plain JSON works fine (no banner).
 */
const createPlanning = async (req, res) => {
  try {
    if (!req.user || !req.user.authId) {
      return res.status(401).json({
        success: false,
        message: 'User authentication information missing',
      });
    }

    const payload = {
      ...req.body,
      authId: req.user.authId,
    };

    // Handle event banner upload (public events - file comes via multer)
    if (req.file) {
      const uploadResult = await bannerUploadService.uploadBanner(
        req.file,
        `event-banners`
      );

      payload.eventBanner = {
        url: uploadResult.url,
        publicId: uploadResult.publicId,
        mimeType: uploadResult.mimeType,
        sizeBytes: uploadResult.sizeBytes,
      };
    }

    const result = await planningService.createPlanning(payload);

    // Publish Kafka event
    try {
      await publishEvent('PLANNING_CREATED', {
        eventId: result.eventId,
        authId: req.user.authId,
        title: result.title,
        category: req.body.category,
        eventScheduleDate: result.eventScheduleDate,
        selectedServices: result.selectedServices,
      });
    } catch (kafkaError) {
      logger.error('Failed to publish PLANNING_CREATED event:', kafkaError);
      // Don't fail the request if Kafka publish fails
    }

    res.status(201).json({
      success: true,
      message: 'Planning event created successfully',
      data: result,
    });
  } catch (error) {
    logger.error('Error in createPlanning:', error);
    res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

/**
 * Get current user's plannings
 * GET /planning/me
 */
const getMyPlannings = async (req, res) => {
  try {
    if (!req.user || !req.user.authId) {
      return res.status(401).json({
        success: false,
        message: 'User authentication information missing',
      });
    }

    let { page = 1, limit = 10 } = req.query;
    page = parseInt(page, 10);
    limit = parseInt(limit, 10);

    if (isNaN(page) || page < 1) page = 1;
    if (isNaN(limit) || limit < 1) limit = 10;
    if (limit > 100) limit = 100;

    const result = await planningService.getPlanningsByAuthId(req.user.authId, page, limit, req.user?.role || null);

    res.status(200).json({
      success: true,
      data: result.plannings,
      pagination: result.pagination,
    });
  } catch (error) {
    logger.error('Error in getMyPlannings:', error);
    res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

/**
 * Get a single planning by eventId
 * GET /planning/:eventId
 */
const getPlanningByEventId = async (req, res) => {
  try {
    const { eventId } = req.params;

    if (!eventId || eventId.trim() === '') {
      return res.status(400).json({
        success: false,
        message: 'Event ID is required',
      });
    }

    const planning = await planningService.getPlanningByEventId(eventId, req.user?.role || null);

    // Regular users can only access their own plannings
    if (
      req.user.role !== 'ADMIN' &&
      req.user.role !== 'MANAGER' &&
      planning.authId !== req.user.authId
    ) {
      return res.status(403).json({
        success: false,
        message: 'Access denied. You can only view your own plannings.',
      });
    }

    const enrichedPlanning = await enrichPlanningWithManagerProfile(planning);

    res.status(200).json({
      success: true,
      data: enrichedPlanning,
    });
  } catch (error) {
    logger.error('Error in getPlanningByEventId:', error);
    res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

/**
 * Get the latest locked quote snapshot for a planning.
 * GET /planning/:eventId/quote/latest
 */
const getPlanningQuoteLatest = async (req, res) => {
  try {
    const { eventId } = req.params;

    if (!eventId || eventId.trim() === '') {
      return res.status(400).json({
        success: false,
        message: 'Event ID is required',
      });
    }

    const planning = await planningService.getPlanningByEventId(eventId, req.user?.role || null);
    if (
      req.user.role !== 'ADMIN' &&
      req.user.role !== 'MANAGER' &&
      planning.authId !== req.user.authId
    ) {
      return res.status(403).json({
        success: false,
        message: 'Access denied. You can only view your own plannings.',
      });
    }

    const quote = await planningQuoteService.getLatestSnapshotForEvent({ eventId });
    return res.status(200).json({
      success: true,
      data: quote,
    });
  } catch (error) {
    logger.error('Error in getPlanningQuoteLatest:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

/**
 * Manually send the latest locked quotation email for a planning event.
 * POST /planning/:eventId/quote/send-email
 */
const sendPlanningQuoteEmail = async (req, res) => {
  try {
    const { eventId } = req.params;

    if (!eventId || eventId.trim() === '') {
      return res.status(400).json({
        success: false,
        message: 'Event ID is required',
      });
    }

    const planning = await planningService.getPlanningByEventId(eventId, req.user?.role || null);
    const ownerAuthId = String(planning?.authId || '').trim();

    if (!ownerAuthId) {
      return res.status(409).json({
        success: false,
        message: 'Planning owner not found',
      });
    }

    const latestQuote = await planningQuoteService.getLatestSnapshotForEvent({ eventId });
    const quoteVersion = Number(latestQuote?.version || 0);

    if (!Number.isFinite(quoteVersion) || quoteVersion <= 0) {
      return res.status(409).json({
        success: false,
        message: 'Latest quote snapshot is not available',
      });
    }

    await publishEvent(
      'PLANNING_QUOTE_LOCKED',
      {
        eventId: String(eventId).trim(),
        authId: ownerAuthId,
        version: quoteVersion,
        forceSend: true,
        emailAudience: 'USER_ONLY',
        manualTriggeredBy: req.user?.authId || null,
      },
      `${String(eventId).trim()}:${quoteVersion}:manual:${Date.now()}`
    );

    return res.status(200).json({
      success: true,
      message: 'Quotation email queued successfully',
      data: {
        eventId: String(eventId).trim(),
        version: quoteVersion,
      },
    });
  } catch (error) {
    logger.error('Error in sendPlanningQuoteEmail:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message || 'Failed to send quotation email',
    });
  }
};

/**
 * Get all plannings with pagination and filters (Admin/Manager)
 * GET /planning
 */
const getAllPlannings = async (req, res) => {
  try {
    let { page = 1, limit = 10, category, status, isUrgent, search } = req.query;

    page = parseInt(page, 10);
    limit = parseInt(limit, 10);

    if (isNaN(page) || page < 1) page = 1;
    if (isNaN(limit) || limit < 1) limit = 10;
    if (limit > 100) limit = 100;

    const filters = {};
    if (category) filters.category = category;
    if (status) filters.status = status;
    if (isUrgent !== undefined) filters.isUrgent = isUrgent;
    if (search && search.trim() !== '') filters.search = search.trim();

    const result = await planningService.getAllPlannings(filters, page, limit, req.user?.role || null);

    res.status(200).json({
      success: true,
      data: result.plannings,
      pagination: result.pagination,
    });
  } catch (error) {
    logger.error('Error in getAllPlannings:', error);
    res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

/**
 * Update planning status (Admin/Manager)
 * PATCH /planning/:eventId/status
 */
const updatePlanningStatus = async (req, res) => {
  try {
    const { eventId } = req.params;
    const { status, assignedManagerId } = req.body;

    if (!eventId || eventId.trim() === '') {
      return res.status(400).json({
        success: false,
        message: 'Event ID is required',
      });
    }

    if (!status && !assignedManagerId) {
      return res.status(400).json({
        success: false,
        message: 'Either status or assignedManagerId is required',
      });
    }

    const planning = status
      ? await planningService.updatePlanningStatus(eventId, status, assignedManagerId)
      : await planningService.assignPlanningManager(eventId, assignedManagerId);

    // Publish Kafka event
    try {
      await publishEvent('PLANNING_STATUS_UPDATED', {
        eventId: planning.eventId,
        authId: planning.authId,
        status: planning.status,
        assignedManagerId: planning.assignedManagerId,
        updatedBy: req.user.authId,
      });

      await publishPlanningLifecycleEventIfNeeded({
        planning,
        updatedBy: req.user?.authId || null,
      });
    } catch (kafkaError) {
      logger.error('Failed to publish PLANNING_STATUS_UPDATED event:', kafkaError);
    }

    res.status(200).json({
      success: true,
      message: 'Planning status updated successfully',
      data: planning,
    });
  } catch (error) {
    logger.error('Error in updatePlanningStatus:', error);
    res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

/**
 * Mark planning event as completed (Owner or assigned Manager)
 * PATCH /planning/:eventId/mark-complete
 */
const markPlanningAsComplete = async (req, res) => {
  try {
    const { eventId } = req.params;

    if (!eventId || eventId.trim() === '') {
      return res.status(400).json({
        success: false,
        message: 'Event ID is required',
      });
    }

    const planning = await planningService.markPlanningAsComplete({
      eventId,
      actorAuthId: req.user?.authId,
      actorUserId: req.user?.userId,
      actorRole: req.user?.role,
    });

    try {
      await publishEvent('PLANNING_STATUS_UPDATED', {
        eventId: planning.eventId,
        authId: planning.authId,
        status: planning.status,
        assignedManagerId: planning.assignedManagerId,
        updatedBy: req.user?.authId || null,
      });

      await publishPlanningLifecycleEventIfNeeded({
        planning,
        updatedBy: req.user?.authId || null,
      });
    } catch (kafkaError) {
      logger.error('Failed to publish PLANNING_STATUS_UPDATED after mark complete:', kafkaError);
    }

    return res.status(200).json({
      success: true,
      message: 'Planning marked as complete',
      data: planning,
    });
  } catch (error) {
    logger.error('Error in markPlanningAsComplete:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

/**
 * Submit post-completion feedback (Owner)
 * PATCH /planning/:eventId/feedback
 */
const submitPlanningFeedback = async (req, res) => {
  try {
    const { eventId } = req.params;

    if (!eventId || eventId.trim() === '') {
      return res.status(400).json({
        success: false,
        message: 'Event ID is required',
      });
    }

    const updated = await planningService.submitPlanningFeedback({
      eventId,
      authId: req.user?.authId,
      platformFeedback: req.body?.platformFeedback,
      vendorFeedback: req.body?.vendorFeedback,
    });

    return res.status(200).json({
      success: true,
      message: 'Feedback submitted successfully',
      data: updated,
    });
  } catch (error) {
    logger.error('Error in submitPlanningFeedback:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message || 'Failed to submit feedback',
    });
  }
};

/**
 * Admin dashboard lists for Planning requests
 * GET /planning/admin/dashboard
 */
const getAdminDashboard = async (req, res) => {
  try {
    const limit = Number(req.query?.limit || 200);
    const result = await planningService.getAdminDashboard({ limit });

    return res.status(200).json({
      success: true,
      data: result,
    });
  } catch (error) {
    logger.error('Error in getPlanningAdminDashboard:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message || 'Failed to fetch planning dashboard',
    });
  }
};

/**
 * Delete a planning
 * DELETE /planning/:eventId
 */
const deletePlanning = async (req, res) => {
  try {
    const { eventId } = req.params;

    if (!eventId || eventId.trim() === '') {
      return res.status(400).json({
        success: false,
        message: 'Event ID is required',
      });
    }

    // Fetch planning first (for ownership check + banner cleanup)
    const planning = await planningService.getPlanningByEventId(eventId, req.user?.role || null);

    // Check ownership (unless admin)
    if (req.user.role !== 'ADMIN' && planning.authId !== req.user.authId) {
      return res.status(403).json({
        success: false,
        message: 'Access denied. You can only delete your own plannings.',
      });
    }

    // Delete banner from Cloudinary if present
    if (planning.eventBanner?.publicId) {
      try {
        await bannerUploadService.deleteBanner(planning.eventBanner.publicId);
      } catch (bannerError) {
        logger.error('Failed to delete banner from Cloudinary:', bannerError);
        // Don't block deletion if banner cleanup fails
      }
    }

    const result = await planningService.deletePlanning(eventId);

    res.status(200).json({
      success: true,
      message: result.message,
    });
  } catch (error) {
    logger.error('Error in deletePlanning:', error);
    res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

/**
 * Get planning statistics (Admin/Manager)
 * GET /planning/stats
 */
const getPlanningStats = async (req, res) => {
  try {
    const stats = await planningService.getPlanningStats();

    res.status(200).json({
      success: true,
      data: stats,
    });
  } catch (error) {
    logger.error('Error in getPlanningStats:', error);
    res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

/**
 * Manager events list
 * GET /planning/manager/events
 */
const getManagerPlanningEvents = async (req, res) => {
  try {
    const limit = Number(req.query?.limit || 200);

    // Manager should fetch their own events by default.
    // Admin may optionally supply ?managerId=... to inspect a specific manager.
    const isAdminOverride = req.user?.role === 'ADMIN' && req.query?.managerId;
    const managerId = isAdminOverride
      ? String(req.query.managerId).trim()
      : await resolveUserServiceIdFromAuthId(req.user?.authId);

    if (!managerId) {
      return res.status(isAdminOverride ? 400 : 404).json({
        success: false,
        message: isAdminOverride ? 'managerId is required' : 'Manager not found',
      });
    }

    const events = await planningService.getPlanningsForManager({ managerId, limit });
    return res.status(200).json({
      success: true,
      data: { events },
    });
  } catch (error) {
    logger.error('Error in getManagerPlanningEvents:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message || 'Failed to fetch manager planning events',
    });
  }
};

/**
 * Manager applications (planning events awaiting approval)
 * GET /planning/manager/applications
 */
const getManagerPlanningApplications = async (req, res) => {
  try {
    const limit = Number(req.query?.limit || 200);

    const isAdminOverride = req.user?.role === 'ADMIN' && req.query?.managerId;
    const managerId = isAdminOverride
      ? String(req.query.managerId).trim()
      : await resolveUserServiceIdFromAuthId(req.user?.authId);

    if (!managerId) {
      return res.status(isAdminOverride ? 400 : 404).json({
        success: false,
        message: isAdminOverride ? 'managerId is required' : 'Manager not found',
      });
    }

    const applications = await planningService.getPlanningApplicationsForManager({ managerId, limit });
    return res.status(200).json({
      success: true,
      data: { applications },
    });
  } catch (error) {
    logger.error('Error in getManagerPlanningApplications:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message || 'Failed to fetch planning applications',
    });
  }
};

/**
 * Update planning core details (Manager/Admin)
 * PATCH /planning/:eventId
 */
const updatePlanningDetails = async (req, res) => {
  try {
    const { eventId } = req.params;

    const updated = await planningService.updatePlanningDetails({
      eventId,
      updates: {
        eventTitle: req.body?.eventTitle,
        eventDescription: req.body?.eventDescription,
        locationName: req.body?.locationName,
      },
      actorRole: req.user?.role,
      actorManagerId: req.user?.role === 'ADMIN' ? null : await resolveUserServiceIdFromAuthId(req.user?.authId),
    });

    return res.status(200).json({
      success: true,
      message: 'Planning updated successfully',
      data: updated,
    });
  } catch (error) {
    logger.error('Error in updatePlanningDetails:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message || 'Failed to update planning',
    });
  }
};

/**
 * Sync planning reservation day (Owner)
 * PATCH /planning/:eventId/reservation-day
 */
const syncPlanningReservationDay = async (req, res) => {
  try {
    const { eventId } = req.params;

    if (!req.user?.authId) {
      return res.status(401).json({
        success: false,
        message: 'Authentication required',
      });
    }

    const updated = await planningService.updatePlanningReservationDayForOwner({
      eventId,
      authId: req.user.authId,
      day: req.body?.day,
    });

    return res.status(200).json({
      success: true,
      message: 'Planning date synced successfully',
      data: updated,
    });
  } catch (error) {
    logger.error('Error in syncPlanningReservationDay:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message || 'Failed to sync planning date',
    });
  }
};

/**
 * Add CORE staff to planning event (Manager/Admin)
 * POST /planning/:eventId/core-staff
 */
const addPlanningCoreStaff = async (req, res) => {
  try {
    const { eventId } = req.params;
    const staffId = req.body?.staffId;

    const updated = await planningService.addPlanningCoreStaff({
      eventId,
      staffId,
      actorRole: req.user?.role,
      actorManagerId: req.user?.role === 'ADMIN' ? null : await resolveUserServiceIdFromAuthId(req.user?.authId),
    });

    return res.status(200).json({
      success: true,
      message: 'Staff assigned successfully',
      data: updated,
    });
  } catch (error) {
    logger.error('Error in addPlanningCoreStaff:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message || 'Failed to assign staff',
    });
  }
};

/**
 * Remove CORE staff from planning event (Manager/Admin)
 * DELETE /planning/:eventId/core-staff/:staffId
 */
const removePlanningCoreStaff = async (req, res) => {
  try {
    const { eventId, staffId } = req.params;

    const updated = await planningService.removePlanningCoreStaff({
      eventId,
      staffId,
      actorRole: req.user?.role,
      actorManagerId: req.user?.role === 'ADMIN' ? null : await resolveUserServiceIdFromAuthId(req.user?.authId),
    });

    return res.status(200).json({
      success: true,
      message: 'Staff removed successfully',
      data: updated,
    });
  } catch (error) {
    logger.error('Error in removePlanningCoreStaff:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message || 'Failed to remove staff',
    });
  }
};

/**
 * Release generated revenue payout to user for public planning event (Manager/Admin)
 * PATCH /planning/:eventId/generated-revenue-payout
 */
const releasePlanningGeneratedRevenuePayout = async (req, res) => {
  try {
    const { eventId } = req.params;

    const updated = await planningService.releasePlanningGeneratedRevenuePayout({
      eventId,
      actorRole: req.user?.role,
      actorAuthId: req.user?.authId,
      actorManagerId: req.user?.role === 'ADMIN' ? null : await resolveUserServiceIdFromAuthId(req.user?.authId),
      mode: req.body?.mode || 'DEMO',
    });

    return res.status(200).json({
      success: true,
      message: updated?.generatedRevenuePayoutSummary?.alreadyProcessed
        ? 'Generated revenue payout already sent to user'
        : 'Generated revenue payout sent to user',
      data: updated,
    });
  } catch (error) {
    logger.error('Error in releasePlanningGeneratedRevenuePayout:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message || 'Failed to release generated revenue payout',
    });
  }
};

/**
 * Trigger EMAIL BLAST promotion action (Manager/Admin)
 * POST /planning/:eventId/promotion-actions/email-blast
 */
const triggerPlanningEmailBlastPromotionAction = async (req, res) => {
  try {
    const { eventId } = req.params;

    const result = await planningService.triggerPlanningEmailBlastPromotionAction({
      eventId,
      actorRole: req.user?.role,
      actorAuthId: req.user?.authId,
      actorManagerId: req.user?.role === 'ADMIN' ? null : await resolveUserServiceIdFromAuthId(req.user?.authId),
    });

    return res.status(200).json({
      success: true,
      message: 'Email blast request submitted successfully',
      data: result,
    });
  } catch (error) {
    logger.error('Error in triggerPlanningEmailBlastPromotionAction:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message || 'Failed to trigger email blast',
    });
  }
};

/**
 * Confirm a planning selection (Owner)
 * POST /planning/:eventId/confirm
 */
const confirmPlanning = async (req, res) => {
  try {
    const { eventId } = req.params;

    if (!req.user?.authId) {
      return res.status(401).json({
        success: false,
        message: 'Authentication required',
      });
    }

    if (!eventId || eventId.trim() === '') {
      return res.status(400).json({
        success: false,
        message: 'Event ID is required',
      });
    }

    const confirmed = await planningService.confirmPlanning({
      eventId,
      authId: req.user.authId,
    });

    return res.status(200).json({
      success: true,
      message: 'Planning confirmed successfully',
      data: confirmed,
    });
  } catch (error) {
    logger.error('Error in confirmPlanning:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

/**
 * Fetch vendor services for a planning, based on a selected serviceCategory.
 *
 * GET /planning/:eventId/vendors?serviceCategory=...&sort=Nearest&priceMin=0&priceMax=200000&radiusKm=50&limit=50&skip=0&q=...
 */
const getVendorsForPlanning = async (req, res) => {
  try {
    const { eventId } = req.params;
    const {
      serviceCategory,
      sort,
      priceMin,
      priceMax,
      radiusKm,
      limit,
      skip,
      q,
      day,
      from,
      to,
      includeUnavailable,
    } = req.query;

    const includeUnavailableVendors = !['false', '0', 'no'].includes(
      String(includeUnavailable == null ? 'true' : includeUnavailable).trim().toLowerCase()
    );

    if (!eventId || !eventId.trim()) {
      return res.status(400).json({ success: false, message: 'Event ID is required' });
    }

    if (!serviceCategory || !String(serviceCategory).trim()) {
      return res.status(400).json({ success: false, message: 'serviceCategory is required' });
    }

    const planning = await ensureAccessToPlanning({ eventId: eventId.trim(), user: req.user });
    const lat1 = toNumber(planning?.location?.latitude, null);
    const lon1 = toNumber(planning?.location?.longitude, null);
    const normalizedServiceCategory = String(serviceCategory).trim();
    const isVenueCategory = normalizedServiceCategory === 'Venue';

    const planningReservationDays = vendorReservationService.planningToReservationDays(planning);
    const planningFromFallback = planningReservationDays[0] || null;
    const planningToFallback = planningReservationDays.length > 0
      ? planningReservationDays[planningReservationDays.length - 1]
      : null;
    const planningDayFallback = planningFromFallback;

    const requestedDay = normalizeIstDayInput(day);
    const requestedFrom = normalizeIstDayInput(from);
    const requestedTo = normalizeIstDayInput(to);

    const isPublicPlanning = String(planning?.category || '').trim().toLowerCase() === 'public';

    const effectiveFrom = requestedFrom || (isPublicPlanning ? planningFromFallback : null);
    const effectiveTo = requestedTo || (isPublicPlanning ? (planningToFallback || effectiveFrom) : null);
    const effectiveDay = requestedDay || (!isPublicPlanning ? planningDayFallback : null);

    const reservationDays = vendorReservationService.resolveReservationDays({
      day: effectiveDay,
      from: effectiveFrom,
      to: effectiveTo,
      days: isPublicPlanning ? planningReservationDays : [],
    });

    const effectiveDemandDay = effectiveDay || effectiveFrom || planningDayFallback;

    const feesConfig = await promoteConfigService.getFees();
    const configuredMultipliers = parseDemandMultipliers(feesConfig);
    const demandTier = getDemandTierForDay(effectiveDemandDay);
    const activeMultipliers = demandTier === 'HIGH_DEMAND'
      ? configuredMultipliers.highDemand
      : configuredMultipliers.normal;

    const hasRadiusParam = Object.prototype.hasOwnProperty.call(req.query, 'radiusKm');
    const rawRadiusKm = hasRadiusParam ? toNumber(radiusKm, 50) : null;
    const effectiveRadiusKm = rawRadiusKm != null && rawRadiusKm > 0 ? rawRadiusKm : (hasRadiusParam ? 50 : null);
    const effectiveLimit = Math.min(toNumber(limit, 100), 100);

    const vendorServices = await fetchAllVendorsBasedOnService({
      serviceCategory: normalizedServiceCategory,
      latitude: lat1,
      longitude: lon1,
      radiusKm: effectiveRadiusKm,
      limit: effectiveLimit,
      skip: toNumber(skip, 0),
      businessName: q ? String(q).trim() : null,
      // Venue uses per-service coordinates (a vendor can have multiple venues across cities),
      // so we do NOT geo-filter upstream. We'll filter in this controller.
      enableGeo: !isVenueCategory && hasRadiusParam && lat1 != null && lon1 != null,
      day: reservationDays.length === 1 ? reservationDays[0] : null,
      from: reservationDays.length > 1 ? reservationDays[0] : null,
      to: reservationDays.length > 1 ? reservationDays[reservationDays.length - 1] : null,
      preferAvailabilityEndpoint: !includeUnavailableVendors,
    });

    const minP = toNumber(priceMin, 0);
    const maxP = toNumber(priceMax, null);
    const sortKey = normalizeSortKey(sort);

    let serviceItems = vendorServices
      .map((s) => {
        const basePrice = toNumber(s?.price, toNumber(s?.basePrice, null));
        const scaledPriceMin = basePrice == null ? null : Math.max(0, Math.round(basePrice * activeMultipliers.min));
        const scaledPriceMax = basePrice == null ? null : Math.max(0, Math.round(basePrice * activeMultipliers.max));

        return {
          serviceId: s?._id,
          vendorAuthId: s?.authId,
          businessName: s?.businessName,
          name: s?.name,
          serviceCategory: s?.serviceCategory,
          categoryId: s?.categoryId,
          basePrice,
          price: scaledPriceMin,
          priceMin: scaledPriceMin,
          priceMax: scaledPriceMax,
          tier: s?.tier,
          description: s?.description,
          details: s?.details || {},
          latitude: s?.latitude,
          longitude: s?.longitude,
          rating: extractRating(s),
          createdAt: s?.createdAt,
        };
      })
      .filter((v) => {
        const minPrice = toNumber(v.priceMin, null);
        const maxPrice = toNumber(v.priceMax, minPrice);
        if (minPrice == null && maxPrice == null) return true;
        if (maxP != null && minPrice != null && minPrice > maxP) return false;
        if (minP != null && maxPrice != null && maxPrice < minP) return false;
        return true;
      });

    // Venue-only geo filter: use the venue listing coordinates stored on the service.
    if (isVenueCategory && hasRadiusParam && lat1 != null && lon1 != null && effectiveRadiusKm != null) {
      serviceItems = serviceItems.filter((s) => {
        const sLat = toNumber(s?.details?.locationLat, toNumber(s?.details?.lat, null));
        const sLon = toNumber(s?.details?.locationLng, toNumber(s?.details?.lng, null));
        if (sLat == null || sLon == null) return false;
        const d = haversineKm({ lat1, lon1, lat2: sLat, lon2: sLon });
        return d <= effectiveRadiusKm;
      });
    }

    const authIds = Array.from(
      new Set(serviceItems.map((s) => s.vendorAuthId).filter(Boolean))
    );

    const vendorApps = await fetchPublicVendorsByAuthIds(authIds);
    const vendorAppByAuthId = new Map(vendorApps.map((v) => [v.authId, v]));

    // Group services by vendor
    let items = authIds
      .map((authId) => {
        const services = serviceItems
          .filter((s) => s.vendorAuthId === authId)
          .sort((a, b) => {
            const ta = a.createdAt ? new Date(a.createdAt).getTime() : 0;
            const tb = b.createdAt ? new Date(b.createdAt).getTime() : 0;
            return tb - ta;
          });

        const app = vendorAppByAuthId.get(authId) || null;

        // Default: vendor HQ coordinates (from vendor app), with service fallback.
        // Venue special-case: distance should be computed from the venue service's own location.
        let vLat = toNumber(app?.latitude, toNumber(services?.[0]?.latitude, null));
        let vLon = toNumber(app?.longitude, toNumber(services?.[0]?.longitude, null));
        let distanceKm =
          lat1 != null && lon1 != null && vLat != null && vLon != null
            ? haversineKm({ lat1, lon1, lat2: vLat, lon2: vLon })
            : null;

        if (isVenueCategory && lat1 != null && lon1 != null) {
          const venueCandidates = services
            .map((s) => {
              const sLat = toNumber(s?.details?.locationLat, toNumber(s?.details?.lat, null));
              const sLon = toNumber(s?.details?.locationLng, toNumber(s?.details?.lng, null));
              if (sLat == null || sLon == null) return null;
              const d = haversineKm({ lat1, lon1, lat2: sLat, lon2: sLon });
              return {
                lat: sLat,
                lon: sLon,
                distanceKm: d,
                locationName: s?.details?.locationAreaName || null,
              };
            })
            .filter(Boolean)
            .sort((a, b) => a.distanceKm - b.distanceKm);

          if (venueCandidates.length > 0) {
            vLat = venueCandidates[0].lat;
            vLon = venueCandidates[0].lon;
            distanceKm = venueCandidates[0].distanceKm;

            // Prefer the specific venue area name when available.
            if (app) {
              app.location = venueCandidates[0].locationName || app.location;
            }
          }
        }

        const pricesMin = services.map((s) => toNumber(s.priceMin, null)).filter((p) => p != null);
        const pricesMax = services.map((s) => toNumber(s.priceMax, null)).filter((p) => p != null);
        const priceMin = pricesMin.length ? Math.min(...pricesMin) : null;
        const priceMax = pricesMax.length ? Math.max(...pricesMax) : null;

        const rating = Math.max(
          0,
          ...services.map((s) => toNumber(s.rating, 0)).filter((n) => n != null)
        );

        const latestCreatedAt = services[0]?.createdAt || null;

        return {
          vendorAuthId: authId,
          businessName: app?.businessName || services?.[0]?.businessName || null,
          serviceCategory: normalizedServiceCategory,
          categoryId: services?.[0]?.categoryId || null,
          rating,
          images: app?.images || null,
          banner: app?.images?.banner?.fileUrl || null,
          profileImage: app?.images?.profile?.fileUrl || null,
          location: {
            name: app?.location || null,
            latitude: vLat,
            longitude: vLon,
            mapsUrl: buildMapsUrl({ latitude: vLat, longitude: vLon }),
          },
          distanceKm,
          description: app?.description || null,
          priceMin,
          priceMax,
          latestCreatedAt,
          services: services.map((s) => ({
            serviceId: s.serviceId,
            name: s.name,
            price: s.price,
            priceMin: s.priceMin,
            priceMax: s.priceMax,
            basePrice: s.basePrice,
            tier: s.tier,
            description: s.description,
            details: s.details,
            rating: s.rating,
            createdAt: s.createdAt,
          })),
        };
      })
      // If vendor apps endpoint returns fewer vendors (e.g. not APPROVED), still allow fallback from service items.
      .filter((v) => Array.isArray(v.services) && v.services.length > 0);

    // Apply vendor-level price filter: keep vendor if any service price is within range
    items = items.filter((v) => {
      const min = toNumber(v.priceMin, null);
      const max = toNumber(v.priceMax, null);
      if (min == null && max == null) return true;
      if (maxP != null && min != null && min > maxP) return false;
      if (minP != null && max != null && max < minP) return false;
      return true;
    });

    // Mark reserved resources by other events on the same day.
    // - Venue: lock is service-level (serviceId)
    // - Other categories: lock is vendor-level (vendorAuthId)
    if (reservationDays.length > 0) {
      if (isVenueCategory) {
        const reservedServiceIds = await vendorReservationService.listReservedServiceIdsForDays({
          days: reservationDays,
          excludeEventId: eventId.trim(),
        });

        const reservedServiceSet = new Set(
          reservedServiceIds
            .map((id) => String(id || '').trim())
            .filter(Boolean)
        );

        items = items
          .map((v) => {
            const services = Array.isArray(v?.services) ? v.services : [];
            if (services.length === 0) return null;

            const mappedServices = services.map((s) => {
              const sid = String(s?.serviceId || '').trim();
              const isLocked = sid ? reservedServiceSet.has(sid) : false;
              return {
                ...s,
                isAvailable: !isLocked,
                unavailableReason: isLocked ? 'CURRENTLY_LOCKED_WITH_ANOTHER_EVENT' : null,
              };
            });

            const hasAnyAvailableService = mappedServices.some((s) => s?.isAvailable !== false);

            return {
              ...v,
              services: mappedServices,
              isAvailable: hasAnyAvailableService,
              unavailableReason: hasAnyAvailableService ? null : 'CURRENTLY_LOCKED_WITH_ANOTHER_EVENT',
            };
          })
          .filter(Boolean);
      } else {
        const reserved = await vendorReservationService.listReservedVendorAuthIdsForDays({
          days: reservationDays,
          excludeEventId: eventId.trim(),
        });

        const reservedSet = new Set(
          reserved
            .map((id) => String(id || '').trim())
            .filter(Boolean)
        );

        items = items
          .map((v) => {
            const vendorAuthId = String(v?.vendorAuthId || '').trim();
            const isLocked = vendorAuthId ? reservedSet.has(vendorAuthId) : false;
            return {
              ...v,
              isAvailable: !isLocked,
              unavailableReason: isLocked ? 'CURRENTLY_LOCKED_WITH_ANOTHER_EVENT' : null,
            };
          })
          .filter((v) => Boolean(String(v?.vendorAuthId || '').trim()));
      }
    } else {
      items = items.map((v) => ({
        ...v,
        isAvailable: true,
        unavailableReason: null,
        services: Array.isArray(v?.services)
          ? v.services.map((s) => ({
            ...s,
            isAvailable: true,
            unavailableReason: null,
          }))
          : v?.services,
      }));
    }

    if (sortKey === 'nearest') {
      items = [...items].sort((a, b) => {
        const da = a.distanceKm == null ? Number.POSITIVE_INFINITY : a.distanceKm;
        const db = b.distanceKm == null ? Number.POSITIVE_INFINITY : b.distanceKm;
        return da - db;
      });
    } else if (sortKey === 'topRated') {
      items = [...items].sort((a, b) => {
        const dr = (b.rating || 0) - (a.rating || 0);
        if (dr !== 0) return dr;
        const da = a.distanceKm == null ? Number.POSITIVE_INFINITY : a.distanceKm;
        const db = b.distanceKm == null ? Number.POSITIVE_INFINITY : b.distanceKm;
        return da - db;
      });
    } else if (sortKey === 'priceLow') {
      items = [...items].sort((a, b) => (toNumber(a.priceMin, Number.POSITIVE_INFINITY) - toNumber(b.priceMin, Number.POSITIVE_INFINITY)));
    } else if (sortKey === 'priceHigh') {
      items = [...items].sort((a, b) => (toNumber(b.priceMax, 0) - toNumber(a.priceMax, 0)));
    } else if (sortKey === 'trending') {
      items = [...items].sort((a, b) => {
        const ta = a.latestCreatedAt ? new Date(a.latestCreatedAt).getTime() : 0;
        const tb = b.latestCreatedAt ? new Date(b.latestCreatedAt).getTime() : 0;
        return tb - ta;
      });
    }

    if (includeUnavailableVendors) {
      items = [...items].sort((a, b) => {
        const aUnavailable = a?.isAvailable === false ? 1 : 0;
        const bUnavailable = b?.isAvailable === false ? 1 : 0;
        return aUnavailable - bUnavailable;
      });
    }

    return res.status(200).json({
      success: true,
      data: {
        serviceCategory: normalizedServiceCategory,
        demandTier,
        pricingMultipliers: {
          active: activeMultipliers,
          normal: configuredMultipliers.normal,
          highDemand: configuredMultipliers.highDemand,
        },
        vendors: items,
      },
    });
  } catch (error) {
    logger.error('Error in getVendorsForPlanning:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message || 'Failed to fetch vendors',
    });
  }
};

module.exports = {
  createPlanning,
  getMyPlannings,
  getPlanningByEventId,
  getPlanningQuoteLatest,
  sendPlanningQuoteEmail,
  getAllPlannings,
  updatePlanningStatus,
  markPlanningAsComplete,
  submitPlanningFeedback,
  unassignPlanningManager,
  getAdminDashboard,
  confirmPlanning,
  deletePlanning,
  getPlanningStats,
  getVendorsForPlanning,
  getManagerPlanningEvents,
  getManagerPlanningApplications,
  updatePlanningDetails,
  syncPlanningReservationDay,
  addPlanningCoreStaff,
  removePlanningCoreStaff,
  releasePlanningGeneratedRevenuePayout,
  triggerPlanningEmailBlastPromotionAction,
};
