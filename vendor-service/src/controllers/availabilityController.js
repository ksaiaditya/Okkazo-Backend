const availabilityService = require('../services/vendorAvailabilityService');
const logger = require('../utils/logger');
const { formatSuccessResponse, formatErrorResponse } = require('../utils/helpers');

// Public: GET /api/vendor/services/available
const searchAvailableVendorServices = async (req, res) => {
  try {
    const result = await availabilityService.searchAvailableServices(req.query);
    return res.status(200).json(formatSuccessResponse(result));
  } catch (error) {
    logger.error('Error in searchAvailableVendorServices:', error);
    return res.status(error.statusCode || 500).json(
      formatErrorResponse(error.statusCode === 400 ? 'VALIDATION_ERROR' : 'INTERNAL_ERROR', error.message)
    );
  }
};

// Vendor: GET /api/vendor/me/availability?day=... OR from/to
const getMyAvailability = async (req, res) => {
  try {
    const vendorAuthId = req.user?.authId;
    if (!vendorAuthId) {
      return res.status(401).json(formatErrorResponse('UNAUTHORIZED', 'User not authenticated'));
    }

    const { fromDay, toDay } = availabilityService.normalizeRange(req.query);
    const items = await availabilityService.listUnavailable({ vendorAuthId, fromDay, toDay });

    return res.status(200).json(formatSuccessResponse({ unavailable: items, from: fromDay, to: toDay }));
  } catch (error) {
    logger.error('Error in getMyAvailability:', error);
    return res.status(error.statusCode || 500).json(
      formatErrorResponse(error.statusCode === 400 ? 'VALIDATION_ERROR' : 'INTERNAL_ERROR', error.message)
    );
  }
};

// Vendor: POST /api/vendor/me/availability/unavailable { day, reason? }
const addMyManualUnavailability = async (req, res) => {
  try {
    const vendorAuthId = req.user?.authId;
    if (!vendorAuthId) {
      return res.status(401).json(formatErrorResponse('UNAUTHORIZED', 'User not authenticated'));
    }

    const { day, reason } = req.body || {};

    const doc = await availabilityService.upsertManualUnavailable({
      vendorAuthId,
      day,
      reason,
      actor: { authId: req.user?.authId, role: req.user?.role },
    });

    return res.status(200).json(formatSuccessResponse(doc, 'Date marked as unavailable'));
  } catch (error) {
    logger.error('Error in addMyManualUnavailability:', error);
    return res.status(error.statusCode || 500).json(
      formatErrorResponse(error.statusCode === 400 ? 'VALIDATION_ERROR' : 'INTERNAL_ERROR', error.message)
    );
  }
};

// Vendor: DELETE /api/vendor/me/availability/unavailable { day }
const removeMyManualUnavailability = async (req, res) => {
  try {
    const vendorAuthId = req.user?.authId;
    if (!vendorAuthId) {
      return res.status(401).json(formatErrorResponse('UNAUTHORIZED', 'User not authenticated'));
    }

    const day = (req.body && req.body.day) || req.query?.day;
    const result = await availabilityService.removeManualUnavailable({ vendorAuthId, day });

    return res.status(200).json(formatSuccessResponse(result, 'Date marked as available'));
  } catch (error) {
    logger.error('Error in removeMyManualUnavailability:', error);
    return res.status(error.statusCode || 500).json(
      formatErrorResponse(error.statusCode === 400 ? 'VALIDATION_ERROR' : 'INTERNAL_ERROR', error.message)
    );
  }
};

// Admin/Manager: GET /api/vendor/availability/:vendorAuthId?day|from/to
const getVendorAvailability = async (req, res) => {
  try {
    const vendorAuthId = req.params.vendorAuthId;
    const { fromDay, toDay } = availabilityService.normalizeRange(req.query);
    const items = await availabilityService.listUnavailable({ vendorAuthId, fromDay, toDay });

    return res.status(200).json(formatSuccessResponse({ vendorAuthId, unavailable: items, from: fromDay, to: toDay }));
  } catch (error) {
    logger.error('Error in getVendorAvailability:', error);
    return res.status(error.statusCode || 500).json(
      formatErrorResponse(error.statusCode === 400 ? 'VALIDATION_ERROR' : 'INTERNAL_ERROR', error.message)
    );
  }
};

// Admin/Manager: POST /api/vendor/availability/bookings/block
const blockVendorForEvent = async (req, res) => {
  try {
    const { vendorAuthId, eventId, days } = req.body || {};

    const result = await availabilityService.blockDaysForEvent({
      vendorAuthId,
      eventId,
      days,
      actor: { authId: req.user?.authId, role: req.user?.role },
    });

    return res.status(200).json(formatSuccessResponse(result, 'Vendor dates blocked for booking'));
  } catch (error) {
    logger.error('Error in blockVendorForEvent:', error);
    return res.status(error.statusCode || 500).json(
      formatErrorResponse(error.statusCode === 400 ? 'VALIDATION_ERROR' : 'INTERNAL_ERROR', error.message)
    );
  }
};

// Admin/Manager: POST /api/vendor/availability/bookings/unblock
const unblockVendorForEvent = async (req, res) => {
  try {
    const { vendorAuthId, eventId, days } = req.body || {};

    const result = await availabilityService.unblockDaysForEvent({
      vendorAuthId,
      eventId,
      days,
    });

    return res.status(200).json(formatSuccessResponse(result, 'Vendor dates unblocked for booking'));
  } catch (error) {
    logger.error('Error in unblockVendorForEvent:', error);
    return res.status(error.statusCode || 500).json(
      formatErrorResponse(error.statusCode === 400 ? 'VALIDATION_ERROR' : 'INTERNAL_ERROR', error.message)
    );
  }
};

module.exports = {
  searchAvailableVendorServices,
  getMyAvailability,
  addMyManualUnavailability,
  removeMyManualUnavailability,
  getVendorAvailability,
  blockVendorForEvent,
  unblockVendorForEvent,
};
