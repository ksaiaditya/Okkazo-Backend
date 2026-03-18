const promoteService = require('../services/promoteService');
const bannerUploadService = require('../services/bannerUploadService');
const { publishEvent } = require('../kafka/eventProducer');
const logger = require('../utils/logger');
const promoteConfigService = require('../services/promoteConfigService');

// ─── Create a new promote record ──────────────────────────────────────────────
/**
 * POST /promote
 *
 * Accepts multipart/form-data:
 *   - eventBanner   : single image file (required)
 *   - authProofs    : up to 10 image files (optional)
 *   - all other fields as JSON strings (parsed by validateCreatePromote)
 */
const createPromote = async (req, res) => {
  try {
    if (!req.user?.authId) {
      return res.status(401).json({ success: false, message: 'Authentication required' });
    }

    const payload = {
      ...req.body,
      authId: req.user.authId,
    };

    // ── Upload event banner (required) ────────────────────────────────────────
    const bannerFile = req.files?.eventBanner?.[0];
    if (!bannerFile) {
      return res.status(400).json({ success: false, message: 'eventBanner image is required' });
    }

    const bannerResult = await bannerUploadService.uploadBanner(
      bannerFile,
      'promote-banners'
    );
    payload.eventBanner = {
      url: bannerResult.url,
      publicId: bannerResult.publicId,
      mimeType: bannerResult.mimeType,
      sizeBytes: bannerResult.sizeBytes,
    };

    // ── Upload authenticity proof images ──────────────────────────────────────
    const proofFiles = req.files?.authProofs || [];
    const uploadedProofs = [];

    for (const file of proofFiles) {
      try {
        const result = await bannerUploadService.uploadBanner(file, 'promote-proofs');
        uploadedProofs.push({
          url: result.url,
          publicId: result.publicId,
          mimeType: result.mimeType,
          sizeBytes: result.sizeBytes,
        });
      } catch (err) {
        logger.error('Failed to upload auth proof:', err.message);
        // Non-blocking: continue with other files
      }
    }

    payload.authenticityProofs = uploadedProofs;

    const result = await promoteService.createPromote(payload);

    // ── Publish Kafka event ───────────────────────────────────────────────────
    try {
      await publishEvent('PROMOTE_CREATED', {
        eventId: result.eventId,
        promoteId: result.promoteId,
        authId: req.user.authId,
        eventTitle: result.eventTitle,
        eventStatus: result.eventStatus,
        schedule: result.schedule,
      });
    } catch (kafkaError) {
      logger.error('Failed to publish PROMOTE_CREATED:', kafkaError.message);
    }

    return res.status(201).json({
      success: true,
      message: 'Promote record created successfully',
      data: result,
    });
  } catch (error) {
    logger.error('Error in createPromote controller:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

// ─── Get own promote records ──────────────────────────────────────────────────
/**
 * GET /promote/me
 */
const getMyPromotes = async (req, res) => {
  try {
    if (!req.user?.authId) {
      return res.status(401).json({ success: false, message: 'Authentication required' });
    }

    let { page = 1, limit = 10 } = req.query;
    page = Math.max(1, parseInt(page, 10) || 1);
    limit = Math.min(100, Math.max(1, parseInt(limit, 10) || 10));

    const result = await promoteService.getMyPromotes(req.user.authId, page, limit);

    return res.status(200).json({ success: true, ...result });
  } catch (error) {
    logger.error('Error in getMyPromotes:', error);
    return res.status(error.statusCode || 500).json({ success: false, message: error.message });
  }
};

// ─── Get single promote by eventId ───────────────────────────────────────────
/**
 * GET /promote/:eventId
 */
const getPromoteByEventId = async (req, res) => {
  try {
    const { eventId } = req.params;

    const promote = await promoteService.getPromoteByEventId(eventId);

    // Regular users can only access their own records
    if (
      req.user.role !== 'ADMIN' &&
      req.user.role !== 'MANAGER' &&
      promote.authId !== req.user.authId
    ) {
      return res.status(403).json({ success: false, message: 'Access denied' });
    }

    return res.status(200).json({ success: true, data: promote });
  } catch (error) {
    logger.error('Error in getPromoteByEventId:', error);
    return res.status(error.statusCode || 500).json({ success: false, message: error.message });
  }
};

// ─── Get all promotes (admin / manager) ──────────────────────────────────────
/**
 * GET /promote
 */
const getAllPromotes = async (req, res) => {
  try {
    let { page = 1, limit = 10, eventStatus, platformFeePaid, authId, search } = req.query;
    page = Math.max(1, parseInt(page, 10) || 1);
    limit = Math.min(100, Math.max(1, parseInt(limit, 10) || 10));

    const filters = {};
    if (eventStatus) filters.eventStatus = eventStatus;
    if (platformFeePaid !== undefined) filters.platformFeePaid = platformFeePaid;
    if (authId) filters.authId = authId;
    if (search?.trim()) filters.search = search.trim();

    const result = await promoteService.getAllPromotes(filters, page, limit);

    return res.status(200).json({ success: true, ...result });
  } catch (error) {
    logger.error('Error in getAllPromotes:', error);
    return res.status(error.statusCode || 500).json({ success: false, message: error.message });
  }
};

// ─── Update promote status (manager / admin) ──────────────────────────────────
/**
 * PATCH /promote/:eventId/status
 */
const updatePromoteStatus = async (req, res) => {
  try {
    const { eventId } = req.params;
    const { eventStatus, assignedManagerId } = req.body;

    if (!eventStatus) {
      return res.status(400).json({ success: false, message: 'eventStatus is required' });
    }

    const promote = await promoteService.updatePromoteStatus(eventId, eventStatus, assignedManagerId);

    // Publish Kafka
    try {
      await publishEvent('PROMOTE_STATUS_UPDATED', {
        eventId: promote.eventId,
        authId: promote.authId,
        eventStatus: promote.eventStatus,
        updatedBy: req.user.authId,
      });
    } catch (kafkaError) {
      logger.error('Failed to publish PROMOTE_STATUS_UPDATED:', kafkaError.message);
    }

    return res.status(200).json({ success: true, message: 'Status updated', data: promote });
  } catch (error) {
    logger.error('Error in updatePromoteStatus:', error);
    return res.status(error.statusCode || 500).json({ success: false, message: error.message });
  }
};

// ─── Assign manager (admin only) ─────────────────────────────────────────────
/**
 * PATCH /promote/:eventId/assign
 */
const assignManager = async (req, res) => {
  try {
    const { eventId } = req.params;
    const { managerId } = req.body;

    if (!managerId) {
      return res.status(400).json({ success: false, message: 'managerId is required' });
    }

    const promote = await promoteService.assignManager(eventId, managerId);

    return res.status(200).json({ success: true, message: 'Manager assigned', data: promote });
  } catch (error) {
    logger.error('Error in assignManager:', error);
    return res.status(error.statusCode || 500).json({ success: false, message: error.message });
  }
};

// ─── Delete promote (owner or admin) ─────────────────────────────────────────
/**
 * DELETE /promote/:eventId
 */
const deletePromote = async (req, res) => {
  try {
    const { eventId } = req.params;

    const promote = await promoteService.getPromoteByEventId(eventId);

    // Ownership check
    if (req.user.role !== 'ADMIN' && promote.authId !== req.user.authId) {
      return res.status(403).json({ success: false, message: 'Access denied' });
    }

    // Clean up Cloudinary assets
    const cleanupJobs = [];

    if (promote.eventBanner?.publicId) {
      cleanupJobs.push(
        bannerUploadService.deleteBanner(promote.eventBanner.publicId).catch((e) =>
          logger.error('Banner cleanup failed:', e.message)
        )
      );
    }

    for (const proof of promote.authenticityProofs || []) {
      if (proof.publicId) {
        cleanupJobs.push(
          bannerUploadService.deleteBanner(proof.publicId).catch((e) =>
            logger.error('Proof cleanup failed:', e.message)
          )
        );
      }
    }

    await Promise.allSettled(cleanupJobs);

    const result = await promoteService.deletePromote(eventId);

    return res.status(200).json({ success: true, message: result.message });
  } catch (error) {
    logger.error('Error in deletePromote:', error);
    return res.status(error.statusCode || 500).json({ success: false, message: error.message });
  }
};

module.exports = {
  createPromote,
  getPlatformFee: async (req, res) => {
    try {
      const result = await promoteConfigService.getPlatformFee();
      return res.status(200).json({ success: true, data: result });
    } catch (error) {
      logger.error('Error in getPlatformFee:', error);
      return res.status(error.statusCode || 500).json({ success: false, message: error.message });
    }
  },
  updatePlatformFee: async (req, res) => {
    try {
      const { platformFee } = req.body;
      const result = await promoteConfigService.updatePlatformFee({
        platformFee,
        updatedByAuthId: req.user?.authId || null,
      });
      return res.status(200).json({ success: true, message: 'Platform fee updated', data: result });
    } catch (error) {
      logger.error('Error in updatePlatformFee:', error);
      return res.status(error.statusCode || 500).json({ success: false, message: error.message });
    }
  },
  getMyPromotes,
  getPromoteByEventId,
  getAllPromotes,
  updatePromoteStatus,
  assignManager,
  deletePromote,
};
