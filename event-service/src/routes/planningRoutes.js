const express = require('express');
const planningController = require('../controllers/planningController');
const { authorizeRoles, isAdminOrManager, isAdmin } = require('../middleware/authorization');
const { validateCreatePlanning } = require('../middleware/planningValidation');
const { upload } = require('../middleware/upload');

const router = express.Router();

// All routes require at least USER role (gateway already enforces auth)

// POST /planning - Create a new planning
// Uses multer to handle optional eventBanner file upload (public events)
// The banner field name is 'eventBanner'
router.post(
  '/planning',
  authorizeRoles(['USER', 'VENDOR', 'ADMIN', 'MANAGER']),
  upload.single('eventBanner'),
  validateCreatePlanning,
  planningController.createPlanning
);

// GET /planning/stats - Get planning statistics (Admin/Manager only)
router.get(
  '/planning/stats',
  isAdminOrManager,
  planningController.getPlanningStats
);

// GET /planning/admin/dashboard - Planning admin dashboard lists (Admin only)
router.get(
  '/planning/admin/dashboard',
  isAdmin,
  planningController.getAdminDashboard
);

// GET /planning/manager/events - Manager's assigned planning events (Manager/Admin)
router.get(
  '/planning/manager/events',
  isAdminOrManager,
  planningController.getManagerPlanningEvents
);

// GET /planning/manager/applications - Manager's assigned planning applications awaiting approval (Manager/Admin)
router.get(
  '/planning/manager/applications',
  isAdminOrManager,
  planningController.getManagerPlanningApplications
);

// GET /planning/me - Get current user's plannings
router.get(
  '/planning/me',
  authorizeRoles(['USER', 'VENDOR', 'ADMIN', 'MANAGER']),
  planningController.getMyPlannings
);

// GET /planning - Get all plannings with filters (Admin/Manager only)
router.get(
  '/planning',
  isAdminOrManager,
  planningController.getAllPlannings
);

// GET /planning/:eventId/quote/latest - Get latest locked quote snapshot
router.get(
  '/planning/:eventId/quote/latest',
  authorizeRoles(['USER', 'VENDOR', 'ADMIN', 'MANAGER']),
  planningController.getPlanningQuoteLatest
);

// POST /planning/:eventId/quote/send-email - Manually send quotation email (Manager/Admin)
router.post(
  '/planning/:eventId/quote/send-email',
  isAdminOrManager,
  planningController.sendPlanningQuoteEmail
);

// GET /planning/:eventId - Get a single planning by eventId
router.get(
  '/planning/:eventId',
  authorizeRoles(['USER', 'VENDOR', 'ADMIN', 'MANAGER']),
  planningController.getPlanningByEventId
);

// PATCH /planning/:eventId - Update planning details (Manager/Admin)
router.patch(
  '/planning/:eventId',
  isAdminOrManager,
  planningController.updatePlanningDetails
);

// PATCH /planning/:eventId/reservation-day - Sync planning day from vendor-selection flow (Owner)
router.patch(
  '/planning/:eventId/reservation-day',
  authorizeRoles(['USER', 'VENDOR', 'ADMIN', 'MANAGER']),
  planningController.syncPlanningReservationDay
);

// POST /planning/:eventId/core-staff - Assign a CORE staff member (Manager/Admin)
router.post(
  '/planning/:eventId/core-staff',
  authorizeRoles(['MANAGER']),
  planningController.addPlanningCoreStaff
);

// DELETE /planning/:eventId/core-staff/:staffId - Unassign a CORE staff member (Manager/Admin)
router.delete(
  '/planning/:eventId/core-staff/:staffId',
  authorizeRoles(['MANAGER']),
  planningController.removePlanningCoreStaff
);

// PATCH /planning/:eventId/generated-revenue-payout - Release generated revenue to user (demo)
router.patch(
  '/planning/:eventId/generated-revenue-payout',
  authorizeRoles(['MANAGER', 'ADMIN']),
  planningController.releasePlanningGeneratedRevenuePayout
);

// POST /planning/:eventId/promotion-actions/email-blast - Trigger email blast promotion
router.post(
  '/planning/:eventId/promotion-actions/email-blast',
  authorizeRoles(['MANAGER', 'ADMIN']),
  planningController.triggerPlanningEmailBlastPromotionAction
);

// GET /planning/:eventId/vendors - Fetch vendors for a service category
router.get(
  '/planning/:eventId/vendors',
  authorizeRoles(['USER', 'VENDOR', 'ADMIN', 'MANAGER']),
  planningController.getVendorsForPlanning
);

// POST /planning/:eventId/confirm - Confirm finalized selection (Owner)
router.post(
  '/planning/:eventId/confirm',
  authorizeRoles(['USER', 'VENDOR', 'ADMIN', 'MANAGER']),
  planningController.confirmPlanning
);

// PATCH /planning/:eventId/status - Update planning status (Admin/Manager only)
router.patch(
  '/planning/:eventId/status',
  isAdminOrManager,
  planningController.updatePlanningStatus
);

// PATCH /planning/:eventId/mark-complete - Mark private planning as completed (Owner/Assigned Manager)
router.patch(
  '/planning/:eventId/mark-complete',
  authorizeRoles(['USER', 'MANAGER']),
  planningController.markPlanningAsComplete
);

// PATCH /planning/:eventId/feedback - Submit post-completion feedback (Owner)
router.patch(
  '/planning/:eventId/feedback',
  authorizeRoles(['USER']),
  planningController.submitPlanningFeedback
);

// PATCH /planning/:eventId/unassign-manager - Unassign manager (Admin only)
router.patch(
  '/planning/:eventId/unassign-manager',
  isAdmin,
  planningController.unassignPlanningManager
);

// DELETE /planning/:eventId - Delete a planning
router.delete(
  '/planning/:eventId',
  authorizeRoles(['USER', 'VENDOR', 'ADMIN', 'MANAGER']),
  planningController.deletePlanning
);

module.exports = router;
