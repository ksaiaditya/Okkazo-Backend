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
