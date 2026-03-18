const express = require('express');
const promoteController = require('../controllers/promoteController');
const { authorizeRoles, isAdminOrManager, isAdmin } = require('../middleware/authorization');
const { validateCreatePromote } = require('../middleware/promoteValidation');
const { promoteUpload } = require('../middleware/upload');

const router = express.Router();

// POST /promote — Create a new promote record (multipart/form-data)
router.post(
  '/promote',
  authorizeRoles(['USER', 'VENDOR', 'ADMIN', 'MANAGER']),
  promoteUpload,
  validateCreatePromote,
  promoteController.createPromote
);

// GET /promote/me — Get current user's own promote records
router.get(
  '/promote/me',
  authorizeRoles(['USER', 'VENDOR', 'ADMIN', 'MANAGER']),
  promoteController.getMyPromotes
);

// GET /promote/platform-fee — Get current platform fee (all authenticated users)
router.get(
  '/promote/platform-fee',
  authorizeRoles(['USER', 'VENDOR', 'ADMIN', 'MANAGER']),
  promoteController.getPlatformFee
);

// PATCH /promote/platform-fee — Update platform fee (Admin only)
router.patch(
  '/promote/platform-fee',
  isAdmin,
  promoteController.updatePlatformFee
);

// GET /promote — Get all promotes (Admin/Manager only)
router.get(
  '/promote',
  isAdminOrManager,
  promoteController.getAllPromotes
);

// GET /promote/:eventId — Get a single promote record
router.get(
  '/promote/:eventId',
  authorizeRoles(['USER', 'VENDOR', 'ADMIN', 'MANAGER']),
  promoteController.getPromoteByEventId
);

// PATCH /promote/:eventId/status — Update event status (Manager/Admin)
router.patch(
  '/promote/:eventId/status',
  isAdminOrManager,
  promoteController.updatePromoteStatus
);

// PATCH /promote/:eventId/assign — Assign a manager (Admin only)
router.patch(
  '/promote/:eventId/assign',
  isAdmin,
  promoteController.assignManager
);

// DELETE /promote/:eventId — Delete a promote record (Owner or Admin)
router.delete(
  '/promote/:eventId',
  authorizeRoles(['USER', 'VENDOR', 'ADMIN', 'MANAGER']),
  promoteController.deletePromote
);

module.exports = router;
