const express = require('express');
const router = express.Router();

const availabilityController = require('../controllers/availabilityController');
const { extractUser } = require('../middleware/extractUser');
const { authorizeRoles } = require('../middleware/authorization');

// Public: available vendor services for a given day/range
router.get('/api/vendor/services/available', availabilityController.searchAvailableVendorServices);

// Protected routes - user context extracted from API Gateway headers
router.use('/api/vendor', extractUser);

// Vendor self-service calendar availability
router.get(
  '/api/vendor/me/availability',
  authorizeRoles(['VENDOR']),
  availabilityController.getMyAvailability
);

router.post(
  '/api/vendor/me/availability/unavailable',
  authorizeRoles(['VENDOR']),
  availabilityController.addMyManualUnavailability
);

router.delete(
  '/api/vendor/me/availability/unavailable',
  authorizeRoles(['VENDOR']),
  availabilityController.removeMyManualUnavailability
);

// Admin/Manager: view vendor availability
router.get(
  '/api/vendor/availability/:vendorAuthId',
  authorizeRoles(['ADMIN', 'MANAGER']),
  availabilityController.getVendorAvailability
);

// Admin/Manager: block/unblock vendor due to booking/event
router.post(
  '/api/vendor/availability/bookings/block',
  authorizeRoles(['ADMIN', 'MANAGER']),
  availabilityController.blockVendorForEvent
);

router.post(
  '/api/vendor/availability/bookings/unblock',
  authorizeRoles(['ADMIN', 'MANAGER']),
  availabilityController.unblockVendorForEvent
);

module.exports = router;
