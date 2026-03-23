const express = require('express');
const vendorSelectionController = require('../controllers/vendorSelectionController');
const { authorizeRoles } = require('../middleware/authorization');

const router = express.Router();

// Vendor-facing request workflow
// GET /vendor/requests - List vendor event requests
router.get(
  '/vendor/requests',
  authorizeRoles(['VENDOR']),
  vendorSelectionController.listVendorRequests
);

// GET /vendor/requests/:eventId - Get vendor event request details
router.get(
  '/vendor/requests/:eventId',
  authorizeRoles(['VENDOR']),
  vendorSelectionController.getVendorRequestDetails
);

// POST /vendor/requests/:eventId/accept - Accept request (optionally by service)
router.post(
  '/vendor/requests/:eventId/accept',
  authorizeRoles(['VENDOR']),
  vendorSelectionController.acceptVendorRequest
);

// POST /vendor/requests/:eventId/reject - Reject request (optionally by service)
router.post(
  '/vendor/requests/:eventId/reject',
  authorizeRoles(['VENDOR']),
  vendorSelectionController.rejectVendorRequest
);

// GET /vendor-selection/:eventId - Get or create vendor selection for planning
router.get(
  '/vendor-selection/:eventId',
  authorizeRoles(['USER', 'VENDOR', 'ADMIN', 'MANAGER']),
  vendorSelectionController.getOrCreateForPlanning
);

// GET /vendor-selection/:eventId/alternatives?service=... - List available alternative vendors for a service
router.get(
  '/vendor-selection/:eventId/alternatives',
  authorizeRoles(['USER', 'ADMIN', 'MANAGER']),
  vendorSelectionController.listAlternativesForService
);

// PATCH /vendor-selection/:eventId/services - Update selected services
router.patch(
  '/vendor-selection/:eventId/services',
  authorizeRoles(['USER', 'VENDOR', 'ADMIN', 'MANAGER']),
  vendorSelectionController.updateSelectedServices
);

// PATCH /vendor-selection/:eventId/vendors - Upsert vendor selection info for a service
router.patch(
  '/vendor-selection/:eventId/vendors',
  authorizeRoles(['USER', 'VENDOR', 'ADMIN', 'MANAGER']),
  vendorSelectionController.upsertVendor
);

module.exports = router;
