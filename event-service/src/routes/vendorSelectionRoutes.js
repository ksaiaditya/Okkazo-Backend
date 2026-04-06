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

// GET /vendor/requests/ledger - Vendor payout ledger across events
router.get(
  '/vendor/requests/ledger',
  authorizeRoles(['VENDOR']),
  vendorSelectionController.listVendorPayoutLedger
);

// GET /vendor/requests/:eventId - Get vendor event request details
router.get(
  '/vendor/requests/:eventId',
  authorizeRoles(['VENDOR']),
  vendorSelectionController.getVendorRequestDetails
);

// POST /vendor/requests/:eventId/lock-price - Lock service price with commission
router.post(
  '/vendor/requests/:eventId/lock-price',
  authorizeRoles(['VENDOR']),
  vendorSelectionController.lockVendorServicePrice
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

// POST /vendor-selection/:eventId/change-request - Submit service change request for managed approval flow
router.post(
  '/vendor-selection/:eventId/change-request',
  authorizeRoles(['USER', 'ADMIN', 'MANAGER']),
  vendorSelectionController.createServiceChangeRequest
);

// PATCH /vendor-selection/:eventId/change-request/:requestId/manager-decision - Manager/Admin decision
router.patch(
  '/vendor-selection/:eventId/change-request/:requestId/manager-decision',
  authorizeRoles(['ADMIN', 'MANAGER']),
  vendorSelectionController.decideServiceChangeRequestByManager
);

// PATCH /vendor-selection/:eventId/change-request/:requestId/vendor-consent - Vendor consent/rejection
router.patch(
  '/vendor-selection/:eventId/change-request/:requestId/vendor-consent',
  authorizeRoles(['VENDOR']),
  vendorSelectionController.submitVendorConsentForServiceChangeRequest
);

// POST /vendor-selection/:eventId/unlock - Release temporary reservation locks
router.post(
  '/vendor-selection/:eventId/unlock',
  authorizeRoles(['USER', 'ADMIN', 'MANAGER']),
  vendorSelectionController.unlockReservations
);

module.exports = router;
