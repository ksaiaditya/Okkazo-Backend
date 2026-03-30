const express = require('express');
const router = express.Router();
const vendorController = require('../controllers/vendorController');
const { extractUser } = require('../middleware/extractUser');
const { authorizeRoles } = require('../middleware/authorization');
const { upload } = require('../middleware/upload');
const {
  validateCreateVendorService,
  validateUpdateVendorService,
  validateServiceIdParam,
} = require('../middleware/vendorValidation');

// Public routes
router.get('/health', vendorController.healthCheck);

// Public service search (no auth required)
router.get('/api/vendor/services/search', vendorController.searchVendorServices);

// Public vendor profiles (sanitized, no auth required)
router.get('/api/vendor/public/vendors', vendorController.getPublicVendors);

// Public vendor profile search (sanitized, no auth required)
router.get('/api/vendor/public/vendors/search', vendorController.searchPublicVendors);

// Public service lookup (sanitized, no auth required)
router.get('/api/vendor/public/services/:serviceId', vendorController.getPublicServiceById);

// Protected routes - user context extracted from API Gateway headers
router.use('/api/vendor', extractUser);

// Get my application - accessible by the vendor
router.get('/api/vendor/me/application', vendorController.getMyApplication);

// Update my business profile fields (description/location)
router.patch('/api/vendor/me/application/profile', vendorController.updateMyApplicationProfile);

// Upload/replace profile or banner image
router.post(
  '/api/vendor/me/application/images/:imageType',
  upload.single('file'),
  vendorController.uploadMyApplicationImage
);

// Get application status - accessible by the applicant
router.get('/api/vendor/registration/status/:applicationId', vendorController.getApplicationStatus);

// Vendor: save a new service
router.post(
  '/api/vendor/services',
  validateCreateVendorService,
  vendorController.createVendorService
);

// Vendor: get own services
router.get('/api/vendor/services/me', vendorController.getMyServices);

// Admin/Manager: get services by vendor authId
router.get(
  '/api/vendor/services/vendor/:vendorAuthId',
  authorizeRoles(['ADMIN', 'MANAGER']),
  vendorController.getVendorServicesByAuthId
);

// Vendor: update a service
router.patch(
  '/api/vendor/services/:serviceId',
  validateServiceIdParam,
  validateUpdateVendorService,
  vendorController.updateVendorService
);

// Vendor: upload venue service images (Venue only)
router.post(
  '/api/vendor/services/:serviceId/images',
  validateServiceIdParam,
  upload.array('files', 10),
  vendorController.uploadVenueServiceImages
);

// Vendor: delete a specific venue service image (Venue only)
router.delete(
  '/api/vendor/services/:serviceId/images',
  validateServiceIdParam,
  vendorController.deleteVenueServiceImage
);

// Vendor: set an existing venue image as the profile image (moves to first)
router.patch(
  '/api/vendor/services/:serviceId/images/profile',
  validateServiceIdParam,
  vendorController.setVenueServiceProfileImage
);

// Vendor: delete a service
router.delete(
  '/api/vendor/services/:serviceId',
  validateServiceIdParam,
  vendorController.deleteVendorService
);

// Upload additional documents
router.post(
  '/api/vendor/registration/:applicationId/documents',
  upload.single('file'),
  vendorController.uploadDocument
);

// Admin routes - get all applications
router.get(
  '/api/vendor/applications',
  authorizeRoles(['ADMIN', 'MANAGER']),
  vendorController.getAllApplications
);

// Admin routes - approve application
router.patch(
  '/api/vendor/applications/:applicationId/approve',
  authorizeRoles(['ADMIN', 'MANAGER']),
  vendorController.approveApplication
);

// Admin routes - reject application
router.patch(
  '/api/vendor/applications/:applicationId/reject',
  authorizeRoles(['ADMIN', 'MANAGER']),
  vendorController.rejectApplication
);

// Admin routes - request additional documents
router.patch(
  '/api/vendor/applications/:applicationId/request-documents',
  authorizeRoles(['ADMIN', 'MANAGER']),
  vendorController.requestDocuments
);

// Admin routes - verify a document
router.patch(
  '/api/vendor/applications/:applicationId/documents/verify',
  authorizeRoles(['ADMIN', 'MANAGER']),
  vendorController.verifyDocument
);

// Admin routes - reject a document
router.patch(
  '/api/vendor/applications/:applicationId/documents/reject',
  authorizeRoles(['ADMIN', 'MANAGER']),
  vendorController.rejectDocument
);

module.exports = router;
