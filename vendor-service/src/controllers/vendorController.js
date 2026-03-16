const vendorService = require('../services/vendorService');
const vendorServiceCatalog = require('../services/vendorServiceCatalog');
const fileUploadService = require('../services/fileUploadService');
const logger = require('../utils/logger');
const { formatSuccessResponse, formatErrorResponse, generateDocumentId } = require('../utils/helpers');

/**
 * Health check
 * GET /health
 */
const healthCheck = async (req, res) => {
  res.status(200).json({
    success: true,
    message: 'Vendor service is running',
    timestamp: new Date().toISOString(),
  });
};

/**
 * Get application status
 * GET /api/vendor/registration/status/:applicationId
 */
const getApplicationStatus = async (req, res) => {
  try {
    const { applicationId } = req.params;

    if (!applicationId || applicationId.trim() === '') {
      return res.status(400).json(
        formatErrorResponse('VALIDATION_ERROR', 'Application ID is required')
      );
    }

    const status = await vendorService.getApplicationStatus(applicationId);

    res.status(200).json(formatSuccessResponse(status));
  } catch (error) {
    logger.error('Error in getApplicationStatus:', error);
    
    if (error.statusCode === 404) {
      return res.status(404).json(
        formatErrorResponse('APPLICATION_NOT_FOUND', error.message)
      );
    }

    res.status(error.statusCode || 500).json(
      formatErrorResponse('INTERNAL_ERROR', error.message)
    );
  }
};

/**
 * Get my application
 * GET /api/vendor/me/application
 */
const getMyApplication = async (req, res) => {
  try {
    const authId = req.user.authId;

    if (!authId) {
      return res.status(401).json(
        formatErrorResponse('UNAUTHORIZED', 'User not authenticated')
      );
    }

    const application = await vendorService.getMyApplication(authId);

    res.status(200).json(formatSuccessResponse(application));
  } catch (error) {
    logger.error('Error in getMyApplication:', error);
    
    if (error.statusCode === 404) {
      return res.status(404).json(
        formatErrorResponse('APPLICATION_NOT_FOUND', error.message)
      );
    }

    res.status(error.statusCode || 500).json(
      formatErrorResponse('INTERNAL_ERROR', error.message)
    );
  }
};

/**
 * Upload additional document
 * POST /api/vendor/registration/:applicationId/documents
 */
const uploadDocument = async (req, res) => {
  try {
    const { applicationId } = req.params;
    const { documentType, description } = req.body;
    const file = req.file;

    // Validation
    if (!applicationId || applicationId.trim() === '') {
      return res.status(400).json(
        formatErrorResponse('VALIDATION_ERROR', 'Application ID is required')
      );
    }

    if (!documentType) {
      return res.status(400).json(
        formatErrorResponse('VALIDATION_ERROR', 'Document type is required', [
          { field: 'documentType', message: 'Document type is required' },
        ])
      );
    }

    if (!file) {
      return res.status(400).json(
        formatErrorResponse('VALIDATION_ERROR', 'File is required', [
          { field: 'file', message: 'File is required' },
        ])
      );
    }

    // Validate document type
    const validDocTypes = ['businessLicense', 'ownerIdentity', 'otherProof'];
    if (!validDocTypes.includes(documentType)) {
      return res.status(400).json(
        formatErrorResponse('VALIDATION_ERROR', 'Invalid document type', [
          {
            field: 'documentType',
            message: 'Document type must be one of: businessLicense, ownerIdentity, otherProof',
          },
        ])
      );
    }

    // Upload file to Cloudinary
    const uploadResult = await fileUploadService.uploadFile(
      file,
      `${applicationId}/${documentType}`
    );

    // Save document info to database
    const documentData = {
      documentId: generateDocumentId(),
      documentType,
      fileName: file.originalname,
      fileUrl: uploadResult.url,
      description: description || null,
    };

    const result = await vendorService.uploadDocument(applicationId, documentData);

    res.status(200).json(formatSuccessResponse(result));
  } catch (error) {
    logger.error('Error in uploadDocument:', error);

    if (error.statusCode === 404) {
      return res.status(404).json(
        formatErrorResponse('APPLICATION_NOT_FOUND', error.message)
      );
    }

    if (error.statusCode === 400) {
      return res.status(400).json(
        formatErrorResponse('VALIDATION_ERROR', error.message)
      );
    }

    if (error.message.includes('file type') || error.message.includes('file size')) {
      return res.status(400).json(
        formatErrorResponse('FILE_UPLOAD_ERROR', error.message)
      );
    }

    res.status(error.statusCode || 500).json(
      formatErrorResponse('INTERNAL_ERROR', error.message)
    );
  }
};

/**
 * Get all applications (Admin only)
 * GET /api/vendor/applications
 */
const getAllApplications = async (req, res) => {
  try {
    const { status, limit, skip } = req.query;

    const filters = {
      status,
      limit: limit || 50,
      skip: skip || 0,
    };

    const result = await vendorService.getAllApplications(filters);

    res.status(200).json(formatSuccessResponse(result));
  } catch (error) {
    logger.error('Error in getAllApplications:', error);
    res.status(error.statusCode || 500).json(
      formatErrorResponse('INTERNAL_ERROR', error.message)
    );
  }
};

/**
 * Approve vendor application (Admin only)
 * PATCH /api/vendor/applications/:applicationId/approve
 */
const approveApplication = async (req, res) => {
  try {
    const { applicationId } = req.params;
    const reviewedBy = req.user?.email || 'ADMIN';

    if (!applicationId || applicationId.trim() === '') {
      return res.status(400).json(
        formatErrorResponse('VALIDATION_ERROR', 'Application ID is required')
      );
    }

    const application = await vendorService.approveApplication(applicationId, reviewedBy);

    res.status(200).json(
      formatSuccessResponse({
        message: 'Application approved successfully',
        application: {
          applicationId: application.applicationId,
          businessName: application.businessName,
          status: application.status,
          approvedAt: application.approvedAt,
          reviewedBy: application.reviewedBy,
        },
      })
    );
  } catch (error) {
    logger.error('Error in approveApplication:', error);
    
    if (error.statusCode === 404) {
      return res.status(404).json(
        formatErrorResponse('APPLICATION_NOT_FOUND', error.message)
      );
    }
    
    if (error.statusCode === 400) {
      return res.status(400).json(
        formatErrorResponse('VALIDATION_ERROR', error.message)
      );
    }

    res.status(error.statusCode || 500).json(
      formatErrorResponse('INTERNAL_ERROR', error.message)
    );
  }
};

/**
 * Reject vendor application (Admin only)
 * PATCH /api/vendor/applications/:applicationId/reject
 */
const rejectApplication = async (req, res) => {
  try {
    const { applicationId } = req.params;
    const { rejectionReason } = req.body;
    const reviewedBy = req.user?.email || 'ADMIN';

    if (!applicationId || applicationId.trim() === '') {
      return res.status(400).json(
        formatErrorResponse('VALIDATION_ERROR', 'Application ID is required')
      );
    }

    if (!rejectionReason || !rejectionReason.trim()) {
      return res.status(400).json(
        formatErrorResponse('VALIDATION_ERROR', 'Rejection reason is required', [
          { field: 'rejectionReason', message: 'Rejection reason is required' },
        ])
      );
    }

    const application = await vendorService.rejectApplication(
      applicationId,
      rejectionReason,
      reviewedBy
    );

    res.status(200).json(
      formatSuccessResponse({
        message: 'Application rejected successfully',
        application: {
          applicationId: application.applicationId,
          businessName: application.businessName,
          status: application.status,
          rejectedAt: application.rejectedAt,
          reviewNotes: application.reviewNotes,
          reviewedBy: application.reviewedBy,
        },
      })
    );
  } catch (error) {
    logger.error('Error in rejectApplication:', error);
    
    if (error.statusCode === 404) {
      return res.status(404).json(
        formatErrorResponse('APPLICATION_NOT_FOUND', error.message)
      );
    }
    
    if (error.statusCode === 400) {
      return res.status(400).json(
        formatErrorResponse('VALIDATION_ERROR', error.message)
      );
    }

    res.status(error.statusCode || 500).json(
      formatErrorResponse('INTERNAL_ERROR', error.message)
    );
  }
};

/**
 * Request additional documents from vendor (Admin only)
 * PATCH /api/vendor/applications/:applicationId/request-documents
 */
const requestDocuments = async (req, res) => {
  try {
    const { applicationId } = req.params;
    const { requestedDocuments } = req.body;
    const reviewedBy = req.user?.email || 'ADMIN';

    if (!applicationId || applicationId.trim() === '') {
      return res.status(400).json(
        formatErrorResponse('VALIDATION_ERROR', 'Application ID is required')
      );
    }

    if (!requestedDocuments || !requestedDocuments.trim()) {
      return res.status(400).json(
        formatErrorResponse('VALIDATION_ERROR', 'Document request details are required', [
          { field: 'requestedDocuments', message: 'Document request details are required' },
        ])
      );
    }

    const application = await vendorService.requestDocuments(
      applicationId,
      requestedDocuments,
      reviewedBy
    );

    res.status(200).json(
      formatSuccessResponse({
        message: 'Document request sent successfully',
        application: {
          applicationId: application.applicationId,
          businessName: application.businessName,
          status: application.status,
          reviewNotes: application.reviewNotes,
          reviewedBy: application.reviewedBy,
        },
      })
    );
  } catch (error) {
    logger.error('Error in requestDocuments:', error);
    
    if (error.statusCode === 404) {
      return res.status(404).json(
        formatErrorResponse('APPLICATION_NOT_FOUND', error.message)
      );
    }
    
    if (error.statusCode === 400) {
      return res.status(400).json(
        formatErrorResponse('VALIDATION_ERROR', error.message)
      );
    }

    res.status(error.statusCode || 500).json(
      formatErrorResponse('INTERNAL_ERROR', error.message)
    );
  }
};

/**
 * Verify a specific document (Admin only)
 * PATCH /api/vendor/applications/:applicationId/documents/verify
 */
const verifyDocument = async (req, res) => {
  try {
    const { applicationId } = req.params;
    const { documentType, documentId } = req.body;
    const reviewedBy = req.user?.email || 'ADMIN';

    if (!applicationId || !documentType) {
      return res.status(400).json(
        formatErrorResponse('VALIDATION_ERROR', 'Application ID and document type are required')
      );
    }

    const application = await vendorService.verifyDocument(applicationId, documentType, documentId, reviewedBy);

    res.status(200).json(
      formatSuccessResponse({
        message: 'Document verified successfully',
        application: {
          applicationId: application.applicationId,
          businessName: application.businessName,
          status: application.status,
        },
      })
    );
  } catch (error) {
    logger.error('Error in verifyDocument:', error);

    if (error.statusCode === 404) {
      return res.status(404).json(
        formatErrorResponse('APPLICATION_NOT_FOUND', error.message)
      );
    }

    if (error.statusCode === 400) {
      return res.status(400).json(
        formatErrorResponse('VALIDATION_ERROR', error.message)
      );
    }

    res.status(error.statusCode || 500).json(
      formatErrorResponse('INTERNAL_ERROR', error.message)
    );
  }
};

/**
 * Reject a specific document (Admin only)
 * PATCH /api/vendor/applications/:applicationId/documents/reject
 */
const rejectDocument = async (req, res) => {
  try {
    const { applicationId } = req.params;
    const { documentType, documentId, rejectionReason } = req.body;
    const reviewedBy = req.user?.email || 'ADMIN';

    if (!applicationId || !documentType) {
      return res.status(400).json(
        formatErrorResponse('VALIDATION_ERROR', 'Application ID and document type are required')
      );
    }

    if (!rejectionReason || !rejectionReason.trim()) {
      return res.status(400).json(
        formatErrorResponse('VALIDATION_ERROR', 'Rejection reason is required')
      );
    }

    const application = await vendorService.rejectDocument(applicationId, documentType, documentId, rejectionReason, reviewedBy);

    res.status(200).json(
      formatSuccessResponse({
        message: 'Document rejected. Vendor will be asked to re-upload.',
        application: {
          applicationId: application.applicationId,
          businessName: application.businessName,
          status: application.status,
        },
      })
    );
  } catch (error) {
    logger.error('Error in rejectDocument:', error);

    if (error.statusCode === 404) {
      return res.status(404).json(
        formatErrorResponse('APPLICATION_NOT_FOUND', error.message)
      );
    }

    if (error.statusCode === 400) {
      return res.status(400).json(
        formatErrorResponse('VALIDATION_ERROR', error.message)
      );
    }

    res.status(error.statusCode || 500).json(
      formatErrorResponse('INTERNAL_ERROR', error.message)
    );
  }
};

/**
 * Get the calling vendor's own services
 * GET /api/vendor/services/me
 */
const getMyServices = async (req, res) => {
  try {
    const authId = req.user?.authId;

    if (!authId) {
      return res.status(401).json(
        formatErrorResponse('UNAUTHORIZED', 'User not authenticated')
      );
    }

    const result = await vendorServiceCatalog.getMyServices(authId, req.query);

    res.status(200).json(formatSuccessResponse(result));
  } catch (error) {
    logger.error('Error in getMyServices:', error);
    res.status(error.statusCode || 500).json(
      formatErrorResponse('INTERNAL_ERROR', error.message)
    );
  }
};

/**
 * Create / save a vendor service
 * POST /api/vendor/services
 */
const createVendorService = async (req, res) => {
  try {
    const authId = req.user?.authId;

    if (!authId) {
      return res.status(401).json(
        formatErrorResponse('UNAUTHORIZED', 'User not authenticated')
      );
    }

    const service = await vendorServiceCatalog.createService(authId, req.body);

    res.status(201).json(
      formatSuccessResponse(service, 'Service created successfully')
    );
  } catch (error) {
    logger.error('Error in createVendorService:', error);
    res.status(error.statusCode || 500).json(
      formatErrorResponse(error.statusCode === 400 ? 'VALIDATION_ERROR' : 'INTERNAL_ERROR', error.message)
    );
  }
};

/**
 * Search vendor services (public)
 * GET /api/vendor/services/search
 */
const searchVendorServices = async (req, res) => {
  try {
    const result = await vendorServiceCatalog.searchServices(req.query);

    res.status(200).json(formatSuccessResponse(result));
  } catch (error) {
    logger.error('Error in searchVendorServices:', error);
    res.status(error.statusCode || 500).json(
      formatErrorResponse(error.statusCode === 400 ? 'VALIDATION_ERROR' : 'INTERNAL_ERROR', error.message)
    );
  }
};

/**
 * Update a vendor service (owned by calling vendor)
 * PATCH /api/vendor/services/:serviceId
 */
const updateVendorService = async (req, res) => {
  try {
    const authId = req.user?.authId;
    const { serviceId } = req.params;

    if (!authId) {
      return res.status(401).json(
        formatErrorResponse('UNAUTHORIZED', 'User not authenticated')
      );
    }

    if (!serviceId || serviceId.trim() === '') {
      return res.status(400).json(
        formatErrorResponse('VALIDATION_ERROR', 'Service ID is required')
      );
    }

    const updated = await vendorServiceCatalog.updateService(authId, serviceId, req.body);

    res.status(200).json(
      formatSuccessResponse(updated, 'Service updated successfully')
    );
  } catch (error) {
    logger.error('Error in updateVendorService:', error);

    if (error.statusCode === 404) {
      return res.status(404).json(
        formatErrorResponse('SERVICE_NOT_FOUND', error.message)
      );
    }

    res.status(error.statusCode || 500).json(
      formatErrorResponse(error.statusCode === 400 ? 'VALIDATION_ERROR' : 'INTERNAL_ERROR', error.message)
    );
  }
};

/**
 * Delete a vendor service (owned by calling vendor)
 * DELETE /api/vendor/services/:serviceId
 */
const deleteVendorService = async (req, res) => {
  try {
    const authId = req.user?.authId;
    const { serviceId } = req.params;

    if (!authId) {
      return res.status(401).json(
        formatErrorResponse('UNAUTHORIZED', 'User not authenticated')
      );
    }

    if (!serviceId || serviceId.trim() === '') {
      return res.status(400).json(
        formatErrorResponse('VALIDATION_ERROR', 'Service ID is required')
      );
    }

    const result = await vendorServiceCatalog.deleteService(authId, serviceId);

    res.status(200).json(
      formatSuccessResponse(result, 'Service deleted successfully')
    );
  } catch (error) {
    logger.error('Error in deleteVendorService:', error);

    if (error.statusCode === 404) {
      return res.status(404).json(
        formatErrorResponse('SERVICE_NOT_FOUND', error.message)
      );
    }

    res.status(error.statusCode || 500).json(
      formatErrorResponse(error.statusCode === 400 ? 'VALIDATION_ERROR' : 'INTERNAL_ERROR', error.message)
    );
  }
};

module.exports = {
  healthCheck,
  getApplicationStatus,
  getMyApplication,
  uploadDocument,
  getAllApplications,
  approveApplication,
  rejectApplication,
  requestDocuments,
  verifyDocument,
  rejectDocument,
  createVendorService,
  searchVendorServices,
  getMyServices,
  updateVendorService,
  deleteVendorService,
};
