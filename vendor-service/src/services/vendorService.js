const VendorApplication = require('../models/VendorApplication');
const ApiError = require('../utils/ApiError');
const logger = require('../utils/logger');
const fileUploadService = require('./fileUploadService');

/**
 * Create vendor application
 */
const createVendorApplication = async (applicationData) => {
  try {
    // Check if application already exists
    const existingApp = await VendorApplication.findOne({
      applicationId: applicationData.applicationId,
    });

    if (existingApp) {
      logger.warn('Application already exists', {
        applicationId: applicationData.applicationId,
      });
      return existingApp;
    }

    // Check for duplicate email
    const existingEmail = await VendorApplication.findOne({
      email: applicationData.email,
      status: { $nin: ['REJECTED'] }, // Allow reapplication after rejection
    });

    if (existingEmail) {
      throw new ApiError(409, 'An application with this email already exists');
    }

    // Check for duplicate business name
    const existingBusiness = await VendorApplication.findOne({
      businessName: applicationData.businessName,
      status: { $nin: ['REJECTED'] },
    });

    if (existingBusiness) {
      throw new ApiError(409, 'An application with this business name already exists');
    }

    // Create new application
    const application = new VendorApplication(applicationData);
    await application.save();

    logger.info('Vendor application created', {
      applicationId: application.applicationId,
      businessName: application.businessName,
    });

    return application;
  } catch (error) {
    logger.error('Error creating vendor application:', error);
    throw error;
  }
};

/**
 * Get application by ID
 */
const getApplicationById = async (applicationId) => {
  try {
    const application = await VendorApplication.findOne({ applicationId });

    if (!application) {
      throw new ApiError(404, 'Application not found');
    }

    return application;
  } catch (error) {
    logger.error('Error getting application:', error);
    throw error;
  }
};

/**
 * Get application status
 */
const getApplicationStatus = async (applicationId) => {
  try {
    const application = await getApplicationById(applicationId);

    const response = {
      applicationId: application.applicationId,
      status: application.status,
      businessName: application.businessName,
      submittedAt: application.submittedAt,
      lastUpdatedAt: application.lastUpdatedAt,
      reviewNotes: application.reviewNotes,
      documents: {
        businessLicense: application.documents.businessLicense
          ? {
              status: application.documents.businessLicense.status,
              fileName: application.documents.businessLicense.fileName,
            }
          : null,
        ownerIdentity: application.documents.ownerIdentity
          ? {
              status: application.documents.ownerIdentity.status,
              fileName: application.documents.ownerIdentity.fileName,
            }
          : null,
        otherProofs: application.documents.otherProofs.map((doc) => ({
          status: doc.status,
          fileName: doc.fileName,
        })),
      },
    };

    return response;
  } catch (error) {
    logger.error('Error getting application status:', error);
    throw error;
  }
};

/**
 * Get my application by authId
 */
const getMyApplication = async (authId) => {
  try {
    const application = await VendorApplication.findOne({ authId });

    if (!application) {
      throw new ApiError(404, 'No vendor application found for your account');
    }

    return {
      applicationId: application.applicationId,
      authId: application.authId,
      businessName: application.businessName,
      serviceCategory: application.serviceCategory,
      images: {
        profile: application.images?.profile || null,
        banner: application.images?.banner || null,
      },
      email: application.email,
      phone: application.phone,
      location: application.location,
      place: application.place,
      country: application.country,
      latitude: application.latitude,
      longitude: application.longitude,
      description: application.description,
      status: application.status,
      submittedAt: application.submittedAt,
      lastUpdatedAt: application.lastUpdatedAt,
      reviewNotes: application.reviewNotes,
      reviewedAt: application.reviewedAt,
      approvedAt: application.approvedAt,
      rejectedAt: application.rejectedAt,
      documents: {
        businessLicense: application.documents.businessLicense
          ? {
              documentId: application.documents.businessLicense.documentId,
              fileName: application.documents.businessLicense.fileName,
              fileUrl: application.documents.businessLicense.fileUrl,
              status: application.documents.businessLicense.status,
              uploadedAt: application.documents.businessLicense.uploadedAt,
              rejectionReason: application.documents.businessLicense.rejectionReason,
            }
          : null,
        ownerIdentity: application.documents.ownerIdentity
          ? {
              documentId: application.documents.ownerIdentity.documentId,
              fileName: application.documents.ownerIdentity.fileName,
              fileUrl: application.documents.ownerIdentity.fileUrl,
              status: application.documents.ownerIdentity.status,
              uploadedAt: application.documents.ownerIdentity.uploadedAt,
              rejectionReason: application.documents.ownerIdentity.rejectionReason,
            }
          : null,
        otherProofs: application.documents.otherProofs.map((doc) => ({
          documentId: doc.documentId,
          fileName: doc.fileName,
          fileUrl: doc.fileUrl,
          status: doc.status,
          uploadedAt: doc.uploadedAt,
          description: doc.description,
          rejectionReason: doc.rejectionReason,
        })),
      },
    };
  } catch (error) {
    logger.error('Error getting my application:', error);
    throw error;
  }
};

/**
 * Upload/replace a vendor's profile or banner image (Cloudinary)
 */
const uploadMyApplicationImage = async (authId, imageType, file) => {
  try {
    if (!authId) {
      throw new ApiError(401, 'User not authenticated');
    }

    if (!['profile', 'banner'].includes(imageType)) {
      throw new ApiError(400, 'Invalid image type');
    }

    const application = await VendorApplication.findOne({ authId });
    if (!application) {
      throw new ApiError(404, 'No vendor application found for your account');
    }

    // Upload to Cloudinary
    const uploadResult = await fileUploadService.uploadFile(
      file,
      `${application.applicationId}/images/${imageType}`
    );

    // Clean up old image if present
    const previousPublicId = application.images?.[imageType]?.publicId;
    if (previousPublicId) {
      try {
        await fileUploadService.deleteFile(previousPublicId);
      } catch (cleanupError) {
        logger.warn('Failed to delete previous Cloudinary image', {
          applicationId: application.applicationId,
          imageType,
          publicId: previousPublicId,
          error: cleanupError?.message,
        });
      }
    }

    if (!application.images) {
      application.images = { profile: null, banner: null };
    }

    application.images[imageType] = {
      fileUrl: uploadResult.url,
      publicId: uploadResult.publicId,
      uploadedAt: new Date(),
    };

    application.markModified('images');
    await application.save();

    return {
      images: {
        profile: application.images?.profile || null,
        banner: application.images?.banner || null,
      },
    };
  } catch (error) {
    logger.error('Error uploading application image:', error);
    throw error;
  }
};

/**
 * Update vendor profile fields for the currently authenticated vendor.
 * Supports description and location fields used by Business Profile.
 */
const updateMyApplicationProfile = async (authId, updates = {}) => {
  try {
    if (!authId) {
      throw new ApiError(401, 'User not authenticated');
    }

    const application = await VendorApplication.findOne({ authId });
    if (!application) {
      throw new ApiError(404, 'No vendor application found for your account');
    }

    const nextDescription =
      updates.description != null ? String(updates.description).trim() : undefined;

    const nextLocation =
      updates.location != null ? String(updates.location).trim() : undefined;

    const nextPlace =
      updates.place != null ? String(updates.place).trim() : undefined;

    const nextCountry =
      updates.country != null ? String(updates.country).trim() : undefined;

    const hasLatitude = updates.latitude != null && String(updates.latitude).trim() !== '';
    const hasLongitude = updates.longitude != null && String(updates.longitude).trim() !== '';

    if (hasLatitude !== hasLongitude) {
      throw new ApiError(400, 'latitude and longitude must be provided together');
    }

    if (nextDescription !== undefined) {
      if (nextDescription.length > 2000) {
        throw new ApiError(400, 'Description cannot exceed 2000 characters');
      }
      application.description = nextDescription || null;
    }

    if (nextLocation !== undefined) {
      if (!nextLocation) {
        throw new ApiError(400, 'location cannot be empty');
      }
      if (nextLocation.length > 500) {
        throw new ApiError(400, 'Location cannot exceed 500 characters');
      }
      application.location = nextLocation;
    }

    if (nextPlace !== undefined) {
      if (nextPlace.length > 200) {
        throw new ApiError(400, 'Place cannot exceed 200 characters');
      }
      application.place = nextPlace || null;
    }

    if (nextCountry !== undefined) {
      if (nextCountry.length > 200) {
        throw new ApiError(400, 'Country cannot exceed 200 characters');
      }
      application.country = nextCountry || null;
    }

    if (hasLatitude && hasLongitude) {
      const lat = Number(updates.latitude);
      const lng = Number(updates.longitude);

      if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
        throw new ApiError(400, 'latitude and longitude must be valid numbers');
      }

      if (Math.abs(lat) > 90 || Math.abs(lng) > 180) {
        throw new ApiError(400, 'latitude or longitude out of range');
      }

      application.latitude = lat;
      application.longitude = lng;
    }

    application.lastUpdatedAt = new Date();
    await application.save();

    return getMyApplication(authId);
  } catch (error) {
    logger.error('Error updating my application profile:', error);
    throw error;
  }
};

/**
 * Upload additional document
 */
const uploadDocument = async (applicationId, documentData) => {
  try {
    const application = await getApplicationById(applicationId);

    // Check if application is in a state that allows document upload
    if (application.status === 'APPROVED') {
      throw new ApiError(400, 'Cannot upload documents for approved application');
    }

    if (application.status === 'SUSPENDED') {
      throw new ApiError(400, 'Cannot upload documents for suspended application. Please contact support.');
    }

    const { documentId, documentType, fileName, fileUrl, description } = documentData;

    const newDocument = {
      documentId,
      documentType,
      fileName,
      fileUrl,
      status: 'PENDING_VERIFICATION',
      uploadedAt: new Date(),
      description,
    };

    // Add document based on type
    if (documentType === 'businessLicense') {
      application.documents.businessLicense = newDocument;
    } else if (documentType === 'ownerIdentity') {
      application.documents.ownerIdentity = newDocument;
    } else if (documentType === 'otherProof') {
      if (application.documents.otherProofs.length >= 3) {
        throw new ApiError(400, 'Maximum 3 other proofs allowed');
      }
      application.documents.otherProofs.push(newDocument);
    } else {
      throw new ApiError(400, 'Invalid document type');
    }

    // Update application status if it was DOCUMENTS_REQUESTED or REJECTED
    if (application.status === 'DOCUMENTS_REQUESTED' || application.status === 'REJECTED') {
      application.status = 'UNDER_VERIFICATION';
    }

    await application.save();

    logger.info('Document uploaded successfully', {
      applicationId,
      documentId,
      documentType,
    });

    return {
      documentId,
      status: 'PENDING_VERIFICATION',
      uploadedAt: newDocument.uploadedAt,
    };
  } catch (error) {
    logger.error('Error uploading document:', error);
    throw error;
  }
};

/**
 * Get all applications (for admin)
 */
const getAllApplications = async (filters = {}) => {
  try {
    const { status, limit = 50, skip = 0 } = filters;

    const query = {};
    if (status) {
      query.status = status;
    }

    const applications = await VendorApplication.find(query)
      .sort({ submittedAt: -1 })
      .limit(parseInt(limit))
      .skip(parseInt(skip));

    const total = await VendorApplication.countDocuments(query);

    return {
      applications,
      total,
      limit: parseInt(limit),
      skip: parseInt(skip),
    };
  } catch (error) {
    logger.error('Error getting all applications:', error);
    throw error;
  }
};

/**
 * Approve vendor application
 */
const approveApplication = async (applicationId, reviewedBy = 'ADMIN') => {
  try {
    const application = await VendorApplication.findOne({ applicationId });

    if (!application) {
      throw new ApiError(404, 'Application not found');
    }

    if (application.status === 'APPROVED') {
      throw new ApiError(400, 'Application is already approved');
    }

    // Update application status
    application.status = 'APPROVED';
    application.approvedAt = new Date();
    application.reviewedAt = new Date();
    application.reviewedBy = reviewedBy;
    application.lastUpdatedAt = new Date();

    // Mark all documents as verified
    if (application.documents.businessLicense) {
      application.documents.businessLicense.status = 'VERIFIED';
      application.documents.businessLicense.verifiedAt = new Date();
    }
    if (application.documents.ownerIdentity) {
      application.documents.ownerIdentity.status = 'VERIFIED';
      application.documents.ownerIdentity.verifiedAt = new Date();
    }
    application.documents.otherProofs.forEach(doc => {
      doc.status = 'VERIFIED';
      doc.verifiedAt = new Date();
    });

    await application.save();

    logger.info('Vendor application approved', {
      applicationId: application.applicationId,
      businessName: application.businessName,
      reviewedBy,
    });

    return application;
  } catch (error) {
    logger.error('Error approving application:', error);
    throw error;
  }
};

/**
 * Reject vendor application
 */
const rejectApplication = async (applicationId, rejectionReason, reviewedBy = 'ADMIN') => {
  try {
    if (!rejectionReason || !rejectionReason.trim()) {
      throw new ApiError(400, 'Rejection reason is required');
    }

    const application = await VendorApplication.findOne({ applicationId });

    if (!application) {
      throw new ApiError(404, 'Application not found');
    }

    if (application.status === 'REJECTED') {
      throw new ApiError(400, 'Application is already rejected');
    }

    if (application.status === 'APPROVED') {
      throw new ApiError(400, 'Cannot reject an approved application');
    }

    // Update application status
    application.status = 'REJECTED';
    application.rejectedAt = new Date();
    application.reviewedAt = new Date();
    application.reviewedBy = reviewedBy;
    application.reviewNotes = rejectionReason;
    application.lastUpdatedAt = new Date();

    await application.save();

    logger.info('Vendor application rejected', {
      applicationId: application.applicationId,
      businessName: application.businessName,
      reason: rejectionReason,
      reviewedBy,
    });

    return application;
  } catch (error) {
    logger.error('Error rejecting application:', error);
    throw error;
  }
};

/**
 * Request additional documents from vendor
 */
const requestDocuments = async (applicationId, requestedDocuments, reviewedBy = 'ADMIN') => {
  try {
    if (!requestedDocuments || !requestedDocuments.trim()) {
      throw new ApiError(400, 'Document request details are required');
    }

    const application = await VendorApplication.findOne({ applicationId });

    if (!application) {
      throw new ApiError(404, 'Application not found');
    }

    if (application.status === 'APPROVED') {
      throw new ApiError(400, 'Cannot request documents from an approved application');
    }

    if (application.status === 'REJECTED') {
      throw new ApiError(400, 'Cannot request documents from a rejected application');
    }

    // Update application status
    application.status = 'DOCUMENTS_REQUESTED';
    application.reviewNotes = requestedDocuments;
    application.reviewedBy = reviewedBy;
    application.lastUpdatedAt = new Date();

    await application.save();

    logger.info('Documents requested from vendor', {
      applicationId: application.applicationId,
      businessName: application.businessName,
      requestedDocuments,
      reviewedBy,
    });

    return application;
  } catch (error) {
    logger.error('Error requesting documents:', error);
    throw error;
  }
};

/**
 * Verify a specific document in an application
 */
const verifyDocument = async (applicationId, documentType, documentId, reviewedBy) => {
  try {
    const application = await VendorApplication.findOne({ applicationId });

    if (!application) {
      throw new ApiError(404, 'Application not found');
    }

    const validTypes = ['businessLicense', 'ownerIdentity'];
    
    if (validTypes.includes(documentType)) {
      const doc = application.documents[documentType];
      if (!doc || !doc.fileUrl) {
        throw new ApiError(404, 'Document not found');
      }
      application.documents[documentType].status = 'VERIFIED';
      application.documents[documentType].verifiedAt = new Date();
      application.documents[documentType].rejectionReason = null;
    } else if (documentType === 'otherProof' && documentId) {
      const proofIndex = application.documents.otherProofs.findIndex(p => p.documentId === documentId);
      if (proofIndex === -1) {
        throw new ApiError(404, 'Document not found');
      }
      application.documents.otherProofs[proofIndex].status = 'VERIFIED';
      application.documents.otherProofs[proofIndex].verifiedAt = new Date();
      application.documents.otherProofs[proofIndex].rejectionReason = null;
    } else {
      throw new ApiError(400, 'Invalid document type');
    }

    application.reviewedBy = reviewedBy;
    application.lastUpdatedAt = new Date();
    application.markModified('documents');
    await application.save();

    logger.info('Document verified', { applicationId, documentType, reviewedBy });

    return application;
  } catch (error) {
    logger.error('Error verifying document:', error);
    throw error;
  }
};

/**
 * Reject a specific document in an application
 */
const rejectDocument = async (applicationId, documentType, documentId, rejectionReason, reviewedBy) => {
  try {
    const application = await VendorApplication.findOne({ applicationId });

    if (!application) {
      throw new ApiError(404, 'Application not found');
    }

    const validTypes = ['businessLicense', 'ownerIdentity'];
    
    if (validTypes.includes(documentType)) {
      const doc = application.documents[documentType];
      if (!doc || !doc.fileUrl) {
        throw new ApiError(404, 'Document not found');
      }
      application.documents[documentType].status = 'REJECTED';
      application.documents[documentType].rejectionReason = rejectionReason;
      application.documents[documentType].verifiedAt = null;
    } else if (documentType === 'otherProof' && documentId) {
      const proofIndex = application.documents.otherProofs.findIndex(p => p.documentId === documentId);
      if (proofIndex === -1) {
        throw new ApiError(404, 'Document not found');
      }
      application.documents.otherProofs[proofIndex].status = 'REJECTED';
      application.documents.otherProofs[proofIndex].rejectionReason = rejectionReason;
      application.documents.otherProofs[proofIndex].verifiedAt = null;
    } else {
      throw new ApiError(400, 'Invalid document type');
    }

    // Set application status to DOCUMENTS_REQUESTED so vendor can re-upload
    application.status = 'DOCUMENTS_REQUESTED';
    application.reviewedBy = reviewedBy;
    application.lastUpdatedAt = new Date();
    application.markModified('documents');
    await application.save();

    logger.info('Document rejected', { applicationId, documentType, rejectionReason, reviewedBy });

    return application;
  } catch (error) {
    logger.error('Error rejecting document:', error);
    throw error;
  }
};

module.exports = {
  createVendorApplication,
  getApplicationById,
  getApplicationStatus,
  getMyApplication,
  uploadMyApplicationImage,
  updateMyApplicationProfile,
  uploadDocument,
  getAllApplications,
  approveApplication,
  rejectApplication,
  requestDocuments,
  verifyDocument,
  rejectDocument,
};
