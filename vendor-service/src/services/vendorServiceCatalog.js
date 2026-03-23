const mongoose = require('mongoose');
const VendorApplication = require('../models/VendorApplication');
const VendorService = require('../models/VendorService');
const ApiError = require('../utils/ApiError');
const fileUploadService = require('./fileUploadService');
const logger = require('../utils/logger');

const parseAuthIds = (value) => {
  if (value == null) return [];

  // Support both comma-separated string and repeated query keys.
  const raw = Array.isArray(value) ? value.join(',') : String(value);

  return raw
    .split(',')
    .map((v) => String(v).trim())
    .filter(Boolean);
};

/**
 * Create a new service listing for the calling vendor.
 *
 * Business rules:
 *  - Vendor must have an APPROVED application.
 *  - businessName, serviceCategory, latitude, longitude are pulled from
 *    the application so the vendor cannot misrepresent themselves.
 *
 * @param {string} authId   - vendor's auth ID (from API Gateway header)
 * @param {object} payload  - { categoryId, name, price, tier, description, details }
 * @returns {object}        - saved VendorService document
 */
const createService = async (authId, payload) => {
  try {
    const application = await VendorApplication.findOne({ authId });

    if (!application) {
      throw new ApiError(404, 'Vendor application not found');
    }

    if (application.status !== 'APPROVED') {
      throw new ApiError(
        403,
        `Your vendor application is not approved yet (status: ${application.status})`
      );
    }

    const categoryId = payload?.categoryId;
    const name = payload?.name;
    const price = payload?.price;
    const tier = payload?.tier === '' ? null : (payload?.tier ?? null);
    const description = payload?.description === '' ? null : (payload?.description ?? null);
    const details = payload?.details || {};

    if (typeof tier === 'string' && tier.trim()) {
      const existingTier = await VendorService.findOne({ authId, tier }).select('_id').lean();
      if (existingTier) {
        throw new ApiError(409, `You already have a service in the ${tier} tier`);
      }
    }

    const newService = new VendorService({
      authId,
      businessName: application.businessName,
      serviceCategory: application.serviceCategory,
      categoryId,
      name,
      price,
      tier,
      description,
      details,
      latitude: application.latitude,
      longitude: application.longitude,
      status: 'Active',
    });

    await newService.save();

    logger.info('Vendor service created', {
      serviceId: newService._id,
      authId,
      categoryId,
    });

    return newService.toObject();
  } catch (error) {
    logger.error('Error in createService:', error);

    // Race-safe: if unique index is present, convert duplicate key to a clean 409
    if (error?.code === 11000) {
      throw new ApiError(409, 'You already have a service in this tier');
    }

    throw error;
  }
};

/**
 * Get all services belonging to the calling vendor.
 *
 * @param {string} authId   - vendor's auth ID
 * @param {object} filters  - { status?, limit?, skip? }
 * @returns {{ services, total, limit, skip }}
 */
const getMyServices = async (authId, filters = {}) => {
  try {
    const { status, limit = 50, skip = 0 } = filters;

    const query = { authId };
    if (status && ['Active', 'Inactive'].includes(status)) {
      query.status = status;
    }

    const parsedLimit = Math.min(parseInt(limit, 10) || 50, 100);
    const parsedSkip = parseInt(skip, 10) || 0;

    const [services, total] = await Promise.all([
      VendorService.find(query)
        .sort({ createdAt: -1 })
        .skip(parsedSkip)
        .limit(parsedLimit)
        .lean(),
      VendorService.countDocuments(query),
    ]);

    return { services, total, limit: parsedLimit, skip: parsedSkip };
  } catch (error) {
    logger.error('Error in getMyServices:', error);
    throw error;
  }
};

/**
 * Update an existing service listing owned by the calling vendor.
 *
 * Updatable fields:
 *  - categoryId, name, price, tier, description, details, status
 *
 * Non-updatable (ignored even if supplied):
 *  - authId, businessName, serviceCategory, latitude, longitude
 *
 * @param {string} authId
 * @param {string} serviceId
 * @param {object} payload
 * @returns {object} updated VendorService document
 */
const updateService = async (authId, serviceId, payload = {}) => {
  try {
    if (!serviceId || !serviceId.trim()) {
      throw new ApiError(400, 'Service ID is required');
    }

    if (!mongoose.Types.ObjectId.isValid(serviceId)) {
      throw new ApiError(400, 'Invalid service ID');
    }

    const service = await VendorService.findOne({ _id: serviceId, authId });
    if (!service) {
      throw new ApiError(404, 'Service not found');
    }

    const validated = payload || {};

    if (Object.keys(validated).length === 0) {
      throw new ApiError(400, 'No updatable fields provided');
    }

    if (Object.prototype.hasOwnProperty.call(validated, 'categoryId')) {
      service.categoryId = validated.categoryId;
    }

    if (Object.prototype.hasOwnProperty.call(validated, 'name')) {
      service.name = validated.name;
    }

    if (Object.prototype.hasOwnProperty.call(validated, 'price')) {
      service.price = validated.price;
    }

    if (Object.prototype.hasOwnProperty.call(validated, 'tier')) {
      if (typeof validated.tier === 'string' && validated.tier.trim()) {
        const existingTier = await VendorService.findOne({
          authId,
          tier: validated.tier,
          _id: { $ne: serviceId },
        })
          .select('_id')
          .lean();

        if (existingTier) {
          throw new ApiError(409, `You already have a service in the ${validated.tier} tier`);
        }
      }

      service.tier = validated.tier === '' ? null : validated.tier;
    }

    if (Object.prototype.hasOwnProperty.call(validated, 'description')) {
      service.description = validated.description === '' ? null : validated.description;
    }

    if (Object.prototype.hasOwnProperty.call(validated, 'details')) {
      if (validated.details === null) {
        service.details = {};
      } else {
        const currentDetails =
          service.details && typeof service.details === 'object' ? service.details : {};
        const nextDetails =
          validated.details && typeof validated.details === 'object' ? validated.details : {};

        // Merge instead of replace so fields not present in the edit form
        // (e.g. Venue images stored at details.images) are preserved.
        service.details = { ...currentDetails, ...nextDetails };
      }
    }

    if (Object.prototype.hasOwnProperty.call(validated, 'status')) {
      service.status = validated.status;
    }

    await service.save();

    logger.info('Vendor service updated', {
      serviceId: service._id,
      authId,
    });

    return service.toObject();
  } catch (error) {
    logger.error('Error in updateService:', error);

    if (error?.code === 11000) {
      throw new ApiError(409, 'You already have a service in this tier');
    }

    throw error;
  }
};

/**
 * Delete an existing service listing owned by the calling vendor.
 *
 * @param {string} authId
 * @param {string} serviceId
 * @returns {{ serviceId: string }}
 */
const deleteService = async (authId, serviceId) => {
  try {
    if (!serviceId || !serviceId.trim()) {
      throw new ApiError(400, 'Service ID is required');
    }

    if (!mongoose.Types.ObjectId.isValid(serviceId)) {
      throw new ApiError(400, 'Invalid service ID');
    }

    const deleted = await VendorService.findOneAndDelete({ _id: serviceId, authId }).lean();
    if (!deleted) {
      throw new ApiError(404, 'Service not found');
    }

    logger.info('Vendor service deleted', {
      serviceId,
      authId,
    });

    return { serviceId };
  } catch (error) {
    logger.error('Error in deleteService:', error);
    throw error;
  }
};

/**
 * Upload venue images to an existing service owned by the vendor.
 * Enforced rule: only services with serviceCategory === 'Venue' can accept images.
 * Images are stored under service.details.images.
 */
const addVenueServiceImages = async (authId, serviceId, files = []) => {
  const MAX_IMAGES = 10;

  try {
    if (!authId) {
      throw new ApiError(401, 'User not authenticated');
    }

    if (!serviceId || !serviceId.trim()) {
      throw new ApiError(400, 'Service ID is required');
    }

    if (!mongoose.Types.ObjectId.isValid(serviceId)) {
      throw new ApiError(400, 'Invalid service ID');
    }

    if (!Array.isArray(files) || files.length === 0) {
      throw new ApiError(400, 'At least one image file is required');
    }

    const service = await VendorService.findOne({ _id: serviceId, authId });
    if (!service) {
      throw new ApiError(404, 'Service not found');
    }

    if (service.serviceCategory !== 'Venue') {
      throw new ApiError(403, 'Images can only be uploaded for Venue services');
    }

    const existingImages = Array.isArray(service.details?.images) ? service.details.images : [];
    if (existingImages.length + files.length > MAX_IMAGES) {
      throw new ApiError(400, `You can upload up to ${MAX_IMAGES} images per service`);
    }

    const uploaded = [];

    try {
      for (const file of files) {
        const uploadResult = await fileUploadService.uploadFile(
          file,
          `services/${serviceId}/venue-images`
        );
        uploaded.push({
          url: uploadResult.url,
          publicId: uploadResult.publicId,
          format: uploadResult.format,
          uploadedAt: new Date(),
        });
      }
    } catch (uploadError) {
      // Best-effort cleanup of already uploaded images
      await Promise.all(
        uploaded
          .map((img) => img.publicId)
          .filter(Boolean)
          .map((publicId) => fileUploadService.deleteFile(publicId).catch(() => null))
      );
      throw uploadError;
    }

    const nextDetails =
      service.details && typeof service.details === 'object' ? service.details : {};

    nextDetails.images = [...existingImages, ...uploaded];
    service.details = nextDetails;
    service.markModified('details');
    await service.save();

    logger.info('Venue images uploaded for service', {
      serviceId,
      authId,
      added: uploaded.length,
      total: nextDetails.images.length,
    });

    return service.toObject();
  } catch (error) {
    logger.error('Error in addVenueServiceImages:', error);
    throw error;
  }
};

/**
 * Delete a single venue image for a service (Venue only).
 * Enforces at least one image remains (venue images are required).
 */
const deleteVenueServiceImage = async (authId, serviceId, publicId) => {
  try {
    if (!authId) throw new ApiError(401, 'User not authenticated');
    if (!serviceId || !serviceId.trim()) throw new ApiError(400, 'Service ID is required');
    if (!mongoose.Types.ObjectId.isValid(serviceId)) throw new ApiError(400, 'Invalid service ID');
    if (!publicId || !String(publicId).trim()) throw new ApiError(400, 'publicId is required');

    const service = await VendorService.findOne({ _id: serviceId, authId });
    if (!service) throw new ApiError(404, 'Service not found');
    if (service.serviceCategory !== 'Venue') {
      throw new ApiError(403, 'Images can only be managed for Venue services');
    }

    const images = Array.isArray(service.details?.images) ? service.details.images : [];
    if (images.length <= 1) {
      throw new ApiError(400, 'At least one venue image is required');
    }

    const idx = images.findIndex((img) => String(img?.publicId) === String(publicId));
    if (idx === -1) {
      throw new ApiError(404, 'Image not found');
    }

    // Delete from Cloudinary first
    await fileUploadService.deleteFile(String(publicId));

    const nextImages = [...images.slice(0, idx), ...images.slice(idx + 1)];
    const nextDetails = service.details && typeof service.details === 'object' ? service.details : {};
    nextDetails.images = nextImages;
    service.details = nextDetails;
    service.markModified('details');
    await service.save();

    logger.info('Venue image deleted for service', {
      serviceId,
      authId,
      publicId,
      remaining: nextImages.length,
    });

    return service.toObject();
  } catch (error) {
    logger.error('Error in deleteVenueServiceImage:', error);
    throw error;
  }
};

/**
 * Set a venue service profile image by moving the selected image to index 0.
 */
const setVenueServiceProfileImage = async (authId, serviceId, publicId) => {
  try {
    if (!authId) throw new ApiError(401, 'User not authenticated');
    if (!serviceId || !serviceId.trim()) throw new ApiError(400, 'Service ID is required');
    if (!mongoose.Types.ObjectId.isValid(serviceId)) throw new ApiError(400, 'Invalid service ID');
    if (!publicId || !String(publicId).trim()) throw new ApiError(400, 'publicId is required');

    const service = await VendorService.findOne({ _id: serviceId, authId });
    if (!service) throw new ApiError(404, 'Service not found');
    if (service.serviceCategory !== 'Venue') {
      throw new ApiError(403, 'Images can only be managed for Venue services');
    }

    const images = Array.isArray(service.details?.images) ? service.details.images : [];
    if (images.length === 0) {
      throw new ApiError(400, 'No images found for this service');
    }

    const idx = images.findIndex((img) => String(img?.publicId) === String(publicId));
    if (idx === -1) {
      throw new ApiError(404, 'Image not found');
    }

    if (idx === 0) return service.toObject();

    const selected = images[idx];
    const nextImages = [selected, ...images.slice(0, idx), ...images.slice(idx + 1)];
    const nextDetails = service.details && typeof service.details === 'object' ? service.details : {};
    nextDetails.images = nextImages;
    service.details = nextDetails;
    service.markModified('details');
    await service.save();

    logger.info('Venue profile image set for service', {
      serviceId,
      authId,
      publicId,
    });

    return service.toObject();
  } catch (error) {
    logger.error('Error in setVenueServiceProfileImage:', error);
    throw error;
  }
};

/**
 * Public search for vendor services.
 *
 * Supported filters (all optional):
 *   businessName    – partial, case-insensitive string match
 *   serviceCategory – exact match against VendorApplication enum values
 *   latitude        – decimal degrees (requires longitude too)
 *   longitude       – decimal degrees (requires latitude too)
 *   radiusKm        – search radius in km (default: 50)
 *   limit           – max results per page (default: 20, max: 100)
 *   skip            – pagination offset (default: 0)
 *
 * Geo strategy: lat/lng bounding-box approximation
 *   1 degree latitude  ≈ 111 km
 *   1 degree longitude ≈ 111 × cos(lat) km
 *
 * @param {object} filters
 * @returns {{ services, total, limit, skip }}
 */
const searchServices = async (filters = {}) => {
  try {
    const {
      businessName,
      serviceCategory,
      latitude,
      longitude,
      radiusKm = 50,
      limit = 20,
      skip = 0,
    } = filters;

    const query = { status: 'Active' };

    const normalizedCategory = String(serviceCategory || '').trim();
    const isVenueCategory = normalizedCategory === 'Venue';

    if (businessName && businessName.trim()) {
      query.businessName = { $regex: businessName.trim(), $options: 'i' };
    }

    if (normalizedCategory) {
      query.serviceCategory = normalizedCategory;
    }

    if (latitude !== undefined && longitude !== undefined) {
      const lat = parseFloat(latitude);
      const lng = parseFloat(longitude);
      const radius = parseFloat(radiusKm);

      if (isNaN(lat) || isNaN(lng)) {
        throw new ApiError(400, 'latitude and longitude must be valid numbers');
      }

      const latDelta = radius / 111;
      const lngDelta = radius / (111 * Math.cos((lat * Math.PI) / 180));

      // Venue vendors can have multiple venue locations across cities.
      // For Venue only, filter using the venue location coordinates stored on the service.
      if (isVenueCategory) {
        query.$or = [
          {
            'details.locationLat': { $gte: lat - latDelta, $lte: lat + latDelta },
            'details.locationLng': { $gte: lng - lngDelta, $lte: lng + lngDelta },
          },
          {
            'details.lat': { $gte: lat - latDelta, $lte: lat + latDelta },
            'details.lng': { $gte: lng - lngDelta, $lte: lng + lngDelta },
          },
        ];
      } else {
        query.latitude = { $gte: lat - latDelta, $lte: lat + latDelta };
        query.longitude = { $gte: lng - lngDelta, $lte: lng + lngDelta };
      }
    }

    const parsedLimit = Math.min(parseInt(limit, 10) || 20, 100);
    const parsedSkip = parseInt(skip, 10) || 0;

    const [services, total] = await Promise.all([
      VendorService.find(query)
        .sort({ createdAt: -1 })
        .skip(parsedSkip)
        .limit(parsedLimit)
        .lean(),
      VendorService.countDocuments(query),
    ]);

    return { services, total, limit: parsedLimit, skip: parsedSkip };
  } catch (error) {
    logger.error('Error in searchServices:', error);
    throw error;
  }
};

/**
 * Public vendor profile lookup for a set of vendor authIds.
 *
 * Returns sanitized VendorApplication fields only (no email/phone/documents).
 * Only APPROVED vendors are returned.
 */
const getPublicVendorsByAuthIds = async (authIds) => {
  const ids = parseAuthIds(authIds);
  if (ids.length === 0) {
    throw new ApiError(400, 'authIds is required');
  }

  // Keep this payload strictly public-safe
  const vendors = await VendorApplication.find({
    authId: { $in: ids },
    status: 'APPROVED',
  })
    .select(
      'authId businessName serviceCategory images location place country latitude longitude description status'
    )
    .lean();

  return vendors;
};

/**
 * Public vendor profile search (sanitized).
 * Only APPROVED vendors.
 */
const searchPublicVendors = async (filters = {}) => {
  const {
    businessName,
    serviceCategory,
    latitude,
    longitude,
    radiusKm = 50,
    limit = 20,
    skip = 0,
  } = filters;

  const query = { status: 'APPROVED' };

  if (businessName && String(businessName).trim()) {
    query.businessName = { $regex: String(businessName).trim(), $options: 'i' };
  }

  if (serviceCategory && String(serviceCategory).trim()) {
    query.serviceCategory = String(serviceCategory).trim();
  }

  if (latitude !== undefined && longitude !== undefined) {
    const lat = parseFloat(latitude);
    const lng = parseFloat(longitude);
    const radius = parseFloat(radiusKm);

    if (isNaN(lat) || isNaN(lng)) {
      throw new ApiError(400, 'latitude and longitude must be valid numbers');
    }

    const latDelta = radius / 111;
    const lngDelta = radius / (111 * Math.cos((lat * Math.PI) / 180));

    query.latitude = { $gte: lat - latDelta, $lte: lat + latDelta };
    query.longitude = { $gte: lng - lngDelta, $lte: lng + lngDelta };
  }

  const parsedLimit = Math.min(parseInt(limit, 10) || 20, 100);
  const parsedSkip = parseInt(skip, 10) || 0;

  const [vendors, total] = await Promise.all([
    VendorApplication.find(query)
      .sort({ createdAt: -1 })
      .skip(parsedSkip)
      .limit(parsedLimit)
      .select('authId businessName serviceCategory images location place country latitude longitude description status')
      .lean(),
    VendorApplication.countDocuments(query),
  ]);

  return { vendors, total, limit: parsedLimit, skip: parsedSkip };
};

/**
 * Public service lookup by id (sanitized).
 * Only returns Active services.
 */
const getPublicServiceById = async (serviceId) => {
  const id = String(serviceId || '').trim();
  if (!id) {
    throw new ApiError(400, 'Service ID is required');
  }

  if (!mongoose.Types.ObjectId.isValid(id)) {
    throw new ApiError(400, 'Invalid service ID');
  }

  const service = await VendorService.findOne({ _id: id, status: 'Active' })
    .select('businessName serviceCategory categoryId name price tier description details latitude longitude')
    .lean();

  if (!service) {
    throw new ApiError(404, 'Service not found');
  }

  return service;
};

module.exports = {
  createService,
  getMyServices,
  searchServices,
  getPublicVendorsByAuthIds,
  searchPublicVendors,
  getPublicServiceById,
  updateService,
  deleteService,
  addVenueServiceImages,
  deleteVenueServiceImage,
  setVenueServiceProfileImage,
};
