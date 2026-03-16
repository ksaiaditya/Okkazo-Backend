const mongoose = require('mongoose');
const VendorApplication = require('../models/VendorApplication');
const VendorService = require('../models/VendorService');
const ApiError = require('../utils/ApiError');
const logger = require('../utils/logger');

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
      service.details = validated.details === null ? {} : validated.details;
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

    if (businessName && businessName.trim()) {
      query.businessName = { $regex: businessName.trim(), $options: 'i' };
    }

    if (serviceCategory && serviceCategory.trim()) {
      query.serviceCategory = serviceCategory.trim();
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

module.exports = {
  createService,
  getMyServices,
  searchServices,
  updateService,
  deleteService,
};
