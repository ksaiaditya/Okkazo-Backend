const { VendorAvailability, SOURCE } = require('../models/VendorAvailability');
const ApiError = require('../utils/ApiError');
const logger = require('../utils/logger');
const VendorService = require('../models/VendorService');

const DAY_RE = /^\d{4}-\d{2}-\d{2}$/;

const normalizeDay = (value, fieldName = 'day') => {
  if (value == null) {
    throw new ApiError(400, `${fieldName} is required`);
  }

  const day = String(value).trim();
  if (!DAY_RE.test(day)) {
    throw new ApiError(400, `${fieldName} must be in YYYY-MM-DD format`);
  }

  return day;
};

const normalizeRange = ({ day, from, to }) => {
  if (day) {
    const d = normalizeDay(day, 'day');
    return { fromDay: d, toDay: d };
  }

  const fromDay = from ? normalizeDay(from, 'from') : null;
  const toDay = to ? normalizeDay(to, 'to') : null;

  if (!fromDay || !toDay) {
    throw new ApiError(400, 'Provide either day OR (from and to)');
  }

  if (fromDay > toDay) {
    throw new ApiError(400, 'from must be <= to');
  }

  return { fromDay, toDay };
};

const upsertManualUnavailable = async ({ vendorAuthId, day, reason, actor }) => {
  const normalizedDay = normalizeDay(day, 'day');
  if (!vendorAuthId || !String(vendorAuthId).trim()) {
    throw new ApiError(400, 'vendorAuthId is required');
  }

  const update = {
    vendorAuthId: String(vendorAuthId).trim(),
    day: normalizedDay,
    source: SOURCE.MANUAL,
    sourceId: SOURCE.MANUAL,
    reason: reason ? String(reason).trim() : null,
    createdByAuthId: actor?.authId ? String(actor.authId) : null,
    createdByRole: actor?.role ? String(actor.role) : null,
  };

  const doc = await VendorAvailability.findOneAndUpdate(
    {
      vendorAuthId: update.vendorAuthId,
      day: update.day,
      source: SOURCE.MANUAL,
      sourceId: SOURCE.MANUAL,
    },
    { $set: update },
    { new: true, upsert: true }
  ).lean();

  return doc;
};

const removeManualUnavailable = async ({ vendorAuthId, day }) => {
  const normalizedDay = normalizeDay(day, 'day');
  if (!vendorAuthId || !String(vendorAuthId).trim()) {
    throw new ApiError(400, 'vendorAuthId is required');
  }

  const result = await VendorAvailability.deleteOne({
    vendorAuthId: String(vendorAuthId).trim(),
    day: normalizedDay,
    source: SOURCE.MANUAL,
    sourceId: SOURCE.MANUAL,
  });

  return { removed: result.deletedCount || 0 };
};

const listUnavailable = async ({ vendorAuthId, fromDay, toDay }) => {
  if (!vendorAuthId || !String(vendorAuthId).trim()) {
    throw new ApiError(400, 'vendorAuthId is required');
  }

  const query = {
    vendorAuthId: String(vendorAuthId).trim(),
    day: { $gte: fromDay, $lte: toDay },
  };

  return VendorAvailability.find(query)
    .sort({ day: 1, createdAt: 1 })
    .lean();
};

const blockDaysForEvent = async ({ vendorAuthId, eventId, days, actor }) => {
  if (!vendorAuthId || !String(vendorAuthId).trim()) {
    throw new ApiError(400, 'vendorAuthId is required');
  }
  if (!eventId || !String(eventId).trim()) {
    throw new ApiError(400, 'eventId is required');
  }
  if (!Array.isArray(days) || days.length === 0) {
    throw new ApiError(400, 'days must be a non-empty array of YYYY-MM-DD');
  }

  const normalizedDays = Array.from(new Set(days.map((d) => normalizeDay(d, 'days[]'))));
  const vendor = String(vendorAuthId).trim();
  const sourceId = String(eventId).trim();

  const ops = normalizedDays.map((day) => ({
    updateOne: {
      filter: { vendorAuthId: vendor, day, source: SOURCE.BOOKING, sourceId },
      update: {
        $setOnInsert: {
          vendorAuthId: vendor,
          day,
          source: SOURCE.BOOKING,
          sourceId,
          reason: 'Booked for event',
          createdByAuthId: actor?.authId ? String(actor.authId) : null,
          createdByRole: actor?.role ? String(actor.role) : null,
        },
      },
      upsert: true,
    },
  }));

  const result = await VendorAvailability.bulkWrite(ops, { ordered: false });

  logger.info('Blocked vendor days for event', {
    vendorAuthId: vendor,
    eventId: sourceId,
    days: normalizedDays.length,
  });

  return {
    upserted: result.upsertedCount || 0,
    matched: result.matchedCount || 0,
    modified: result.modifiedCount || 0,
    days: normalizedDays,
  };
};

const unblockDaysForEvent = async ({ vendorAuthId, eventId, days }) => {
  if (!vendorAuthId || !String(vendorAuthId).trim()) {
    throw new ApiError(400, 'vendorAuthId is required');
  }
  if (!eventId || !String(eventId).trim()) {
    throw new ApiError(400, 'eventId is required');
  }

  const vendor = String(vendorAuthId).trim();
  const sourceId = String(eventId).trim();

  let queryDays = null;
  if (Array.isArray(days) && days.length > 0) {
    queryDays = Array.from(new Set(days.map((d) => normalizeDay(d, 'days[]'))));
  }

  const query = {
    vendorAuthId: vendor,
    source: SOURCE.BOOKING,
    sourceId,
    ...(queryDays ? { day: { $in: queryDays } } : {}),
  };

  const result = await VendorAvailability.deleteMany(query);

  logger.info('Unblocked vendor days for event', {
    vendorAuthId: vendor,
    eventId: sourceId,
    removed: result.deletedCount || 0,
  });

  return { removed: result.deletedCount || 0 };
};

/**
 * Public search for available vendor services.
 * Filters mirror VendorServiceCatalog.searchServices, plus:
 *   day OR from/to (YYYY-MM-DD)
 */
const searchAvailableServices = async (filters = {}) => {
  const {
    businessName,
    serviceCategory,
    latitude,
    longitude,
    radiusKm = 50,
    limit = 20,
    skip = 0,
    day,
    from,
    to,
  } = filters;

  const { fromDay, toDay } = normalizeRange({ day, from, to });

  const query = { status: 'Active' };

  const normalizedCategory = String(serviceCategory || '').trim();
  const isVenueCategory = normalizedCategory === 'Venue';

  if (businessName && String(businessName).trim()) {
    query.businessName = { $regex: String(businessName).trim(), $options: 'i' };
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

  // Find blocked vendors for the requested day range
  const blockedVendorAuthIds = await VendorAvailability.distinct('vendorAuthId', {
    day: { $gte: fromDay, $lte: toDay },
  });

  if (blockedVendorAuthIds.length > 0) {
    query.authId = { $nin: blockedVendorAuthIds };
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

  return {
    services,
    total,
    limit: parsedLimit,
    skip: parsedSkip,
    availability: { from: fromDay, to: toDay, blockedVendors: blockedVendorAuthIds.length },
  };
};

module.exports = {
  normalizeDay,
  normalizeRange,
  upsertManualUnavailable,
  removeManualUnavailable,
  listUnavailable,
  blockDaysForEvent,
  unblockDaysForEvent,
  searchAvailableServices,
};
