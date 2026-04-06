const Planning = require('../models/Planning');
const vendorSelectionService = require('../services/vendorSelectionService');
const createApiError = require('../utils/ApiError');
const logger = require('../utils/logger');
const axios = require('axios');
const { STATUS: PLANNING_STATUS } = require('../utils/planningConstants');
const { SERVICE_OPTIONS, VENDOR_STATUS } = require('../utils/vendorSelectionConstants');
const vendorReservationService = require('../services/vendorReservationService');
const { fetchUserById } = require('../services/userServiceClient');
const { publishEvent } = require('../kafka/eventProducer');
const { ensureEventChatSeeded } = require('../services/chatSeedService');
const { sendEventConversationMessage, sendEventDmConversationMessage } = require('../services/chatServiceClient');
const { encodeRichChatMessage } = require('../utils/richChat');
const planningQuoteService = require('../services/planningQuoteService');
const commissionService = require('../services/commissionService');
const { normalizeIstDayInput } = require('../utils/istDateTime');

const defaultVendorServiceUrl = process.env.SERVICE_HOST
  ? 'http://vendor-service:8084' // docker-compose service name
  : 'http://localhost:8084';
const vendorServiceUrl = process.env.VENDOR_SERVICE_URL || defaultVendorServiceUrl;
const defaultOrderServiceUrl = process.env.SERVICE_HOST
  ? 'http://order-service:8087'
  : 'http://localhost:8087';
const orderServiceUrl = process.env.ORDER_SERVICE_URL || defaultOrderServiceUrl;
const upstreamTimeoutMs = parseInt(process.env.UPSTREAM_HTTP_TIMEOUT_MS || '10000', 10);
const HOLD_REPAIR_WINDOW_MS = Math.max(
  60 * 1000,
  Number(process.env.VENDOR_RESERVATION_HOLD_TTL_MS || 10 * 60 * 1000)
);
const STICKY_PLANNING_STATUSES = new Set([
  PLANNING_STATUS.CONFIRMED,
  PLANNING_STATUS.COMPLETED,
  PLANNING_STATUS.VENDOR_PAYMENT_PENDING,
  PLANNING_STATUS.CLOSED,
]);

// Keep alternatives scoped to a reasonable distance from event location.
const ALTERNATIVES_RADIUS_KM = 120;

const toNumberOrNull = (value) => {
  const n = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(n) ? n : null;
};

const toMoneyOrNull = (value) => {
  const n = typeof value === 'number' ? value : Number(value);
  if (!Number.isFinite(n)) return null;
  return Math.round(n * 100) / 100;
};

const haversineKm = ({ lat1, lon1, lat2, lon2 }) => {
  const R = 6371;
  const toRad = (deg) => (deg * Math.PI) / 180;

  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);

  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) * Math.sin(dLon / 2);

  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
};

const formatDistance = (km) => {
  const n = toNumberOrNull(km);
  if (n == null) return null;
  if (n < 1) return `${Math.round(n * 1000)} m`;
  return `${n.toFixed(1)} km`;
};

const buildInternalOrderServiceHeaders = (user = {}) => ({
  'x-auth-id': String(user?.authId || 'event-service').trim() || 'event-service',
  'x-user-id': String(user?.userId || '').trim(),
  'x-user-email': String(user?.email || '').trim(),
  'x-user-username': String(user?.username || 'event-service').trim(),
  'x-user-role': 'MANAGER',
});

const fetchVendorPayoutsForEventFromOrderService = async ({ eventId, user }) => {
  const normalizedEventId = String(eventId || '').trim();
  if (!normalizedEventId) return [];

  const response = await axios.get(
    `${orderServiceUrl}/orders/vendor-payouts/event/${encodeURIComponent(normalizedEventId)}`,
    {
      timeout: upstreamTimeoutMs,
      headers: buildInternalOrderServiceHeaders(user),
    }
  );

  const payouts = Array.isArray(response?.data?.data?.payouts) ? response.data.data.payouts : [];
  return payouts;
};

const fetchVendorPayoutsForVendorFromOrderService = async ({ vendorAuthId, user }) => {
  const normalizedVendorAuthId = String(vendorAuthId || '').trim();
  if (!normalizedVendorAuthId) return [];

  const response = await axios.get(
    `${orderServiceUrl}/orders/vendor-payouts/vendor/${encodeURIComponent(normalizedVendorAuthId)}`,
    {
      timeout: upstreamTimeoutMs,
      headers: buildInternalOrderServiceHeaders(user),
    }
  );

  const payouts = Array.isArray(response?.data?.data?.payouts) ? response.data.data.payouts : [];
  return payouts;
};

const toInrFromPaise = (value) => {
  const paise = Number(value || 0);
  if (!Number.isFinite(paise) || paise <= 0) return 0;
  return Number((paise / 100).toFixed(2));
};

const toDateLabel = (value) => {
  const d = value ? new Date(value) : null;
  if (!d || Number.isNaN(d.getTime())) return '—';
  return d.toLocaleDateString(undefined, { day: '2-digit', month: 'short', year: 'numeric' });
};

const fetchPublicVendorsByAuthIds = async (authIds) => {
  if (!Array.isArray(authIds) || authIds.length === 0) return [];

  const response = await axios.get(`${vendorServiceUrl}/api/vendor/public/vendors`, {
    timeout: upstreamTimeoutMs,
    params: {
      authIds: authIds.join(','),
    },
  });

  const vendors = response.data?.data?.vendors;
  return Array.isArray(vendors) ? vendors : [];
};

const fetchPublicServiceById = async (serviceId) => {
  const id = String(serviceId || '').trim();
  if (!id) return null;

  const response = await axios.get(`${vendorServiceUrl}/api/vendor/public/services/${encodeURIComponent(id)}`,
    {
      timeout: upstreamTimeoutMs,
    }
  );

  return response.data?.data?.service || null;
};

const searchPublicVendorServices = async ({ serviceCategory, latitude, longitude, radiusKm, limit, skip }) => {
  const response = await axios.get(`${vendorServiceUrl}/api/vendor/services/search`, {
    timeout: upstreamTimeoutMs,
    params: {
      serviceCategory,
      latitude,
      longitude,
      radiusKm,
      limit,
      skip,
    },
  });

  const services = response.data?.data?.services;
  return Array.isArray(services) ? services : [];
};

const searchPublicVendors = async ({ serviceCategory, businessName, latitude, longitude, radiusKm, limit, skip }) => {
  const response = await axios.get(`${vendorServiceUrl}/api/vendor/public/vendors/search`, {
    timeout: upstreamTimeoutMs,
    params: {
      serviceCategory,
      businessName,
      latitude,
      longitude,
      radiusKm,
      limit,
      skip,
    },
  });

  const vendors = response.data?.data?.vendors;
  return Array.isArray(vendors) ? vendors : [];
};

const toFiniteNumberOrNull = (value) => {
  const n = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(n) ? n : null;
};

const computeVenueLocationFromService = (service) => {
  if (!service || typeof service !== 'object') return null;

  const details = service.details && typeof service.details === 'object' ? service.details : {};

  // IMPORTANT: use per-venue coordinates only (a vendor can have many venues).
  // Do NOT fall back to vendor application lat/lng.
  const lat =
    toFiniteNumberOrNull(details.locationLat) ??
    toFiniteNumberOrNull(details.lat);
  const lng =
    toFiniteNumberOrNull(details.locationLng) ??
    toFiniteNumberOrNull(details.lng);

  if (lat == null || lng == null) return null;

  const nameCandidates = [
    details.locationAreaName,
    details.location,
    details.mapsUrl,
    details.address,
    service.name,
    service.businessName,
  ];
  const normalizedCandidates = nameCandidates
    .map((v) => (v == null ? '' : String(v).trim()))
    .filter((v) => v.length > 0);

  // Avoid showing a raw URL as a location label when possible.
  const nonUrlName = normalizedCandidates.find((v) => !/^https?:\/\//i.test(v));
  const name = nonUrlName || normalizedCandidates[0] || '';

  if (!name) return null;

  return { name, latitude: lat, longitude: lng };
};

const extractServiceLocationLabel = (service) => {
  if (!service || typeof service !== 'object') return null;

  const details = service.details && typeof service.details === 'object' ? service.details : {};

  const deriveReadableLocationFromMapUrl = (rawUrl) => {
    if (!rawUrl) return null;
    try {
      const u = new URL(String(rawUrl).trim());

      // Common format: /maps/place/<label>/...
      const placeMatch = String(u.pathname || '').match(/\/maps\/place\/([^/]+)/i);
      if (placeMatch?.[1]) {
        const decoded = decodeURIComponent(placeMatch[1]).replace(/\+/g, ' ').trim();
        if (decoded) return decoded;
      }

      // Fallback for query labels (ignore raw lat,lng query).
      const q = String(u.searchParams.get('q') || u.searchParams.get('query') || '').trim();
      if (q) {
        const looksLikeCoordinates = /^-?\d+(?:\.\d+)?\s*,\s*-?\d+(?:\.\d+)?$/.test(q);
        if (!looksLikeCoordinates) {
          const decodedQ = decodeURIComponent(q).replace(/\+/g, ' ').trim();
          if (decodedQ) return decodedQ;
        }
      }

      return null;
    } catch {
      return null;
    }
  };

  const mapUrl = details.locationMapsUrl || details.mapsUrl || details.googleMapsUrl;
  const mapUrlLabel = deriveReadableLocationFromMapUrl(mapUrl);
  const candidates = [
    details.locationAreaName,
    details.location,
    details.address,
    mapUrlLabel,
    details.locationMapsUrl,
    details.mapsUrl,
    details.googleMapsUrl,
  ]
    .map((v) => (v == null ? '' : String(v).trim()))
    .filter((v) => v.length > 0);

  const nonUrl = candidates.find((v) => !/^https?:\/\//i.test(v));
  return nonUrl || candidates[0] || null;
};

const ensureAccessToPlanning = async ({ eventId, user }) => {
  if (!eventId?.trim()) throw createApiError(400, 'Event ID is required');

  const planning = await Planning.findOne({ eventId: eventId.trim() });
  if (!planning) throw createApiError(404, 'Planning not found');

  if (
    user?.role !== 'ADMIN' &&
    user?.role !== 'MANAGER' &&
    planning.authId !== user?.authId
  ) {
    throw createApiError(403, 'Access denied');
  }

  return planning;
};

const normalizeVendorAuthId = (req) => {
  const vendorAuthId = String(req?.user?.authId || '').trim();
  if (!vendorAuthId) {
    const err = createApiError(401, 'Authentication required');
    err.statusCode = 401;
    throw err;
  }
  if (req?.user?.role !== 'VENDOR') {
    throw createApiError(403, 'Access denied');
  }
  return vendorAuthId;
};

const normalizeEventIdParam = (eventId) => {
  const eid = String(eventId || '').trim();
  if (!eid) throw createApiError(400, 'Event ID is required');
  return eid;
};

const summarizeVendorItems = (items = []) => {
  const pending = items.filter((i) => i?.status === VENDOR_STATUS.YET_TO_SELECT).length;
  const accepted = items.filter((i) => i?.status === VENDOR_STATUS.ACCEPTED).length;
  const rejected = items.filter((i) => i?.status === VENDOR_STATUS.REJECTED).length;
  const total = items.length;

  let summaryStatus = 'PENDING';
  if (total === 0) summaryStatus = 'UNKNOWN';
  else if (pending > 0) summaryStatus = 'PENDING';
  else if (rejected > 0) summaryStatus = 'REJECTED';
  else summaryStatus = 'ACCEPTED';

  return { pending, accepted, rejected, total, summaryStatus };
};

const clearStaleSelectionLocks = async ({ selection, planning, dayOverride = null, fromOverride = null, toOverride = null }) => {
  if (!selection || !planning) {
    return selection?.toObject ? selection.toObject() : selection;
  }

  const requestedDay = dayOverride != null ? normalizeIstDayInput(dayOverride) : null;
  const requestedFrom = fromOverride != null ? normalizeIstDayInput(fromOverride) : null;
  const requestedTo = toOverride != null ? normalizeIstDayInput(toOverride) : null;
  const requestedDays = vendorReservationService.resolveReservationDays({
    day: requestedDay,
    from: requestedFrom,
    to: requestedTo,
  });
  const planningDays = vendorReservationService.planningToReservationDays(planning);
  const candidateDays = Array.from(new Set([...requestedDays, ...planningDays]));
  const eventId = String(planning?.eventId || '').trim();
  if (candidateDays.length === 0 || !eventId) {
    return selection?.toObject ? selection.toObject() : selection;
  }

  const rawSelection = selection?.toObject ? selection.toObject() : selection;
  const currentVendors = Array.isArray(rawSelection?.vendors) ? rawSelection.vendors : [];
  if (currentVendors.length === 0) {
    return rawSelection;
  }

  const selectionUpdatedAtMs = (() => {
    const d = rawSelection?.updatedAt ? new Date(rawSelection.updatedAt) : null;
    const ts = d?.getTime?.();
    return Number.isFinite(ts) ? ts : null;
  })();

  const isRecentSelection = selectionUpdatedAtMs != null
    ? (Date.now() - selectionUpdatedAtMs) <= HOLD_REPAIR_WINDOW_MS
    : false;

  const planningStatus = String(planning?.status || '').trim();
  const isStickyPlanning =
    Boolean(planning?.depositPaid) ||
    Boolean(planning?.vendorConfirmationPaid) ||
    Boolean(planning?.fullPaymentPaid) ||
    STICKY_PLANNING_STATUSES.has(planningStatus);

  const shouldAttemptHoldRepair = isStickyPlanning || isRecentSelection;
  const preferredRepairDays = requestedDays.length > 0 ? requestedDays : planningDays;

  const activeReservations = await vendorReservationService.listActiveReservationsForEventDays({
    eventId,
    days: candidateDays,
  });

  const resolveReservationServiceId = (reservation) => {
    const direct = reservation?.serviceId != null ? String(reservation.serviceId).trim() : '';
    if (direct) return direct;

    const lockId = String(reservation?.vendorAuthId || '').trim();
    if (lockId.startsWith('service:')) {
      const parsed = String(lockId.slice('service:'.length) || '').trim();
      return parsed || null;
    }

    return null;
  };

  const toReservationKey = ({ service, serviceId }) => {
    const normalizedService = vendorSelectionService.canonicalizeService(service);
    if (!normalizedService) return null;

    if (normalizedService === 'Venue') {
      const sid = String(serviceId || '').trim();
      if (!sid) return null;
      return `${normalizedService}::${sid}`;
    }

    return `${normalizedService}::`;
  };

  const reservationCoverageByServiceKey = new Map();
  for (const reservation of (Array.isArray(activeReservations) ? activeReservations : [])) {
    const key = toReservationKey({
      service: reservation?.service,
      serviceId: resolveReservationServiceId(reservation),
    });
    if (!key) continue;

    const reservationDay = normalizeIstDayInput(reservation?.day);
    if (!reservationDay) continue;

    const entry = reservationCoverageByServiceKey.get(key) || { byDay: new Map() };
    const existingForDay = entry.byDay.get(reservationDay);

    const existingCreatedAt = existingForDay?.createdAt ? new Date(existingForDay.createdAt).getTime() : 0;
    const candidateCreatedAt = reservation?.createdAt ? new Date(reservation.createdAt).getTime() : 0;

    if (!existingForDay || candidateCreatedAt >= existingCreatedAt) {
      entry.byDay.set(reservationDay, reservation);
    }

    reservationCoverageByServiceKey.set(key, entry);
  }

  const nextVendors = [];

  for (const item of currentVendors) {
    const normalizedService = vendorSelectionService.canonicalizeService(item?.service);
    const serviceId = item?.serviceId != null ? String(item.serviceId).trim() : null;
    const vendorAuthId = item?.vendorAuthId != null ? String(item.vendorAuthId).trim() : '';

    const reservationKey = toReservationKey({ service: normalizedService, serviceId });
    const reservationEntry = reservationKey ? reservationCoverageByServiceKey.get(reservationKey) : null;

    const hasFullCoverage = Boolean(
      reservationEntry && candidateDays.every((day) => reservationEntry.byDay.has(day))
    );

    let restoredVendorAuthId = '';
    let restoredServiceId = null;

    if (hasFullCoverage) {
      const coverageRows = candidateDays
        .map((day) => reservationEntry.byDay.get(day))
        .filter(Boolean);

      const restoredVendorIds = Array.from(new Set(
        coverageRows
          .map((row) => String(row?.ownerVendorAuthId || row?.vendorAuthId || '').trim())
          .filter(Boolean)
      ));

      if (restoredVendorIds.length === 1) {
        restoredVendorAuthId = restoredVendorIds[0];
      }

      if (normalizedService === 'Venue') {
        const restoredServiceIds = Array.from(new Set(
          coverageRows
            .map((row) => String(resolveReservationServiceId(row) || '').trim())
            .filter(Boolean)
        ));

        if (restoredServiceIds.length === 1) {
          restoredServiceId = restoredServiceIds[0];
        } else if (restoredServiceIds.length === 0) {
          restoredServiceId = serviceId || null;
        } else {
          restoredVendorAuthId = '';
          restoredServiceId = null;
        }
      }
    }

    if (!vendorAuthId) {
      if (restoredVendorAuthId) {
        nextVendors.push({
          ...(item || {}),
          vendorAuthId: restoredVendorAuthId,
          serviceId: normalizedService === 'Venue'
            ? (restoredServiceId || serviceId || null)
            : (serviceId || null),
        });
      } else {
        nextVendors.push(item);
      }
      continue;
    }

    let isHeld = false;
    let holdCheckFailed = false;
    try {
      isHeld = await vendorReservationService.isHeldByEventForDays({
        vendorAuthId,
        days: candidateDays,
        eventId,
        service: normalizedService,
        serviceId,
      });
    } catch (error) {
      holdCheckFailed = true;
      logger.warn('Failed to verify reservation ownership while hydrating selection', {
        eventId,
        days: candidateDays,
        service: normalizedService,
        vendorAuthId,
        serviceId,
        error: error?.message || String(error),
      });
    }

    // Fail-open on verification errors to avoid transient refresh/poll races
    // from wiping selected vendors in the user experience.
    if (isHeld || holdCheckFailed) {
      nextVendors.push(item);
      continue;
    }

    if (shouldAttemptHoldRepair && preferredRepairDays.length > 0) {
      try {
        await vendorReservationService.claimForDays({
          vendorAuthId,
          days: preferredRepairDays,
          eventId,
          authId: String(planning?.authId || ''),
          service: normalizedService,
          serviceId,
        });

        nextVendors.push(item);
        continue;
      } catch (error) {
        logger.warn('Failed to repair reservation during selection hydration', {
          eventId,
          days: preferredRepairDays,
          service: normalizedService,
          vendorAuthId,
          serviceId,
          error: error?.message || String(error),
        });
      }
    }

    if (restoredVendorAuthId) {
      nextVendors.push({
        ...(item || {}),
        vendorAuthId: restoredVendorAuthId,
        serviceId: normalizedService === 'Venue'
          ? (restoredServiceId || serviceId || null)
          : (serviceId || null),
      });
      continue;
    }

    nextVendors.push({
      ...(item || {}),
      vendorAuthId: null,
      serviceId: null,
      status: VENDOR_STATUS.YET_TO_SELECT,
      rejectionReason: null,
      alternativeNeeded: false,
      servicePrice: { min: 0, max: 0 },
      pricingUnit: null,
      pricingQuantity: null,
      pricingQuantityUnit: null,
    });
  }

  // Keep GET hydration non-destructive: stale lock projection is returned to the client,
  // but we do not mutate persisted VendorSelection on read paths.
  return {
    ...(rawSelection || {}),
    vendors: nextVendors,
  };
};

/**
 * GET /vendor-selection/:eventId/alternatives?service=...&limit=...
 * Returns available vendor options for the given service (same category), filtered by event date reservations.
 */
const listAlternativesForService = async (req, res) => {
  try {
    const { eventId } = req.params;
    const rawService = String(req.query.service || '').trim();
    const service = vendorSelectionService.canonicalizeService(rawService);
    const limit = Math.min(parseInt(req.query.limit, 10) || 20, 50);

    if (!rawService) {
      return res.status(400).json({ success: false, message: 'service query param is required' });
    }

    if (!SERVICE_OPTIONS.includes(service)) {
      return res.status(400).json({
        success: false,
        message: `Invalid service. Expected one of: ${SERVICE_OPTIONS.join(', ')}`,
      });
    }

    const planning = await ensureAccessToPlanning({ eventId, user: req.user });
    const requestedDay = normalizeIstDayInput(req.query.day);
    const requestedFrom = normalizeIstDayInput(req.query.from);
    const requestedTo = normalizeIstDayInput(req.query.to);
    const planningDays = vendorReservationService.planningToReservationDays(planning);
    const days = vendorReservationService.resolveReservationDays({
      day: requestedDay,
      from: requestedFrom,
      to: requestedTo,
      days: planningDays,
    });

    if (days.length === 0) {
      return res.status(400).json({ success: false, message: 'Event date is required to find alternatives' });
    }

    const day = days[0];
    const from = days.length > 1 ? days[0] : null;
    const to = days.length > 1 ? days[days.length - 1] : null;

    const selection = await vendorSelectionService.ensureForPlanning(planning);
    const currentVendorForService = Array.isArray(selection?.vendors)
      ? selection.vendors.find((v) => v?.service === service)
      : null;

    const currentVendorAuthId = currentVendorForService?.vendorAuthId != null
      ? String(currentVendorForService.vendorAuthId).trim()
      : '';

    const reservedVendorByOthers = await vendorReservationService.listReservedVendorAuthIdsForDays({
      days,
      excludeEventId: eventId,
    });
    const reservedServiceByOthers = await vendorReservationService.listReservedServiceIdsForDays({
      days,
      excludeEventId: eventId,
    });

    // Optional geo filter (best-effort; only when both lat/lng are present)
    const lat = planning?.location?.latitude;
    const lng = planning?.location?.longitude;
    const hasGeo = typeof lat === 'number' && typeof lng === 'number' && Number.isFinite(lat) && Number.isFinite(lng);

    const radiusKm = hasGeo ? ALTERNATIVES_RADIUS_KM : undefined;

    const isVenue = service === 'Venue';

    let services = await searchPublicVendorServices({
      serviceCategory: service,
      latitude: hasGeo ? lat : undefined,
      longitude: hasGeo ? lng : undefined,
      radiusKm,
      limit: Math.max(limit * 4, 20),
      skip: 0,
    });

    // Best-effort fallback: if geo-constrained search yields nothing, retry without geo.
    // This avoids a "no alternatives" result when the stored radius is too strict.
    // For Venue, never drop geo constraints, otherwise we can suggest venues in other cities.
    if (!isVenue && hasGeo && (!Array.isArray(services) || services.length === 0)) {
      logger.info('No vendor services found with geo filter; retrying without geo', {
        eventId,
        service,
        latitude: lat,
        longitude: lng,
        radiusKm,
      });
      services = await searchPublicVendorServices({
        serviceCategory: service,
        limit: Math.max(limit * 4, 20),
        skip: 0,
      });
    }

    const reservedVendorSet = new Set((reservedVendorByOthers || []).map((v) => String(v || '').trim()).filter(Boolean));
    const reservedServiceSet = new Set((reservedServiceByOthers || []).map((v) => String(v || '').trim()).filter(Boolean));
    const currentServiceId = currentVendorForService?.serviceId != null
      ? String(currentVendorForService.serviceId).trim()
      : '';

    if (currentVendorAuthId) reservedVendorSet.add(currentVendorAuthId);
    if (currentServiceId) reservedServiceSet.add(currentServiceId);

    // Filter by availability and build alternatives
    // - Venue: return individual services (venues) using *service* location coordinates
    // - Non-Venue: group by vendor and include vendor.services[] so the client can choose a specific package
    let alternatives = [];

    if (isVenue) {
      const rows = [];
      for (const svc of services) {
        const vendorAuthId = String(svc?.authId || '').trim();
        if (!vendorAuthId) continue;
        const nextServiceId = svc?._id != null ? String(svc._id).trim() : '';
        if (!nextServiceId) continue;
        if (reservedServiceSet.has(nextServiceId)) continue;

        const venueLocation = computeVenueLocationFromService(svc);
        if (!venueLocation) continue;

        const distanceKm = hasGeo
          ? haversineKm({ lat1: lat, lon1: lng, lat2: venueLocation.latitude, lon2: venueLocation.longitude })
          : null;

        if (hasGeo && radiusKm && Number.isFinite(distanceKm) && distanceKm > radiusKm) continue;

        rows.push({
          vendorAuthId,
          serviceId: nextServiceId,
          businessName: svc?.businessName || null,
          serviceCategory: svc?.serviceCategory || null,
          name: svc?.name || null,
          tier: svc?.tier || null,
          price: Number(svc?.price || 0),
          description: svc?.description || null,
          latitude: venueLocation.latitude,
          longitude: venueLocation.longitude,
          location: venueLocation.name,
        });
      }

      alternatives = rows
        .filter((a) => a && a.vendorAuthId && a.serviceId)
        .sort((a, b) => (Number(a.price || 0) - Number(b.price || 0)))
        .slice(0, limit);
    } else {
      const byVendor = new Map();
      for (const svc of services) {
        const vendorAuthId = String(svc?.authId || '').trim();
        if (!vendorAuthId) continue;
        if (reservedVendorSet.has(vendorAuthId)) continue;

        const entry = byVendor.get(vendorAuthId) || {
          vendorAuthId,
          businessName: svc?.businessName || null,
          serviceCategory: svc?.serviceCategory || null,
          description: null,
          latitude: null,
          longitude: null,
          serviceId: null,
          name: null,
          tier: null,
          price: null,
          services: [],
        };

        const price = Number(svc?.price || 0);
        const serviceObj = {
          serviceId: svc?._id || null,
          name: svc?.name || null,
          tier: svc?.tier || null,
          price: Number.isFinite(price) && price > 0 ? price : null,
          description: svc?.description || null,
          details: svc?.details || null,
          rating: toFiniteNumberOrNull(svc?.rating),
          createdAt: svc?.createdAt || null,
        };

        entry.services.push(serviceObj);

        // Keep top-level fields aligned to the cheapest priced package (for collapsed card + distance)
        const isBetter = serviceObj.price != null && (
          entry.price == null || (Number.isFinite(Number(entry.price)) && serviceObj.price < Number(entry.price))
        );

        if (isBetter) {
          entry.serviceId = serviceObj.serviceId;
          entry.name = serviceObj.name;
          entry.tier = serviceObj.tier;
          entry.price = serviceObj.price;
          entry.description = serviceObj.description;
          entry.latitude = typeof svc?.latitude === 'number' ? svc.latitude : null;
          entry.longitude = typeof svc?.longitude === 'number' ? svc.longitude : null;
        }

        byVendor.set(vendorAuthId, entry);
      }

      alternatives = Array.from(byVendor.values())
        .filter((a) => a && a.vendorAuthId)
        .map((a) => ({
          ...a,
          services: Array.isArray(a.services)
            ? a.services
              .filter((s) => s && (s.serviceId || s.name || s.tier || s.price != null))
              .sort((x, y) => (Number(x?.price || 0) - Number(y?.price || 0)))
            : [],
        }))
        .sort((a, b) => (Number(a.price || 0) - Number(b.price || 0)))
        .slice(0, limit);
    }

    // Fallback: if vendors haven't created services yet, search vendor profiles directly
    if (alternatives.length === 0) {
      // Venue alternatives must be selected from concrete venue services (each with its own location).
      // Never fall back to vendor profiles for Venue.
      if (isVenue) {
        return res.status(200).json({
          success: true,
          data: {
            eventId: String(eventId || '').trim(),
            service,
            day,
            from,
            to,
            alternatives: [],
            vendorProfiles: [],
          },
        });
      }

      let vendorApps = await searchPublicVendors({
        serviceCategory: service,
        latitude: hasGeo ? lat : undefined,
        longitude: hasGeo ? lng : undefined,
        radiusKm,
        limit: Math.max(limit * 4, 20),
        skip: 0,
      });

      if (hasGeo && (!Array.isArray(vendorApps) || vendorApps.length === 0)) {
        logger.info('No vendors found with geo filter; retrying without geo', {
          eventId,
          service,
          latitude: lat,
          longitude: lng,
          radiusKm,
        });
        vendorApps = await searchPublicVendors({
          serviceCategory: service,
          limit: Math.max(limit * 4, 20),
          skip: 0,
        });
      }

      const fallback = (vendorApps || [])
        .map((v) => {
          const vendorAuthId = v?.authId != null ? String(v.authId).trim() : '';
          if (!vendorAuthId) return null;
          if (reservedVendorSet.has(vendorAuthId)) return null;

          const vLat = toNumberOrNull(v?.latitude);
          const vLon = toNumberOrNull(v?.longitude);
          const distanceKm = hasGeo && vLat != null && vLon != null
            ? haversineKm({ lat1: lat, lon1: lng, lat2: vLat, lon2: vLon })
            : null;
          return {
            vendorAuthId,
            serviceId: null,
            businessName: v?.businessName || null,
            serviceCategory: v?.serviceCategory || null,
            name: null,
            tier: null,
            price: null,
            description: v?.description || null,
            latitude: typeof v?.latitude === 'number' ? v.latitude : null,
            longitude: typeof v?.longitude === 'number' ? v.longitude : null,
            distanceKm,
            distanceText: formatDistance(distanceKm),
          };
        })
        .filter(Boolean)
        .slice(0, limit);

      return res.status(200).json({
        success: true,
        data: {
          eventId: String(eventId || '').trim(),
          service,
          day,
          from,
          to,
          alternatives: fallback,
          vendorProfiles: vendorApps,
        },
      });
    }

    const alternativesWithDistance = alternatives.map((a) => {
      const aLat = toNumberOrNull(a?.latitude);
      const aLon = toNumberOrNull(a?.longitude);
      const distanceKm = hasGeo && aLat != null && aLon != null
        ? haversineKm({ lat1: lat, lon1: lng, lat2: aLat, lon2: aLon })
        : null;

      return {
        ...a,
        distanceKm,
        distanceText: formatDistance(distanceKm),
      };
    });

    const vendorProfiles = await fetchPublicVendorsByAuthIds(alternatives.map((a) => a.vendorAuthId));

    return res.status(200).json({
      success: true,
      data: {
        eventId: String(eventId || '').trim(),
        service,
        day,
        from,
        to,
        alternatives: alternativesWithDistance,
        vendorProfiles,
      },
    });
  } catch (error) {
    logger.error('Error in listAlternativesForService:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

/**
 * GET /vendor/requests
 * Vendor-facing: list this vendor's event requests (grouped by eventId).
 */
const listVendorRequests = async (req, res) => {
  try {
    const vendorAuthId = normalizeVendorAuthId(req);

    const selections = await vendorSelectionService.listSelectionsForVendor({ vendorAuthId });
    const eventIds = selections.map((s) => s.eventId).filter(Boolean);
    const plannings = await Planning.find({ eventId: { $in: eventIds } })
      .select(
        'eventId authId eventTitle category eventType customEventType eventField eventDescription eventBanner schedule eventDate eventTime guestCount tickets location assignedManagerId status'
      )
      .lean();

    const planningByEventId = new Map(plannings.map((p) => [String(p.eventId), p]));

    const rows = selections
      .map((sel) => {
        const planning = planningByEventId.get(String(sel.eventId)) || null;
        const vendorItems = (sel.vendorItems || []).map((v) => ({
          service: v.service,
          status: v.status,
          rejectionReason: v.rejectionReason || null,
          alternativeNeeded: Boolean(v.alternativeNeeded),
          serviceId: v.serviceId || null,
          servicePrice: v.servicePrice || { min: 0, max: 0 },
          vendorQuotedPrice: v.vendorQuotedPrice ?? null,
          commissionPercent: v.commissionPercent ?? null,
          commissionAmount: v.commissionAmount ?? null,
          priceHikeReason: v.priceHikeReason || null,
          priceLocked: Boolean(v.priceLocked),
          pricingUnit: v.pricingUnit || null,
          pricingQuantity: v.pricingQuantity ?? null,
          pricingQuantityUnit: v.pricingQuantityUnit || null,
        }));

        const summary = summarizeVendorItems(vendorItems);
        const eventDate = planning?.eventDate || planning?.schedule?.startAt || null;
        return {
          eventId: sel.eventId,
          planningStatus: planning?.status || null,
          vendorSelectionId: sel._id,
          vendorSelectionStatus: sel.status,
          vendorsAccepted: Boolean(sel.vendorsAccepted),
          managerId: sel.managerId || planning?.assignedManagerId || null,
          managerAssigned: Boolean(sel.managerId || planning?.assignedManagerId),
          eventTitle: planning?.eventTitle || null,
          category: planning?.category || null,
          eventType: planning?.eventType || null,
          eventField: planning?.eventField || null,
          eventDescription: planning?.eventDescription || null,
          locationName: planning?.location?.name || null,
          eventDate,
          eventTime: planning?.eventTime || null,
          guestCount: planning?.guestCount ?? null,
          eventBannerUrl: planning?.eventBanner?.url || null,
          vendorItems,
          summary,
        };
      })
      .filter((row) => row.eventId);

    return res.status(200).json({
      success: true,
      data: {
        requests: rows,
      },
    });
  } catch (error) {
    logger.error('Error in listVendorRequests:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

/**
 * GET /vendor/requests/ledger
 * Vendor-facing: payout ledger across events.
 */
const listVendorPayoutLedger = async (req, res) => {
  try {
    const vendorAuthId = normalizeVendorAuthId(req);

    let payoutRows = [];
    try {
      payoutRows = await fetchVendorPayoutsForVendorFromOrderService({ vendorAuthId, user: req.user });
    } catch (payoutError) {
      logger.error('Failed to fetch vendor payout ledger from order-service', {
        vendorAuthId,
        message: payoutError?.message || String(payoutError),
      });
      throw createApiError(502, 'Unable to load vendor payout ledger right now');
    }

    const eventIds = Array.from(new Set(
      (Array.isArray(payoutRows) ? payoutRows : [])
        .map((row) => String(row?.eventId || '').trim())
        .filter(Boolean)
    ));

    const plannings = eventIds.length > 0
      ? await Planning.find({ eventId: { $in: eventIds } })
        .select('eventId eventTitle status eventDate schedule category')
        .lean()
      : [];
    const planningByEventId = new Map(plannings.map((row) => [String(row?.eventId || '').trim(), row]));

    const rows = (Array.isArray(payoutRows) ? payoutRows : [])
      .slice()
      .sort((a, b) => {
        const at = new Date(a?.paidAt || a?.createdAt || 0).getTime();
        const bt = new Date(b?.paidAt || b?.createdAt || 0).getTime();
        return bt - at;
      })
      .map((row, index) => {
        const normalizedEventId = String(row?.eventId || '').trim();
        const planning = planningByEventId.get(normalizedEventId) || null;
        const paidAt = row?.paidAt || row?.createdAt || null;
        const mode = String(row?.payoutMode || 'DEMO').trim().toUpperCase() === 'RAZORPAY'
          ? 'RAZORPAY'
          : 'DEMO';
        const amountInr = toInrFromPaise(row?.payoutAmountPaise);
        const status = String(row?.status || '').trim().toUpperCase();

        return {
          id: String(row?.payoutId || `ledger-${index + 1}`),
          payoutId: row?.payoutId || null,
          eventId: normalizedEventId,
          eventTitle: planning?.eventTitle || `Event ${normalizedEventId || 'Unknown'}`,
          eventDate: planning?.eventDate || planning?.schedule?.startAt || null,
          planningStatus: planning?.status || null,
          service: row?.service || null,
          status,
          amountInr,
          currency: row?.currency || 'INR',
          payoutMode: mode,
          paidAt,
          dateLabel: toDateLabel(paidAt),
          managerAuthId: row?.managerAuthId || null,
          sourcePaymentId: row?.sourcePaymentId || null,
          razorpayTransferId: row?.razorpayTransferId || null,
        };
      });

    const successfulRows = rows.filter((row) => row.status === 'SUCCESS');
    const totalReceived = Number(successfulRows.reduce((sum, row) => sum + Number(row.amountInr || 0), 0).toFixed(2));

    return res.status(200).json({
      success: true,
      data: {
        vendorAuthId,
        rows,
        summary: {
          totalRows: rows.length,
          successfulPayoutCount: successfulRows.length,
          totalReceived,
          currency: 'INR',
        },
      },
    });
  } catch (error) {
    logger.error('Error in listVendorPayoutLedger:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

/**
 * GET /vendor/requests/:eventId
 * Vendor-facing: detailed view for this vendor for a given eventId.
 */
const getVendorRequestDetails = async (req, res) => {
  try {
    const vendorAuthId = normalizeVendorAuthId(req);
    const eventId = normalizeEventIdParam(req.params.eventId);

    const selection = await vendorSelectionService.getSelectionForVendorEvent({ eventId, vendorAuthId });
    const planning = await Planning.findOne({ eventId })
      .select(
        'eventId authId eventTitle category eventType customEventType eventField eventDescription eventBanner schedule eventDate eventTime guestCount tickets location assignedManagerId status'
      )
      .lean();

    let managerProfile = null;
    const managerId = selection?.managerId || planning?.assignedManagerId || null;
    if (managerId) {
      try {
        managerProfile = await fetchUserById(managerId);
      } catch (e) {
        logger.warn('Failed to fetch manager profile for vendor request', { eventId, managerId: String(managerId) });
      }
    }

    const extractReservationServiceId = (reservation) => {
      const direct = reservation?.serviceId != null ? String(reservation.serviceId).trim() : '';
      if (direct) return direct;

      const lockId = String(reservation?.vendorAuthId || '').trim();
      if (lockId.startsWith('service:')) {
        const parsed = String(lockId.slice('service:'.length) || '').trim();
        return parsed || null;
      }

      return null;
    };

    const planningDays = vendorReservationService.planningToReservationDays(planning);
    const rawVendorItems = Array.isArray(selection?.vendorItems) ? selection.vendorItems : [];
    const hasMissingServiceId = rawVendorItems.some((v) => !String(v?.serviceId || '').trim());

    const recoveredServiceIdByService = new Map();
    if (hasMissingServiceId && planningDays.length > 0) {
      try {
        const activeReservations = await vendorReservationService.listActiveReservationsForEventDays({
          eventId,
          days: planningDays,
        });

        const idsByService = new Map();
        for (const row of (Array.isArray(activeReservations) ? activeReservations : [])) {
          const rowVendorAuthId = String(row?.ownerVendorAuthId || row?.vendorAuthId || '').trim();
          if (!rowVendorAuthId || rowVendorAuthId !== vendorAuthId) continue;

          const normalizedService = vendorSelectionService.canonicalizeService(row?.service);
          if (!normalizedService) continue;

          const recoveredId = String(extractReservationServiceId(row) || '').trim();
          if (!recoveredId) continue;

          const key = `${normalizedService}::${rowVendorAuthId}`;
          const set = idsByService.get(key) || new Set();
          set.add(recoveredId);
          idsByService.set(key, set);
        }

        for (const [key, ids] of idsByService.entries()) {
          if (ids.size === 1) {
            recoveredServiceIdByService.set(key, Array.from(ids)[0]);
          }
        }
      } catch (repairError) {
        logger.warn('Failed to recover missing serviceId for vendor request details', {
          eventId,
          vendorAuthId,
          error: repairError?.message || String(repairError),
        });
      }
    }

    // Persist recovered serviceIds so legacy rows are healed permanently.
    if (recoveredServiceIdByService.size > 0) {
      try {
        const updates = rawVendorItems
          .map((v) => {
            const normalizedService = vendorSelectionService.canonicalizeService(v?.service);
            if (!normalizedService) return null;

            const directServiceId = v?.serviceId != null ? String(v.serviceId).trim() : '';
            if (directServiceId) return null;

            const recoveredServiceId = String(recoveredServiceIdByService.get(`${normalizedService}::${vendorAuthId}`) || '').trim();
            if (!recoveredServiceId) return null;

            return {
              service: normalizedService,
              serviceId: recoveredServiceId,
            };
          })
          .filter(Boolean);

        if (updates.length > 0) {
          const VendorSelection = require('../models/VendorSelection');
          await Promise.all(
            updates.map((entry) =>
              VendorSelection.updateOne(
                {
                  eventId,
                  vendors: {
                    $elemMatch: {
                      service: entry.service,
                      vendorAuthId,
                      $or: [{ serviceId: null }, { serviceId: '' }],
                    },
                  },
                },
                {
                  $set: {
                    'vendors.$.serviceId': entry.serviceId,
                  },
                }
              )
            )
          );
        }
      } catch (persistError) {
        logger.warn('Failed to persist recovered serviceId for vendor request details', {
          eventId,
          vendorAuthId,
          error: persistError?.message || String(persistError),
        });
      }
    }

    let commissionRateByService = new Map();
    let vendorHikeRate = commissionService.DEFAULT_VENDOR_HIKE_RATE;
    try {
      const cfg = await commissionService.getCommissionConfig();
      const hikeRateRaw = Number(cfg?.vendorHikeRate);
      vendorHikeRate = Number.isFinite(hikeRateRaw) && hikeRateRaw >= 1
        ? hikeRateRaw
        : commissionService.DEFAULT_VENDOR_HIKE_RATE;
      commissionRateByService = new Map(
        Object.entries(cfg?.rates || {}).map(([serviceName, percent]) => {
          const normalized = vendorSelectionService.canonicalizeService(serviceName) || String(serviceName || '').trim();
          const pct = Number(percent);
          return [normalized, Number.isFinite(pct) && pct >= 0 ? pct : 0];
        })
      );
    } catch (e) {
      logger.warn('Failed to load commission config while hydrating vendor request details', {
        eventId,
        vendorAuthId,
        message: e?.message || String(e),
      });
    }

    const pricingRepairUpdates = [];
    const vendorItems = rawVendorItems.map((v) => {
      const normalizedService = vendorSelectionService.canonicalizeService(v?.service);
      const directServiceId = v?.serviceId != null ? String(v.serviceId).trim() : '';
      const recoveredServiceId = normalizedService
        ? String(recoveredServiceIdByService.get(`${normalizedService}::${vendorAuthId}`) || '').trim()
        : '';

      const servicePriceMinRaw = Number(v?.servicePrice?.min || 0);
      const servicePriceMaxRaw = Number(v?.servicePrice?.max || 0);
      const servicePriceMin = Number.isFinite(servicePriceMinRaw) && servicePriceMinRaw > 0
        ? Math.round(servicePriceMinRaw * 100) / 100
        : 0;
      const servicePriceMaxBase = Number.isFinite(servicePriceMaxRaw) && servicePriceMaxRaw > 0
        ? Math.round(servicePriceMaxRaw * 100) / 100
        : servicePriceMin;

      let vendorQuotedPrice = (() => {
        const raw = Number(v?.vendorQuotedPrice);
        return Number.isFinite(raw) && raw > 0 ? Math.round(raw * 100) / 100 : null;
      })();

      let commissionPercent = (() => {
        const raw = Number(v?.commissionPercent);
        return Number.isFinite(raw) && raw >= 0 && raw <= 100 ? raw : null;
      })();

      let commissionAmount = (() => {
        const raw = Number(v?.commissionAmount);
        return Number.isFinite(raw) && raw >= 0 ? Math.round(raw * 100) / 100 : null;
      })();

      const commissionRate = normalizedService
        ? Number(commissionRateByService.get(normalizedService))
        : 0;
      const safeCommissionRate = Number.isFinite(commissionRate) && commissionRate >= 0 ? commissionRate : 0;

      const lockedPriceCandidate = servicePriceMin > 0 ? servicePriceMin : 0;

      // Commission-inclusive rule:
      // vendorQuotedPrice represents final client payable amount.
      // Commission is a split inside this amount, not an add-on.
      if (vendorQuotedPrice == null && lockedPriceCandidate > 0) {
        vendorQuotedPrice = toMoneyOrNull(lockedPriceCandidate);
      }

      if (commissionPercent == null) {
        commissionPercent = safeCommissionRate;
      }

      if (commissionAmount == null && vendorQuotedPrice != null && commissionPercent != null) {
        commissionAmount = toMoneyOrNull((vendorQuotedPrice * commissionPercent) / 100);
      }

      // Legacy fallback for old additive records where locked total may have been persisted.
      if (commissionAmount == null && vendorQuotedPrice != null && lockedPriceCandidate > vendorQuotedPrice) {
        commissionAmount = toMoneyOrNull(Math.max(0, lockedPriceCandidate - vendorQuotedPrice));
      }

      if (commissionPercent == null && vendorQuotedPrice != null && commissionAmount != null && vendorQuotedPrice > 0) {
        commissionPercent = Number(((commissionAmount / vendorQuotedPrice) * 100).toFixed(2));
      }

      if (commissionPercent == null) commissionPercent = 0;
      if (commissionAmount == null) commissionAmount = 0;

      const isAccepted = String(v?.status || '').trim() === VENDOR_STATUS.ACCEPTED;
      let priceLocked = Boolean(v?.priceLocked);

      if (!priceLocked && (isAccepted && vendorQuotedPrice != null && vendorQuotedPrice > 0)) {
        priceLocked = true;
      }

      if (
        isAccepted &&
        (
          !Boolean(v?.priceLocked) ||
          v?.vendorQuotedPrice == null ||
          v?.commissionAmount == null ||
          v?.commissionPercent == null
        )
      ) {
        pricingRepairUpdates.push({
          service: normalizedService || String(v?.service || '').trim(),
          serviceId: directServiceId || recoveredServiceId || null,
          vendorQuotedPrice,
          commissionPercent,
          commissionAmount,
          priceLocked,
        });
      }

      return {
        service: v.service,
        status: v.status,
        rejectionReason: v.rejectionReason || null,
        alternativeNeeded: Boolean(v.alternativeNeeded),
        serviceId: directServiceId || recoveredServiceId || null,
        servicePrice: {
          min: servicePriceMin,
          max: servicePriceMaxBase,
        },
        vendorQuotedPrice: vendorQuotedPrice ?? null,
        commissionPercent: commissionPercent ?? null,
        commissionAmount: commissionAmount ?? null,
        priceHikeReason: v.priceHikeReason || null,
        priceLocked,
        pricingUnit: v.pricingUnit || null,
        pricingQuantity: v.pricingQuantity ?? null,
        pricingQuantityUnit: v.pricingQuantityUnit || null,
      };
    });

    if (pricingRepairUpdates.length > 0) {
      try {
        const VendorSelection = require('../models/VendorSelection');
        await Promise.all(
          pricingRepairUpdates.map((entry) => {
            const match = {
              service: entry.service,
              vendorAuthId,
            };
            if (entry.serviceId) {
              match.serviceId = entry.serviceId;
            }

            return VendorSelection.updateOne(
              {
                eventId,
                vendors: {
                  $elemMatch: match,
                },
              },
              {
                $set: {
                  'vendors.$.vendorQuotedPrice': entry.vendorQuotedPrice,
                  'vendors.$.commissionPercent': entry.commissionPercent,
                  'vendors.$.commissionAmount': entry.commissionAmount,
                  'vendors.$.priceLocked': entry.priceLocked,
                },
              }
            );
          })
        );
      } catch (repairError) {
        logger.warn('Failed to persist vendor pricing repair while hydrating vendor request details', {
          eventId,
          vendorAuthId,
          message: repairError?.message || String(repairError),
        });
      }
    }

    let eventPayoutRows = [];
    try {
      eventPayoutRows = await fetchVendorPayoutsForEventFromOrderService({ eventId, user: req.user });
    } catch (payoutError) {
      logger.warn('Failed to fetch event payouts while hydrating vendor request details', {
        eventId,
        vendorAuthId,
        message: payoutError?.message || String(payoutError),
      });
    }

    const successfulVendorPayouts = (Array.isArray(eventPayoutRows) ? eventPayoutRows : [])
      .filter((row) => String(row?.vendorAuthId || '').trim() === vendorAuthId)
      .filter((row) => String(row?.status || '').trim().toUpperCase() === 'SUCCESS');

    const amountReceived = successfulVendorPayouts.reduce(
      (sum, row) => sum + toInrFromPaise(row?.payoutAmountPaise),
      0
    );

    const payoutModeCounts = successfulVendorPayouts.reduce((acc, row) => {
      const mode = String(row?.payoutMode || 'DEMO').trim().toUpperCase() === 'RAZORPAY'
        ? 'RAZORPAY'
        : 'DEMO';
      acc[mode] = (acc[mode] || 0) + 1;
      return acc;
    }, {});

    const ledgerEntries = successfulVendorPayouts
      .slice()
      .sort((a, b) => {
        const at = new Date(a?.paidAt || a?.createdAt || 0).getTime();
        const bt = new Date(b?.paidAt || b?.createdAt || 0).getTime();
        return bt - at;
      })
      .map((row) => {
        const paidAt = row?.paidAt || row?.createdAt || null;
        const mode = String(row?.payoutMode || 'DEMO').trim().toUpperCase() === 'RAZORPAY'
          ? 'RAZORPAY'
          : 'DEMO';
        const amountInr = toInrFromPaise(row?.payoutAmountPaise);

        return {
          id: String(row?.payoutId || `${eventId}:${row?.service || 'service'}`),
          payoutId: row?.payoutId || null,
          eventId,
          eventTitle: planning?.eventTitle || null,
          dateLabel: toDateLabel(paidAt),
          date: paidAt,
          description: `${row?.service || 'Service'} payout received`,
          status: 'RECEIVED',
          type: 'Credit',
          signedAmount: amountInr,
          amountInr,
          payoutMode: mode,
          service: row?.service || null,
          currency: row?.currency || 'INR',
          managerAuthId: row?.managerAuthId || null,
          razorpayTransferId: row?.razorpayTransferId || null,
          sourcePaymentId: row?.sourcePaymentId || null,
        };
      });

    const baseSummary = summarizeVendorItems(vendorItems);

    return res.status(200).json({
      success: true,
      data: {
        eventId,
        planning: planning || null,
        vendorSelection: {
          _id: selection._id,
          status: selection.status,
          vendorsAccepted: Boolean(selection.vendorsAccepted),
          managerId: selection.managerId || null,
          managerAssigned: Boolean(selection.managerId),
        },
        managerProfile,
        pricingConfig: {
          vendorHikeRate,
        },
        vendorItems,
        ledgerEntries,
        payment: {
          amountReceived,
          payoutCount: successfulVendorPayouts.length,
          payoutModeCounts,
          lastPaidAt: successfulVendorPayouts[0]?.paidAt || successfulVendorPayouts[0]?.createdAt || null,
        },
        summary: {
          ...baseSummary,
          amountReceived,
          payoutCount: successfulVendorPayouts.length,
          payoutModeCounts,
        },
      },
    });
  } catch (error) {
    logger.error('Error in getVendorRequestDetails:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

const lockVendorServicePrice = async (req, res) => {
  try {
    const vendorAuthId = normalizeVendorAuthId(req);
    const eventId = normalizeEventIdParam(req.params.eventId);
    const { service, price, priceHikeReason } = req.body || {};

    const result = await vendorSelectionService.lockPriceForVendor({
      eventId,
      vendorAuthId,
      service,
      quotedPrice: price,
      priceHikeReason,
    });

    const vendorItems = (result?.selection?.vendors || [])
      .filter((v) => String(v?.vendorAuthId || '').trim() === vendorAuthId)
      .map((v) => ({
        service: v.service,
        status: v.status,
        rejectionReason: v.rejectionReason || null,
        alternativeNeeded: Boolean(v.alternativeNeeded),
        serviceId: v.serviceId || null,
        servicePrice: v.servicePrice || { min: 0, max: 0 },
        vendorQuotedPrice: v.vendorQuotedPrice ?? null,
        commissionPercent: v.commissionPercent ?? null,
        commissionAmount: v.commissionAmount ?? null,
        priceHikeReason: v.priceHikeReason || null,
        priceLocked: Boolean(v.priceLocked),
        pricingUnit: v.pricingUnit || null,
        pricingQuantity: v.pricingQuantity ?? null,
        pricingQuantityUnit: v.pricingQuantityUnit || null,
      }));

    return res.status(200).json({
      success: true,
      message: 'Price locked successfully',
      data: {
        eventId,
        service: result.service,
        quotedPrice: result.quotedPrice,
        commissionPercent: result.commissionPercent,
        commissionAmount: result.commissionAmount,
        lockedPrice: result.lockedPrice,
        priceHikeReason: result.priceHikeReason || null,
        vendorHikeRate: result.vendorHikeRate,
        vendorItems,
        summary: summarizeVendorItems(vendorItems),
      },
    });
  } catch (error) {
    logger.error('Error in lockVendorServicePrice:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

const acceptVendorRequest = async (req, res) => {
  try {
    const vendorAuthId = normalizeVendorAuthId(req);
    const eventId = normalizeEventIdParam(req.params.eventId);

    const { service } = req.body || {};
    const { selection } = await vendorSelectionService.respondForVendor({
      eventId,
      vendorAuthId,
      action: 'accept',
      service,
    });

    // Business rule: if Venue vendor accepts and a concrete venue serviceId was selected,
    // update Planning.location to the venue's location (best-effort).
    try {
      const isVenueService = (value) => {
        const canonical = vendorSelectionService.canonicalizeService(value);
        return String(canonical || '').trim() === 'Venue';
      };

      const venueItem = (selection?.vendors || []).find(
        (v) =>
          isVenueService(v?.service) &&
          String(v?.vendorAuthId || '').trim() === vendorAuthId &&
          v?.status === VENDOR_STATUS.ACCEPTED &&
          v?.serviceId
      );

      if (venueItem?.serviceId) {
        const venueService = await fetchPublicServiceById(venueItem.serviceId);
        const venueLocation = computeVenueLocationFromService(venueService);
        if (venueLocation) {
          await Planning.updateOne(
            { eventId },
            {
              $set: {
                location: venueLocation,
              },
            }
          );
        }
      }
    } catch (e) {
      logger.warn('Failed to update planning location from accepted venue service', {
        eventId,
        vendorAuthId,
        error: e?.message,
      });
    }

    const planning = await Planning.findOne({ eventId }).select('status eventId authId assignedManagerId eventTitle').lean();
    let planningStatusUpdated = false;
    let nextPlanningStatus = planning?.status || null;

    if (selection?.vendorsAccepted && planning?.status !== PLANNING_STATUS.APPROVED) {
      await Planning.updateOne({ eventId }, { $set: { status: PLANNING_STATUS.APPROVED } });
      planningStatusUpdated = true;
      nextPlanningStatus = PLANNING_STATUS.APPROVED;

      try {
        await planningQuoteService.lockQuoteAtApproved({ eventId, lockedByAuthId: vendorAuthId });
      } catch (err) {
        logger.warn('Failed to lock quote after vendors accepted', {
          eventId,
          message: err?.message,
        });
      }
    }

    const progress = summarizeVendorItems(Array.isArray(selection?.vendors) ? selection.vendors : []);
    try {
      await publishEvent('VENDOR_REQUEST_ACCEPTED', {
        eventId: String(eventId || '').trim(),
        authId: String(planning?.authId || '').trim() || null,
        managerAuthId: String(planning?.assignedManagerId || '').trim() || null,
        vendorAuthId,
        service: service != null ? String(service).trim() : null,
        eventTitle: String(planning?.eventTitle || '').trim() || null,
        progress: {
          ...progress,
          vendorsAccepted: Boolean(selection?.vendorsAccepted),
        },
        planningStatus: nextPlanningStatus || null,
        occurredAt: new Date().toISOString(),
      });
    } catch (publishError) {
      logger.warn('Failed to publish VENDOR_REQUEST_ACCEPTED', {
        eventId,
        vendorAuthId,
        service: service || null,
        message: publishError?.message || String(publishError),
      });
    }

    return res.status(200).json({
      success: true,
      message: 'Request accepted',
      data: {
        eventId,
        vendorsAccepted: Boolean(selection?.vendorsAccepted),
        vendorSelectionStatus: selection?.status,
        planningStatus: nextPlanningStatus,
        planningStatusUpdated,
      },
    });
  } catch (error) {
    logger.error('Error in acceptVendorRequest:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

const rejectVendorRequest = async (req, res) => {
  try {
    const vendorAuthId = normalizeVendorAuthId(req);
    const eventId = normalizeEventIdParam(req.params.eventId);

    const { service: rawService, reason } = req.body || {};
    const rejectionReason = reason != null ? String(reason).trim() : '';
    if (!rejectionReason) {
      throw createApiError(400, 'reason is required');
    }

    const service = rawService != null && String(rawService).trim()
      ? vendorSelectionService.canonicalizeService(String(rawService).trim())
      : null;

    const planning = await Planning.findOne({ eventId })
      .select('_id status eventId authId assignedManagerId selectedServices eventTitle category eventType customEventType eventDate schedule location')
      .lean();
    const planningDays = vendorReservationService.planningToReservationDays(planning);
    const planningDay = planningDays[0] || normalizeIstDayInput(planning?.eventDate);

    const { selection, vendorAcceptedAnyServiceAfter, transitionedRejectedServices } = await vendorSelectionService.respondForVendor({
      eventId,
      vendorAuthId,
      action: 'reject',
      service,
      rejectionReason,
    });

    logger.info('Vendor request rejected', {
      eventId,
      vendorAuthId,
      service: service || null,
      transitionedRejectedServices: Array.isArray(transitionedRejectedServices) ? transitionedRejectedServices : [],
    });

    let planningStatusUpdated = false;
    let nextPlanningStatus = planning?.status || null;

    // Business rule: event is APPROVED only when all vendors have accepted.
    // If a vendor rejects after approval, move it back to PENDING_APPROVAL.
    if (!selection?.vendorsAccepted && planning?.status === PLANNING_STATUS.APPROVED) {
      await Planning.updateOne({ eventId }, { $set: { status: PLANNING_STATUS.PENDING_APPROVAL } });
      planningStatusUpdated = true;
      nextPlanningStatus = PLANNING_STATUS.PENDING_APPROVAL;
    }

    const rejectedServices = Array.isArray(transitionedRejectedServices) && transitionedRejectedServices.length > 0
      ? transitionedRejectedServices
      : (service ? [service] : []);
    const progress = summarizeVendorItems(Array.isArray(selection?.vendors) ? selection.vendors : []);
    for (const rejectedService of rejectedServices) {
      try {
        await publishEvent('VENDOR_REQUEST_REJECTED', {
          eventId: String(eventId || '').trim(),
          authId: String(planning?.authId || '').trim() || null,
          managerAuthId: String(planning?.assignedManagerId || '').trim() || null,
          vendorAuthId,
          service: rejectedService,
          rejectionReason,
          eventTitle: String(planning?.eventTitle || '').trim() || null,
          progress: {
            ...progress,
            vendorsAccepted: Boolean(selection?.vendorsAccepted),
          },
          planningStatus: nextPlanningStatus || null,
          occurredAt: new Date().toISOString(),
        });
      } catch (publishError) {
        logger.warn('Failed to publish VENDOR_REQUEST_REJECTED', {
          eventId,
          vendorAuthId,
          service: rejectedService,
          message: publishError?.message || String(publishError),
        });
      }
    }

    // If vendor is no longer participating in any service for this event, release reservation(s) (best-effort).
    // Venue locks are service-level, so release per serviceId.
    if (!vendorAcceptedAnyServiceAfter && planningDays.length > 0) {
      const vendorItems = Array.isArray(selection?.vendors)
        ? selection.vendors.filter((v) => String(v?.vendorAuthId || '').trim() === vendorAuthId)
        : [];

      if (vendorItems.length > 0) {
        await Promise.all(
          vendorItems.map((item) =>
            vendorReservationService
              .releaseForDays({
                vendorAuthId,
                days: planningDays,
                eventId,
                service: item?.service,
                serviceId: item?.serviceId,
              })
              .catch((e) => {
                logger.warn('Failed to release vendor reservation after rejection', {
                  eventId,
                  vendorAuthId,
                  service: item?.service || null,
                  serviceId: item?.serviceId || null,
                  error: e.message,
                });
              })
          )
        );
      } else {
        vendorReservationService
          .releaseForDays({ vendorAuthId, days: planningDays, eventId })
          .catch((e) => logger.warn('Failed to release vendor reservation after rejection', { eventId, vendorAuthId, error: e.message }));
      }
    }

    // Best-effort automation: when a vendor rejects service(s), send alternatives to client via chat + email.
    // IMPORTANT: Never block the vendor rejection response on upstream calls.
    const servicesToAutoSend = Array.isArray(transitionedRejectedServices)
      ? transitionedRejectedServices
      : [];

    if (planningDay && servicesToAutoSend.length > 0) {
      (async () => {
        const managerAuthId = planning?.assignedManagerId != null ? String(planning.assignedManagerId).trim() : '';
        const userAuthId = planning?.authId != null ? String(planning.authId).trim() : '';

        if (!managerAuthId || !userAuthId) {
          logger.warn('Skipping auto alternatives send on rejection (missing manager/user)', {
            eventId,
            servicesToAutoSend,
            managerAuthId: Boolean(managerAuthId),
            userAuthId: Boolean(userAuthId),
          });
          return;
        }

        // Ensure both participants exist once.
        await ensureEventChatSeeded({ eventId, userAuthId, managerAuthId });

        // Find alternatives (reuse same logic as GET /vendor-selection/:eventId/alternatives).
        const selectionDoc = await vendorSelectionService.ensureForPlanning(planning);

        const reservedVendorByOthers = await vendorReservationService.listReservedVendorAuthIdsForDay({
          day: planningDay,
          excludeEventId: eventId,
        });
        const reservedServiceByOthers = await vendorReservationService.listReservedServiceIdsForDay({
          day: planningDay,
          excludeEventId: eventId,
        });

        const lat = planning?.location?.latitude;
        const lng = planning?.location?.longitude;
        const hasGeo = typeof lat === 'number' && typeof lng === 'number' && Number.isFinite(lat) && Number.isFinite(lng);
        const radiusKm = hasGeo ? ALTERNATIVES_RADIUS_KM : undefined;
        const baseReservedVendorSet = new Set((reservedVendorByOthers || []).map((v) => String(v || '').trim()).filter(Boolean));
        const baseReservedServiceSet = new Set((reservedServiceByOthers || []).map((v) => String(v || '').trim()).filter(Boolean));

        for (const rejectedService of servicesToAutoSend) {
          const serviceLabel = vendorSelectionService.canonicalizeService(String(rejectedService));
          const isVenue = serviceLabel === 'Venue';

          const currentVendorForService = Array.isArray(selectionDoc?.vendors)
            ? selectionDoc.vendors.find((v) => v?.service === serviceLabel)
            : null;

          const currentVendorAuthId = currentVendorForService?.vendorAuthId != null
            ? String(currentVendorForService.vendorAuthId).trim()
            : '';

          const currentServiceId = currentVendorForService?.serviceId != null
            ? String(currentVendorForService.serviceId).trim()
            : '';

          const reservedVendorSet = new Set(baseReservedVendorSet);
          const reservedServiceSet = new Set(baseReservedServiceSet);

          if (currentVendorAuthId) reservedVendorSet.add(currentVendorAuthId);
          if (currentServiceId) reservedServiceSet.add(currentServiceId);

          const limit = 50;
          let services = await searchPublicVendorServices({
            serviceCategory: serviceLabel,
            latitude: hasGeo ? lat : undefined,
            longitude: hasGeo ? lng : undefined,
            radiusKm,
            limit: Math.max(limit * 4, 20),
            skip: 0,
          });

          // For Venue alternatives, do not drop geo constraints, otherwise we can suggest venues in other cities.
          if (!isVenue && hasGeo && (!Array.isArray(services) || services.length === 0)) {
            logger.info('No vendor services found with geo filter for rejection auto-send; retrying without geo', {
              eventId,
              service: serviceLabel,
              latitude: lat,
              longitude: lng,
              radiusKm,
            });
            services = await searchPublicVendorServices({
              serviceCategory: serviceLabel,
              limit: Math.max(limit * 4, 20),
              skip: 0,
            });
          }

          const buildAlternatives = () => {
            if (isVenue) {
              const rows = [];
              for (const svc of services) {
                const nextVendorAuthId = String(svc?.authId || '').trim();
                if (!nextVendorAuthId) continue;
                const nextServiceId = svc?._id != null ? String(svc._id).trim() : '';
                if (!nextServiceId) continue;
                if (reservedServiceSet.has(nextServiceId)) continue;

                const venueLocation = computeVenueLocationFromService(svc);
                if (!venueLocation) continue;

                const distanceKm = hasGeo
                  ? haversineKm({ lat1: lat, lon1: lng, lat2: venueLocation.latitude, lon2: venueLocation.longitude })
                  : null;

                if (hasGeo && radiusKm && Number.isFinite(distanceKm) && distanceKm > radiusKm) continue;

                rows.push({
                  vendorAuthId: nextVendorAuthId,
                  serviceId: nextServiceId,
                  businessName: svc?.businessName || null,
                  serviceCategory: svc?.serviceCategory || null,
                  name: svc?.name || null,
                  tier: svc?.tier || null,
                  price: Number(svc?.price || 0),
                  description: svc?.description || null,
                  latitude: venueLocation.latitude,
                  longitude: venueLocation.longitude,
                  location: venueLocation.name,
                });
              }

              return rows
                .filter((a) => a && a.vendorAuthId && a.serviceId)
                .sort((a, b) => (Number(a.price || 0) - Number(b.price || 0)))
                .slice(0, limit);
            }

            const byVendor = new Map();
            for (const svc of services) {
              const nextVendorAuthId = String(svc?.authId || '').trim();
              if (!nextVendorAuthId) continue;
              if (reservedVendorSet.has(nextVendorAuthId)) continue;

              const entry = byVendor.get(nextVendorAuthId) || {
                vendorAuthId: nextVendorAuthId,
                businessName: svc?.businessName || null,
                serviceCategory: svc?.serviceCategory || null,
                description: null,
                latitude: null,
                longitude: null,
                serviceId: null,
                name: null,
                tier: null,
                price: null,
                services: [],
              };

              const price = Number(svc?.price || 0);
              const serviceObj = {
                serviceId: svc?._id || null,
                name: svc?.name || null,
                tier: svc?.tier || null,
                price: Number.isFinite(price) && price > 0 ? price : null,
                description: svc?.description || null,
                details: svc?.details || null,
                rating: toFiniteNumberOrNull(svc?.rating),
                createdAt: svc?.createdAt || null,
              };

              entry.services.push(serviceObj);

              const isBetter = serviceObj.price != null && (
                entry.price == null || (Number.isFinite(Number(entry.price)) && serviceObj.price < Number(entry.price))
              );

              if (isBetter) {
                entry.serviceId = serviceObj.serviceId;
                entry.name = serviceObj.name;
                entry.tier = serviceObj.tier;
                entry.price = serviceObj.price;
                entry.description = serviceObj.description;
                entry.latitude = typeof svc?.latitude === 'number' ? svc.latitude : null;
                entry.longitude = typeof svc?.longitude === 'number' ? svc.longitude : null;
              }

              byVendor.set(nextVendorAuthId, entry);
            }

            return Array.from(byVendor.values())
              .filter((a) => a && a.vendorAuthId)
              .map((a) => ({
                ...a,
                services: Array.isArray(a.services)
                  ? a.services
                    .filter((s) => s && (s.serviceId || s.name || s.tier || s.price != null))
                    .sort((x, y) => (Number(x?.price || 0) - Number(y?.price || 0)))
                  : [],
              }))
              .sort((a, b) => (Number(a.price || 0) - Number(b.price || 0)))
              .slice(0, limit);
          };

          let alternatives = buildAlternatives();

          let vendorProfiles = [];
          if (alternatives.length === 0) {
            if (isVenue) {
              logger.info('No Venue alternatives available to auto-send after rejection', {
                eventId,
                service: serviceLabel,
                hasGeo,
                radiusKm: radiusKm || null,
              });
              continue;
            }

            let vendorApps = await searchPublicVendors({
              serviceCategory: serviceLabel,
              latitude: hasGeo ? lat : undefined,
              longitude: hasGeo ? lng : undefined,
              radiusKm,
              limit: Math.max(limit * 4, 20),
              skip: 0,
            });

            if (hasGeo && (!Array.isArray(vendorApps) || vendorApps.length === 0)) {
              logger.info('No vendors found with geo filter for rejection auto-send; retrying without geo', {
                eventId,
                service: serviceLabel,
                latitude: lat,
                longitude: lng,
                radiusKm,
              });
              vendorApps = await searchPublicVendors({
                serviceCategory: serviceLabel,
                limit: Math.max(limit * 4, 20),
                skip: 0,
              });
            }

            vendorProfiles = Array.isArray(vendorApps) ? vendorApps : [];
            alternatives = vendorProfiles
              .map((v) => {
                const nextVendorAuthId = v?.authId != null ? String(v.authId).trim() : '';
                if (!nextVendorAuthId) return null;
                if (reservedVendorSet.has(nextVendorAuthId)) return null;
                return {
                  vendorAuthId: nextVendorAuthId,
                  serviceId: null,
                  businessName: v?.businessName || null,
                  serviceCategory: v?.serviceCategory || null,
                  name: null,
                  tier: null,
                  price: null,
                  description: v?.description || null,
                  latitude: typeof v?.latitude === 'number' ? v.latitude : null,
                  longitude: typeof v?.longitude === 'number' ? v.longitude : null,
                };
              })
              .filter(Boolean)
              .slice(0, limit);
          } else {
            vendorProfiles = await fetchPublicVendorsByAuthIds(alternatives.map((a) => a.vendorAuthId));
          }

          const profileByAuthId = new Map(
            (Array.isArray(vendorProfiles) ? vendorProfiles : [])
              .map((p) => [String(p?.authId || '').trim(), p])
              .filter(([k]) => Boolean(k))
          );

          const options = (Array.isArray(alternatives) ? alternatives : [])
            .slice(0, 10)
            .map((a) => {
              const optVendorAuthId = String(a?.vendorAuthId || a?.authId || '').trim();
              const profile = optVendorAuthId ? profileByAuthId.get(optVendorAuthId) : null;

              const prices = Array.isArray(a?.services)
                ? a.services
                  .map((s) => Number(s?.price))
                  .filter((p) => Number.isFinite(p) && p > 0)
                : [];

              const derivedPriceMin = prices.length > 0 ? prices.reduce((m, p) => (p < m ? p : m), prices[0]) : null;
              const derivedPriceMax = prices.length > 0 ? prices.reduce((m, p) => (p > m ? p : m), prices[0]) : null;

              const aLat = toNumberOrNull(a?.latitude);
              const aLon = toNumberOrNull(a?.longitude);
              const distanceKm = hasGeo && aLat != null && aLon != null
                ? haversineKm({ lat1: lat, lon1: lng, lat2: aLat, lon2: aLon })
                : null;

              return {
                vendorAuthId: optVendorAuthId || null,
                serviceId: a?.serviceId || null,
                name: a?.name || null,
                businessName: a?.businessName || profile?.businessName || 'Vendor',
                tier: a?.tier || null,
                price: Number(a?.price || 0) || null,
                priceMin: derivedPriceMin,
                priceMax: derivedPriceMax,
                services: Array.isArray(a?.services) ? a.services : [],
                serviceCategory: a?.serviceCategory || profile?.serviceCategory || null,
                location: a?.location || profile?.location || profile?.place || null,
                country: profile?.country || null,
                description: profile?.description || a?.description || null,
                distanceKm,
                distanceText: formatDistance(distanceKm),
              };
            })
            .filter((o) => o.vendorAuthId);

          if (options.length === 0) {
            logger.info('No alternatives available to auto-send after rejection', {
              eventId,
              service: serviceLabel,
              hasGeo,
              radiusKm: radiusKm || null,
              reservedByOthersCount: Array.isArray(reservedVendorByOthers) ? reservedVendorByOthers.length : 0,
              reservedSetCount: isVenue ? reservedServiceSet.size : reservedVendorSet.size,
              servicesCount: Array.isArray(services) ? services.length : 0,
              alternativesAfterFilterCount: Array.isArray(alternatives) ? alternatives.length : 0,
              vendorProfilesCount: Array.isArray(vendorProfiles) ? vendorProfiles.length : 0,
            });
            continue;
          }

          const rich = encodeRichChatMessage({
            kind: 'vendorAlternatives',
            payload: {
              eventId: String(eventId),
              serviceLabel,
              radiusKm: radiusKm || null,
              options,
            },
          });

          const withinText = radiusKm ? ` (within ${radiusKm} km of the event)` : '';
          const fallbackText = `The vendor for ${serviceLabel} is not available. Please select one of the alternatives${withinText}.`;
          const text = `${fallbackText}\n\n${rich}`;

          // Send alternatives into manager-user DM so the user sees them in their active chat surface.
          await sendEventDmConversationMessage({
            eventId,
            otherAuthId: userAuthId,
            senderAuthId: managerAuthId,
            senderRole: 'MANAGER',
            text,
          });

          try {
            await publishEvent('VENDOR_REQUEST_REJECTED_ALTERNATIVES', {
              eventId: String(eventId),
              authId: userAuthId,
              managerAuthId,
              vendorAuthId,
              service: serviceLabel,
              rejectionReason,
              radiusKm: radiusKm || null,
              options,
              occurredAt: new Date().toISOString(),
            });
          } catch (e) {
            logger.error('Failed to publish VENDOR_REQUEST_REJECTED_ALTERNATIVES', {
              eventId,
              service: serviceLabel,
              message: e?.message || String(e),
            });
          }
        }
      })().catch((e) => {
        logger.error('Auto alternatives send failed after vendor rejection', {
          eventId,
          servicesToAutoSend,
          message: e?.message || String(e),
        });
      });
    }

    return res.status(200).json({
      success: true,
      message: 'Request rejected',
      data: {
        eventId,
        vendorsAccepted: Boolean(selection?.vendorsAccepted),
        vendorSelectionStatus: selection?.status,
        planningStatus: nextPlanningStatus,
        planningStatusUpdated,
      },
    });
  } catch (error) {
    logger.error('Error in rejectVendorRequest:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

/**
 * GET /vendor-selection/:eventId
 * Ensures a VendorSelection exists for the planning and returns it.
 */
const getOrCreateForPlanning = async (req, res) => {
  try {
    const { eventId } = req.params;
    const planning = await ensureAccessToPlanning({ eventId, user: req.user });

    const selection = await vendorSelectionService.ensureForPlanning(planning);
    const normalizedSelection = await clearStaleSelectionLocks({
      selection,
      planning,
      dayOverride: req.query?.day,
      fromOverride: req.query?.from,
      toOverride: req.query?.to,
    });

    const includeVendors = String(req.query.includeVendors || '').toLowerCase() === 'true';
    if (includeVendors) {
      const rawVendors = Array.isArray(normalizedSelection?.vendors) ? normalizedSelection.vendors : [];

      const serviceIds = Array.from(
        new Set(
          rawVendors
            .map((v) => (v?.serviceId != null ? String(v.serviceId).trim() : ''))
            .filter(Boolean)
        )
      );

      const serviceMetaById = new Map();
      await Promise.all(serviceIds.map(async (serviceId) => {
        try {
          const svc = await fetchPublicServiceById(serviceId);
          const name = String(svc?.name || '').trim();
          const rawCategory = String(svc?.serviceCategory || svc?.category || '').trim();
          const location = extractServiceLocationLabel(svc);

          serviceMetaById.set(serviceId, {
            name: name || null,
            category: rawCategory || null,
            location: location || null,
          });
        } catch (e) {
          logger.warn('Failed to resolve service name for vendor selection row', {
            eventId: String(eventId || '').trim(),
            serviceId,
            message: e?.message,
          });
        }
      }));

      const enrichedVendors = rawVendors.map((v) => {
        const sid = v?.serviceId != null ? String(v.serviceId).trim() : '';
        const meta = sid ? serviceMetaById.get(sid) : null;
        const directName = String(v?.serviceName || '').trim();
        const resolvedName = directName || String(meta?.name || '').trim();

        const normalizedService = vendorSelectionService.canonicalizeService(String(v?.service || '').trim());
        const normalizedMetaCategory = vendorSelectionService.canonicalizeService(String(meta?.category || '').trim());
        const isVenue = normalizedService === 'venue' || normalizedMetaCategory === 'venue';

        return {
          ...v,
          serviceName: resolvedName || null,
          serviceCategory: v?.serviceCategory || meta?.category || null,
          serviceLocation: isVenue ? (v?.serviceLocation || meta?.location || null) : null,
        };
      });

      const vendorAuthIds = Array.from(
        new Set(
          rawVendors
            .map((v) => (v?.vendorAuthId != null ? String(v.vendorAuthId).trim() : ''))
            .filter(Boolean)
        )
      );

      const vendorProfiles = await fetchPublicVendorsByAuthIds(vendorAuthIds);
      return res.status(200).json({
        success: true,
        data: {
          ...normalizedSelection,
          vendors: enrichedVendors,
          vendorProfiles,
        },
      });
    }

    return res.status(200).json({
      success: true,
      data: normalizedSelection,
    });
  } catch (error) {
    logger.error('Error in getOrCreateForPlanning:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

/**
 * PATCH /vendor-selection/:eventId/services
 * Body: { selectedServices: string[] }
 * Updates VendorSelection.selectedServices and keeps Planning.selectedServices in sync.
 */
const updateSelectedServices = async (req, res) => {
  try {
    const { eventId } = req.params;
    const { selectedServices } = req.body;

    if (!req.user?.authId) {
      return res.status(401).json({ success: false, message: 'Authentication required' });
    }

    // Authorization check via planning; also gives us planning owner authId
    const planning = await ensureAccessToPlanning({ eventId, user: req.user });

    const selection = await vendorSelectionService.updateSelectedServices({
      eventId,
      authId: planning.authId,
      selectedServices,
      actorRole: req.user?.role,
      actorAuthId: req.user?.authId,
      emergencyOverride: Boolean(req.body?.emergencyOverride),
      emergencyReason: req.body?.emergencyReason,
    });

    return res.status(200).json({
      success: true,
      message: 'Selected services updated successfully',
      data: selection,
    });
  } catch (error) {
    logger.error('Error in updateSelectedServices:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

/**
 * PATCH /vendor-selection/:eventId/vendors
 * Body: { service, vendorAuthId?, status?, rejectionReason?, alternativeNeeded?, servicePrice?: {min,max}, pricingUnit?, pricingQuantity?, pricingQuantityUnit? }
 */
const upsertVendor = async (req, res) => {
  try {
    const { eventId } = req.params;

    if (!req.user?.authId) {
      return res.status(401).json({ success: false, message: 'Authentication required' });
    }

    const planning = await ensureAccessToPlanning({ eventId, user: req.user });
    const previousSelection = await vendorSelectionService.getByEventId(eventId);

    const selection = await vendorSelectionService.upsertVendor({
      eventId,
      authId: planning.authId,
      vendorUpdate: req.body,
      actorRole: req.user?.role,
      actorAuthId: req.user?.authId,
      emergencyOverride: Boolean(req.body?.emergencyOverride),
      emergencyReason: req.body?.emergencyReason,
    });

    const requestedService = vendorSelectionService.canonicalizeService(String(req.body?.service || '').trim());
    const previousItem = Array.isArray(previousSelection?.vendors)
      ? previousSelection.vendors.find((row) => String(row?.service || '').trim() === requestedService)
      : null;
    const nextItem = Array.isArray(selection?.vendors)
      ? selection.vendors.find((row) => String(row?.service || '').trim() === requestedService)
      : null;

    const previousVendorAuthId = previousItem?.vendorAuthId != null ? String(previousItem.vendorAuthId).trim() : '';
    const nextVendorAuthId = nextItem?.vendorAuthId != null ? String(nextItem.vendorAuthId).trim() : '';
    const previousStatus = String(previousItem?.status || '').trim();
    const nextStatus = String(nextItem?.status || '').trim();

    const shouldNotifyBookingRequest = Boolean(nextVendorAuthId)
      && nextStatus === VENDOR_STATUS.YET_TO_SELECT
      && (nextVendorAuthId !== previousVendorAuthId || previousStatus !== VENDOR_STATUS.YET_TO_SELECT);

    if (shouldNotifyBookingRequest) {
      const progress = summarizeVendorItems(Array.isArray(selection?.vendors) ? selection.vendors : []);
      try {
        await publishEvent('VENDOR_BOOKING_REQUEST_RECEIVED', {
          eventId: String(eventId || '').trim(),
          authId: String(planning?.authId || '').trim() || null,
          managerAuthId: String(planning?.assignedManagerId || '').trim() || null,
          vendorAuthId: nextVendorAuthId,
          service: requestedService || null,
          eventTitle: String(planning?.eventTitle || '').trim() || null,
          progress,
          occurredAt: new Date().toISOString(),
        });
      } catch (publishError) {
        logger.warn('Failed to publish VENDOR_BOOKING_REQUEST_RECEIVED', {
          eventId,
          service: requestedService,
          vendorAuthId: nextVendorAuthId,
          message: publishError?.message || String(publishError),
        });
      }
    }

    return res.status(200).json({
      success: true,
      message: 'Vendor updated successfully',
      data: selection,
    });
  } catch (error) {
    logger.error('Error in upsertVendor:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

/**
 * POST /vendor-selection/:eventId/change-request
 * Body: { selectedServices: string[], reason?: string, emergencyOverride?: boolean, emergencyReason?: string }
 * Creates a managed service-change request (used after vendor confirmation lock).
 */
const createServiceChangeRequest = async (req, res) => {
  try {
    const { eventId } = req.params;

    if (!req.user?.authId) {
      return res.status(401).json({ success: false, message: 'Authentication required' });
    }

    const planning = await ensureAccessToPlanning({ eventId, user: req.user });
    const result = await vendorSelectionService.createServiceChangeRequest({
      eventId,
      authId: planning.authId,
      selectedServices: req.body?.selectedServices,
      reason: req.body?.reason,
      actorRole: req.user?.role,
      actorAuthId: req.user?.authId,
      emergencyOverride: Boolean(req.body?.emergencyOverride),
      emergencyReason: req.body?.emergencyReason,
    });

    return res.status(201).json({
      success: true,
      message: 'Change request submitted for manager review',
      data: result,
    });
  } catch (error) {
    logger.error('Error in createServiceChangeRequest:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

/**
 * PATCH /vendor-selection/:eventId/change-request/:requestId/manager-decision
 * Body: { approve: boolean, note?: string }
 */
const decideServiceChangeRequestByManager = async (req, res) => {
  try {
    const { eventId, requestId } = req.params;

    if (!req.user?.authId) {
      return res.status(401).json({ success: false, message: 'Authentication required' });
    }

    const planning = await ensureAccessToPlanning({ eventId, user: req.user });
    const approve = req.body?.approve === true;

    const result = await vendorSelectionService.decideServiceChangeRequestByManager({
      eventId,
      authId: planning.authId,
      requestId,
      managerAuthId: req.user?.authId,
      approve,
      note: req.body?.note,
    });

    return res.status(200).json({
      success: true,
      message: approve
        ? (result?.applied ? 'Change request approved and applied' : 'Change request approved; awaiting vendor consent')
        : 'Change request rejected',
      data: result,
    });
  } catch (error) {
    logger.error('Error in decideServiceChangeRequestByManager:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

/**
 * PATCH /vendor-selection/:eventId/change-request/:requestId/vendor-consent
 * Body: { approve: boolean, note?: string }
 */
const submitVendorConsentForServiceChangeRequest = async (req, res) => {
  try {
    const { eventId, requestId } = req.params;
    const vendorAuthId = normalizeVendorAuthId(req);
    const approve = req.body?.approve === true;

    const planning = await Planning.findOne({ eventId: String(eventId || '').trim() })
      .select('authId eventId')
      .lean();
    if (!planning) {
      return res.status(404).json({ success: false, message: 'Planning not found' });
    }

    const result = await vendorSelectionService.submitVendorConsentForServiceChangeRequest({
      eventId,
      authId: planning.authId,
      requestId,
      vendorAuthId,
      approve,
      note: req.body?.note,
    });

    return res.status(200).json({
      success: true,
      message: approve
        ? (result?.applied ? 'Consent recorded and change request applied' : 'Consent recorded')
        : 'Consent rejected; change request closed',
      data: result,
    });
  } catch (error) {
    logger.error('Error in submitVendorConsentForServiceChangeRequest:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

/**
 * POST /vendor-selection/:eventId/unlock
 * Body: { service?: string, force?: boolean }
 * Releases temporary reservation locks for the planning selection.
 */
const unlockReservations = async (req, res) => {
  try {
    const { eventId } = req.params;

    if (!req.user?.authId) {
      return res.status(401).json({ success: false, message: 'Authentication required' });
    }

    const planning = await ensureAccessToPlanning({ eventId, user: req.user });
    const force = (req.user?.role === 'ADMIN' || req.user?.role === 'MANAGER')
      ? Boolean(req.body?.force)
      : false;

    const result = await vendorSelectionService.releaseReservationsForPlanning({
      eventId,
      authId: planning.authId,
      service: req.body?.service,
      force,
    });

    return res.status(200).json({
      success: true,
      message: result?.skipped ? 'Unlock skipped' : 'Reservations unlocked',
      data: result,
    });
  } catch (error) {
    logger.error('Error in unlockReservations:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

module.exports = {
  getOrCreateForPlanning,
  updateSelectedServices,
  upsertVendor,
  createServiceChangeRequest,
  decideServiceChangeRequestByManager,
  submitVendorConsentForServiceChangeRequest,
  unlockReservations,
  listVendorRequests,
  listVendorPayoutLedger,
  getVendorRequestDetails,
  lockVendorServicePrice,
  acceptVendorRequest,
  rejectVendorRequest,
  listAlternativesForService,
};
