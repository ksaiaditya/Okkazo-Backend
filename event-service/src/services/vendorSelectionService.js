const Planning = require('../models/Planning');
const VendorSelection = require('../models/VendorSelection');
const createApiError = require('../utils/ApiError');
const logger = require('../utils/logger');
const { SERVICE_OPTIONS, VENDOR_STATUS } = require('../utils/vendorSelectionConstants');
const vendorReservationService = require('./vendorReservationService');

const SERVICE_ALIASES = {
  catering: 'Catering & Drinks',
  'catering and drinks': 'Catering & Drinks',
  'catering & drink': 'Catering & Drinks',
};

const canonicalizeService = (value) => {
  const raw = value == null ? '' : String(value).trim();
  if (!raw) return '';

  // Exact match first
  if (SERVICE_OPTIONS.includes(raw)) return raw;

  const key = raw.toLowerCase();
  const alias = SERVICE_ALIASES[key];
  if (alias && SERVICE_OPTIONS.includes(alias)) return alias;

  // Case-insensitive match against canonical list
  const ci = SERVICE_OPTIONS.find((s) => String(s).toLowerCase() === key);
  return ci || raw;
};

const sanitizeExistingSelectionServices = (services) => {
  const list = Array.isArray(services) ? services : [];

  const canonical = list
    .map((s) => canonicalizeService(s))
    .map((s) => (s == null ? '' : String(s).trim()))
    .filter((s) => s.length > 0);

  // Keep only valid canonical options.
  const valid = canonical.filter((s) => SERVICE_OPTIONS.includes(s));
  return Array.from(new Set(valid));
};

const normalizeVendorAuthId = (vendorAuthId) => {
  const v = String(vendorAuthId || '').trim();
  if (!v) throw createApiError(400, 'vendorAuthId is required');
  return v;
};

const normalizeEventId = (eventId) => {
  const eid = String(eventId || '').trim();
  if (!eid) throw createApiError(400, 'Event ID is required');
  return eid;
};

const normalizeServices = (selectedServices) => {
  if (!Array.isArray(selectedServices)) {
    throw createApiError(400, 'selectedServices must be an array');
  }

  const normalized = selectedServices
    .map((s) => (s == null ? '' : String(s).trim()))
    .filter((s) => s.length > 0);

  const unique = Array.from(new Set(normalized));
  if (unique.length === 0) {
    throw createApiError(400, 'At least one service must be selected');
  }

  const invalid = unique.filter((s) => !SERVICE_OPTIONS.includes(s));
  if (invalid.length > 0) {
    throw createApiError(400, `Invalid services: ${invalid.join(', ')}`);
  }

  return unique;
};

const ensureForPlanning = async (planning) => {
  if (!planning?.eventId || !planning?.authId) {
    throw createApiError(400, 'Planning eventId and authId are required');
  }

  const selectedServices = normalizeServices(planning.selectedServices || []);

  let selection = await VendorSelection.findOne({ eventId: planning.eventId });
  if (!selection) {
    selection = await VendorSelection.create({
      eventId: planning.eventId,
      authId: planning.authId,
      planningId: planning._id,
      managerId: planning.assignedManagerId ? planning.assignedManagerId : null,
      selectedServices,
      vendors: selectedServices.map((service) => ({
        service,
        vendorAuthId: null,
        status: VENDOR_STATUS.YET_TO_SELECT,
        alternativeNeeded: false,
        rejectionReason: null,
        servicePrice: { min: 0, max: 0 },
      })),
    });

    // Only linking the VendorSelection; do NOT revalidate the full Planning document.
    // This prevents unrelated validations (e.g., public ticketAvailability date rules)
    // from blocking vendor-selection workflows on legacy/partial plannings.
    await Planning.updateOne(
      { _id: planning._id },
      { $set: { vendorSelectionId: selection._id } }
    );

    logger.info('Created VendorSelection for planning', { eventId: planning.eventId });
    return selection;
  }

  // Sanitize legacy/invalid service values on existing selections.
  // This protects downstream workflows (alternatives search requires exact serviceCategory enum).
  let selectionDirty = false;
  const sanitized = sanitizeExistingSelectionServices(selection.selectedServices);
  const nextServices = sanitized.length === 0 ? selectedServices : sanitized;

  const currentServices = Array.isArray(selection.selectedServices) ? selection.selectedServices : [];
  const sameServices =
    currentServices.length === nextServices.length &&
    currentServices.every((s, i) => String(s) === String(nextServices[i]));

  if (!sameServices) {
    selection.selectedServices = nextServices;
    selectionDirty = true;
  }

  // Ensure vendors array has an entry for every selected service (and drop invalid ones)
  const existingByService = new Map();
  for (const v of selection.vendors || []) {
    const key = canonicalizeService(v?.service);
    if (!key) continue;
    // Prefer an existing entry that already has a selected vendor
    if (!existingByService.has(key)) {
      existingByService.set(key, v);
      continue;
    }

    const prev = existingByService.get(key);
    const prevHasVendor = prev?.vendorAuthId != null && String(prev.vendorAuthId).trim();
    const nextHasVendor = v?.vendorAuthId != null && String(v.vendorAuthId).trim();
    if (!prevHasVendor && nextHasVendor) {
      existingByService.set(key, v);
    }
  }
  const nextVendors = (selection.selectedServices || []).map((service) => {
    const existing = existingByService.get(service);
    if (existing) {
      const asObj = existing?.toObject ? existing.toObject() : existing;
      return { ...(asObj || {}), service };
    }
    return {
      service,
      vendorAuthId: null,
      status: VENDOR_STATUS.YET_TO_SELECT,
      alternativeNeeded: false,
      rejectionReason: null,
      servicePrice: { min: 0, max: 0 },
    };
  });

  const currentVendors = Array.isArray(selection.vendors) ? selection.vendors : [];
  const sameVendorServices =
    currentVendors.length === nextVendors.length &&
    currentVendors.every((v, i) => String(v?.service || '') === String(nextVendors[i]?.service || ''));

  if (!sameVendorServices) {
    selection.vendors = nextVendors;
    selectionDirty = true;
  }

  // Link planning to selection if missing
  let planningChanged = false;
  if (!selection.planningId) {
    selection.planningId = planning._id;
    selectionDirty = true;
  }
  if (!planning.vendorSelectionId) {
    planningChanged = true;
  }

  // Keep manager assignment aligned (planning is source-of-truth)
  const nextManagerId = planning.assignedManagerId ? String(planning.assignedManagerId).trim() : null;
  const currentManagerId = selection.managerId ? String(selection.managerId).trim() : null;
  if (currentManagerId !== nextManagerId) {
    selection.managerId = nextManagerId;
    selectionDirty = true;
  }

  // Keep selectedServices aligned to planning by default (but do not overwrite if selection was already customized)
  if (!Array.isArray(selection.selectedServices) || selection.selectedServices.length === 0) {
    selection.selectedServices = selectedServices;
    selectionDirty = true;
  }

  if (selectionDirty) {
    await selection.save();
  }

  if (planningChanged) {
    await Planning.updateOne(
      { _id: planning._id },
      { $set: { vendorSelectionId: selection._id } }
    );
  }

  return selection;
};

const getByEventId = async (eventId) => {
  if (!eventId?.trim()) throw createApiError(400, 'Event ID is required');
  const selection = await VendorSelection.findOne({ eventId: eventId.trim() }).lean();
  if (!selection) throw createApiError(404, 'Vendor selection not found');
  return selection;
};

const updateSelectedServices = async ({ eventId, authId, selectedServices }) => {
  if (!eventId?.trim()) throw createApiError(400, 'Event ID is required');
  if (!authId?.trim()) throw createApiError(400, 'Auth ID is required');

  const services = normalizeServices(selectedServices);

  const planning = await Planning.findOne({ eventId: eventId.trim(), authId: authId.trim() });
  if (!planning) throw createApiError(404, 'Planning not found');

  const selection = await ensureForPlanning(planning);

  selection.selectedServices = services;

  // Ensure vendors array has an entry for every selected service
  const existingByService = new Map((selection.vendors || []).map((v) => [v.service, v]));
  selection.vendors = services.map((service) => {
    const existing = existingByService.get(service);
    if (existing) return existing;
    return {
      service,
      vendorAuthId: null,
      status: VENDOR_STATUS.YET_TO_SELECT,
      alternativeNeeded: false,
      rejectionReason: null,
      servicePrice: { min: 0, max: 0 },
    };
  });

  await selection.save();

  // Keep Planning form in sync
  // Avoid triggering full Planning validation when syncing selected services.
  await Planning.updateOne(
    { _id: planning._id },
    { $set: { selectedServices: services } }
  );

  return selection.toObject();
};

const upsertVendor = async ({ eventId, authId, vendorUpdate }) => {
  if (!eventId?.trim()) throw createApiError(400, 'Event ID is required');
  if (!authId?.trim()) throw createApiError(400, 'Auth ID is required');

  const planning = await Planning.findOne({ eventId: eventId.trim(), authId: authId.trim() });
  if (!planning) throw createApiError(404, 'Planning not found');

  const selection = await ensureForPlanning(planning);

  const planningDay = vendorReservationService.planningToDay(planning);

  const service = String(vendorUpdate?.service || '').trim();
  if (!service) throw createApiError(400, 'service is required');
  if (!SERVICE_OPTIONS.includes(service)) throw createApiError(400, 'Invalid service');

  if (!selection.selectedServices.includes(service)) {
    throw createApiError(409, 'Service is not selected for this planning');
  }

  const next = {
    service,
    vendorAuthId:
      vendorUpdate.vendorAuthId != null && String(vendorUpdate.vendorAuthId).trim()
        ? String(vendorUpdate.vendorAuthId).trim()
        : null,
    serviceId:
      vendorUpdate.serviceId != null && String(vendorUpdate.serviceId).trim()
        ? String(vendorUpdate.serviceId).trim()
        : null,
    status: vendorUpdate.status || VENDOR_STATUS.YET_TO_SELECT,
    rejectionReason: vendorUpdate.rejectionReason != null && String(vendorUpdate.rejectionReason).trim() ? String(vendorUpdate.rejectionReason).trim() : null,
    alternativeNeeded: Boolean(vendorUpdate.alternativeNeeded),
    servicePrice: {
      min: (() => {
        const n = Number(vendorUpdate?.servicePrice?.min ?? 0);
        return Number.isFinite(n) && n > 0 ? n : 0;
      })(),
      max: (() => {
        const minIn = Number(vendorUpdate?.servicePrice?.min ?? 0);
        const safeMin = Number.isFinite(minIn) && minIn > 0 ? minIn : 0;

        const maxIn = Number(vendorUpdate?.servicePrice?.max ?? 0);
        const safeMax = Number.isFinite(maxIn) && maxIn > 0 ? maxIn : 0;

        if (safeMin > 0 && (safeMax <= 0 || safeMax === safeMin)) {
          return Math.ceil(safeMin * 1.5);
        }

        return safeMax;
      })(),
    },
  };

  const vendors = Array.isArray(selection.vendors) ? selection.vendors : [];
  const idx = vendors.findIndex((v) => v.service === service);
  const currentVendorAuthId =
    idx >= 0
      ? (vendors[idx]?.vendorAuthId != null && String(vendors[idx].vendorAuthId).trim() ? String(vendors[idx].vendorAuthId).trim() : null)
      : null;

  // Claim new reservation first (so we don't drop an existing valid selection on conflict)
  let claimedReservation = false;
  if (next.vendorAuthId && next.vendorAuthId !== currentVendorAuthId) {
    if (!planningDay) {
      throw createApiError(400, 'Event date is required before selecting vendors');
    }

    await vendorReservationService.claim({
      vendorAuthId: next.vendorAuthId,
      day: planningDay,
      eventId: eventId.trim(),
      authId: authId.trim(),
      service,
    });
    claimedReservation = true;
  }

  if (idx >= 0) {
    const current = vendors[idx]?.toObject ? vendors[idx].toObject() : vendors[idx];
    vendors[idx] = { ...(current || {}), ...next };
  } else {
    vendors.push(next);
  }

  // If rejected, ensure an alternative slot exists for that service
  if (next.status === VENDOR_STATUS.REJECTED) {
    const alt = Array.isArray(selection.serviceAlternativeVendor) ? selection.serviceAlternativeVendor : [];
    const altIdx = alt.findIndex((a) => a.service === service);
    if (altIdx === -1) {
      alt.push({ service, vendorAuthId: null });
      selection.serviceAlternativeVendor = alt;
    }
  }

  selection.vendors = vendors;
  try {
    await selection.save();
  } catch (e) {
    // Roll back any newly-claimed reservation if selection save fails
    if (claimedReservation && planningDay && next.vendorAuthId) {
      vendorReservationService
        .release({ vendorAuthId: next.vendorAuthId, day: planningDay, eventId: eventId.trim() })
        .catch((rollbackErr) => logger.error('Failed to rollback vendor reservation after save error:', rollbackErr));
    }
    throw e;
  }

  // Release old reservation after successful save (best-effort)
  if (planningDay && currentVendorAuthId && currentVendorAuthId !== next.vendorAuthId) {
    vendorReservationService
      .release({ vendorAuthId: currentVendorAuthId, day: planningDay, eventId: eventId.trim() })
      .catch((e) => logger.error('Failed to release vendor reservation:', e));
  }

  return selection.toObject();
};

// ─── Vendor-facing workflows ───────────────────────────────────────────────

const listSelectionsForVendor = async ({ vendorAuthId }) => {
  const vendor = normalizeVendorAuthId(vendorAuthId);

  const selections = await VendorSelection.find({ 'vendors.vendorAuthId': vendor })
    .select('eventId status vendorsAccepted managerId vendors')
    .lean();

  return (selections || []).map((sel) => {
    const vendorItems = Array.isArray(sel.vendors)
      ? sel.vendors.filter((v) => v?.vendorAuthId && String(v.vendorAuthId).trim() === vendor)
      : [];

    return {
      _id: sel._id,
      eventId: sel.eventId,
      status: sel.status,
      vendorsAccepted: Boolean(sel.vendorsAccepted),
      managerId: sel.managerId || null,
      vendorItems,
    };
  });
};

const getSelectionForVendorEvent = async ({ eventId, vendorAuthId }) => {
  const eid = normalizeEventId(eventId);
  const vendor = normalizeVendorAuthId(vendorAuthId);

  const selection = await VendorSelection.findOne({ eventId: eid }).lean();
  if (!selection) throw createApiError(404, 'Vendor selection not found');

  const vendorItems = Array.isArray(selection.vendors)
    ? selection.vendors.filter((v) => v?.vendorAuthId && String(v.vendorAuthId).trim() === vendor)
    : [];

  if (vendorItems.length === 0) {
    throw createApiError(403, 'Access denied');
  }

  return {
    _id: selection._id,
    eventId: selection.eventId,
    status: selection.status,
    vendorsAccepted: Boolean(selection.vendorsAccepted),
    managerId: selection.managerId || null,
    vendorItems,
  };
};

const respondForVendor = async ({ eventId, vendorAuthId, action, service, rejectionReason }) => {
  const eid = normalizeEventId(eventId);
  const vendor = normalizeVendorAuthId(vendorAuthId);

  const selection = await VendorSelection.findOne({ eventId: eid });
  if (!selection) throw createApiError(404, 'Vendor selection not found');

  const vendors = Array.isArray(selection.vendors) ? selection.vendors : [];
  const matchingItems = vendors.filter((v) => v?.vendorAuthId && String(v.vendorAuthId).trim() === vendor);
  if (matchingItems.length === 0) throw createApiError(403, 'Access denied');

  const requestedService = service != null && String(service).trim() ? String(service).trim() : null;
  let targetItems = matchingItems;
  if (requestedService) {
    targetItems = matchingItems.filter((v) => v?.service === requestedService);
    if (targetItems.length === 0) throw createApiError(404, 'Request not found for given service');
  }

  // Default: operate only on pending items for safety
  if (!requestedService) {
    const pending = targetItems.filter((v) => v?.status === VENDOR_STATUS.YET_TO_SELECT);
    if (pending.length > 0) targetItems = pending;
  }

  // Track status transitions so callers can avoid duplicate side-effects (chat/email sends).
  // IMPORTANT: Only track items we are actually going to modify.
  const beforeStatuses = targetItems.map((v) => ({
    service: v?.service || null,
    status: v?.status || null,
  }));

  if (action !== 'accept' && action !== 'reject') {
    throw createApiError(400, 'Invalid action');
  }

  if (action === 'reject') {
    const reason = String(rejectionReason || '').trim();
    if (!reason) throw createApiError(400, 'rejectionReason is required');
    for (const item of targetItems) {
      item.status = VENDOR_STATUS.REJECTED;
      item.rejectionReason = reason;
      item.alternativeNeeded = true;
    }
  } else {
    for (const item of targetItems) {
      item.status = VENDOR_STATUS.ACCEPTED;
      item.rejectionReason = null;
      item.alternativeNeeded = false;
    }
  }

  selection.vendors = vendors;
  await selection.save();

  const transitionedRejectedServices = action === 'reject'
    ? beforeStatuses
      .filter((b) => b?.status !== VENDOR_STATUS.REJECTED)
      .map((b) => (b?.service != null ? String(b.service).trim() : ''))
      .filter(Boolean)
    : [];

  const didTransitionToRejected = transitionedRejectedServices.length > 0;

  // After save, compute if vendor still accepted any service for this event
  const afterItems = (selection.vendors || []).filter(
    (v) => v?.vendorAuthId && String(v.vendorAuthId).trim() === vendor
  );
  const vendorAcceptedAnyServiceAfter = afterItems.some((v) => v?.status === VENDOR_STATUS.ACCEPTED);

  return {
    selection: selection.toObject(),
    vendorAcceptedAnyServiceAfter,
    didTransitionToRejected,
    transitionedRejectedServices,
  };
};

module.exports = {
  canonicalizeService,
  ensureForPlanning,
  getByEventId,
  updateSelectedServices,
  upsertVendor,
  listSelectionsForVendor,
  getSelectionForVendorEvent,
  respondForVendor,
};
