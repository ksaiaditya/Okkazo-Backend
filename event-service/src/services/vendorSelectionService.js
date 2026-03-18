const Planning = require('../models/Planning');
const VendorSelection = require('../models/VendorSelection');
const createApiError = require('../utils/ApiError');
const logger = require('../utils/logger');
const { SERVICE_OPTIONS, VENDOR_STATUS } = require('../utils/vendorSelectionConstants');
const vendorReservationService = require('./vendorReservationService');

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

  // Link planning to selection if missing
  let planningChanged = false;
  if (!selection.planningId) {
    selection.planningId = planning._id;
    await selection.save();
  }
  if (!planning.vendorSelectionId) {
    planningChanged = true;
  }

  // Keep selectedServices aligned to planning by default (but do not overwrite if selection was already customized)
  if (!Array.isArray(selection.selectedServices) || selection.selectedServices.length === 0) {
    selection.selectedServices = selectedServices;
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
    status: vendorUpdate.status || VENDOR_STATUS.YET_TO_SELECT,
    rejectionReason: vendorUpdate.rejectionReason != null && String(vendorUpdate.rejectionReason).trim() ? String(vendorUpdate.rejectionReason).trim() : null,
    alternativeNeeded: Boolean(vendorUpdate.alternativeNeeded),
    servicePrice: {
      min: Number(vendorUpdate?.servicePrice?.min ?? 0),
      max: Number(vendorUpdate?.servicePrice?.max ?? 0),
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

module.exports = {
  ensureForPlanning,
  getByEventId,
  updateSelectedServices,
  upsertVendor,
};
