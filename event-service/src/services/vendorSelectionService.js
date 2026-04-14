const Planning = require('../models/Planning');
const VendorSelection = require('../models/VendorSelection');
const createApiError = require('../utils/ApiError');
const logger = require('../utils/logger');
const { SERVICE_OPTIONS, VENDOR_STATUS } = require('../utils/vendorSelectionConstants');
const { STATUS } = require('../utils/planningConstants');
const vendorReservationService = require('./vendorReservationService');
const commissionService = require('./commissionService');
const planningQuoteService = require('./planningQuoteService');
const { publishEvent } = require('../kafka/eventProducer');

const STICKY_PLANNING_STATUSES = new Set([
  STATUS.CONFIRMED,
  STATUS.COMPLETED,
  STATUS.VENDOR_PAYMENT_PENDING,
]);
const REMOVAL_FREEZE_HOURS = 72;
const PRICING_UNIT_VALUES = new Set(['EVENT', 'PER_PERSON', 'PER_PLATE', 'PER_KG', 'PER_100_UNITS', 'FIXED']);

const toSortedUniqueDays = (days = []) => {
  const list = Array.isArray(days) ? days : [days];
  return Array.from(new Set(list.map((d) => String(d || '').trim()).filter(Boolean))).sort();
};

const areSameDaySets = (left = [], right = []) => {
  const a = toSortedUniqueDays(left);
  const b = toSortedUniqueDays(right);
  if (a.length !== b.length) return false;
  return a.every((v, idx) => v === b[idx]);
};

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
    .map((s) => canonicalizeService(s))
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

const sanitizePricingUnit = (value) => {
  const raw = value == null ? '' : String(value).trim().toUpperCase();
  if (!raw) return null;
  return PRICING_UNIT_VALUES.has(raw) ? raw : null;
};

const sanitizePricingQuantity = (value) => {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return null;
  return Math.round(n * 100) / 100;
};

const sanitizePricingQuantityUnit = (value) => {
  const raw = value == null ? '' : String(value).trim();
  if (!raw) return null;
  return raw.slice(0, 32);
};

const toMoney = (value) => {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return 0;
  return Math.round(n * 100) / 100;
};

const isVendorItemPriceLocked = (item) => {
  const quoted = Number(item?.vendorQuotedPrice || 0);
  const hasExplicitQuote = Number.isFinite(quoted) && quoted > 0;

  // Final quotation is considered locked only when vendor explicitly locks price
  // and provides quoted amount. Do not tie lock state to booking min/max range.
  return Boolean(item?.priceLocked) && hasExplicitQuote;
};

const toPaise = (inr) => {
  const n = Number(inr || 0);
  if (!Number.isFinite(n) || n <= 0) return 0;
  return Math.round(n * 100);
};

const getPlanningStartAt = (planning) => {
  if (!planning) return null;
  const startCandidate = planning?.schedule?.startAt || planning?.eventDate || null;
  if (!startCandidate) return null;
  const startAt = new Date(startCandidate);
  if (Number.isNaN(startAt.getTime())) return null;
  return startAt;
};

const getFreezeInfo = (planning) => {
  const startAt = getPlanningStartAt(planning);
  if (!startAt) {
    return {
      enabled: false,
      hoursUntilStart: null,
    };
  }

  const diffHours = (startAt.getTime() - Date.now()) / (1000 * 60 * 60);
  return {
    enabled: Number.isFinite(diffHours) && diffHours <= REMOVAL_FREEZE_HOURS,
    hoursUntilStart: Number.isFinite(diffHours) ? diffHours : null,
  };
};

const isDirectEditBlockedByFinalization = (planning) => {
  const status = String(planning?.status || '').trim();
  if (STICKY_PLANNING_STATUSES.has(status)) return true;
  return Boolean(planning?.vendorConfirmationPaid) || Boolean(planning?.fullPaymentPaid);
};

const computeSelectionTotalPaise = ({ selection, selectedServices }) => {
  const services = Array.isArray(selectedServices) ? selectedServices.map((s) => String(s || '').trim()).filter(Boolean) : [];
  const vendors = Array.isArray(selection?.vendors) ? selection.vendors : [];
  const byService = new Map();

  for (const row of vendors) {
    const service = String(row?.service || '').trim();
    if (!service || !services.includes(service)) continue;
    byService.set(service, row);
  }

  let total = 0;
  for (const service of services) {
    const row = byService.get(service);
    if (!row) continue;

    const quoted = Number(row?.vendorQuotedPrice || 0);
    const isLocked = isVendorItemPriceLocked(row);
    if (isLocked && Number.isFinite(quoted) && quoted > 0) {
      total += toPaise(quoted);
      continue;
    }

    const minPrice = Number(row?.servicePrice?.min || 0);
    if (Number.isFinite(minPrice) && minPrice > 0) {
      total += toPaise(minPrice);
    }
  }

  return Math.max(0, total);
};

const makeChangeRequestId = () => `CR-${Date.now()}-${Math.floor(Math.random() * 100000)}`;

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
        vendorQuotedPrice: null,
        commissionPercent: null,
        commissionAmount: null,
        priceHikeReason: null,
        priceLocked: false,
        pricingUnit: null,
        pricingQuantity: null,
        pricingQuantityUnit: null,
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
      vendorQuotedPrice: null,
      commissionPercent: null,
      commissionAmount: null,
      priceHikeReason: null,
      priceLocked: false,
      pricingUnit: null,
      pricingQuantity: null,
      pricingQuantityUnit: null,
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

const updateSelectedServices = async ({
  eventId,
  authId,
  selectedServices,
  actorRole = 'USER',
  actorAuthId = null,
  emergencyOverride = false,
  emergencyReason = '',
}) => {
  if (!eventId?.trim()) throw createApiError(400, 'Event ID is required');
  if (!authId?.trim()) throw createApiError(400, 'Auth ID is required');

  const services = normalizeServices(selectedServices);
  const normalizedRole = String(actorRole || 'USER').trim().toUpperCase();
  const isPrivilegedActor = normalizedRole === 'ADMIN' || normalizedRole === 'MANAGER';
  const normalizedEmergencyReason = String(emergencyReason || '').trim();

  const planning = await Planning.findOne({ eventId: eventId.trim(), authId: authId.trim() });
  if (!planning) throw createApiError(404, 'Planning not found');

  const selection = await ensureForPlanning(planning);
  const planningDays = vendorReservationService.planningToReservationDays(planning);
  const previousServices = Array.isArray(selection.selectedServices)
    ? selection.selectedServices.map((s) => String(s || '').trim()).filter(Boolean)
    : [];
  const previousVendors = Array.isArray(selection.vendors)
    ? selection.vendors.map((v) => (v?.toObject ? v.toObject() : v))
    : [];

  const previousServiceSet = new Set(previousServices);
  const nextServiceSet = new Set(services.map((s) => String(s || '').trim()).filter(Boolean));
  const removedServices = previousServices.filter((s) => !nextServiceSet.has(s));
  const addedServices = services.filter((s) => !previousServiceSet.has(s));

  if (addedServices.length === 0 && removedServices.length === 0) {
    return selection.toObject();
  }

  const freezeInfo = getFreezeInfo(planning);
  if (removedServices.length > 0 && freezeInfo.enabled) {
    if (!isPrivilegedActor || !emergencyOverride || !normalizedEmergencyReason) {
      throw createApiError(
        409,
        `Service removals are frozen within ${REMOVAL_FREEZE_HOURS} hours of event start. Ask manager/admin for emergency override.`
      );
    }
  }

  if (isDirectEditBlockedByFinalization(planning)) {
    if (!isPrivilegedActor || !emergencyOverride || !normalizedEmergencyReason) {
      throw createApiError(
        409,
        'Direct service edits are locked after vendor confirmation. Submit a change request for manager approval.'
      );
    }
  }

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
      vendorQuotedPrice: null,
      commissionPercent: null,
      commissionAmount: null,
      priceHikeReason: null,
      priceLocked: false,
      pricingUnit: null,
      pricingQuantity: null,
      pricingQuantityUnit: null,
    };
  });

  await selection.save();

  // Keep Planning form in sync
  // Avoid triggering full Planning validation when syncing selected services.
  await Planning.updateOne(
    { _id: planning._id },
    { $set: { selectedServices: services } }
  );

  const acceptedRemovedVendors = previousVendors.filter((item) => {
    const service = String(item?.service || '').trim();
    if (!service || !removedServices.includes(service)) return false;
    return String(item?.status || '').trim().toUpperCase() === VENDOR_STATUS.ACCEPTED;
  });

  const currentPlanningStatus = String(planning.status || '').trim();
  const vendorsAcceptedAfterSelection = Boolean(selection?.vendorsAccepted);
  let planningStatusReset = false;
  let planningStatusAutoApproved = false;
  let nextPlanningStatus = currentPlanningStatus || null;

  if (!planning.vendorConfirmationPaid) {
    if (vendorsAcceptedAfterSelection && currentPlanningStatus !== STATUS.APPROVED) {
      await Planning.updateOne(
        { _id: planning._id },
        {
          $set: {
            status: STATUS.APPROVED,
          },
        }
      );
      planningStatusAutoApproved = true;
      nextPlanningStatus = STATUS.APPROVED;

      try {
        await planningQuoteService.lockQuoteAtApproved({
          eventId: eventId.trim(),
          lockedByAuthId: String(actorAuthId || authId || '').trim() || null,
        });
      } catch (error) {
        logger.warn('Failed to lock quote after auto-approving planning on service update', {
          eventId: eventId.trim(),
          message: error?.message || String(error),
        });
      }
    } else if (!vendorsAcceptedAfterSelection && currentPlanningStatus === STATUS.APPROVED) {
      await Planning.updateOne(
        { _id: planning._id },
        {
          $set: {
            status: STATUS.PENDING_APPROVAL,
            quoteLockedAt: null,
            quoteLockedVersion: null,
          },
        }
      );
      planningStatusReset = true;
      nextPlanningStatus = STATUS.PENDING_APPROVAL;
    }
  }

  // Release reservations for services that were removed from selection.
  if (planningDays.length > 0) {
    const selectedServiceSet = new Set(services.map((s) => String(s || '').trim()).filter(Boolean));
    const toRelease = previousVendors.filter((item) => {
      const service = String(item?.service || '').trim();
      const vendorAuthId = String(item?.vendorAuthId || '').trim();
      if (!service || !vendorAuthId) return false;
      return !selectedServiceSet.has(service);
    });

    await Promise.all(
      toRelease.map(async (item) => {
        try {
          await vendorReservationService.releaseForDays({
            vendorAuthId: item.vendorAuthId,
            days: planningDays,
            eventId: eventId.trim(),
            service: item.service,
            serviceId: item.serviceId || null,
          });
        } catch (error) {
          logger.warn('Failed to release reservation for removed service', {
            eventId: eventId.trim(),
            service: item?.service || null,
            vendorAuthId: item?.vendorAuthId || null,
            serviceId: item?.serviceId || null,
            error: error?.message || String(error),
          });
        }
      })
    );
  }

  try {
    const affectedVendorAuthIds = Array.from(
      new Set(
        previousVendors
          .filter((row) => removedServices.includes(String(row?.service || '').trim()))
          .map((row) => String(row?.vendorAuthId || '').trim())
          .filter(Boolean)
      )
    );

    await publishEvent(
      'PLANNING_VENDOR_SELECTION_CHANGED',
      {
        eventId: eventId.trim(),
        planningAuthId: authId.trim(),
        actorAuthId: String(actorAuthId || authId || '').trim() || null,
        actorRole: normalizedRole,
        addedServices,
        removedServices,
        acceptedRemovedServices: acceptedRemovedVendors.map((row) => ({
          service: String(row?.service || '').trim(),
          vendorAuthId: String(row?.vendorAuthId || '').trim() || null,
          serviceId: row?.serviceId ? String(row.serviceId).trim() : null,
        })),
        affectedVendorAuthIds,
        planningStatusReset,
        planningStatusAutoApproved,
        nextPlanningStatus,
      },
      `${eventId.trim()}:${Date.now()}:services`
    );
  } catch (error) {
    logger.warn('Failed to publish PLANNING_VENDOR_SELECTION_CHANGED event', {
      eventId: eventId.trim(),
      message: error?.message || String(error),
    });
  }

  const result = selection.toObject();
  return {
    ...result,
    policy: {
      addedServices,
      removedServices,
      planningStatusReset,
      planningStatusAutoApproved,
      nextPlanningStatus,
    },
  };
};

const upsertVendor = async ({
  eventId,
  authId,
  vendorUpdate,
  actorRole = 'USER',
  actorAuthId = null,
  emergencyOverride = false,
  emergencyReason = '',
}) => {
  if (!eventId?.trim()) throw createApiError(400, 'Event ID is required');
  if (!authId?.trim()) throw createApiError(400, 'Auth ID is required');

  const normalizedRole = String(actorRole || 'USER').trim().toUpperCase();
  const isPrivilegedActor = normalizedRole === 'ADMIN' || normalizedRole === 'MANAGER';
  const normalizedEmergencyReason = String(emergencyReason || '').trim();

  const planning = await Planning.findOne({ eventId: eventId.trim(), authId: authId.trim() });
  if (!planning) throw createApiError(404, 'Planning not found');

  const selection = await ensureForPlanning(planning);

  const planningDays = vendorReservationService.planningToReservationDays(planning);
  const requestedDay = vendorUpdate?.day != null ? String(vendorUpdate.day).trim() : '';
  const requestedFrom = vendorUpdate?.from != null ? String(vendorUpdate.from).trim() : '';
  const requestedTo = vendorUpdate?.to != null ? String(vendorUpdate.to).trim() : '';
  const requestedDays = vendorReservationService.resolveReservationDays({
    day: requestedDay,
    from: requestedFrom,
    to: requestedTo,
  });
  const reservationDays = requestedDays.length > 0 ? requestedDays : planningDays;
  const dayChangedAgainstPlanning = requestedDays.length > 0 && !areSameDaySets(requestedDays, planningDays);

  const service = canonicalizeService(vendorUpdate?.service);
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
    vendorQuotedPrice: null,
    commissionPercent: null,
    commissionAmount: null,
    priceHikeReason: null,
    priceLocked: false,
    pricingUnit: sanitizePricingUnit(vendorUpdate?.pricingUnit),
    pricingQuantity: sanitizePricingQuantity(vendorUpdate?.pricingQuantity),
    pricingQuantityUnit: sanitizePricingQuantityUnit(vendorUpdate?.pricingQuantityUnit),
  };

  if (!next.pricingQuantity) {
    next.pricingQuantityUnit = null;
  }

  if (!next.vendorAuthId) {
    next.serviceId = null;
    next.pricingUnit = null;
    next.pricingQuantity = null;
    next.pricingQuantityUnit = null;
    next.servicePrice = { min: 0, max: 0 };
    next.vendorQuotedPrice = null;
    next.commissionPercent = null;
    next.commissionAmount = null;
    next.priceHikeReason = null;
    next.priceLocked = false;
  }

  if (service === 'Venue' && next.vendorAuthId && !next.serviceId) {
    throw createApiError(400, 'serviceId is required when selecting a venue');
  }

  const vendors = Array.isArray(selection.vendors) ? selection.vendors : [];
  const idx = vendors.findIndex((v) => v.service === service);
  const currentVendorAuthId =
    idx >= 0
      ? (vendors[idx]?.vendorAuthId != null && String(vendors[idx].vendorAuthId).trim() ? String(vendors[idx].vendorAuthId).trim() : null)
      : null;
  const currentServiceId =
    idx >= 0 && vendors[idx]?.serviceId != null && String(vendors[idx].serviceId).trim()
      ? String(vendors[idx].serviceId).trim()
      : null;

  const reservationIdentityChanged =
    (next.vendorAuthId != null && (
      next.vendorAuthId !== currentVendorAuthId ||
      ((next.serviceId || null) !== (currentServiceId || null))
    )) ||
    (next.vendorAuthId == null && currentVendorAuthId != null);
  const deselectRequested = next.vendorAuthId == null;

  const freezeInfo = getFreezeInfo(planning);
  const isRemovalAction = Boolean(currentVendorAuthId) && (
    deselectRequested ||
    (next.vendorAuthId && next.vendorAuthId !== currentVendorAuthId) ||
    ((next.serviceId || null) !== (currentServiceId || null))
  );

  if (isRemovalAction && freezeInfo.enabled) {
    if (!isPrivilegedActor || !emergencyOverride || !normalizedEmergencyReason) {
      throw createApiError(
        409,
        `Vendor removals are frozen within ${REMOVAL_FREEZE_HOURS} hours of event start. Ask manager/admin for emergency override.`
      );
    }
  }

  if (isDirectEditBlockedByFinalization(planning) && reservationIdentityChanged) {
    if (!isPrivilegedActor || !emergencyOverride || !normalizedEmergencyReason) {
      throw createApiError(
        409,
        'Direct vendor changes are locked after vendor confirmation. Submit a change request for manager approval.'
      );
    }
  }

  // Claim reservation first (so we don't drop an existing valid selection on conflict)
  // We intentionally claim even when vendor/service identity is unchanged to ensure
  // date changes are validated against current locks.
  let claimedReservation = false;
  if (next.vendorAuthId) {
    if (reservationDays.length === 0) {
      throw createApiError(400, 'Event date is required before selecting vendors');
    }

    await vendorReservationService.claimForDays({
      vendorAuthId: next.vendorAuthId,
      days: reservationDays,
      eventId: eventId.trim(),
      authId: authId.trim(),
      service,
      serviceId: next.serviceId,
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
    if (claimedReservation && reservationDays.length > 0 && next.vendorAuthId) {
      vendorReservationService
        .releaseForDays({
          vendorAuthId: next.vendorAuthId,
          days: reservationDays,
          eventId: eventId.trim(),
          service,
          serviceId: next.serviceId,
        })
        .catch((rollbackErr) => logger.error('Failed to rollback vendor reservation after save error:', rollbackErr));
    }
    throw e;
  }

  // Keep only the latest reservation for this service/day/event.
  if (reservationDays.length > 0 && (reservationIdentityChanged || dayChangedAgainstPlanning || deselectRequested)) {
    try {
      await vendorReservationService.releasePriorReservationsForEventServiceDays({
        eventId: eventId.trim(),
        days: reservationDays,
        service,
        serviceId: next.serviceId,
        keepVendorAuthId: next.vendorAuthId,
      });
    } catch (error) {
      logger.error('Failed to cleanup prior vendor reservations after upsert:', error);
    }
  }

  // Release old reservation against known candidate days to avoid stale locks
  // when payload day differs from the planning-derived day.
  if (currentVendorAuthId && (reservationIdentityChanged || dayChangedAgainstPlanning)) {
    const isSameVendorIdentity =
      Boolean(next.vendorAuthId) &&
      next.vendorAuthId === currentVendorAuthId &&
      ((next.serviceId || null) === (currentServiceId || null));

    let releaseDays = [];

    if (dayChangedAgainstPlanning && isSameVendorIdentity && !reservationIdentityChanged) {
      // Same vendor selected on a different day/range:
      // release only stale previously-planning days, never the freshly claimed days.
      const selectedDaySet = new Set(toSortedUniqueDays(reservationDays));
      releaseDays = toSortedUniqueDays(planningDays).filter((d) => !selectedDaySet.has(d));
    } else {
      releaseDays = toSortedUniqueDays([...reservationDays, ...planningDays]);
    }

    if (releaseDays.length === 0) {
      return selection.toObject();
    }

    await Promise.all(
      releaseDays.map(async (releaseDay) => {
        try {
          await vendorReservationService.release({
            vendorAuthId: currentVendorAuthId,
            day: releaseDay,
            eventId: eventId.trim(),
            service,
            serviceId: currentServiceId,
          });
        } catch (error) {
          logger.error('Failed to release previous vendor reservation:', {
            eventId: eventId.trim(),
            day: releaseDay,
            service,
            vendorAuthId: currentVendorAuthId,
            error: error?.message || String(error),
          });
        }
      })
    );
  }

  let planningStatusReset = false;
  if (!planning.vendorConfirmationPaid && String(planning.status || '').trim() === STATUS.APPROVED && reservationIdentityChanged) {
    await Planning.updateOne(
      { _id: planning._id },
      {
        $set: {
          status: STATUS.PENDING_APPROVAL,
          quoteLockedAt: null,
          quoteLockedVersion: null,
        },
      }
    );
    planningStatusReset = true;
  }

  try {
    await publishEvent(
      'PLANNING_VENDOR_ASSIGNMENT_CHANGED',
      {
        eventId: eventId.trim(),
        planningAuthId: authId.trim(),
        actorAuthId: String(actorAuthId || authId || '').trim() || null,
        actorRole: normalizedRole,
        service,
        previous: {
          vendorAuthId: currentVendorAuthId,
          serviceId: currentServiceId,
        },
        next: {
          vendorAuthId: next.vendorAuthId || null,
          serviceId: next.serviceId || null,
        },
        planningStatusReset,
      },
      `${eventId.trim()}:${service}:${Date.now()}:vendor`
    );
  } catch (error) {
    logger.warn('Failed to publish PLANNING_VENDOR_ASSIGNMENT_CHANGED event', {
      eventId: eventId.trim(),
      service,
      message: error?.message || String(error),
    });
  }

  const result = selection.toObject();
  return {
    ...result,
    policy: {
      planningStatusReset,
      nextPlanningStatus: planningStatusReset ? STATUS.PENDING_APPROVAL : String(planning.status || '').trim() || null,
    },
  };
};

const createServiceChangeRequest = async ({
  eventId,
  authId,
  selectedServices,
  reason = '',
  actorRole = 'USER',
  actorAuthId = null,
  emergencyOverride = false,
  emergencyReason = '',
}) => {
  if (!eventId?.trim()) throw createApiError(400, 'Event ID is required');
  if (!authId?.trim()) throw createApiError(400, 'Auth ID is required');

  const normalizedRole = String(actorRole || 'USER').trim().toUpperCase();
  const normalizedReason = String(reason || '').trim();
  const normalizedEmergencyReason = String(emergencyReason || '').trim();
  const isPrivilegedActor = normalizedRole === 'ADMIN' || normalizedRole === 'MANAGER';

  const services = normalizeServices(selectedServices);
  const planning = await Planning.findOne({ eventId: eventId.trim(), authId: authId.trim() });
  if (!planning) throw createApiError(404, 'Planning not found');

  const selection = await ensureForPlanning(planning);
  const currentServices = Array.isArray(selection.selectedServices)
    ? selection.selectedServices.map((s) => String(s || '').trim()).filter(Boolean)
    : [];

  const currentSet = new Set(currentServices);
  const nextSet = new Set(services.map((s) => String(s || '').trim()).filter(Boolean));
  const addedServices = services.filter((s) => !currentSet.has(s));
  const removedServices = currentServices.filter((s) => !nextSet.has(s));

  if (addedServices.length === 0 && removedServices.length === 0) {
    throw createApiError(400, 'No changes detected for service change request');
  }

  const freezeInfo = getFreezeInfo(planning);
  if (removedServices.length > 0 && freezeInfo.enabled) {
    if (!isPrivilegedActor || !emergencyOverride || !normalizedEmergencyReason) {
      throw createApiError(
        409,
        `Service removals are frozen within ${REMOVAL_FREEZE_HOURS} hours of event start. Ask manager/admin for emergency override.`
      );
    }
  }

  const rows = Array.isArray(selection.vendors) ? selection.vendors : [];
  const affectedAcceptedServices = rows
    .filter((row) => removedServices.includes(String(row?.service || '').trim()))
    .filter((row) => String(row?.status || '').trim().toUpperCase() === VENDOR_STATUS.ACCEPTED)
    .map((row) => ({
      service: String(row?.service || '').trim(),
      previousVendorAuthId: String(row?.vendorAuthId || '').trim() || null,
      previousServiceId: row?.serviceId ? String(row.serviceId).trim() : null,
      requiresVendorConsent: true,
    }));

  const currentGrandTotalPaise = computeSelectionTotalPaise({ selection, selectedServices: currentServices });
  const proposedGrandTotalPaise = computeSelectionTotalPaise({ selection, selectedServices: services });
  const deltaPaise = proposedGrandTotalPaise - currentGrandTotalPaise;

  const request = {
    requestId: makeChangeRequestId(),
    type: 'SERVICE_CHANGE',
    status: 'PENDING_MANAGER_APPROVAL',
    requestedByAuthId: String(actorAuthId || authId || '').trim() || authId.trim(),
    requestedByRole: normalizedRole,
    reason: normalizedReason || null,
    emergencyOverride: Boolean(emergencyOverride),
    emergencyReason: normalizedEmergencyReason || null,
    serviceDelta: {
      added: addedServices,
      removed: removedServices,
    },
    proposedSelectedServices: services,
    affectedAcceptedServices,
    vendorConsents: Array.from(
      new Set(
        affectedAcceptedServices
          .map((entry) => String(entry?.previousVendorAuthId || '').trim())
          .filter(Boolean)
      )
    ).map((vendorAuthId) => ({
      vendorAuthId,
      status: 'PENDING',
      note: null,
      at: null,
    })),
    priceDelta: {
      currentGrandTotalPaise,
      proposedGrandTotalPaise,
      deltaPaise,
      // Placeholder until policy matrix is configured.
      suggestedAdjustmentFeePaise: 0,
    },
    vendorConsent: {
      status: affectedAcceptedServices.length > 0 ? 'PENDING' : 'NOT_REQUIRED',
      note: null,
      at: null,
    },
    createdAt: new Date(),
  };

  await Planning.updateOne(
    { _id: planning._id },
    {
      $push: {
        changeRequests: request,
      },
    }
  );

  try {
    await publishEvent(
      'PLANNING_CHANGE_REQUEST_CREATED',
      {
        eventId: eventId.trim(),
        planningAuthId: authId.trim(),
        requestId: request.requestId,
        requestType: request.type,
        requestedByAuthId: request.requestedByAuthId,
        requestedByRole: request.requestedByRole,
        addedServices,
        removedServices,
        affectedAcceptedServices,
        priceDelta: request.priceDelta,
      },
      `${eventId.trim()}:${request.requestId}`
    );
  } catch (error) {
    logger.warn('Failed to publish PLANNING_CHANGE_REQUEST_CREATED event', {
      eventId: eventId.trim(),
      requestId: request.requestId,
      message: error?.message || String(error),
    });
  }

  return {
    request,
    preview: {
      addedServices,
      removedServices,
      affectedAcceptedServices,
      priceDelta: request.priceDelta,
      requiresVendorConsent: affectedAcceptedServices.length > 0,
      managerApprovalRequired: true,
    },
  };
};

const decideServiceChangeRequestByManager = async ({
  eventId,
  authId,
  requestId,
  managerAuthId,
  approve,
  note = '',
}) => {
  const eid = normalizeEventId(eventId);
  const ownerAuthId = String(authId || '').trim();
  if (!ownerAuthId) throw createApiError(400, 'authId is required');

  const rid = String(requestId || '').trim();
  if (!rid) throw createApiError(400, 'requestId is required');

  const actorAuthId = String(managerAuthId || '').trim();
  if (!actorAuthId) throw createApiError(400, 'managerAuthId is required');

  const managerNote = String(note || '').trim() || null;

  const planning = await Planning.findOne({ eventId: eid, authId: ownerAuthId });
  if (!planning) throw createApiError(404, 'Planning not found');

  const idx = Array.isArray(planning.changeRequests)
    ? planning.changeRequests.findIndex((item) => String(item?.requestId || '').trim() === rid)
    : -1;
  if (idx < 0) throw createApiError(404, 'Change request not found');

  const request = planning.changeRequests[idx];
  if (String(request?.type || '').trim() !== 'SERVICE_CHANGE') {
    throw createApiError(409, 'Unsupported change request type');
  }

  const currentStatus = String(request?.status || '').trim();
  if (currentStatus !== 'PENDING_MANAGER_APPROVAL') {
    throw createApiError(409, `Change request is already ${currentStatus}`);
  }

  request.managerDecision = {
    byAuthId: actorAuthId,
    at: new Date(),
    note: managerNote,
  };

  if (!approve) {
    request.status = 'REJECTED';
    await planning.save({ validateBeforeSave: false });
    return { request: request.toObject ? request.toObject() : request, applied: false };
  }

  const vendorConsents = Array.isArray(request.vendorConsents) ? request.vendorConsents : [];
  const pendingConsents = vendorConsents.filter((consent) => String(consent?.status || '').trim() === 'PENDING');

  if (pendingConsents.length > 0) {
    request.status = 'PENDING_VENDOR_CONSENT';
    request.vendorConsent = {
      ...(request.vendorConsent || {}),
      status: 'PENDING',
    };
    await planning.save({ validateBeforeSave: false });
    return { request: request.toObject ? request.toObject() : request, applied: false };
  }

  const targetServices = Array.isArray(request?.proposedSelectedServices)
    ? request.proposedSelectedServices.map((s) => String(s || '').trim()).filter(Boolean)
    : [];
  if (targetServices.length === 0) {
    throw createApiError(409, 'Change request does not include target services');
  }

  await updateSelectedServices({
    eventId: eid,
    authId: ownerAuthId,
    selectedServices: targetServices,
    actorRole: 'MANAGER',
    actorAuthId,
    emergencyOverride: true,
    emergencyReason: String(request?.emergencyReason || request?.reason || 'Approved managed change request').trim() || 'Approved managed change request',
  });

  request.status = 'APPROVED';
  request.vendorConsent = {
    ...(request.vendorConsent || {}),
    status: vendorConsents.length > 0 ? 'APPROVED' : 'NOT_REQUIRED',
    at: new Date(),
  };

  await planning.save({ validateBeforeSave: false });
  return { request: request.toObject ? request.toObject() : request, applied: true };
};

const submitVendorConsentForServiceChangeRequest = async ({
  eventId,
  authId,
  requestId,
  vendorAuthId,
  approve,
  note = '',
}) => {
  const eid = normalizeEventId(eventId);
  const ownerAuthId = String(authId || '').trim();
  if (!ownerAuthId) throw createApiError(400, 'authId is required');

  const rid = String(requestId || '').trim();
  if (!rid) throw createApiError(400, 'requestId is required');

  const actorVendorAuthId = normalizeVendorAuthId(vendorAuthId);
  const vendorNote = String(note || '').trim() || null;

  const planning = await Planning.findOne({ eventId: eid, authId: ownerAuthId });
  if (!planning) throw createApiError(404, 'Planning not found');

  const idx = Array.isArray(planning.changeRequests)
    ? planning.changeRequests.findIndex((item) => String(item?.requestId || '').trim() === rid)
    : -1;
  if (idx < 0) throw createApiError(404, 'Change request not found');

  const request = planning.changeRequests[idx];
  const currentStatus = String(request?.status || '').trim();
  if (currentStatus !== 'PENDING_VENDOR_CONSENT') {
    throw createApiError(409, `Change request is ${currentStatus}, vendor consent is not allowed now`);
  }

  const vendorConsents = Array.isArray(request.vendorConsents) ? request.vendorConsents : [];
  const consentEntry = vendorConsents.find((entry) => String(entry?.vendorAuthId || '').trim() === actorVendorAuthId);
  if (!consentEntry) {
    throw createApiError(403, 'You are not an affected vendor for this change request');
  }

  const consentStatus = String(consentEntry?.status || '').trim();
  if (consentStatus !== 'PENDING') {
    throw createApiError(409, `You already responded as ${consentStatus}`);
  }

  consentEntry.status = approve ? 'APPROVED' : 'REJECTED';
  consentEntry.note = vendorNote;
  consentEntry.at = new Date();

  if (!approve) {
    request.status = 'REJECTED';
    request.vendorConsent = {
      ...(request.vendorConsent || {}),
      status: 'REJECTED',
      note: vendorNote,
      at: new Date(),
    };

    await planning.save({ validateBeforeSave: false });
    return { request: request.toObject ? request.toObject() : request, applied: false };
  }

  const allApproved = vendorConsents.every((entry) => String(entry?.status || '').trim() === 'APPROVED');
  if (!allApproved) {
    request.vendorConsent = {
      ...(request.vendorConsent || {}),
      status: 'PENDING',
    };

    await planning.save({ validateBeforeSave: false });
    return { request: request.toObject ? request.toObject() : request, applied: false };
  }

  const targetServices = Array.isArray(request?.proposedSelectedServices)
    ? request.proposedSelectedServices.map((s) => String(s || '').trim()).filter(Boolean)
    : [];
  if (targetServices.length === 0) {
    throw createApiError(409, 'Change request does not include target services');
  }

  const managerAuthId = String(request?.managerDecision?.byAuthId || '').trim() || ownerAuthId;
  await updateSelectedServices({
    eventId: eid,
    authId: ownerAuthId,
    selectedServices: targetServices,
    actorRole: 'MANAGER',
    actorAuthId: managerAuthId,
    emergencyOverride: true,
    emergencyReason: String(request?.emergencyReason || request?.reason || 'Approved vendor-consent change request').trim() || 'Approved vendor-consent change request',
  });

  request.status = 'APPROVED';
  request.vendorConsent = {
    ...(request.vendorConsent || {}),
    status: 'APPROVED',
    at: new Date(),
  };

  await planning.save({ validateBeforeSave: false });
  return { request: request.toObject ? request.toObject() : request, applied: true };
};

const releaseReservationsForPlanning = async ({
  eventId,
  authId,
  service = null,
  force = false,
  preserveSelection = false,
}) => {
  if (!eventId?.trim()) throw createApiError(400, 'Event ID is required');
  if (!authId?.trim()) throw createApiError(400, 'Auth ID is required');

  const eid = eventId.trim();
  const uid = authId.trim();

  const planning = await Planning.findOne({ eventId: eid, authId: uid }).select(
    'eventId authId status platformFeePaid isPaid depositPaid vendorConfirmationPaid fullPaymentPaid eventDate schedule'
  );
  if (!planning) throw createApiError(404, 'Planning not found');

  if (!force) {
    const hasPaymentProgress =
      Boolean(planning.depositPaid) ||
      Boolean(planning.vendorConfirmationPaid) ||
      Boolean(planning.fullPaymentPaid);

    const isStickyStatus = STICKY_PLANNING_STATUSES.has(String(planning.status || '').trim());
    if (hasPaymentProgress || isStickyStatus) {
      return {
        released: 0,
        candidates: 0,
        skipped: true,
        reason: 'Planning is in a sticky reservation state',
      };
    }
  }

  const selection = await ensureForPlanning(planning);
  const planningDays = vendorReservationService.planningToReservationDays(planning);
  if (planningDays.length === 0) {
    return {
      released: 0,
      candidates: 0,
      skipped: true,
      reason: 'Event date is not available',
    };
  }

  const targetService = service != null && String(service).trim()
    ? canonicalizeService(String(service).trim())
    : null;

  if (targetService && !SERVICE_OPTIONS.includes(targetService)) {
    throw createApiError(400, 'Invalid service');
  }

  const vendors = Array.isArray(selection?.vendors)
    ? selection.vendors.map((v) => (v?.toObject ? v.toObject() : v))
    : [];

  const candidates = vendors.filter((item) => {
    const vendorAuthId = String(item?.vendorAuthId || '').trim();
    if (!vendorAuthId) return false;
    if (targetService) {
      return String(item?.service || '').trim() === targetService;
    }
    return true;
  });

  const releasedCounts = await Promise.all(
    candidates.map(async (item) => {
      try {
        const result = await vendorReservationService.releaseForDays({
          vendorAuthId: item.vendorAuthId,
          days: planningDays,
          eventId: eid,
          service: item.service,
          serviceId: item.serviceId || null,
        });
        return Number(result?.removed || 0);
      } catch (error) {
        logger.warn('Failed to release reservation during unlock', {
          eventId: eid,
          vendorAuthId: item?.vendorAuthId || null,
          service: item?.service || null,
          serviceId: item?.serviceId || null,
          error: error?.message || String(error),
        });
        return 0;
      }
    })
  );

  const released = releasedCounts.reduce((acc, n) => acc + n, 0);
  const candidateKeys = new Set(
    candidates
      .map((item) => {
        const serviceValue = String(item?.service || '').trim();
        const vendorValue = String(item?.vendorAuthId || '').trim();
        if (!serviceValue || !vendorValue) return null;
        const serviceIdValue = item?.serviceId != null ? String(item.serviceId).trim() : '';
        return `${serviceValue}::${vendorValue}::${serviceIdValue}`;
      })
      .filter(Boolean)
  );

  let selectionCleared = 0;
  if (!preserveSelection && candidateKeys.size > 0) {
    const currentVendors = Array.isArray(selection?.vendors)
      ? selection.vendors.map((v) => (v?.toObject ? v.toObject() : v))
      : [];

    const nextVendors = currentVendors.map((item) => {
      const serviceValue = String(item?.service || '').trim();
      const vendorValue = String(item?.vendorAuthId || '').trim();
      if (!serviceValue || !vendorValue) return item;

      const serviceIdValue = item?.serviceId != null ? String(item.serviceId).trim() : '';
      const key = `${serviceValue}::${vendorValue}::${serviceIdValue}`;
      if (!candidateKeys.has(key)) return item;

      selectionCleared += 1;
      return {
        ...(item || {}),
        vendorAuthId: null,
        serviceId: null,
        status: VENDOR_STATUS.YET_TO_SELECT,
        rejectionReason: null,
        alternativeNeeded: false,
        servicePrice: { min: 0, max: 0 },
        vendorQuotedPrice: null,
        commissionPercent: null,
        commissionAmount: null,
        priceLocked: false,
        pricingUnit: null,
        pricingQuantity: null,
        pricingQuantityUnit: null,
      };
    });

    if (selectionCleared > 0) {
      selection.vendors = nextVendors;
      await selection.save();
    }
  }

  return {
    released,
    candidates: candidates.length,
    selectionCleared,
    preserveSelection: Boolean(preserveSelection),
    skipped: false,
    days: planningDays,
    service: targetService,
  };
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

const lockPriceForVendor = async ({ eventId, vendorAuthId, service, quotedPrice, priceHikeReason }) => {
  const eid = normalizeEventId(eventId);
  const vendor = normalizeVendorAuthId(vendorAuthId);
  const canonicalService = canonicalizeService(service);

  if (!canonicalService) throw createApiError(400, 'service is required');
  if (!SERVICE_OPTIONS.includes(canonicalService)) throw createApiError(400, 'Invalid service');

  const quoted = toMoney(quotedPrice);
  if (!quoted || quoted <= 0) {
    throw createApiError(400, 'price must be greater than 0');
  }

  const selection = await VendorSelection.findOne({ eventId: eid });
  if (!selection) throw createApiError(404, 'Vendor selection not found');

  const vendors = Array.isArray(selection.vendors) ? selection.vendors : [];
  const item = vendors.find(
    (v) =>
      String(v?.vendorAuthId || '').trim() === vendor &&
      String(v?.service || '').trim() === canonicalService
  );

  if (!item) throw createApiError(404, 'Request not found for given service');
  if (item?.status !== VENDOR_STATUS.YET_TO_SELECT) {
    throw createApiError(409, 'Price can only be locked while request is pending');
  }

  const minRange = toMoney(item?.servicePrice?.min);
  const maxRange = toMoney(item?.servicePrice?.max);
  const cfg = await commissionService.getCommissionConfig();
  const percentRaw = Number(cfg?.rates?.[canonicalService] ?? 0);
  const configuredPercent = Number.isFinite(percentRaw) && percentRaw > 0 ? percentRaw : 0;
  const hikeRateRaw = Number(cfg?.vendorHikeRate);
  const vendorHikeRate = Number.isFinite(hikeRateRaw) && hikeRateRaw >= 1
    ? hikeRateRaw
    : commissionService.DEFAULT_VENDOR_HIKE_RATE;
  const normalizedPriceHikeReason = String(priceHikeReason || '').trim();

  let commissionPercent = 0;
  let commissionAmount = 0;
  let lockedPrice = quoted;

  const hasValidRange = minRange > 0 && maxRange > minRange;
  if (hasValidRange) {
    const quoteCap = toMoney(Math.min(maxRange, minRange * vendorHikeRate));
    if (quoted > quoteCap) {
      throw createApiError(
        400,
        `Quoted price cannot exceed ${quoteCap} for ${canonicalService}. Max allowed is min*${vendorHikeRate} based on booking range.`
      );
    }

    if (quoted > minRange && !normalizedPriceHikeReason) {
      throw createApiError(400, 'Please provide reason for price increase above minimum range');
    }

    if (configuredPercent > 0) {
      // Prefer admin-configured service commission when set.
      commissionPercent = configuredPercent;
      commissionAmount = toMoney((quoted * commissionPercent) / 100);
    } else {
      // Fallback behavior when no admin commission is configured.
      const commissionSlice = toMoney(Math.max(0, maxRange - quoteCap));
      commissionAmount = toMoney(Math.min(quoted, commissionSlice));
      commissionPercent = quoted > 0 ? toMoney((commissionAmount / quoted) * 100) : 0;
    }
    // Commission is included in vendor-entered quote, not added on top.
    lockedPrice = quoted;
  } else {
    commissionPercent = configuredPercent;
    commissionAmount = toMoney((quoted * commissionPercent) / 100);
    // Commission is included in vendor-entered quote, not added on top.
    lockedPrice = quoted;
  }

  item.vendorQuotedPrice = quoted;
  item.commissionPercent = commissionPercent;
  item.commissionAmount = commissionAmount;
  item.priceHikeReason = hasValidRange && quoted > minRange ? normalizedPriceHikeReason : null;
  item.priceLocked = true;

  selection.vendors = vendors;
  await selection.save();

  return {
    selection: selection.toObject(),
    service: canonicalService,
    quotedPrice: quoted,
    commissionPercent,
    commissionAmount,
    lockedPrice,
    priceHikeReason: item.priceHikeReason || null,
    vendorHikeRate,
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

  const itemsRequiringLock = targetItems.filter((v) => v?.status === VENDOR_STATUS.YET_TO_SELECT);
  if (itemsRequiringLock.length > 0) {
    const unlockedServices = itemsRequiringLock
      .filter((v) => !isVendorItemPriceLocked(v))
      .map((v) => String(v?.service || '').trim())
      .filter(Boolean);

    if (unlockedServices.length > 0) {
      throw createApiError(
        409,
        `Please provide and lock final quotation before ${action === 'accept' ? 'accepting' : 'declining'} for: ${unlockedServices.join(', ')}`
      );
    }
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
  releaseReservationsForPlanning,
  listSelectionsForVendor,
  getSelectionForVendorEvent,
  lockPriceForVendor,
  respondForVendor,
  createServiceChangeRequest,
  decideServiceChangeRequestByManager,
  submitVendorConsentForServiceChangeRequest,
};
