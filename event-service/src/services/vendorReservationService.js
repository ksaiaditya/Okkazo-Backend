const VendorReservation = require('../models/VendorReservation');
const Planning = require('../models/Planning');
const VendorSelection = require('../models/VendorSelection');
const { STATUS } = require('../utils/planningConstants');
const createApiError = require('../utils/ApiError');
const {
  toDateOrNull,
  toIstDayString,
  parseIstDayStart,
  normalizeIstDayInput,
  addDays,
} = require('../utils/istDateTime');

const HOLD_TTL_MS = Math.max(60 * 1000, Number(process.env.VENDOR_RESERVATION_HOLD_TTL_MS || 10 * 60 * 1000));
const STICKY_PLANNING_STATUSES = new Set([
  STATUS.CONFIRMED,
  STATUS.COMPLETED,
  STATUS.VENDOR_PAYMENT_PENDING,
]);
const VENUE_SERVICE_LABEL = 'Venue';
const DAY_MS = 24 * 60 * 60 * 1000;
const MAX_RANGE_DAYS = 366;

const normalizeDay = (day) => {
  const normalized = normalizeIstDayInput(day);
  if (!normalized) {
    throw createApiError(400, 'Planning day is required (YYYY-MM-DD)');
  }
  return normalized;
};

const sortUniqueDays = (days = []) => {
  const unique = Array.from(new Set((Array.isArray(days) ? days : [days]).map((d) => normalizeDayOrNull(d)).filter(Boolean)));
  return unique.sort((a, b) => {
    const da = parseIstDayStart(a)?.getTime?.() || 0;
    const db = parseIstDayStart(b)?.getTime?.() || 0;
    return da - db;
  });
};

const expandDayRangeInclusive = ({ from, to }) => {
  const startDay = normalizeDayOrNull(from);
  const endDay = normalizeDayOrNull(to);
  if (!startDay || !endDay) return [];

  let start = parseIstDayStart(startDay);
  let end = parseIstDayStart(endDay);
  if (!start || !end) return [];

  if (start.getTime() > end.getTime()) {
    const tmp = start;
    start = end;
    end = tmp;
  }

  const days = [];
  let cursor = new Date(start.getTime());
  let guard = 0;
  while (cursor.getTime() <= end.getTime() && guard < MAX_RANGE_DAYS) {
    const day = toIstDayString(cursor);
    if (day) days.push(day);
    cursor = new Date(cursor.getTime() + DAY_MS);
    guard += 1;
  }

  return sortUniqueDays(days);
};

const resolveReservationDays = ({ day = null, from = null, to = null, days = [] } = {}) => {
  const requestedDays = sortUniqueDays(days);
  const rangeDays = expandDayRangeInclusive({ from, to });
  const single = normalizeDayOrNull(day);

  const merged = [
    ...requestedDays,
    ...rangeDays,
    ...(single ? [single] : []),
  ];

  return sortUniqueDays(merged);
};

const planningToReservationDays = (planning) => {
  const category = String(planning?.category || '').trim().toLowerCase();
  if (category === 'public') {
    const from = toIstDayString(planning?.schedule?.startAt);
    const to = toIstDayString(planning?.schedule?.endAt || planning?.schedule?.startAt);
    const range = resolveReservationDays({ from, to });
    if (range.length > 0) return range;
  }

  const privateDay = toIstDayString(planning?.eventDate) || toIstDayString(planning?.schedule?.startAt);
  return privateDay ? [privateDay] : [];
};

const planningToDay = (planning) => {
  const days = planningToReservationDays(planning);
  return days[0] || null;
};

const buildExpiresAt = (now = new Date()) => new Date(now.getTime() + HOLD_TTL_MS);

const isVenueService = (service) => String(service || '').trim() === VENUE_SERVICE_LABEL;

const normalizeOptionalId = (value) => {
  const v = String(value || '').trim();
  return v || null;
};

const normalizeDayOrNull = (day) => {
  return normalizeIstDayInput(day);
};

const isStickyPlanningPolicy = (planning = {}) => {
  if (!planning || typeof planning !== 'object') return false;

  return (
    Boolean(planning.depositPaid) ||
    Boolean(planning.vendorConfirmationPaid) ||
    Boolean(planning.fullPaymentPaid) ||
    STICKY_PLANNING_STATUSES.has(String(planning.status || '').trim())
  );
};

const buildDayOverlapGate = (normalizedDay) => {
  const dayStart = parseIstDayStart(normalizedDay);
  const dayEnd = dayStart ? addDays(dayStart, 1) : null;
  if (!dayStart || !dayEnd) return null;

  return {
    dayStart,
    dayEnd,
    dateGate: {
      $or: [
        { eventDate: { $gte: dayStart, $lt: dayEnd } },
        {
          $and: [
            { 'schedule.startAt': { $lt: dayEnd } },
            { 'schedule.endAt': { $gte: dayStart } },
          ],
        },
        { 'schedule.startAt': { $gte: dayStart, $lt: dayEnd } },
      ],
    },
  };
};

const listFallbackSelectionLocksForDay = async ({ day, excludeEventId }) => {
  const normalizedDay = normalizeDay(day);
  const overlapGate = buildDayOverlapGate(normalizedDay);
  if (!overlapGate) {
    return { vendorAuthIds: [], serviceIds: [] };
  }

  const planningQuery = {
    $and: [overlapGate.dateGate],
  };

  if (excludeEventId && String(excludeEventId).trim()) {
    planningQuery.eventId = { $ne: String(excludeEventId).trim() };
  }

  const overlappingPlannings = await Planning.find(planningQuery)
    .select({
      eventId: 1,
      status: 1,
      depositPaid: 1,
      vendorConfirmationPaid: 1,
      fullPaymentPaid: 1,
      selectedVendors: 1,
    })
    .lean();

  if (!Array.isArray(overlappingPlannings) || overlappingPlannings.length === 0) {
    return { vendorAuthIds: [], serviceIds: [] };
  }

  const planningByEventId = new Map();
  const eventIds = [];
  for (const planning of overlappingPlannings) {
    const eventId = String(planning?.eventId || '').trim();
    if (!eventId) continue;
    planningByEventId.set(eventId, planning);
    eventIds.push(eventId);
  }

  if (eventIds.length === 0) {
    return { vendorAuthIds: [], serviceIds: [] };
  }

  const selections = await VendorSelection.find({ eventId: { $in: eventIds } })
    .select({ eventId: 1, vendors: 1, updatedAt: 1 })
    .lean();

  const selectionByEventId = new Map();
  for (const selection of (Array.isArray(selections) ? selections : [])) {
    const eventId = String(selection?.eventId || '').trim();
    if (!eventId) continue;
    selectionByEventId.set(eventId, selection);
  }

  const now = new Date();
  const vendorAuthIds = [];
  const serviceIds = [];

  for (const eventId of eventIds) {
    const planning = planningByEventId.get(eventId);
    if (!planning) continue;

    const planningStatus = String(planning?.status || '').trim().toUpperCase();
    if (planningStatus === 'CANCELLED' || planningStatus === 'CANCELED' || planningStatus === 'CLOSED') {
      // Cancelled/closed events must not keep vendors blocked for new selections.
      continue;
    }

    const stickyPlanning = isStickyPlanningPolicy(planning);
    const selection = selectionByEventId.get(eventId);
    const selectionUpdatedAt = toDateOrNull(selection?.updatedAt);
    const recentSelection = Boolean(
      selectionUpdatedAt && (selectionUpdatedAt.getTime() + HOLD_TTL_MS > now.getTime())
    );

    const considerSelectionRows = Boolean(selection && (stickyPlanning || recentSelection));
    if (considerSelectionRows) {
      const vendorRows = Array.isArray(selection?.vendors) ? selection.vendors : [];
      for (const row of vendorRows) {
        const vendorAuthId = String(row?.vendorAuthId || '').trim();
        if (!vendorAuthId) continue;

        if (isVenueService(row?.service)) {
          const sid = normalizeOptionalId(row?.serviceId);
          if (sid) serviceIds.push(sid);
          continue;
        }

        vendorAuthIds.push(vendorAuthId);
      }
    }

    // Legacy sticky fallback from planning snapshot.
    if (stickyPlanning) {
      const selected = Array.isArray(planning?.selectedVendors) ? planning.selectedVendors : [];
      for (const row of selected) {
        const vendorAuthId = String(row?.vendorAuthId || '').trim();
        if (vendorAuthId) vendorAuthIds.push(vendorAuthId);
      }
    }
  }

  return {
    vendorAuthIds: Array.from(new Set(vendorAuthIds.filter(Boolean))),
    serviceIds: Array.from(new Set(serviceIds.filter(Boolean))),
  };
};

const buildReservationIdentity = ({ vendorAuthId, service, serviceId }) => {
  const vendor = String(vendorAuthId || '').trim();
  if (!vendor) throw createApiError(400, 'vendorAuthId is required');

  const normalizedService = String(service || '').trim() || null;
  const normalizedServiceId = normalizeOptionalId(serviceId);

  if (isVenueService(normalizedService) && normalizedServiceId) {
    return {
      lockId: `service:${normalizedServiceId}`,
      ownerVendorAuthId: vendor,
      serviceId: normalizedServiceId,
      service: normalizedService,
    };
  }

  return {
    lockId: vendor,
    ownerVendorAuthId: vendor,
    // Non-venue locks are vendor-level only.
    serviceId: null,
    service: normalizedService,
  };
};

const getReservationPolicyForEvent = async ({ eventId }) => {
  const eid = String(eventId || '').trim();
  if (!eid) {
    return {
      sticky: false,
      planningDay: null,
    };
  }

  const planning = await Planning.findOne({ eventId: eid })
    .select('status depositPaid vendorConfirmationPaid fullPaymentPaid category eventDate schedule.startAt schedule.endAt')
    .lean();

  if (!planning) {
    return {
      sticky: false,
      planningDay: null,
    };
  }

  const hasPaymentProgress =
    Boolean(planning.depositPaid) ||
    Boolean(planning.vendorConfirmationPaid) ||
    Boolean(planning.fullPaymentPaid);

  return {
    sticky: hasPaymentProgress || STICKY_PLANNING_STATUSES.has(String(planning.status || '').trim()),
    planningDay: normalizeDayOrNull(planningToDay(planning)),
    planningDays: planningToReservationDays(planning),
  };
};

const reassignEventReservationsDay = async ({ eventId, fromDay, toDay }) => {
  const eid = String(eventId || '').trim();
  if (!eid) throw createApiError(400, 'eventId is required');

  const sourceDay = normalizeDayOrNull(fromDay);
  const targetDay = normalizeDay(toDay);
  if (!sourceDay) {
    return {
      scanned: 0,
      moved: 0,
      removed: 0,
      conflicts: 0,
      fromDay: null,
      toDay: targetDay,
    };
  }

  if (sourceDay === targetDay) {
    return {
      scanned: 0,
      moved: 0,
      removed: 0,
      conflicts: 0,
      fromDay: sourceDay,
      toDay: targetDay,
    };
  }

  const sourceRows = await VendorReservation.find({ eventId: eid, day: sourceDay })
    .select({ _id: 1, vendorAuthId: 1, ownerVendorAuthId: 1, service: 1, serviceId: 1, eventId: 1, authId: 1, expiresAt: 1, createdAt: 1 })
    .lean();

  let moved = 0;
  let removed = 0;
  let conflicts = 0;

  for (const row of sourceRows) {
    if (!row?._id) continue;

    const target = await VendorReservation.findOne({ vendorAuthId: row.vendorAuthId, day: targetDay })
      .select({ _id: 1, eventId: 1 })
      .lean();

    if (target?._id) {
      if (String(target.eventId || '').trim() === eid) {
        await VendorReservation.updateOne(
          { _id: target._id },
          {
            $set: {
              ownerVendorAuthId: row.ownerVendorAuthId || row.vendorAuthId || null,
              service: row.service || null,
              serviceId: normalizeOptionalId(row.serviceId),
              authId: row.authId || null,
              eventId: eid,
              expiresAt: row.expiresAt ?? null,
            },
          }
        );
      } else {
        conflicts += 1;
      }

      const del = await VendorReservation.deleteOne({ _id: row._id });
      removed += Number(del?.deletedCount || 0);
      continue;
    }

    try {
      const upd = await VendorReservation.updateOne(
        { _id: row._id },
        { $set: { day: targetDay } }
      );
      moved += Number(upd?.modifiedCount || 0);
    } catch (error) {
      // Duplicate key can happen under races; drop stale source row as fallback.
      if (error && (error.code === 11000 || String(error.message || '').includes('E11000'))) {
        conflicts += 1;
        const del = await VendorReservation.deleteOne({ _id: row._id });
        removed += Number(del?.deletedCount || 0);
        continue;
      }
      throw error;
    }
  }

  return {
    scanned: sourceRows.length,
    moved,
    removed,
    conflicts,
    fromDay: sourceDay,
    toDay: targetDay,
  };
};

const isReservationExpired = ({ reservation, now, sticky }) => {
  if (sticky) return false;

  const expiresAt = toDateOrNull(reservation?.expiresAt);
  if (expiresAt) return expiresAt <= now;

  // Legacy rows without expiresAt: infer expiry from creation time.
  const createdAt = toDateOrNull(reservation?.createdAt);
  if (!createdAt) return false;
  return createdAt.getTime() + HOLD_TTL_MS <= now.getTime();
};

const normalizeOrReleaseReservation = async ({ reservation, now, stickyCache }) => {
  if (!reservation?._id) {
    return { active: false, sticky: false, expired: true };
  }

  const eventId = String(reservation.eventId || '').trim();
  if (!eventId) {
    await VendorReservation.deleteOne({ _id: reservation._id });
    return { active: false, sticky: false, expired: true };
  }

  let policy = stickyCache.get(eventId);
  if (policy === undefined) {
    policy = await getReservationPolicyForEvent({ eventId });
    stickyCache.set(eventId, policy);
  }

  const sticky = Boolean(policy?.sticky);
  const planningDay = normalizeDayOrNull(policy?.planningDay);
  const planningDays = sortUniqueDays(policy?.planningDays || (planningDay ? [planningDay] : []));
  const planningDaySet = new Set(planningDays);
  const reservationDay = normalizeDayOrNull(reservation.day);

  if (!reservationDay) {
    await VendorReservation.deleteOne({ _id: reservation._id });
    return { active: false, sticky, expired: true };
  }

  // Sticky reservations must stay within the active planning reservation window.
  if (sticky && planningDaySet.size > 0 && !planningDaySet.has(reservationDay)) {
    if (planningDaySet.size === 1 && planningDay) {
      await reassignEventReservationsDay({
        eventId,
        fromDay: reservationDay,
        toDay: planningDay,
      });
    } else {
      await VendorReservation.deleteOne({ _id: reservation._id });
    }
    return { active: false, sticky, expired: false };
  }

  const expired = isReservationExpired({ reservation, now, sticky });
  if (expired) {
    await VendorReservation.deleteOne({ _id: reservation._id });
    return { active: false, sticky, expired: true };
  }

  const currentExpiresAt = toDateOrNull(reservation.expiresAt);

  // Read paths must never extend a hold window. We only normalize legacy rows.
  if (sticky) {
    if (currentExpiresAt !== null) {
      await VendorReservation.updateOne(
        { _id: reservation._id },
        { $set: { expiresAt: null } }
      );
    }
    return { active: true, sticky, expired: false };
  }

  // Legacy non-sticky rows may have null expiresAt; infer a fixed expiry once.
  if (!currentExpiresAt) {
    const createdAt = toDateOrNull(reservation.createdAt);
    const inferredExpiresAt = createdAt
      ? new Date(createdAt.getTime() + HOLD_TTL_MS)
      : buildExpiresAt(now);

    await VendorReservation.updateOne(
      { _id: reservation._id },
      { $set: { expiresAt: inferredExpiresAt } }
    );
  }

  return { active: true, sticky, expired: false };
};

const claim = async ({ vendorAuthId, day, eventId, authId, service, serviceId }) => {
  const vendor = String(vendorAuthId || '').trim();
  const eid = String(eventId || '').trim();
  const uid = String(authId || '').trim();

  if (!vendor) throw createApiError(400, 'vendorAuthId is required');
  if (!eid) throw createApiError(400, 'eventId is required');
  if (!uid) throw createApiError(400, 'authId is required');

  const identity = buildReservationIdentity({ vendorAuthId: vendor, service, serviceId });
  const normalizedDay = normalizeDay(day);
  const now = new Date();
  const stickyCache = new Map();

  // If already reserved, normalize stale/legacy rows before enforcing conflict.
  const existing = await VendorReservation.findOne({ vendorAuthId: identity.lockId, day: normalizedDay }).lean();
  if (existing) {
    const state = await normalizeOrReleaseReservation({ reservation: existing, now, stickyCache });
    if (!state.active) {
      // Stale hold released; continue and claim below.
    } else if (existing.eventId !== eid) {
      throw createApiError(409, 'Vendor is not available for the selected date');
    } else {
      const sticky = state.sticky;
      const expiresAt = sticky ? null : buildExpiresAt(now);

      // Same event: refresh metadata + hold window.
      await VendorReservation.updateOne(
        { _id: existing._id },
        {
          $set: {
            authId: uid,
            ownerVendorAuthId: identity.ownerVendorAuthId,
            service: identity.service || existing.service,
            serviceId: identity.serviceId,
            expiresAt,
          },
        }
      );
      return {
        ...existing,
        authId: uid,
        ownerVendorAuthId: identity.ownerVendorAuthId,
        service: identity.service || existing.service,
        serviceId: identity.serviceId,
        expiresAt,
      };
    }
  }

  // Safety net: if the reservation row is missing due races/transient drops,
  // still enforce conflict using selection-backed active locks from other events.
  if (String(identity.lockId || '').startsWith('service:')) {
    const sid = normalizeOptionalId(identity.serviceId);
    if (sid) {
      const reservedServiceIds = await listReservedServiceIdsForDay({
        day: normalizedDay,
        excludeEventId: eid,
      });

      if (reservedServiceIds.includes(sid)) {
        throw createApiError(409, 'Vendor is not available for the selected date');
      }
    }
  } else {
    const reservedVendorIds = await listReservedVendorAuthIdsForDay({
      day: normalizedDay,
      excludeEventId: eid,
    });

    if (reservedVendorIds.includes(identity.lockId)) {
      throw createApiError(409, 'Vendor is not available for the selected date');
    }
  }

  const policy = await getReservationPolicyForEvent({ eventId: eid });
  const sticky = Boolean(policy?.sticky);
  const expiresAt = sticky ? null : buildExpiresAt(now);

  try {
    const doc = await VendorReservation.create({
      vendorAuthId: identity.lockId,
      ownerVendorAuthId: identity.ownerVendorAuthId,
      serviceId: identity.serviceId,
      day: normalizedDay,
      eventId: eid,
      authId: uid,
      service: identity.service,
      expiresAt,
    });

    return doc.toObject();
  } catch (e) {
    // Duplicate key race: someone else reserved simultaneously
    if (e && (e.code === 11000 || String(e.message || '').includes('E11000'))) {
      throw createApiError(409, 'Vendor is not available for the selected date');
    }
    throw e;
  }
};

const claimForDays = async ({ vendorAuthId, days = [], eventId, authId, service, serviceId }) => {
  const normalizedDays = sortUniqueDays(days);
  if (normalizedDays.length === 0) {
    throw createApiError(400, 'At least one reservation day is required');
  }

  const claimedDays = [];
  const reservations = [];

  for (const day of normalizedDays) {
    try {
      const reservation = await claim({ vendorAuthId, day, eventId, authId, service, serviceId });
      reservations.push(reservation);
      claimedDays.push(day);
    } catch (error) {
      if (claimedDays.length > 0) {
        await Promise.all(
          claimedDays.map(async (claimedDay) => {
            try {
              await release({ vendorAuthId, day: claimedDay, eventId, service, serviceId });
            } catch {
              // best-effort rollback only
            }
          })
        );
      }
      throw error;
    }
  }

  return {
    claimedDays,
    reservations,
  };
};

const release = async ({ vendorAuthId, day, eventId, service, serviceId }) => {
  const vendor = String(vendorAuthId || '').trim();
  const eid = String(eventId || '').trim();
  if (!vendor) throw createApiError(400, 'vendorAuthId is required');
  if (!eid) throw createApiError(400, 'eventId is required');

  const identity = buildReservationIdentity({ vendorAuthId: vendor, service, serviceId });
  const normalizedDay = normalizeDay(day);

  // Only release if this event owns the reservation
  const existing = await VendorReservation.findOne({ vendorAuthId: identity.lockId, day: normalizedDay }).lean();
  if (!existing) return { removed: 0 };
  if (existing.eventId !== eid) return { removed: 0 };

  const result = await VendorReservation.deleteOne({ _id: existing._id });
  return { removed: result.deletedCount || 0 };
};

const releaseForDays = async ({ vendorAuthId, days = [], eventId, service, serviceId }) => {
  const normalizedDays = sortUniqueDays(days);
  if (normalizedDays.length === 0) {
    return { removed: 0, days: [] };
  }

  const removedByDay = await Promise.all(
    normalizedDays.map(async (day) => {
      const result = await release({ vendorAuthId, day, eventId, service, serviceId });
      return Number(result?.removed || 0);
    })
  );

  return {
    removed: removedByDay.reduce((acc, n) => acc + n, 0),
    days: normalizedDays,
  };
};

const releasePriorReservationsForEventService = async ({ eventId, day, service, serviceId, keepVendorAuthId = null }) => {
  const eid = String(eventId || '').trim();
  if (!eid) throw createApiError(400, 'eventId is required');

  const normalizedDay = normalizeDay(day);
  const normalizedService = String(service || '').trim();
  if (!normalizedService) {
    return { removed: 0 };
  }

  const query = {
    eventId: eid,
    day: normalizedDay,
    service: normalizedService,
  };

  if (isVenueService(normalizedService)) {
    query.$or = [
      { service: VENUE_SERVICE_LABEL },
      { vendorAuthId: { $regex: '^service:' } },
    ];
  }

  if (keepVendorAuthId != null && String(keepVendorAuthId).trim()) {
    const keepIdentity = buildReservationIdentity({
      vendorAuthId: String(keepVendorAuthId).trim(),
      service: normalizedService,
      serviceId,
    });
    query.vendorAuthId = { $ne: keepIdentity.lockId };
  }

  const result = await VendorReservation.deleteMany(query);
  return { removed: result.deletedCount || 0 };
};

const releasePriorReservationsForEventServiceDays = async ({ eventId, days = [], service, serviceId, keepVendorAuthId = null }) => {
  const normalizedDays = sortUniqueDays(days);
  if (normalizedDays.length === 0) {
    return { removed: 0, days: [] };
  }

  let removed = 0;
  for (const day of normalizedDays) {
    const result = await releasePriorReservationsForEventService({
      eventId,
      day,
      service,
      serviceId,
      keepVendorAuthId,
    });
    removed += Number(result?.removed || 0);
  }

  return {
    removed,
    days: normalizedDays,
  };
};

const listReservedVendorAuthIdsForDay = async ({ day, excludeEventId }) => {
  const normalizedDay = normalizeDay(day);
  const query = { day: normalizedDay };
  if (excludeEventId && String(excludeEventId).trim()) {
    query.eventId = { $ne: String(excludeEventId).trim() };
  }

  const now = new Date();
  const stickyCache = new Map();
  const docs = await VendorReservation.find(query)
    .select({ _id: 1, vendorAuthId: 1, ownerVendorAuthId: 1, service: 1, serviceId: 1, eventId: 1, expiresAt: 1, createdAt: 1 })
    .lean();

  const activeVendorIds = [];
  for (const doc of docs) {
    const state = await normalizeOrReleaseReservation({ reservation: doc, now, stickyCache });
    if (!state.active) continue;

    // Vendor-level locks only (non-Venue). Venue service locks are tracked separately.
    const isServiceLock =
      String(doc?.vendorAuthId || '').startsWith('service:') ||
      isVenueService(doc?.service);
    if (!isServiceLock && doc?.vendorAuthId) {
      activeVendorIds.push(doc.ownerVendorAuthId || doc.vendorAuthId);
    }
  }

  // Fallback: include selected vendors from sticky (paid/confirmed/completed) plannings on the same day.
  // This protects availability filtering for older events where a sticky VendorReservation row was never created
  // (e.g., a temporary hold expired before payment was recorded).
  try {
    const fallback = await listFallbackSelectionLocksForDay({ day: normalizedDay, excludeEventId });
    const vendorAuthIds = Array.isArray(fallback?.vendorAuthIds) ? fallback.vendorAuthIds : [];
    activeVendorIds.push(...vendorAuthIds);
  } catch (error) {
    // Best-effort only; primary source of truth remains VendorReservation collection.
  }

  return Array.from(new Set(activeVendorIds.filter(Boolean)));
};

const listReservedVendorAuthIdsForDays = async ({ days = [], excludeEventId }) => {
  const normalizedDays = sortUniqueDays(days);
  if (normalizedDays.length === 0) return [];

  const reserved = await Promise.all(
    normalizedDays.map((day) => listReservedVendorAuthIdsForDay({ day, excludeEventId }))
  );

  return Array.from(new Set(reserved.flat().map((v) => String(v || '').trim()).filter(Boolean)));
};

const listReservedServiceIdsForDay = async ({ day, excludeEventId }) => {
  const normalizedDay = normalizeDay(day);
  const query = { day: normalizedDay };
  if (excludeEventId && String(excludeEventId).trim()) {
    query.eventId = { $ne: String(excludeEventId).trim() };
  }

  const now = new Date();
  const stickyCache = new Map();
  const docs = await VendorReservation.find(query)
    .select({ _id: 1, vendorAuthId: 1, service: 1, serviceId: 1, eventId: 1, expiresAt: 1, createdAt: 1 })
    .lean();

  const activeServiceIds = [];
  for (const doc of docs) {
    const state = await normalizeOrReleaseReservation({ reservation: doc, now, stickyCache });
    if (!state.active) continue;

    const isVenueLock =
      String(doc?.vendorAuthId || '').startsWith('service:') ||
      isVenueService(doc?.service);
    if (!isVenueLock) continue;

    const sid = normalizeOptionalId(doc?.serviceId)
      || (() => {
        const raw = String(doc?.vendorAuthId || '').trim();
        if (!raw.startsWith('service:')) return null;
        return normalizeOptionalId(raw.slice('service:'.length));
      })();

    if (sid) activeServiceIds.push(sid);
  }

  // Fallback: include selected venue serviceIds from sticky/recent selections
  // when reservation rows are missing.
  try {
    const fallback = await listFallbackSelectionLocksForDay({ day: normalizedDay, excludeEventId });
    const serviceIds = Array.isArray(fallback?.serviceIds) ? fallback.serviceIds : [];
    activeServiceIds.push(...serviceIds);
  } catch (error) {
    // Best-effort only; primary source of truth remains VendorReservation collection.
  }

  return Array.from(new Set(activeServiceIds));
};

const listReservedServiceIdsForDays = async ({ days = [], excludeEventId }) => {
  const normalizedDays = sortUniqueDays(days);
  if (normalizedDays.length === 0) return [];

  const reserved = await Promise.all(
    normalizedDays.map((day) => listReservedServiceIdsForDay({ day, excludeEventId }))
  );

  return Array.from(new Set(reserved.flat().map((v) => String(v || '').trim()).filter(Boolean)));
};

const listActiveReservationsForEventDays = async ({ eventId, days = [] }) => {
  const eid = String(eventId || '').trim();
  if (!eid) throw createApiError(400, 'eventId is required');

  const requestedDays = Array.isArray(days) ? days : [days];
  const normalizedDays = Array.from(new Set(requestedDays.map((d) => normalizeDayOrNull(d)).filter(Boolean)));
  if (normalizedDays.length === 0) return [];

  const now = new Date();
  const stickyCache = new Map();
  const docs = await VendorReservation.find({
    eventId: eid,
    day: { $in: normalizedDays },
  })
    .select({ _id: 1, vendorAuthId: 1, ownerVendorAuthId: 1, service: 1, serviceId: 1, day: 1, eventId: 1, expiresAt: 1, createdAt: 1 })
    .lean();

  const active = [];
  for (const doc of docs) {
    const state = await normalizeOrReleaseReservation({ reservation: doc, now, stickyCache });
    if (!state.active) continue;
    active.push(doc);
  }

  return active;
};

const isHeldByEvent = async ({ vendorAuthId, day, eventId, service, serviceId }) => {
  const vendor = String(vendorAuthId || '').trim();
  const eid = String(eventId || '').trim();
  if (!vendor) throw createApiError(400, 'vendorAuthId is required');
  if (!eid) throw createApiError(400, 'eventId is required');

  const identity = buildReservationIdentity({ vendorAuthId: vendor, service, serviceId });
  const normalizedDay = normalizeDay(day);

  const now = new Date();
  const stickyCache = new Map();
  const existing = await VendorReservation.findOne({
    vendorAuthId: identity.lockId,
    day: normalizedDay,
  })
    .select({ _id: 1, vendorAuthId: 1, ownerVendorAuthId: 1, service: 1, serviceId: 1, eventId: 1, expiresAt: 1, createdAt: 1 })
    .lean();

  if (!existing) return false;

  const state = await normalizeOrReleaseReservation({ reservation: existing, now, stickyCache });
  if (!state.active) return false;

  return String(existing.eventId || '').trim() === eid;
};

const isHeldByEventForDays = async ({ vendorAuthId, days = [], eventId, service, serviceId }) => {
  const normalizedDays = sortUniqueDays(days);
  if (normalizedDays.length === 0) return false;

  for (const day of normalizedDays) {
    const held = await isHeldByEvent({ vendorAuthId, day, eventId, service, serviceId });
    if (!held) return false;
  }

  return true;
};

module.exports = {
  resolveReservationDays,
  planningToReservationDays,
  planningToDay,
  claim,
  claimForDays,
  release,
  releaseForDays,
  reassignEventReservationsDay,
  releasePriorReservationsForEventService,
  releasePriorReservationsForEventServiceDays,
  listReservedVendorAuthIdsForDay,
  listReservedVendorAuthIdsForDays,
  listReservedServiceIdsForDay,
  listReservedServiceIdsForDays,
  listActiveReservationsForEventDays,
  isHeldByEvent,
  isHeldByEventForDays,
};
