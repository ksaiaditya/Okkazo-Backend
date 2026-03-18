const VendorReservation = require('../models/VendorReservation');
const createApiError = require('../utils/ApiError');

const DAY_RE = /^\d{4}-\d{2}-\d{2}$/;

const normalizeDay = (day) => {
  const d = String(day || '').trim();
  if (!d || !DAY_RE.test(d)) {
    throw createApiError(400, 'Planning day is required (YYYY-MM-DD)');
  }
  return d;
};

const planningToDay = (planning) => {
  const dt =
    (planning?.eventDate instanceof Date && !isNaN(planning.eventDate))
      ? planning.eventDate
      : ((planning?.schedule?.startAt instanceof Date && !isNaN(planning.schedule.startAt))
        ? planning.schedule.startAt
        : null);

  if (!dt) return null;
  return dt.toISOString().slice(0, 10);
};

const claim = async ({ vendorAuthId, day, eventId, authId, service }) => {
  const vendor = String(vendorAuthId || '').trim();
  const eid = String(eventId || '').trim();
  const uid = String(authId || '').trim();

  if (!vendor) throw createApiError(400, 'vendorAuthId is required');
  if (!eid) throw createApiError(400, 'eventId is required');
  if (!uid) throw createApiError(400, 'authId is required');

  const normalizedDay = normalizeDay(day);

  // If already reserved by same event, keep it.
  const existing = await VendorReservation.findOne({ vendorAuthId: vendor, day: normalizedDay }).lean();
  if (existing) {
    if (existing.eventId !== eid) {
      throw createApiError(409, 'Vendor is not available for the selected date');
    }

    // Same event: update metadata best-effort
    await VendorReservation.updateOne(
      { _id: existing._id },
      { $set: { authId: uid, service: service ? String(service).trim() : existing.service } }
    );
    return existing;
  }

  try {
    const doc = await VendorReservation.create({
      vendorAuthId: vendor,
      day: normalizedDay,
      eventId: eid,
      authId: uid,
      service: service ? String(service).trim() : null,
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

const release = async ({ vendorAuthId, day, eventId }) => {
  const vendor = String(vendorAuthId || '').trim();
  const eid = String(eventId || '').trim();
  if (!vendor) throw createApiError(400, 'vendorAuthId is required');
  if (!eid) throw createApiError(400, 'eventId is required');

  const normalizedDay = normalizeDay(day);

  // Only release if this event owns the reservation
  const existing = await VendorReservation.findOne({ vendorAuthId: vendor, day: normalizedDay }).lean();
  if (!existing) return { removed: 0 };
  if (existing.eventId !== eid) return { removed: 0 };

  const result = await VendorReservation.deleteOne({ _id: existing._id });
  return { removed: result.deletedCount || 0 };
};

const listReservedVendorAuthIdsForDay = async ({ day, excludeEventId }) => {
  const normalizedDay = normalizeDay(day);
  const query = { day: normalizedDay };
  if (excludeEventId && String(excludeEventId).trim()) {
    query.eventId = { $ne: String(excludeEventId).trim() };
  }

  const docs = await VendorReservation.find(query).select({ vendorAuthId: 1, _id: 0 }).lean();
  return Array.from(new Set(docs.map((d) => d.vendorAuthId).filter(Boolean)));
};

module.exports = {
  planningToDay,
  claim,
  release,
  listReservedVendorAuthIdsForDay,
};
