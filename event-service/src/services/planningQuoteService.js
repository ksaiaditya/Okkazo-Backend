const Planning = require('../models/Planning');
const VendorSelection = require('../models/VendorSelection');
const PlanningQuoteSnapshot = require('../models/PlanningQuoteSnapshot');
const createApiError = require('../utils/ApiError');
const commissionService = require('./commissionService');
const promoteConfigService = require('./promoteConfigService');
const promotionConfigService = require('./promotionConfigService');
const { publishEvent } = require('../kafka/eventProducer');
const logger = require('../utils/logger');
const { STATUS, CATEGORY } = require('../utils/planningConstants');

const HIGH_DEMAND_WINDOW_DAYS = 20;

const toPaise = (inr) => {
  const n = Number(inr || 0);
  if (!Number.isFinite(n)) return 0;
  return Math.max(0, Math.round(n * 100));
};

const fromPaiseToInr = (paise) => {
  const n = Number(paise || 0);
  if (!Number.isFinite(n)) return 0;
  return Math.round(n) / 100;
};

const normalizeServiceKey = (value) => String(value || '').trim();

const getEventStartAt = (planning) => {
  if (!planning) return null;
  if (String(planning.category || '').trim() === CATEGORY.PUBLIC) {
    return planning.schedule?.startAt ? new Date(planning.schedule.startAt) : null;
  }
  return planning.eventDate ? new Date(planning.eventDate) : null;
};

const computeDaysUntil = (eventStartAt) => {
  if (!eventStartAt) return null;
  const now = new Date();
  const start = new Date(eventStartAt);
  const ms = start.getTime() - now.getTime();
  const days = Math.ceil(ms / (24 * 60 * 60 * 1000));
  return Number.isFinite(days) ? days : null;
};

const getDemandTier = (daysUntilEvent) => {
  const d = Number(daysUntilEvent);
  if (!Number.isFinite(d)) return 'NORMAL';
  return d <= HIGH_DEMAND_WINDOW_DAYS ? 'HIGH_DEMAND' : 'NORMAL';
};

const toValidMultiplier = (value, fallback) => {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return n;
};

const getTierMultipliers = ({ tier, feesConfig }) => {
  const normalMin = toValidMultiplier(feesConfig?.demandPricingMultipliers?.normal?.min, 1);
  const normalMaxRaw = toValidMultiplier(feesConfig?.demandPricingMultipliers?.normal?.max, 1);
  const normalMax = normalMaxRaw >= normalMin ? normalMaxRaw : normalMin;

  const highMin = toValidMultiplier(feesConfig?.demandPricingMultipliers?.highDemand?.min, 1.5);
  const highMaxRaw = toValidMultiplier(feesConfig?.demandPricingMultipliers?.highDemand?.max, 2.25);
  const highMax = highMaxRaw >= highMin ? highMaxRaw : highMin;

  if (tier === 'HIGH_DEMAND') {
    return { min: highMin, max: highMax };
  }
  return { min: normalMin, max: normalMax };
};

const applyMultiplierPaise = (paise, multiplier) => {
  const n = Number(paise || 0);
  const m = Number(multiplier || 1);
  if (!Number.isFinite(n) || !Number.isFinite(m)) return 0;
  return Math.max(0, Math.round(n * m));
};

const lockQuoteAtApproved = async ({ eventId, lockedByAuthId = null } = {}) => {
  if (!eventId || !String(eventId).trim()) {
    throw createApiError(400, 'eventId is required');
  }

  const planning = await Planning.findOne({ eventId: String(eventId).trim() }).lean();
  if (!planning) throw createApiError(404, 'Planning not found');

  if (String(planning.status || '').trim() !== STATUS.APPROVED) {
    throw createApiError(409, 'Quote can only be locked when planning status is APPROVED');
  }

  const existing = await PlanningQuoteSnapshot.findOne({ eventId: String(eventId).trim(), statusAtLock: STATUS.APPROVED })
    .sort({ version: -1 })
    .lean();
  const existingVersion = Number(existing?.version || 0);
  const stampedVersion = Number(planning?.quoteLockedVersion || 0);
  if (existing && stampedVersion > 0 && existingVersion === stampedVersion) {
    try {
      await publishEvent(
        'PLANNING_QUOTE_LOCKED',
        {
          eventId: String(eventId).trim(),
          authId: String(existing.authId || planning.authId || '').trim() || null,
          version: existing.version,
        },
        `${String(eventId).trim()}:${existing.version}`
      );
    } catch (e) {
      logger.warn('Failed to publish PLANNING_QUOTE_LOCKED event (existing snapshot)', {
        eventId,
        message: e?.message,
      });
    }

    return existing;
  }

  const eventStartAt = getEventStartAt(planning);
  if (!eventStartAt) throw createApiError(409, 'Event start date is not available to build quote');

  const daysUntilEvent = computeDaysUntil(eventStartAt);
  if (daysUntilEvent == null) throw createApiError(409, 'Unable to compute daysUntilEvent');

  const demandTier = getDemandTier(daysUntilEvent);
  const feesConfig = await promoteConfigService.getFees();
  const multipliers = getTierMultipliers({ tier: demandTier, feesConfig });

  const selectionDoc = await VendorSelection.findOne({ eventId: String(eventId).trim() });
  if (!selectionDoc) {
    throw createApiError(409, 'VendorSelection is required to lock quote');
  }

  const selection = selectionDoc.toObject();
  const selectedServices = Array.isArray(selectionDoc.selectedServices) ? selectionDoc.selectedServices : [];
  const selectedSet = new Set(selectedServices.map(normalizeServiceKey));

  const { rates: commissionRates } = await commissionService.getCommissionConfig();

  // Public event promotions (visibility boost) are client-facing add-ons.
  // They should affect clientGrandTotal but not vendorSubtotal.
  let promotions = [];
  let promotionsTotalPaise = 0;
  if (String(planning.category || '').trim() === CATEGORY.PUBLIC) {
    const selectedPromos = Array.isArray(planning.promotionType) ? planning.promotionType : [];
    if (selectedPromos.length > 0) {
      try {
        const cfg = await promotionConfigService.getPromotions();
        const byValue = new Map(
          (Array.isArray(cfg?.publicPromotionOptions) ? cfg.publicPromotionOptions : [])
            .filter((it) => it && typeof it.value === 'string')
            .map((it) => [String(it.value).trim().toLowerCase(), it])
        );

        for (const raw of selectedPromos) {
          const value = String(raw || '').trim();
          if (!value) continue;
          const matched = byValue.get(value.toLowerCase());
          const feeInr = Number(matched?.fee ?? 0);
          const feePaise = Number.isFinite(feeInr) && feeInr > 0 ? Math.round(feeInr * 100) : 0;
          promotions.push({ value, feePaise });
          promotionsTotalPaise += feePaise;
        }
      } catch (err) {
        logger.warn('Failed to resolve promotion fees for quote snapshot', {
          eventId,
          message: err?.message,
        });
      }
    }
  }

  const byService = new Map();
  for (const v of Array.isArray(selection.vendors) ? selection.vendors : []) {
    const service = normalizeServiceKey(v?.service);
    if (!service || !selectedSet.has(service)) continue;
    byService.set(service, v);
  }

  let vendorMinTotalPaise = 0;
  let vendorMaxTotalPaise = 0;
  let serviceChargeMinPaise = 0;
  let serviceChargeMaxPaise = 0;

  const items = [];
  for (const service of selectedServices.map(normalizeServiceKey)) {
    const item = byService.get(service);
    const baseMinPaise = toPaise(item?.servicePrice?.min);
    const baseMaxPaise = toPaise(item?.servicePrice?.max || item?.servicePrice?.min);
    const vendorMinPaise = applyMultiplierPaise(baseMinPaise, multipliers.min);
    const vendorMaxPaise = applyMultiplierPaise(baseMaxPaise, multipliers.max);

    const percent = Number(commissionRates?.[service] ?? 0);
    const safePercent = Number.isFinite(percent) && percent >= 0 ? percent : 0;

    const chargeMin = Math.max(0, Math.round((vendorMinPaise * safePercent) / 100));
    const chargeMax = Math.max(0, Math.round((vendorMaxPaise * safePercent) / 100));

    vendorMinTotalPaise += vendorMinPaise;
    vendorMaxTotalPaise += vendorMaxPaise;
    serviceChargeMinPaise += chargeMin;
    serviceChargeMaxPaise += chargeMax;

    items.push({
      service,
      vendorAuthId: item?.vendorAuthId ? String(item.vendorAuthId).trim() : null,
      vendorTotal: { minPaise: vendorMinPaise, maxPaise: vendorMaxPaise },
      commissionPercent: safePercent,
      serviceCharge: { minPaise: chargeMin, maxPaise: chargeMax },
      clientTotal: {
        minPaise: vendorMinPaise + chargeMin,
        maxPaise: vendorMaxPaise + chargeMax,
      },
    });
  }

  const vendorSubtotal = { minPaise: vendorMinTotalPaise, maxPaise: vendorMaxTotalPaise };
  const serviceChargeTotal = { minPaise: serviceChargeMinPaise, maxPaise: serviceChargeMaxPaise };
  const promotionsTotal = { minPaise: promotionsTotalPaise, maxPaise: promotionsTotalPaise };
  const clientGrandTotal = {
    minPaise: vendorMinTotalPaise + serviceChargeMinPaise + promotionsTotalPaise,
    maxPaise: vendorMaxTotalPaise + serviceChargeMaxPaise + promotionsTotalPaise,
  };

  const latest = await PlanningQuoteSnapshot.findOne({ eventId: String(eventId).trim() })
    .sort({ version: -1 })
    .select('version')
    .lean();

  const nextVersion = (latest?.version ? Number(latest.version) : 0) + 1;

  let snapshot;
  try {
    snapshot = await PlanningQuoteSnapshot.create({
      eventId: String(eventId).trim(),
      authId: String(planning.authId).trim(),
      version: nextVersion,
      lockedAt: new Date(),
      statusAtLock: STATUS.APPROVED,
      category: String(planning.category || '').trim(),
      eventStartAt,
      daysUntilEvent,
      demandTier,
      multipliers,
      currency: 'INR',
      vendorSubtotal,
      serviceChargeTotal,
      promotionsTotal,
      clientGrandTotal,
      items,
      promotions,
      commissionRates: commissionRates || {},
      lockedByAuthId: lockedByAuthId ? String(lockedByAuthId).trim() : null,
    });
  } catch (err) {
    // Concurrency safety: if another request created the same version first, return the latest snapshot.
    if (err && (err.code === 11000 || err.code === 11001)) {
      const winner = await PlanningQuoteSnapshot.findOne({ eventId: String(eventId).trim(), statusAtLock: STATUS.APPROVED })
        .sort({ version: -1 })
        .lean();
      if (winner) return winner;
    }
    throw err;
  }

  try {
    await Planning.updateOne(
      { eventId: String(eventId).trim() },
      {
        $set: {
          quoteLockedAt: snapshot.lockedAt,
          quoteLockedVersion: snapshot.version,
        },
      }
    );
  } catch (e) {
    logger.warn('Failed to stamp planning quote lock metadata', { eventId, message: e?.message });
  }

  try {
    await publishEvent(
      'PLANNING_QUOTE_LOCKED',
      {
        eventId: String(eventId).trim(),
        authId: String(snapshot.authId || '').trim() || null,
        version: snapshot.version,
      },
      `${String(eventId).trim()}:${snapshot.version}`
    );
  } catch (e) {
    logger.warn('Failed to publish PLANNING_QUOTE_LOCKED event', {
      eventId,
      message: e?.message,
    });
  }

  return snapshot.toObject();
};

const getLatestSnapshotForEvent = async ({ eventId } = {}) => {
  if (!eventId || !String(eventId).trim()) {
    throw createApiError(400, 'eventId is required');
  }

  const snap = await PlanningQuoteSnapshot.findOne({ eventId: String(eventId).trim() })
    .sort({ version: -1 })
    .lean();

  if (!snap) throw createApiError(404, 'Quote snapshot not found');
  return snap;
};

module.exports = {
  HIGH_DEMAND_WINDOW_DAYS,
  lockQuoteAtApproved,
  getLatestSnapshotForEvent,
};
