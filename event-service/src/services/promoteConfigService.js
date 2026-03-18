const PromoteConfig = require('../models/PromoteConfig');
const createApiError = require('../utils/ApiError');

const CONFIG_KEY = 'default';

const getDefaultPlatformFee = () => {
  const fromEnv = Number(process.env.PROMOTE_PLATFORM_FEE || process.env.PROMOTE_PLATFORM_FEE_DEFAULT);
  if (Number.isFinite(fromEnv) && fromEnv >= 0) return fromEnv;
  return 15000;
};

const getDefaultServiceChargePercent = () => {
  const fromEnv = Number(process.env.SERVICE_CHARGE_PERCENT || process.env.SERVICE_CHARGE_PERCENT_DEFAULT);
  if (Number.isFinite(fromEnv) && fromEnv >= 0 && fromEnv <= 100) return fromEnv;
  return 2.5;
};

const getOrCreateConfig = async () => {
  const platformFee = getDefaultPlatformFee();
  const serviceChargePercent = getDefaultServiceChargePercent();

  const cfg = await PromoteConfig.findOneAndUpdate(
    { key: CONFIG_KEY },
    { $setOnInsert: { key: CONFIG_KEY, platformFee, serviceChargePercent } },
    { new: true, upsert: true }
  ).lean();

  // Backfill missing fields for legacy config docs
  const needsBackfill =
    cfg.platformFee === undefined ||
    cfg.platformFee === null ||
    cfg.serviceChargePercent === undefined ||
    cfg.serviceChargePercent === null;

  if (!needsBackfill) return cfg;

  return PromoteConfig.findOneAndUpdate(
    { key: CONFIG_KEY },
    {
      $set: {
        ...(cfg.platformFee === undefined || cfg.platformFee === null ? { platformFee } : {}),
        ...(cfg.serviceChargePercent === undefined || cfg.serviceChargePercent === null ? { serviceChargePercent } : {}),
      },
    },
    { new: true }
  ).lean();
};

const getFees = async () => {
  const cfg = await getOrCreateConfig();
  return {
    platformFee: cfg.platformFee,
    serviceChargePercent: cfg.serviceChargePercent,
    updatedAt: cfg.updatedAt,
  };
};

const getPlatformFee = async () => {
  const cfg = await getOrCreateConfig();
  return {
    platformFee: cfg.platformFee,
    updatedAt: cfg.updatedAt,
  };
};

const updatePlatformFee = async ({ platformFee, updatedByAuthId }) => {
  const fee = Number(platformFee);
  if (!Number.isFinite(fee) || fee < 0) {
    throw createApiError(400, 'platformFee must be a non-negative number');
  }

  const updated = await PromoteConfig.findOneAndUpdate(
    { key: CONFIG_KEY },
    {
      $set: { platformFee: fee, updatedByAuthId: updatedByAuthId || null },
      $setOnInsert: { key: CONFIG_KEY, serviceChargePercent: getDefaultServiceChargePercent() },
    },
    { new: true, upsert: true }
  ).lean();

  return {
    platformFee: updated.platformFee,
    serviceChargePercent: updated.serviceChargePercent,
    updatedAt: updated.updatedAt,
    updatedByAuthId: updated.updatedByAuthId,
  };
};

const updateFees = async ({ platformFee, serviceChargePercent, updatedByAuthId }) => {
  const updates = {};

  if (platformFee !== undefined) {
    const fee = Number(platformFee);
    if (!Number.isFinite(fee) || fee < 0) {
      throw createApiError(400, 'platformFee must be a non-negative number');
    }
    updates.platformFee = fee;
  }

  if (serviceChargePercent !== undefined) {
    const pct = Number(serviceChargePercent);
    if (!Number.isFinite(pct) || pct < 0 || pct > 100) {
      throw createApiError(400, 'serviceChargePercent must be between 0 and 100');
    }
    updates.serviceChargePercent = pct;
  }

  if (Object.keys(updates).length === 0) {
    throw createApiError(400, 'No updates provided');
  }

  updates.updatedByAuthId = updatedByAuthId || null;

  // NOTE: MongoDB disallows updating the same path across operators in one update
  // (e.g., setting platformFee in both $set and $setOnInsert), even though $setOnInsert
  // only applies during inserts.
  const setOnInsert = { key: CONFIG_KEY };
  if (updates.platformFee === undefined) setOnInsert.platformFee = getDefaultPlatformFee();
  if (updates.serviceChargePercent === undefined) setOnInsert.serviceChargePercent = getDefaultServiceChargePercent();

  const updated = await PromoteConfig.findOneAndUpdate(
    { key: CONFIG_KEY },
    { $set: updates, $setOnInsert: setOnInsert },
    { new: true, upsert: true }
  ).lean();

  return {
    platformFee: updated.platformFee,
    serviceChargePercent: updated.serviceChargePercent,
    updatedAt: updated.updatedAt,
    updatedByAuthId: updated.updatedByAuthId,
  };
};

module.exports = {
  getFees,
  updateFees,
  getPlatformFee,
  updatePlatformFee,
  getDefaultPlatformFee,
  getDefaultServiceChargePercent,
};
