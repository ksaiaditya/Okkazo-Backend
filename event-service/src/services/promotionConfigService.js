const PromotionConfig = require('../models/PromotionConfig');
const createApiError = require('../utils/ApiError');
const { PUBLIC_PROMOTION_OPTIONS } = require('../utils/planningConstants');
const { PROMOTION_PACKAGES } = require('../utils/promoteConstants');

const CONFIG_KEY = 'default';

const DEFAULT_FEES = {
  // Promote packages
  'featured placement': 500,
  'email blast': 250,
  'social synergy': 150,
  'advanced analytics': 100,
  // Planning public options (note: matches existing planningConstants.js values)
  'Social Synergy': 150,
  'advance analysis': 100,
};

const buildDefaultItems = (values) => {
  const arr = Array.isArray(values) ? values : [];
  return arr
    .filter((v) => typeof v === 'string' && v.trim())
    .map((v) => ({
      value: v.trim(),
      label: null,
      fee: Number(DEFAULT_FEES[v.trim()] ?? 0),
      active: true,
    }));
};

const getOrCreateConfig = async () => {
  const defaults = {
    publicPromotionOptions: buildDefaultItems(PUBLIC_PROMOTION_OPTIONS),
    promotePackages: buildDefaultItems(PROMOTION_PACKAGES),
  };

  return PromotionConfig.findOneAndUpdate(
    { key: CONFIG_KEY },
    { $setOnInsert: { key: CONFIG_KEY, ...defaults } },
    { new: true, upsert: true }
  ).lean();
};

const normalizeItem = (raw) => {
  if (!raw || typeof raw !== 'object') throw createApiError(400, 'Invalid promotion item');

  const value = typeof raw.value === 'string' ? raw.value.trim() : '';
  if (!value) throw createApiError(400, 'Promotion item value is required');

  const fee = Number(raw.fee);
  if (!Number.isFinite(fee) || fee < 0) throw createApiError(400, `Invalid fee for promotion: ${value}`);

  const label = (raw.label === null || raw.label === undefined)
    ? null
    : String(raw.label).trim() || null;

  const active = raw.active === undefined ? true : Boolean(raw.active);

  return { value, fee, label, active };
};

const normalizeList = (list, fieldName) => {
  if (list === undefined) return undefined;
  if (!Array.isArray(list)) throw createApiError(400, `${fieldName} must be an array`);

  const normalized = list.map(normalizeItem);

  const seen = new Set();
  for (const item of normalized) {
    const k = item.value.toLowerCase();
    if (seen.has(k)) throw createApiError(400, `${fieldName} contains duplicate value: ${item.value}`);
    seen.add(k);
  }

  return normalized;
};

const getPromotions = async () => {
  const cfg = await getOrCreateConfig();
  return {
    publicPromotionOptions: cfg.publicPromotionOptions || [],
    promotePackages: cfg.promotePackages || [],
    updatedAt: cfg.updatedAt,
    updatedByAuthId: cfg.updatedByAuthId || null,
  };
};

const updatePromotions = async ({ publicPromotionOptions, promotePackages, updatedByAuthId }) => {
  const updates = {};

  const normalizedPublic = normalizeList(publicPromotionOptions, 'publicPromotionOptions');
  const normalizedPromote = normalizeList(promotePackages, 'promotePackages');

  if (normalizedPublic !== undefined) updates.publicPromotionOptions = normalizedPublic;
  if (normalizedPromote !== undefined) updates.promotePackages = normalizedPromote;

  if (Object.keys(updates).length === 0) throw createApiError(400, 'No updates provided');

  updates.updatedByAuthId = updatedByAuthId || null;

  // Ensure the config document exists (and is seeded with defaults) before updating.
  // This avoids MongoDB "conflicting update path" errors when combining $set and $setOnInsert
  // on the same fields.
  await getOrCreateConfig();

  const updated = await PromotionConfig.findOneAndUpdate(
    { key: CONFIG_KEY },
    { $set: updates },
    { new: true }
  ).lean();

  if (!updated) throw createApiError(500, 'Failed to update promotions config');

  return {
    publicPromotionOptions: updated.publicPromotionOptions || [],
    promotePackages: updated.promotePackages || [],
    updatedAt: updated.updatedAt,
    updatedByAuthId: updated.updatedByAuthId || null,
  };
};

const getAllowedActiveValues = (items) => {
  const arr = Array.isArray(items) ? items : [];
  return arr
    .filter((it) => it && it.active !== false && typeof it.value === 'string' && it.value.trim())
    .map((it) => it.value.trim());
};

module.exports = {
  getPromotions,
  updatePromotions,
  getAllowedActiveValues,
};
