const CommissionConfig = require('../models/CommissionConfig');
const createApiError = require('../utils/ApiError');

const CONFIG_KEY = 'commission';

const SERVICE_CATEGORIES = [
  'Venue',
  'Catering & Drinks',
  'Photography',
  'Videography',
  'Decor & Styling',
  'Entertainment & Artists',
  'Makeup & Grooming',
  'Invitations & Printing',
  'Sound & Lighting',
  'Equipment Rental',
  'Security & Safety',
  'Transportation',
  'Live Streaming & Media',
  'Cake & Desserts',
  'Other',
];

const DEFAULT_COMMISSION_PERCENT = 0;
const DEFAULT_VENDOR_HIKE_RATE = 1.25;

const getDefaultRates = () => {
  const rates = {};
  for (const c of SERVICE_CATEGORIES) rates[c] = DEFAULT_COMMISSION_PERCENT;
  return rates;
};

const normalizeRatesInput = (rates) => {
  if (rates == null) return null;

  if (!rates || typeof rates !== 'object' || Array.isArray(rates)) {
    throw createApiError(400, 'rates (object) is required');
  }

  const allowed = new Set(SERVICE_CATEGORIES);
  const keys = Object.keys(rates);

  for (const k of keys) {
    if (!allowed.has(k)) {
      throw createApiError(400, `Unknown service category: ${k}`);
    }

    const n = Number(rates[k]);
    if (!Number.isFinite(n) || n < 0 || n > 100) {
      throw createApiError(400, `Invalid commission for ${k}. Must be 0–100.`);
    }
  }

  // Ensure missing categories are preserved by caller; this only validates.
  return rates;
};

const normalizeVendorHikeRateInput = (vendorHikeRate) => {
  if (vendorHikeRate == null || vendorHikeRate === '') return null;

  const n = Number(vendorHikeRate);
  if (!Number.isFinite(n) || n < 1 || n > 10) {
    throw createApiError(400, 'vendorHikeRate must be a number between 1 and 10');
  }

  return Math.round(n * 100) / 100;
};

const getCommissionConfig = async () => {
  let cfg = await CommissionConfig.findOne({ key: CONFIG_KEY }).lean();

  if (!cfg) {
    const created = await CommissionConfig.create({
      key: CONFIG_KEY,
      rates: getDefaultRates(),
      vendorHikeRate: DEFAULT_VENDOR_HIKE_RATE,
      updatedByAuthId: null,
    });
    cfg = created.toObject();
  }

  if (!Number.isFinite(Number(cfg?.vendorHikeRate)) || Number(cfg.vendorHikeRate) < 1) {
    cfg.vendorHikeRate = DEFAULT_VENDOR_HIKE_RATE;
  }

  return cfg;
};

const updateCommissionRates = async ({ rates, vendorHikeRate, updatedByAuthId }) => {
  const incoming = normalizeRatesInput(rates);
  const normalizedVendorHikeRate = normalizeVendorHikeRateInput(vendorHikeRate);

  if (!incoming && normalizedVendorHikeRate == null) {
    throw createApiError(400, 'At least one of rates or vendorHikeRate is required');
  }

  const existing = await CommissionConfig.findOne({ key: CONFIG_KEY });

  const nextRates = { ...(existing?.rates?.toObject?.() || existing?.rates || getDefaultRates()) };
  if (incoming) {
    for (const [k, v] of Object.entries(incoming)) {
      nextRates[k] = Number(v);
    }
  }

  const nextVendorHikeRate = normalizedVendorHikeRate != null
    ? normalizedVendorHikeRate
    : (() => {
      const raw = Number(existing?.vendorHikeRate);
      return Number.isFinite(raw) && raw >= 1 ? raw : DEFAULT_VENDOR_HIKE_RATE;
    })();

  const updated = await CommissionConfig.findOneAndUpdate(
    { key: CONFIG_KEY },
    {
      $set: {
        rates: nextRates,
        vendorHikeRate: nextVendorHikeRate,
        updatedByAuthId: updatedByAuthId || null,
      },
    },
    {
      new: true,
      upsert: true,
    }
  ).lean();

  return updated;
};

module.exports = {
  SERVICE_CATEGORIES,
  DEFAULT_VENDOR_HIKE_RATE,
  getDefaultRates,
  getCommissionConfig,
  updateCommissionRates,
};
