const Joi = require('joi');
const PaymentSettings = require('../models/PaymentSettings');
const createApiError = require('../utils/ApiError');

const updateSchema = Joi.object({
  planningDepositPercent: Joi.number().integer().min(1).max(100),
  vendorPayoutMode: Joi.string().valid('DEMO', 'RAZORPAY').uppercase(),
})
  .or('planningDepositPercent', 'vendorPayoutMode');

const getOrCreate = async () => {
  let doc = await PaymentSettings.findOne({}).sort({ createdAt: -1 });
  if (!doc) {
    doc = await PaymentSettings.create({
      planningDepositPercent: 25,
      vendorPayoutMode: 'DEMO',
    });
  }
  return doc;
};

const normalizePayoutMode = (value) => {
  const mode = String(value || 'DEMO').trim().toUpperCase();
  if (mode === 'RAZORPAY') return 'RAZORPAY';
  return 'DEMO';
};

const isRazorpayVendorPayoutEnabled = () => {
  const raw = String(process.env.ENABLE_RAZORPAY_VENDOR_PAYOUTS || '').trim().toLowerCase();
  return raw === 'true' || raw === '1' || raw === 'yes';
};

const resolveEffectivePayoutMode = (storedMode) => {
  const normalized = normalizePayoutMode(storedMode);
  if (normalized === 'RAZORPAY' && isRazorpayVendorPayoutEnabled()) {
    return 'RAZORPAY';
  }
  return 'DEMO';
};

const getSettings = async () => {
  const doc = await getOrCreate();
  return {
    planningDepositPercent: Number(doc.planningDepositPercent || 25),
    vendorPayoutMode: resolveEffectivePayoutMode(doc.vendorPayoutMode),
    updatedAt: doc.updatedAt,
  };
};

const updateSettings = async (payload) => {
  const { value, error } = updateSchema.validate(payload);
  if (error) {
    throw createApiError(400, error.details[0].message);
  }

  const doc = await getOrCreate();
  if (value.planningDepositPercent !== undefined) {
    doc.planningDepositPercent = value.planningDepositPercent;
  }
  if (value.vendorPayoutMode !== undefined) {
    doc.vendorPayoutMode = normalizePayoutMode(value.vendorPayoutMode);
  }
  await doc.save();

  return {
    planningDepositPercent: Number(doc.planningDepositPercent || 25),
    vendorPayoutMode: resolveEffectivePayoutMode(doc.vendorPayoutMode),
    updatedAt: doc.updatedAt,
  };
};

module.exports = {
  getSettings,
  updateSettings,
};
