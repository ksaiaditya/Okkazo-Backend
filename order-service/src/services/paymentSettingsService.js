const Joi = require('joi');
const PaymentSettings = require('../models/PaymentSettings');
const createApiError = require('../utils/ApiError');

const updateSchema = Joi.object({
  planningDepositPercent: Joi.number().integer().min(1).max(100).required(),
});

const getOrCreate = async () => {
  let doc = await PaymentSettings.findOne({}).sort({ createdAt: -1 });
  if (!doc) {
    doc = await PaymentSettings.create({ planningDepositPercent: 25 });
  }
  return doc;
};

const getSettings = async () => {
  const doc = await getOrCreate();
  return {
    planningDepositPercent: Number(doc.planningDepositPercent || 25),
    updatedAt: doc.updatedAt,
  };
};

const updateSettings = async (payload) => {
  const { value, error } = updateSchema.validate(payload);
  if (error) {
    throw createApiError(400, error.details[0].message);
  }

  const doc = await getOrCreate();
  doc.planningDepositPercent = value.planningDepositPercent;
  await doc.save();

  return {
    planningDepositPercent: Number(doc.planningDepositPercent || 25),
    updatedAt: doc.updatedAt,
  };
};

module.exports = {
  getSettings,
  updateSettings,
};
