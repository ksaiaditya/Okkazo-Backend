const Joi = require('joi');
const { formatErrorResponse } = require('../utils/helpers');

const objectIdSchema = Joi.string()
  .trim()
  .pattern(/^[a-fA-F0-9]{24}$/)
  .required();

const createVendorServiceSchema = Joi.object({
  categoryId: Joi.string().trim().required(),
  name: Joi.string().trim().max(200).required(),
  price: Joi.number().min(0).required(),
  tier: Joi.string().trim().allow('', null).default(null),
  description: Joi.string().trim().max(2000).allow('', null).default(null),
  details: Joi.object().default({}),
}).unknown(false);

const updateVendorServiceSchema = Joi.object({
  categoryId: Joi.string().trim(),
  name: Joi.string().trim().max(200),
  price: Joi.number().min(0),
  tier: Joi.string().trim().allow('', null),
  description: Joi.string().trim().max(2000).allow('', null),
  details: Joi.object().allow(null),
  status: Joi.string().valid('Active', 'Inactive'),
})
  .min(1)
  .unknown(false);

const normalizeVendorServicePayload = (value) => {
  const normalized = { ...(value || {}) };

  if (Object.prototype.hasOwnProperty.call(normalized, 'tier') && normalized.tier === '') {
    normalized.tier = null;
  }

  if (
    Object.prototype.hasOwnProperty.call(normalized, 'description') &&
    normalized.description === ''
  ) {
    normalized.description = null;
  }

  if (Object.prototype.hasOwnProperty.call(normalized, 'details') && normalized.details === null) {
    normalized.details = {};
  }

  return normalized;
};

const validateBody = (schema) => (req, res, next) => {
  const { value, error } = schema.validate(req.body || {}, {
    abortEarly: true,
    stripUnknown: true,
    convert: true,
  });

  if (error) {
    const msg = error.details?.[0]?.message || 'Invalid request payload';
    return res.status(400).json(formatErrorResponse('VALIDATION_ERROR', msg.replace(/"/g, '')));
  }

  req.body = normalizeVendorServicePayload(value);
  return next();
};

const validateParams = (schema) => (req, res, next) => {
  const { value, error } = schema.validate(req.params || {}, {
    abortEarly: true,
    stripUnknown: true,
    convert: true,
  });

  if (error) {
    const msg = error.details?.[0]?.message || 'Invalid request params';
    return res.status(400).json(formatErrorResponse('VALIDATION_ERROR', msg.replace(/"/g, '')));
  }

  req.params = value;
  return next();
};

const validateServiceIdParam = validateParams(
  Joi.object({
    serviceId: objectIdSchema,
  }).unknown(true)
);

const validateCreateVendorService = validateBody(createVendorServiceSchema);
const validateUpdateVendorService = validateBody(updateVendorServiceSchema);

module.exports = {
  validateCreateVendorService,
  validateUpdateVendorService,
  validateServiceIdParam,
};
