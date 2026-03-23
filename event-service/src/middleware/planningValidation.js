const Joi = require('joi');
const {
  CATEGORY,
  PRIVATE_EVENT_TYPES,
  PUBLIC_EVENT_TYPES,
  SERVICE_OPTIONS,
  PUBLIC_PROMOTION_OPTIONS,
} = require('../utils/planningConstants');
const promotionConfigService = require('../services/promotionConfigService');
const logger = require('../utils/logger');

const deriveEventField = (body) => {
  if (!body || typeof body !== 'object') return undefined;

  const direct = body.eventField ?? body.field;
  if (typeof direct === 'string' && direct.trim()) return direct.trim();

  const interests = body.interests;
  if (Array.isArray(interests)) {
    const first = interests.find((v) => typeof v === 'string' && v.trim());
    return first ? first.trim() : undefined;
  }
  if (typeof interests === 'string' && interests.trim()) return interests.trim();

  return undefined;
};

/**
 * Common fields shared by both private and public plannings
 */
const commonFields = {
  category: Joi.string()
    .valid(CATEGORY.PUBLIC, CATEGORY.PRIVATE)
    .required()
    .messages({ 'any.only': 'category must be either "public" or "private"' }),

  eventTitle: Joi.string().trim().min(2).max(200).required(),

  eventType: Joi.string().trim().required(),

  // Domain / industry focus of the event (UI calls this "Field")
  eventField: Joi.string().trim().max(120).allow(null, ''),

  customEventType: Joi.string().trim().max(120).allow(null, ''),

  eventDescription: Joi.string().trim().max(1000).allow(null, ''),

  location: Joi.object({
    name: Joi.string().trim().required(),
    latitude: Joi.number().min(-90).max(90).required(),
    longitude: Joi.number().min(-180).max(180).required(),
  }).required(),

  selectedServices: Joi.array()
    .items(Joi.string().valid(...SERVICE_OPTIONS))
    .min(1)
    .required()
    .messages({ 'array.min': 'At least one service must be selected' }),

  platformFeePaid: Joi.boolean().default(false),
};

/**
 * Private planning schema
 */
const privatePlanningSchema = Joi.object({
  ...commonFields,
  category: Joi.string().valid(CATEGORY.PRIVATE).required(),
  eventType: Joi.string()
    .valid(...PRIVATE_EVENT_TYPES)
    .required(),
  eventDate: Joi.date().iso().required().messages({
    'date.base': 'eventDate must be a valid date',
    'any.required': 'eventDate is required for private events',
  }),
  eventTime: Joi.string()
    .pattern(/^([01]\d|2[0-3]):([0-5]\d)$/)
    .required()
    .messages({
      'string.pattern.base': 'eventTime must be in HH:mm format',
      'any.required': 'eventTime is required for private events',
    }),
  guestCount: Joi.number().integer().min(1).required().messages({
    'number.min': 'guestCount must be at least 1',
    'any.required': 'guestCount is required for private events',
  }),
});

/**
 * Public planning schema
 *
 * Note: eventBanner is NOT validated here — it arrives as a multipart file
 * and is processed by the multer middleware + bannerUploadService.
 * The Cloudinary URL is attached to the planning in the controller.
 */
const publicPlanningSchema = Joi.object({
  ...commonFields,
  category: Joi.string().valid(CATEGORY.PUBLIC).required(),
  eventType: Joi.string()
    .valid(...PUBLIC_EVENT_TYPES)
    .required(),
  eventDescription: Joi.string().trim().min(1).max(1000).required().messages({
    'any.required': 'eventDescription is required for public events',
    'string.max': 'eventDescription must be at most 1000 characters',
  }),
  schedule: Joi.object({
    startAt: Joi.date().iso().required(),
    endAt: Joi.date().iso().greater(Joi.ref('startAt')).required(),
  }).required(),
  ticketAvailability: Joi.object({
    startAt: Joi.date().iso().required(),
    endAt: Joi.date().iso().greater(Joi.ref('startAt')).required(),
  }).required(),
  tickets: Joi.object({
    totalTickets: Joi.number().integer().min(1).required(),
    ticketType: Joi.string().valid('free', 'paid').required(),
    tiers: Joi.array()
      .items(
        Joi.object({
          tierName: Joi.string().trim().required(),
          ticketPrice: Joi.number().min(0).required(),
          ticketCount: Joi.number().integer().min(1).required(),
        })
      )
      .default([]),
  }).required(),
  promotionType: Joi.array()
    .items(Joi.string())
    .default([]),
});

/**
 * Middleware to validate planning creation request
 *
 * For multipart/form-data requests, nested objects (location, schedule, etc.)
 * arrive as JSON strings and must be parsed before validation.
 */
const validateCreatePlanning = async (req, res, next) => {
  // Parse JSON string fields that arrive via multipart/form-data
  parseJsonFields(req);

  // Normalize the various possible frontend names into one persisted field.
  // Frontend may send: eventField (string) OR field (string) OR interests ([string] or string).
  const eventField = deriveEventField(req.body);
  if (eventField) {
    req.body.eventField = eventField;
  }
  // Keep payload tidy; stripUnknown will also remove them.
  delete req.body.field;
  delete req.body.interests;

  const { category } = req.body;

  let allowedPromotionValues = null;
  if (category === CATEGORY.PUBLIC) {
    try {
      const cfg = await promotionConfigService.getPromotions();
      allowedPromotionValues = promotionConfigService.getAllowedActiveValues(cfg.publicPromotionOptions);
    } catch (e) {
      // Fallback so public planning does not break if config is unavailable.
      allowedPromotionValues = Array.isArray(PUBLIC_PROMOTION_OPTIONS) ? PUBLIC_PROMOTION_OPTIONS : [];
    }
  }

  let schema;
  if (category === CATEGORY.PRIVATE) {
    schema = privatePlanningSchema;
  } else if (category === CATEGORY.PUBLIC) {
    schema = publicPlanningSchema.keys({
      promotionType: Joi.array()
        .items(Joi.string().valid(...(allowedPromotionValues || [])))
        .default([])
        .messages({
          'any.only': `promotionType items must be one of: ${(allowedPromotionValues || []).join(', ')}`,
        }),
    });
  } else {
    return res.status(400).json({
      success: false,
      message: 'category must be either "public" or "private"',
    });
  }

  const { error, value } = schema.validate(req.body, {
    abortEarly: false,
    stripUnknown: true,
  });

  if (error) {
    const messages = error.details.map((detail) => detail.message);
    logger.warn('Planning validation failed', { errors: messages });
    return res.status(400).json({
      success: false,
      message: 'Validation failed',
      errors: messages,
    });
  }

  req.body = value;
  next();
};

/**
 * When multipart/form-data is used, nested objects come as JSON strings.
 * This helper parses them back into objects so Joi can validate properly.
 */
const parseJsonFields = (req) => {
  const jsonFields = [
    'location',
    'schedule',
    'ticketAvailability',
    'tickets',
    'selectedServices',
    'promotionType',
  ];

  for (const field of jsonFields) {
    if (typeof req.body[field] === 'string') {
      try {
        req.body[field] = JSON.parse(req.body[field]);
      } catch (e) {
        // Leave as-is — Joi will catch the type mismatch
      }
    }
  }

  // Parse simple type coercions for multipart
  if (typeof req.body.platformFeePaid === 'string') {
    req.body.platformFeePaid = req.body.platformFeePaid === 'true';
  }
  // Backward-compat: older clients might still send `isPaid`
  if (typeof req.body.isPaid === 'string') {
    const legacyPaid = req.body.isPaid === 'true';
    // Payment should only ever move forward (false -> true). Do not let legacy `false`
    // overwrite an existing paid flag.
    if (legacyPaid && req.body.platformFeePaid === undefined) {
      req.body.platformFeePaid = true;
    }
    delete req.body.isPaid;
  }
  if (typeof req.body.isPaid === 'boolean') {
    const legacyPaid = req.body.isPaid === true;
    if (legacyPaid && req.body.platformFeePaid === undefined) {
      req.body.platformFeePaid = true;
    }
    delete req.body.isPaid;
  }
  if (typeof req.body.guestCount === 'string') {
    req.body.guestCount = Number(req.body.guestCount);
  }
};

module.exports = {
  validateCreatePlanning,
};
