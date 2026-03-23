const Joi = require('joi');
const { PROMOTE_EVENT_CATEGORIES, PROMOTION_PACKAGES } = require('../utils/promoteConstants');
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

// ─── Tier sub-schema ──────────────────────────────────────────────────────────

const tierSchema = Joi.object({
  name: Joi.string().trim().required().messages({
    'any.required': 'Tier name is required',
  }),
  price: Joi.number().min(0).required().messages({
    'number.min': 'Tier price must be ≥ 0',
    'any.required': 'Tier price is required',
  }),
  quantity: Joi.number().integer().min(1).required().messages({
    'number.min': 'Tier quantity must be at least 1',
    'any.required': 'Tier quantity is required',
  }),
});

// ─── Main promote creation schema ─────────────────────────────────────────────

const createPromoteSchema = Joi.object({
  // Core identity
  eventTitle: Joi.string().trim().min(2).max(200).required().messages({
    'any.required': 'eventTitle is required',
  }),
  eventDescription: Joi.string().trim().min(10).max(2000).required().messages({
    'any.required': 'eventDescription is required',
    'string.min': 'eventDescription must be at least 10 characters',
  }),
  eventCategory: Joi.string()
    .valid(...PROMOTE_EVENT_CATEGORIES)
    .required()
    .messages({
      'any.only': `eventCategory must be one of: ${PROMOTE_EVENT_CATEGORIES.join(', ')}`,
      'any.required': 'eventCategory is required',
    }),

  // Domain / industry focus of the event (UI calls this "Field")
  eventField: Joi.string().trim().max(120).allow(null, '').optional(),
  customCategory: Joi.when('eventCategory', {
    is: 'Other',
    then: Joi.string().trim().max(120).required().messages({
      'any.required': 'customCategory is required when eventCategory is Other',
    }),
    otherwise: Joi.string().trim().max(120).allow(null, '').optional(),
  }),

  // Tickets
  tickets: Joi.object({
    noOfTickets: Joi.number().integer().min(1).required().messages({
      'any.required': 'tickets.noOfTickets is required',
    }),
    ticketType: Joi.string().valid('free', 'paid').required().messages({
      'any.required': 'tickets.ticketType is required',
    }),
    tiers: Joi.when('ticketType', {
      is: 'paid',
      then: Joi.array().items(tierSchema).min(1).required().messages({
        'array.min': 'At least one tier is required for paid events',
      }),
      otherwise: Joi.array().items(tierSchema).max(0).default([]).messages({
        'array.max': 'Tiers must be empty for free events',
      }),
    }),
  }).required(),

  // Schedule
  schedule: Joi.object({
    startAt: Joi.date().iso().required().messages({
      'any.required': 'schedule.startAt is required',
    }),
    endAt: Joi.date().iso().greater(Joi.ref('startAt')).required().messages({
      'any.required': 'schedule.endAt is required',
      'date.greater': 'schedule.endAt must be after schedule.startAt',
    }),
  }).required(),

  // Ticket availability
  ticketAvailability: Joi.object({
    startAt: Joi.date().iso().required().messages({
      'any.required': 'ticketAvailability.startAt is required',
    }),
    endAt: Joi.date().iso().greater(Joi.ref('startAt')).required().messages({
      'any.required': 'ticketAvailability.endAt is required',
      'date.greater': 'ticketAvailability.endAt must be after ticketAvailability.startAt',
    }),
  }).required(),

  // Venue
  venue: Joi.object({
    locationName: Joi.string().trim().required().messages({
      'any.required': 'venue.locationName is required',
    }),
    latitude: Joi.number().min(-90).max(90).required().messages({
      'any.required': 'venue.latitude is required',
    }),
    longitude: Joi.number().min(-180).max(180).required().messages({
      'any.required': 'venue.longitude is required',
    }),
  }).required(),

  // Promotions (validated dynamically in middleware)
  promotion: Joi.array().items(Joi.string()).default([]),
});

// ─── JSON field list for multipart parsing ────────────────────────────────────

const JSON_FIELDS = [
  'tickets',
  'schedule',
  'ticketAvailability',
  'venue',
  'promotion',
];

// ─── Middleware ───────────────────────────────────────────────────────────────

const validateCreatePromote = async (req, res, next) => {
  // Parse JSON-string fields that come in via multipart/form-data
  for (const field of JSON_FIELDS) {
    if (typeof req.body[field] === 'string') {
      try {
        req.body[field] = JSON.parse(req.body[field]);
      } catch (_) {
        // Leave as-is; Joi will catch the type error
      }
    }
  }

  // Normalize to a single persisted field name.
  const eventField = deriveEventField(req.body);
  if (eventField) {
    req.body.eventField = eventField;
  }
  delete req.body.field;
  delete req.body.interests;

  let allowedPromotionValues;
  try {
    const cfg = await promotionConfigService.getPromotions();
    allowedPromotionValues = promotionConfigService.getAllowedActiveValues(cfg.promotePackages);
  } catch (e) {
    allowedPromotionValues = Array.isArray(PROMOTION_PACKAGES) ? PROMOTION_PACKAGES : [];
  }

  const schema = createPromoteSchema.keys({
    promotion: Joi.array()
      .items(Joi.string().valid(...(allowedPromotionValues || [])))
      .default([])
      .messages({
        'any.only': `promotion items must be one of: ${(allowedPromotionValues || []).join(', ')}`,
      }),
  });

  const { error, value } = schema.validate(req.body, {
    abortEarly: false,
    stripUnknown: true,
  });

  if (error) {
    const messages = error.details.map((d) => d.message);
    logger.warn('Promote validation failed', { errors: messages });
    return res.status(400).json({
      success: false,
      message: 'Validation failed',
      errors: messages,
    });
  }

  req.body = value;
  next();
};

module.exports = { validateCreatePromote, JSON_FIELDS };
