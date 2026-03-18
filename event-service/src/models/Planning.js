const mongoose = require('mongoose');
const { v4: uuidv4 } = require('uuid');
const {
  CATEGORY,
  PRIVATE_EVENT_TYPES,
  PUBLIC_EVENT_TYPES,
  SERVICE_OPTIONS,
  PUBLIC_PROMOTION_OPTIONS,
  STATUS,
  STATUS_VALUES,
} = require('../utils/planningConstants');
const { planningMinimumDate, isWithinUrgentWindow, startOfTomorrow } = require('../utils/dateRules');

const GeoLocationSchema = new mongoose.Schema(
  {
    name: {
      type: String,
      required: true,
      trim: true,
    },
    latitude: {
      type: Number,
      required: true,
      min: -90,
      max: 90,
    },
    longitude: {
      type: Number,
      required: true,
      min: -180,
      max: 180,
    },
  },
  { _id: false }
);

const EventBannerSchema = new mongoose.Schema(
  {
    url: {
      type: String,
      trim: true,
    },
    publicId: {
      type: String,
      trim: true,
    },
    mimeType: {
      type: String,
      trim: true,
      match: /^image\//,
    },
    sizeBytes: {
      type: Number,
      min: 5 * 1024,
      max: 50 * 1024 * 1024,
    },
  },
  { _id: false }
);

const TicketTierSchema = new mongoose.Schema(
  {
    tierName: {
      type: String,
      trim: true,
      required: true,
    },
    ticketPrice: {
      type: Number,
      required: true,
      min: 0,
    },
    ticketCount: {
      type: Number,
      required: true,
      min: 1,
    },
  },
  { _id: false }
);

const PlanningSchema = new mongoose.Schema(
  {
    eventId: {
      type: String,
      required: true,
      unique: true,
      default: () => uuidv4(),
      index: true,
    },
    authId: {
      type: String,
      required: true,
      index: true,
      trim: true,
    },
    category: {
      type: String,
      required: true,
      enum: Object.values(CATEGORY),
      index: true,
    },
    eventTitle: {
      type: String,
      required: true,
      trim: true,
      minlength: 2,
      maxlength: 200,
    },
    eventType: {
      type: String,
      required: true,
      trim: true,
    },
    customEventType: {
      type: String,
      trim: true,
      maxlength: 120,
    },
    eventDescription: {
      type: String,
      trim: true,
      maxlength: 1000,
    },
    location: {
      type: GeoLocationSchema,
      required: true,
    },

    // Private fields
    eventDate: {
      type: Date,
    },
    eventTime: {
      type: String,
      trim: true,
      match: /^([01]\d|2[0-3]):([0-5]\d)$/,
    },
    guestCount: {
      type: Number,
      min: 1,
    },

    // Public fields
    schedule: {
      startAt: Date,
      endAt: Date,
    },
    ticketAvailability: {
      startAt: Date,
      endAt: Date,
    },
    eventBanner: EventBannerSchema,
    tickets: {
      totalTickets: {
        type: Number,
        min: 1,
      },
      ticketType: {
        type: String,
        enum: ['free', 'paid'],
      },
      tiers: {
        type: [TicketTierSchema],
        default: [],
      },
    },
    promotionType: {
      type: [String],
      default: [],
      validate: {
        validator: (values) => values.every((value) => PUBLIC_PROMOTION_OPTIONS.includes(value)),
        message: 'Invalid promotion type provided',
      },
    },

    selectedServices: {
      type: [String],
      required: true,
      validate: {
        validator: (values) => values.length > 0 && values.every((value) => SERVICE_OPTIONS.includes(value)),
        message: 'Selected services must include valid service options',
      },
    },

    // Snapshot of the user's selected vendors at confirmation time.
    // This keeps planning queries simple without always joining VendorSelection.
    selectedVendors: {
      type: [
        {
          service: {
            type: String,
            trim: true,
            required: true,
          },
          vendorAuthId: {
            type: String,
            trim: true,
            required: true,
          },
        },
      ],
      default: [],
    },
    isUrgent: {
      type: Boolean,
      default: false,
      index: true,
    },
    isPaid: {
      type: Boolean,
      default: false,
      index: true,
    },
    platformFee: {
      type: Number,
      default: null,
      min: 0,
    },
    fullPaymentPaid: {
      type: Boolean,
      default: false,
      index: true,
    },
    totalAmount: {
      type: Number,
      default: null,
      min: 0,
    },
    status: {
      type: String,
      enum: STATUS_VALUES,
      default: STATUS.PENDING_APPROVAL,
      index: true,
    },
    assignedManagerId: {
      type: mongoose.Schema.Types.ObjectId,
      default: null,
      index: true,
    },
    vendorSelectionId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: 'VendorSelection',
      default: null,
      index: true,
    },
  },
  {
    timestamps: true,
    collection: 'plannings',
  }
);

const computeDefaultStatus = (doc) => {
  if (doc.isPaid) {
    return STATUS.IMMEDIATE_ACTION;
  }

  if (doc.assignedManagerId) {
    return STATUS.PENDING_APPROVAL;
  }

  return STATUS.PAYMENT_PENDING;
};

PlanningSchema.pre('validate', function preValidate(next) {
  const minPlanningDate = planningMinimumDate();

  if (this.category === CATEGORY.PRIVATE) {
    if (!PRIVATE_EVENT_TYPES.includes(this.eventType)) {
      this.invalidate('eventType', `eventType must be one of: ${PRIVATE_EVENT_TYPES.join(', ')}`);
    }

    if (this.eventType === 'Other' && !this.customEventType) {
      this.invalidate('customEventType', 'customEventType is required when eventType is Other');
    }

    if (!this.eventDate) {
      this.invalidate('eventDate', 'eventDate is required for private category');
    } else if (new Date(this.eventDate) < minPlanningDate) {
      this.invalidate('eventDate', 'eventDate must be at least today + 6 days');
    }

    if (!this.eventTime) {
      this.invalidate('eventTime', 'eventTime is required for private category');
    }

    if (!this.guestCount || this.guestCount < 1) {
      this.invalidate('guestCount', 'guestCount is required and must be greater than 0 for private category');
    }

    this.isUrgent = isWithinUrgentWindow(this.eventDate);
  }

  if (this.category === CATEGORY.PUBLIC) {
    if (!PUBLIC_EVENT_TYPES.includes(this.eventType)) {
      this.invalidate('eventType', `eventType must be one of: ${PUBLIC_EVENT_TYPES.join(', ')}`);
    }

    if (!this.eventDescription || this.eventDescription.trim().length === 0) {
      this.invalidate('eventDescription', 'eventDescription is required for public category');
    } else if (this.eventDescription.length > 1000) {
      this.invalidate('eventDescription', 'eventDescription must be less than or equal to 1000 characters');
    }

    if (this.eventType === 'Other' && !this.customEventType) {
      this.invalidate('customEventType', 'customEventType is required when eventType is Other');
    }

    if (!this.schedule || !this.schedule.startAt || !this.schedule.endAt) {
      this.invalidate('schedule', 'schedule.startAt and schedule.endAt are required for public category');
    } else {
      if (new Date(this.schedule.startAt) < minPlanningDate) {
        this.invalidate('schedule.startAt', 'schedule.startAt must be at least today + 6 days');
      }

      if (new Date(this.schedule.endAt) <= new Date(this.schedule.startAt)) {
        this.invalidate('schedule.endAt', 'schedule.endAt must be greater than schedule.startAt');
      }

      this.isUrgent = isWithinUrgentWindow(this.schedule.startAt);
    }

    if (!this.ticketAvailability || !this.ticketAvailability.startAt || !this.ticketAvailability.endAt) {
      this.invalidate(
        'ticketAvailability',
        'ticketAvailability.startAt and ticketAvailability.endAt are required for public category'
      );
    } else {
      if (new Date(this.ticketAvailability.startAt) < startOfTomorrow()) {
        this.invalidate('ticketAvailability.startAt', 'ticketAvailability.startAt must be from tomorrow 00:00 onward');
      }

      if (new Date(this.ticketAvailability.endAt) <= new Date(this.ticketAvailability.startAt)) {
        this.invalidate('ticketAvailability.endAt', 'ticketAvailability.endAt must be greater than ticketAvailability.startAt');
      }

      if (this.schedule?.startAt && new Date(this.ticketAvailability.endAt) >= new Date(this.schedule.startAt)) {
        this.invalidate('ticketAvailability.endAt', 'ticketAvailability.endAt must be before schedule.startAt');
      }
    }

    if (!this.tickets || !this.tickets.totalTickets || !this.tickets.ticketType) {
      this.invalidate('tickets', 'tickets.totalTickets and tickets.ticketType are required for public category');
    } else if (this.tickets.ticketType === 'paid' && this.tickets.tiers.length === 0) {
      this.invalidate('tickets.tiers', 'At least one ticket tier is required when ticketType is paid');
    }

    if (this.tickets?.ticketType === 'free' && this.tickets?.tiers?.length > 0) {
      this.invalidate('tickets.tiers', 'tiers must be empty when ticketType is free');
    }
  }

  if (this.isNew) {
    this.status = computeDefaultStatus(this);
  } else if (
    (this.isModified('isPaid') || this.isModified('assignedManagerId')) &&
    [STATUS.IMMEDIATE_ACTION, STATUS.PENDING_APPROVAL].includes(this.status)
  ) {
    this.status = computeDefaultStatus(this);
  }

  next();
});

PlanningSchema.index({ authId: 1, createdAt: -1 });
PlanningSchema.index({ category: 1, status: 1, isUrgent: 1 });

module.exports = mongoose.model('Planning', PlanningSchema);
