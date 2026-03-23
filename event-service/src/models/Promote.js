const mongoose = require('mongoose');
const { v4: uuidv4 } = require('uuid');
const {
  PROMOTE_EVENT_CATEGORIES,
  PROMOTE_STATUS,
  PROMOTE_STATUS_VALUES,
  TICKET_STATUS,
  SERVICE_CHARGE_RATE,
} = require('../utils/promoteConstants');
const { planningMinimumDate, startOfTomorrow } = require('../utils/dateRules');

// ─── Sub-schemas ──────────────────────────────────────────────────────────────

const TicketTierSchema = new mongoose.Schema(
  {
    name: {
      type: String,
      required: true,
      trim: true,
    },
    price: {
      type: Number,
      required: true,
      min: 0,
    },
    quantity: {
      type: Number,
      required: true,
      min: 1,
    },
  },
  { _id: false }
);

const VenueSchema = new mongoose.Schema(
  {
    locationName: {
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

const CloudinaryImageSchema = new mongoose.Schema(
  {
    url: { type: String, required: true, trim: true },
    publicId: { type: String, required: true, trim: true },
    mimeType: { type: String, trim: true, match: /^image\// },
    sizeBytes: { type: Number, min: 1 },
  },
  { _id: false }
);

// ─── Main Schema ──────────────────────────────────────────────────────────────

const PromoteSchema = new mongoose.Schema(
  {
    // Identifiers
    promoteId: {
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
    // Assigned CORE staff (user-service _id values) for this event.
    coreStaffIds: {
      type: [String],
      default: [],
      index: true,
    },
    eventId: {
      type: String,
      required: true,
      unique: true,
      default: () => uuidv4(),
      index: true,
    },

    // Core event identity
    eventTitle: {
      type: String,
      required: true,
      trim: true,
      minlength: 2,
      maxlength: 200,
    },
    eventDescription: {
      type: String,
      required: true,
      trim: true,
      minlength: 10,
      maxlength: 2000,
    },
    eventCategory: {
      type: String,
      required: true,
      index: true,
      trim: true,
    },
    // Domain / industry focus of the event (UI calls this "Field")
    eventField: {
      type: String,
      trim: true,
      maxlength: 120,
      default: null,
    },
    customCategory: {
      type: String,
      trim: true,
      maxlength: 120,
    },

    // Media
    eventBanner: {
      type: CloudinaryImageSchema,
      required: true,
    },

    // Tickets
    tickets: {
      noOfTickets: {
        type: Number,
        required: true,
        min: 1,
      },
      ticketType: {
        type: String,
        required: true,
        enum: ['free', 'paid'],
      },
      tiers: {
        type: [TicketTierSchema],
        default: [],
      },
    },

    // Schedule
    schedule: {
      startAt: { type: Date, required: true },
      endAt: { type: Date, required: true },
    },

    // Ticket availability window
    ticketAvailability: {
      startAt: { type: Date, required: true },
      endAt: { type: Date, required: true },
    },

    // Venue
    venue: {
      type: VenueSchema,
      required: true,
    },

    // Promotion packages selected
    promotion: {
      type: [String],
      default: [],
    },

    // Authenticity proof images (uploaded by organizer)
    authenticityProofs: {
      type: [CloudinaryImageSchema],
      default: [],
    },

    // Payment
    platformFee: {
      type: Number,
      default: null,
      min: 0,
    },
    platformFeePaid: {
      type: Boolean,
      required: true,
      default: false,
      index: true,
    },

    serviceChargePercent: {
      type: Number,
      default: null,
      min: 0,
      max: 100,
    },

    // Revenue calculations (computed and stored on save)
    totalAmount: {
      type: Number,
      default: 0,
      min: 0,
    },
    serviceCharge: {
      type: Number,
      default: 0,
      min: 0,
    },
    estimatedNetRevenue: {
      type: Number,
      default: 0,
    },

    // Manager
    assignedManagerId: {
      type: String,
      trim: true,
      default: null,
      index: true,
    },

    // Admin decision workflow (separate from eventStatus)
    adminDecision: {
      status: {
        type: String,
        enum: ['PENDING', 'APPROVED', 'REJECTED'],
        default: 'PENDING',
        index: true,
      },
      decidedAt: { type: Date, default: null },
      decidedByAuthId: { type: String, trim: true, default: null },
      rejectionReason: { type: String, trim: true, maxlength: 500, default: null },
    },

    managerAssignment: {
      assignedAt: { type: Date, default: null },
      assignedByAuthId: { type: String, trim: true, default: null },
      autoAssigned: { type: Boolean, default: false },
    },

    // Event status
    eventStatus: {
      type: String,
      enum: PROMOTE_STATUS_VALUES,
      default: PROMOTE_STATUS.PAYMENT_REQUIRED,
      index: true,
    },

    // Ticket analytics (derived, updated when tickets sell)
    ticketAnalytics: {
      ticketsYetToSell: {
        type: Number,
        default: 0,
      },
      ticketsSold: {
        type: Number,
        default: 0,
        min: 0,
      },
      ticketStatus: {
        type: String,
        enum: Object.values(TICKET_STATUS),
        default: TICKET_STATUS.READY,
      },
    },
  },
  {
    timestamps: true,
    collection: 'promotes',
  }
);

// ─── Revenue computation helper ───────────────────────────────────────────────

const computeRevenue = (doc) => {
  if (doc.tickets?.ticketType === 'paid' && Array.isArray(doc.tickets.tiers)) {
    const total = doc.tickets.tiers.reduce(
      (acc, tier) => acc + tier.price * tier.quantity,
      0
    );
    const fallbackPercent = SERVICE_CHARGE_RATE * 100;
    const percent = Number.isFinite(doc.serviceChargePercent) ? doc.serviceChargePercent : fallbackPercent;
    const rate = Math.max(0, Math.min(100, Number(percent))) / 100;
    const charge = total * rate;
    return { totalAmount: total, serviceCharge: charge, estimatedNetRevenue: total - charge };
  }
  return { totalAmount: 0, serviceCharge: 0, estimatedNetRevenue: 0 };
};

// ─── Status computation helper ────────────────────────────────────────────────

const computeEventStatus = (doc) => {
  if (!doc.platformFeePaid) return PROMOTE_STATUS.PAYMENT_REQUIRED;
  if (!doc.assignedManagerId) return PROMOTE_STATUS.MANAGER_UNASSIGNED;
  return PROMOTE_STATUS.IN_REVIEW;
};

// ─── Ticket analytics helper ──────────────────────────────────────────────────

const computeTicketAnalytics = (doc) => {
  const noOfTickets = doc.tickets?.noOfTickets || 0;
  const sold = doc.ticketAnalytics?.ticketsSold || 0;
  const now = new Date();

  let status;
  if (sold >= noOfTickets && noOfTickets > 0) {
    status = TICKET_STATUS.SOLD_OUT;
  } else if (doc.ticketAvailability?.startAt && now < new Date(doc.ticketAvailability.startAt)) {
    status = TICKET_STATUS.READY;
  } else if (doc.ticketAvailability?.endAt && now > new Date(doc.ticketAvailability.endAt)) {
    status = TICKET_STATUS.SALES_ENDED;
  } else {
    status = TICKET_STATUS.LIVE;
  }

  return {
    ticketsYetToSell: Math.max(0, noOfTickets - sold),
    ticketsSold: sold,
    ticketStatus: status,
  };
};

// ─── Pre-validate hook ────────────────────────────────────────────────────────

PromoteSchema.pre('validate', function preValidate(next) {
  const minPlanningDate = planningMinimumDate();
  const tomorrow = startOfTomorrow();

  // Validate eventCategory (allow custom when 'Other')
  if (this.eventCategory !== 'Other' && !PROMOTE_EVENT_CATEGORIES.includes(this.eventCategory)) {
    this.invalidate('eventCategory', `eventCategory must be one of: ${PROMOTE_EVENT_CATEGORIES.join(', ')}`);
  }

  if (this.eventCategory === 'Other' && !this.customCategory) {
    this.invalidate('customCategory', 'customCategory is required when eventCategory is Other');
  }

  // Schedule validation
  if (!this.schedule?.startAt || !this.schedule?.endAt) {
    this.invalidate('schedule', 'schedule.startAt and schedule.endAt are required');
  } else {
    if (new Date(this.schedule.startAt) < minPlanningDate) {
      this.invalidate('schedule.startAt', 'schedule.startAt must be at least today + 6 days');
    }
    if (new Date(this.schedule.endAt) <= new Date(this.schedule.startAt)) {
      this.invalidate('schedule.endAt', 'schedule.endAt must be after schedule.startAt');
    }
  }

  // Ticket availability validation
  if (!this.ticketAvailability?.startAt || !this.ticketAvailability?.endAt) {
    this.invalidate('ticketAvailability', 'ticketAvailability.startAt and ticketAvailability.endAt are required');
  } else {
    if (new Date(this.ticketAvailability.startAt) < tomorrow) {
      this.invalidate('ticketAvailability.startAt', 'ticketAvailability.startAt must be from tomorrow onward');
    }
    if (new Date(this.ticketAvailability.endAt) <= new Date(this.ticketAvailability.startAt)) {
      this.invalidate('ticketAvailability.endAt', 'ticketAvailability.endAt must be after ticketAvailability.startAt');
    }
    if (
      this.schedule?.startAt &&
      new Date(this.ticketAvailability.endAt) >= new Date(this.schedule.startAt)
    ) {
      this.invalidate(
        'ticketAvailability.endAt',
        'ticketAvailability.endAt must be before schedule.startAt'
      );
    }
  }

  // Ticket type validation
  if (this.tickets?.ticketType === 'paid' && (!this.tickets.tiers || this.tickets.tiers.length === 0)) {
    this.invalidate('tickets.tiers', 'At least one tier is required for paid events');
  }
  if (this.tickets?.ticketType === 'free' && this.tickets?.tiers?.length > 0) {
    this.invalidate('tickets.tiers', 'Tiers must be empty for free events');
  }

  // Revenue calculations
  const revenue = computeRevenue(this);
  this.totalAmount = revenue.totalAmount;
  this.serviceCharge = revenue.serviceCharge;
  this.estimatedNetRevenue = revenue.estimatedNetRevenue;

  // Status
  if (
    this.isNew ||
    this.isModified('platformFeePaid') ||
    this.isModified('assignedManagerId')
  ) {
    const computed = computeEventStatus(this);
    // Only auto-compute if NOT already manually set to LIVE or COMPLETE
    if (![PROMOTE_STATUS.LIVE, PROMOTE_STATUS.COMPLETE].includes(this.eventStatus)) {
      this.eventStatus = computed;
    }
  }

  // Ticket analytics
  const analytics = computeTicketAnalytics(this);
  this.ticketAnalytics = {
    ticketsYetToSell: analytics.ticketsYetToSell,
    ticketsSold: this.ticketAnalytics?.ticketsSold || 0,
    ticketStatus: analytics.status || analytics.ticketStatus,
  };

  next();
});

// ─── Indexes ──────────────────────────────────────────────────────────────────

PromoteSchema.index({ authId: 1, createdAt: -1 });
PromoteSchema.index({ eventStatus: 1, platformFeePaid: 1 });
PromoteSchema.index({ 'schedule.startAt': 1 });

module.exports = mongoose.model('Promote', PromoteSchema);
