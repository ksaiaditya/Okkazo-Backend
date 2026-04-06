const mongoose = require('mongoose');
const { v4: uuidv4 } = require('uuid');
const {
  CATEGORY,
  PRIVATE_EVENT_TYPES,
  PUBLIC_EVENT_TYPES,
  SERVICE_OPTIONS,
  STATUS,
  STATUS_VALUES,
} = require('../utils/planningConstants');
const { planningMinimumDate, isWithinUrgentWindow, startOfTomorrow } = require('../utils/dateRules');
const { toIstDayString } = require('../utils/istDateTime');

const DAY_RE = /^\d{4}-\d{2}-\d{2}$/;
const IST_OFFSET = '+05:30';

const toSafeInt = (value) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Math.trunc(parsed) : 0;
};

const getInclusiveIstDaysInRange = (startAt, endAt) => {
  const startDay = toIstDayString(startAt);
  const endDay = toIstDayString(endAt || startAt);
  if (!startDay || !endDay) return [];

  const start = new Date(`${startDay}T00:00:00${IST_OFFSET}`);
  const end = new Date(`${endDay}T00:00:00${IST_OFFSET}`);
  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) return [];

  const min = start.getTime() <= end.getTime() ? start : end;
  const max = start.getTime() <= end.getTime() ? end : start;

  const days = [];
  const cursor = new Date(min.getTime());
  let guard = 0;
  while (cursor.getTime() <= max.getTime() && guard < 400) {
    const day = toIstDayString(cursor);
    if (day) days.push(day);
    cursor.setUTCDate(cursor.getUTCDate() + 1);
    guard += 1;
  }

  return days;
};

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

const TicketDayAllocationSchema = new mongoose.Schema(
  {
    day: {
      type: String,
      required: true,
      trim: true,
      match: DAY_RE,
    },
    ticketCount: {
      type: Number,
      required: true,
      min: 1,
    },
    tierBreakdown: {
      type: [
        new mongoose.Schema(
          {
            tierName: {
              type: String,
              required: true,
              trim: true,
            },
            ticketCount: {
              type: Number,
              required: true,
              min: 0,
            },
          },
          { _id: false }
        ),
      ],
      default: [],
    },
  },
  { _id: false }
);

const ChangeRequestAffectedServiceSchema = new mongoose.Schema(
  {
    service: {
      type: String,
      trim: true,
      enum: SERVICE_OPTIONS,
      required: true,
    },
    previousVendorAuthId: {
      type: String,
      trim: true,
      default: null,
    },
    previousServiceId: {
      type: String,
      trim: true,
      default: null,
    },
    requiresVendorConsent: {
      type: Boolean,
      default: false,
    },
  },
  { _id: false }
);

const ChangeRequestVendorConsentSchema = new mongoose.Schema(
  {
    vendorAuthId: {
      type: String,
      required: true,
      trim: true,
    },
    status: {
      type: String,
      required: true,
      enum: ['PENDING', 'APPROVED', 'REJECTED'],
      default: 'PENDING',
      trim: true,
    },
    note: {
      type: String,
      trim: true,
      maxlength: 1000,
      default: null,
    },
    at: {
      type: Date,
      default: null,
    },
  },
  { _id: false }
);

const PlanningChangeRequestSchema = new mongoose.Schema(
  {
    requestId: {
      type: String,
      required: true,
      default: () => uuidv4(),
      trim: true,
      index: true,
    },
    type: {
      type: String,
      required: true,
      enum: ['SERVICE_CHANGE', 'VENDOR_CHANGE'],
      default: 'SERVICE_CHANGE',
      trim: true,
    },
    status: {
      type: String,
      required: true,
      enum: [
        'PENDING_MANAGER_APPROVAL',
        'PENDING_VENDOR_CONSENT',
        'APPROVED',
        'REJECTED',
        'CANCELLED',
      ],
      default: 'PENDING_MANAGER_APPROVAL',
      trim: true,
      index: true,
    },
    requestedByAuthId: {
      type: String,
      required: true,
      trim: true,
    },
    requestedByRole: {
      type: String,
      required: true,
      trim: true,
    },
    reason: {
      type: String,
      trim: true,
      maxlength: 1000,
      default: null,
    },
    emergencyOverride: {
      type: Boolean,
      default: false,
    },
    emergencyReason: {
      type: String,
      trim: true,
      maxlength: 1000,
      default: null,
    },
    serviceDelta: {
      added: {
        type: [String],
        default: [],
      },
      removed: {
        type: [String],
        default: [],
      },
    },
    proposedSelectedServices: {
      type: [String],
      default: [],
    },
    affectedAcceptedServices: {
      type: [ChangeRequestAffectedServiceSchema],
      default: [],
    },
    vendorConsents: {
      type: [ChangeRequestVendorConsentSchema],
      default: [],
    },
    priceDelta: {
      currentGrandTotalPaise: {
        type: Number,
        min: 0,
        default: 0,
      },
      proposedGrandTotalPaise: {
        type: Number,
        min: 0,
        default: 0,
      },
      deltaPaise: {
        type: Number,
        default: 0,
      },
      suggestedAdjustmentFeePaise: {
        type: Number,
        min: 0,
        default: 0,
      },
    },
    managerDecision: {
      byAuthId: {
        type: String,
        trim: true,
        default: null,
      },
      at: {
        type: Date,
        default: null,
      },
      note: {
        type: String,
        trim: true,
        maxlength: 1000,
        default: null,
      },
    },
    vendorConsent: {
      status: {
        type: String,
        enum: ['NOT_REQUIRED', 'PENDING', 'APPROVED', 'REJECTED'],
        default: 'NOT_REQUIRED',
        trim: true,
      },
      note: {
        type: String,
        trim: true,
        maxlength: 1000,
        default: null,
      },
      at: {
        type: Date,
        default: null,
      },
    },
    createdAt: {
      type: Date,
      default: Date.now,
    },
  },
  { _id: false }
);

const PlanningPlatformFeedbackSchema = new mongoose.Schema(
  {
    rating: {
      type: Number,
      min: 1,
      max: 5,
      required: true,
    },
    review: {
      type: String,
      trim: true,
      maxlength: 1000,
      default: null,
    },
    submittedAt: {
      type: Date,
      default: Date.now,
    },
  },
  { _id: false }
);

const PlanningVendorFeedbackSchema = new mongoose.Schema(
  {
    vendorAuthId: {
      type: String,
      required: true,
      trim: true,
    },
    service: {
      type: String,
      required: true,
      trim: true,
    },
    rating: {
      type: Number,
      min: 1,
      max: 5,
      required: true,
    },
    review: {
      type: String,
      trim: true,
      maxlength: 1000,
      default: null,
    },
    submittedAt: {
      type: Date,
      default: Date.now,
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
    // Assigned CORE staff (user-service _id values) for this event.
    coreStaffIds: {
      type: [String],
      default: [],
      index: true,
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
    // Domain / industry focus of the event (UI calls this "Field")
    eventField: {
      type: String,
      trim: true,
      maxlength: 120,
      default: null,
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
      dayWiseAllocations: {
        type: [TicketDayAllocationSchema],
        default: [],
      },
    },
    promotionType: {
      type: [String],
      default: [],
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
    changeRequests: {
      type: [PlanningChangeRequestSchema],
      default: [],
    },
    isUrgent: {
      type: Boolean,
      default: false,
      index: true,
    },
    // New canonical flag for platform fee payment.
    // NOTE: `isPaid` is kept for backward compatibility with existing DB rows.
    platformFeePaid: {
      type: Boolean,
      default: false,
      index: true,
    },
    // Legacy field (do not use in new code).
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
    // New fields for multi-step payment flows.
    depositPaid: {
      type: Boolean,
      default: false,
      index: true,
    },
    depositPaidAmountPaise: {
      type: Number,
      default: null,
      min: 0,
    },
    depositPaidCurrency: {
      type: String,
      default: null,
      trim: true,
    },
    depositPaidAt: {
      type: Date,
      default: null,
    },

    vendorConfirmationPaid: {
      type: Boolean,
      default: false,
      index: true,
    },
    vendorConfirmationPaidAmountPaise: {
      type: Number,
      default: null,
      min: 0,
    },
    vendorConfirmationPaidCurrency: {
      type: String,
      default: null,
      trim: true,
    },
    vendorConfirmationPaidAt: {
      type: Date,
      default: null,
    },

    remainingPaymentPaid: {
      type: Boolean,
      default: false,
      index: true,
    },
    remainingPaymentPaidAmountPaise: {
      type: Number,
      default: null,
      min: 0,
    },
    remainingPaymentPaidCurrency: {
      type: String,
      default: null,
      trim: true,
    },
    remainingPaymentPaidAt: {
      type: Date,
      default: null,
    },

    generatedRevenuePayout: {
      mode: {
        type: String,
        enum: ['DEMO', 'RAZORPAY'],
        default: null,
      },
      status: {
        type: String,
        enum: ['PENDING', 'SUCCESS', 'FAILED'],
        default: null,
      },
      amountPaise: {
        type: Number,
        min: 0,
        default: null,
      },
      currency: {
        type: String,
        trim: true,
        default: 'INR',
      },
      paidAt: {
        type: Date,
        default: null,
      },
      paidByAuthId: {
        type: String,
        trim: true,
        default: null,
      },
      transactionRef: {
        type: String,
        trim: true,
        default: null,
      },
      notes: {
        type: String,
        trim: true,
        maxlength: 500,
        default: null,
      },
    },

    feedback: {
      platform: {
        type: PlanningPlatformFeedbackSchema,
        default: null,
      },
      vendors: {
        type: [PlanningVendorFeedbackSchema],
        default: [],
      },
    },

    quoteLockedAt: {
      type: Date,
      default: null,
      index: true,
    },
    quoteLockedVersion: {
      type: Number,
      default: null,
      min: 1,
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
      type: String,
      trim: true,
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

PlanningSchema.set('toJSON', {
  transform: (doc, ret) => {
    // Hide legacy name from API responses.
    delete ret.isPaid;
    return ret;
  },
});

// Backward-compatible payment detection.
// IMPORTANT: do NOT use nullish-coalescing here. A stored `platformFeePaid: false`
// would incorrectly mask legacy `isPaid: true` on older records.
const isPlatformFeePaid = (doc) => Boolean(doc?.platformFeePaid) || Boolean(doc?.isPaid);

const computeDefaultStatus = (doc) => {
  // Payment is the first gate; unpaid should remain in PAYMENT_PENDING even if a managerId exists.
  if (!isPlatformFeePaid(doc)) return STATUS.PAYMENT_PENDING;

  // Once paid, the next stage depends on manager assignment.
  if (doc.assignedManagerId) return STATUS.PENDING_APPROVAL;
  return STATUS.IMMEDIATE_ACTION;
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

    const dayAllocations = Array.isArray(this.tickets?.dayWiseAllocations)
      ? this.tickets.dayWiseAllocations
      : [];

    // Keep backward compatibility for older public rows by only enforcing this
    // requirement on new rows or when day-wise allocations are already present.
    if (this.isNew || dayAllocations.length > 0) {
      if (dayAllocations.length === 0) {
        this.invalidate('tickets.dayWiseAllocations', 'tickets.dayWiseAllocations is required for public category');
      } else {
        const expectedDays = getInclusiveIstDaysInRange(this.schedule?.startAt, this.schedule?.endAt);
        const expectedSet = new Set(expectedDays);
        const actualSet = new Set();

        for (const row of dayAllocations) {
          const day = String(row?.day || '').trim();
          if (!DAY_RE.test(day)) {
            this.invalidate('tickets.dayWiseAllocations.day', 'day must be in YYYY-MM-DD format');
            continue;
          }
          if (actualSet.has(day)) {
            this.invalidate('tickets.dayWiseAllocations', 'tickets.dayWiseAllocations cannot contain duplicate days');
            continue;
          }
          actualSet.add(day);

          const dayCount = Number(row?.ticketCount || 0);
          if (!Number.isFinite(dayCount) || dayCount < 1) {
            this.invalidate('tickets.dayWiseAllocations.ticketCount', 'ticketCount must be at least 1 for each day');
            continue;
          }
          if (Number(this.tickets?.totalTickets || 0) > 0 && dayCount > Number(this.tickets.totalTickets)) {
            this.invalidate('tickets.dayWiseAllocations.ticketCount', 'ticketCount cannot exceed tickets.totalTickets');
          }
          if (expectedSet.size > 0 && !expectedSet.has(day)) {
            this.invalidate('tickets.dayWiseAllocations', `day ${day} is outside the schedule range`);
          }
        }

        if (expectedSet.size > 0) {
          if (actualSet.size !== expectedSet.size) {
            this.invalidate('tickets.dayWiseAllocations', 'tickets.dayWiseAllocations must include every schedule day');
          }
          for (const expectedDay of expectedSet) {
            if (!actualSet.has(expectedDay)) {
              this.invalidate('tickets.dayWiseAllocations', `tickets.dayWiseAllocations is missing schedule day ${expectedDay}`);
              break;
            }
          }
        }

        const ticketType = String(this.tickets?.ticketType || '').trim().toLowerCase();
        const tiers = Array.isArray(this.tickets?.tiers) ? this.tickets.tiers : [];
        const tierTargets = new Map();
        const hasAnyTierBreakdown = dayAllocations.some((row) => Array.isArray(row?.tierBreakdown) && row.tierBreakdown.length > 0);
        const shouldValidateTierBreakdown = this.isNew || hasAnyTierBreakdown;

        if (ticketType === 'paid' && shouldValidateTierBreakdown) {
          for (const tier of tiers) {
            const tierName = String(tier?.tierName || '').trim();
            if (!tierName) {
              this.invalidate('tickets.tiers.tierName', 'tierName is required for paid events');
              continue;
            }
            if (tierTargets.has(tierName)) {
              this.invalidate('tickets.tiers', 'tickets.tiers cannot contain duplicate tierName values');
              continue;
            }
            tierTargets.set(tierName, toSafeInt(tier?.ticketCount));
          }

          const tierTotals = Object.fromEntries(Array.from(tierTargets.keys()).map((name) => [name, 0]));

          for (const row of dayAllocations) {
            const day = String(row?.day || '').trim();
            const dayCount = toSafeInt(row?.ticketCount);
            const breakdown = Array.isArray(row?.tierBreakdown) ? row.tierBreakdown : [];

            if (breakdown.length !== tierTargets.size) {
              this.invalidate(
                'tickets.dayWiseAllocations.tierBreakdown',
                `tierBreakdown must include every tier for day ${day}`
              );
              continue;
            }

            let dayTierTotal = 0;
            const dayTierSeen = new Set();
            for (const item of breakdown) {
              const tierName = String(item?.tierName || '').trim();
              const itemCount = toSafeInt(item?.ticketCount);

              if (!tierTargets.has(tierName)) {
                this.invalidate('tickets.dayWiseAllocations.tierBreakdown', `Unknown tierName ${tierName} in tierBreakdown`);
                continue;
              }
              if (dayTierSeen.has(tierName)) {
                this.invalidate(
                  'tickets.dayWiseAllocations.tierBreakdown',
                  `Duplicate tierName ${tierName} in tierBreakdown for day ${day}`
                );
                continue;
              }

              dayTierSeen.add(tierName);
              dayTierTotal += itemCount;
              tierTotals[tierName] += itemCount;
            }

            if (dayTierTotal !== dayCount) {
              this.invalidate(
                'tickets.dayWiseAllocations.tierBreakdown',
                `tierBreakdown total must equal ticketCount for day ${day}`
              );
            }
          }

          for (const [tierName, targetCount] of tierTargets.entries()) {
            if (toSafeInt(tierTotals[tierName]) !== toSafeInt(targetCount)) {
              this.invalidate(
                'tickets.dayWiseAllocations.tierBreakdown',
                `tierBreakdown total for ${tierName} must match tickets.tiers.ticketCount`
              );
            }
          }
        }

        if (ticketType === 'free' && shouldValidateTierBreakdown) {
          const hasTierBreakdown = dayAllocations.some((row) => Array.isArray(row?.tierBreakdown) && row.tierBreakdown.length > 0);
          if (hasTierBreakdown) {
            this.invalidate('tickets.dayWiseAllocations.tierBreakdown', 'tierBreakdown must be empty when ticketType is free');
          }
        }
      }
    }
  }

  if (this.isNew) {
    this.status = computeDefaultStatus(this);
  } else if (
    (this.isModified('platformFeePaid') || this.isModified('isPaid') || this.isModified('assignedManagerId')) &&
    [STATUS.PAYMENT_PENDING, STATUS.IMMEDIATE_ACTION, STATUS.PENDING_APPROVAL].includes(this.status)
  ) {
    this.status = computeDefaultStatus(this);
  }

  next();
});

PlanningSchema.index({ authId: 1, createdAt: -1 });
PlanningSchema.index({ category: 1, status: 1, isUrgent: 1 });

module.exports = mongoose.model('Planning', PlanningSchema);
