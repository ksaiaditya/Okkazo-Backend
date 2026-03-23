const mongoose = require('mongoose');

const MoneyRangeSchema = new mongoose.Schema(
  {
    minPaise: { type: Number, required: true, min: 0 },
    maxPaise: { type: Number, required: true, min: 0 },
  },
  { _id: false }
);

const QuoteLineItemSchema = new mongoose.Schema(
  {
    service: { type: String, required: true, trim: true },
    vendorAuthId: { type: String, default: null, trim: true },

    vendorTotal: { type: MoneyRangeSchema, required: true },
    commissionPercent: { type: Number, required: true, min: 0, max: 100 },
    serviceCharge: { type: MoneyRangeSchema, required: true },
    clientTotal: { type: MoneyRangeSchema, required: true },
  },
  { _id: false }
);

const PromotionSelectionSchema = new mongoose.Schema(
  {
    value: { type: String, required: true, trim: true },
    feePaise: { type: Number, required: true, min: 0 },
  },
  { _id: false }
);

const PlanningQuoteSnapshotSchema = new mongoose.Schema(
  {
    eventId: { type: String, required: true, trim: true, index: true },
    authId: { type: String, required: true, trim: true, index: true },

    version: { type: Number, required: true, min: 1 },
    lockedAt: { type: Date, required: true },
    statusAtLock: { type: String, required: true, trim: true },

    category: { type: String, required: true, trim: true },
    eventStartAt: { type: Date, required: true },
    daysUntilEvent: { type: Number, required: true },

    demandTier: { type: String, required: true, enum: ['NORMAL', 'HIGH_DEMAND'] },
    multipliers: {
      min: { type: Number, required: true },
      max: { type: Number, required: true },
    },

    currency: { type: String, required: true, default: 'INR', trim: true },

    vendorSubtotal: { type: MoneyRangeSchema, required: true },
    serviceChargeTotal: { type: MoneyRangeSchema, required: true },
    promotionsTotal: { type: MoneyRangeSchema, required: true },
    clientGrandTotal: { type: MoneyRangeSchema, required: true },

    promotions: { type: [PromotionSelectionSchema], default: [] },

    items: { type: [QuoteLineItemSchema], default: [] },

    // For debugging/auditing only: the exact commission map used at lock time.
    commissionRates: { type: mongoose.Schema.Types.Mixed, default: {} },

    lockedByAuthId: { type: String, default: null, trim: true },
  },
  {
    timestamps: true,
    collection: 'planning_quote_snapshots',
  }
);

PlanningQuoteSnapshotSchema.index({ eventId: 1, version: -1 }, { unique: true });

module.exports = mongoose.model('PlanningQuoteSnapshot', PlanningQuoteSnapshotSchema);
