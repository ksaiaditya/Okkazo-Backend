const mongoose = require('mongoose');
const {
  SERVICE_OPTIONS,
  VENDOR_STATUS,
  VENDOR_STATUS_VALUES,
  VENDOR_SELECTION_STATUS,
  VENDOR_SELECTION_STATUS_VALUES,
} = require('../utils/vendorSelectionConstants');

const PRICING_UNIT_VALUES = ['EVENT', 'PER_PERSON', 'PER_PLATE', 'PER_KG', 'PER_100_UNITS', 'FIXED'];

const MoneyRangeSchema = new mongoose.Schema(
  {
    min: { type: Number, min: 0, default: 0 },
    max: { type: Number, min: 0, default: 0 },
  },
  { _id: false }
);

const VendorItemSchema = new mongoose.Schema(
  {
    service: {
      type: String,
      required: true,
      trim: true,
      enum: SERVICE_OPTIONS,
      index: true,
    },
    vendorAuthId: {
      type: String,
      default: null,
      trim: true,
    },
    // Optional: for Venue selections we store the concrete vendor serviceId that user picked.
    serviceId: {
      type: String,
      default: null,
      trim: true,
    },
    status: {
      type: String,
      required: true,
      enum: VENDOR_STATUS_VALUES,
      default: VENDOR_STATUS.YET_TO_SELECT,
      index: true,
    },
    rejectionReason: {
      type: String,
      default: null,
      trim: true,
      maxlength: 500,
    },
    alternativeNeeded: {
      type: Boolean,
      default: false,
      index: true,
    },
    servicePrice: {
      type: MoneyRangeSchema,
      default: () => ({ min: 0, max: 0 }),
    },
    vendorQuotedPrice: {
      type: Number,
      min: 0,
      default: null,
    },
    commissionPercent: {
      type: Number,
      min: 0,
      max: 100,
      default: null,
    },
    commissionAmount: {
      type: Number,
      min: 0,
      default: null,
    },
    priceHikeReason: {
      type: String,
      default: null,
      trim: true,
      maxlength: 500,
    },
    priceLocked: {
      type: Boolean,
      default: false,
    },
    pricingUnit: {
      type: String,
      enum: PRICING_UNIT_VALUES,
      default: null,
      trim: true,
      set: (value) => {
        if (value == null) return null;
        const raw = String(value).trim().toUpperCase();
        return raw || null;
      },
    },
    pricingQuantity: {
      type: Number,
      min: 0,
      default: null,
    },
    pricingQuantityUnit: {
      type: String,
      default: null,
      trim: true,
      maxlength: 32,
    },
  },
  { _id: false }
);

const AlternativeVendorSchema = new mongoose.Schema(
  {
    service: {
      type: String,
      required: true,
      trim: true,
      enum: SERVICE_OPTIONS,
      index: true,
    },
    vendorAuthId: {
      type: String,
      default: null,
      trim: true,
    },
  },
  { _id: false }
);

const VendorSelectionSchema = new mongoose.Schema(
  {
    authId: {
      type: String,
      required: true,
      index: true,
      trim: true,
    },
    eventId: {
      type: String,
      required: true,
      unique: true,
      index: true,
      trim: true,
    },
    planningId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: 'Planning',
      default: null,
      index: true,
    },

    selectedServices: {
      type: [String],
      required: true,
      validate: {
        validator: (values) => Array.isArray(values) && values.length > 0 && values.every((v) => SERVICE_OPTIONS.includes(v)),
        message: 'selectedServices must include valid service options',
      },
    },

    vendors: {
      type: [VendorItemSchema],
      default: [],
    },

    serviceAlternativeVendor: {
      type: [AlternativeVendorSchema],
      default: [],
    },

    managerAssigned: {
      type: Boolean,
      default: false,
      index: true,
    },
    managerId: {
      type: mongoose.Schema.Types.ObjectId,
      default: null,
      index: true,
    },

    status: {
      type: String,
      enum: VENDOR_SELECTION_STATUS_VALUES,
      default: VENDOR_SELECTION_STATUS.MANAGER_YET_TO_BE_ASSIGNED,
      index: true,
    },

    totalMinAmount: {
      type: Number,
      min: 0,
      default: 0,
    },
    totalMaxAmount: {
      type: Number,
      min: 0,
      default: 0,
    },

    vendorsAccepted: {
      type: Boolean,
      default: false,
      index: true,
    },
  },
  {
    timestamps: true,
    collection: 'vendor_selections',
  }
);

const computeTotals = (doc) => {
  const services = Array.isArray(doc.selectedServices) ? doc.selectedServices : [];
  const vendorItems = Array.isArray(doc.vendors) ? doc.vendors : [];

  const byService = new Map();
  for (const v of vendorItems) {
    if (!v?.service) continue;
    if (!services.includes(v.service)) continue;
    byService.set(v.service, v);
  }

  let min = 0;
  let max = 0;
  for (const service of services) {
    const item = byService.get(service);
    if (!item?.servicePrice) continue;
    min += Number(item.servicePrice.min || 0);
    max += Number(item.servicePrice.max || 0);
  }

  return {
    totalMinAmount: Math.max(0, min),
    totalMaxAmount: Math.max(0, max),
  };
};

const computeVendorsAccepted = (doc) => {
  const services = Array.isArray(doc.selectedServices) ? doc.selectedServices : [];
  if (services.length === 0) return false;

  const vendorItems = Array.isArray(doc.vendors) ? doc.vendors : [];

  for (const service of services) {
    const matching = vendorItems.find((v) => v?.service === service);
    if (!matching || matching.status !== VENDOR_STATUS.ACCEPTED) {
      return false;
    }
  }

  return true;
};

VendorSelectionSchema.pre('validate', function preValidate(next) {
  // Normalize managerAssigned
  this.managerAssigned = Boolean(this.managerId);

  for (const v of this.vendors || []) {
    if (!v) continue;

    if (v.status === VENDOR_STATUS.REJECTED) {
      if (!v.rejectionReason || !String(v.rejectionReason).trim()) {
        this.invalidate('vendors.rejectionReason', 'rejectionReason is required when vendor status is REJECTED');
      }
      if (v.alternativeNeeded === false) {
        v.alternativeNeeded = true;
      }
    } else {
      if (v.rejectionReason) v.rejectionReason = null;
      if (v.alternativeNeeded) v.alternativeNeeded = false;
    }

    if (v.servicePrice?.min != null && v.servicePrice?.max != null) {
      if (Number(v.servicePrice.max) < Number(v.servicePrice.min)) {
        this.invalidate('vendors.servicePrice', 'servicePrice.max must be greater than or equal to servicePrice.min');
      }
    }

    if (v.vendorQuotedPrice != null) {
      const quoted = Number(v.vendorQuotedPrice);
      v.vendorQuotedPrice = Number.isFinite(quoted) && quoted > 0 ? quoted : null;
    }

    if (v.commissionPercent != null) {
      const pct = Number(v.commissionPercent);
      v.commissionPercent = Number.isFinite(pct) && pct >= 0 && pct <= 100 ? pct : null;
    }

    if (v.commissionAmount != null) {
      const amt = Number(v.commissionAmount);
      v.commissionAmount = Number.isFinite(amt) && amt >= 0 ? amt : null;
    }

    if (v.priceHikeReason != null) {
      const reason = String(v.priceHikeReason || '').trim();
      v.priceHikeReason = reason || null;
    }

    if (v.priceLocked && !v.vendorQuotedPrice) {
      v.priceLocked = false;
    }

    if (v.priceLocked) {
      const quoted = Number(v.vendorQuotedPrice || 0);
      const percent = Number(v.commissionPercent || 0);

      if (v.commissionAmount == null && quoted > 0 && percent >= 0) {
        v.commissionAmount = Math.round(((quoted * percent) / 100) * 100) / 100;
      }

      if (v.commissionPercent == null) {
        const amount = Number(v.commissionAmount || 0);
        v.commissionPercent = quoted > 0 && amount >= 0
          ? Math.round(((amount / quoted) * 100) * 100) / 100
          : 0;
      }
    }

    if (!v.priceLocked) {
      v.vendorQuotedPrice = null;
      v.commissionPercent = null;
      v.commissionAmount = null;
      v.priceHikeReason = null;
    }

    if (v.pricingQuantity != null) {
      const qty = Number(v.pricingQuantity);
      if (!Number.isFinite(qty) || qty <= 0) {
        v.pricingQuantity = null;
      }
    }

    if (!v.pricingQuantity && v.pricingQuantityUnit) {
      v.pricingQuantityUnit = null;
    }
  }

  const totals = computeTotals(this);
  this.totalMinAmount = totals.totalMinAmount;
  this.totalMaxAmount = totals.totalMaxAmount;
  this.vendorsAccepted = computeVendorsAccepted(this);

  // Auto-compute status unless already COMPLETE
  if (this.status !== VENDOR_SELECTION_STATUS.COMPLETE) {
    if (!this.managerAssigned) {
      this.status = VENDOR_SELECTION_STATUS.MANAGER_YET_TO_BE_ASSIGNED;
    } else if (!this.vendorsAccepted) {
      this.status = VENDOR_SELECTION_STATUS.IN_REVIEW;
    } else {
      this.status = VENDOR_SELECTION_STATUS.FINALIZED;
    }
  }

  next();
});

VendorSelectionSchema.index({ authId: 1, createdAt: -1 });

module.exports = mongoose.model('VendorSelection', VendorSelectionSchema);
