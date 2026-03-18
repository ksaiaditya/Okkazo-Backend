const mongoose = require('mongoose');

const SOURCE = {
  MANUAL: 'MANUAL',
  BOOKING: 'BOOKING',
};

const vendorAvailabilitySchema = new mongoose.Schema(
  {
    // Vendor identifier (matches VendorApplication.authId / VendorService.authId)
    vendorAuthId: {
      type: String,
      required: true,
      index: true,
      trim: true,
    },

    // Day key stored as YYYY-MM-DD to avoid timezone issues
    day: {
      type: String,
      required: true,
      trim: true,
      match: [/^\d{4}-\d{2}-\d{2}$/, 'day must be in YYYY-MM-DD format'],
      index: true,
    },

    // Why this day is blocked
    source: {
      type: String,
      required: true,
      enum: Object.values(SOURCE),
      index: true,
    },

    // A stable identifier for the source:
    // - MANUAL: "MANUAL"
    // - BOOKING: eventId (or bookingId)
    sourceId: {
      type: String,
      required: true,
      trim: true,
      index: true,
    },

    // Optional vendor-provided note
    reason: {
      type: String,
      default: null,
      trim: true,
      maxlength: 500,
    },

    // Actor metadata
    createdByAuthId: {
      type: String,
      default: null,
      trim: true,
    },
    createdByRole: {
      type: String,
      default: null,
      trim: true,
    },
  },
  {
    timestamps: true,
    versionKey: false,
  }
);

// One vendor can have multiple blocks per day (e.g., MANUAL + BOOKING), but not duplicates.
vendorAvailabilitySchema.index(
  { vendorAuthId: 1, day: 1, source: 1, sourceId: 1 },
  { unique: true }
);

vendorAvailabilitySchema.index({ vendorAuthId: 1, day: 1 });

const VendorAvailability = mongoose.model('VendorAvailability', vendorAvailabilitySchema);

module.exports = {
  VendorAvailability,
  SOURCE,
};
