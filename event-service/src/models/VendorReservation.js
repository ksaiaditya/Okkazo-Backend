const mongoose = require('mongoose');

const VendorReservationSchema = new mongoose.Schema(
  {
    vendorAuthId: {
      type: String,
      required: true,
      trim: true,
      index: true,
    },
    // YYYY-MM-DD
    day: {
      type: String,
      required: true,
      trim: true,
      match: [/^\d{4}-\d{2}-\d{2}$/, 'day must be in YYYY-MM-DD format'],
      index: true,
    },
    eventId: {
      type: String,
      required: true,
      trim: true,
      index: true,
    },
    authId: {
      type: String,
      required: true,
      trim: true,
      index: true,
    },
    // Optional: which service category reserved this vendor for this event
    service: {
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

// One vendor can only be reserved for one event per day.
VendorReservationSchema.index({ vendorAuthId: 1, day: 1 }, { unique: true });

const VendorReservation = mongoose.model('VendorReservation', VendorReservationSchema);

module.exports = VendorReservation;
