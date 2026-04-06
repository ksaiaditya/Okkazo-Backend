const mongoose = require('mongoose');
const { v4: uuidv4 } = require('uuid');

const VendorPayoutSchema = new mongoose.Schema(
  {
    payoutId: {
      type: String,
      required: true,
      unique: true,
      default: () => uuidv4(),
      index: true,
      trim: true,
    },
    eventId: {
      type: String,
      required: true,
      trim: true,
      index: true,
    },
    vendorAuthId: {
      type: String,
      required: true,
      trim: true,
      index: true,
    },
    service: {
      type: String,
      required: true,
      trim: true,
      index: true,
    },
    serviceId: {
      type: String,
      trim: true,
      default: null,
    },
    managerAuthId: {
      type: String,
      required: true,
      trim: true,
      index: true,
    },
    linkedAccountId: {
      type: String,
      required: true,
      trim: true,
      index: true,
    },
    sourcePaymentId: {
      type: String,
      required: true,
      trim: true,
      index: true,
    },
    lockedAmountPaise: {
      type: Number,
      required: true,
      min: 0,
    },
    commissionAmountPaise: {
      type: Number,
      required: true,
      min: 0,
    },
    payoutAmountPaise: {
      type: Number,
      required: true,
      min: 1,
    },
    currency: {
      type: String,
      required: true,
      default: 'INR',
      trim: true,
    },
    status: {
      type: String,
      enum: ['INITIATED', 'SUCCESS', 'FAILED'],
      default: 'INITIATED',
      index: true,
    },
    razorpayTransferId: {
      type: String,
      trim: true,
      default: null,
      index: true,
    },
    failureReason: {
      type: String,
      trim: true,
      default: null,
      maxlength: 600,
    },
    paidAt: {
      type: Date,
      default: null,
    },
    notes: {
      type: mongoose.Schema.Types.Mixed,
      default: {},
    },
  },
  {
    timestamps: true,
    collection: 'vendor_payouts',
  }
);

VendorPayoutSchema.index({ eventId: 1, vendorAuthId: 1, service: 1 }, { unique: true });

module.exports = mongoose.model('VendorPayout', VendorPayoutSchema);
