const mongoose = require('mongoose');

const UserPayoutAccountSchema = new mongoose.Schema(
  {
    userAuthId: {
      type: String,
      required: true,
      unique: true,
      index: true,
      trim: true,
    },
    razorpayLinkedAccountId: {
      type: String,
      trim: true,
      default: null,
      index: true,
    },
    onboardingStatus: {
      type: String,
      enum: ['NOT_STARTED', 'PENDING', 'COMPLETED', 'REJECTED'],
      default: 'NOT_STARTED',
      index: true,
    },
    payoutsEnabled: {
      type: Boolean,
      default: false,
      index: true,
    },
    accountStatus: {
      type: String,
      trim: true,
      default: null,
    },
    legalBusinessName: {
      type: String,
      trim: true,
      default: null,
    },
    contactEmail: {
      type: String,
      trim: true,
      default: null,
    },
    contactPhone: {
      type: String,
      trim: true,
      default: null,
    },
    lastOnboardingLinkAt: {
      type: Date,
      default: null,
    },
    lastStatusSyncAt: {
      type: Date,
      default: null,
    },
    metadata: {
      type: mongoose.Schema.Types.Mixed,
      default: {},
    },
  },
  {
    timestamps: true,
    collection: 'user_payout_accounts',
  }
);

module.exports = mongoose.model('UserPayoutAccount', UserPayoutAccountSchema);
