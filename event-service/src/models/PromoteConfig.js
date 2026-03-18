const mongoose = require('mongoose');

const PromoteConfigSchema = new mongoose.Schema(
  {
    key: {
      type: String,
      required: true,
      unique: true,
      index: true,
      trim: true,
    },
    platformFee: {
      type: Number,
      required: true,
      min: 0,
    },
    serviceChargePercent: {
      type: Number,
      required: true,
      min: 0,
      max: 100,
    },
    updatedByAuthId: {
      type: String,
      default: null,
      trim: true,
    },
  },
  {
    timestamps: true,
    collection: 'promote_config',
  }
);

module.exports = mongoose.model('PromoteConfig', PromoteConfigSchema);
