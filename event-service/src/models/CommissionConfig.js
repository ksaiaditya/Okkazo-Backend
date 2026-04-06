const mongoose = require('mongoose');

const CommissionConfigSchema = new mongoose.Schema(
  {
    key: {
      type: String,
      required: true,
      unique: true,
      index: true,
    },
    rates: {
      type: Map,
      of: Number,
      default: {},
    },
    vendorHikeRate: {
      type: Number,
      min: 1,
      max: 10,
      default: 1.25,
    },
    updatedByAuthId: {
      type: String,
      default: null,
    },
  },
  {
    timestamps: true,
  }
);

module.exports = mongoose.model('CommissionConfig', CommissionConfigSchema);
