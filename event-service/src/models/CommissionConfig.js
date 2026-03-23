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
