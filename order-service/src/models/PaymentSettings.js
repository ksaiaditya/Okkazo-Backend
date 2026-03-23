const mongoose = require('mongoose');

const PaymentSettingsSchema = new mongoose.Schema(
  {
    planningDepositPercent: {
      type: Number,
      min: 1,
      max: 100,
      default: 25,
    },
  },
  {
    timestamps: true,
    collection: 'payment_settings',
  }
);

// Ensure we only ever keep one settings doc around.
PaymentSettingsSchema.index({ createdAt: 1 });

module.exports = mongoose.model('PaymentSettings', PaymentSettingsSchema);
