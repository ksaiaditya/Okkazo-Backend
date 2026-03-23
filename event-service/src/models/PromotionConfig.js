const mongoose = require('mongoose');

const PromotionItemSchema = new mongoose.Schema(
  {
    value: {
      type: String,
      required: true,
      trim: true,
    },
    label: {
      type: String,
      default: null,
      trim: true,
    },
    fee: {
      type: Number,
      required: true,
      min: 0,
    },
    active: {
      type: Boolean,
      default: true,
    },
  },
  { _id: false }
);

const PromotionConfigSchema = new mongoose.Schema(
  {
    key: {
      type: String,
      required: true,
      unique: true,
      index: true,
      trim: true,
    },
    publicPromotionOptions: {
      type: [PromotionItemSchema],
      default: [],
    },
    promotePackages: {
      type: [PromotionItemSchema],
      default: [],
    },
    updatedByAuthId: {
      type: String,
      default: null,
      trim: true,
    },
  },
  { timestamps: true }
);

module.exports = mongoose.model('PromotionConfig', PromotionConfigSchema);
