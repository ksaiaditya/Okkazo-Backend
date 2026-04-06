const mongoose = require('mongoose');

const notificationSchema = new mongoose.Schema(
  {
    notificationId: {
      type: String,
      required: true,
      unique: true,
      index: true,
      trim: true,
    },
    recipientAuthId: {
      type: String,
      required: true,
      index: true,
      trim: true,
    },
    recipientRole: {
      type: String,
      required: true,
      index: true,
      trim: true,
      uppercase: true,
    },
    type: {
      type: String,
      required: true,
      index: true,
      trim: true,
      uppercase: true,
    },
    category: {
      type: String,
      required: true,
      index: true,
      trim: true,
      uppercase: true,
    },
    title: {
      type: String,
      required: true,
      trim: true,
      maxlength: 180,
    },
    message: {
      type: String,
      required: true,
      trim: true,
      maxlength: 1200,
    },
    actionUrl: {
      type: String,
      default: null,
      trim: true,
    },
    metadata: {
      type: mongoose.Schema.Types.Mixed,
      default: {},
    },
    dedupeKey: {
      type: String,
      default: null,
      trim: true,
      index: true,
      sparse: true,
    },
    unread: {
      type: Boolean,
      default: true,
      index: true,
    },
    readAt: {
      type: Date,
      default: null,
    },
  },
  {
    timestamps: true,
    versionKey: false,
  }
);

notificationSchema.index({ recipientAuthId: 1, createdAt: -1 });
notificationSchema.index({ recipientAuthId: 1, unread: 1, createdAt: -1 });
notificationSchema.index({ recipientAuthId: 1, dedupeKey: 1 }, { unique: true, sparse: true });

module.exports = mongoose.model('Notification', notificationSchema);
