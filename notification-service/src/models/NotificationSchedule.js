const mongoose = require('mongoose');

const notificationScheduleSchema = new mongoose.Schema(
  {
    scheduleId: {
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
      trim: true,
      uppercase: true,
    },
    eventId: {
      type: String,
      default: null,
      index: true,
      trim: true,
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
    triggerAt: {
      type: Date,
      required: true,
      index: true,
    },
    status: {
      type: String,
      enum: ['PENDING', 'SENT', 'FAILED'],
      default: 'PENDING',
      index: true,
    },
    attempts: {
      type: Number,
      default: 0,
      min: 0,
    },
    lastError: {
      type: String,
      default: null,
      maxlength: 500,
    },
    processedAt: {
      type: Date,
      default: null,
    },
  },
  {
    timestamps: true,
    versionKey: false,
  }
);

notificationScheduleSchema.index({ status: 1, triggerAt: 1 });
notificationScheduleSchema.index({ recipientAuthId: 1, dedupeKey: 1 }, { unique: true, sparse: true });

module.exports = mongoose.model('NotificationSchedule', notificationScheduleSchema);
