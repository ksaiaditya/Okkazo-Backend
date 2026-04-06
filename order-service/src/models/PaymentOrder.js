const mongoose = require('mongoose');
const { v4: uuidv4 } = require('uuid');

const PaymentOrderSchema = new mongoose.Schema(
  {
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
    orderType: {
      type: String,
      enum: ['PLANNING EVENT', 'PLANNING EVENT DEPOSIT FEE', 'PLANNING EVENT VENDOR CONFIRMATION FEE', 'PLANNING EVENT REMAINING FEE', 'PROMOTE EVENT', 'TICKET SALE', 'REFUND'],
      required: true,
      index: true,
    },
    transactionId: {
      type: String,
      required: true,
      unique: true,
      default: () => uuidv4(),
      index: true,
      trim: true,
    },
    amount: {
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
    razorpayOrderId: {
      type: String,
      required: true,
      unique: true,
      index: true,
      trim: true,
    },
    receipt: {
      type: String,
      trim: true,
    },
    status: {
      type: String,
      enum: ['CREATED', 'PAID', 'FAILED', 'REFUNDED', 'REFUND_FAILED'],
      default: 'CREATED',
      index: true,
    },
    razorpayPaymentId: {
      type: String,
      trim: true,
      index: true,
    },
    razorpaySignature: {
      type: String,
      trim: true,
    },
    paidAt: {
      type: Date,
      default: null,
    },
    refundedAt: {
      type: Date,
      default: null,
    },
    refundedAmount: {
      type: Number,
      default: null,
    },
    razorpayRefundId: {
      type: String,
      trim: true,
      index: true,
    },
    refundReason: {
      type: String,
      trim: true,
      maxlength: 500,
    },
    notes: {
      type: mongoose.Schema.Types.Mixed,
      default: {},
    },
  },
  {
    timestamps: true,
    collection: 'payment_orders',
  }
);

PaymentOrderSchema.index({ eventId: 1, authId: 1, createdAt: -1 });

module.exports = mongoose.model('PaymentOrder', PaymentOrderSchema);
