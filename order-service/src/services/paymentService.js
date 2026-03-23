const axios = require('axios');
const crypto = require('crypto');
const Joi = require('joi');
const PaymentOrder = require('../models/PaymentOrder');
const paymentSettingsService = require('./paymentSettingsService');
const { getRazorpayClient } = require('../config/razorpay');
const { publishEvent } = require('../kafka/eventProducer');
const createApiError = require('../utils/ApiError');
const logger = require('../utils/logger');
const {
  isAxiosLikeError,
  normalizeAxiosError,
  normalizeRazorpayError,
} = require('../utils/normalizeError');

const createOrderSchema = Joi.object({
  eventId: Joi.string().trim().required(),
  orderType: Joi
    .string()
    .trim()
    .valid(
      'PLANNING EVENT',
      'PLANNING EVENT DEPOSIT FEE',
      'PLANNING EVENT VENDOR CONFIRMATION FEE',
      'PROMOTE EVENT',
      'TICKET SALE',
      'REFUND'
    )
    .required(),
  amount: Joi.number().positive().optional(),
  currency: Joi.string().trim().uppercase().length(3).optional(),
  // Razorpay receipt has a hard 40-character limit
  receipt: Joi.string().trim().max(40).optional(),
  notes: Joi.object().optional(),
});

const buildRazorpayReceipt = ({ eventId, authId }) => {
  // 40 hex chars (sha1) => always within Razorpay's 40-char limit
  return crypto
    .createHash('sha1')
    .update(`${eventId}:${authId}:${Date.now()}:${crypto.randomBytes(8).toString('hex')}`)
    .digest('hex');
};

const verifyPaymentSchema = Joi.object({
  eventId: Joi.string().trim().required(),
  razorpay_order_id: Joi.string().trim().required(),
  razorpay_payment_id: Joi.string().trim().required(),
  razorpay_signature: Joi.string().trim().required(),
});

const refundPaymentSchema = Joi.object({
  eventId: Joi.string().trim().required(),
  amount: Joi.number().positive().optional(),
  notes: Joi.object().optional(),
  reason: Joi.string().trim().max(500).optional(),
});

const buildTransactionEnvelope = ({
  order,
  user,
  orderType,
  paymentStatus,
  transactionId,
  amount,
  source,
}) => {
  const now = new Date();
  const iso = now.toISOString();

  return {
    eventId: order.eventId,
    authId: order.authId,
    username: user?.username || null,
    transactionId,
    transactionDate: iso.slice(0, 10),
    transactionTime: iso.slice(11, 19),
    transactionTimestamp: iso,
    orderType,
    amount,
    currency: order.currency,
    paymentStatus,
    source,
    paymentOrderId: order._id.toString(),
    razorpayOrderId: order.razorpayOrderId,
    razorpayPaymentId: order.razorpayPaymentId || null,
    razorpayRefundId: order.razorpayRefundId || null,
  };
};

const publishTransactionUpdate = async (payload) => {
  try {
    await publishEvent('PAYMENT_TRANSACTION_UPDATED', payload, payload.eventId);
  } catch (kafkaError) {
    logger.error('Failed to publish PAYMENT_TRANSACTION_UPDATED event:', kafkaError);
  }
};

const getPlanningForUser = async (eventId, user) => {
  const eventServiceUrl = process.env.EVENT_SERVICE_URL || 'http://event-service:8086';
  const response = await axios.get(`${eventServiceUrl}/planning/${eventId}`, {
    timeout: 10000,
    headers: {
      'x-auth-id': user.authId,
      'x-user-id': user.userId || '',
      'x-user-email': user.email || '',
      'x-user-username': user.username || '',
      'x-user-role': user.role || 'USER',
    },
  });

  return response.data?.data;
};

const getPromoteForUser = async (eventId, user) => {
  const eventServiceUrl = process.env.EVENT_SERVICE_URL || 'http://event-service:8086';
  const response = await axios.get(`${eventServiceUrl}/promote/${eventId}`, {
    timeout: 10000,
    headers: {
      'x-auth-id': user.authId,
      'x-user-id': user.userId || '',
      'x-user-email': user.email || '',
      'x-user-username': user.username || '',
      'x-user-role': user.role || 'USER',
    },
  });

  return response.data?.data;
};

const getVendorSelectionForUser = async (eventId, user) => {
  const eventServiceUrl = process.env.EVENT_SERVICE_URL || 'http://event-service:8086';
  const response = await axios.get(`${eventServiceUrl}/vendor-selection/${encodeURIComponent(eventId)}`, {
    timeout: 10000,
    headers: {
      'x-auth-id': user.authId,
      'x-user-id': user.userId || '',
      'x-user-email': user.email || '',
      'x-user-username': user.username || '',
      'x-user-role': user.role || 'USER',
    },
  });

  return response.data?.data;
};

const getPlanningQuoteLatestForUser = async (eventId, user) => {
  const eventServiceUrl = process.env.EVENT_SERVICE_URL || 'http://event-service:8086';
  const response = await axios.get(`${eventServiceUrl}/planning/${encodeURIComponent(eventId)}/quote/latest`, {
    timeout: 10000,
    headers: {
      'x-auth-id': user.authId,
      'x-user-id': user.userId || '',
      'x-user-email': user.email || '',
      'x-user-username': user.username || '',
      'x-user-role': user.role || 'USER',
    },
  });

  return response.data?.data;
};

const createOrder = async (payload, user) => {
  if (!user?.authId) {
    throw createApiError(401, 'User authentication information missing');
  }

  const { value, error } = createOrderSchema.validate(payload);
  if (error) {
    throw createApiError(400, error.details[0].message);
  }

  const orderType = value.orderType;

  let upstreamRecord;
  try {
    if (orderType === 'PROMOTE EVENT') {
      upstreamRecord = await getPromoteForUser(value.eventId, user);
    } else {
      upstreamRecord = await getPlanningForUser(value.eventId, user);
    }
  } catch (err) {
    if (isAxiosLikeError(err)) {
      throw normalizeAxiosError(err, { upstreamName: 'event-service' });
    }
    throw err;
  }

  if (!upstreamRecord) {
    throw createApiError(404, orderType === 'PROMOTE EVENT' ? 'Promote event not found' : 'Planning not found');
  }

  // Payment gating differs by order type.
  if (orderType === 'PROMOTE EVENT') {
    if (Boolean(upstreamRecord.platformFeePaid)) {
      throw createApiError(409, 'Payment is already completed for this event');
    }
  } else if (orderType === 'PLANNING EVENT DEPOSIT FEE') {
    const planningPlatformFeePaid = Boolean(upstreamRecord.platformFeePaid) || Boolean(upstreamRecord.isPaid);
    if (!planningPlatformFeePaid) {
      throw createApiError(409, 'Planning fee must be paid before paying deposit');
    }
    if (Boolean(upstreamRecord.depositPaid)) {
      throw createApiError(409, 'Deposit is already paid for this event');
    }
  } else if (orderType === 'PLANNING EVENT VENDOR CONFIRMATION FEE') {
    const planningPlatformFeePaid = Boolean(upstreamRecord.platformFeePaid) || Boolean(upstreamRecord.isPaid);
    if (!planningPlatformFeePaid) {
      throw createApiError(409, 'Planning fee must be paid before paying vendor confirmation');
    }
    if (!Boolean(upstreamRecord.depositPaid)) {
      throw createApiError(409, 'Deposit must be paid before vendor confirmation');
    }

    const depositPaidAmountPaise = upstreamRecord.depositPaidAmountPaise;
    if (depositPaidAmountPaise == null) {
      throw createApiError(409, 'Deposit amount is not recorded yet for this event');
    }

    const normalizedStatus = String(upstreamRecord.status || '').trim().toUpperCase();
    if (normalizedStatus !== 'APPROVED') {
      throw createApiError(409, 'Planning must be APPROVED before paying vendor confirmation');
    }

    if (Boolean(upstreamRecord.vendorConfirmationPaid)) {
      throw createApiError(409, 'Vendor confirmation is already paid for this event');
    }
  } else {
    const alreadyPaid = Boolean(upstreamRecord.platformFeePaid) || Boolean(upstreamRecord.isPaid);
    if (alreadyPaid) {
      throw createApiError(409, 'Payment is already completed for this event');
    }
  }

  // For most order types, it's fine to reuse the latest CREATED order.
  // For deposit + vendor confirmation orders, the amount is derived from upstream state,
  // so only reuse an existing order if the amount still matches.
  if (orderType !== 'PLANNING EVENT DEPOSIT FEE' && orderType !== 'PLANNING EVENT VENDOR CONFIRMATION FEE') {
    const activeOrder = await PaymentOrder.findOne({
      eventId: value.eventId,
      authId: user.authId,
      orderType,
      status: 'CREATED',
    })
      .sort({ createdAt: -1 })
      .lean();

    if (activeOrder) {
      return {
        eventId: activeOrder.eventId,
        orderId: activeOrder._id,
        razorpayOrderId: activeOrder.razorpayOrderId,
        transactionId: activeOrder.transactionId,
        orderType: activeOrder.orderType,
        amount: activeOrder.amount,
        currency: activeOrder.currency,
        keyId: process.env.RAZORPAY_KEY_ID,
        status: activeOrder.status,
      };
    }
  }

  let amountInInr;
  let amountInPaiseOverride = null;
  let depositPercent = null;
  const currency = value.currency || 'INR';
  if (orderType === 'PLANNING EVENT DEPOSIT FEE') {
    let selection;
    try {
      selection = await getVendorSelectionForUser(value.eventId, user);
    } catch (err) {
      if (isAxiosLikeError(err)) {
        throw normalizeAxiosError(err, { upstreamName: 'event-service' });
      }
      throw err;
    }

    const totalMinAmount = Number(selection?.totalMinAmount ?? 0);
    if (!Number.isFinite(totalMinAmount) || totalMinAmount <= 0) {
      throw createApiError(409, 'Cannot take deposit until a minimum total amount is available');
    }

    const settings = await paymentSettingsService.getSettings();
    const percent = Number(settings?.planningDepositPercent ?? 25);
    const safePercent = Number.isFinite(percent) && percent > 0 ? percent : 25;
    depositPercent = safePercent;

    amountInInr = Math.round((totalMinAmount * safePercent) / 100);
    if (!Number.isFinite(amountInInr) || amountInInr <= 0) {
      throw createApiError(409, 'Computed deposit amount is invalid');
    }
  } else if (orderType === 'PLANNING EVENT VENDOR CONFIRMATION FEE') {
    let quote;
    try {
      quote = await getPlanningQuoteLatestForUser(value.eventId, user);
    } catch (err) {
      if (isAxiosLikeError(err)) {
        throw normalizeAxiosError(err, { upstreamName: 'event-service' });
      }
      throw err;
    }

    const vendorMinPaise = Number(quote?.vendorSubtotal?.minPaise ?? 0);
    if (!Number.isFinite(vendorMinPaise) || vendorMinPaise <= 0) {
      throw createApiError(409, 'Cannot compute vendor confirmation amount without a locked quote');
    }

    const depositPaidAmountPaise = Number(upstreamRecord.depositPaidAmountPaise ?? 0);
    if (!Number.isFinite(depositPaidAmountPaise) || depositPaidAmountPaise < 0) {
      throw createApiError(409, 'Deposit amount is invalid for this event');
    }

    const confirmationDuePaise = Math.max(0, Math.round((vendorMinPaise * 25) / 100) - depositPaidAmountPaise);
    if (!Number.isFinite(confirmationDuePaise) || confirmationDuePaise <= 0) {
      throw createApiError(409, 'No vendor confirmation amount is due for this event');
    }

    amountInPaiseOverride = confirmationDuePaise;
  } else {
    amountInInr = value.amount || Number(process.env.DEFAULT_PLATFORM_FEE_INR) || 15000;
  }

  const amountInPaise = amountInPaiseOverride != null
    ? Math.round(Number(amountInPaiseOverride))
    : Math.round(Number(amountInInr) * 100);

  if (orderType === 'PLANNING EVENT DEPOSIT FEE') {
    const matchingActive = await PaymentOrder.findOne({
      eventId: value.eventId,
      authId: user.authId,
      orderType,
      status: 'CREATED',
      currency,
      amount: amountInPaise,
    })
      .sort({ createdAt: -1 })
      .lean();

    if (matchingActive) {
      return {
        eventId: matchingActive.eventId,
        orderId: matchingActive._id,
        razorpayOrderId: matchingActive.razorpayOrderId,
        transactionId: matchingActive.transactionId,
        orderType: matchingActive.orderType,
        amount: matchingActive.amount,
        currency: matchingActive.currency,
        keyId: process.env.RAZORPAY_KEY_ID,
        status: matchingActive.status,
      };
    }
  }

  if (orderType === 'PLANNING EVENT VENDOR CONFIRMATION FEE') {
    const matchingActive = await PaymentOrder.findOne({
      eventId: value.eventId,
      authId: user.authId,
      orderType,
      status: 'CREATED',
      currency,
      amount: amountInPaise,
    })
      .sort({ createdAt: -1 })
      .lean();

    if (matchingActive) {
      return {
        eventId: matchingActive.eventId,
        orderId: matchingActive._id,
        razorpayOrderId: matchingActive.razorpayOrderId,
        transactionId: matchingActive.transactionId,
        orderType: matchingActive.orderType,
        amount: matchingActive.amount,
        currency: matchingActive.currency,
        keyId: process.env.RAZORPAY_KEY_ID,
        status: matchingActive.status,
      };
    }
  }

  const razorpay = getRazorpayClient();
  let order;
  try {
    order = await razorpay.orders.create({
      amount: amountInPaise,
      currency,
      receipt: value.receipt || buildRazorpayReceipt({ eventId: value.eventId, authId: user.authId }),
      notes: {
        eventId: value.eventId,
        authId: user.authId,
        ...(orderType === 'PLANNING EVENT DEPOSIT FEE'
          ? {
              computedFrom: 'vendor-selection.totalMinAmount',
              planningDepositPercent: depositPercent,
            }
          : {}),
        ...(orderType === 'PLANNING EVENT VENDOR CONFIRMATION FEE'
          ? {
              computedFrom: 'planning-quote.vendorSubtotal.minPaise',
              vendorConfirmationPercent: 25,
              depositPaidAmountPaise: upstreamRecord.depositPaidAmountPaise,
            }
          : {}),
        ...(value.notes || {}),
      },
    });
  } catch (err) {
    // Razorpay SDK errors often keep the real message under `err.error.description`
    throw normalizeRazorpayError(err, {
      defaultStatusCode: 422,
      defaultMessage: 'Failed to create payment order with provider',
    });
  }

  const paymentOrder = await PaymentOrder.create({
    eventId: value.eventId,
    authId: user.authId,
    orderType,
    amount: order.amount,
    currency: order.currency,
    razorpayOrderId: order.id,
    receipt: order.receipt,
    status: 'CREATED',
    notes: order.notes || {},
  });

  try {
    await publishEvent('PAYMENT_ORDER_CREATED', {
      eventId: value.eventId,
      authId: user.authId,
      transactionId: paymentOrder.transactionId,
      orderType: paymentOrder.orderType,
      razorpayOrderId: order.id,
      amount: order.amount,
      currency: order.currency,
      paymentStatus: paymentOrder.status,
    });
  } catch (kafkaError) {
    logger.error('Failed to publish PAYMENT_ORDER_CREATED event:', kafkaError);
  }

  await publishTransactionUpdate(
    buildTransactionEnvelope({
      order: paymentOrder,
      user,
      orderType,
      paymentStatus: 'CREATED',
      transactionId: paymentOrder.transactionId,
      amount: paymentOrder.amount,
      source: 'order-create',
    })
  );

  return {
    eventId: value.eventId,
    orderId: paymentOrder._id,
    razorpayOrderId: order.id,
    transactionId: paymentOrder.transactionId,
    orderType: paymentOrder.orderType,
    amount: order.amount,
    currency: order.currency,
    keyId: process.env.RAZORPAY_KEY_ID,
    status: paymentOrder.status,
  };
};

const verifyPayment = async (payload, user) => {
  if (!user?.authId) {
    throw createApiError(401, 'User authentication information missing');
  }

  const { value, error } = verifyPaymentSchema.validate(payload);
  if (error) {
    throw createApiError(400, error.details[0].message);
  }

  const paymentOrder = await PaymentOrder.findOne({
    eventId: value.eventId,
    authId: user.authId,
    razorpayOrderId: value.razorpay_order_id,
  });

  if (!paymentOrder) {
    throw createApiError(404, 'Payment order not found');
  }

  if (paymentOrder.status === 'PAID') {
    return {
      eventId: paymentOrder.eventId,
      transactionId: paymentOrder.transactionId,
      orderType: paymentOrder.orderType,
      status: paymentOrder.status,
      paymentId: paymentOrder.razorpayPaymentId,
      orderId: paymentOrder.razorpayOrderId,
    };
  }

  const generatedSignature = crypto
    .createHmac('sha256', process.env.RAZORPAY_KEY_SECRET)
    .update(`${value.razorpay_order_id}|${value.razorpay_payment_id}`)
    .digest('hex');

  if (generatedSignature !== value.razorpay_signature) {
    paymentOrder.status = 'FAILED';
    await paymentOrder.save();

    await publishTransactionUpdate(
      buildTransactionEnvelope({
        order: paymentOrder,
        user,
        orderType: paymentOrder.orderType,
        paymentStatus: 'FAILED',
        transactionId: paymentOrder.transactionId,
        amount: paymentOrder.amount,
        source: 'verify-endpoint',
      })
    );

    throw createApiError(400, 'Payment signature verification failed');
  }

  paymentOrder.status = 'PAID';
  paymentOrder.razorpayPaymentId = value.razorpay_payment_id;
  paymentOrder.razorpaySignature = value.razorpay_signature;
  paymentOrder.paidAt = new Date();
  await paymentOrder.save();

  try {
    await publishEvent('PAYMENT_SUCCESS', {
      eventId: paymentOrder.eventId,
      authId: paymentOrder.authId,
      paymentOrderId: paymentOrder._id.toString(),
      transactionId: paymentOrder.transactionId,
      orderType: paymentOrder.orderType,
      razorpayOrderId: paymentOrder.razorpayOrderId,
      razorpayPaymentId: paymentOrder.razorpayPaymentId,
      amount: paymentOrder.amount,
      currency: paymentOrder.currency,
      paidAt: paymentOrder.paidAt.toISOString(),
      paymentStatus: paymentOrder.status,
      source: 'verify-endpoint',
    });
  } catch (kafkaError) {
    logger.error('Failed to publish PAYMENT_SUCCESS event:', kafkaError);
  }

  await publishTransactionUpdate(
    buildTransactionEnvelope({
      order: paymentOrder,
      user,
      orderType: paymentOrder.orderType,
      paymentStatus: 'PAID',
      transactionId: paymentOrder.transactionId,
      amount: paymentOrder.amount,
      source: 'verify-endpoint',
    })
  );

  return {
    eventId: paymentOrder.eventId,
    transactionId: paymentOrder.transactionId,
    orderType: paymentOrder.orderType,
    status: paymentOrder.status,
    paymentId: paymentOrder.razorpayPaymentId,
    orderId: paymentOrder.razorpayOrderId,
  };
};

const verifyWebhookSignature = (rawBody, signature) => {
  const webhookSecret = process.env.RAZORPAY_WEBHOOK_SECRET;
  if (!webhookSecret) {
    throw createApiError(500, 'Razorpay webhook secret is not configured');
  }

  const expectedSignature = crypto.createHmac('sha256', webhookSecret).update(rawBody).digest('hex');
  return expectedSignature === signature;
};

const handleWebhook = async (rawBody, signatureHeader) => {
  if (!signatureHeader) {
    throw createApiError(400, 'Missing Razorpay webhook signature');
  }

  if (!verifyWebhookSignature(rawBody, signatureHeader)) {
    throw createApiError(400, 'Invalid Razorpay webhook signature');
  }

  const payload = JSON.parse(rawBody.toString('utf8'));
  const eventType = payload.event;

  if (eventType !== 'payment.captured') {
    return { acknowledged: true, ignored: true, eventType };
  }

  const entity = payload.payload?.payment?.entity;
  const orderId = entity?.order_id;
  const paymentId = entity?.id;
  const amount = entity?.amount;
  const currency = entity?.currency;

  if (!orderId || !paymentId) {
    throw createApiError(400, 'Invalid webhook payload for payment.captured');
  }

  const paymentOrder = await PaymentOrder.findOne({ razorpayOrderId: orderId });
  if (!paymentOrder) {
    throw createApiError(404, 'Payment order not found for webhook payload');
  }

  if (paymentOrder.status !== 'PAID') {
    paymentOrder.status = 'PAID';
    paymentOrder.razorpayPaymentId = paymentId;
    paymentOrder.paidAt = new Date();
    await paymentOrder.save();

    try {
      await publishEvent('PAYMENT_SUCCESS', {
        eventId: paymentOrder.eventId,
        authId: paymentOrder.authId,
        paymentOrderId: paymentOrder._id.toString(),
        transactionId: paymentOrder.transactionId,
        orderType: paymentOrder.orderType,
        razorpayOrderId: paymentOrder.razorpayOrderId,
        razorpayPaymentId: paymentOrder.razorpayPaymentId,
        amount: amount || paymentOrder.amount,
        currency: currency || paymentOrder.currency,
        paidAt: paymentOrder.paidAt.toISOString(),
        paymentStatus: paymentOrder.status,
        source: 'razorpay-webhook',
      });
    } catch (kafkaError) {
      logger.error('Failed to publish PAYMENT_SUCCESS from webhook:', kafkaError);
    }

    await publishTransactionUpdate(
      buildTransactionEnvelope({
        order: paymentOrder,
        orderType: paymentOrder.orderType,
        paymentStatus: 'PAID',
        transactionId: paymentOrder.transactionId,
        amount: amount || paymentOrder.amount,
        source: 'razorpay-webhook',
      })
    );
  }

  return {
    acknowledged: true,
    eventType,
    orderId,
    paymentId,
  };
};

const getOrderByEventId = async (eventId, user) => {
  const order = await PaymentOrder.findOne({ eventId, authId: user.authId }).sort({ createdAt: -1 }).lean();

  if (!order) {
    throw createApiError(404, 'No payment order found for event');
  }

  return {
    eventId: order.eventId,
    transactionId: order.transactionId,
    orderType: order.orderType,
    status: order.status,
    amount: order.amount,
    currency: order.currency,
    razorpayOrderId: order.razorpayOrderId,
    razorpayPaymentId: order.razorpayPaymentId,
    razorpayRefundId: order.razorpayRefundId,
    paidAt: order.paidAt,
    refundedAt: order.refundedAt,
    refundedAmount: order.refundedAmount,
    createdAt: order.createdAt,
  };
};

const getOrdersByEventIdForAdmin = async (eventId) => {
  if (!eventId || !String(eventId).trim()) {
    throw createApiError(400, 'Event ID is required');
  }

  const orders = await PaymentOrder.find({ eventId: String(eventId).trim() })
    .sort({ createdAt: -1 })
    .lean();

  return {
    eventId: String(eventId).trim(),
    orders: orders.map((order) => ({
      eventId: order.eventId,
      authId: order.authId,
      transactionId: order.transactionId,
      orderType: order.orderType,
      status: order.status,
      amount: order.amount,
      currency: order.currency,
      razorpayOrderId: order.razorpayOrderId,
      razorpayPaymentId: order.razorpayPaymentId,
      razorpayRefundId: order.razorpayRefundId,
      paidAt: order.paidAt,
      refundedAt: order.refundedAt,
      refundedAmount: order.refundedAmount,
      createdAt: order.createdAt,
    })),
  };
};

const refundPayment = async (payload, user) => {
  if (!user?.authId) {
    throw createApiError(401, 'User authentication information missing');
  }

  const { value, error } = refundPaymentSchema.validate(payload);
  if (error) {
    throw createApiError(400, error.details[0].message);
  }

  const paymentOrder = await PaymentOrder.findOne({
    eventId: value.eventId,
    authId: user.authId,
  }).sort({ createdAt: -1 });

  if (!paymentOrder) {
    throw createApiError(404, 'Payment order not found');
  }

  if (paymentOrder.status !== 'PAID') {
    throw createApiError(409, 'Refund can only be processed for PAID orders');
  }

  if (!paymentOrder.razorpayPaymentId) {
    throw createApiError(400, 'Razorpay payment id is missing for this order');
  }

  const refundAmount = value.amount ? Math.round(value.amount * 100) : paymentOrder.amount;

  const razorpay = getRazorpayClient();
  const refund = await razorpay.payments.refund(paymentOrder.razorpayPaymentId, {
    amount: refundAmount,
    speed: 'normal',
    notes: {
      eventId: paymentOrder.eventId,
      authId: paymentOrder.authId,
      reason: value.reason || 'User initiated refund',
      ...(value.notes || {}),
    },
  });

  paymentOrder.status = 'REFUNDED';
  paymentOrder.orderType = 'REFUND';
  paymentOrder.refundedAt = new Date();
  paymentOrder.refundedAmount = refund.amount;
  paymentOrder.razorpayRefundId = refund.id;
  paymentOrder.refundReason = value.reason || null;
  await paymentOrder.save();

  try {
    await publishEvent('PAYMENT_REFUND_SUCCESS', {
      eventId: paymentOrder.eventId,
      authId: paymentOrder.authId,
      paymentOrderId: paymentOrder._id.toString(),
      transactionId: paymentOrder.transactionId,
      orderType: 'REFUND',
      paymentStatus: paymentOrder.status,
      razorpayOrderId: paymentOrder.razorpayOrderId,
      razorpayPaymentId: paymentOrder.razorpayPaymentId,
      razorpayRefundId: paymentOrder.razorpayRefundId,
      amount: paymentOrder.refundedAmount,
      currency: paymentOrder.currency,
      refundedAt: paymentOrder.refundedAt.toISOString(),
    });
  } catch (kafkaError) {
    logger.error('Failed to publish PAYMENT_REFUND_SUCCESS event:', kafkaError);
  }

  await publishTransactionUpdate(
    buildTransactionEnvelope({
      order: paymentOrder,
      user,
      orderType: 'REFUND',
      paymentStatus: 'REFUNDED',
      transactionId: paymentOrder.transactionId,
      amount: paymentOrder.refundedAmount,
      source: 'refund-endpoint',
    })
  );

  return {
    eventId: paymentOrder.eventId,
    transactionId: paymentOrder.transactionId,
    orderType: 'REFUND',
    status: paymentOrder.status,
    refundId: paymentOrder.razorpayRefundId,
    refundedAmount: paymentOrder.refundedAmount,
    refundedAt: paymentOrder.refundedAt,
  };
};

module.exports = {
  createOrder,
  verifyPayment,
  refundPayment,
  handleWebhook,
  getOrderByEventId,
  getOrdersByEventIdForAdmin,
};
