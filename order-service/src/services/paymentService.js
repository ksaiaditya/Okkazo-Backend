const axios = require('axios');
const crypto = require('crypto');
const Joi = require('joi');
const PDFDocument = require('pdfkit');
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
  notes: Joi.object().unknown(true).optional(),
});

const buildRazorpayReceipt = ({ eventId, authId }) => {
  // 40 hex chars (sha1) => always within Razorpay's 40-char limit
  return crypto
    .createHash('sha1')
    .update(`${eventId}:${authId}:${Date.now()}:${crypto.randomBytes(8).toString('hex')}`)
    .digest('hex');
};

const buildTicketLink = (ticketId) => {
  const normalizedTicketId = String(ticketId || '').trim();
  if (!normalizedTicketId) return null;

  const frontendBaseUrl = (process.env.FRONTEND_URL || 'http://localhost:5173').replace(/\/$/, '');
  return `${frontendBaseUrl}/user/ticket/${encodeURIComponent(normalizedTicketId)}`;
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

const adminLedgerQuerySchema = Joi.object({
  page: Joi.number().integer().min(1).default(1),
  limit: Joi.number().integer().min(1).max(100).default(10),
  search: Joi.string().trim().allow('').optional(),
  status: Joi.string().trim().uppercase().valid('CREATED', 'PAID', 'FAILED', 'REFUNDED', 'REFUND_FAILED').optional(),
  type: Joi.string().trim().valid(
    'PLANNING EVENT',
    'PLANNING EVENT DEPOSIT FEE',
    'PLANNING EVENT VENDOR CONFIRMATION FEE',
    'PROMOTE EVENT',
    'TICKET SALE',
    'REFUND'
  ).optional(),
  days: Joi.number().integer().min(1).max(3650).optional(),
  from: Joi.date().iso().optional(),
  to: Joi.date().iso().optional(),
  sortBy: Joi.string().trim().valid('createdAt', 'paidAt', 'amount').default('createdAt'),
  sortDir: Joi.string().trim().valid('asc', 'desc').default('desc'),
});

const adminReportQuerySchema = Joi.object({
  range: Joi.string().trim().lowercase().valid('last30', 'last90', 'ytd', 'custom').default('last30'),
  from: Joi.date().iso().optional(),
  to: Joi.date().iso().optional(),
  search: Joi.string().trim().allow('').optional(),
  recentLimit: Joi.number().integer().min(1).max(20).default(4),
});

const REPORT_REVENUE_STATUSES = ['PAID', 'REFUNDED'];

const normalizeLedgerAmount = (order) => {
  const isRefund = order.orderType === 'REFUND' || order.status === 'REFUNDED';
  const baseAmount = Number(order.refundedAmount ?? order.amount ?? 0) || 0;
  return isRefund ? -Math.abs(baseAmount) : Math.abs(baseAmount);
};

const mapOrderToAdminLedgerRow = (order) => ({
  transactionId: order.transactionId,
  eventId: order.eventId,
  authId: order.authId,
  type: order.orderType,
  status: order.status,
  amount: normalizeLedgerAmount(order),
  currency: order.currency || 'INR',
  vendor: order?.notes?.vendorName || order?.notes?.businessName || order?.notes?.vendor || 'System Auto',
  razorpayOrderId: order.razorpayOrderId || null,
  razorpayPaymentId: order.razorpayPaymentId || null,
  razorpayRefundId: order.razorpayRefundId || null,
  createdAt: order.createdAt,
  paidAt: order.paidAt,
  refundedAt: order.refundedAt,
});

const buildAdminLedgerMongoQuery = (filters) => {
  const query = {};

  if (filters.status) {
    query.status = filters.status;
  }

  if (filters.type) {
    query.orderType = filters.type;
  }

  const dateQuery = {};
  if (filters.days) {
    const start = new Date();
    start.setUTCDate(start.getUTCDate() - Number(filters.days));
    dateQuery.$gte = start;
  }

  if (filters.from) {
    const from = new Date(filters.from);
    if (!Number.isNaN(from.getTime())) {
      dateQuery.$gte = from;
    }
  }

  if (filters.to) {
    const to = new Date(filters.to);
    if (!Number.isNaN(to.getTime())) {
      to.setUTCHours(23, 59, 59, 999);
      dateQuery.$lte = to;
    }
  }

  if (Object.keys(dateQuery).length > 0) {
    query.createdAt = dateQuery;
  }

  if (filters.search) {
    const regex = new RegExp(filters.search, 'i');
    query.$or = [
      { transactionId: regex },
      { eventId: regex },
      { authId: regex },
      { orderType: regex },
      { status: regex },
      { razorpayOrderId: regex },
      { razorpayPaymentId: regex },
      { razorpayRefundId: regex },
    ];
  }

  return query;
};

const buildReportDateRange = ({ range, from, to }) => {
  const now = new Date();
  const currentEnd = new Date(now);
  let currentStart;

  if (range === 'ytd') {
    currentStart = new Date(Date.UTC(now.getUTCFullYear(), 0, 1, 0, 0, 0, 0));
  } else if (range === 'last90') {
    currentStart = new Date(now);
    currentStart.setUTCDate(currentStart.getUTCDate() - 89);
    currentStart.setUTCHours(0, 0, 0, 0);
  } else if (range === 'custom') {
    if (!from || !to) {
      throw createApiError(400, 'Both from and to dates are required for custom range');
    }

    currentStart = new Date(from);
    currentStart.setUTCHours(0, 0, 0, 0);

    currentEnd.setTime(new Date(to).getTime());
    currentEnd.setUTCHours(23, 59, 59, 999);
  } else {
    currentStart = new Date(now);
    currentStart.setUTCDate(currentStart.getUTCDate() - 29);
    currentStart.setUTCHours(0, 0, 0, 0);
  }

  if (currentStart.getTime() > currentEnd.getTime()) {
    throw createApiError(400, 'Invalid date range: start date is after end date');
  }

  const periodMs = currentEnd.getTime() - currentStart.getTime() + 1;
  const previousEnd = new Date(currentStart.getTime() - 1);
  const previousStart = new Date(previousEnd.getTime() - periodMs + 1);

  return {
    currentStart,
    currentEnd,
    previousStart,
    previousEnd,
    periodMs,
  };
};

const buildReportSearchQuery = (search) => {
  const normalizedSearch = String(search || '').trim();
  if (!normalizedSearch) {
    return {};
  }

  const regex = new RegExp(normalizedSearch, 'i');
  return {
    $or: [
      { transactionId: regex },
      { eventId: regex },
      { authId: regex },
      { orderType: regex },
      { status: regex },
      { razorpayOrderId: regex },
      { razorpayPaymentId: regex },
      { razorpayRefundId: regex },
    ],
  };
};

const toReportOrder = (order) => ({
  ...order,
  createdAtMs: new Date(order.createdAt).getTime(),
  signedAmount: normalizeLedgerAmount(order),
});

const sumSignedRevenue = (orders) => orders.reduce((sum, order) => sum + Number(order.signedAmount || 0), 0);

const buildReportBuckets = ({ start, end, steps = 5, prefix = 'WEEK' }) => {
  const startMs = start.getTime();
  const endMs = end.getTime();
  const periodMs = Math.max(1, endMs - startMs + 1);
  const bucketSizeMs = Math.max(1, Math.ceil(periodMs / steps));

  return Array.from({ length: steps }, (_, index) => {
    const bucketStartMs = startMs + (index * bucketSizeMs);
    const bucketEndMs = Math.min(endMs, bucketStartMs + bucketSizeMs - 1);

    return {
      label: `${prefix} ${index + 1}`,
      startMs: bucketStartMs,
      endMs: bucketEndMs,
    };
  });
};

const sumBucketRevenue = (orders, startMs, endMs) => {
  let total = 0;
  orders.forEach((order) => {
    const ts = Number(order.createdAtMs || 0);
    if (ts >= startMs && ts <= endMs) {
      total += Number(order.signedAmount || 0);
    }
  });
  return total;
};

const buildAdminLedgerCsv = (rows) => {
  const header = [
    'transactionId',
    'eventId',
    'authId',
    'type',
    'status',
    'amount',
    'currency',
    'vendor',
    'createdAt',
    'paidAt',
    'refundedAt',
    'razorpayOrderId',
    'razorpayPaymentId',
    'razorpayRefundId',
  ];

  const escapeCsv = (value) => {
    if (value == null) return '';
    const stringValue = String(value);
    if (/[,"\n]/.test(stringValue)) {
      return `"${stringValue.replace(/"/g, '""')}"`;
    }
    return stringValue;
  };

  const lines = rows.map((row) => [
    row.transactionId,
    row.eventId,
    row.authId,
    row.type,
    row.status,
    row.amount,
    row.currency,
    row.vendor,
    row.createdAt ? new Date(row.createdAt).toISOString() : '',
    row.paidAt ? new Date(row.paidAt).toISOString() : '',
    row.refundedAt ? new Date(row.refundedAt).toISOString() : '',
    row.razorpayOrderId,
    row.razorpayPaymentId,
    row.razorpayRefundId,
  ].map(escapeCsv).join(','));

  return `${header.join(',')}\n${lines.join('\n')}`;
};

const buildServiceHeaders = (user = {}) => ({
  'x-auth-id': user.authId || '',
  'x-user-id': user.userId || '',
  'x-user-email': user.email || '',
  'x-user-username': user.username || '',
  'x-user-role': user.role || 'ADMIN',
});

const formatInrFromPaise = (paise) => ((Number(paise || 0) || 0) / 100).toFixed(2);

const formatReceiptAmount = (paise) => `INR ${formatInrFromPaise(paise)}`;

const formatReceiptDateTime = (value) => {
  const date = value ? new Date(value) : null;
  if (!date || Number.isNaN(date.getTime())) {
    return 'N/A';
  }

  return date.toISOString().replace('T', ' ').slice(0, 19);
};

const formatReceiptStatus = (status) => {
  const normalized = String(status || '').trim().toUpperCase();
  if (normalized === 'PAID') return 'COMPLETED';
  if (!normalized) return 'UNKNOWN';
  return normalized;
};

const escapeCsvCell = (value) => {
  if (value == null) return '';
  const stringValue = String(value);
  if (/[,"\n]/.test(stringValue)) {
    return `"${stringValue.replace(/"/g, '""')}"`;
  }
  return stringValue;
};

const csvRow = (values) => values.map(escapeCsvCell).join(',');

const createTransactionHistory = (order) => {
  const history = [];

  if (order?.createdAt) {
    history.push({
      status: 'CREATED',
      date: new Date(order.createdAt).toISOString(),
      desc: 'Transaction was created.',
    });
  }

  if (order?.paidAt) {
    history.push({
      status: 'PAID',
      date: new Date(order.paidAt).toISOString(),
      desc: 'Payment was successfully verified.',
    });
  }

  if (order?.status === 'FAILED') {
    history.push({
      status: 'FAILED',
      date: order?.updatedAt ? new Date(order.updatedAt).toISOString() : new Date().toISOString(),
      desc: 'Payment verification failed.',
    });
  }

  if (order?.refundedAt) {
    history.push({
      status: 'REFUNDED',
      date: new Date(order.refundedAt).toISOString(),
      desc: 'Refund completed.',
    });
  }

  if (order?.status === 'REFUND_FAILED') {
    history.push({
      status: 'REFUND_FAILED',
      date: order?.updatedAt ? new Date(order.updatedAt).toISOString() : new Date().toISOString(),
      desc: 'Refund attempt failed.',
    });
  }

  return history.sort((a, b) => new Date(a.date).getTime() - new Date(b.date).getTime());
};

const getUserProfileByAuthId = async (authId, user) => {
  const normalizedAuthId = String(authId || '').trim();
  if (!normalizedAuthId) {
    return null;
  }

  const userServiceUrl = process.env.USER_SERVICE_URL || 'http://user-service:8082';

  try {
    const response = await axios.get(`${userServiceUrl}/auth/${encodeURIComponent(normalizedAuthId)}`, {
      timeout: 10000,
      headers: buildServiceHeaders(user),
    });

    const profile = response.data?.data;
    if (!profile) {
      return null;
    }

    return {
      authId: profile.authId || normalizedAuthId,
      name: profile.name || profile.fullName || profile.username || null,
      email: profile.email || null,
      phone: profile.phone || profile.mobile || null,
    };
  } catch (_error) {
    return {
      authId: normalizedAuthId,
      name: null,
      email: null,
      phone: null,
    };
  }
};

const getEventDetailsByEventId = async (eventId, user) => {
  const normalizedEventId = String(eventId || '').trim();
  if (!normalizedEventId) {
    return null;
  }

  const eventServiceUrl = process.env.EVENT_SERVICE_URL || 'http://event-service:8086';
  const headers = buildServiceHeaders(user);

  try {
    const planningResponse = await axios.get(`${eventServiceUrl}/planning/${encodeURIComponent(normalizedEventId)}`, {
      timeout: 10000,
      headers,
    });

    const planning = planningResponse.data?.data;
    if (planning) {
      return {
        eventId: planning.eventId || normalizedEventId,
        title: planning.eventTitle || 'Untitled Event',
        category: planning.category || null,
        type: planning.eventType || null,
        date: planning.eventDate || planning?.schedule?.startAt || null,
        source: 'PLANNING',
      };
    }
  } catch (_error) {
    // Fallback to promote lookup.
  }

  try {
    const promoteResponse = await axios.get(`${eventServiceUrl}/promote/${encodeURIComponent(normalizedEventId)}`, {
      timeout: 10000,
      headers,
    });

    const promote = promoteResponse.data?.data;
    if (promote) {
      return {
        eventId: promote.eventId || normalizedEventId,
        title: promote.eventTitle || 'Untitled Event',
        category: promote.eventCategory || null,
        type: promote.customCategory || null,
        date: promote?.schedule?.startAt || null,
        source: 'PROMOTE',
      };
    }
  } catch (_error) {
    // Keep graceful fallback.
  }

  return {
    eventId: normalizedEventId,
    title: 'Event details unavailable',
    category: null,
    type: null,
    date: null,
    source: 'UNKNOWN',
  };
};

const buildReportsCsv = (reportPayload) => {
  const summary = reportPayload?.summary || {};
  const overview = Array.isArray(reportPayload?.revenueOverview) ? reportPayload.revenueOverview : [];
  const categories = Array.isArray(reportPayload?.categoryBreakdown) ? reportPayload.categoryBreakdown : [];
  const recents = Array.isArray(reportPayload?.recentEntries) ? reportPayload.recentEntries : [];

  const lines = [];

  lines.push(csvRow(['section', 'metric', 'value']));
  lines.push(csvRow(['SUMMARY', 'generatedAt', new Date().toISOString()]));
  lines.push(csvRow(['SUMMARY', 'totalRevenueInr', formatInrFromPaise(summary.totalRevenue)]));
  lines.push(csvRow(['SUMMARY', 'averageTransactionInr', formatInrFromPaise(summary.averageTransaction)]));
  lines.push(csvRow(['SUMMARY', 'growthRatePercent', Number(summary.growthRatePercent || 0).toFixed(2)]));
  lines.push(csvRow(['SUMMARY', 'totalTransactions', Number(summary.totalTransactions || 0)]));

  lines.push('');
  lines.push(csvRow(['REVENUE_OVERVIEW', 'bucket', 'currentRevenueInr', 'previousRevenueInr']));
  overview.forEach((row) => {
    lines.push(csvRow([
      'REVENUE_OVERVIEW',
      row.label,
      formatInrFromPaise(row.currentRevenue),
      formatInrFromPaise(row.previousRevenue),
    ]));
  });

  lines.push('');
  lines.push(csvRow(['CATEGORY_BREAKDOWN', 'category', 'amountInr', 'percentage']));
  categories.forEach((row) => {
    lines.push(csvRow([
      'CATEGORY_BREAKDOWN',
      String(row.category || 'OTHER').replace(/_/g, ' '),
      formatInrFromPaise(row.amount),
      Number(row.percentage || 0).toFixed(2),
    ]));
  });

  lines.push('');
  lines.push(csvRow(['RECENT_ENTRIES', 'transactionId', 'category', 'date', 'amountInr', 'status', 'eventId']));
  recents.forEach((row) => {
    const isoDate = row.date ? new Date(row.date).toISOString() : '';
    lines.push(csvRow([
      'RECENT_ENTRIES',
      row.transactionId,
      row.category,
      isoDate,
      formatInrFromPaise(row.amount),
      row.status,
      row.eventId,
    ]));
  });

  return lines.join('\n');
};

const buildAdminReceiptPdfBuffer = (details = {}) => new Promise((resolve, reject) => {
  const doc = new PDFDocument({ margin: 36, size: 'A4' });
  const chunks = [];

  doc.on('data', (chunk) => chunks.push(chunk));
  doc.on('end', () => resolve(Buffer.concat(chunks)));
  doc.on('error', reject);

  const transaction = details.transaction || {};
  const event = transaction.event || {};
  const payer = transaction.payer || {};
  const vendor = transaction.vendor || {};

  const status = formatReceiptStatus(transaction.status);
  const statusBg = status === 'COMPLETED' ? '#dcfce7' : '#e2e8f0';
  const statusColor = status === 'COMPLETED' ? '#166534' : '#334155';
  const pageWidth = doc.page.width;
  const contentWidth = pageWidth - (doc.page.margins.left * 2);

  doc.save();
  doc.rect(0, 0, pageWidth, 130).fill('#0b2d49');
  doc.restore();

  doc.fillColor('#ffffff').font('Helvetica-Bold').fontSize(10).text('OKKAZO', 36, 28);
  doc.font('Helvetica-Bold').fontSize(24).text('PAYMENT RECEIPT', 36, 44);
  doc.font('Helvetica').fontSize(10).fillColor('#cbd5e1').text(`Generated ${formatReceiptDateTime(new Date())}`, 36, 92);

  const badgeWidth = 108;
  doc.roundedRect(pageWidth - 36 - badgeWidth, 44, badgeWidth, 24, 12).fill(statusBg);
  doc.fillColor(statusColor)
    .font('Helvetica-Bold')
    .fontSize(10)
    .text(status, pageWidth - 36 - badgeWidth + 12, 52, { width: badgeWidth - 24, align: 'center' });

  const cardTop = 146;
  doc.roundedRect(36, cardTop, contentWidth, 150, 12).fillAndStroke('#ffffff', '#e2e8f0');

  doc.fillColor('#94a3b8').font('Helvetica-Bold').fontSize(9).text('TRANSACTION ID', 52, cardTop + 22);
  doc.fillColor('#0f172a').font('Helvetica-Bold').fontSize(13).text(transaction.transactionId || 'N/A', 52, cardTop + 36, { width: 270 });

  doc.fillColor('#94a3b8').font('Helvetica-Bold').fontSize(9).text('EVENT', 52, cardTop + 68);
  doc.fillColor('#0f172a').font('Helvetica').fontSize(11).text(event.title || 'Event details unavailable', 52, cardTop + 82, { width: 270 });

  doc.fillColor('#94a3b8').font('Helvetica-Bold').fontSize(9).text('EVENT ID', 52, cardTop + 110);
  doc.fillColor('#334155').font('Helvetica').fontSize(10).text(transaction.eventId || event.eventId || 'N/A', 52, cardTop + 124, { width: 270 });

  doc.fillColor('#94a3b8').font('Helvetica-Bold').fontSize(9).text('RECORDED ON', 340, cardTop + 22);
  doc.fillColor('#0f172a').font('Helvetica').fontSize(11).text(formatReceiptDateTime(transaction.createdAt), 340, cardTop + 36, { width: 190 });

  doc.fillColor('#94a3b8').font('Helvetica-Bold').fontSize(9).text('PAYMENT METHOD', 340, cardTop + 68);
  doc.fillColor('#0f172a').font('Helvetica').fontSize(11).text(`Razorpay (${transaction.type || 'PAYMENT'})`, 340, cardTop + 82, { width: 190 });

  doc.fillColor('#94a3b8').font('Helvetica-Bold').fontSize(9).text('SYSTEM REF', 340, cardTop + 110);
  doc.fillColor('#334155').font('Helvetica').fontSize(10).text(transaction?.references?.systemReference || 'N/A', 340, cardTop + 124, { width: 190 });

  const amountTop = 314;
  const leftBoxWidth = Math.floor((contentWidth - 20) / 2);

  doc.roundedRect(36, amountTop, leftBoxWidth, 122, 12).fillAndStroke('#ecfeff', '#bae6fd');
  doc.fillColor('#0369a1').font('Helvetica-Bold').fontSize(9).text('TOTAL SETTLEMENT', 52, amountTop + 18);
  doc.fillColor('#0f172a').font('Helvetica-Bold').fontSize(28).text(formatReceiptAmount(transaction.settlementAmount), 52, amountTop + 40, { width: leftBoxWidth - 32 });
  doc.fillColor('#0f766e').font('Helvetica').fontSize(10).text('Settled after platform fee and GST', 52, amountTop + 92, { width: leftBoxWidth - 32 });

  const rightBoxX = 36 + leftBoxWidth + 20;
  doc.roundedRect(rightBoxX, amountTop, leftBoxWidth, 122, 12).fillAndStroke('#ffffff', '#e2e8f0');
  doc.fillColor('#94a3b8').font('Helvetica-Bold').fontSize(9).text('FEE BREAKDOWN', rightBoxX + 16, amountTop + 18);

  const feeRows = [
    ['Subtotal', formatReceiptAmount(transaction.grossAmount)],
    ['Platform Fee (2.5%)', formatReceiptAmount(transaction.platformFee)],
    ['Tax (GST 18%)', formatReceiptAmount(transaction.tax)],
  ];

  let rowY = amountTop + 40;
  feeRows.forEach(([label, value]) => {
    doc.fillColor('#334155').font('Helvetica').fontSize(10).text(label, rightBoxX + 16, rowY, { width: 170 });
    doc.fillColor('#0f172a').font('Helvetica-Bold').fontSize(10).text(value, rightBoxX + leftBoxWidth - 86, rowY, { width: 70, align: 'right' });
    rowY += 24;
  });

  const profileTop = 456;
  doc.roundedRect(36, profileTop, contentWidth, 180, 12).fillAndStroke('#ffffff', '#e2e8f0');

  doc.fillColor('#0f172a').font('Helvetica-Bold').fontSize(12).text('Parties', 52, profileTop + 18);
  doc.moveTo(pageWidth / 2, profileTop + 48).lineTo(pageWidth / 2, profileTop + 154).lineWidth(1).strokeColor('#e2e8f0').stroke();

  doc.fillColor('#94a3b8').font('Helvetica-Bold').fontSize(9).text('RECEIVER', 52, profileTop + 48);
  doc.fillColor('#0f172a').font('Helvetica-Bold').fontSize(11).text(vendor.name || 'System Auto', 52, profileTop + 64, { width: 210 });
  doc.fillColor('#334155').font('Helvetica').fontSize(10).text(`Vendor ID: ${vendor.id || 'N/A'}`, 52, profileTop + 88, { width: 210 });
  doc.fillColor('#334155').font('Helvetica').fontSize(10).text(`Email: ${vendor.email || 'N/A'}`, 52, profileTop + 106, { width: 210 });

  doc.fillColor('#94a3b8').font('Helvetica-Bold').fontSize(9).text('PAYER', pageWidth / 2 + 16, profileTop + 48);
  doc.fillColor('#0f172a').font('Helvetica-Bold').fontSize(11).text(payer.name || payer.authId || 'Unknown', pageWidth / 2 + 16, profileTop + 64, { width: 210 });
  doc.fillColor('#334155').font('Helvetica').fontSize(10).text(`Auth ID: ${payer.authId || transaction.authId || 'N/A'}`, pageWidth / 2 + 16, profileTop + 88, { width: 210 });
  doc.fillColor('#334155').font('Helvetica').fontSize(10).text(`Email: ${payer.email || 'N/A'}`, pageWidth / 2 + 16, profileTop + 106, { width: 210 });

  doc.fillColor('#64748b').font('Helvetica').fontSize(9).text('This receipt is system-generated and valid without signature.', 36, 750, {
    width: contentWidth,
    align: 'center',
  });

  doc.end();
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
  const isTicketSale = orderType === 'TICKET SALE';

  let upstreamRecord;
  if (!isTicketSale) {
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
  }

  // For most order types, it's fine to reuse the latest CREATED order.
  // For deposit + vendor confirmation orders, the amount is derived from upstream state,
  // so only reuse an existing order if the amount still matches.
  if (
    orderType !== 'PLANNING EVENT DEPOSIT FEE'
    && orderType !== 'PLANNING EVENT VENDOR CONFIRMATION FEE'
    && orderType !== 'TICKET SALE'
  ) {
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
  const orderNotes = value.notes || {};

  if (isTicketSale) {
    amountInInr = Number(value.amount);
    if (!Number.isFinite(amountInInr) || amountInInr <= 0) {
      throw createApiError(400, 'Amount is required for TICKET SALE orders');
    }
  } else if (orderType === 'PLANNING EVENT DEPOSIT FEE') {
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

    const clientGrandTotalMinPaise = Number(quote?.clientGrandTotal?.minPaise ?? 0);
    if (!Number.isFinite(clientGrandTotalMinPaise) || clientGrandTotalMinPaise <= 0) {
      throw createApiError(409, 'Cannot compute vendor confirmation amount without a locked quote');
    }

    const depositPaidAmountPaise = Number(upstreamRecord.depositPaidAmountPaise ?? 0);
    if (!Number.isFinite(depositPaidAmountPaise) || depositPaidAmountPaise < 0) {
      throw createApiError(409, 'Deposit amount is invalid for this event');
    }

    // Business rule: vendor confirmation = 25% of (client total min - already paid deposit).
    const payableBasePaise = Math.max(0, clientGrandTotalMinPaise - depositPaidAmountPaise);
    const confirmationDuePaise = Math.max(0, Math.round((payableBasePaise * 25) / 100));
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
              computedFrom: 'planning-quote.clientGrandTotal.minPaise',
              vendorConfirmationPercent: 25,
              depositPaidAmountPaise: upstreamRecord.depositPaidAmountPaise,
            }
          : {}),
        ...orderNotes,
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
    notes: {
      ...orderNotes,
      ...(order.notes || {}),
    },
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
    ticketId: paymentOrder?.notes?.ticketId || null,
    ticketLink: buildTicketLink(paymentOrder?.notes?.ticketId),
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
      ticketId: paymentOrder?.notes?.ticketId || null,
      ticketLink: buildTicketLink(paymentOrder?.notes?.ticketId),
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
      notes: paymentOrder.notes || {},
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
    ticketId: paymentOrder?.notes?.ticketId || null,
    ticketLink: buildTicketLink(paymentOrder?.notes?.ticketId),
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
        notes: paymentOrder.notes || {},
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
    paymentOrderId: order._id.toString(),
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
    notes: order.notes || {},
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

const getAdminLedger = async (rawQuery = {}, options = {}) => {
  const maxLimit = Number(options.maxLimit || 100);
  const schema = adminLedgerQuerySchema.keys({
    limit: Joi.number().integer().min(1).max(maxLimit).default(10),
  });

  const { value, error } = schema.validate(rawQuery, { stripUnknown: true });
  if (error) {
    throw createApiError(400, error.details[0].message);
  }

  const query = buildAdminLedgerMongoQuery(value);
  const page = Number(value.page || 1);
  const limit = Number(value.limit || 10);
  const skip = (page - 1) * limit;
  const sortField = value.sortBy || 'createdAt';
  const sortDirection = value.sortDir === 'asc' ? 1 : -1;

  const [orders, total, summaryAgg, distinctAuthIds] = await Promise.all([
    PaymentOrder.find(query)
      .sort({ [sortField]: sortDirection })
      .skip(skip)
      .limit(limit)
      .lean(),
    PaymentOrder.countDocuments(query),
    PaymentOrder.aggregate([
      { $match: query },
      {
        $group: {
          _id: null,
          totalLedgerVolume: {
            $sum: {
              $cond: [
                { $or: [{ $eq: ['$orderType', 'REFUND'] }, { $eq: ['$status', 'REFUNDED'] }] },
                { $multiply: [{ $ifNull: ['$refundedAmount', '$amount'] }, -1] },
                { $ifNull: ['$amount', 0] },
              ],
            },
          },
          pendingSettlements: {
            $sum: {
              $cond: [
                { $eq: ['$status', 'CREATED'] },
                { $ifNull: ['$amount', 0] },
                0,
              ],
            },
          },
        },
      },
    ]),
    PaymentOrder.distinct('authId', query),
  ]);

  const summary = summaryAgg[0] || { totalLedgerVolume: 0, pendingSettlements: 0 };
  const totalPages = Math.max(1, Math.ceil(total / limit));

  return {
    summary: {
      totalLedgerVolume: Number(summary.totalLedgerVolume || 0),
      pendingSettlements: Number(summary.pendingSettlements || 0),
      activeVendors: Array.isArray(distinctAuthIds) ? distinctAuthIds.length : 0,
      currency: 'INR',
    },
    pagination: {
      page,
      limit,
      total,
      totalPages,
      hasNext: page < totalPages,
      hasPrev: page > 1,
    },
    transactions: orders.map(mapOrderToAdminLedgerRow),
  };
};

const getAdminLedgerTransactionById = async (transactionId) => {
  const normalizedTransactionId = String(transactionId || '').trim();
  if (!normalizedTransactionId) {
    throw createApiError(400, 'Transaction ID is required');
  }

  const order = await PaymentOrder.findOne({ transactionId: normalizedTransactionId }).lean();
  if (!order) {
    throw createApiError(404, 'Transaction not found');
  }

  return mapOrderToAdminLedgerRow(order);
};

const getAdminTransactionsByEventIdDetailed = async (eventId, user) => {
  const normalizedEventId = String(eventId || '').trim();
  if (!normalizedEventId) {
    throw createApiError(400, 'Event ID is required');
  }

  const orders = await PaymentOrder.find({ eventId: normalizedEventId })
    .sort({ createdAt: -1 })
    .lean();

  if (orders.length === 0) {
    return {
      event: await getEventDetailsByEventId(normalizedEventId, user),
      transactions: [],
    };
  }

  const uniqueAuthIds = [...new Set(orders.map((row) => String(row.authId || '').trim()).filter(Boolean))];
  const profiles = await Promise.all(uniqueAuthIds.map((authId) => getUserProfileByAuthId(authId, user)));
  const profileByAuthId = new Map(profiles.filter(Boolean).map((profile) => [String(profile.authId), profile]));

  return {
    event: await getEventDetailsByEventId(normalizedEventId, user),
    transactions: orders.map((row) => {
      const profile = profileByAuthId.get(String(row.authId || '').trim()) || null;
      return {
        transactionId: row.transactionId,
        eventId: row.eventId,
        authId: row.authId,
        payer: profile,
        type: row.orderType,
        status: row.status,
        amount: normalizeLedgerAmount(row),
        currency: row.currency || 'INR',
        createdAt: row.createdAt,
        paidAt: row.paidAt,
        refundedAt: row.refundedAt,
      };
    }),
  };
};

const getAdminTransactionDetails = async (transactionId, user) => {
  const normalizedTransactionId = String(transactionId || '').trim();
  if (!normalizedTransactionId) {
    throw createApiError(400, 'Transaction ID is required');
  }

  const order = await PaymentOrder.findOne({ transactionId: normalizedTransactionId }).lean();
  if (!order) {
    throw createApiError(404, 'Transaction not found');
  }

  const [eventInfo, payer] = await Promise.all([
    getEventDetailsByEventId(order.eventId, user),
    getUserProfileByAuthId(order.authId, user),
  ]);

  const grossAmount = Math.abs(Number(order.refundedAmount ?? order.amount ?? 0) || 0);
  const platformFee = Math.round(grossAmount * 0.025);
  const tax = Math.round(platformFee * 0.18);
  const signedAmount = normalizeLedgerAmount(order);
  const settlementAmount = signedAmount >= 0 ? grossAmount - platformFee - tax : -grossAmount;

  const eventTransactionsResult = await getAdminTransactionsByEventIdDetailed(order.eventId, user);

  return {
    transaction: {
      transactionId: order.transactionId,
      eventId: order.eventId,
      authId: order.authId,
      type: order.orderType,
      status: order.status,
      amount: signedAmount,
      grossAmount,
      currency: order.currency || 'INR',
      platformFee,
      tax,
      settlementAmount,
      createdAt: order.createdAt,
      paidAt: order.paidAt,
      refundedAt: order.refundedAt,
      vendor: {
        name: order?.notes?.vendorName || order?.notes?.businessName || order?.notes?.vendor || 'System Auto',
        id: order?.notes?.vendorId || order?.notes?.vendorAuthId || null,
        email: order?.notes?.vendorEmail || null,
      },
      payer: payer || {
        authId: order.authId,
        name: null,
        email: null,
        phone: null,
      },
      event: eventInfo,
      references: {
        systemReference: order.razorpayPaymentId || order.razorpayOrderId || order.transactionId,
        razorpayOrderId: order.razorpayOrderId || null,
        razorpayPaymentId: order.razorpayPaymentId || null,
        razorpayRefundId: order.razorpayRefundId || null,
      },
      history: createTransactionHistory(order),
    },
    eventTransactions: eventTransactionsResult.transactions,
  };
};

const exportAdminLedgerCsv = async (rawQuery = {}) => {
  const exportFilters = {
    ...rawQuery,
    page: 1,
    limit: 5000,
  };

  const { transactions } = await getAdminLedger(exportFilters, { maxLimit: 5000 });
  return buildAdminLedgerCsv(transactions);
};

const getAdminReports = async (rawQuery = {}) => {
  const { value, error } = adminReportQuerySchema.validate(rawQuery, { stripUnknown: true });
  if (error) {
    throw createApiError(400, error.details[0].message);
  }

  const {
    currentStart,
    currentEnd,
    previousStart,
    previousEnd,
  } = buildReportDateRange(value);

  const searchQuery = buildReportSearchQuery(value.search);

  const currentQuery = {
    ...searchQuery,
    status: { $in: REPORT_REVENUE_STATUSES },
    createdAt: { $gte: currentStart, $lte: currentEnd },
  };

  const previousQuery = {
    ...searchQuery,
    status: { $in: REPORT_REVENUE_STATUSES },
    createdAt: { $gte: previousStart, $lte: previousEnd },
  };

  const [currentOrdersRaw, previousOrdersRaw] = await Promise.all([
    PaymentOrder.find(currentQuery).sort({ createdAt: -1 }).lean(),
    PaymentOrder.find(previousQuery).lean(),
  ]);

  const currentOrders = currentOrdersRaw.map(toReportOrder);
  const previousOrders = previousOrdersRaw.map(toReportOrder);

  const totalRevenue = sumSignedRevenue(currentOrders);
  const previousRevenue = sumSignedRevenue(previousOrders);
  const totalTransactions = currentOrders.length;
  const averageTransaction = totalTransactions > 0 ? Math.round(totalRevenue / totalTransactions) : 0;

  const growthRatePercent = previousRevenue === 0
    ? (totalRevenue === 0 ? 0 : 100)
    : Number((((totalRevenue - previousRevenue) / Math.abs(previousRevenue)) * 100).toFixed(2));

  const categoryTotals = new Map();
  currentOrders.forEach((order) => {
    const key = String(order.orderType || 'OTHER').trim() || 'OTHER';
    const existing = Number(categoryTotals.get(key) || 0);
    categoryTotals.set(key, existing + Number(order.signedAmount || 0));
  });

  const totalCategoryMagnitude = Array.from(categoryTotals.values())
    .reduce((sum, amount) => sum + Math.abs(Number(amount || 0)), 0);

  const categoryBreakdown = Array.from(categoryTotals.entries())
    .map(([category, amount]) => ({
      category,
      amount: Number(amount || 0),
      percentage: totalCategoryMagnitude > 0
        ? Number(((Math.abs(Number(amount || 0)) / totalCategoryMagnitude) * 100).toFixed(2))
        : 0,
    }))
    .sort((a, b) => Math.abs(b.amount) - Math.abs(a.amount));

  const currentBuckets = buildReportBuckets({ start: currentStart, end: currentEnd, steps: 5, prefix: 'WEEK' });
  const previousBuckets = buildReportBuckets({ start: previousStart, end: previousEnd, steps: 5, prefix: 'WEEK' });

  const revenueOverview = currentBuckets.map((bucket, index) => {
    const previousBucket = previousBuckets[index];
    return {
      label: bucket.label,
      currentRevenue: sumBucketRevenue(currentOrders, bucket.startMs, bucket.endMs),
      previousRevenue: sumBucketRevenue(previousOrders, previousBucket.startMs, previousBucket.endMs),
    };
  });

  const recentEntries = currentOrders
    .slice(0, Number(value.recentLimit || 4))
    .map((order) => ({
      transactionId: order.transactionId,
      category: order.orderType,
      date: order.createdAt,
      amount: Number(order.signedAmount || 0),
      status: order.status,
      eventId: order.eventId,
    }));

  return {
    range: {
      key: value.range,
      current: {
        from: currentStart.toISOString(),
        to: currentEnd.toISOString(),
      },
      previous: {
        from: previousStart.toISOString(),
        to: previousEnd.toISOString(),
      },
    },
    summary: {
      totalRevenue,
      averageTransaction,
      growthRatePercent,
      totalTransactions,
      currency: 'INR',
    },
    revenueOverview,
    categoryBreakdown,
    recentEntries,
  };
};

const exportAdminReportsCsv = async (rawQuery = {}) => {
  const reportData = await getAdminReports(rawQuery);
  return buildReportsCsv(reportData);
};

const exportAdminTransactionReceiptPdf = async (transactionId, user) => {
  const details = await getAdminTransactionDetails(transactionId, user);
  return buildAdminReceiptPdfBuffer(details);
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
  getAdminLedger,
  getAdminLedgerTransactionById,
  getAdminTransactionsByEventIdDetailed,
  getAdminTransactionDetails,
  exportAdminLedgerCsv,
  getAdminReports,
  exportAdminReportsCsv,
  exportAdminTransactionReceiptPdf,
};
