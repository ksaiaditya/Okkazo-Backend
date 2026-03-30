const paymentService = require('../services/paymentService');
const paymentSettingsService = require('../services/paymentSettingsService');

const createOrder = async (req, res) => {
  const result = await paymentService.createOrder(req.body, req.user);

  res.status(201).json({
    success: true,
    message: 'Payment order created successfully',
    data: result,
  });
};

const verifyPayment = async (req, res) => {
  const result = await paymentService.verifyPayment(req.body, req.user);

  res.status(200).json({
    success: true,
    message: 'Payment verified successfully',
    data: result,
  });
};

const refundPayment = async (req, res) => {
  const result = await paymentService.refundPayment(req.body, req.user);

  res.status(200).json({
    success: true,
    message: 'Refund processed successfully',
    data: result,
  });
};

const webhook = async (req, res) => {
  const signature = req.headers['x-razorpay-signature'];
  const result = await paymentService.handleWebhook(req.body, signature);

  res.status(200).json({
    success: true,
    message: 'Webhook processed',
    data: result,
  });
};

const getOrderByEventId = async (req, res) => {
  const result = await paymentService.getOrderByEventId(req.params.eventId, req.user);

  res.status(200).json({
    success: true,
    data: result,
  });
};

const getOrdersByEventIdForAdmin = async (req, res) => {
  const result = await paymentService.getOrdersByEventIdForAdmin(req.params.eventId);

  res.status(200).json({
    success: true,
    data: result,
  });
};

const getAdminLedger = async (req, res) => {
  const result = await paymentService.getAdminLedger(req.query);

  res.status(200).json({
    success: true,
    data: result,
  });
};

const getAdminLedgerTransactionById = async (req, res) => {
  const result = await paymentService.getAdminLedgerTransactionById(req.params.transactionId);

  res.status(200).json({
    success: true,
    data: result,
  });
};

const getAdminTransactionsByEventIdDetailed = async (req, res) => {
  const result = await paymentService.getAdminTransactionsByEventIdDetailed(req.params.eventId, req.user);

  res.status(200).json({
    success: true,
    data: result,
  });
};

const getAdminTransactionDetails = async (req, res) => {
  const result = await paymentService.getAdminTransactionDetails(req.params.transactionId, req.user);

  res.status(200).json({
    success: true,
    data: result,
  });
};

const exportAdminTransactionReceiptPdf = async (req, res) => {
  const normalizedTransactionId = String(req.params.transactionId || '').trim();
  const pdfBuffer = await paymentService.exportAdminTransactionReceiptPdf(normalizedTransactionId, req.user);

  res.setHeader('Content-Type', 'application/pdf');
  res.setHeader('Content-Disposition', `attachment; filename="receipt-${normalizedTransactionId || 'transaction'}.pdf"`);
  res.status(200).send(pdfBuffer);
};

const exportAdminLedgerCsv = async (req, res) => {
  const csv = await paymentService.exportAdminLedgerCsv(req.query);

  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="admin-ledger-${new Date().toISOString().slice(0, 10)}.csv"`);
  res.status(200).send(csv);
};

const getAdminReports = async (req, res) => {
  const result = await paymentService.getAdminReports(req.query);

  res.status(200).json({
    success: true,
    data: result,
  });
};

const exportAdminReportsCsv = async (req, res) => {
  const csv = await paymentService.exportAdminReportsCsv(req.query);

  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="admin-report-${new Date().toISOString().slice(0, 10)}.csv"`);
  res.status(200).send(csv);
};

const getPaymentSettings = async (req, res) => {
  const result = await paymentSettingsService.getSettings();

  res.status(200).json({
    success: true,
    data: result,
  });
};

const updatePaymentSettings = async (req, res) => {
  const result = await paymentSettingsService.updateSettings(req.body);

  res.status(200).json({
    success: true,
    message: 'Payment settings updated successfully',
    data: result,
  });
};

module.exports = {
  createOrder,
  verifyPayment,
  refundPayment,
  webhook,
  getOrderByEventId,
  getOrdersByEventIdForAdmin,
  getAdminLedger,
  getAdminLedgerTransactionById,
  getAdminTransactionsByEventIdDetailed,
  getAdminTransactionDetails,
  exportAdminTransactionReceiptPdf,
  exportAdminLedgerCsv,
  getAdminReports,
  exportAdminReportsCsv,
  getPaymentSettings,
  updatePaymentSettings,
};
