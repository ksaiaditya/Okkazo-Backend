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
  getPaymentSettings,
  updatePaymentSettings,
};
