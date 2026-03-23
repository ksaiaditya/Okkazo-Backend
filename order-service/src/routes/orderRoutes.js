const express = require('express');
const orderController = require('../controllers/orderController');
const { authorizeRoles } = require('../middleware/authorization');

const router = express.Router();

router.post('/orders/create', authorizeRoles(['USER', 'VENDOR', 'ADMIN', 'MANAGER']), orderController.createOrder);
router.post('/orders/verify', authorizeRoles(['USER', 'VENDOR', 'ADMIN', 'MANAGER']), orderController.verifyPayment);
router.post('/orders/refund', authorizeRoles(['USER', 'VENDOR', 'ADMIN', 'MANAGER']), orderController.refundPayment);
router.get('/orders/settings', authorizeRoles(['USER', 'VENDOR', 'ADMIN', 'MANAGER']), orderController.getPaymentSettings);
router.patch('/orders/settings', authorizeRoles(['ADMIN']), orderController.updatePaymentSettings);
router.get('/orders/admin/:eventId', authorizeRoles(['ADMIN', 'MANAGER']), orderController.getOrdersByEventIdForAdmin);
router.get('/orders/:eventId', authorizeRoles(['USER', 'VENDOR', 'ADMIN', 'MANAGER']), orderController.getOrderByEventId);

module.exports = router;
