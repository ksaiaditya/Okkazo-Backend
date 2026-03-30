const express = require('express');
const orderController = require('../controllers/orderController');
const { authorizeRoles } = require('../middleware/authorization');

const router = express.Router();

router.post('/orders/create', authorizeRoles(['USER', 'VENDOR', 'ADMIN', 'MANAGER']), orderController.createOrder);
router.post('/orders/verify', authorizeRoles(['USER', 'VENDOR', 'ADMIN', 'MANAGER']), orderController.verifyPayment);
router.post('/orders/refund', authorizeRoles(['USER', 'VENDOR', 'ADMIN', 'MANAGER']), orderController.refundPayment);
router.get('/orders/settings', authorizeRoles(['USER', 'VENDOR', 'ADMIN', 'MANAGER']), orderController.getPaymentSettings);
router.patch('/orders/settings', authorizeRoles(['ADMIN']), orderController.updatePaymentSettings);
router.get('/orders/admin/ledger', authorizeRoles(['ADMIN', 'MANAGER']), orderController.getAdminLedger);
router.get('/orders/admin/ledger/export/csv', authorizeRoles(['ADMIN', 'MANAGER']), orderController.exportAdminLedgerCsv);
router.get('/orders/admin/ledger/:transactionId/receipt/pdf', authorizeRoles(['ADMIN', 'MANAGER']), orderController.exportAdminTransactionReceiptPdf);
router.get('/orders/admin/ledger/:transactionId/details', authorizeRoles(['ADMIN', 'MANAGER']), orderController.getAdminTransactionDetails);
router.get('/orders/admin/ledger/:transactionId', authorizeRoles(['ADMIN', 'MANAGER']), orderController.getAdminLedgerTransactionById);
router.get('/orders/admin/reports', authorizeRoles(['ADMIN', 'MANAGER']), orderController.getAdminReports);
router.get('/orders/admin/reports/export/csv', authorizeRoles(['ADMIN', 'MANAGER']), orderController.exportAdminReportsCsv);
router.get('/orders/admin/event/:eventId/transactions', authorizeRoles(['ADMIN', 'MANAGER']), orderController.getAdminTransactionsByEventIdDetailed);
router.get('/orders/admin/:eventId', authorizeRoles(['ADMIN', 'MANAGER']), orderController.getOrdersByEventIdForAdmin);
router.get('/orders/:eventId', authorizeRoles(['USER', 'VENDOR', 'ADMIN', 'MANAGER']), orderController.getOrderByEventId);

module.exports = router;
