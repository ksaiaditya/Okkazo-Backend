const express = require('express');
const router = express.Router();

const notificationController = require('../controllers/notificationController');
const { extractUser } = require('../middleware/extractUser');
const { authorizeRoles } = require('../middleware/authorization');

router.get('/health', notificationController.healthCheck);

router.use(extractUser);

router.get('/', notificationController.getNotifications);
router.get('/unread-count', notificationController.getUnreadCount);
router.patch('/mark-all-read', notificationController.markAllRead);
router.patch('/:notificationId/read', notificationController.markNotificationRead);

router.post(
  '/system/broadcast',
  authorizeRoles(['ADMIN']),
  notificationController.broadcastSystemNotification
);

module.exports = router;
