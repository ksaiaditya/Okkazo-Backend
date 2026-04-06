const express = require('express');
const ticketMarketplaceController = require('../controllers/ticketMarketplaceController');
const { authorizeRoles } = require('../middleware/authorization');

const router = express.Router();

// GET /tickets/marketplace/events
// Public-event feed for user dashboard (planning public + promote)
router.get(
  '/tickets/marketplace/events',
  authorizeRoles(['USER', 'VENDOR', 'ADMIN', 'MANAGER']),
  ticketMarketplaceController.getTicketMarketplaceEvents
);

router.get(
  '/tickets/my/interests',
  authorizeRoles(['USER', 'VENDOR', 'ADMIN', 'MANAGER']),
  ticketMarketplaceController.getMyTicketInterests
);

router.post(
  '/tickets/purchase/prepare',
  authorizeRoles(['USER', 'VENDOR', 'ADMIN', 'MANAGER']),
  ticketMarketplaceController.prepareTicketPurchase
);

router.post(
  '/tickets/purchase/confirm-free',
  authorizeRoles(['USER', 'VENDOR', 'ADMIN', 'MANAGER']),
  ticketMarketplaceController.confirmFreeTicketPurchase
);

router.get(
  '/tickets/my',
  authorizeRoles(['USER', 'VENDOR', 'ADMIN', 'MANAGER']),
  ticketMarketplaceController.getMyTickets
);

router.get(
  '/tickets/my/:ticketId',
  authorizeRoles(['USER', 'VENDOR', 'ADMIN', 'MANAGER']),
  ticketMarketplaceController.getMyTicketByTicketId
);

router.get(
  '/tickets/events/:eventId/guests',
  authorizeRoles(['ADMIN', 'MANAGER']),
  ticketMarketplaceController.getEventTicketGuests
);

router.post(
  '/tickets/verify-qr',
  authorizeRoles(['VENDOR', 'ADMIN', 'MANAGER']),
  ticketMarketplaceController.verifyTicketQr
);

module.exports = router;
