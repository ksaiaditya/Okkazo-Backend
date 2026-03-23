const express = require('express');
const configController = require('../controllers/configController');
const { authorizeRoles, isAdmin } = require('../middleware/authorization');

const router = express.Router();

// GET /config/fees — all authenticated roles
router.get(
  '/config/fees',
  authorizeRoles(['USER', 'VENDOR', 'ADMIN', 'MANAGER']),
  configController.getFees
);

// PATCH /config/fees — admin only
router.patch(
  '/config/fees',
  isAdmin,
  configController.updateFees
);

// GET /config/promotions — all authenticated roles
router.get(
  '/config/promotions',
  authorizeRoles(['USER', 'VENDOR', 'ADMIN', 'MANAGER']),
  configController.getPromotions
);

// PATCH /config/promotions — admin only
router.patch(
  '/config/promotions',
  isAdmin,
  configController.updatePromotions
);

// GET /config/manager-autoassign — admin only
router.get(
  '/config/manager-autoassign',
  isAdmin,
  configController.getManagerAutoAssign
);

// PATCH /config/manager-autoassign — admin only
router.patch(
  '/config/manager-autoassign',
  isAdmin,
  configController.updateManagerAutoAssign
);

module.exports = router;
