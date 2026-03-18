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

module.exports = router;
