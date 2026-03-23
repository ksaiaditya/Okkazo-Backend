const express = require('express');
const commissionController = require('../controllers/commissionController');
const { isAdmin } = require('../middleware/authorization');

const router = express.Router();

// GET /admin/commission — admin only
router.get('/admin/commission', isAdmin, commissionController.getCommission);

// PUT /admin/commission — admin only
router.put('/admin/commission', isAdmin, commissionController.updateCommission);

module.exports = router;
