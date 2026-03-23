const commissionService = require('../services/commissionService');
const logger = require('../utils/logger');

// GET /admin/commission (Admin only)
const getCommission = async (req, res) => {
  try {
    const cfg = await commissionService.getCommissionConfig();

    return res.status(200).json({
      success: true,
      data: {
        rates: cfg?.rates || commissionService.getDefaultRates(),
        updatedAt: cfg?.updatedAt || null,
        updatedByAuthId: cfg?.updatedByAuthId || null,
      },
    });
  } catch (error) {
    logger.error('Error in getCommission:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message || 'Failed to fetch commission config',
    });
  }
};

// PUT /admin/commission (Admin only)
const updateCommission = async (req, res) => {
  try {
    const { rates } = req.body || {};

    const updated = await commissionService.updateCommissionRates({
      rates,
      updatedByAuthId: req.user?.authId,
    });

    return res.status(200).json({
      success: true,
      message: 'Commission rates updated',
      data: {
        rates: updated?.rates || commissionService.getDefaultRates(),
        updatedAt: updated?.updatedAt || null,
        updatedByAuthId: updated?.updatedByAuthId || null,
      },
    });
  } catch (error) {
    logger.error('Error in updateCommission:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message || 'Failed to update commission config',
    });
  }
};

module.exports = {
  getCommission,
  updateCommission,
};
