const promoteConfigService = require('../services/promoteConfigService');
const logger = require('../utils/logger');

// GET /config/fees
const getFees = async (req, res) => {
  try {
    const cfg = await promoteConfigService.getFees();
    return res.status(200).json({
      success: true,
      data: cfg,
    });
  } catch (error) {
    logger.error('Error in getFees:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

// PATCH /config/fees (Admin only)
const updateFees = async (req, res) => {
  try {
    const { platformFee, serviceChargePercent } = req.body || {};

    const updated = await promoteConfigService.updateFees({
      platformFee,
      serviceChargePercent,
      updatedByAuthId: req.user?.authId,
    });

    return res.status(200).json({
      success: true,
      message: 'Fees updated successfully',
      data: updated,
    });
  } catch (error) {
    logger.error('Error in updateFees:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message,
    });
  }
};

module.exports = {
  getFees,
  updateFees,
};
