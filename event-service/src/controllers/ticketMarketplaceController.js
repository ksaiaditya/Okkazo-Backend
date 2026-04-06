const ticketMarketplaceService = require('../services/ticketMarketplaceService');
const { publishEvent } = require('../kafka/eventProducer');
const logger = require('../utils/logger');

const getTicketMarketplaceEvents = async (req, res) => {
  try {
    const { page = 1, limit = 20 } = req.query;

    const result = await ticketMarketplaceService.getTicketMarketplaceEvents({
      page,
      limit,
    });

    return res.status(200).json({
      success: true,
      data: result,
    });
  } catch (error) {
    logger.error('Error in getTicketMarketplaceEvents:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message || 'Failed to fetch ticket marketplace events',
    });
  }
};

const getMyTicketInterests = async (req, res) => {
  try {
    const userAuthId = req.user?.authId;

    const result = await ticketMarketplaceService.getMyTicketInterests({
      userAuthId,
    });

    return res.status(200).json({
      success: true,
      data: result,
    });
  } catch (error) {
    logger.error('Error in getMyTicketInterests:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message || 'Failed to fetch ticket interests',
    });
  }
};

const prepareTicketPurchase = async (req, res) => {
  try {
    const result = await ticketMarketplaceService.prepareTicketPurchase({
      eventId: req.body?.eventId,
      userAuthId: req.user?.authId,
      userId: req.user?.userId,
      tiers: req.body?.tiers,
      selectedDay: req.body?.selectedDay,
    });

    return res.status(201).json({
      success: true,
      message: 'Ticket purchase initialized',
      data: result,
    });
  } catch (error) {
    logger.error('Error in prepareTicketPurchase:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message || 'Failed to initialize ticket purchase',
    });
  }
};

const confirmFreeTicketPurchase = async (req, res) => {
  try {
    const result = await ticketMarketplaceService.confirmFreeTicketPurchase({
      eventId: req.body?.eventId,
      ticketId: req.body?.ticketId,
      userAuthId: req.user?.authId,
    });

    return res.status(200).json({
      success: true,
      message: 'Free ticket confirmed',
      data: result,
    });
  } catch (error) {
    logger.error('Error in confirmFreeTicketPurchase:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message || 'Failed to confirm free ticket',
    });
  }
};

const getMyTicketByTicketId = async (req, res) => {
  try {
    const result = await ticketMarketplaceService.getMyTicketByTicketId({
      ticketId: req.params.ticketId,
      userAuthId: req.user?.authId,
    });

    return res.status(200).json({
      success: true,
      data: result,
    });
  } catch (error) {
    logger.error('Error in getMyTicketByTicketId:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message || 'Failed to fetch ticket',
    });
  }
};

const getMyTickets = async (req, res) => {
  try {
    const result = await ticketMarketplaceService.getMyTickets({
      userAuthId: req.user?.authId,
    });

    return res.status(200).json({
      success: true,
      data: result,
    });
  } catch (error) {
    logger.error('Error in getMyTickets:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message || 'Failed to fetch my tickets',
    });
  }
};

const getEventTicketGuests = async (req, res) => {
  try {
    const result = await ticketMarketplaceService.getEventTicketGuests({
      eventId: req.params?.eventId,
      page: req.query?.page,
      limit: req.query?.limit,
      query: req.query?.query,
    });

    return res.status(200).json({
      success: true,
      data: result,
    });
  } catch (error) {
    logger.error('Error in getEventTicketGuests:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message || 'Failed to fetch event guests',
    });
  }
};

const verifyTicketQr = async (req, res) => {
  try {
    const result = await ticketMarketplaceService.verifyTicketQr({
      token: req.body?.token,
      scannedByAuthId: req.user?.authId,
      scannedByRole: req.user?.role,
    });

    if (!result?.alreadyScanned) {
      try {
        await publishEvent('TICKET_VERIFIED', {
          eventId: result?.eventId || null,
          authId: result?.userAuthId || null,
          ticketId: result?.ticketId || null,
          eventTitle: result?.eventTitle || null,
          verifiedAt: result?.verifiedAt || new Date().toISOString(),
          selectedDay: result?.selectedDay || null,
          scannedByAuthId: req.user?.authId || null,
          scannedByRole: req.user?.role || null,
        });
      } catch (publishError) {
        logger.warn('Failed to publish TICKET_VERIFIED event', {
          ticketId: result?.ticketId || null,
          eventId: result?.eventId || null,
          message: publishError?.message || String(publishError),
        });
      }
    }

    return res.status(200).json({
      success: true,
      message: result?.alreadyScanned ? "It's already scanned" : 'Ticket verified successfully',
      data: result,
    });
  } catch (error) {
    logger.error('Error in verifyTicketQr:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message || 'Failed to verify ticket QR',
    });
  }
};

module.exports = {
  getTicketMarketplaceEvents,
  getMyTicketInterests,
  prepareTicketPurchase,
  confirmFreeTicketPurchase,
  getMyTickets,
  getMyTicketByTicketId,
  getEventTicketGuests,
  verifyTicketQr,
};
