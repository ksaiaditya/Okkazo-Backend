const kafkaConfig = require('../config/kafka');
const planningService = require('../services/planningService');
const promoteService  = require('../services/promoteService');
const { publishEvent } = require('./eventProducer');
const logger = require('../utils/logger');

let consumer = null;
const authTopic = process.env.KAFKA_AUTH_TOPIC || 'auth_events';
const paymentTopic = process.env.KAFKA_PAYMENT_TOPIC || 'payment_events';

const initialize = async () => {
  const maxRetries = 5;
  const retryDelay = 3000;

  for (let attempt = 1; attempt <= maxRetries; attempt += 1) {
    try {
      consumer = await kafkaConfig.createConsumer();
      await new Promise((resolve) => setTimeout(resolve, 1500));
      await consumer.subscribe({ topic: authTopic, fromBeginning: false });
      await consumer.subscribe({ topic: paymentTopic, fromBeginning: false });

      logger.info(`Subscribed to Kafka topics: ${authTopic}, ${paymentTopic}`);
      return;
    } catch (error) {
      logger.error(`Error initializing Kafka consumer (attempt ${attempt}/${maxRetries}):`, error.message);
      if (attempt < maxRetries) {
        await new Promise((resolve) => setTimeout(resolve, retryDelay));
      } else {
        throw error;
      }
    }
  }
};

const startConsuming = async () => {
  if (!consumer) {
    return;
  }

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const payload = JSON.parse(message.value.toString());
        const eventType = payload.type || payload.eventType;

        if (eventType === 'PAYMENT_SUCCESS' && payload.eventId) {
          const orderType = payload.orderType || '';

          if (orderType === 'PROMOTE EVENT') {
            // ── Promote payment confirmed ────────────────────────────────────
            try {
              const promote = await promoteService.markPromotePaid(payload.eventId);
              await publishEvent('PROMOTE_PAYMENT_CONFIRMED', {
                eventId: promote.eventId,
                promoteId: promote.promoteId,
                authId: promote.authId,
                platformFeePaid: promote.platformFeePaid,
                eventStatus: promote.eventStatus,
                paymentOrderId: payload.paymentOrderId,
                razorpayOrderId: payload.razorpayOrderId,
                razorpayPaymentId: payload.razorpayPaymentId,
                paidAt: payload.paidAt,
              });
            } catch (err) {
              logger.error('Failed to process PROMOTE payment success:', err.message);
            }
          } else if (orderType === 'PLANNING EVENT DEPOSIT FEE') {
            // ── Planning deposit confirmed ─────────────────────────────────
            const planning = await planningService.markPlanningDepositPaid(payload.eventId, {
              amountPaise: payload.amount,
              currency: payload.currency,
              paidAt: payload.paidAt,
            });

            try {
              await publishEvent('PLANNING_DEPOSIT_PAYMENT_CONFIRMED', {
                eventId: planning.eventId,
                authId: planning.authId,
                platformFeePaid: Boolean(planning.platformFeePaid) || Boolean(planning.isPaid),
                depositPaid: Boolean(planning.depositPaid),
                fullPaymentPaid: Boolean(planning.fullPaymentPaid),
                paymentOrderId: payload.paymentOrderId,
                razorpayOrderId: payload.razorpayOrderId,
                razorpayPaymentId: payload.razorpayPaymentId,
                paidAt: payload.paidAt,
              });
            } catch (publishError) {
              logger.error('Failed to publish PLANNING_DEPOSIT_PAYMENT_CONFIRMED event:', publishError.message);
            }
          } else if (orderType === 'PLANNING EVENT VENDOR CONFIRMATION FEE') {
            // ── Planning vendor confirmation confirmed ────────────────────
            const planning = await planningService.markPlanningVendorConfirmationPaid(payload.eventId, {
              amountPaise: payload.amount,
              currency: payload.currency,
              paidAt: payload.paidAt,
            });

            try {
              await publishEvent('PLANNING_VENDOR_CONFIRMATION_PAYMENT_CONFIRMED', {
                eventId: planning.eventId,
                authId: planning.authId,
                status: planning.status,
                platformFeePaid: Boolean(planning.platformFeePaid) || Boolean(planning.isPaid),
                depositPaid: Boolean(planning.depositPaid),
                vendorConfirmationPaid: Boolean(planning.vendorConfirmationPaid),
                paymentOrderId: payload.paymentOrderId,
                razorpayOrderId: payload.razorpayOrderId,
                razorpayPaymentId: payload.razorpayPaymentId,
                paidAt: payload.paidAt,
              });
            } catch (publishError) {
              logger.error('Failed to publish PLANNING_VENDOR_CONFIRMATION_PAYMENT_CONFIRMED event:', publishError.message);
            }
          } else {
            // ── Planning payment confirmed (default: platform fee) ──────────
            const planning = await planningService.markPlanningPaid(payload.eventId);

            try {
              await publishEvent('PLANNING_PAYMENT_CONFIRMED', {
                eventId: planning.eventId,
                authId: planning.authId,
                platformFeePaid: Boolean(planning.platformFeePaid) || Boolean(planning.isPaid),
                depositPaid: Boolean(planning.depositPaid),
                fullPaymentPaid: Boolean(planning.fullPaymentPaid),
                paymentOrderId: payload.paymentOrderId,
                razorpayOrderId: payload.razorpayOrderId,
                razorpayPaymentId: payload.razorpayPaymentId,
                paidAt: payload.paidAt,
              });
            } catch (publishError) {
              logger.error('Failed to publish PLANNING_PAYMENT_CONFIRMED event:', publishError.message);
            }
          }
        }

        logger.info('Received Kafka message in event-service', {
          eventType,
          key: message.key ? message.key.toString() : null,
        });
      } catch (error) {
        logger.error('Error processing Kafka message in event-service:', error.message);
      }
    },
  });

  logger.info('Kafka consumer started successfully');
};

const shutdown = async () => {
  try {
    if (consumer) {
      await consumer.disconnect();
      logger.info('Kafka consumer shut down gracefully');
    }
  } catch (error) {
    logger.error('Error shutting down Kafka consumer:', error.message);
  }
};

module.exports = {
  initialize,
  startConsuming,
  shutdown,
};
