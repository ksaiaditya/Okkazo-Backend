const { processDueReminderSchedules } = require('../services/reminderService');
const logger = require('../utils/logger');

let intervalRef = null;

const startReminderScheduler = () => {
  const pollMs = Math.max(15000, Number(process.env.NOTIFICATION_SCHEDULER_POLL_MS || 60000));

  if (intervalRef) {
    clearInterval(intervalRef);
  }

  intervalRef = setInterval(async () => {
    try {
      const result = await processDueReminderSchedules({
        batchSize: Number(process.env.NOTIFICATION_SCHEDULER_BATCH_SIZE || 150),
      });

      if (result.processed > 0) {
        logger.info('Reminder scheduler processed due reminders', result);
      }
    } catch (error) {
      logger.error('Reminder scheduler failed', { message: error?.message || String(error) });
    }
  }, pollMs);

  logger.info(`Reminder scheduler started (poll=${pollMs}ms)`);
};

const stopReminderScheduler = () => {
  if (intervalRef) {
    clearInterval(intervalRef);
    intervalRef = null;
    logger.info('Reminder scheduler stopped');
  }
};

module.exports = {
  startReminderScheduler,
  stopReminderScheduler,
};
