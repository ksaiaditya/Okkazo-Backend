require('dotenv').config();
require('express-async-errors');
const express = require('express');
const helmet = require('helmet');
const compression = require('compression');
const morgan = require('morgan');

const connectDB = require('./config/database');
const eurekaClient = require('./config/eureka');
const notificationEventConsumer = require('./kafka/notificationEventConsumer');
const { startReminderScheduler, stopReminderScheduler } = require('./jobs/reminderScheduler');
const logger = require('./utils/logger');
const { errorHandler, notFound } = require('./middleware/errorHandler');

const notificationRoutes = require('./routes/notificationRoutes');

const app = express();

app.use(helmet());
app.use(compression());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(morgan('combined', { stream: { write: (message) => logger.http(message.trim()) } }));

app.get('/health', (req, res) => {
  res.status(200).json({
    success: true,
    message: 'Notification service is running',
    timestamp: new Date().toISOString(),
  });
});

app.use('/', notificationRoutes);

app.use(notFound);
app.use(errorHandler);

const PORT = process.env.PORT || 8088;
const NODE_ENV = process.env.NODE_ENV || 'development';

const startServer = async () => {
  try {
    await connectDB();
    logger.info('MongoDB connection established');

    await notificationEventConsumer.initialize();
    await notificationEventConsumer.startConsuming();
    logger.info('Notification Kafka consumer initialized and started');

    if (process.env.EUREKA_REGISTER_WITH_EUREKA !== 'false') {
      eurekaClient.start();
      logger.info('Eureka client started');
    }

    startReminderScheduler();

    const server = app.listen(PORT, () => {
      logger.info(`Server running in ${NODE_ENV} mode on port ${PORT}`);
      logger.info(`Service: ${process.env.SERVICE_NAME || 'notification-service'}`);
    });

    const gracefulShutdown = async (signal) => {
      logger.info(`${signal} received. Starting graceful shutdown...`);

      server.close(async () => {
        logger.info('HTTP server closed');

        try {
          await notificationEventConsumer.shutdown();
          logger.info('Kafka consumer stopped');

          eurekaClient.stop();
          logger.info('Eureka client stopped');

          stopReminderScheduler();

          const mongoose = require('mongoose');
          await mongoose.connection.close();
          logger.info('MongoDB connection closed');

          logger.info('Graceful shutdown completed');
          process.exit(0);
        } catch (error) {
          logger.error('Error during shutdown:', error);
          process.exit(1);
        }
      });

      setTimeout(() => {
        logger.error('Forced shutdown after timeout');
        process.exit(1);
      }, 30000);
    };

    process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
    process.on('SIGINT', () => gracefulShutdown('SIGINT'));

    process.on('uncaughtException', (error) => {
      logger.error('Uncaught Exception:', error);
      gracefulShutdown('UNCAUGHT_EXCEPTION');
    });

    process.on('unhandledRejection', (reason, promise) => {
      logger.error('Unhandled Rejection at:', promise, 'reason:', reason);
      gracefulShutdown('UNHANDLED_REJECTION');
    });
  } catch (error) {
    logger.error('Failed to start server:', error);
    process.exit(1);
  }
};

startServer();

module.exports = app;
