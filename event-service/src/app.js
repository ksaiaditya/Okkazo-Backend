require('dotenv').config();
require('express-async-errors');
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const compression = require('compression');
const morgan = require('morgan');

const connectDB = require('./config/database');
const eurekaClient = require('./config/eureka');
const kafkaConfig = require('./config/kafka');
const eventConsumer = require('./kafka/eventConsumer');
const logger = require('./utils/logger');
const { errorHandler, notFound } = require('./middleware/errorHandler');
const { extractUser } = require('./middleware/extractUser');

// Import routes
const planningRoutes = require('./routes/planningRoutes');
const promoteRoutes  = require('./routes/promoteRoutes');
const vendorSelectionRoutes = require('./routes/vendorSelectionRoutes');
const configRoutes = require('./routes/configRoutes');

// Initialize Express app
const app = express();

// Middleware
app.use(helmet()); // Security headers
// app.use(cors()); // CORS handled by API Gateway
app.use(compression()); // Compress responses
app.use(express.json()); // Parse JSON bodies
app.use(express.urlencoded({ extended: true })); // Parse URL-encoded bodies
app.use(morgan('combined', { stream: { write: (message) => logger.http(message.trim()) } }));

// Health check route (before authentication)
app.get('/health', (req, res) => {
  res.status(200).json({
    success: true,
    message: 'Event service is running',
    timestamp: new Date().toISOString(),
  });
});

// Extract user from gateway headers for all routes
app.use(extractUser);

// API Routes
app.use('/', planningRoutes);
app.use('/', promoteRoutes);
app.use('/', vendorSelectionRoutes);
app.use('/', configRoutes);

// 404 handler
app.use(notFound);

// Error handler (must be last)
app.use(errorHandler);

// Server configuration
const PORT = process.env.PORT || 8086;
const NODE_ENV = process.env.NODE_ENV || 'development';

// Start server
const startServer = async () => {
  try {
    // Connect to MongoDB
    await connectDB();
    logger.info('MongoDB connection established');

    // Initialize Kafka producer
    await kafkaConfig.createProducer();
    logger.info('Kafka producer initialized');

    // Initialize and start Kafka consumer
    await eventConsumer.initialize();
    await eventConsumer.startConsuming();
    logger.info('Kafka consumer initialized and started');

    // Start Eureka client
    if (process.env.EUREKA_REGISTER_WITH_EUREKA !== 'false') {
      eurekaClient.start();
      logger.info('Eureka client started');
    }

    // Start Express server
    const server = app.listen(PORT, () => {
      logger.info(`Server running in ${NODE_ENV} mode on port ${PORT}`);
      logger.info(`Service: ${process.env.SERVICE_NAME || 'event-service'}`);
    });

    // Graceful shutdown
    const gracefulShutdown = async (signal) => {
      logger.info(`${signal} received. Starting graceful shutdown...`);

      server.close(async () => {
        logger.info('HTTP server closed');

        try {
          // Stop Kafka consumer
          await eventConsumer.shutdown();
          logger.info('Kafka consumer stopped');

          // Disconnect Kafka
          await kafkaConfig.disconnect();
          logger.info('Kafka disconnected');

          // Stop Eureka client
          eurekaClient.stop();
          logger.info('Eureka client stopped');

          // Close MongoDB connection
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

      // Force shutdown after 30 seconds
      setTimeout(() => {
        logger.error('Forced shutdown after timeout');
        process.exit(1);
      }, 30000);
    };

    // Handle shutdown signals
    process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
    process.on('SIGINT', () => gracefulShutdown('SIGINT'));

    // Handle uncaught exceptions
    process.on('uncaughtException', (error) => {
      logger.error('Uncaught Exception:', error);
      gracefulShutdown('UNCAUGHT_EXCEPTION');
    });

    // Handle unhandled promise rejections
    process.on('unhandledRejection', (reason, promise) => {
      logger.error('Unhandled Rejection at:', promise, 'reason:', reason);
      gracefulShutdown('UNHANDLED_REJECTION');
    });
  } catch (error) {
    logger.error('Failed to start server:', error);
    process.exit(1);
  }
};

// Start the server
startServer();

module.exports = app;
