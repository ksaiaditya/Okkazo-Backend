const kafkaConfig = require('../config/kafka');
const userService = require('../services/userService');
const logger = require('../utils/logger');

// Module state
let consumer = null;
const topic = process.env.KAFKA_TOPIC || 'auth_events';

const initialize = async () => {
  const maxRetries = 5;
  const retryDelay = 3000; // 3 seconds

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      consumer = await kafkaConfig.createConsumer();
      
      // Add a small delay before subscribing to allow metadata to sync
      await new Promise(resolve => setTimeout(resolve, 2000));
      
      await consumer.subscribe({
        topic: topic,
        fromBeginning: false,
      });

      logger.info(`Subscribed to Kafka topic: ${topic}`);
      return; // Success, exit the function
    } catch (error) {
      logger.error(`Error initializing Kafka consumer (attempt ${attempt}/${maxRetries}):`, error.message);
      
      if (attempt < maxRetries) {
        logger.info(`Retrying in ${retryDelay / 1000} seconds...`);
        await new Promise(resolve => setTimeout(resolve, retryDelay));
      } else {
        logger.error('Max retries reached. Kafka consumer initialization failed.');
        throw error;
      }
    }
  }
};

const startConsuming = async () => {
  try {
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const key = message.key ? message.key.toString() : null;
          const rawValue = message.value.toString();
          const value = JSON.parse(rawValue);

          // Handle both 'type' and 'eventType' fields for backward compatibility
          const eventType = value.type || value.eventType;

          logger.info(`Received Kafka message: ${eventType}`, {
            topic,
            partition,
            offset: message.offset,
            key,
          });

          // Normalize to 'type' field
          if (value.eventType && !value.type) {
            value.type = value.eventType;
          }

          await handleEvent(value);
        } catch (error) {
          logger.error('Error processing Kafka message:', error);
          // Don't throw to prevent consumer from stopping
        }
      },
    });

    logger.info('Kafka consumer started successfully');
  } catch (error) {
    logger.error('Error starting Kafka consumer:', error);
    throw error;
  }
};

const handleEvent = async (event) => {
  try {
    switch (event.type) {
      case 'USER_REGISTERED':
        await handleUserRegistered(event);
        break;

      case 'PASSWORD_RESET_REQUESTED':
        logger.info('Password reset requested event received', {
          authId: event.authId,
        });
        // Handle password reset event if needed
        break;
      
      case 'USER_LOGIN':
        await handleUserLogin(event);
        break;

      case 'EMAIL_VERIFICATION_RESEND':
        logger.info('Email verification resend event received', {
          authId: event.authId,
        });
        // Handle email verification resend if needed
        break;

      case 'USER_ROLE_CHANGED':
        await handleUserRoleChanged(event);
        break;

      case 'MANAGER_ACCOUNT_CREATED':
        await handleManagerAccountCreated(event);
        break;

      default:
        logger.warn(`Unknown event type: ${event.type}`);
    }
  } catch (error) {
    logger.error(`Error handling event ${event.type}:`, error);
    throw error;
  }
};

const handleUserRegistered = async (event) => {
  try {
    const { authId, email, username, verificationToken } = event;

    // Validate required fields
    if (!authId || !email || !username) {
      logger.error('USER_REGISTERED event missing required fields', { event });
      throw new Error('Invalid event data: authId, email, and username are required');
    }

    // Validate email format
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(email)) {
      logger.error('USER_REGISTERED event has invalid email format', { email });
      throw new Error('Invalid email format');
    }

    logger.info('Processing USER_REGISTERED event', { authId, email, username });

    // Create user profile
    const userData = {
      authId: authId,
      name: username,
      email: email,
      role: 'USER',
      profileIsComplete: false,
      memberSince: new Date(),
      isActive: true,
    };

    const user = await userService.createUser(userData);
    
    logger.info('User profile created successfully', {
      authId,
      userId: user.id,
      email: user.email,
    });

    return user;
  } catch (error) {
    if (error.code === 11000) {
      logger.warn('User already exists', { authId: event.authId });
      return null;
    }
    logger.error('Error handling USER_REGISTERED event:', error);
    throw error;
  }
};

const handleUserLogin = async (event) => {
  try {
    const { authId, email, Date: loginDate } = event;

    // Validate required fields
    if (!authId) {
      logger.error('USER_LOGIN event missing authId', { event });
      throw new Error('Invalid event data: authId is required');
    }

    logger.info('Processing USER_LOGIN event', { 
      authId, 
      email,
      loginDate 
    });

    // Update last login timestamp
    await userService.updateLastLogin(authId);
    
    logger.info('Last login updated successfully', {
      authId,
      email,
    });

  } catch (error) {
    if (error.message.includes('not found')) {
      logger.warn('User not found for login event', { authId: event.authId });
      return null;
    }
    logger.error('Error handling USER_LOGIN event:', error);
    throw error;
  }
};

const handleUserRoleChanged = async (event) => {
  try {
    const { authId, email, previousRole, newRole, changedAt } = event;

    // Validate required fields
    if (!authId || !newRole) {
      logger.error('USER_ROLE_CHANGED event missing required fields', { event });
      throw new Error('Invalid event data: authId and newRole are required');
    }

    logger.info('Processing USER_ROLE_CHANGED event', { 
      authId, 
      email,
      previousRole,
      newRole,
      changedAt
    });

    // Update user role in user service
    const user = await userService.updateUserRole(authId, newRole);
    
    logger.info('User role updated successfully', {
      authId,
      email: user.email,
      previousRole,
      newRole,
    });

    return user;
  } catch (error) {
    if (error.message.includes('not found')) {
      logger.warn('User not found for role change event', { authId: event.authId });
      return null;
    }
    logger.error('Error handling USER_ROLE_CHANGED event:', error);
    throw error;
  }
};

const handleManagerAccountCreated = async (event) => {
  try {
    const { authId, email, name, department, assignedRole } = event;

    if (!authId || !email || !name) {
      logger.error('MANAGER_ACCOUNT_CREATED event missing required fields', { event });
      throw new Error('Invalid event data: authId, email, and name are required');
    }

    logger.info('Processing MANAGER_ACCOUNT_CREATED event', { authId, email, name, department });

    const userData = {
      authId: authId,
      name: name,
      email: email,
      role: 'MANAGER',
      department: department || null,
      assignedRole: assignedRole || 'MANAGER',
      profileIsComplete: false,
      memberSince: new Date(),
      isActive: true,
    };

    const user = await userService.createUser(userData);

    logger.info('Manager profile created successfully', {
      authId,
      userId: user.id,
      email: user.email,
      department,
    });

    return user;
  } catch (error) {
    if (error.code === 11000) {
      logger.warn('Manager user already exists', { authId: event.authId });
      return null;
    }
    logger.error('Error handling MANAGER_ACCOUNT_CREATED event:', error);
    throw error;
  }
};

const shutdown = async () => {
  try {
    if (consumer) {
      await consumer.disconnect();
      logger.info('Kafka consumer shut down gracefully');
    }
  } catch (error) {
    logger.error('Error shutting down Kafka consumer:', error);
  }
};

module.exports = {
  initialize,
  startConsuming,
  shutdown,
};
