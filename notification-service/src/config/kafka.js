const { Kafka } = require('kafkajs');
const logger = require('../utils/logger');

const brokers = process.env.KAFKA_BROKERS || process.env.KAFKA_BROKER || 'localhost:9092';
const brokerList = brokers.split(',');

const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID || 'notification-service',
  brokers: brokerList,
  retry: {
    initialRetryTime: 300,
    retries: 10,
  },
});

let consumer = null;

const createConsumer = async (groupId) => {
  try {
    consumer = kafka.consumer({
      groupId: groupId || process.env.KAFKA_GROUP_ID || 'notification-service-group',
      sessionTimeout: 30000,
      heartbeatInterval: 3000,
    });

    await consumer.connect();
    logger.info('Kafka consumer connected successfully');
    return consumer;
  } catch (error) {
    logger.error('Error creating Kafka consumer:', error);
    throw error;
  }
};

const disconnect = async () => {
  try {
    if (consumer) {
      await consumer.disconnect();
      logger.info('Kafka consumer disconnected');
    }
  } catch (error) {
    logger.error('Error disconnecting Kafka:', error);
  }
};

module.exports = {
  createConsumer,
  disconnect,
};
