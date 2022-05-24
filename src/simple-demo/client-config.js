const { Kafka, logLevel } = require('kafkajs');

// kafka client
const kafka = new Kafka({
  clientId: 'simple-producer-consumer-application',
  brokers: ['localhost:9092'],
  connectionTimeout: 3000, // time in ms defauult value is 1000
  requestTimeout: 25000, // defauult 30000 ms
  retry: {
    initialRetryTime: 100,
    retries: 8,
  },
  logLevel: logLevel.INFO,
});

kafka.logger().info('any message');
