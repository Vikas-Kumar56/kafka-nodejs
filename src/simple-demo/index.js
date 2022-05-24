const { Kafka } = require('kafkajs');

// kafka client
const kafka = new Kafka({
  clientId: 'simple-producer-consumer-application',
  brokers: ['localhost:9092'],
});

const producerRun = async () => {
  // kafka prodcuer
  const producer = kafka.producer();
  await producer.connect();

  await producer.send({
    topic: 'simple-topic',
    messages: [
      {
        value: 'My Fist Nodejs Message',
      },
    ],
  });

  await producer.send({
    topic: 'simple-topic',
    messages: [
      {
        value: 'second message',
      },
    ],
  });

  await producer.disconnect();
};

const startConsumer = async () => {
  const consumer = kafka.consumer({ groupId: 'simple-group' });

  await consumer.connect();
  await consumer.subscribe({ topic: 'simple-topic', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        value: `consumer reading ${message.value.toString()}`,
      });
    },
  });
};

producerRun().then(() => {
  startConsumer();
});
