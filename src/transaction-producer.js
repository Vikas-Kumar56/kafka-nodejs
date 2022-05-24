const { Kafka } = require('kafkajs');

const kafkaClient = new Kafka({
  clientId: 'transaction-producer',
  brokers: ['localhost:9092'],
});

const startProducer = async () => {
  const producer = kafkaClient.producer({
    idempotent: true,
    maxInFlightRequests: 1,
    transactionalId: 'someId',
  });

  await producer.connect();

  const transaction = await producer.transaction();

  try {
    await transaction.send({
      topic: 'topic-transaction',
      messages: [
        {
          key: 'transaction-1',
          value: 'Transaction prodcuer demo',
        },
      ],
    });

    // throw new Error('some error occured');

    await transaction.commit();
    await producer.disconnect();
  } catch (error) {
    console.log('error occured', error);
    await transaction.abort();
    await producer.disconnect();
  }
};

const startConsumer = async () => {
  const consumer = kafkaClient.consumer({ groupId: 'simple-group' });

  await consumer.connect();
  await consumer.subscribe({ topic: 'topic-transaction', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        key: message.key.toString(),
        value: message.value.toString(),
        headers: message.headers.toString(),
        topic: topic,
        partition,
      });
    },
  });
};

startProducer().then(() => {
  startConsumer();
});
