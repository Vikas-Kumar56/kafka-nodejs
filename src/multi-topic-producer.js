const { Kafka } = require('kafkajs');

const kafkaClient = new Kafka({
  clientId: 'multi-topic-producer',
  brokers: ['localhost:9092'],
});

const startProducer = async () => {
  const producer = kafkaClient.producer();
  await producer.connect();

  const messages = [
    {
      topic: 'topic-a',
      messages: [
        {
          key: 'key',
          value: 'hello world frok topic a',
        },
      ],
    },
    {
      topic: 'topic-b',
      messages: [
        {
          key: 'key-b',
          value: 'hello world frok topic b',
        },
      ],
    },
  ];

  await producer.sendBatch({ topicMessages: messages });
};

const startConsumer = async () => {
  const consumer = kafkaClient.consumer({ groupId: 'simple-group' });

  await consumer.connect();
  await consumer.subscribe({ topic: 'topic-a', fromBeginning: true });
  await consumer.subscribe({ topic: 'topic-b', fromBeginning: true });

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
