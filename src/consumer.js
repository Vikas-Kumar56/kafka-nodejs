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

const processMessage = async ({
  batch,
  resolveOffset,
  heartbeat,
  commitOffsetsIfNecessary,
  uncommittedOffsets,
  isRunning,
  isState,
}) => {
  for (let message of batch.messages) {
    console.log({
      topic: batch.topic,
      message: {
        offset: message.offset,
        key: message.key.toString(),
        value: message.value.toString(),
      },
    });

    resolveOffset(message.offset);
    await heartbeat();
  }
};

const startConsumer = async () => {
  const consumer = kafkaClient.consumer({ groupId: 'simple-group' });

  await consumer.connect();

  // listen to multi-topic
  await consumer.subscribe({ topic: 'topic-a', fromBeginning: true });
  await consumer.subscribe({ topic: 'topic-b', fromBeginning: true });

  await consumer.run({
    eachBatchAutoResolve: true,
    partitionsConsumedConcurrently: 3,
    eachBatch: processMessage,
  });
};

startProducer().then(() => {
  startConsumer();
});
