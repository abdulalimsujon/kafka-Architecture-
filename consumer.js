

const { kafka } = require('./client');

const group = process.argv[2] || 'default-group'; // fallback

async function init() {
  const consumer = kafka.consumer({ groupId: group }); 
  await consumer.connect();

  await consumer.subscribe({
    topic: 'rider-updates',
    fromBeginning: true
  });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log(`group->${group} : topic->[${topic}][PART-> ${partition}] ${message.value.toString()}`);
    }
  });
}

init();

