const express = require('express');
const Kafka = require('node-rdkafka');

const app = express();
app.use(express.json());

let messages = [];
let consumer; // Định nghĩa biến consumer toàn cục

// Kafka consumer configuration function
function createConsumer(config, onData) {
  consumer = new Kafka.KafkaConsumer(config, { 'auto.offset.reset': 'earliest' });

  return new Promise((resolve, reject) => {
    consumer
      .on('ready', () => resolve(consumer))
      .on('data', onData)
      .on('event.error', (err) => {
        console.error('Kafka event.error:', err);
        reject(err);
      });
    consumer.connect();
  });
}

// Function to consume Kafka messages
async function consumeMessages(bootstrapServers) {
  const config = {
    'bootstrap.servers': bootstrapServers,
    'group.id': 'kafka-nodejs-getting-started',
    'auto.offset.reset': 'earliest'
  };

  const topic = "Introduce";
  await createConsumer(config, ({ key, value }) => {
    // Kiểm tra nếu `key` hoặc `value` là null hoặc undefined
    const keyString = key ? key.toString() : '<null>';
    const valueString = value ? value.toString() : '<null>';
    
    const message = `Consumed event from topic ${topic}: key = ${keyString} value = ${valueString}`;
    console.log(message);
    messages.push({ key: keyString, value: valueString, timestamp: new Date().toISOString() });
  });

  consumer.subscribe([topic]);
  consumer.consume();
}

// API endpoint to trigger Kafka consumer
app.post('/consumer', async (req, res) => {
  const { bootstrap_server } = req.body;
  
  if (!bootstrap_server) {
    return res.status(400).json({ error: 'bootstrap_server in request body is required' });
  }

  // Ensure previous consumer is disconnected before starting a new one
  if (consumer) {
    consumer.disconnect(() => {
      console.log('Old consumer disconnected');
    });
  }

  try {
    await consumeMessages(bootstrap_server);
    res.json({ messages });
  } catch (err) {
    console.error(`Error consuming messages: ${err}`);
    res.status(500).json({ error: 'Error consuming messages' });
  }
});

// API endpoint to retrieve consumed messages
app.get('/consumer/messages', (req, res) => {
  res.json({ messages });
});

// Xử lý ngắt kết nối khi nhận tín hiệu SIGINT
process.on('SIGINT', () => {
  console.log('\nDisconnecting consumer ...');
  if (consumer) {
    consumer.disconnect(() => {
      console.log('Consumer disconnected');
      process.exit(0);
    });
  } else {
    process.exit(0);
  }
});

// Start the server
const PORT = 8081;
const HOST = '0.0.0.0';
app.listen(PORT, HOST, () => {
  console.log(`Consumer server is running on http://${HOST}:${PORT}`);
});
