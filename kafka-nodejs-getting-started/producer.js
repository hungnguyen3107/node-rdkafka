const express = require('express');
const Kafka = require('node-rdkafka');

const app = express();
app.use(express.json()); 

app.get('/', (req, res) => {
  res.send('<h1>Welcome to Kafka Producer Service</h1><p>Use the <code>/produce</code> endpoint to produce messages to Kafka.</p>');
});


function createProducer(config, onDeliveryReport) {
  const producer = new Kafka.Producer(config);

  return new Promise((resolve, reject) => {
    producer
      .on('ready', () => {
        console.log("Producer is ready");
        resolve(producer);
      })
      .on('delivery-report', onDeliveryReport)
      .on('event.error', (err) => {
        console.warn('Producer error:', err);
        reject(err);
      });
    producer.connect();
  });
}

async function produceExample(bootstrapServers, message) {
  const config = {
    'bootstrap.servers': bootstrapServers,
    'acks': 'all',
    'dr_msg_cb': true,
  };

  let topic = "Introduce";

  try {
    const producer = await createProducer(config, (err, report) => {
      if (err) {
        console.warn('Error producing:', err);
      } else {
        const { topic, key, value } = report;

        let k = key ? key.toString().padEnd(10, ' ') : '<null>';
        console.log(`Produced event to topic ${topic}: key = ${k} value = ${value}`);
      }
    });

    producer.produce(topic, -1, Buffer.from(message));

    producer.flush(10000, () => {
      console.log('All messages sent successfully');
      producer.disconnect();
    });
  } catch (error) {
    console.error('Failed to produce messages:', error);
  }
}

app.post('/produce', async (req, res) => {
  const { bootstrap_server, message } = req.body;

  if (!bootstrap_server || !message) {
    return res.status(400).send('Request body must include "bootstrap_server" and "message".');
  }

  console.log(`Received request to produce with bootstrap: ${bootstrap_server}`);
  console.log(`Message to produce: ${message}`);

  try {
    await produceExample(bootstrap_server, message);
    res.status(200).send('Kafka message produced successfully');
  } catch (err) {
    console.error(`Something went wrong:\n${err}`);
    res.status(500).send('Failed to produce Kafka message');
  }
});

  
