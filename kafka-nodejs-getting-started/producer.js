const commander = require('commander');
const Kafka = require('node-rdkafka');

// Configure commander to handle command-line arguments
commander
  .version('1.0.0', '-v, --version')
  .usage('[OPTIONS]...')
  .option('-b, --bootstrap <address>', 'Bootstrap servers address', 'my-cluster-kafka-bootstrap:9092')  // Default address if none is provided
  .parse(process.argv);

const options = commander.opts();

// Kafka producer configuration
function createProducer(config, onDeliveryReport) {
  const producer = new Kafka.Producer(config);

  return new Promise((resolve, reject) => {
    producer
      .on('ready', () => resolve(producer))
      .on('delivery-report', onDeliveryReport)
      .on('event.error', (err) => {
        console.warn('event.error', err);
        reject(err);
      });
    producer.connect();
  });
}

async function produceExample() {
  // Use the bootstrap address from command-line arguments
  const config = {
    'bootstrap.servers': options.bootstrap,
    //'sasl.username':     '<CLUSTER API KEY>',
    //'sasl.password':     '<CLUSTER API SECRET>',

    // Fixed properties
    //'security.protocol': 'SASL_SSL',
    //'sasl.mechanisms':   'PLAIN',
    'acks':              'all',

    // Needed for delivery callback to be invoked
    'dr_msg_cb': true
  };

  let topic = "purchases";
  let users = [ "eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther" ];
  let items = [ "book", "alarm clock", "t-shirts", "gift card", "batteries" ];

  const producer = await createProducer(config, (err, report) => {
    if (err) {
      console.warn('Error producing', err);
    } else {
      const {topic, key, value} = report;
      let k = key.toString().padEnd(10, ' ');
      console.log(`Produced event to topic ${topic}: key = ${k} value = ${value}`);
    }
  });

  let numEvents = 10;
  for (let idx = 0; idx < numEvents; ++idx) {
    const key = users[Math.floor(Math.random() * users.length)];
    const value = Buffer.from(items[Math.floor(Math.random() * items.length)]);

    producer.produce(topic, -1, value, key);
  }

  producer.flush(10000, () => {
    producer.disconnect();
  });
}

produceExample()
  .catch((err) => {
    console.error(`Something went wrong:\n${err}`);
    process.exit(1);
  });
