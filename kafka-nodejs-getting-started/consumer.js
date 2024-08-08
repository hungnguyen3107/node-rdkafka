const commander = require('commander');
const Kafka = require('node-rdkafka');

// Configure commander to handle command-line arguments
commander
  .version('1.0.0', '-v, --version')
  .usage('[OPTIONS]...')
  .option('-b, --bootstrap <address>', 'Bootstrap servers address', 'my-cluster-kafka-bootstrap:9092')  // Default address if none is provided
  .parse(process.argv);

const options = commander.opts();

// Kafka consumer configuration
function createConsumer(config, onData) {
  const consumer = new Kafka.KafkaConsumer(config, {'auto.offset.reset': 'earliest'});

  return new Promise((resolve, reject) => {
    consumer
      .on('ready', () => resolve(consumer))
      .on('data', onData)
      .on('event.error', (err) => {
        console.error('Kafka event.error:', err);
        reject(err);
      })
      .on('event.log', (log) => {
        console.log('Kafka event.log:', log);
      });
    consumer.connect();
  });
}

async function consumerExample() {
  // Use the bootstrap address from command-line arguments
  const config = {
    'bootstrap.servers': options.bootstrap,
    //'sasl.username':     '<CLUSTER API KEY>',
    //'sasl.password':     '<CLUSTER API SECRET>',

    // Fixed properties
    //'security.protocol': 'SASL_SSL',
    //'sasl.mechanisms':   'PLAIN',
    'group.id':          'kafka-nodejs-getting-started'
  };

  const topic = "purchases";

  const consumer = await createConsumer(config, ({ key, value }) => {
    let k = key.toString().padEnd(10, ' ');
    const timestamp = new Date().toISOString(); // Timestamp
    console.log(`[${timestamp}] Consumed event from topic ${topic}: key = ${k} value = ${value}`);
  });

  consumer.subscribe([topic]);
  consumer.consume();

  process.on('SIGINT', () => {
    console.log('\nDisconnecting consumer ...');
    consumer.disconnect();
  });
}

consumerExample()
  .catch((err) => {
    console.error(`Something went wrong:\n${err}`);
    process.exit(1);
  });
