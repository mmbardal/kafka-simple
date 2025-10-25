import { Kafka } from 'kafkajs';
import * as fs from 'fs';
import * as readline from 'readline';

// --- Configuration ---
const KAFKA_BROKERS = ['localhost:9092', 'localhost:9094', 'localhost:9096'];
const TOPIC_NAME = 'events-topic';
const FILE_PATH = './events.ndjson';

// Initialize Kafka Client
const kafka = new Kafka({
  clientId: 'events-producer',
  brokers: KAFKA_BROKERS,
  // Retry configuration for better resilience
  retry: {
    initialRetryTime: 300,
    retries: 5
  }
});

const producer = kafka.producer();

/**
 * Reads the NDJSON file line by line and sends each line as a Kafka message.
 */
async function produceEvents() {
  console.log(`[PRODUCER] Connecting to Kafka brokers: ${KAFKA_BROKERS.join(', ')}...`);
  await producer.connect();
  console.log('[PRODUCER] Connection successful.');

  let linesSent = 0;
  
  try {
    const fileStream = fs.createReadStream(FILE_PATH);
    const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity // Detect all line endings
    });

    console.log(`[PRODUCER] Starting to read file: ${FILE_PATH}`);

    // Process file line by line
    for await (const line of rl) {
      if (line.trim().length === 0) continue; // Skip empty lines

      // The line itself is the value (the raw JSON string)
      const message = { value: line };
      
      // Sending messages in batches is much more efficient than one by one
      await producer.send({
        topic: TOPIC_NAME,
        messages: [message],
        acks: 1 // Wait for leader acknowledgment
      });
      
      linesSent++;
      
      // Log progress periodically
      if (linesSent % 1000 === 0) {
        process.stdout.write(`\r[PRODUCER] Messages sent: ${linesSent}`);
      }
    }

    console.log(`\n[PRODUCER] File reading complete. Total messages sent: ${linesSent}`);
    
  } catch (error) {
    console.error(`[PRODUCER] An error occurred while producing messages:`, error);
  } finally {
    await producer.disconnect();
    console.log('[PRODUCER] Disconnected.');
  }
}

produceEvents();
