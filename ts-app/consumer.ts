import { Kafka, EachBatchPayload, Consumer } from 'kafkajs';
import * as mysql from 'mysql2/promise';

// --- Configuration ---
const KAFKA_BROKERS = ['localhost:9092', 'localhost:9094', 'localhost:9096'];
const TOPIC_NAME = 'events-topic';
const GROUP_ID = 'mysql-writer-group';

// MySQL Connection Details (Based on your request)
const MYSQL_CONFIG = {
  host: '192.168.101.92',
  port: 3310,
  user: 'root',
  password: '123456789',
  database: 'kafka_test',
};
const MYSQL_TABLE = 'data';

// Initialize Kafka Client
const kafka = new Kafka({
  clientId: 'events-consumer',
  brokers: KAFKA_BROKERS,
});

// Create Kafka Consumer
const consumer = kafka.consumer({ groupId: GROUP_ID });

// Create MySQL Connection Pool
let dbPool: mysql.Pool;

/**
 * Handles the processing of each batch of messages received from Kafka.
 * @param payload The payload containing the batch of messages and metadata.
 */
// FIX 1 & 2: Removed unused imports (Partition, TopicPartition) and removed commitOffsetsInTransaction from destructuring
const eachBatch = async ({ batch, resolveOffset, heartbeat }: EachBatchPayload) => {
  const messagesToInsert = batch.messages
    .filter(message => message.value !== null)
    .map(message => [
      message.value!.toString() // The raw NDJSON string
    ]);

  if (messagesToInsert.length === 0) {
    // Nothing to process, just resolve the offset
    return resolveOffset(batch.lastOffset());
  }

  console.log(`[CONSUMER] Received batch of ${messagesToInsert.length} messages from topic ${batch.topic}, partition ${batch.partition}`);

  try {
    // Use a connection from the pool to execute the batch insert
    const connection = await dbPool.getConnection();
    try {
      // Construct the multi-row insert query
      const sql = `INSERT INTO ${MYSQL_TABLE} (data) VALUES ?`;
      
      // Execute the query
      await connection.query(sql, [messagesToInsert]);
      
      // Manually commit the offsets after successful database insert
      await consumer.commitOffsets([
          { 
            topic: batch.topic, 
            // Ensures partition is a number (required by the type definition)
            partition: Number(batch.partition), 
            // Final Fix: Calculate the next offset, convert to string, and assert its type
            offset: String(Number(batch.lastOffset) + 1) as string
          }
      ]);
      
      console.log(`[CONSUMER] Successfully inserted ${messagesToInsert.length} records and committed offset ${batch.lastOffset}`);
      
    } catch (dbError) {
      console.error(`[CONSUMER] Failed to insert batch into MySQL. Retrying or handling error is needed.`, dbError);
      // Throwing the error will allow KafkaJS to retry the batch
      throw dbError; 
    } finally {
      connection.release(); // Release the connection back to the pool
    }

  } catch (error) {
    console.error(`[CONSUMER] Error processing batch:`, error);
    // Important: Do not commit offsets if the DB write failed, ensuring the batch is retried.
  }

  await heartbeat(); // Keep the consumer session alive
};

/**
 * Main function to run the consumer.
 */
async function runConsumer() {
  try {
    // 1. Initialize MySQL Connection Pool
    console.log(`[CONSUMER] Initializing MySQL connection pool to ${MYSQL_CONFIG.host}:${MYSQL_CONFIG.port}...`);
    dbPool = mysql.createPool(MYSQL_CONFIG);
    await dbPool.query('SELECT 1'); // Test connection
    console.log('[CONSUMER] MySQL connection successful.');
    
    // 2. Connect to Kafka
    console.log('[CONSUMER] Connecting to Kafka...');
    await consumer.connect();
    
    // 3. Subscribe to Topic
    await consumer.subscribe({ topic: TOPIC_NAME, fromBeginning: true });
    
    // 4. Start Consuming
    console.log(`[CONSUMER] Subscribed to topic ${TOPIC_NAME}. Starting to consume.`);
    await consumer.run({
      eachBatch: eachBatch,
      autoCommit: false, // We commit manually after successful DB write
    });

  } catch (error) {
    console.error('[CONSUMER] Fatal error in consumer setup:', error);
    // Cleanup on failure
    try {
      await consumer.disconnect();
    } catch (e) { /* ignore */ }
    process.exit(1);
  }
}

runConsumer();
