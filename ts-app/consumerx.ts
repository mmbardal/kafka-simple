import { Kafka, EachBatchPayload, Consumer } from 'kafkajs';
import neo4j, { Driver, Session, Transaction } from 'neo4j-driver';

// --- Configuration ---
const KAFKA_BROKERS = ['localhost:9092', 'localhost:9094', 'localhost:9096'];
const TOPIC_NAME = 'events-topic';

// --- تنظیمات تکرار (برای شروع مجدد خواندن) ---
// هر بار که می‌خواهید خواندن از کافکا را از صفر شروع کنید، این عدد را افزایش دهید.
const GROUP_RUN_NUMBER = 2; // Increased to 9 after fixing the Neo4j syntax error in closureQuery
const GROUP_ID = `neo4j-graph-builder-group-${GROUP_RUN_NUMBER}`;

// Neo4j Connection Details (Matches docker-compose.yml)
const NEO4J_URI = 'bolt://localhost:7687';
const NEO4J_USER = 'neo4j';
const NEO4J_PASSWORD = 'password';

// Initialize Kafka Client
const kafka = new Kafka({
  clientId: 'events-consumer-neo4j',
  brokers: KAFKA_BROKERS,
});

// Create Kafka Consumer
const consumer: Consumer = kafka.consumer({ groupId: GROUP_ID });

// Create Neo4j Driver
let driver: Driver;

// Interface for our event data (without mandatory timestamp)
interface EventData {
  id: string;
  type: string;
  sourceId: string;
  targetId: string;
  clientId: string;
  timestamp?: number; // Optional, as we generate it here
}

/**
 * Sanitizes the 'type' string to be a valid Cypher relationship type.
 * e.g., "processcreate" -> "PROCESSCREATE"
 */
function sanitizeRelationshipType(type: string): string | null {
  if (!type || type.trim() === '') {
    return null;
  }
  // Converts to uppercase and replaces invalid chars with underscore
  const sanitized = type.toUpperCase().replace(/[^A-Z0-9_]/g, '_');
  
  // Ensure it starts with a letter (Cypher requirement)
  if (/^[A-Z]/.test(sanitized)) {
    return sanitized;
  }
  return null; // Invalid type
}

/**
 * Processes a single event and models its lifecycle using startTime and endTime.
 * This is the core logic for temporal modeling.
 */
async function processMessage(tx: Transaction, data: EventData, currentTimestamp: number): Promise<void> {
  const relationshipType = sanitizeRelationshipType(data.type);

  if (!relationshipType) {
    console.warn(`[CONSUMER] Skipping message with invalid type for relationship: ${data.type}`);
    return;
  }

  // Common Cypher block: Ensure nodes and permanent HOSTS relationships exist
  const baseQuery = `
    MERGE (c:Client {id: $clientId})
    MERGE (source:Entity {id: $sourceId})
    ${data.targetId && data.targetId.trim() !== '' ? 'MERGE (target:Entity {id: $targetId})' : ''}
    MERGE (c)-[:HOSTS]->(source)
    ${data.targetId && data.targetId.trim() !== '' ? 'MERGE (c)-[:HOSTS]->(target)' : ''}
  `;
  const params = { ...data, currentTimestamp }; 

  if (data.type === 'processterminate') {
  const closureQuery = `
    ${baseQuery}
    WITH source, $currentTimestamp AS endTime
    MATCH (source)-[r]->()
    WHERE r.endTime IS NULL
    SET r.endTime = endTime
    SET source.status = 'TERMINATED'
    RETURN count(r) AS closedRelationships
  `;
  const result = await tx.run(closureQuery, params);
  const count = result.records[0].get('closedRelationships');
  //console.log(`[CONSUMER] Terminated process ${data.sourceId}. Closed ${count} active relationships.`);
}else if (data.targetId && data.targetId.trim() !== '') {
    // --- START LOGIC: Create a new relationship with startTime ---
    const startQuery = `
      ${baseQuery}
      // NOTE: The previous WITH clause was removed to fix the Cypher syntax error.
      // 'source', 'target', and all parameters ($id, $currentTimestamp, $clientId) 
      // are available in the scope of the CREATE statement.
      // Create a new relationship with a startTime property using Processing Time.
      CREATE (source)-[r:${relationshipType} {
        eventId: $id,
        startTime: $currentTimestamp, // Use Processing Time for start
        clientId: $clientId
      }]->(target)
      SET source.status = 'ACTIVE' 
    `;
    await tx.run(startQuery, params);
  } else {
      // NOTE: Removed console.warn for skipping non-closure events with no targetId, as requested.
  }
}

/**
 * Handles the processing of each batch of messages received from Kafka.
 */
const eachBatch = async ({ batch, heartbeat }: EachBatchPayload) => {
  const session: Session = driver.session({ database: 'neo4j' });
  const tx: Transaction = session.beginTransaction();
  
  console.log(`[CONSUMER] Received batch of ${batch.messages.length} messages from partition ${batch.partition}`);

  try {
    for (const message of batch.messages) {
      if (!message.value) continue;

      let data: EventData;
      try {
        data = JSON.parse(message.value.toString());
        
        // --- VALIDATION: Check for mandatory fields ---
        if (!data.id || !data.type || !data.clientId || !data.sourceId) {
            console.warn(`[CONSUMER] Skipping malformed message (missing critical fields): ${message.value.toString()}`);
            continue;
        }

      } catch (parseError) {
        console.error(`[CONSUMER] Failed to parse JSON, skipping message at offset ${message.offset}: ${message.value.toString()}`, parseError);
        continue; // Skip this message
      }
      
      // 1. Capture the current system time (substituting for missing event timestamp)
      const currentTimestamp = Date.now(); 

      // 2. Process the message with the generated timestamp
      await processMessage(tx, data, currentTimestamp);
      
      // 3. APPLY 1ms DELAY as requested to simulate time progression between events
      await new Promise(resolve => setTimeout(resolve, 1));
    }

    // If all messages in the batch are processed, commit the Neo4j transaction
    await tx.commit();
    console.log(`[CONSUMER] Successfully processed batch and committed to Neo4j.`);

    // Manually commit the final offset of the batch to Kafka
    const lastOffset = String(Number(batch.lastOffset()) + 1);
    await consumer.commitOffsets([{ 
      topic: batch.topic, 
      partition: Number(batch.partition), 
      offset: lastOffset
    }]);
    console.log(`[CONSUMER] Committed Kafka offset ${lastOffset}`);

  } catch (error) {
    console.error(`[CONSUMER] Error processing batch with Neo4j, rolling back transaction.`, error);
    // If an error occurred (e.g., Neo4j connection failure), roll back the transaction
    await tx.rollback();
    // Re-throw the error to make KafkaJS retry this entire batch
    throw error;
  } finally {
    await session.close();
    await heartbeat();
  }
};

/**
 * Main function to run the consumer.
 */
async function runConsumer() {
  try {
    // 1. Initialize Neo4j Driver
    console.log(`[CONSUMER] Initializing Neo4j driver to ${NEO4J_URI}...`);
    driver = neo4j.driver(NEO4J_URI, neo4j.auth.basic(NEO4J_USER, NEO4J_PASSWORD));
    await driver.verifyConnectivity();
    console.log('[CONSUMER] Neo4j connection successful.');
    
    // 2. Connect to Kafka
    console.log('[CONSUMER] Connecting to Kafka...');
    await consumer.connect();
    
    // 3. Subscribe to Topic
    await consumer.subscribe({ topic: TOPIC_NAME, fromBeginning: true });
    
    // 4. Start Consuming
    console.log(`[CONSUMER] Subscribed to topic ${TOPIC_NAME}. Starting to consume. Group ID: ${GROUP_ID}`);
    await consumer.run({
      eachBatch: eachBatch,
      autoCommit: false, // We commit offsets manually after successful Neo4j transaction
    });

  } catch (error) {
    console.error('[CONSUMER] Fatal error in consumer setup:', error);
    // Cleanup on failure
    try {
      await consumer.disconnect();
      if (driver) {
        await driver.close();
      }
    } catch (e) { /* ignore */ }
    process.exit(1);
  }
}

// Handle graceful shutdown
process.on('SIGINT', async () => {
  console.log('[CONSUMER] Received SIGINT, shutting down gracefully...');
  try {
    await consumer.disconnect();
    if (driver) {
      await driver.close();
    }
  } catch (e) {
    console.error('Error during shutdown', e);
  } finally {
    process.exit(0);
  }
});

runConsumer();
