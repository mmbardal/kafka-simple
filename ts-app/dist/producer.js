"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
const kafkajs_1 = require("kafkajs");
const fs = __importStar(require("fs"));
const readline = __importStar(require("readline"));
// --- Configuration ---
const KAFKA_BROKERS = ['localhost:9092', 'localhost:9094', 'localhost:9096'];
const TOPIC_NAME = 'events-topic';
const FILE_PATH = './events.ndjson';
// Initialize Kafka Client
const kafka = new kafkajs_1.Kafka({
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
            if (line.trim().length === 0)
                continue; // Skip empty lines
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
    }
    catch (error) {
        console.error(`[PRODUCER] An error occurred while producing messages:`, error);
    }
    finally {
        await producer.disconnect();
        console.log('[PRODUCER] Disconnected.');
    }
}
produceEvents();
