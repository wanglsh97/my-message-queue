import { Kafka, logLevel } from "kafkajs";
import { config } from "./config.js";

export const TOPIC = "kafka-demo.tasks.v1";

export function createKafka(clientId: string): Kafka {
  return new Kafka({ clientId, brokers: [...config.brokers], logLevel: logLevel.WARN });
}

export async function ensureTopic(kafka: Kafka): Promise<void> {
  const admin = kafka.admin();
  await admin.connect();
  try {
    const topics = await admin.listTopics();
    if (!topics.includes(TOPIC)) {
      await admin.createTopics({
        waitForLeaders: true,
        topics: [{ topic: TOPIC, numPartitions: 3, replicationFactor: 1 }],
      });
    }
  } finally {
    await admin.disconnect();
  }
}
