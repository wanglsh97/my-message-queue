import "dotenv/config";

function positiveInteger(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined) return fallback;
  const value = Number.parseInt(raw, 10);
  if (!Number.isInteger(value) || value <= 0) throw new Error(`${name} 必须是正整数`);
  return value;
}

export const config = {
  brokers: (process.env.KAFKA_BROKERS ?? "localhost:9092").split(",").map((item) => item.trim()),
  messageCount: positiveInteger("MESSAGE_COUNT", 5),
  concurrency: positiveInteger("CONSUMER_CONCURRENCY", 3),
  processingDelayMs: positiveInteger("PROCESSING_DELAY_MS", 500),
} as const;
