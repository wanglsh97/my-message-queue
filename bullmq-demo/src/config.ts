import "dotenv/config";

function positiveInteger(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined) return fallback;
  const value = Number.parseInt(raw, 10);
  if (!Number.isInteger(value) || value <= 0) throw new Error(`${name} 必须是正整数`);
  return value;
}

export const config = {
  redisUrl: process.env.REDIS_URL ?? "redis://localhost:6379",
  messageCount: positiveInteger("MESSAGE_COUNT", 5),
  concurrency: positiveInteger("WORKER_CONCURRENCY", 3),
  processingDelayMs: positiveInteger("PROCESSING_DELAY_MS", 500),
  demoFailures: process.env.DEMO_FAILURES === "true",
} as const;
