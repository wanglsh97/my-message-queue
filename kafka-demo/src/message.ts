import { randomUUID } from "node:crypto";

export type TaskMessage = {
  id: string;
  type: "task.created";
  occurredAt: string;
  payload: { sequence: number; task: string };
};

export function createTaskMessage(sequence: number): TaskMessage {
  return {
    id: randomUUID(),
    type: "task.created",
    occurredAt: new Date().toISOString(),
    payload: { sequence, task: `Kafka 学习事件 ${sequence}` },
  };
}

export function parseTaskMessage(raw: string): TaskMessage {
  const value: unknown = JSON.parse(raw);
  if (typeof value !== "object" || value === null || !("id" in value) || !("payload" in value)) {
    throw new Error("消息格式不合法");
  }
  return value as TaskMessage;
}
