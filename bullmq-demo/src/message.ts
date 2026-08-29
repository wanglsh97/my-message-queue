import { randomUUID } from "node:crypto";

export type TaskMessage = {
  id: string;
  type: "task.created";
  occurredAt: string;
  payload: { sequence: number; task: string; shouldFail: boolean };
};

export function createTaskMessage(sequence: number, shouldFail: boolean): TaskMessage {
  return {
    id: randomUUID(),
    type: "task.created",
    occurredAt: new Date().toISOString(),
    payload: { sequence, task: `BullMQ 学习任务 ${sequence}`, shouldFail },
  };
}
