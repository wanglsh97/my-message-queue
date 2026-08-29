import { Injectable } from "@nestjs/common";
import { InjectQueue } from "@nestjs/bullmq";
import type { Queue } from "bullmq";
import { TASK_QUEUE } from "./task.constants";
import type { TaskData } from "./task.types";

@Injectable()
export class TaskService {
  constructor(@InjectQueue(TASK_QUEUE) private readonly taskQueue: Queue<TaskData>) {}

  async add(task: string) {
    return this.taskQueue.add("execute", { task }, {
      attempts: 3,
      backoff: { type: "exponential", delay: 1_000 },
    });
  }
}
