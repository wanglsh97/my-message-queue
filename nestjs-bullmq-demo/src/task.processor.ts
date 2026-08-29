import { setTimeout as sleep } from "node:timers/promises";
import { Logger } from "@nestjs/common";
import { OnWorkerEvent, Processor, WorkerHost } from "@nestjs/bullmq";
import type { Job } from "bullmq";
import { TASK_QUEUE } from "./task.constants";
import type { TaskData } from "./task.types";

@Processor(TASK_QUEUE)
export class TaskProcessor extends WorkerHost {
  private readonly logger = new Logger(TaskProcessor.name);

  async process(job: Job<TaskData>): Promise<{ processedAt: string }> {
    this.logger.log(`开始处理 Job ${job.id}: ${job.data.task}`);
    await sleep(500);
    return { processedAt: new Date().toISOString() };
  }

  @OnWorkerEvent("completed")
  onCompleted(job: Job<TaskData>) {
    this.logger.log(`Job ${job.id} 处理完成`);
  }

  @OnWorkerEvent("failed")
  onFailed(job: Job<TaskData> | undefined, error: Error) {
    this.logger.error(`Job ${job?.id ?? "unknown"} 处理失败: ${error.message}`);
  }
}
