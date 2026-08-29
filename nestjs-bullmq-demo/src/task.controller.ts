import { Body, Controller, Post } from "@nestjs/common";
import { TaskService } from "./task.service";

@Controller("tasks")
export class TaskController {
  constructor(private readonly taskService: TaskService) {}

  @Post()
  async create(@Body() body: { task?: string }) {
    const job = await this.taskService.add(body.task ?? "NestJS BullMQ 示例任务");
    return { jobId: job.id, state: "queued" };
  }
}
