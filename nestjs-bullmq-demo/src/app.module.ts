import { Module } from "@nestjs/common";
import { BullModule } from "@nestjs/bullmq";
import { TASK_QUEUE } from "./task.constants";
import { TaskController } from "./task.controller";
import { TaskProcessor } from "./task.processor";
import { TaskService } from "./task.service";

@Module({
  imports: [
    BullModule.forRoot({
      connection: {
        host: "127.0.0.1",
        port: 6379,
      },
    }),
    BullModule.registerQueue({
      name: TASK_QUEUE,
    }),
  ],
  controllers: [TaskController],
  providers: [TaskService, TaskProcessor],
})
export class AppModule {}
