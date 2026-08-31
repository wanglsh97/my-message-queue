import { NestFactory } from "@nestjs/core";
import { MicroserviceOptions, Transport } from "@nestjs/microservices";
import { AppModule } from "./app.module.js";
import { ORDER_QUEUE, orderQueueOptions, rabbitUrl } from "./rabbit.constants.js";

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(AppModule);
  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.RMQ,
    options: {
      urls: [rabbitUrl],
      queue: ORDER_QUEUE,
      queueOptions: orderQueueOptions,
      noAck: false,
      prefetchCount: 5,
    },
  });
  await app.startAllMicroservices();
  await app.listen(Number(process.env.PORT ?? 3001));
}

void bootstrap();
