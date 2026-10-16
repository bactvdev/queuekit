import {
  InMemoryQueueDriver,
  QueueManager,
  RedisQueueDriver,
  SyncQueueDriver,
} from "../shared/queue";
import { AuditLogRepository } from "../modules/user/application/tasks/WriteAuditLogTask";
import { Mailer, SendWelcomeEmailTask } from "../modules/user/application/tasks/SendWelcomeEmailTask";
import { WriteAuditLogTask } from "../modules/user/application/tasks/WriteAuditLogTask";

export interface QueueDeps {
  mailer: Mailer;
  auditLogs: AuditLogRepository;
}

/** Composition root: nơi DUY NHẤT biết các loại queue và đăng ký task */
export function createQueueManager(deps: QueueDeps): QueueManager {
  const redisUrl = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";

  const queue = new QueueManager({
    default: "redis",
    connections: {
      sync: new SyncQueueDriver(),
      memory: new InMemoryQueueDriver({ concurrency: 2 }),
      redis: new RedisQueueDriver({ queue: "default", connection: redisUrl, concurrency: 5 }),
      "redis-critical": new RedisQueueDriver({ queue: "critical", connection: redisUrl, concurrency: 10 }),
    },
    defaults: { maxAttempts: 3, timeout: 30_000 },
  });

  queue.registerTasks(new SendWelcomeEmailTask(deps.mailer), new WriteAuditLogTask(deps.auditLogs));

  queue.on("retrying", (job, err, delay) =>
    console.warn(`[queue] ${job.task}#${job.id} lỗi lần ${job.attempts}/${job.maxAttempts}: ${err.message}, thử lại sau ${delay}ms`),
  );
  queue.on("failed", (job, err) => console.error(`[queue] ${job.task}#${job.id} fail hẳn: ${err.message}`));
  queue.on("completed", (job) => console.log(`[queue] ${job.task}#${job.id} xong`));

  return queue;
}
