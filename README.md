# Queue module (DDD, Ports & Adapters)

```
src/shared/queue/
  domain/            Task, QueueDriver (port), TaskDispatcher (port), PendingDispatch, RetryPolicy, types, errors
  application/       TaskRegistry, JobRunner (timeout + retry), QueueManager (facade)
  infrastructure/    SyncQueueDriver, InMemoryQueueDriver, RedisQueueDriver (adapters)
src/modules/user/application/   ví dụ Task + Application Service
src/bootstrap/                  composition root (queue.ts) + entry của worker (worker.ts)
tests-manual/                   smoke test chạy tay (cần redis ở localhost:6379)
```

Thêm loại queue mới (SQS, RabbitMQ, DB...): implement `QueueDriver`, rồi thêm vào `connections` trong `bootstrap/queue.ts`.

# Example
````import { JobOptions, Task, TaskContext } from "../../../../shared/queue";

```
export interface Mailer {
  send(to: string, subject: string, body: string, signal?: AbortSignal): Promise<void>;
}

export interface SendWelcomeEmailPayload {
  userId: string;
  email: string;
}

/** Gửi email chào mừng: chậm, có thể lỗi mạng => nên chạy nền trên Redis, có retry */
export class SendWelcomeEmailTask extends Task<SendWelcomeEmailPayload, void> {
  readonly name = "user.send-welcome-email";
  override readonly connection = "redis"; // queue mặc định của task này
  override readonly options: JobOptions = {
    maxAttempts: 5,
    timeout: 10_000,
    backoff: { type: "exponential", delay: 2000 }, // ~2s, 4s, 8s, 16s
  };

  constructor(private readonly mailer: Mailer) {
    super();
  }

  async handle({ email }: SendWelcomeEmailPayload, ctx: TaskContext): Promise<void> {
    // truyền ctx.signal để timeout dừng được request thật sự
    await this.mailer.send(email, "Chào mừng!", "Cảm ơn bạn đã đăng ký.", ctx.signal);
  }

  override async onFailed({ userId }: SendWelcomeEmailPayload, error: Error): Promise<void> {
    console.error(`Không gửi được email chào mừng cho user ${userId}:`, error.message);
  }
}
  
```
import { TaskDispatcher } from "../../../../shared/queue";
import { SendWelcomeEmailTask } from "../tasks/SendWelcomeEmailTask";
import { WriteAuditLogTask } from "../tasks/WriteAuditLogTask";

interface User {
  id: string;
  email: string;
}
export interface UserRepository {
  create(email: string): Promise<User>;
}

/**
 * Application service chỉ phụ thuộc vào PORT `TaskDispatcher`,
 * không biết Redis/Sync/Memory là gì.
 */
export class RegisterUserService {
  constructor(
    private readonly users: UserRepository,
    private readonly tasks: TaskDispatcher,
  ) {}

  async execute(email: string): Promise<User> {
    const user = await this.users.create(email);

    // 1) Dùng connection mặc định của task (redis)
    await this.tasks.dispatch(SendWelcomeEmailTask, { userId: user.id, email });

    // 2) Ép chạy loại queue khác chỉ cho lần gọi này
    await this.tasks.dispatch(SendWelcomeEmailTask, { userId: user.id, email }).onConnection("memory");

    // 3) Tuỳ chỉnh retry/timeout/delay + chống trùng cho lần gọi này
    await this.tasks
      .dispatch(SendWelcomeEmailTask, { userId: user.id, email })
      .delay(60_000)
      .attempts(2)
      .timeout(5000)
      .withId(`welcome-reminder:${user.id}`);

    // 4) Task mặc định sync: `await` xong là đã chạy xong, có kết quả
    const receipt = await this.tasks.dispatch(WriteAuditLogTask, { action: "user.registered", userId: user.id });
    console.log(receipt.result); // { written: true }

    return user;
  }
}

```
