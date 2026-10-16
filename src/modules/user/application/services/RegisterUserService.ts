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
