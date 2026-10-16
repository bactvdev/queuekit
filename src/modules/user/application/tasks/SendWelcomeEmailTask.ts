import { JobOptions, Task, TaskContext } from "../../../../shared/queue";

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
