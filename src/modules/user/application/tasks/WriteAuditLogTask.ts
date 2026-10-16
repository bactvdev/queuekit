import { Task, TaskContext } from "../../../../shared/queue";

export interface AuditLogRepository {
  append(entry: { action: string; userId: string; at: Date }): Promise<void>;
}

export interface WriteAuditLogPayload {
  action: string;
  userId: string;
}

/** Ghi audit log: nhẹ, cần chắc chắn xong => mặc định chạy đồng bộ */
export class WriteAuditLogTask extends Task<WriteAuditLogPayload, { written: true }> {
  readonly name = "audit.write-log";
  override readonly connection = "sync";

  constructor(private readonly repo: AuditLogRepository) {
    super();
  }

  async handle({ action, userId }: WriteAuditLogPayload, _ctx: TaskContext) {
    await this.repo.append({ action, userId, at: new Date() });
    return { written: true } as const;
  }
}
