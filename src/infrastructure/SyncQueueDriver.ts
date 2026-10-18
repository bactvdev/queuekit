import { JobExecutor, QueueDriver } from "../domain/QueueDriver";
import { JobEnvelope, PushResult, QueueStats } from "../domain/types";

export interface SyncQueueDriverOptions {
  /**
   * true: retry ngay trong lời gọi (có sleep theo backoff). Mặc định false:
   * chạy đúng 1 lần, lỗi được ném thẳng cho nơi dispatch (giống Laravel "sync").
   */
  retry?: boolean;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * Chạy job NGAY trong process hiện tại, `await dispatch()` sẽ trả về kết quả của task.
 * Không có worker, không có delay. Phù hợp cho test, dev, hoặc tác vụ cần kết quả tức thì.
 */
export class SyncQueueDriver implements QueueDriver {
  private executor?: JobExecutor;

  constructor(private readonly options: SyncQueueDriverOptions = {}) {}

  attach(executor: JobExecutor): void {
    this.executor = executor;
  }

  async push(job: JobEnvelope): Promise<PushResult> {
    if (!this.executor) throw new Error("SyncQueueDriver chưa được attach vào QueueManager");
    const retry = this.options.retry ?? false;

    for (let attempts = 1; ; attempts++) {
      const res = await this.executor.execute({
        ...job,
        attempts,
        maxAttempts: retry ? job.maxAttempts : 1,
      });
      if (res.status === "completed") return { jobId: job.id, result: res.result };
      if (res.status === "retry") {
        await sleep(res.delayMs);
        continue;
      }
      throw res.error; // failed | aborted
    }
  }

  async startWorker(): Promise<void> {}
  async stopWorker(): Promise<void> {}
  async close(): Promise<void> {}

  async getStats(): Promise<QueueStats> {
    return { waiting: 0, delayed: 0, active: 0, failed: 0 };
  }
}
