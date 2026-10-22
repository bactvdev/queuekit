import { JobAbortedError } from "../domain/errors";
import { JobExecutor, QueueDriver } from "../domain/QueueDriver";
import { JobEnvelope, PushOptions, PushResult, QueueStats } from "../domain/types";

export interface InMemoryQueueDriverOptions {
  concurrency?: number;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * Queue bất đồng bộ trong bộ nhớ của process (mất khi restart).
 * Có concurrency, delay, retry, timeout. Hợp cho dev/test hoặc tác vụ nhẹ, không cần bền vững.
 * Job chỉ được xử lý sau khi gọi startWorker().
 */
export class InMemoryQueueDriver implements QueueDriver {
  private executor?: JobExecutor;
  private started = false;
  private readonly concurrency: number;
  private waiting: JobEnvelope[] = [];
  private readonly timers = new Set<NodeJS.Timeout>();
  private readonly active = new Map<string, AbortController>();
  private failed = 0;

  constructor(options: InMemoryQueueDriverOptions = {}) {
    this.concurrency = options.concurrency ?? 5;
  }

  attach(executor: JobExecutor): void {
    this.executor = executor;
  }

  async push(job: JobEnvelope, { delay = 0 }: PushOptions = {}): Promise<PushResult> {
    this.schedule(job, delay);
    return { jobId: job.id };
  }

  private schedule(job: JobEnvelope, delay: number) {
    if (delay <= 0) {
      this.waiting.push(job);
      this.pump();
      return;
    }
    const t = setTimeout(() => {
      this.timers.delete(t);
      this.waiting.push(job);
      this.pump();
    }, delay);
    this.timers.add(t);
  }

  private pump() {
    while (this.started && this.active.size < this.concurrency && this.waiting.length > 0) {
      void this.run(this.waiting.shift()!);
    }
  }

  private async run(job: JobEnvelope) {
    const ac = new AbortController();
    this.active.set(job.id, ac);
    const current = { ...job, attempts: job.attempts + 1 };
    try {
      const res = await this.executor!.execute(current, ac.signal);
      if (res.status === "retry") this.schedule(current, res.delayMs);
      else if (res.status === "failed") this.failed++;
    } finally {
      this.active.delete(job.id);
      this.pump();
    }
  }

  async startWorker(): Promise<void> {
    this.started = true;
    this.pump();
  }

  async stopWorker(graceMs = 10_000): Promise<void> {
    this.started = false;
    const deadline = Date.now() + graceMs;
    while (this.active.size > 0 && Date.now() < deadline) await sleep(50);
    for (const ac of this.active.values()) ac.abort(new JobAbortedError("Worker shutting down"));
  }

  /** Chờ đến khi hết job chờ/đang chạy/delayed. Tiện cho unit test. */
  async drain(): Promise<void> {
    while (this.waiting.length > 0 || this.timers.size > 0 || this.active.size > 0) await sleep(10);
  }

  async getStats(): Promise<QueueStats> {
    return {
      waiting: this.waiting.length,
      delayed: this.timers.size,
      active: this.active.size,
      failed: this.failed,
    };
  }

  async close(): Promise<void> {
    await this.stopWorker(0);
    this.timers.forEach(clearTimeout);
    this.timers.clear();
    this.waiting = [];
  }
}
