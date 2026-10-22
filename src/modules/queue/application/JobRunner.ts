import { EventEmitter } from "node:events";
import { JobAbortedError, JobTimeoutError, UnrecoverableError } from "../domain/errors";
import { JobExecutor } from "../domain/QueueDriver";
import { RetryPolicy } from "../domain/RetryPolicy";
import { Task } from "../domain/Task";
import { ExecutionResult, JobEnvelope, TaskContext } from "../domain/types";
import { TaskRegistry } from "./TaskRegistry";

const toError = (e: unknown): Error => (e instanceof Error ? e : new Error(String(e)));

/**
 * Chạy 1 job: tìm task, áp timeout, quyết định retry/fail.
 * Toàn bộ logic này dùng chung cho MỌI driver nên driver chỉ lo phần lưu trữ.
 *
 * Events: active | completed | retrying | failed | error
 */
export class JobRunner implements JobExecutor {
  constructor(
    private readonly registry: TaskRegistry,
    private readonly events: EventEmitter,
  ) {}

  async execute(job: JobEnvelope, external?: AbortSignal): Promise<ExecutionResult> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    let task: Task<any, any>;
    try {
      task = this.registry.resolveByName(job.task);
    } catch (e) {
      const error = toError(e);
      this.events.emit("failed", job, error);
      return { status: "failed", error };
    }

    // Gộp tín hiệu abort bên ngoài (shutdown / mất lock) với timeout nội bộ
    const ac = new AbortController();
    const forward = () => ac.abort(external?.reason ?? new JobAbortedError());
    if (external?.aborted) forward();
    else external?.addEventListener("abort", forward, { once: true });

    const ctx: TaskContext = {
      jobId: job.id,
      connection: job.connection,
      attempt: job.attempts,
      maxAttempts: job.maxAttempts,
      signal: ac.signal,
    };

    let timer: NodeJS.Timeout | undefined;
    let timedOut = false;
    this.events.emit("active", job);

    try {
      const result = await new Promise<unknown>((resolve, reject) => {
        timer = setTimeout(() => {
          timedOut = true;
          const err = new JobTimeoutError(job.id, job.timeout);
          ac.abort(err); // báo handler dừng việc đang làm
          reject(err);
        }, job.timeout);

        if (ac.signal.aborted) return reject(new JobAbortedError());
        // Promise.resolve().then bắt cả lỗi ném đồng bộ trong handler
        Promise.resolve()
          .then(() => task.handle(job.payload, ctx))
          .then(resolve, reject);
      });
      this.events.emit("completed", job, result);
      return { status: "completed", result };
    } catch (e) {
      const error = toError(e);
      if (!timedOut && external?.aborted) return { status: "aborted", error };
      return await this.handleFailure(task, job, ctx, error);
    } finally {
      if (timer) clearTimeout(timer);
      external?.removeEventListener("abort", forward);
    }
  }

  private async handleFailure(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    task: Task<any, any>,
    job: JobEnvelope,
    ctx: TaskContext,
    error: Error,
  ): Promise<ExecutionResult> {
    const canRetry =
      job.attempts < job.maxAttempts && !(error instanceof UnrecoverableError) && task.shouldRetry(error);

    if (canRetry) {
      const delayMs = RetryPolicy.delay(job.backoff, job.attempts);
      this.events.emit("retrying", job, error, delayMs);
      return { status: "retry", error, delayMs };
    }

    try {
      await task.onFailed(job.payload, error, ctx);
    } catch (hookError) {
      if (this.events.listenerCount("error") > 0) this.events.emit("error", toError(hookError));
      else console.error(`[queue] onFailed của task "${task.name}" ném lỗi:`, hookError);
    }
    this.events.emit("failed", job, error);
    return { status: "failed", error };
  }
}
