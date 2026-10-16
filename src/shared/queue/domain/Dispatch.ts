import { TaskClass } from "./Task";
import { BackoffOptions, JobOptions } from "./types";

export interface DispatchReceipt<R = unknown> {
  jobId: string;
  /** Connection thực sự được dùng */
  connection: string;
  /** Chỉ có khi connection chạy đồng bộ (sync) */
  result?: R;
}

export interface DispatchSpec<P, R> {
  taskClass: TaskClass<P, R>;
  payload: P;
  connection?: string;
  options: JobOptions;
}

/**
 * Builder fluent cho 1 lần dispatch. Có thể `await` trực tiếp:
 *   await dispatcher.dispatch(MyTask, payload).onConnection("sync").delay(5000);
 * Chỉ gửi đúng 1 lần dù gọi send()/await nhiều lần.
 */
export class PendingDispatch<P, R = unknown> implements PromiseLike<DispatchReceipt<R>> {
  private connectionName?: string;
  private readonly opts: JobOptions = {};
  private promise?: Promise<DispatchReceipt<R>>;

  constructor(
    private readonly taskClass: TaskClass<P, R>,
    private readonly payload: P,
    private readonly sender: (spec: DispatchSpec<P, R>) => Promise<DispatchReceipt<R>>,
  ) {}

  /** Chọn loại queue cho lần dispatch này (ghi đè Task.connection) */
  onConnection(name: string): this {
    this.connectionName = name;
    return this;
  }
  delay(ms: number): this {
    this.opts.delay = ms;
    return this;
  }
  attempts(n: number): this {
    this.opts.maxAttempts = n;
    return this;
  }
  timeout(ms: number): this {
    this.opts.timeout = ms;
    return this;
  }
  backoff(b: BackoffOptions): this {
    this.opts.backoff = b;
    return this;
  }
  /** Đặt jobId để chống trùng (idempotent) */
  withId(id: string): this {
    this.opts.jobId = id;
    return this;
  }

  send(): Promise<DispatchReceipt<R>> {
    this.promise ??= this.sender({
      taskClass: this.taskClass,
      payload: this.payload,
      connection: this.connectionName,
      options: { ...this.opts },
    });
    return this.promise;
  }

  then<T1 = DispatchReceipt<R>, T2 = never>(
    onfulfilled?: ((value: DispatchReceipt<R>) => T1 | PromiseLike<T1>) | null,
    onrejected?: ((reason: unknown) => T2 | PromiseLike<T2>) | null,
  ): PromiseLike<T1 | T2> {
    return this.send().then(onfulfilled, onrejected);
  }
}

/** Port mà application layer phụ thuộc vào (không biết gì về Redis/Sync...) */
export interface TaskDispatcher {
  dispatch<P, R = unknown>(task: TaskClass<P, R>, payload: P): PendingDispatch<P, R>;
}
