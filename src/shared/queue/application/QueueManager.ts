import { EventEmitter } from "node:events";
import { randomUUID } from "node:crypto";
import { UnknownConnectionError } from "../domain/errors";
import { DispatchReceipt, DispatchSpec, PendingDispatch, TaskDispatcher } from "../domain/Dispatch";
import { QueueDriver } from "../domain/QueueDriver";
import { Task, TaskClass } from "../domain/Task";
import { JobEnvelope, JobOptions, QueueStats, ResolvedJobOptions } from "../domain/types";
import { JobRunner } from "./JobRunner";
import { TaskRegistry } from "./TaskRegistry";

export interface QueueManagerConfig {
  /** Tên connection mặc định khi Task và dispatch() đều không chỉ định */
  default: string;
  /** Các loại queue sẵn có, đặt tên tuỳ ý: { sync: ..., redis: ..., "redis-critical": ... } */
  connections: Record<string, QueueDriver>;
  /** Mặc định chung cho mọi job */
  defaults?: JobOptions;
}

const BUILTIN_DEFAULTS: ResolvedJobOptions = {
  maxAttempts: 3,
  timeout: 30_000,
  backoff: { type: "exponential", delay: 1000 },
};

/** Bỏ các key có giá trị undefined để không ghi đè nhầm khi merge */
const defined = (o: JobOptions): JobOptions =>
  Object.fromEntries(Object.entries(o).filter(([, v]) => v !== undefined)) as JobOptions;

/**
 * Điểm vào duy nhất của hệ thống queue (facade).
 *  - dispatch(): chọn connection theo thứ tự  dispatch().onConnection() > Task.connection > config.default
 *  - startWorkers(): chạy consumer cho các connection cần xử lý trong process này
 *
 * Events: dispatched | active | completed | retrying | failed | error
 */
export class QueueManager extends EventEmitter implements TaskDispatcher {
  private readonly registry = new TaskRegistry();
  private readonly runner: JobRunner;
  private readonly defaults: ResolvedJobOptions;

  constructor(private readonly config: QueueManagerConfig) {
    super();
    if (!config.connections[config.default]) throw new UnknownConnectionError(config.default);
    this.defaults = { ...BUILTIN_DEFAULTS, ...defined(config.defaults ?? {}) } as ResolvedJobOptions;
    this.runner = new JobRunner(this.registry, this);
    for (const driver of Object.values(config.connections)) driver.attach(this.runner);
  }

  /* --------------------------- tasks --------------------------- */

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  registerTask(task: Task<any, any>): this {
    this.registry.register(task);
    return this;
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  registerTasks(...tasks: Task<any, any>[]): this {
    tasks.forEach((t) => this.registry.register(t));
    return this;
  }

  /* --------------------------- producer --------------------------- */

  dispatch<P, R = unknown>(task: TaskClass<P, R>, payload: P): PendingDispatch<P, R> {
    return new PendingDispatch<P, R>(task, payload, (spec) => this.send(spec));
  }

  private async send<P, R>(spec: DispatchSpec<P, R>): Promise<DispatchReceipt<R>> {
    const task = this.registry.resolveByClass(spec.taskClass);
    const connection = spec.connection ?? task.connection ?? this.config.default;
    const driver = this.connection(connection);

    const o = { ...this.defaults, ...defined(task.options), ...defined(spec.options) } as ResolvedJobOptions;
    const envelope: JobEnvelope = {
      id: o.jobId ?? randomUUID(),
      task: task.name,
      payload: spec.payload,
      attempts: 0,
      maxAttempts: o.maxAttempts,
      timeout: o.timeout,
      backoff: o.backoff,
      createdAt: Date.now(),
      connection,
    };

    const pushed = await driver.push(envelope, { delay: o.delay });
    this.emit("dispatched", envelope);
    return { jobId: pushed.jobId, connection, result: pushed.result as R | undefined };
  }

  /* --------------------------- worker --------------------------- */

  /** Chạy consumer cho các connection chỉ định (mặc định: tất cả) */
  async startWorkers(connections: string[] = Object.keys(this.config.connections)): Promise<void> {
    await Promise.all(connections.map((name) => this.connection(name).startWorker()));
  }

  async stopWorkers(graceMs = 10_000): Promise<void> {
    await Promise.all(Object.values(this.config.connections).map((d) => d.stopWorker(graceMs)));
  }

  /* --------------------------- misc --------------------------- */

  connection(name: string = this.config.default): QueueDriver {
    const driver = this.config.connections[name];
    if (!driver) throw new UnknownConnectionError(name);
    return driver;
  }

  async getStats(): Promise<Record<string, QueueStats>> {
    const entries = await Promise.all(
      Object.entries(this.config.connections).map(async ([name, d]) => [name, await d.getStats()] as const),
    );
    return Object.fromEntries(entries);
  }

  /** Dừng worker (có chờ) rồi đóng toàn bộ kết nối */
  async shutdown(graceMs = 10_000): Promise<void> {
    await this.stopWorkers(graceMs);
    await Promise.all(Object.values(this.config.connections).map((d) => d.close()));
  }
}
