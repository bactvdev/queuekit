import { JobOptions, TaskContext } from "./types";

/**
 * Một đơn vị công việc bất đồng bộ trong domain/application.
 * Mỗi Task là 1 class, nhận dependency qua constructor (DI) và được
 * đăng ký 1 lần trong composition root.
 */
export abstract class Task<TPayload = unknown, TResult = unknown> {
  /** Tên duy nhất, ví dụ "user.send-welcome-email". Được lưu vào queue nên đừng đổi tuỳ tiện */
  abstract readonly name: string;

  /** Connection mặc định (tên đã cấu hình trong QueueManager). Bỏ trống = dùng default */
  readonly connection?: string;

  /** Tuỳ chọn mặc định của task (retry, timeout...) */
  readonly options: JobOptions = {};

  abstract handle(payload: TPayload, ctx: TaskContext): Promise<TResult>;

  /** Quyết định lỗi nào được retry. Mặc định: mọi lỗi (trừ UnrecoverableError) */
  shouldRetry(_error: Error): boolean {
    return true;
  }

  /** Hook gọi khi job fail hẳn (hết lượt retry / lỗi không retry được) */
  async onFailed(_payload: TPayload, _error: Error, _ctx: TaskContext): Promise<void> {}
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type TaskClass<P = any, R = any> = abstract new (...args: any[]) => Task<P, R>;
