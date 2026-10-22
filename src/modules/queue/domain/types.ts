export type BackoffType = "fixed" | "exponential";

export interface BackoffOptions {
  type: BackoffType;
  /** Độ trễ cơ sở (ms) */
  delay: number;
}

/** Tuỳ chọn cho 1 job. Độ ưu tiên: dispatch() > Task.options > QueueManager.defaults */
export interface JobOptions {
  /** Tự đặt id (dùng để chống trùng job) */
  jobId?: string;
  /** Trì hoãn chạy (ms). Sync driver bỏ qua */
  delay?: number;
  /** Tổng số lần được chạy, gồm lần đầu */
  maxAttempts?: number;
  /** Timeout mỗi lần chạy (ms) */
  timeout?: number;
  backoff?: BackoffOptions;
}

export type ResolvedJobOptions = Required<Omit<JobOptions, "jobId" | "delay">> &
  Pick<JobOptions, "jobId" | "delay">;

/** Dạng serializable của 1 job đi qua driver */
export interface JobEnvelope {
  id: string;
  /** Tên task (Task.name) dùng để tìm handler */
  task: string;
  payload: unknown;
  /** Số lần đã chạy (lần đang chạy tính cả vào khi truyền cho executor) */
  attempts: number;
  maxAttempts: number;
  timeout: number;
  backoff: { type: "fixed" | "exponential"; delay: number };
  createdAt: number;
  /** Tên connection đã dispatch vào */
  connection: string;
}

/** Ngữ cảnh truyền vào Task.handle() */
export interface TaskContext {
  jobId: string;
  connection: string;
  /** Lần chạy hiện tại (bắt đầu từ 1) */
  attempt: number;
  maxAttempts: number;
  /** Bị abort khi timeout / shutdown. Truyền vào fetch, axios... */
  signal: AbortSignal;
}

/** Kết quả 1 lần thực thi, driver dựa vào đây để quyết định lưu trữ */
export type ExecutionResult =
  | { status: "completed"; result: unknown }
  | { status: "retry"; error: Error; delayMs: number }
  | { status: "failed"; error: Error }
  | { status: "aborted"; error: Error };

export interface PushOptions {
  delay?: number;
}

export interface PushResult {
  jobId: string;
  /** Chỉ có với driver chạy đồng bộ */
  result?: unknown;
}

export interface QueueStats {
  waiting: number;
  delayed: number;
  active: number;
  failed: number;
}
