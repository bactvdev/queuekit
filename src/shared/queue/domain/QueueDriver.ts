import { ExecutionResult, JobEnvelope, PushOptions, PushResult, QueueStats } from "./types";

/** Cổng để driver nhờ "thực thi 1 job" mà không phụ thuộc vào tầng application */
export interface JobExecutor {
  /** Không bao giờ reject; kết quả cho driver biết cần làm gì tiếp */
  execute(job: JobEnvelope, signal?: AbortSignal): Promise<ExecutionResult>;
}

/**
 * Port của hạ tầng queue. Mỗi loại queue (Redis, Sync, Memory, SQS, DB...)
 * là một adapter implement interface này.
 */
export interface QueueDriver {
  /** QueueManager gọi 1 lần để driver biết cách chạy job */
  attach(executor: JobExecutor): void;

  /** Đưa job vào queue */
  push(job: JobEnvelope, options: PushOptions): Promise<PushResult>;

  /** Bắt đầu consume job (process worker). Sync driver: no-op */
  startWorker(): Promise<void>;
  /** Ngừng nhận job mới, chờ job đang chạy tối đa graceMs */
  stopWorker(graceMs?: number): Promise<void>;

  getStats(): Promise<QueueStats>;
  /** Giải phóng tài nguyên (đóng kết nối...) */
  close(): Promise<void>;
}
