import { BackoffOptions } from "./types";

const MAX_BACKOFF_MS = 60 * 60 * 1000;

export class RetryPolicy {
  /**
   * Độ trễ trước lần thử kế tiếp.
   * @param attempt số lần đã chạy (>= 1)
   */
  static delay(backoff: BackoffOptions, attempt: number): number {
    const base = backoff.type === "exponential" ? backoff.delay * 2 ** (attempt - 1) : backoff.delay;
    const jitter = base * 0.1 * (Math.random() * 2 - 1); // ±10% tránh thundering herd
    return Math.min(MAX_BACKOFF_MS, Math.max(0, Math.round(base + jitter)));
  }
}
