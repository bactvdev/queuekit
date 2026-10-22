import { DomainEvent } from "./DomainEvent";

export interface ListenerOptions {
  /** Tổng số lần được chạy (gồm lần đầu) */
  maxAttempts?: number;
  /** Timeout mỗi lần chạy (ms) */
  timeout?: number;
  backoff?: { type: "fixed" | "exponential"; delay: number };
  /** Trì hoãn xử lý sau khi publish (ms) */
  delay?: number;
}

export interface ListenerContext {
  eventId: string;
  listenerName: string;
  /** Lần chạy hiện tại (bắt đầu từ 1) */
  attempt: number;
  maxAttempts: number;
  /** Bị abort khi timeout / shutdown. Truyền vào fetch, axios... */
  signal: AbortSignal;
}

/**
 * Một handler phản ứng với 1 loại event. Mỗi listener chạy ĐỘC LẬP (job riêng):
 * listener này lỗi/retry không ảnh hưởng listener khác của cùng event.
 * Listener không biết gì về queue, chỉ phụ thuộc vào domain.
 *
 * Lưu ý: giao hàng "at-least-once", nên handle() cần idempotent.
 */
export abstract class DomainEventListener<E extends DomainEvent = DomainEvent> {
  /** Tên duy nhất, ví dụ "user.send-welcome-email". Được lưu trong queue nên đừng đổi tuỳ tiện */
  abstract readonly name: string;

  /** Queue connection riêng cho listener này (vd "redis", "redis-critical"). Bỏ trống = mặc định của bus */
  readonly connection?: string;

  /** Retry/timeout/delay riêng */
  readonly options: ListenerOptions = {};

  abstract handle(event: E, ctx: ListenerContext): Promise<void>;

  /** Quyết định lỗi nào được retry. Mặc định: mọi lỗi */
  shouldRetry(_error: Error): boolean {
    return true;
  }

  /** Hook gọi khi listener fail hẳn (hết lượt retry / lỗi không retry được) */
  async onFailed(_event: E, _error: Error, _ctx: ListenerContext): Promise<void> { }
}
