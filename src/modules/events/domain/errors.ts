export interface EventPublishFailure {
  eventName: string;
  eventId: string;
  listener: string;
  error: Error;
}

/**
 * Có listener không thể đẩy vào queue (vd Redis down, hoặc listener chạy sync bị lỗi).
 * Các listener còn lại vẫn đã được đẩy thành công.
 */
export class EventPublishError extends Error {
  constructor(public readonly failures: EventPublishFailure[]) {
    super(
      `Publish thất bại ${failures.length} listener: ` +
      failures.map((f) => `${f.eventName} -> ${f.listener} (${f.error.message})`).join("; "),
    );
    this.name = "EventPublishError";
  }
}

export class DuplicateListenerError extends Error {
  constructor(eventName: string, listener: string) {
    super(`Listener "${listener}" đã subscribe event "${eventName}"`);
    this.name = "DuplicateListenerError";
  }
}
