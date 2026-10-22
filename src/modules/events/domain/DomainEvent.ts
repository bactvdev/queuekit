import { randomUUID } from "node:crypto";

export interface EventMeta {
  eventId?: string;
  occurredAt?: Date;
}

/** Dạng thuần JSON của event, thứ thực sự đi qua queue/Redis */
export interface EventEnvelope {
  eventId: string;
  eventName: string;
  /** ISO 8601 */
  occurredAt: string;
  data: Record<string, unknown>;
}

/**
 * "Hợp đồng" của một class event (phần static). Mỗi event class PHẢI có:
 *   static readonly eventName = "user.registered";
 *   static fromPrimitives(data, meta) { ... }   // dựng lại instance từ dữ liệu thuần
 */
export interface EventClass<E extends DomainEvent = DomainEvent> {
  readonly eventName: string;
  fromPrimitives(data: Record<string, unknown>, meta: Required<EventMeta>): E;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  new(...args: any[]): E;
}

/**
 * Sự kiện nghiệp vụ đã xảy ra (tên ở thì quá khứ: UserRegistered, OrderPaid...).
 * Bất biến, mang dữ liệu thuần; khi qua queue sẽ được dựng lại bằng fromPrimitives()
 * nên listener luôn nhận được instance thật (có đủ method), không phải object JSON trơ.
 */
export abstract class DomainEvent {
  readonly eventId: string;
  readonly occurredAt: Date;

  constructor(meta: EventMeta = {}) {
    this.eventId = meta.eventId ?? randomUUID();
    this.occurredAt = meta.occurredAt ?? new Date();
  }

  /** Lấy từ static eventName của class con, chỉ khai báo một chỗ */
  get eventName(): string {
    const name = (this.constructor as unknown as { eventName?: string }).eventName;
    if (!name) throw new Error(`${this.constructor.name} thiếu "static readonly eventName"`);
    return name;
  }

  /** Dữ liệu thuần JSON-safe (string, number, boolean, null, mảng, object). Không đưa class instance vào đây */
  abstract toPrimitives(): Record<string, unknown>;

  toEnvelope(): EventEnvelope {
    return {
      eventId: this.eventId,
      eventName: this.eventName,
      occurredAt: this.occurredAt.toISOString(),
      data: this.toPrimitives(),
    };
  }
}
