import { DomainEvent, EventClass } from "./DomainEvent";
import { DomainEventListener } from "./DomainEventListener";

export interface PublishResult {
  eventId: string;
  eventName: string;
  /** Số listener đã được đẩy vào queue */
  listeners: number;
}

/** Cho phía phát event (application service) chỉ cần port này */
export interface EventPublisher {
  publish(event: DomainEvent): Promise<PublishResult>;
  publishAll(events: DomainEvent[]): Promise<PublishResult[]>;
}

/** Cho composition root đăng ký listener */
export interface EventSubscriber {
  /** 1 event có thể có nhiều listener: subscribe(Event, a, b, c) hoặc gọi nhiều lần */
  subscribe<E extends DomainEvent>(eventClass: EventClass<E>, ...listeners: DomainEventListener<E>[]): void;
}

export interface EventBus extends EventPublisher, EventSubscriber { }
