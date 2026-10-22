import { JobOptions, Task, TaskContext } from "../../queue";
import { DomainEvent, EventClass, EventEnvelope } from "../domain/DomainEvent";
import { DomainEventListener, ListenerContext } from "../domain/DomainEventListener";

/**
 * Adapter: bọc 1 (event, listener) thành một Task của module queue.
 * Payload của job là EventEnvelope (JSON thuần); khi chạy sẽ dựng lại event
 * bằng EventClass.fromPrimitives() rồi mới gọi listener.
 */
export class ListenerJobTask<E extends DomainEvent> extends Task<EventEnvelope, void> {
  readonly name: string;
  override readonly connection?: string;
  override readonly options: JobOptions;

  constructor(
    private readonly eventClass: EventClass<E>,
    private readonly listener: DomainEventListener<E>,
    defaultConnection?: string,
  ) {
    super();
    // Gồm cả tên event để 1 listener có thể subscribe nhiều event khác nhau
    this.name = `event-listener:${eventClass.eventName}:${listener.name}`;
    this.connection = listener.connection ?? defaultConnection;
    this.options = { ...listener.options };
  }

  async handle(envelope: EventEnvelope, ctx: TaskContext): Promise<void> {
    await this.listener.handle(this.rebuild(envelope), this.toListenerContext(envelope, ctx));
  }

  override shouldRetry(error: Error): boolean {
    return this.listener.shouldRetry(error);
  }

  override async onFailed(envelope: EventEnvelope, error: Error, ctx: TaskContext): Promise<void> {
    await this.listener.onFailed(this.rebuild(envelope), error, this.toListenerContext(envelope, ctx));
  }

  private rebuild(envelope: EventEnvelope): E {
    return this.eventClass.fromPrimitives(envelope.data, {
      eventId: envelope.eventId,
      occurredAt: new Date(envelope.occurredAt),
    });
  }

  private toListenerContext(envelope: EventEnvelope, ctx: TaskContext): ListenerContext {
    return {
      eventId: envelope.eventId,
      listenerName: this.listener.name,
      attempt: ctx.attempt,
      maxAttempts: ctx.maxAttempts,
      signal: ctx.signal,
    };
  }
}
