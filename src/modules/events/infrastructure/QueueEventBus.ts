import { Task, TaskClass, TaskDispatcher } from "../../queue";
import { DomainEvent, EventClass, EventEnvelope } from "../domain/DomainEvent";
import { DomainEventListener } from "../domain/DomainEventListener";
import { EventBus, PublishResult } from "../domain/EventBus";
import { DuplicateListenerError, EventPublishError, EventPublishFailure } from "../domain/errors";
import { ListenerJobTask } from "./ListenerJobTask";

/**
 * Phần của QueueManager mà event bus cần: dispatch task + đăng ký task.
 * QueueManager của module queue thoả interface này sẵn.
 */
export interface TaskQueue extends TaskDispatcher {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  registerTask(task: Task<any, any>): unknown;
}

export interface QueueEventBusOptions {
  /** Connection mặc định cho listener không tự chỉ định. Bỏ trống = default của QueueManager */
  defaultConnection?: string;
}

interface Subscription {
  listenerName: string;
  jobClass: TaskClass<EventEnvelope, void>;
}

/**
 * EventBus đẩy MỖI listener thành 1 job trong queue:
 *   publish(event) ──► [job: listenerA] [job: listenerB] [job: listenerC]
 * => mỗi listener có retry/timeout/connection riêng và lỗi của listener này không ảnh hưởng listener khác.
 *
 * QUAN TRỌNG: subscribe() phải được gọi ở CẢ process phát event lẫn process worker
 * (cùng composition root), trước khi queue.startWorkers().
 */
export class QueueEventBus implements EventBus {
  private readonly subscriptions = new Map<string, Subscription[]>();

  constructor(
    private readonly queue: TaskQueue,
    private readonly options: QueueEventBusOptions = {},
  ) { }

  subscribe<E extends DomainEvent>(eventClass: EventClass<E>, ...listeners: DomainEventListener<E>[]): void {
    for (const listener of listeners) {
      const subs = this.subscriptions.get(eventClass.eventName) ?? [];
      if (subs.some((s) => s.listenerName === listener.name)) {
        throw new DuplicateListenerError(eventClass.eventName, listener.name);
      }

      // QueueManager tra Task theo CLASS, nên mỗi subscription cần một class riêng:
      // `class extends ...` được đánh giá lại mỗi lần gọi => mỗi lần là một constructor khác nhau.
      const JobClass = class extends ListenerJobTask<E> { };
      this.queue.registerTask(new JobClass(eventClass, listener, this.options.defaultConnection));

      subs.push({ listenerName: listener.name, jobClass: JobClass });
      this.subscriptions.set(eventClass.eventName, subs);
    }
  }

  async publish(event: DomainEvent): Promise<PublishResult> {
    const failures = await this.dispatchToListeners(event);
    if (failures.length > 0) throw new EventPublishError(failures);
    return this.resultOf(event);
  }

  /** Publish lần lượt từng event (giữ thứ tự đẩy vào queue); gom lỗi rồi ném một lần ở cuối */
  async publishAll(events: DomainEvent[]): Promise<PublishResult[]> {
    const results: PublishResult[] = [];
    const failures: EventPublishFailure[] = [];
    for (const event of events) {
      const f = await this.dispatchToListeners(event);
      failures.push(...f);
      results.push(this.resultOf(event));
    }
    if (failures.length > 0) throw new EventPublishError(failures);
    return results;
  }

  /** Số listener đang subscribe một event (tiện cho test/debug) */
  listenerCount(eventClass: Pick<EventClass, "eventName">): number {
    return this.subscriptions.get(eventClass.eventName)?.length ?? 0;
  }

  private resultOf(event: DomainEvent): PublishResult {
    return {
      eventId: event.eventId,
      eventName: event.eventName,
      listeners: this.subscriptions.get(event.eventName)?.length ?? 0,
    };
  }

  private async dispatchToListeners(event: DomainEvent): Promise<EventPublishFailure[]> {
    const subs = this.subscriptions.get(event.eventName) ?? [];
    if (subs.length === 0) return [];

    // Một vòng JSON: lỗi dữ liệu (BigInt, vòng tham chiếu) lộ ngay lúc publish,
    // và mỗi listener nhận bản sao riêng (kể cả khi chạy sync/memory)
    const wire = JSON.stringify(event.toEnvelope());

    const settled = await Promise.allSettled(
      subs.map((s) =>
        this.queue
          .dispatch(s.jobClass, JSON.parse(wire) as EventEnvelope)
          // Chống trùng: publish lại cùng 1 event không chạy lại cùng 1 listener (với Redis)
          .withId(`${event.eventId}:${s.listenerName}`)
          .send(),
      ),
    );

    const failures: EventPublishFailure[] = [];
    settled.forEach((r, i) => {
      if (r.status === "rejected") {
        failures.push({
          eventName: event.eventName,
          eventId: event.eventId,
          listener: subs[i].listenerName,
          error: r.reason instanceof Error ? r.reason : new Error(String(r.reason)),
        });
      }
    });
    return failures;
  }
}
