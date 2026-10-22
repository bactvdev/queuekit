import { DomainEvent } from "./DomainEvent";

/** Aggregate ghi nhận event khi trạng thái đổi; application service lấy ra rồi publish sau khi lưu */
export abstract class AggregateRoot {
  private domainEvents: DomainEvent[] = [];

  protected record(event: DomainEvent): void {
    this.domainEvents.push(event);
  }

  /** Lấy và xoá toàn bộ event đã ghi nhận */
  pullDomainEvents(): DomainEvent[] {
    const events = this.domainEvents;
    this.domainEvents = [];
    return events;
  }
}
