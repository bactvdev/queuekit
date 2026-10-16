# Queue module (DDD, Ports & Adapters)

```
src/shared/queue/
  domain/            Task, QueueDriver (port), TaskDispatcher (port), PendingDispatch, RetryPolicy, types, errors
  application/       TaskRegistry, JobRunner (timeout + retry), QueueManager (facade)
  infrastructure/    SyncQueueDriver, InMemoryQueueDriver, RedisQueueDriver (adapters)
src/modules/user/application/   ví dụ Task + Application Service
src/bootstrap/                  composition root (queue.ts) + entry của worker (worker.ts)
tests-manual/                   smoke test chạy tay (cần redis ở localhost:6379)
```

Thêm loại queue mới (SQS, RabbitMQ, DB...): implement `QueueDriver`, rồi thêm vào `connections` trong `bootstrap/queue.ts`.
