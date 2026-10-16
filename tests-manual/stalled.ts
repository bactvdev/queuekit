import Redis from "ioredis";
import { QueueManager, RedisQueueDriver, Task } from "../src/shared/queue";

class PingTask extends Task<{ x: number }, void> {
  readonly name = "t.ping";
  async handle(p: { x: number }) { console.log("handled by recovered worker, x =", p.x); }
}
(async () => {
  const redis = new Redis();
  const queue = "stall" + Date.now();
  const driver = new RedisQueueDriver({ queue, connection: redis, pollInterval: 50, stalledInterval: 200, lockDuration: 600 });
  const q = new QueueManager({ default: "r", connections: { r: driver } });
  q.registerTask(new PingTask());
  await q.dispatch(PingTask, { x: 42 }).withId("job-stall");
  // Giả lập worker chết: lấy job sang active với lock 300ms rồi "biến mất"
  const id = await redis.rpop(`jq:${queue}:wait`);
  await redis.zadd(`jq:${queue}:active`, Date.now() + 300, id!);
  await redis.hincrby(`jq:${queue}:job:${id}`, "attempts", 1);
  console.log("simulated crash, stats:", JSON.stringify(await q.getStats()));
  await q.startWorkers();
  await new Promise((r) => setTimeout(r, 1500));
  console.log("final stats:", JSON.stringify(await q.getStats()));
  await q.shutdown(200); await redis.quit();
})();
