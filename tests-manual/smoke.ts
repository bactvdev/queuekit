import { InMemoryQueueDriver, QueueManager, SyncQueueDriver, RedisQueueDriver, Task, UnrecoverableError, TaskContext } from "../src/shared/queue";

let flaky = 0;
class FlakyTask extends Task<{ n: number }, string> {
  readonly name = "t.flaky";
  override readonly options = { maxAttempts: 3, backoff: { type: "fixed" as const, delay: 50 } };
  async handle(p: { n: number }, ctx: TaskContext) { flaky++; if (ctx.attempt < 3) throw new Error("boom" + ctx.attempt); return "ok" + p.n; }
}
class SlowTask extends Task<{}, void> {
  readonly name = "t.slow";
  override readonly options = { timeout: 100, maxAttempts: 2, backoff: { type: "fixed" as const, delay: 20 } };
  failedCalled = 0;
  async handle(_: {}, ctx: TaskContext) { await new Promise((r, j) => { const t = setTimeout(r, 2000); ctx.signal.addEventListener("abort", () => { clearTimeout(t); j(ctx.signal.reason); }); }); }
  override async onFailed() { this.failedCalled++; }
}
class BadTask extends Task<{}, void> {
  readonly name = "t.bad";
  async handle() { throw new UnrecoverableError("nope"); }
}

async function run(label: string, extra: Record<string, any>) {
  const slow = new SlowTask();
  const mem = new InMemoryQueueDriver();
  const q = new QueueManager({ default: "memory", connections: { sync: new SyncQueueDriver({ retry: true }), memory: mem, ...extra } });
  q.registerTasks(new FlakyTask(), slow, new BadTask());
  const ev: string[] = [];
  q.on("retrying", (j) => ev.push("retry:" + j.task)); q.on("failed", (j, e) => ev.push("failed:" + j.task + ":" + e.name)); q.on("completed", (j) => ev.push("done:" + j.task));
  await q.startWorkers();
  flaky = 0;
  const conns = ["sync", "memory", ...Object.keys(extra)];
  for (const c of conns) {
    ev.length = 0; flaky = 0; slow.failedCalled = 0;
    const r = await q.dispatch(FlakyTask, { n: 1 }).onConnection(c).withId(`f-${label}-${c}`);
    await q.dispatch(SlowTask, {}).onConnection(c).withId(`s-${label}-${c}`).then(() => {}, () => {});
    await q.dispatch(BadTask, {}).onConnection(c).withId(`b-${label}-${c}`).then(() => {}, () => {});
    await q.dispatch(FlakyTask, { n: 2 }).onConnection(c).delay(c === "sync" ? 0 : 100).withId(`d-${label}-${c}`).then(() => {}, () => {});
    if (c === "memory") await mem.drain(); else await new Promise((r) => setTimeout(r, 1800));
    console.log(label, c, "result:", r.result, "flakyRuns:", flaky, "slow.onFailed:", slow.failedCalled, "\n   events:", ev.join(" "));
  }
  console.log(label, "stats", JSON.stringify(await q.getStats()));
  await q.shutdown(500);
}

(async () => {
  await run("A", {});
  if (process.env.WITH_REDIS) await run("R", { redis: new RedisQueueDriver({ queue: "t" + Date.now(), pollInterval: 50 }) });
})();
