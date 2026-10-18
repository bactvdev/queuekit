import Redis, { RedisOptions } from "ioredis";
import { JobAbortedError } from "../domain/errors";
import { JobExecutor, QueueDriver } from "../domain/QueueDriver";
import { BackoffType, ExecutionResult, JobEnvelope, PushOptions, PushResult, QueueStats } from "../domain/types";

export interface RedisQueueDriverOptions {
  /** Tên hàng đợi trong Redis. Mặc định "default" */
  queue?: string;
  /** Redis URL, RedisOptions hoặc instance Redis có sẵn */
  connection?: string | RedisOptions | Redis;
  /** Số job chạy song song. Mặc định 5 */
  concurrency?: number;
  /** Thời gian lock job (ms); heartbeat gia hạn mỗi lockDuration/3. Mặc định 30s */
  lockDuration?: number;
  /** Chu kỳ poll job mới + promote job delayed (ms). Mặc định 300 */
  pollInterval?: number;
  /** Chu kỳ quét job stalled (ms). Mặc định 15s */
  stalledInterval?: number;
  /** Giữ job completed bao lâu (giây). 0 = xoá ngay. Mặc định 3600 */
  completedTtlSec?: number;
  onError?: (err: Error) => void;
}

/* ---------------- Lua scripts (atomic) ---------------- */

/** wait -> active, tăng attempts */
const LUA_FETCH = `
local id = redis.call('RPOP', KEYS[1])
if not id then return nil end
redis.call('ZADD', KEYS[2], tonumber(ARGV[1]) + tonumber(ARGV[2]), id)
local jk = KEYS[3] .. id
redis.call('HINCRBY', jk, 'attempts', 1)
redis.call('HSET', jk, 'status', 'active')
return id
`;

/** delayed (đến hạn) -> wait */
const LUA_PROMOTE = `
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
for _, id in ipairs(ids) do
  redis.call('ZREM', KEYS[1], id)
  redis.call('HSET', KEYS[3] .. id, 'status', 'waiting')
  redis.call('LPUSH', KEYS[2], id)
end
return #ids
`;

/** active (lock hết hạn) -> wait, hoặc dead nếu hết lượt */
const LUA_STALLED = `
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 100)
for _, id in ipairs(ids) do
  redis.call('ZREM', KEYS[1], id)
  local jk = KEYS[4] .. id
  local attempts = tonumber(redis.call('HGET', jk, 'attempts')) or 0
  local max = tonumber(redis.call('HGET', jk, 'maxAttempts')) or 1
  if attempts >= max then
    redis.call('ZADD', KEYS[3], ARGV[1], id)
    redis.call('HSET', jk, 'status', 'failed', 'error', 'Job stalled (worker crashed or lost its lock)')
  else
    redis.call('HSET', jk, 'status', 'waiting', 'error', 'Job stalled (worker crashed or lost its lock)')
    redis.call('LPUSH', KEYS[2], id)
  end
end
return #ids
`;

type ScriptedRedis = Redis & {
  jqFetch(wait: string, active: string, jobPrefix: string, now: number, lockMs: number): Promise<string | null>;
  jqPromote(delayed: string, wait: string, jobPrefix: string, now: number, limit: number): Promise<number>;
  jqStalled(active: string, wait: string, dead: string, jobPrefix: string, now: number): Promise<number>;
};

interface RunningEntry {
  ac: AbortController;
  requeued: boolean;
}

/**
 * Queue bền vững trên Redis. Có thể tạo nhiều instance với `queue` khác nhau
 * (ví dụ "default", "critical") để tách worker theo mức ưu tiên.
 *
 * Cấu trúc key (prefix `jq:{queue}:`):
 *   wait (LIST) | delayed (ZSET) | active (ZSET, score = hạn lock) | dead (ZSET) | job:{id} (HASH)
 *
 * Lưu ý: Lua ghép key động nên chưa tương thích Redis Cluster (cần hash tag).
 */
export class RedisQueueDriver implements QueueDriver {
  private readonly redis: ScriptedRedis;
  private readonly ownsConnection: boolean;
  private readonly prefix: string;
  private readonly concurrency: number;
  private readonly lockDuration: number;
  private readonly pollInterval: number;
  private readonly stalledInterval: number;
  private readonly completedTtlSec: number;
  private readonly onError: (err: Error) => void;

  private executor?: JobExecutor;
  private started = false;
  private pumping = false;
  private timers: NodeJS.Timeout[] = [];
  private readonly running = new Map<string, RunningEntry>();

  constructor(opts: RedisQueueDriverOptions = {}) {
    this.prefix = `jq:${opts.queue ?? "default"}:`;
    this.concurrency = opts.concurrency ?? 5;
    this.lockDuration = opts.lockDuration ?? 30_000;
    this.pollInterval = opts.pollInterval ?? 300;
    this.stalledInterval = opts.stalledInterval ?? 15_000;
    this.completedTtlSec = opts.completedTtlSec ?? 3600;
    this.onError = opts.onError ?? ((e) => console.error(`[RedisQueueDriver ${this.prefix}]`, e));

    const conn = opts.connection;
    if (conn instanceof Redis) {
      this.redis = conn as ScriptedRedis;
      this.ownsConnection = false;
    } else {
      this.redis = (typeof conn === "string" ? new Redis(conn) : new Redis(conn ?? {})) as ScriptedRedis;
      this.ownsConnection = true;
    }
    this.redis.defineCommand("jqFetch", { numberOfKeys: 3, lua: LUA_FETCH });
    this.redis.defineCommand("jqPromote", { numberOfKeys: 3, lua: LUA_PROMOTE });
    this.redis.defineCommand("jqStalled", { numberOfKeys: 4, lua: LUA_STALLED });
  }

  private k(s: "wait" | "delayed" | "active" | "dead") {
    return this.prefix + s;
  }
  private get jobPrefix() {
    return `${this.prefix}job:`;
  }
  private jobKey(id: string) {
    return this.jobPrefix + id;
  }

  attach(executor: JobExecutor): void {
    this.executor = executor;
  }

  /* ------------------------- producer ------------------------- */

  async push(job: JobEnvelope, { delay = 0 }: PushOptions = {}): Promise<PushResult> {
    const key = this.jobKey(job.id);
    // Trùng jobId => bỏ qua (idempotent)
    if (!(await this.redis.hsetnx(key, "id", job.id))) return { jobId: job.id };

    const multi = this.redis.multi();
    multi.hset(key, {
      id: job.id,
      task: job.task,
      payload: JSON.stringify(job.payload ?? null),
      attempts: 0,
      maxAttempts: job.maxAttempts,
      timeout: job.timeout,
      backoffType: job.backoff.type,
      backoffDelay: job.backoff.delay,
      createdAt: job.createdAt,
      connection: job.connection,
      status: delay > 0 ? "delayed" : "waiting",
    });
    if (delay > 0) multi.zadd(this.k("delayed"), Date.now() + delay, job.id);
    else multi.lpush(this.k("wait"), job.id);
    await multi.exec();
    return { jobId: job.id };
  }

  /* ------------------------- worker ------------------------- */

  async startWorker(): Promise<void> {
    if (!this.executor) throw new Error("RedisQueueDriver chưa được attach vào QueueManager");
    if (this.started) return;
    this.started = true;
    this.timers.push(
      setInterval(() => void this.tick(), this.pollInterval),
      setInterval(() => void this.recoverStalled(), this.stalledInterval),
      setInterval(() => void this.heartbeat(), Math.max(1000, Math.floor(this.lockDuration / 3))),
    );
    void this.recoverStalled(); // dọn job mồ côi từ lần chạy trước
    void this.tick();
  }

  private async tick() {
    if (!this.started) return;
    try {
      await this.redis.jqPromote(this.k("delayed"), this.k("wait"), this.jobPrefix, Date.now(), 100);
    } catch (e) {
      this.reportError(e);
    }
    await this.pump();
  }

  private async pump() {
    if (this.pumping) return;
    this.pumping = true;
    try {
      while (this.started && this.running.size < this.concurrency) {
        const id = await this.redis.jqFetch(
          this.k("wait"),
          this.k("active"),
          this.jobPrefix,
          Date.now(),
          this.lockDuration,
        );
        if (!id) break;
        const job = await this.loadJob(id);
        if (!job) {
          await this.redis.zrem(this.k("active"), id);
          continue;
        }
        void this.runJob(job);
      }
    } catch (e) {
      this.reportError(e);
    } finally {
      this.pumping = false;
    }
  }

  private async runJob(job: JobEnvelope) {
    const entry: RunningEntry = { ac: new AbortController(), requeued: false };
    this.running.set(job.id, entry);
    try {
      const res = await this.executor!.execute(job, entry.ac.signal);
      if (!entry.requeued) await this.settle(job, res);
    } catch (e) {
      this.reportError(e);
    } finally {
      this.running.delete(job.id);
      void this.pump(); // có slot trống, lấy job tiếp ngay
    }
  }

  /** Ghi kết quả thực thi vào Redis */
  private async settle(job: JobEnvelope, res: ExecutionResult) {
    if (res.status === "aborted") return; // shutdown / mất lock: đã có nơi khác xử lý
    // zrem = 0 nghĩa là job đã bị worker khác nhận lại (mất lock) => bỏ qua kết quả
    if (!(await this.redis.zrem(this.k("active"), job.id))) return;

    const key = this.jobKey(job.id);
    switch (res.status) {
      case "completed":
        if (this.completedTtlSec <= 0) {
          await this.redis.del(key);
        } else {
          await this.redis
            .multi()
            .hset(key, {
              status: "completed",
              result: JSON.stringify(res.result ?? null),
              finishedAt: Date.now(),
            })
            .expire(key, this.completedTtlSec)
            .exec();
        }
        break;
      case "retry":
        await this.redis
          .multi()
          .hset(key, { status: "delayed", error: res.error.message })
          .zadd(this.k("delayed"), Date.now() + res.delayMs, job.id)
          .exec();
        break;
      case "failed":
        await this.redis
          .multi()
          .hset(key, { status: "failed", error: res.error.message, finishedAt: Date.now() })
          .zadd(this.k("dead"), Date.now(), job.id)
          .exec();
        break;
    }
  }

  /** Gia hạn lock các job đang chạy; mất lock thì abort handler */
  private async heartbeat() {
    if (this.running.size === 0) return;
    const expiry = Date.now() + this.lockDuration;
    for (const [id, entry] of this.running) {
      try {
        // XX: chỉ cập nhật nếu còn trong active; CH: trả về số phần tử thay đổi
        const changed = await this.redis.zadd(this.k("active"), "XX", "CH", expiry, id);
        if (changed === 0) entry.ac.abort(new JobAbortedError("Lost job lock"));
      } catch (e) {
        this.reportError(e);
      }
    }
  }

  /** Phục hồi job có lock hết hạn (worker chết) */
  private async recoverStalled() {
    if (!this.started) return;
    try {
      await this.redis.jqStalled(this.k("active"), this.k("wait"), this.k("dead"), this.jobPrefix, Date.now());
    } catch (e) {
      this.reportError(e);
    }
  }

  async stopWorker(graceMs = 10_000): Promise<void> {
    this.started = false;
    this.timers.forEach(clearInterval);
    this.timers = [];

    const deadline = Date.now() + graceMs;
    while (this.running.size > 0 && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 100));
    }

    // Job còn dang dở: abort và trả lại đầu hàng đợi, không tốn lượt retry
    for (const [id, entry] of [...this.running]) {
      entry.requeued = true;
      entry.ac.abort(new JobAbortedError("Worker shutting down"));
      try {
        if (await this.redis.zrem(this.k("active"), id)) {
          await this.redis
            .multi()
            .hincrby(this.jobKey(id), "attempts", -1)
            .hset(this.jobKey(id), "status", "waiting")
            .rpush(this.k("wait"), id)
            .exec();
        }
      } catch (e) {
        this.reportError(e);
      }
    }
    this.running.clear();
  }

  async close(): Promise<void> {
    await this.stopWorker(0);
    if (this.ownsConnection) await this.redis.quit();
  }

  /* ------------------------- management ------------------------- */

  async getStats(): Promise<QueueStats> {
    const [waiting, delayed, active, failed] = await Promise.all([
      this.redis.llen(this.k("wait")),
      this.redis.zcard(this.k("delayed")),
      this.redis.zcard(this.k("active")),
      this.redis.zcard(this.k("dead")),
    ]);
    return { waiting, delayed, active, failed };
  }

  /** Id các job trong dead letter queue (mới nhất trước) */
  async getFailedIds(limit = 100): Promise<string[]> {
    return this.redis.zrevrange(this.k("dead"), 0, limit - 1);
  }

  /** Đưa job từ dead letter queue về hàng đợi để chạy lại (reset attempts) */
  async retryFailed(jobId: string): Promise<boolean> {
    if (!(await this.redis.zrem(this.k("dead"), jobId))) return false;
    await this.redis
      .multi()
      .hset(this.jobKey(jobId), { status: "waiting", attempts: 0 })
      .hdel(this.jobKey(jobId), "error")
      .lpush(this.k("wait"), jobId)
      .exec();
    return true;
  }

  private async loadJob(id: string): Promise<JobEnvelope | null> {
    const h = await this.redis.hgetall(this.jobKey(id));
    if (!h.id) return null;
    return {
      id: h.id,
      task: h.task,
      payload: JSON.parse(h.payload),
      attempts: Number(h.attempts),
      maxAttempts: Number(h.maxAttempts),
      timeout: Number(h.timeout),
      backoff: { type: (h.backoffType as BackoffType) ?? "fixed", delay: Number(h.backoffDelay) },
      createdAt: Number(h.createdAt),
      connection: h.connection,
    };
  }

  private reportError(e: unknown) {
    this.onError(e instanceof Error ? e : new Error(String(e)));
  }
}
