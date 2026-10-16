import { createQueueManager } from "./queue";

/**
 * Entry point của process worker (chạy riêng khỏi web server):
 *   node dist/bootstrap/worker.js
 * Chỉ consume các connection cần thiết (sync không cần worker).
 */
async function main() {
  const queue = createQueueManager({
    mailer: { async send() {} }, // thay bằng implement thật
    auditLogs: { async append() {} },
  });

  queue.on("error", (err) => console.error("[queue] error:", err));
  await queue.startWorkers(["redis", "redis-critical", "memory"]);
  console.log("Worker đang chạy...");

  for (const sig of ["SIGINT", "SIGTERM"] as const) {
    process.on(sig, async () => {
      console.log(`Nhận ${sig}, đang dừng worker...`);
      await queue.shutdown(10_000);
      process.exit(0);
    });
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
