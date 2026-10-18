/** Job chạy quá thời gian cho phép */
export class JobTimeoutError extends Error {
  constructor(
    public readonly jobId: string,
    public readonly timeoutMs: number,
  ) {
    super(`Job ${jobId} timed out after ${timeoutMs}ms`);
    this.name = "JobTimeoutError";
  }
}

/** Ném trong Task.handle() để job fail ngay, không retry */
export class UnrecoverableError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "UnrecoverableError";
  }
}

/** Task chưa được đăng ký trong TaskRegistry (không bao giờ retry được) */
export class UnknownTaskError extends UnrecoverableError {
  constructor(task: string) {
    super(`Task "${task}" chưa được đăng ký`);
    this.name = "UnknownTaskError";
  }
}

export class UnknownConnectionError extends Error {
  constructor(name: string) {
    super(`Queue connection "${name}" chưa được cấu hình`);
    this.name = "UnknownConnectionError";
  }
}

/** Job bị dừng từ bên ngoài (worker shutdown / mất lock), không phải lỗi của task */
export class JobAbortedError extends Error {
  constructor(message = "Job aborted") {
    super(message);
    this.name = "JobAbortedError";
  }
}
