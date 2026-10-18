import { UnknownTaskError } from "../domain/errors";
import { Task, TaskClass } from "../domain/Task";

/** Nơi đăng ký các Task (đã inject dependency) để tra theo class hoặc theo tên */
export class TaskRegistry {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private readonly byName = new Map<string, Task<any, any>>();
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private readonly byClass = new Map<unknown, Task<any, any>>();

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  register(task: Task<any, any>): this {
    if (this.byName.has(task.name)) {
      throw new Error(`Task "${task.name}" đã được đăng ký`);
    }
    this.byName.set(task.name, task);
    this.byClass.set(task.constructor, task);
    return this;
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  resolveByName(name: string): Task<any, any> {
    const task = this.byName.get(name);
    if (!task) throw new UnknownTaskError(name);
    return task;
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  resolveByClass(taskClass: TaskClass): Task<any, any> {
    const task = this.byClass.get(taskClass);
    if (!task) throw new UnknownTaskError(taskClass.name);
    return task;
  }
}
