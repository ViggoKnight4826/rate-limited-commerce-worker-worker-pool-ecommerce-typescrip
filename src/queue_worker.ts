import { commerceJobSchema, nextJobs, queueMessageSchema, type CommerceJob, type QueueMessage } from "./commerce_jobs.js";
import { infrai, type QueueClient } from "./infrai_queue.js";

export type JobEffect = (job: CommerceJob) => Promise<void>;

export class StartRateLimiter {
  private nextStart = 0;
  private readonly spacingMs: number;

  constructor(startsPerSecond: number) {
    if (startsPerSecond <= 0) throw new Error("startsPerSecond must be positive");
    this.spacingMs = 1_000 / startsPerSecond;
  }

  async wait(): Promise<void> {
    const now = Date.now();
    const startAt = Math.max(now, this.nextStart);
    this.nextStart = startAt + this.spacingMs;
    if (startAt > now) await new Promise((resolve) => setTimeout(resolve, startAt - now));
  }
}

export async function applyJob(job: CommerceJob): Promise<void> {
  switch (job.kind) {
    case "checkout":
      console.log("checkout accepted", { orderId: job.orderId, itemCount: job.items.length });
      return;
    case "fulfillment":
      console.log("fulfillment completed", { orderId: job.orderId, units: job.items.reduce((n, item) => n + item.quantity, 0) });
      return;
    case "receipt":
      console.log("receipt sent", { orderId: job.orderId, receiptNumber: job.receiptNumber, email: job.email });
      return;
    case "customer_order_update":
      console.log("customer order updated", { orderId: job.orderId, customerId: job.customerId, status: job.status });
  }
}

export async function processMessage(
  message: QueueMessage,
  client: QueueClient,
  effect: JobEffect = applyJob
): Promise<void> {
  await effect(message.payload);
  const successors = nextJobs(message.payload);
  await Promise.all(successors.map((payload) =>
    client.queue.publish(
      { payload: commerceJobSchema.parse(payload) },
      `${message.message_id}:${payload.kind}`
    )
  ));
  await client.queue.ack({ message_id: message.message_id }, `ack:${message.message_id}`);
}

export async function runWorker(
  client: QueueClient = infrai,
  options: { concurrency: number; startsPerSecond: number } = { concurrency: 3, startsPerSecond: 4 }
): Promise<never> {
  const limiter = new StartRateLimiter(options.startsPerSecond);

  while (true) {
    const batch = await client.queue.consume({
      max_messages: options.concurrency,
      visibility_timeout: 30
    });
    const messages = batch.messages.map((value) => queueMessageSchema.parse(value));
    if (messages.length === 0) {
      await new Promise((resolve) => setTimeout(resolve, 500));
      continue;
    }
    await Promise.all(messages.map(async (message) => {
      await limiter.wait();
      await processMessage(message, client);
    }));
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  runWorker().catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 1;
  });
}
