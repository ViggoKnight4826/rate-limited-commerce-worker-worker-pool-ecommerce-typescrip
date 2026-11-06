import assert from "node:assert/strict";
import test from "node:test";
import type { CommerceJob } from "../src/commerce_jobs.js";
import type { QueueClient } from "../src/infrai_queue.js";
import { processMessage } from "../src/queue_worker.js";

test("fulfillment queues the receipt and customer update before acknowledging", async () => {
  const events: string[] = [];
  const published: CommerceJob[] = [];
  const client = {
    queue: {
      publish: async ({ payload }: { payload: unknown }) => {
        const job = payload as CommerceJob;
        published.push(job);
        events.push(`publish:${job.kind}`);
        return { message_id: `next-${job.kind}` };
      },
      consume: async () => ({ messages: [] }),
      ack: async ({ message_id }: { message_id: string }) => {
        events.push(`ack:${message_id}`);
        return {};
      }
    }
  } as QueueClient;

  await processMessage({
    message_id: "msg-42",
    payload: {
      kind: "fulfillment",
      orderId: "ord-42",
      customerId: "cus-7",
      email: "buyer@example.com",
      items: [{ sku: "mug", quantity: 2 }]
    }
  }, client, async () => {
    events.push("effect:fulfillment");
  });

  assert.deepEqual(published.map((job) => job.kind), ["receipt", "customer_order_update"]);
  assert.deepEqual(events, [
    "effect:fulfillment",
    "publish:receipt",
    "publish:customer_order_update",
    "ack:msg-42"
  ]);
});

test("a failed business effect leaves the message unacknowledged", async () => {
  let acknowledged = false;
  const client = {
    queue: {
      publish: async () => ({ message_id: "unused" }),
      consume: async () => ({ messages: [] }),
      ack: async () => {
        acknowledged = true;
        return {};
      }
    }
  } as QueueClient;

  await assert.rejects(
    processMessage({
      message_id: "msg-43",
      payload: {
        kind: "receipt",
        orderId: "ord-43",
        customerId: "cus-8",
        email: "buyer@example.com",
        receiptNumber: "rcpt-ord-43"
      }
    }, client, async () => { throw new Error("mail provider rejected send"); })
  );
  assert.equal(acknowledged, false);
});
