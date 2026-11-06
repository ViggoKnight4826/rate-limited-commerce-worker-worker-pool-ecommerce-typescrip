import { z } from "zod";

export const checkoutRequestSchema = z.object({
  orderId: z.string().min(1),
  customerId: z.string().min(1),
  email: z.string().email(),
  items: z.array(z.object({ sku: z.string().min(1), quantity: z.number().int().positive() })).min(1)
}).strict();

const jobBase = {
  orderId: z.string().min(1),
  customerId: z.string().min(1),
  email: z.string().email()
};

export const commerceJobSchema = z.discriminatedUnion("kind", [
  z.object({ ...jobBase, kind: z.literal("checkout"), items: checkoutRequestSchema.shape.items }),
  z.object({ ...jobBase, kind: z.literal("fulfillment"), items: checkoutRequestSchema.shape.items }),
  z.object({ ...jobBase, kind: z.literal("receipt"), receiptNumber: z.string().min(1) }),
  z.object({ ...jobBase, kind: z.literal("customer_order_update"), status: z.literal("fulfilled") })
]);

export const queueMessageSchema = z.object({
  message_id: z.string().min(1),
  payload: commerceJobSchema
});

export type CheckoutRequest = z.infer<typeof checkoutRequestSchema>;
export type CommerceJob = z.infer<typeof commerceJobSchema>;
export type QueueMessage = z.infer<typeof queueMessageSchema>;

export function checkoutJob(input: CheckoutRequest): CommerceJob {
  return { kind: "checkout", ...input };
}

export function nextJobs(job: CommerceJob): CommerceJob[] {
  if (job.kind === "checkout") {
    return [{ ...job, kind: "fulfillment" }];
  }
  if (job.kind === "fulfillment") {
    const common = { orderId: job.orderId, customerId: job.customerId, email: job.email };
    return [
      { ...common, kind: "receipt", receiptNumber: `rcpt-${job.orderId}` },
      { ...common, kind: "customer_order_update", status: "fulfilled" }
    ];
  }
  return [];
}
