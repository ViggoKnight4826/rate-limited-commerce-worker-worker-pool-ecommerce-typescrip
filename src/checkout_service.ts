import { createServer, type ServerResponse } from "node:http";
import { ZodError } from "zod";
import { checkoutJob, checkoutRequestSchema } from "./commerce_jobs.js";
import { infrai, InfraiError } from "./infrai_queue.js";

const port = Number(process.env.PORT ?? 3000);

function json(response: ServerResponse, status: number, body: unknown): void {
  response.writeHead(status, { "content-type": "application/json" });
  response.end(JSON.stringify(body));
}

createServer(async (request, response) => {
  if (request.method !== "POST" || request.url !== "/checkout") {
    json(response, 404, { error: "route_not_found" });
    return;
  }

  try {
    const chunks: Buffer[] = [];
    for await (const chunk of request) chunks.push(Buffer.from(chunk));
    const input = checkoutRequestSchema.parse(JSON.parse(Buffer.concat(chunks).toString("utf8")));
    const job = checkoutJob(input);
    const queued = await infrai.queue.publish({ payload: job }, `checkout:${input.orderId}`);
    json(response, 202, { orderId: input.orderId, state: "queued", messageId: queued.message_id });
  } catch (error) {
    if (error instanceof ZodError || error instanceof SyntaxError) {
      json(response, 400, { error: "invalid_checkout", details: error.message });
      return;
    }
    if (error instanceof InfraiError && error.status >= 400 && error.status < 500) {
      json(response, error.status, { error: error.code, details: error.message });
      return;
    }
    json(response, 502, { error: "queue_request_failed" });
  }
}).listen(port, () => console.log(`checkout service listening on http://localhost:${port}`));
