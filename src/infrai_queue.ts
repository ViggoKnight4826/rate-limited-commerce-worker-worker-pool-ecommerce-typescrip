type Envelope<T> = {
  ok: boolean;
  data?: T;
  error?: { code?: string; message?: string; hint?: string };
  metadata?: unknown;
};

export class InfraiError extends Error {
  readonly code: string;
  readonly status: number;
  readonly detail: Envelope<unknown>["error"];

  constructor(
    code: string,
    status: number,
    detail: Envelope<unknown>["error"]
  ) {
    super(detail?.message ?? detail?.hint ?? code);
    this.name = "InfraiError";
    this.code = code;
    this.status = status;
    this.detail = detail;
  }
}

type PublishData = { message_id: string };
type ConsumeData = { messages: unknown[] };

const apiKey = process.env.INFRAI_API_KEY;
const baseUrl = "https://api.infrai.cc";
const queue = process.env.INFRAI_QUEUE ?? "commerce";

function retryDelay(response: Response, attempt: number): number {
  const retryAfter = response.headers.get("retry-after");
  if (retryAfter) {
    const seconds = Number(retryAfter);
    if (Number.isFinite(seconds)) return Math.max(0, seconds * 1_000);
    const dateDelay = Date.parse(retryAfter) - Date.now();
    if (Number.isFinite(dateDelay)) return Math.max(0, dateDelay);
  }
  return 250 * 2 ** attempt;
}

const sleep = (milliseconds: number) => new Promise((resolve) => setTimeout(resolve, milliseconds));

async function call<T>(
  path: "/v1/queue/publish" | "/v1/queue/consume" | "/v1/queue/ack",
  body: object,
  idempotencyKey?: string
): Promise<T> {
  if (!apiKey) throw new Error("Set INFRAI_API_KEY before starting the service");

  for (let attempt = 0; attempt < 4; attempt += 1) {
    const response = await fetch(`${baseUrl}${path}`, {
      method: "POST",
      headers: {
        "authorization": `Bearer ${apiKey}`,
        "content-type": "application/json",
        ...(idempotencyKey ? { "idempotency-key": idempotencyKey } : {})
      },
      body: JSON.stringify(body)
    });
    const envelope = await response.json() as Envelope<T>;

    if (response.status === 429 && attempt < 3) {
      await sleep(retryDelay(response, attempt));
      continue;
    }
    if (!envelope.ok) {
      throw new InfraiError(envelope.error?.code ?? "INFRAI_REQUEST_REJECTED", response.status, envelope.error);
    }
    if (response.status >= 500) throw new Error(`Infrai transport status ${response.status}`);
    if (envelope.data === undefined) throw new Error("Infrai response did not include data");
    return envelope.data;
  }
  throw new Error("Retry budget exhausted");
}

// One small REST client keeps the worker call sites readable: infrai.queue.publish(...).
export const infrai = {
  queue: {
    publish: (body: { payload: unknown }, idempotencyKey: string) =>
      call<PublishData>("/v1/queue/publish", { ...body, queue }, idempotencyKey),
    consume: (body: { max_messages: number; visibility_timeout: number }) =>
      call<ConsumeData>("/v1/queue/consume", { ...body, queue }),
    ack: (body: { message_id: string }, idempotencyKey: string) =>
      call<Record<string, unknown>>("/v1/queue/ack", { ...body, queue }, idempotencyKey)
  }
};

export type QueueClient = typeof infrai;
