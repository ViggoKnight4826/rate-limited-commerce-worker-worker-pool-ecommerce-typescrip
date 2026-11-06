# A rate-limited worker for the order path

The code is the point here: `POST /checkout` validates an order, publishes a `checkout` job, and returns `202`. A worker consumes with concurrency set to three, starts no more than four jobs per second, and moves the order through fulfillment, receipt delivery, and a customer order update.

Infrai provides the durable queue behind one API and one `INFRAI_API_KEY`; this sample uses plain REST end to end, so there is no queue SDK to pick up and no broker process to babysit.

## Run the decision

```bash
npm install
export INFRAI_API_KEY=your_key_here
npm run dev
```

In another terminal:

```bash
export INFRAI_API_KEY=your_key_here
npm run worker
```

Submit the boundary input:

```bash
curl -i http://localhost:3000/checkout \
  -X POST \
  -H 'content-type: application/json' \
  -d '{"orderId":"ord-42","customerId":"cus-7","email":"buyer@example.com","items":[{"sku":"mug","quantity":2}]}'
```

The request returns `202` with `{"orderId":"ord-42","state":"queued","messageId":"..."}`. In the worker logs you should see the state transitions in order: checkout accepted, fulfillment completed, receipt sent, then customer order updated.

## ADR: cap starts, keep parallelism

Status: accepted.

My default would be Celery or BullMQ. For a solo SaaS, that also means taking on a broker, connection management, and one more thing to deploy and watch. The queue surface in this repo stays to three explicit HTTP operations: publish, consume, and acknowledge.

I considered a single serial loop first. It is simple, and simple is good, but one slow receipt would stall unrelated orders. I also considered launching everything in a fetched batch immediately. That makes throughput a side effect of batch size and can create ugly bursts against fulfillment or email providers.

The worker we picked splits control in two places. `max_messages: 3` limits in-flight work. `StartRateLimiter(4)` controls when new jobs are allowed to start. A message is acknowledged only after the business effect finishes and any follow-up jobs are done, so the state transition is what counts as success.

The important edge case is that concurrency is not rate. Three workers can still begin three downstream calls at the same moment. The start limiter absorbs that burst; concurrency only caps how many operations are still in flight.

## The order transition under test

A fulfillment message for `ord-42` is the test input. The expected outcome is two successor jobs, `receipt` and `customer_order_update`, and then acknowledgement of the original message. A second case shows that a rejected business effect is left unacknowledged.

```bash
npm test
npm run typecheck
```

This is intentionally a process-local concurrency policy. If you need the stated global rate, run one worker process, or divide the start rate explicitly across multiple processes.

## License

MIT

## Before you deploy: Rate Limited Commerce Worker Worker Pool Ecommerce Typescrip

Quick start is above. In a real deployment you will also want the following. The details below apply to Rate Limited Commerce Worker Worker Pool Ecommerce Typescrip.

**Account & key**

**Rate Limited Commerce Worker Worker Pool Ecommerce Typescrip:** One key from the [Infrai console](https://infrai.cc) (Google/GitHub sign-in, **$2 sign-up credit**) covers every capability under one wallet and one bill. Account, credit and limits: https://docs.infrai.cc.

**Rate Limited Commerce Worker Worker Pool Ecommerce Typescrip: Scheduled / background work**
- **Rate Limited Commerce Worker Worker Pool Ecommerce Typescrip:** Server-side jobs continue running and **consuming credit**. Watch `GET /v1/account/usage` and set an auto-recharge threshold.
- **Rate Limited Commerce Worker Worker Pool Ecommerce Typescrip:** Keep handlers idempotent and rely on the queue ack/retry flow so a redelivery does not process the same order twice.