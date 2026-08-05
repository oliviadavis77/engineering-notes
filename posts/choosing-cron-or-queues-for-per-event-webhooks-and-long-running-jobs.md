# Choosing Cron or Queues for Per-Event Webhooks and Long-Running Jobs

Use cron when work belongs to a repeating clock, otherwise reach for a message queue when each event has its own delivery time. Short answer: delayed webhook tasks usually need durable per-event records and queue-style workers, while cron remains useful for periodic reconciliation; long-running jobs should leave the public HTTPS request path after a quick durable handoff.

That is the operating rule I put in the runbook. The scheduler decides when an item becomes eligible, the queue controls who attempts it, and the worker owns retries and idempotency. Combining those responsibilities in one HTTP handler makes recovery hard to reason about.

Keep the boundary plain.

## Should cron or a message queue schedule each delayed webhook event?

Start with the thing that owns the deadline. A clock owns work such as “reconcile overdue deliveries every five minutes.” An event owns work such as “try this webhook after its requested delivery time.” Cron expresses the first shape directly. For the second, store one durable record per event with a stable event ID, destination, payload reference, state, attempt count, and `due_at` timestamp. A dispatcher can then make due records available to workers. The underlying timer may be a broker feature, a database index, or a dedicated scheduler; the contract should stay the same.

I don't treat a database-backed dispatcher as a fake queue. It can be the smallest sound design when traffic is modest and the team already operates the database. The catch is that concurrent claiming, lease expiry, and hot indexes become your responsibility. A message broker is a better fit when dispatch rate must scale separately from scheduling, multiple worker groups consume the same class of work, or backpressure needs to be explicit. It still doesn't remove the need for application state: delivery systems can repeat messages, and business correctness lives above transport acknowledgement.

| Work shape | Primary mechanism | State that must survive | Main operational risk |
|---|---|---|---|
| Fixed recurring sweep | Cron | Last successful window | Missed or overlapping runs |
| Per-event delayed webhook | Durable schedule plus queue workers | Event ID, due time, attempt state | Duplicate delivery |
| Immediate webhook receipt | Public HTTPS ingress plus durable handoff | Verified event and receipt time | Doing work before acknowledgement |
| Long-running task | Queue worker with a lease and checkpoints | Progress, lease owner, result | Lease expiry during useful work |

Cron is not suitable when every event carries a different timestamp or when retrying one destination would rerun an entire batch. A queue alone is not suitable as the system of record when audit, cancellation, or rescheduling must be queryable. In that case I keep canonical schedule state in storage and treat the queue message as a claimable notification, not the only copy of intent.

## Make retries safe before making scheduling clever

Assume an attempt can happen twice. A worker may finish the side effect and lose its acknowledgement, or its lease may expire just before completion. Ordering can reduce some races, but it cannot prove that an external webhook receiver applied a request exactly once. FIFO queue documentation describes ordering and deduplication behavior, yet the consumer still needs a business-level identity. My rule is one immutable event ID, one deterministic idempotency key for the intended operation, and an attempt record that can be inspected after the page.

I learned that reflex during one duplicate-write incident: a naive retry ran the same operation twice, producing 2 rows for one event because the first write committed before the worker lost its acknowledgement. The retry code was behaving as written. Our transaction boundary was wrong — and that distinction mattered in the postmortem.

The safe path is to claim the event conditionally, send the same idempotency key on every delivery of that logical operation, and record the outcome. This focused Go example leaves storage details behind an interface so the claim can be implemented with a transaction and a uniqueness constraint rather than an in-process mutex:

```go
type Delivery struct {
	EventID       string
	Destination   string
	Payload       []byte
	IdempotencyKey string
}

type Store interface {
	Claim(ctx context.Context, eventID string) (bool, error)
	Complete(ctx context.Context, eventID string, status int) error
}

func deliver(ctx context.Context, client *http.Client, store Store, d Delivery) error {
	claimed, err := store.Claim(ctx, d.EventID)
	if err != nil || !claimed {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.Destination, bytes.NewReader(d.Payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", d.IdempotencyKey)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return store.Complete(ctx, d.EventID, resp.StatusCode)
}
```

There is a deeper write boundary before dispatch. If changing business state and creating the future delivery are separate commits, a crash between them can lose the task. The transactional outbox pattern puts the business change and an outbox record in the same database transaction, then lets a relay publish later. It avoids asking a database and broker to commit atomically, though the relay can publish more than once, so the idempotency rule still stands.

## How should a public HTTPS endpoint hand off long-running webhook jobs?

A public HTTPS endpoint should authenticate the sender, validate the envelope, durably record the event, and return promptly. It should not hold the connection open while a delayed task sleeps, a webhook retry loop runs, or a large report finishes. Endpoint time limits vary by gateway, runtime, and sender, so I'm not sure a universal cutoff exists; your mileage may vary. The invariant is more useful than a vendor-specific number: the request deadline must cover admission, not the whole job.

After admission, a worker leases the task for a bounded period. The lease needs an owner token and expiry time so a stale worker cannot mark a newer attempt complete. For long-running jobs, renew the lease only while useful progress continues and write checkpoints at meaningful boundaries. A checkpoint should describe completed business work, not just elapsed time. If step three charged an account, restarting from step two must recognize that charge by its idempotency key rather than perform it again.

Retries need a budget. Separate errors that might succeed later from permanent rejection, add jitter to backoff, cap attempts, and move exhausted work to a reviewable terminal state. Don't turn “dead letter” into a synonym for forgotten. I want an owner, an age alert, the last error class, and a replay command that preserves the original event ID. This is runbook material, not dashboard decoration.

Retries happen.

Watch scheduling lag, oldest eligible item age, lease-expiry count, attempt distribution, and terminal failures. Queue depth by itself mixes healthy future work with overdue work and can page the team for the wrong reason. For the public endpoint, watch admission latency and durable-write failures separately from downstream completion. That split tells the on-call engineer whether new work is entering safely or old work is merely processing slowly.

A full workflow engine is not suitable when tasks are short, state transitions are few, and a database table plus workers remain easy to rehearse. It earns consideration when jobs wait across many external events, require human approval, or need resumable multi-step history. The programming and operational model is heavier. Be honest about that.

## Verify the design, deploy it, and know how to roll it back

Test time without waiting for time. Inject a clock into the scheduler, advance it across the due boundary, and assert that one event becomes eligible. Then exercise redelivery: return the same message twice, overlap two claims, expire a lease, and stop a worker after the external call but before acknowledgement. The important assertion is not “the handler ran once.” It is “the intended business operation has one durable identity and repeated attempts converge on the same result.”

Deployment should preserve that identity across old and new worker versions. I first ship schema and readers that tolerate both message shapes, then writers, then the new workers. During a drain, workers stop claiming new tasks, finish or relinquish current leases, and expose how many claims remain. If a release increases scheduling lag or duplicate suppression unexpectedly, pause dispatch before rolling back workers; leaving admission active is safe only when the durable store can absorb the backlog and operators know its capacity limits. Recovery then has two distinct paths. A missed clock tick requires a reconciliation sweep over an explicit time window, with overlapping windows made harmless by event identity. A delayed queue requires replay from durable schedule state or a terminal queue, retaining the same logical key. Never “fix” either case by blindly rerunning a batch against public endpoints. That turns an availability incident into a correctness incident. The final go/no-go review is short: can we find every accepted event, determine whether it is early, eligible, leased, complete, or terminal, and explain who may transition it next? Can we replay without inventing a new identity? Can the receiving side see a stable idempotency key? If any answer is no, I don't care how polished the scheduler looks; it isn't ready for production.

Use cron for clock-owned reconciliation and queue workers for event-owned delivery. Put durable state and idempotency between them. That design is less exciting than arguing over scheduler features, but it is the one I want at 3 a.m.

## References

- https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-fifo-queues.html
- https://microservices.io/patterns/data/transactional-outbox.html
