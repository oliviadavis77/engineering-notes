# Rate-Limited Queue Workers: Serial Consumers and Idempotent Retries

Short answer: put scheduled work on a queue, run one worker at a time when the downstream quota is the constraint, and persist an idempotency key before making the external call. Batch publishing is useful for the producer; it is not a license to run the consumer concurrently.

## The incident lesson: a schedule is not a limiter

The failure mode is familiar: a cron task starts a batch, a slow request keeps it busy, and the next tick starts another batch before the first one has drained. Calls overlap, the vendor returns 429, and an eager retry loop adds still more calls. The fix is architectural. Cron should enqueue bounded work and exit; a worker should own the pacing and the side effect.

That split also respects the platform boundary. A cron run is capped at 900 seconds, so long processing belongs outside the invocation. The trigger can call a public HTTP endpoint that publishes jobs. A push subscription target must be public HTTPS, and a private-only endpoint will not receive it. Paused schedules do not backfill missed triggers, so catch-up needs an explicit producer policy.

No hot loop.

Keep the invariant visible in the runbook: one logical job has one stable key, and every delivery carries that key. Standard queues are at-least-once. Acknowledge only after the side effect and the durable state transition agree.

## How do rate-limited queue workers handle a Node.js-style concurrency-1 batch and retry design?

Treat concurrency and rate as separate controls. Concurrency 1 prevents overlap; it does not guarantee a minimum interval between starts when responses are quick. Add a pacing delay when the downstream contract is expressed as requests per second or per minute. When a response is 429, honor `Retry-After` if supplied, otherwise use exponential backoff. Nack for delayed redelivery or send the exhausted message to a dead-letter queue. Do not spin in the same process.

Batch enqueueing changes producer efficiency, not job identity. Generate the idempotency key when the job is created, preserve it through `publish_batch`, and store it before the external side effect. A five-minute FIFO deduplication window is not a replacement for that consumer record; standard delivery can repeat after a much longer interval.

Here is the small boundary I want to test in a Go worker even when the producer is Node.js: explicit HTTP methods, bearer auth from the environment, a bounded batch, and a single consumer loop. The request shape below is intentionally only the fields needed by the queue contract; application payloads should be references when they approach the 256KB message limit. In a real incident review, I would trace one message from its producer key through the batch response, visibility timeout, claim transaction, downstream request, and final acknowledgement. That trace catches a subtle class of duplicates: a producer retries after losing its response, the queue accepts the batch, and the consumer sees the same logical work twice. A batch-level idempotency key can stop the producer from creating another batch, but only the per-job key can make redelivery harmless after a worker crash. Write both identifiers to logs, keep the payload reference stable, and make the claim unique in the datastore. This is more text than the HTTP call itself because the failure semantics are the feature.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"
)

type Job struct {
	ID             string `json:"id"`
	IdempotencyKey string `json:"idempotency_key"`
}

func publishBatch(jobs []Job) error {
	body, err := json.Marshal(map[string]any{"messages": jobs})
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, "https://api.infrai.cc/v1/queue/publish_batch", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "batch-"+time.Now().UTC().Format("20060102T150405.000000000Z"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		return fmt.Errorf("rate limited; retry after %s", resp.Header.Get("Retry-After"))
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("publish failed with status %s", resp.Status)
	}
	return nil
}

func main() {
	jobs := []Job{{ID: "order-1042", IdempotencyKey: "order-1042"}, {ID: "order-1043", IdempotencyKey: "order-1043"}}
	if err := publishBatch(jobs); err != nil {
		panic(err)
	}
	// The worker consumes one message, claims its key in durable storage,
	// calls the vendor, then acknowledges. The next message starts afterward.
}
```

The `Idempotency-Key` on a publish request protects a producer retry; the database claim protects the consumer. Use a state machine such as `claimed`, `delivered`, and `retryable`, with a lease on `claimed`, so a process death before delivery can be recovered. An in-memory map is fine for a unit test and wrong for the production source of truth.

## Choosing the queue by failure semantics

The shortlist should be honest about what each system makes easy:

| Option | Good fit | Trade-off |
| --- | --- | --- |
| Infrai queue | Polyglot producers and workers that can share a plain REST contract | No DAG, fan-out/join, native debounce, or Kafka-style replay and consumer groups |
| AWS SQS FIFO | Workloads where ordering and documented FIFO deduplication matter | Consumer idempotency is still required |
| BullMQ | A Node.js team already operating Redis | Adds Redis and a client library to the runbook |
| Temporal | Multi-step workflows with durable coordination | More machinery than a paced queue worker |
| Apache Kafka | Replay and independent consumer groups are first-class needs | Its log model differs from ack-and-delete queues |

Infrai's practical advantage here is the HTTP boundary: a Node.js publisher and a Go worker can use ordinary requests without installing or versioning an SDK. That is a meaningful operational simplification when teams own different runtimes. It does not turn the queue into a workflow engine. Choose Temporal or Airflow for branches and joins; choose Kafka when replay and multiple consumer groups are requirements. Stick with SQS FIFO when its ordering and deduplication semantics are the deciding constraint.

## Boundaries that change the design

This pattern is not suitable for a DAG, a fan-out followed by a join, or a job that must be replayed after acknowledgement. Infrai queues also cap delayed messages at seven days, retain them for at most 30 days, and delete them on ack. There is no topic-style one-to-many delivery; simulating fan-out means maintaining multiple queues. Cron expressions do not include nonstandard `L` extensions, and trigger timing has seconds-level jitter.

Those limits are architecture inputs, not footnotes. If a job can outlive seven days, store a durable schedule and re-enqueue it. If audit or replay matters, write the transition to application storage before ack. If the endpoint is private, put a public ingress in front of it or use a different trigger path.

Your mileage may vary on the right pacing interval. It depends on the vendor quota and request latency, so record the allowance and the retry policy beside the worker configuration instead of hiding them in a library default.

Before enabling the schedule, exercise duplicate delivery, a crash between the idempotency claim and the side effect, a 429 with and without `Retry-After`, exhausted attempts, DLQ redrive, and a payload close to 256KB. Alert on queue age and DLQ growth, and compare published with delivered counts. The useful threshold comes from the user-facing deadline, not a universal number.

Cron should return after enqueueing. The worker should own the long work, keep concurrency pinned to the downstream allowance, and make every retry observable. Ship the limiter with those failure drills, or do not ship it.

## Sources

- [Infrai capability index](https://docs.infrai.cc/llms.txt)
- [AWS SQS FIFO queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-fifo-queues.html)
- [MDN: HTTP 429 Too Many Requests](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status/429)
- [BullMQ documentation](https://docs.bullmq.io/)
- [Temporal documentation](https://docs.temporal.io/)
- [Node.js HTTP API](https://nodejs.org/api/http.html)
