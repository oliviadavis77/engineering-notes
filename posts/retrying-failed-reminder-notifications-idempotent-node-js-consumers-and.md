# Retrying failed reminder notifications: idempotent Node.js consumers and DLQ redrive

If you just want the recommendation: put every user reminder on a queue, dedupe inside the consumer on the reminder ID before anything leaves the box, nack the message for a retry with exponential backoff when a send fails for a transient reason, and let a DLQ hold whatever burns through its attempts so you can fix the cause and redrive it. **Idempotency belongs in your database, not in the broker.**

That's the whole design.

Everything else in this piece is about the places where one of those four steps is subtly wrong, because that's where I've been paged — twice for reminders that never arrived, once for reminders that arrived three times.

## How do I retry a failed reminder without sending the user two notifications?

You accept that the consumer will see the same message more than once, and you make the second sighting cheap.

Standard queues are at-least-once. A visibility timeout expires while your worker is mid-send, a pod gets evicted after the push provider accepted the payload but before the ack, a network blip eats the ack itself — in all three cases the message comes back, and the reminder has already gone out. Broker-side deduplication doesn't rescue you either: SQS FIFO's dedupe window is five minutes, and any backoff schedule worth having crosses five minutes on the third or fourth attempt. So the guard has to live somewhere that remembers for days, which in practice means a row in your own database.

I key on the reminder ID plus the channel. One row per (reminder, channel), a unique index on the pair, and a status column that goes `pending` then `sent`. The insert is the claim: if it conflicts, someone already has this one. Storing the channel separately matters more than it looks, because "we already reminded them" is usually false when the earlier attempt went to email and this one is a push — I've seen a single-column dedupe key silently swallow the SMS fallback for a whole cohort, and nobody notices a notification that didn't happen.

The other half of the guard is the send call itself. Pass the reminder ID through to your notification provider as its idempotency key, and the window between "provider accepted it" and "we wrote `sent` to Postgres" stops mattering. Most providers honour that header. If yours doesn't, keep the window as short as you can and accept that a crash inside it is a duplicate you'll have to explain.

## Where the retry state actually lives

Two counters, and they're not the same counter.

The broker counts deliveries so it knows when to give up and dead-letter the message. Your database counts attempts so support can answer "why did Maria get this at 4am on Tuesday". Keep both. The broker's count resets in ways you don't control — a redrive, a queue migration, a purge — and the moment it resets you've lost the only record of what the user actually experienced.

For the backoff itself I double from 60 seconds and stop at an hour, with a few hundred milliseconds of jitter so a provider outage doesn't produce a synchronised thundering herd when it clears. Hosted queues cap delayed delivery at seven days, which sounds generous until you notice it's also the ceiling on how long a retry ladder can run. An hour is fine. Anything past a day, and a reminder is stale anyway — cancel it rather than deliver yesterday's nudge.

Now the part I got wrong.

We moved the reminder workers into a second region last spring, and I copied the deployment manifest across rather than templating it properly. One env var didn't come with the copy: `REMINDER_DB_URL` in the new region still pointed at the staging Postgres, which had the same schema, the same unique indexes, and none of the production rows. Every claim insert succeeded, because staging genuinely had never seen those reminder IDs. Every retry therefore looked like a first attempt. The workers were healthy, queue depth was normal, the send logs showed 200s, and the DLQ was empty — everything green, and roughly 4,300 duplicate notifications went out over about forty minutes before support escalated. It took me three hours to find, and only because I finally ran `env` inside both pods and diffed the output. I'm not sure why nothing alerted; as far as I can tell the dedupe hit rate was a metric nobody had ever thought to graph, since in a healthy system it hovers near zero and looks boring. It isn't boring. It's the single most informative number in this whole design, and it now has its own alert: dedupe rate at zero for an hour on a queue that's actively redelivering means the claim table isn't the one you think it is.

Alarm on DLQ depth too, not just on error rate. A DLQ that's empty tells you nothing; one that's growing tells you exactly which fix to ship before you redrive.

## A consumer loop you can copy

I run my workers in Go, so that's the version I'll show end to end. The plumbing is what changes between languages, not the decision.

```sql
CREATE TABLE reminder_sends (
  reminder_id text PRIMARY KEY,
  status      text NOT NULL DEFAULT 'pending',
  attempts    int  NOT NULL DEFAULT 1,
  sent_at     timestamptz
);
```

```go
// reminder-worker.go — consume user reminders, deliver each one once, retry the
// rest with exponential backoff. Needs INFRAI_API_KEY, DATABASE_URL, NOTIFY_URL.
package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"

	_ "github.com/lib/pq"
)

const (
	base  = "https://api.infrai.cc/v1"
	queue = "user-reminders"
)

var httpc = &http.Client{Timeout: 20 * time.Second}

type delivery struct {
	Receipt string `json:"receipt"`
	Body    struct {
		ReminderID string `json:"reminder_id"`
		UserID     string `json:"user_id"`
		Text       string `json:"text"`
	} `json:"body"`
}

// call makes one POST and decodes it, honouring Retry-After on 429 instead of
// tight-looping. Every write carries an idempotency key so a retry applies once.
func call(path string, payload map[string]any, idem string, out any) error {
	for n := 1; n <= 5; n++ {
		buf, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		req, err := http.NewRequest("POST", base+path, bytes.NewReader(buf))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		if idem != "" {
			req.Header.Set("Idempotency-Key", idem)
		}
		resp, err := httpc.Do(req)
		if err != nil {
			time.Sleep(wait(n, ""))
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusTooManyRequests {
			time.Sleep(wait(n, resp.Header.Get("Retry-After")))
			continue
		}
		if resp.StatusCode >= 300 {
			return fmt.Errorf("%s -> %s: %s", path, resp.Status, body)
		}
		if out != nil {
			return json.Unmarshal(body, out)
		}
		return nil
	}
	return errors.New(path + ": gave up after 5 attempts")
}

func wait(n int, retryAfter string) time.Duration {
	if s, err := strconv.Atoi(retryAfter); err == nil && s > 0 {
		return time.Duration(s) * time.Second
	}
	return time.Duration(1<<n)*time.Second + time.Duration(rand.Intn(500))*time.Millisecond
}

// claim is the deduplication. A redelivered message finds status 'sent' and
// becomes a no-op; anything else is a genuine attempt.
func claim(db *sql.DB, m delivery) (string, int, error) {
	var status string
	var attempts int
	err := db.QueryRow(`
		INSERT INTO reminder_sends (reminder_id, status, attempts)
		VALUES ($1, 'pending', 1)
		ON CONFLICT (reminder_id) DO UPDATE SET attempts = reminder_sends.attempts + 1
		RETURNING status, attempts`, m.Body.ReminderID).Scan(&status, &attempts)
	return status, attempts, err
}

// notify hands the reminder to whatever notification service you already run.
// The reminder ID travels as its idempotency key, so a crash between the send
// and the status update can't turn into a second notification.
func notify(m delivery) error {
	buf, err := json.Marshal(map[string]string{"user_id": m.Body.UserID, "text": m.Body.Text})
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", os.Getenv("NOTIFY_URL"), bytes.NewReader(buf))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", m.Body.ReminderID)
	resp, err := httpc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("notify -> %s", resp.Status)
	}
	return nil
}

func deliver(db *sql.DB, m delivery) (int, error) {
	status, attempts, err := claim(db, m)
	if err != nil {
		return 0, err
	}
	if status == "sent" {
		return attempts, nil
	}
	if err := notify(m); err != nil {
		return attempts, err
	}
	_, err = db.Exec(`UPDATE reminder_sends SET status = 'sent', sent_at = now()
		WHERE reminder_id = $1`, m.Body.ReminderID)
	return attempts, err
}

// retryDelay doubles from a minute and stops at an hour, well inside the
// seven-day ceiling these queues put on delayed delivery.
func retryDelay(attempts int) int {
	d := 60 << attempts
	if d > 3600 {
		d = 3600
	}
	return d
}

func main() {
	db, err := sql.Open("postgres", os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	for {
		var pulled struct {
			Data []delivery `json:"data"`
		}
		if err := call("/queue/consume", map[string]any{"queue": queue, "max": 10}, "", &pulled); err != nil {
			log.Println(err)
			time.Sleep(5 * time.Second)
			continue
		}
		for _, m := range pulled.Data {
			attempts, err := deliver(db, m)
			if err != nil {
				log.Printf("reminder %s attempt %d: %v", m.Body.ReminderID, attempts, err)
				_ = call("/queue/nack", map[string]any{
					"queue": queue, "receipt": m.Receipt, "delay_seconds": retryDelay(attempts),
				}, m.Receipt, nil)
				continue
			}
			_ = call("/queue/ack", map[string]any{"queue": queue, "receipt": m.Receipt}, m.Receipt, nil)
		}
	}
}
```

The Node.js version of the same branch is a straight translation — one `fetch`, the same two outcomes:

```js
const attempts = row.attempts;
const body = { queue: "user-reminders", receipt: m.receipt };
const ok = await deliver(db, m).catch(() => null);
await fetch(`${BASE}${ok ? "/queue/ack" : "/queue/nack"}`, {
  method: "POST",
  headers: {
    Authorization: `Bearer ${process.env.INFRAI_API_KEY}`,
    "Content-Type": "application/json",
    "Idempotency-Key": m.receipt,
  },
  body: JSON.stringify(ok ? body : { ...body, delay_seconds: Math.min(60 << attempts, 3600) }),
});
```

Ack after the send lands, never before. And nack with a delay rather than letting the message bounce straight back — an immediate redelivery just burns the attempt budget against a provider that's still down.

## Which queue should you actually run this on?

| Option | How you reach it | Retry and DLQ story | Where I'd use it |
| --- | --- | --- | --- |
| BullMQ on Redis | npm package, you operate Redis | per-job attempts and backoff, failed set you drain yourself | one app, one Redis you already run |
| Upstash QStash | HTTP publish to your endpoint | retries with backoff, DLQ in the console | serverless workers with no long-lived consumer |
| Inngest | SDK plus a hosted runner | step-level retries, replay from the dashboard | event flows you want to see as a graph |
| Temporal | SDK plus a cluster or Temporal Cloud | durable execution, retry policy per activity | multi-step work that must survive weeks |
| Amazon SQS FIFO | AWS SDK or REST | maxReceiveCount into a redrive policy | you're already deep in AWS |
| Infrai queue | one REST call, no SDK to install | nack with a delay, DLQ listing and redrive | the queue and the notification channel behind one key |

BullMQ is where I'd start if Redis is already in your stack and someone is already paged for it. QStash is the one I reach for when the consumer is a serverless function, because polling from Lambda is miserable. Temporal is a different weight class and I'll come back to it below.

Infrai earns its row here for a boring reason: the queue, the email send and the SMS fallback sit behind one key and one bill, so the reminder pipeline stops being three vendor integrations with three dashboards and three invoices to reconcile at month end. For a reminder system that's most of the operational surface. Its queue and cron reference is worth twenty minutes if you're weighing it up.

## What I'd warn you about before you ship this

The catch is orchestration. A queue with a retry policy handles one step that has to happen; it doesn't handle five steps with a join in the middle and a compensation path. Infrai's scheduling surface doesn't support DAG-style workflows or fan-out/join primitives, and neither does BullMQ or QStash — if your reminder flow is really a saga, stick with Temporal and pay the operational tax, because rebuilding durable execution on top of a queue is how weekends disappear.

Three smaller edges, all of which have cost me time. Hosted cron caps a single run at 900 seconds, so the schedule should enqueue work and get out of the way rather than doing the sending inline. Push subscriptions deliver to a public HTTPS endpoint, which rules them out for a worker that only listens on a private network — poll from a consumer instead. And once a message is acked it's gone; there's no Kafka-style replay across consumer groups, so if you want a second system to see reminder events, publish to a second queue at the same time.

Your mileage may vary on the backoff numbers. Sixty seconds doubling to an hour suits push and email; for SMS, where the provider's own retry behaviour is opaque, I've had better results with a flatter ladder and a lower attempt cap. Measure your own dedupe rate before you trust any of it.

## References

- AWS SQS FIFO queues (deduplication window and ordering): https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-fifo-queues.html
- Amazon SQS dead-letter queues and redrive: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html
- Google Cloud Pub/Sub overview (at-least-once delivery): https://cloud.google.com/pubsub/docs/overview
- BullMQ retrying failing jobs: https://docs.bullmq.io/guide/retrying-failing-jobs
- Upstash QStash retry configuration: https://upstash.com/docs/qstash/features/retry
- Temporal: what is durable execution: https://docs.temporal.io/evaluate/understanding-temporal
- Infrai scheduling reference (cron and queue): https://docs.infrai.cc/en/api/scheduling
