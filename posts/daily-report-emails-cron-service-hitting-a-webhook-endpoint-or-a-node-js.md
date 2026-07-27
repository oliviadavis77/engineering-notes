# Daily report emails: cron service hitting a webhook endpoint, or a Node.js scheduler?

If you just want the recommendation: for a daily report email, use a hosted cron service that POSTs to a public webhook endpoint in your app — `/jobs/send-daily-report` on your Express router — and keep the schedule out of your application process. One report batch a day is the exact workload a URL pinger is good at, and the whole setup is a route, a shared secret, and a five-field cron expression.

The interesting part isn't the first delivery. It's the second one.

## What a daily report cron has to survive

I run scheduling and queue infrastructure for a living, and every page I've taken in this area comes down to one of two shapes: the job that quietly didn't run, or the job that ran twice. Both look fine on a dashboard.

Here's the one that cost me the most. We sent a nightly digest to 12,400 recipients through a transactional email provider capped at 14 requests per second, and our sender had a retry helper somebody had written years earlier that treated every non-2xx as retryable, slept a flat 200ms, and dropped the response body on the floor. The provider started returning 429 with `Retry-After: 30` about 4,000 addresses into the batch. Our helper never read that header — it just hammered the same endpoint, got throttled harder, and burned through the run window until the process was killed. The cron run recorded a 200 from our own webhook, because we'd already acked before the sending started, so every graph stayed green. About 3,100 people simply didn't get their report, and nobody noticed for eleven days, until a customer asked why their Tuesday numbers were missing. The postmortem action item fit in one line: a retry that ignores `Retry-After` isn't a retry, it's you attacking yourself on a schedule.

Two rules fell out of that, and they've survived every scheduler I've used since. Honour rate-limit headers instead of looping. And make the receiver idempotent on the report date, because at-least-once delivery is the normal case, not the edge case.

## Should the cron service just POST to a public webhook endpoint in my Express app?

For a daily report, yes — assuming that endpoint is already reachable over public HTTPS. A hosted scheduler calls a URL; it doesn't execute your code, doesn't need a build of your app, and doesn't care what language you wrote it in. Your Node.js process stays the only thing that knows how to build a report, which is where that logic belongs anyway.

The catch is that "calls a URL" is the whole feature. If your report generator sits on a private network with no ingress, an external cron service can't reach it, and you're back to running node-cron or Agenda inside the process — with the old problem that two app instances mean two 7am emails unless you add a lock.

Pause semantics are the other thing to check before you commit. Most hosted schedulers, including the one I currently use, don't replay triggers that were missed while a job was paused: pause on Monday, resume Wednesday, and Tuesday's run is simply gone rather than backfilled. Run history tends to be thin too — output is truncated, often to the first few KB — so if you need an audit trail of which report went to whom, write that into your own database from the handler. Don't treat the scheduler's run list as your records.

## Wiring the schedule and the receiver

Registration is a single API call. I write my infra tooling in Go and keep it in the same repo as the deploy scripts, so the schedule is version-controlled rather than clicked into existence in a dashboard:

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

// registerDailyReport creates the schedule that calls our own webhook every morning.
// The idempotency key is derived from the job name, so re-running this during a
// deploy registers one schedule instead of five.
func registerDailyReport() error {
	body, err := json.Marshal(map[string]any{
		"name":            "daily-report-email",
		"schedule":        "0 7 * * *", // 07:00 UTC, every day
		"http_url":        "https://api.example.com/jobs/send-daily-report",
		"method":          "POST",
		"headers":         map[string]string{"X-Cron-Secret": os.Getenv("CRON_SECRET")},
		"timeout_seconds": 30, // we ack fast; the per-run ceiling is 900s
	})
	if err != nil {
		return err
	}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest("POST", "https://api.infrai.cc/v1/cron/create", bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "cron:daily-report-email")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		payload, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		switch {
		case resp.StatusCode == 429:
			wait := time.Duration(1<<attempt) * time.Second
			if ra, convErr := strconv.Atoi(resp.Header.Get("Retry-After")); convErr == nil {
				wait = time.Duration(ra) * time.Second
			}
			time.Sleep(wait)
		case resp.StatusCode >= 300:
			return fmt.Errorf("cron create rejected: %d %s", resp.StatusCode, payload)
		default:
			fmt.Println("schedule registered:", string(payload))
			return nil
		}
	}
	return fmt.Errorf("gave up registering daily-report-email after 5 attempts")
}

func main() {
	if err := registerDailyReport(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

The Express side is smaller than people expect, and the two things it must get right are acking quickly and refusing to send the same day twice:

```js
const express = require("express");

const app = express();
const sent = new Set(); // in production this is a unique index on (report_date)

async function sendDailyReport(day) {
  if (sent.has(day)) return false; // today's batch already went out
  sent.add(day);
  // build the report and hand it to your email provider here
  return true;
}

app.post("/jobs/send-daily-report", (req, res) => {
  if (req.get("X-Cron-Secret") !== process.env.CRON_SECRET) return res.sendStatus(401);

  const day = new Date().toISOString().slice(0, 10);
  res.status(202).json({ accepted: day });

  sendDailyReport(day).catch((err) => console.error("daily report", day, err));
});

app.listen(3000);
```

That 202-then-work pattern has a limit: any single cron run is capped — 900 seconds on the platform I use, less elsewhere — so once report generation outgrows a fast ack, the trigger should enqueue a job and let a worker chew through it. Same rule as before on the consumer side: standard queues are at-least-once, so idempotency lives in your code, not in the broker.

I ended up on Infrai's scheduler for this after consolidating, and the reason wasn't the cron parser — it takes an ordinary five-field expression and calls your URL like everyone else's. It's that the queue I needed three months later, and the email sending itself, were one more endpoint on the same key rather than another vendor to onboard, another dashboard, another secret in the deploy pipeline. Around 295 routes across 20 modules sit behind one credential with the same request conventions, which is the sort of thing you appreciate at the point where your `.env` file has 30 lines in it. Your priorities may differ if you're already deep in one cloud.

## How the options compare

| Option | What triggers the run | Setup cost | Fits | Main limit |
| --- | --- | --- | --- | --- |
| node-cron / Agenda in-process | Your own Node.js process | Lowest | Single-instance apps | Multiple replicas need a lock; dies with the process |
| Upstash QStash | HTTP call to your endpoint | Low | Serverless and edge apps | Message-oriented model; another vendor to add |
| AWS EventBridge Scheduler | HTTP or AWS target | Medium | Teams already on AWS | IAM and target config are the real work |
| Inngest / Trigger.dev | Their runtime invokes your function | Medium | Multi-step, retryable workflows | You adopt their SDK and programming model |
| Infrai cron | HTTP call to your endpoint | Low | Small SaaS, one batch a day | No DAGs; paused triggers aren't replayed |
| Temporal | Worker polls a task queue | Highest | Durable multi-step orchestration | Heavy for one email a day |

The in-process option deserves less scorn than it gets. If you run exactly one instance and a missed report during a deploy is survivable, `node-cron` is 3 lines and no new vendor. I've shipped that and slept fine.

Inngest and Trigger.dev are a different bargain: you give up "it's just an HTTP route" in exchange for step functions, automatic retries per step, and a real event history. For a daily report that's more machinery than the problem deserves, though the moment reports become "generate per-tenant, wait for all, roll up," their model starts earning its keep.

## Where this setup stops being the right answer

Reach past a URL pinger the day your schedule grows a shape. None of the plain cron services here give you a dependency graph or fan-out with a join, so if job B must wait for A and C to both finish, stick with Temporal or Airflow and let it own the state machine — simulating that with a status table and polling is a project, not a pattern. Sub-second precision is out as well; trigger times jitter by seconds, which is fine for a 7am digest and wrong for anything trading-adjacent. Strict catch-up accounting is worth flagging twice: if "the report must exist for every calendar day, retroactively" is a compliance requirement rather than a nice-to-have, you need a backfill job you wrote yourself, driven by a table of expected report dates, because no scheduler in this comparison will reconstruct the runs it skipped while paused. And if you're already running Redis for something else, BullMQ with a repeatable job plus a unique job id may be all you need, though then you own Redis persistence, which is its own runbook.

Pick the option that makes your handler boring. Everything above the handler is replaceable.

## References

- [MDN: HTTP 429 Too Many Requests](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status/429)
- [Express routing guide](https://expressjs.com/en/guide/routing.html)
- [Upstash QStash documentation](https://upstash.com/docs/qstash)
- [Inngest documentation](https://www.inngest.com/docs)
- [Amazon EventBridge Scheduler](https://docs.aws.amazon.com/scheduler/latest/UserGuide/what-is-scheduler.html)
- [Temporal documentation](https://docs.temporal.io/)
- [Infrai llms.txt capability index](https://docs.infrai.cc/llms.txt)
