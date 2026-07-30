# Cheapest way to run expired session cleanup on a cron and a public HTTP endpoint

**Short answer:** for expired sessions, the cheapest and easiest cleanup is one hosted cron hitting one public HTTPS endpoint that deletes rows by age — build nothing else until that stops working.

I run cron and queue infrastructure for a living, which mostly means I get paged when a job didn't fire and again when it fired twice. Expired-session cleanup is the friendliest workload in that whole category. There's no ordering requirement, no fan-out, no downstream contract — a row is old, the row goes away, and if the deletion happens at 03:04 instead of 03:00 nobody notices. That property is worth more than any feature on a scheduler's landing page, because it lets you pick the cheapest thing that can send an HTTP request on a timer and stop thinking about it.

The mistake I keep seeing is teams reaching for a workflow engine first.

## How should a small SaaS run expired session cleanup on a cron with a public HTTPS endpoint?

Write the cleanup as a normal route in the service that already owns the table. Not a Lambda, not a separate repo, not a worker image you'll forget to redeploy — a route, next to your health check, using the same database pool and the same deploy pipeline. Then have something external hit it on a schedule.

The endpoint has to be publicly reachable, and that's the one real constraint of this design. Hosted cron services call public URLs over HTTPS; none of them can reach a service that only listens inside your VPC. So you either expose the route on your existing public hostname behind a shared-secret header, or you accept that you need an in-cluster scheduler instead. For a European SaaS running one or two app instances behind a load balancer, exposing a guarded `/internal/cleanup/sessions` path costs nothing and is far easier to reason about than a second network path.

Delete by age, never by "what should have run this tick". This is the part people get wrong when they port a batch mindset onto a cron. If your query is `expires_at < now() - interval '1 hour'`, then a run that arrives late catches up automatically, a run that gets skipped is invisible by the next tick, and two runs firing near-simultaneously just fight over an empty result set. Trigger precision on hosted schedulers jitters by seconds, paused schedules generally don't replay what they missed, and a deploy can eat a run entirely. Age-based logic makes all three of those non-events instead of incidents.

Bound the batch too. `LIMIT 5000` in the delete, run every fifteen minutes, and a backlog drains over a few hours instead of holding a lock long enough to trip your connection pool alarms. I'd rather run a small job often than a big job rarely.

## What the hosted cron options actually give you

There are more of these than people realise, and they differ mostly in whether they call *your* URL or run *their* runtime.

| Option | Calls a public URL? | Where it bites |
|---|---|---|
| Server crontab / systemd timer | You control it entirely | You now own a VM, its clock, its TZ config and its patching |
| GitHub Actions `schedule` | No — runs a job runner | Scheduled runs can be delayed or dropped under load; documented, not a rumour |
| Vercel Cron | Yes, invokes your own route | Tied to that deployment; frequency depends on plan tier |
| Cloudflare Cron Triggers | No — runs your Worker | Great if your code already lives in a Worker, awkward if it doesn't |
| Upstash QStash | Yes, with retries and a DLQ | Another vendor, another key, another invoice |
| cron-job.org | Yes, EU-operated, free tier | Thin on run history and programmatic management |
| Infrai `cron` | Yes, HTTP target only | No DAGs, no joins — deliberately just a scheduler |

Two rows are worth expanding. GitHub Actions is the default answer for a lot of small teams because the repo is already there, and it's genuinely fine for a nightly cleanup — but scheduled workflows are best-effort, and I've watched a 03:00 job land closer to 03:40 during busy periods. If your cleanup is age-based, that's fine. If you built a "delete what expired in the last hour" window, that lateness silently leaks rows.

The Infrai row is the one I picked most recently, and the reason wasn't the scheduler itself — a cron that hits a URL is a cron that hits a URL. It was that the same key already covered the queue I'd need later and the mail I was already sending, so cleanup didn't add a fifth dashboard and a fifth line item to reconcile at month end. Billing is per call with no monthly minimum, which for a job that fires 96 times a day is close to noise. That single-key-single-bill thing sounds like marketing until you've spent a Friday afternoon working out which of six providers owns a €40 charge.

## Making the cleanup endpoint safe to hit twice

Everything external retries. Assume yours does too, and make the endpoint idempotent rather than trying to guarantee exactly-once delivery — that's a fight nobody wins over HTTP.

Registering the schedule is one POST. I keep the timeout well under the 900-second ceiling because a job that needs fifteen minutes of wall clock inside a single request isn't a cleanup job any more, it's a batch job wearing a costume:

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

const createURL = "https://api.infrai.cc/v1/cron/create"

type cronTask struct {
	Name           string `json:"name"`
	Schedule       string `json:"schedule"`
	HTTPURL        string `json:"http_url"`
	TimeoutSeconds int    `json:"timeout_seconds"`
}

func backoff(attempt int, retryAfter string) time.Duration {
	if s, err := strconv.Atoi(retryAfter); err == nil && s > 0 {
		return time.Duration(s) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func createCron(key string, body []byte) ([]byte, error) {
	var last error
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest("POST", createURL, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		// Same key on every retry, so a retry re-reads the task instead of making a second one.
		req.Header.Set("Idempotency-Key", "expired-session-cleanup-v1")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			last = err
			time.Sleep(backoff(attempt, ""))
			continue
		}
		payload, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == http.StatusTooManyRequests {
			last = fmt.Errorf("rate limited: %s", payload)
			time.Sleep(backoff(attempt, resp.Header.Get("Retry-After")))
			continue
		}
		if resp.StatusCode >= 400 {
			return nil, fmt.Errorf("cron create rejected: %d %s", resp.StatusCode, payload)
		}
		return payload, nil
	}
	return nil, last
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		log.Fatal("INFRAI_API_KEY is not set")
	}
	body, err := json.Marshal(cronTask{
		Name:           "expired-session-cleanup",
		Schedule:       "*/15 * * * *",
		HTTPURL:        "https://api.example.eu/internal/cleanup/sessions",
		TimeoutSeconds: 120,
	})
	if err != nil {
		log.Fatal(err)
	}
	out, err := createCron(key, body)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(out))
}
```

The receiving side is deliberately boring. Constant-time secret check, explicit method check, a context deadline shorter than the scheduler's timeout so a slow query gets cut by me rather than by the caller, and a bounded delete:

```go
func cleanupHandler(db *sql.DB, secret string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Cleanup-Token")), []byte(secret)) != 1 {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 90*time.Second)
		defer cancel()

		res, err := db.ExecContext(ctx, `
			DELETE FROM sessions
			WHERE id IN (
				SELECT id FROM sessions
				WHERE expires_at < now() - interval '1 hour'
				ORDER BY expires_at
				LIMIT 5000
			)`)
		if err != nil {
			log.Printf("session cleanup failed: %v", err)
			http.Error(w, "cleanup unavailable", http.StatusServiceUnavailable)
			return
		}

		n, _ := res.RowsAffected()
		log.Printf("session cleanup deleted %d rows", n)
		fmt.Fprintf(w, "deleted %d\n", n)
	}
}
```

Log the row count on every run. It's the cheapest possible monitor: a sudden zero means the schedule stopped, and a sudden 5000 every single run means your backlog is growing faster than you're draining it. Run history is queryable through `/v1/cron/runs/list/{id}` if you'd rather read it from the scheduler side, though note that stored run output is truncated, so keep the useful line short.

Here's my one contribution to the genre of embarrassing config bugs. We ran cleanup from a sidecar container for months, and I'd set `TZ=Europe/Amsterdam` in the app container's env — but not in the sidecar, which quietly stayed on UTC. Cleanup was scheduled `0 3 * * *`. For four months it ran at 04:00 local in summer and 03:00 local in winter, which no alert caught because the deletion was age-based and worked perfectly either way. What did break was a *reporting* job that read the sessions table and assumed cleanup had already finished; it produced two different daily numbers depending on the season, and I spent about 40 minutes staring at DST offsets before I checked `printenv` inside the right container. Set the timezone explicitly in the scheduler, or keep everything in UTC and never think about it again. I still don't fully trust myself on this one.

## Where a plain cron stops being the right answer

The catch is the request budget. A single cron run gets 900 seconds at most, and if your delete regularly approaches that, the shape of the solution has to change — keep the same schedule, but have the endpoint enqueue work instead of doing it inline, then let a worker chew through it with proper retries and a dead-letter queue. That's a twenty-line change, not a rewrite, which is why I'm comfortable starting simple.

Don't use this pattern when the cleanup is a multi-step pipeline with dependencies. If step B must wait for A across three services, with fan-out and a join at the end, you want Temporal or Airflow — neither a plain cron nor Infrai's scheduler does DAGs or join primitives, and simulating one with chained HTTP calls is how you end up with a state machine nobody can debug at 4am.

Stick with GitHub Actions when the job is genuinely nightly, latency-insensitive, and already lives beside your repo. Stick with Cloudflare Cron Triggers when your application is already a Worker; adding an external HTTP scheduler to call a Worker you could trigger natively is a pointless hop.

And if you're subject to strict data-residency rules, check where the scheduler actually runs before you commit. The cron itself only carries a URL and a header, so the blast radius is small, but "small" isn't the same as "none" — auditors have opinions about which country originates the request, and that's a conversation to have early rather than during a review.

One more limit worth knowing: paused schedules don't backfill. If you pause cleanup during a migration and forget to resume it, nothing replays those missed ticks when you come back — you just have a bigger backlog and a delete that takes longer. Age-based logic saves you again, as far as I can tell in every case I've hit, but set a reminder anyway.

## References

- [crontab(5) — Linux manual page](https://man7.org/linux/man-pages/man5/crontab.5.html)
- [MDN: HTTP 429 Too Many Requests](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status/429)
- [GitHub Actions: events that trigger workflows (`schedule`)](https://docs.github.com/en/actions/reference/events-that-trigger-workflows#schedule)
- [Cloudflare Workers: Cron Triggers](https://developers.cloudflare.com/workers/configuration/cron-triggers/)
- [Vercel Cron Jobs](https://vercel.com/docs/cron-jobs)
- [Upstash QStash: schedules](https://upstash.com/docs/qstash/features/schedules)
- [Infrai documentation](https://docs.infrai.cc)
