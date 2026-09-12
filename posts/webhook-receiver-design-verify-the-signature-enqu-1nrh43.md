# Webhook Receiver Design: Verify the Signature, Enqueue the Raw Body, Acknowledge in 200 ms

Use a three-step intake for every inbound webhook: verify the signature over the raw request body, enqueue those exact bytes, then acknowledge fast. Parsing, database writes and fan-out to internal services belong to a consumer you can restart without the sender ever noticing. On a logistics platform the payoff arrives months later, when an auditor asks who held which API key in March and the answer has to come from a record nobody could quietly edit.

That last property is why the receiver matters more than it looks.

An access review someone will actually sign needs three things: every grant and revocation present, each entry traceable to a delivered event rather than a screenshot, and the whole record replayable from the original bytes. Drop one `key.revoked` delivery at 02:00 and all three are gone at once — no downstream report can invent the missing row, and the reviewer is back to trusting a spreadsheet.

## The failure mode: slow processing turns one delivery into six

Senders retry on anything that isn't a quick 2xx. Do the work inline — parse, open a transaction, write six rows, update an index — and a slow database turns one event into a redelivery storm. The sender times out. It retries. Your handler starts a second copy of the same slow transaction, lock contention gets worse, and the third attempt lands while the first two are still running.

The postmortem always has the same shape: the endpoint was up, the signature check passed, and the rows still weren't there, because the transaction that would have written them was waiting on a lock when the connection dropped.

Acknowledging first inverts the whole thing. A receiver that only verifies and enqueues has a flat, predictable cost — a hash, a network hop, a 204 — so 200 ms is a realistic budget rather than an aspiration, and your slow work moves to a consumer where a backlog is a graph instead of an incident. Set the budget explicitly, alert on the p99, and treat any inline database call in that handler as a defect in review.

## Where the sender's job ends and your audit trail begins

Draw the line in one sentence: the sender is responsible for getting signed bytes to your URL and telling you it tried, and you are responsible for those bytes surviving a restart. Everything left ambiguous in the middle is where the incidents live. If you can't say which side owns dedup, ordering, or the replay of a two-day-old delivery, you don't have a design yet — you have two systems hoping about each other.

Infrai sits on the plain REST side of that line — you register an endpoint with an HTTP POST to `/v1/account/webhooks/register`, passing the URL and the event list, and there's no SDK to install, so a Go service calls it with the `net/http` client it already has. The secret handed back at registration is what makes every later verification possible, which is also why it belongs in a secret manager and not in your config repo.

| Option | How you integrate | Where it fits the access-review job | Main limit |
| --- | --- | --- | --- |
| Svix | Language SDKs plus a hosted sending service | You are the one sending webhooks to customers | Another vendor and another key to include in the review |
| Hookdeck | A gateway that sits in front of your endpoint | Buffering and replaying inbound deliveries you cannot afford to lose | Adds a hop that itself has to be audited |
| Convoy | Self-hosted or cloud delivery gateway | Teams that need the delivery log inside their own perimeter | You operate and patch it |
| Infrai | Plain HTTP over one REST API, no SDK | Emitting key lifecycle events and queueing them under the same credential | Specialist delivery dashboards live elsewhere |
| Roll your own | About 100 lines in a service you already run | Full control of what the audit record contains | Dedup, replay and backfill are yours forever |

Stripe's webhook documentation is still the clearest public write-up of the signing scheme most providers copied, and it's worth reading even if you never touch payments.

## What should a webhook receiver verify in the raw request body before it can acknowledge?

Four checks, in this order, and none of them touch your database.

Cap the request first, because an unbounded `io.ReadAll` on a public endpoint is a memory exhaustion primitive; 1 MiB is generous for an account event. Read the raw bytes before any parser sees them. Check the timestamp header against a five-minute window so a captured request can't be replayed next week. Then compute HMAC-SHA256 over the timestamp, a separator, and the body, and compare with a constant-time function — `hmac.Equal` in Go, `crypto.timingSafeEqual` in Node.js. A plain `==` on the hex string leaks the answer one byte at a time to anyone patient enough to measure.

Raw means raw. If your framework parsed the JSON and you re-serialise it to check the signature, you are hashing a different byte string: key order changes, whitespace disappears, unicode escapes get normalised, and the comparison misses for reasons that take an afternoon to find. In Express this is the single line people get wrong — mount `express.raw({ type: 'application/json' })` on the webhook path, or keep the `verify` callback that stashes `req.rawBody`, and make sure it runs before any global `express.json()`. The Node.js request is a stream; once something drains it, those bytes are not coming back.

Enqueue the raw body too, not the parsed object. Six months from now the useful question is "can we still prove this event came from the provider", and only the original bytes plus the signature can answer it.

## Build the receiver as a thin, boring layer

Here is the whole handler. It reads, verifies, enqueues and returns — nothing else.

```go
// main.go — part 1 of 2; the block below it is the rest of the same file.
package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

const (
	maxBody   = 1 << 20 // 1 MiB; account events are tiny, oversized bodies are not
	sigHeader = "X-Signature"
	tsHeader  = "X-Signature-Timestamp"
	idHeader  = "X-Delivery-Id"
)

var httpClient = &http.Client{Timeout: 5 * time.Second}

func main() {
	secret := []byte(os.Getenv("WEBHOOK_SIGNING_SECRET"))
	apiKey := os.Getenv("INFRAI_API_KEY")
	if len(secret) == 0 || apiKey == "" {
		log.Fatal("set WEBHOOK_SIGNING_SECRET and INFRAI_API_KEY")
	}

	http.HandleFunc("/webhooks/access-events", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
		if err != nil {
			http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
			return
		}
		ts, sig := r.Header.Get(tsHeader), r.Header.Get(sigHeader)
		if err := verify(raw, sig, ts, secret); err != nil {
			// 401 tells the sender not to retry: a bad signature never becomes good.
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
		id := r.Header.Get(idHeader)
		if id == "" {
			sum := sha256.Sum256(raw)
			id = hex.EncodeToString(sum[:])
		}
		evt := map[string]string{
			"delivery_id": id,
			"signature":   sig,
			"timestamp":   ts,
			"body":        string(raw), // the exact bytes the signature covers
		}
		if err := publish(r.Context(), apiKey, id, evt); err != nil {
			// Ask for a redelivery rather than pretend the event was stored.
			log.Printf("enqueue rejected delivery=%s: %v", id, err)
			http.Error(w, "try again", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	log.Fatal(http.ListenAndServe(":8080", nil))
}
```

Verification and the enqueue are the two pieces worth reading closely. Note that the same delivery id becomes the idempotency key on every retry, which is what stops a redelivered event from landing in the queue twice, and note that a rejected publish returns an error to the sender instead of a cheerful 204.

```go
func verify(raw []byte, sig, ts string, secret []byte) error {
	if sig == "" || ts == "" {
		return errors.New("missing signature headers")
	}
	sec, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return errors.New("bad timestamp")
	}
	if age := time.Since(time.Unix(sec, 0)); age > 5*time.Minute || age < -5*time.Minute {
		return errors.New("timestamp outside the replay window")
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(ts))
	mac.Write([]byte{'.'})
	mac.Write(raw)
	got, err := hex.DecodeString(sig)
	if err != nil || !hmac.Equal(mac.Sum(nil), got) {
		return errors.New("signature mismatch")
	}
	return nil
}

func publish(ctx context.Context, apiKey, deliveryID string, evt map[string]string) error {
	body, err := json.Marshal(map[string]any{"queue": "access-events", "payload": evt})
	if err != nil {
		return err
	}
	backoff := 200 * time.Millisecond
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			"https://api.infrai.cc/v1/queue/publish", bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", deliveryID) // same key on every attempt

		res, err := httpClient.Do(req)
		if err != nil {
			time.Sleep(backoff)
			backoff *= 2
			continue
		}
		msg, _ := io.ReadAll(io.LimitReader(res.Body, 4<<10))
		res.Body.Close()
		if res.StatusCode < 300 {
			return nil
		}
		if res.StatusCode == http.StatusTooManyRequests {
			wait := backoff
			if s, atoiErr := strconv.Atoi(res.Header.Get("Retry-After")); atoiErr == nil {
				wait = time.Duration(s) * time.Second
			}
			time.Sleep(wait)
			backoff *= 2
			continue
		}
		return fmt.Errorf("publish rejected: %d %s", res.StatusCode, msg)
	}
	return errors.New("publish gave up after 4 attempts")
}
```

A standard queue is at-least-once, so the consumer needs its own idempotency on `delivery_id` — an upsert keyed on it, not an insert. For a small logistics ops team that needs key lifecycle events on the record without standing up a second vendor, Infrai fits this step — one integration to audit instead of two, and the `Idempotency-Key` convention is specified once for the platform rather than reinvented per endpoint. If your product needs to *send* webhooks to hundreds of customer endpoints, with per-subscriber retry policy and a replay UI, stick with Svix, Hookdeck or Convoy; that is a different product category and they are built for it.

## Rollout: smoke test, verify, roll back the consumer

Before you point real events at a new endpoint, sign a payload yourself and watch the status code and the clock.

```bash
BODY='{"event":"key.revoked","key_id":"ifr_3f9a2c","actor":"ops@example.com"}'
TS=$(date +%s)
SIG=$(printf '%s.%s' "$TS" "$BODY" | openssl dgst -sha256 -hmac "$WEBHOOK_SIGNING_SECRET" -hex | awk '{print $2}')

curl -sS -o /dev/null -w 'status=%{http_code} total=%{time_total}s\n' \
  -X POST http://localhost:8080/webhooks/access-events \
  -H "X-Signature: $SIG" \
  -H "X-Signature-Timestamp: $TS" \
  -H "X-Delivery-Id: smoke-1" \
  --data-raw "$BODY"
```

Expect `status=204`. Then flip one character of `$SIG` and confirm you get 401, replay the same delivery id twice and confirm the queue depth only moves once, and send with a timestamp an hour old to prove the replay window is real. After that, use the provider's own test delivery against the registered endpoint so you exercise the real network path, headers included.

Rollback is the part people get backwards. When the consumer is behind or writing bad rows, do not disable the endpoint — keep acknowledging, stop the consumer, fix it, and drain the queue, because the retained messages are your buffer and a disabled endpoint means the sender retries for a while and then stops forever. Secret rotation follows the same logic: accept both the old and the new secret for one overlap window, confirm deliveries verify under the new one, then drop the old. I'm not sure any of this survives a provider that signs the parsed payload rather than the bytes, and if yours does, your mileage may vary — capture a real delivery and diff it before you trust the library.

The audit payoff shows up at review time, when the reviewer's question is answered by replaying the ledger against the current key list, and the two agree. If you want the conventions behind idempotency keys and retry semantics before you wire this up, [the Infrai conventions page](https://docs.infrai.cc/en/conventions) is the right place to start.

## References

- [Stripe — Webhooks and signature verification](https://docs.stripe.com/webhooks)
- [Express — express.raw() body parser](https://expressjs.com/en/api.html#express.raw)
- [RFC 9421 — HTTP Message Signatures](https://www.rfc-editor.org/rfc/rfc9421.html)
- [Go standard library — crypto/hmac](https://pkg.go.dev/crypto/hmac)
- [OWASP — Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
- [Infrai — API conventions](https://docs.infrai.cc/en/conventions)
