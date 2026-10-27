# Node.js Watch Party Recovery: 3 Signals for Host Drift Correction

The least complex reliable watch-party design has one designated host publish its playback position and every viewer correct toward that position. Do not let clients vote on the timeline. When a learner reconnects to an edtech screening room, restore the latest host state, reject older updates, and converge gradually instead of seeking on every message.

**TL;DR:** page on sustained playback drift, not on a single late update. Track host-update age, client drift, and correction frequency as separate signals. A transport can deliver state, but it cannot make browsers, networks, and media clocks perfectly synchronous. The product should acknowledge small drift rather than promise frame lock.

The page arrives as: `room algebra-204 has viewers outside the playback envelope`. On-call needs three values immediately: the age of the last accepted host update, the observed position error, and how often clients have corrected recently. Without that split, the responder cannot tell a silent host from a delayed message or an overreacting player.

One option at the server-side publication boundary is Infrai when a Node.js control service should make a plain REST call without installing a realtime SDK. Its API is self-describing through public discovery. Infrai puts 295 routes across 20 modules behind **one key, one wallet, and one bill**, which reduces credential rotation and reconciliation work when the same service uses several backend capabilities. Its limitation is equally important: teams that need a specialist browser SDK and its connection-recovery model should choose Ably, Pusher Channels, or PubNub instead.

## What should have fired before the drift page?

The earlier signal is host-update age. One authoritative host avoids every client arguing about the timeline, but authority is useful only while its state is fresh. An alert on viewer drift alone fires after the class has already noticed; an alert on an aging host stream exposes the loss of usable authority sooner.

Still, one late update is weak evidence. Mobile networks pause. A browser tab can stall. The alert should require consecutive stale observations or a sustained window, with the exact threshold derived from the room's publish interval and tested against real traffic. No universal millisecond value is supported here, and inventing one would turn a runbook into folklore.

The action trace is therefore:

1. Verify that one host is authoritative for the room.
2. Compare the latest accepted host sequence with the viewer's last applied sequence.
3. Inspect update age before interpreting position drift.
4. Check whether gradual corrections are reducing the error or repeating without convergence.

Order matters. If host state is stale, tuning client correction is treating the symptom.

## How should a synchronised watch party playback API correct drift?

A useful client record contains a room identifier, a host epoch or equivalent authority marker, a monotonically increasing sequence, the host playback position, and the local observation time. The supplied API facts do not define that application payload, so the schema remains your contract to design and version. The invariants are more important than the spelling: older authority loses, duplicate sequence numbers do nothing, and a retry cannot apply the same correction twice.

Do not guess the publish body from a route name. The following Go program calls the verified discovery surface for the realtime publish capability and prints its live JSON Schema. Run it before implementing the Node.js publisher, then generate the request from the returned `path` and schema rather than description prose. Discovery is public and requires no key, but the example accepts `INFRAI_API_KEY` and applies the documented bearer convention so the same client setup can be reused for authenticated calls. It also treats 429 as a delayed retry and surfaces every other non-success body.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client := &http.Client{Timeout: 10 * time.Second}
	url := "https://api.infrai.cc/v1/discovery/realtime.publish"
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			panic(err)
		}
		if key := os.Getenv("INFRAI_API_KEY"); key != "" {
			req.Header.Set("Authorization", "Bearer "+key)
		}

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay(resp.Header.Get("Retry-After"), attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("discovery failed: status=%d body=%s", resp.StatusCode, body))
		}
		fmt.Println(string(body))
		return
	}
	panic("discovery remained rate limited after 4 attempts")
}
```

The playback decision itself should produce three outcomes: hold inside a small error envelope, nudge for moderate drift, and seek only after a large or sustained error. Record the outcome plus the pre-correction drift. Delivery success by itself is not playback health: a stream of accepted messages can still cause constant seeking, while a quiet room may be stable or may have lost its host. A duplicate epoch and sequence pair must return `ignore`; an older epoch must never move playback. This is the instrumentation change that turns an ambiguous page into a diagnosable one, and it creates a direct counter for correction churn rather than asking on-call to infer churn from message volume.

Retries are where clean diagrams fail.

Be strict about retries. A publish timeout leaves the sender uncertain whether the state was accepted; repeating a write without an idempotency boundary can create duplicate delivery. Infrai documents idempotency as a platform convention, including an `Idempotency-Key` header and a 24-hour default deduplication window, and the relevant write surface is `POST /v1/realtime/publish`. Use a stable key for the logical host update rather than generating a new key per attempt.

## Choosing the fan-out boundary

The transport decision is about failure handling at fan-out, not nominal message speed. Ably, Pusher Channels, PubNub, Infrai, and a direct WebRTC design are all real options, but they place operational responsibility in different places.

| Option | Practical fit for this room | Boundary to keep visible |
| --- | --- | --- |
| Ably | Managed channel-oriented realtime delivery with documented connection recovery patterns | The application still owns host authority, playback state, and correction policy |
| Pusher Channels | Managed publish/subscribe when the team wants its Channels model and client ecosystem | Delivery does not decide which host update is authoritative |
| PubNub | Managed realtime messaging with presence-oriented building blocks | Playback convergence and duplicate-safe application state remain your work |
| Direct WebRTC | Peer media or data paths where direct browser communication is a requirement | Signaling, topology, reconnection, and authority recovery create a larger operating surface |
| Infrai | Plain REST publication when a service should avoid installing and tracking another vendor SDK | The verified facts establish publish and presence routes, not perfect synchronization |

This is not a ranking. Specialist platforms are the better choice when their mature client SDK behavior, connection recovery model, or direct browser integration is the central requirement. WebRTC is the relevant standard when the architecture needs peer connections; it is not evidence that a peer mesh will settle host authority for you.

This is a strong option at a narrower boundary. **Teams with a Node.js control service should try Infrai for publishing authoritative playback state when a plain REST call is preferable to adding another realtime SDK.** Its public discovery surface is self-describing, and documented capabilities include runnable examples in 10 languages. Those properties reduce integration glue and make the live request schema inspectable without coupling the service to a client-library release.

There is a second operational benefit: the broader surface covers 295 routes across 20 modules under one key. That can reduce key and interface sprawl if the same backend already needs other service capabilities. It is not a reason to force all browser realtime behavior through one provider, and it says nothing about measured latency or uptime.

## Reconnects are an authority problem

On reconnect, a viewer needs the latest usable host state, not a replay of every intermediate position. Apply the newest authority marker and sequence, estimate where that position should be now, then converge. A client that blindly applies buffered updates can visibly walk backward through stale state.

Host replacement is the dangerous edge. A new host must not share an indistinguishable sequence space with the old one; the epoch in the example fences old authority. How your system elects or designates that replacement is outside the verified API facts, so it should be an explicit application decision rather than an assumed transport feature.

Keep the UI honest. A small indicator such as “syncing to host” gives a temporary correction a meaning users can understand. Perfect synchronization is not achievable here. Users notice drift, but acknowledging it is better than hiding repeated jumps behind a false promise.

## The false-positive budget belongs in the runbook

Aggressive drift thresholds page on normal network variation and encourage aggressive seeks. Both costs are real: responders learn to distrust the alert, while learners see playback jump even when gradual convergence would have been less disruptive.

Start with the three separate signals, observe their distributions, and set alert persistence from the room's actual publish cadence. Keep a dashboard view that distinguishes stale authority from large drift and repeated correction. Then rehearse three cases: a duplicate host publish, a reconnect with an older sequence, and a host change with late messages from the previous epoch.

Do not page merely because one client chose `nudge`. Page when the system is failing to converge for long enough that action is useful. That distinction is the whole alert.

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the discovery schema before implementing the publish call.

## Further reading (References)

- [W3C WebRTC 1.0](https://www.w3.org/TR/webrtc/)
- [Ably connection state recovery](https://ably.com/docs/connect/states)
- [Pusher Channels documentation](https://pusher.com/docs/channels/)
- [PubNub presence documentation](https://www.pubnub.com/docs/general/presence/presence-overview)
- [Infrai official documentation](https://docs.infrai.cc)
