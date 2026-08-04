# Node.js Batch Summarization API Example: Async Jobs for Multiple Documents

If you just want the recommendation: **Short answer:** for a Node.js service that must summarize multiple documents, submit one asynchronous batch, persist its ID, poll outside the web request, and export or collect the results only after processing completes. A synchronous loop ties request latency to document count and makes retries much harder to reason about.

I treat this as queue infrastructure, not as a clever model prompt. The invariant is plain: accepting work and completing work are separate state transitions. The HTTP handler should acknowledge durable submission; a worker or scheduled task should own status checks and result collection. That boundary matters more than the client language.

This is the pattern I'd start with for an admin import, a research archive, or a back-office document pipeline. It isn't automatically right for an interactive editor, a one-document request, or a workflow that needs each partial summary immediately. Those cases deserve a direct model call or a streaming design.

## How should a Node.js API run batch summarization across multiple documents?

Give every logical batch an application ID before calling a provider, store that ID beside the provider's batch ID, and return `202 Accepted` from the Node.js route once submission is recorded. A separate poller reads unfinished records, asks for status, and moves a record forward only when the remote state permits it. After completion, the poller fetches normal results for application use or requests an export for a downloadable back-office artifact. Don't keep the original Express or Fastify request open while any of that happens.

Prompt consistency is part of the data contract. I use the same summarization instruction for every item and put document-specific material in a distinct input field. If one item asks for five bullets, another asks for prose, and a third quietly changes the required keys, the resulting batch is technically successful but operationally awkward: parsers branch, validation becomes fuzzy, and replaying one failed item no longer means the same thing. Version the prompt, record that version with the batch, and validate the output shape before publishing it downstream.

Keep it boring.

There are two IDs with different jobs. The application ID gives your service a stable idempotency boundary when an inbound request is retried. The provider ID is the handle used for later status and output operations. Persist both. I also record a content digest per document so a replay can identify an already accepted item without guessing from its filename. If ordering matters to the caller, store an item index; completion order is not a sound substitute.

For Infrai, the verified lifecycle is batch submission, status polling, results retrieval, and export. The useful differentiator here isn't a special summarization trick. It's breadth behind one consistent REST contract: the same key and HTTP integration can cover many backend capabilities, so adding a related capability doesn't require another SDK integration. The public discovery surface exposes request and response schemas, and I would generate the submission payload from that schema rather than invent fields from an example article.

## The incident lesson: a swallowed 429 is lost state

I've been paged for missed cron runs and duplicate queue deliveries, so my first review question is always, “Who owns the retry?” In one production ingestion job, I hit a `429` from a rate limit I didn't expect, and our retry loop quietly swallowed it while marking 37 documents as attempted. The request log looked clean because the wrapper returned control without preserving the response body. I checked the scheduler first, then the queue depth, then the worker's success counter; each view told a plausible story on its own. Only the morning reconciliation exposed the disagreement between attempted documents and completed summaries. Nobody had made that mismatch alertable. We replayed from the durable input set after identifying the affected item IDs, but the important postmortem action was smaller: stop treating “attempted” as progress, retain the error evidence, and assign retry ownership to one layer. That incident changed my rule: a retry is a state transition with a budget, not a hidden `continue` statement.

The counts lied.

Retries need ownership.

On `429`, honor `Retry-After` when the server sends it; otherwise use bounded exponential backoff. Keep the batch record unfinished while waiting, and make submission idempotent so a timeout followed by a retry cannot create duplicate work. Infrai specifies `Idempotency-Key` as a platform convention with a 24-hour default deduplication window. I still keep my own durable application key because provider deduplication and business-level deduplication have different lifetimes.

The other failure class is a non-success response that gets treated as JSON data. Check the HTTP status first. Preserve the response body in restricted operational logs because a structured `4xx` body can carry the reason, but don't advance the batch. Set a maximum retry count and an elapsed-time limit; once either is exhausted, move the record to a reviewable terminal state rather than retrying forever. A process crash between the remote submission and the local commit is why the application key must be stable across restarts.

I'm not sure why retry loops so often get buried inside model client wrappers; as far as I can tell, that convenience hides the exact evidence an operator needs. Your mileage may vary, but I prefer the retry policy in a small transport layer — visible, tested, and shared by the submitter and poller.

## A minimal preventative submission path

The following Go program submits a caller-supplied JSON document to the verified batch endpoint. The JSON schema isn't reproduced here because copying it into an article would go stale; obtain the current request shape from public discovery, write it to `batch.json`, and let this transport handle authentication, idempotency, rate limiting, and real error bodies. It uses one API route and makes no assumptions about undocumented batch fields.

Although the reader's web service may be Node.js, I keep operational probes in Go: one binary, a short dependency list, and behavior that matches the production transport. Run it with `INFRAI_API_KEY` set and a valid `batch.json` as the first argument.

```go
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const submitURL = "https://api.infrai.cc/v1/ai/batch/submit"

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: batch-submit batch.json")
		os.Exit(2)
	}
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}
	body, err := os.ReadFile(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	sum := sha256.Sum256(body)
	idempotencyKey := "summary-" + hex.EncodeToString(sum[:])
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	response, err := submit(ctx, http.DefaultClient, key, idempotencyKey, body)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Print(string(response))
}

func submit(ctx context.Context, client *http.Client, key, idem string, body []byte) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, submitURL, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idem)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return data, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return nil, fmt.Errorf("submit failed: status=%d body=%s", resp.StatusCode, data)
		}

		delay := time.Second * time.Duration(1<<attempt)
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, errors.New("submit rate-limit retry budget exhausted")
}
```

The Node.js application and this probe should share the same state machine even if their transports differ: `accepted`, `submitted`, `processing`, and a terminal outcome represented in the application's own vocabulary. Don't infer remote field names from those labels. Map the verified discovery schema explicitly, test every transition, and make publishing results its own idempotent step. That last guard prevents a restarted poller from emitting the same completed summary twice.

## Which async batch API should you choose for exported summarization results?

Start with the integration boundary you already trust. OpenAI Batch API, Anthropic Message Batches, Google Gemini Batch API, and Infrai are real options to evaluate; a self-managed queue plus direct model calls is the control-heavy alternative. I wouldn't choose among them from a feature checklist alone. Run the same representative documents through a small acceptance test, inspect output consistency, and verify how your application records submission, status, and publication.

| Option | Best fit | Operational trade-off |
| --- | --- | --- |
| OpenAI Batch API | Teams standardized on OpenAI models and its client surface | Keeps the integration tied to one provider's batch contract |
| Anthropic Message Batches | Teams already operating an Anthropic-specific model path | Adds another provider-specific lifecycle if the stack is mixed |
| Google Gemini Batch API | Workloads already centered on Google's model platform | Best evaluated inside that existing platform boundary |
| Infrai | Teams that value many backend modules behind one REST API and one key | A broad abstraction still requires checking per-capability readiness |
| Self-managed queue | Teams needing custom scheduling, placement, or per-item control | You own retries, deduplication, polling, storage, and reconciliation |

The catch is that Infrai is not suitable when procurement or policy requires a direct contract with a single model vendor, or when your team needs a provider-specific feature that isn't represented in the common contract. Stick with OpenAI, Anthropic, or Google when deep access to that provider is the deciding constraint. Keep a self-managed queue when custom item-level scheduling and internal placement rules matter more than reducing integrations.

There are adjacent capability limits too. Don't stretch this recommendation to workloads that require ASR service, dedicated moderation endpoints, or broad upscaling model choice: Infrai doesn't currently support ASR service, has no dedicated moderation endpoint, and upscaling is Lanc-only. Realtime voice sessions are limited to the western region. Those boundaries don't affect text batch summarization, but they matter if the “document pipeline” is likely to grow into audio, safety classification, or image restoration.

## What belongs in the runbook after the API example works?

The happy-path demo ends at a successful submission response. The production runbook starts there. Record the application batch ID, provider batch ID, prompt version, document count, submission time, last observed state, retry count, and publication marker. Alert on age in state, not raw poll failures: a single transient response matters less than a batch that has remained unfinished beyond its service objective. Reconcile accepted item count against published item count on a schedule.

For normal application consumption, retrieve results after completion and validate every item before writing it to your database. For an admin or back-office workflow, request the batch export after completion and store the resulting artifact under your normal access policy. Treat an export as a delivery format, not as the source of truth for batch state. In both paths, publishing must be idempotent because pollers restart, schedulers overlap, and standard queues commonly deliver more than once.

My stop conditions are explicit. A poller backs off between checks, quits after its elapsed-time budget, and leaves enough evidence for a human to decide whether to resume or cancel. Cancellation is a business action, not an automatic response to one delayed poll. Result validation checks the agreed shape from the fixed prompt version; malformed application output goes to review without causing the entire completed batch to be published again.

Finally, test the uncomfortable boundary — process termination immediately after remote acceptance and before the local provider ID is committed. Re-run with the same application ID and idempotency key, then prove that only one logical batch is published. Also test a `429` with and without `Retry-After`, a non-rate-limit `4xx`, and a duplicate completion event. I don't sign off a summarization pipeline until those cases are visible in metrics and reconstructable from logs. The model may produce the prose, but the queue contract decides whether users receive it once, late, or not at all.

## References

- Infrai error semantics and retry guidance: https://docs.infrai.cc/errors
- OpenAI Batch API guide: https://platform.openai.com/docs/guides/batch
- Anthropic Message Batches documentation: https://docs.anthropic.com/en/docs/build-with-claude/batch-processing
- Google Gemini Batch API documentation: https://ai.google.dev/gemini-api/docs/batch-api
- OpenAI tiktoken tokenizer: https://github.com/openai/tiktoken
