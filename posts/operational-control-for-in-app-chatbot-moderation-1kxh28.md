# Operational Control for In-App Chatbot Moderation Through an LLM JSON Schema API

Short answer: choose a chat API only if its schema-constrained output can sit behind an application-owned state machine with explicit timeout, retry, audit, and rollback behavior; without a dedicated moderation endpoint, the model is a basic classifier, not the safety boundary.

For an in-app chatbot, run two separate checks: screen the submitted message before generation, then screen the draft reply before publication. Validate both verdicts locally. A malformed or late verdict is `indeterminate`, never an implicit allow. The deciding constraint isn't the model leaderboard. It's whether the whole path remains predictable when a request is duplicated, delayed, cancelled, or replayed.

This is deliberately modest. Basic moderation can reduce exposure to ordinary unsafe content, but OWASP's guidance on LLM application risks reaches beyond content classification: prompt injection, sensitive-information disclosure, improper output handling, and excessive agency all require controls elsewhere in the application. A JSON schema narrows an output contract. It doesn't establish truth, intent, or authorization.

## How should an in-app chatbot API use LLM JSON schema without a moderation endpoint?

Give the screening call a small, closed contract. A useful verdict contains an allow decision, one category from a fixed set, and an internal reason code. Keep free-form explanations out of the authorization path. The application should reject missing fields, unknown categories, extra fields, oversized values, and any body that fails schema validation, even if a human could guess what the model meant.

Treat model output as hostile input — because the user can influence it indirectly through the text being classified. Never execute a returned string, splice it into a query, render it as trusted markup, or let it select tools. OWASP calls out improper output handling and excessive agency as distinct risks; schema validation helps at the parsing boundary, while ordinary authorization and output encoding still have to hold after parsing.

The sequence is a state machine, not a loose chain of API calls:

1. Accept a logical turn with an application-generated operation ID.
2. Apply deterministic checks such as byte limits, account state, attachment policy, and rate limits.
3. Classify the user input against a versioned policy and schema.
4. Generate a draft only after an allow verdict.
5. Classify the draft under the same release unit.
6. Commit the approved response and its decision record once.

Do not send a full conversation history merely because it is available. The screening prompt should receive only the context needed to apply the stated policy. Smaller context limits disclosure, makes fixtures easier to reason about, and gives an operator a better chance of reproducing the decision. Store stable identifiers and digests in general logs; put raw conversation text behind separate access and retention controls if review genuinely requires it.

There are three outcomes at either gate: `allow`, `deny`, and `indeterminate`. The third includes cancellation, deadline exhaustion, transport failure, invalid JSON, and schema mismatch. Mixing it with `deny` destroys operational visibility. Mixing it with `allow` silently removes the gate.

No silent bypass.

A public posting surface will often map `indeterminate` to fail closed. A low-risk private drafting feature may permit a restricted mode instead, such as saving without publishing. I'm not sure there is one correct default across products; the answer depends on abuse exposure, the harm of a false allow, and the cost of blocking legitimate use. Write the choice down before launch so an incident doesn't turn policy into an improvisation.

## Make duplicate delivery a harmless event

I've been paged for missed jobs and duplicate deliveries. That history changes the selection test: a provider request ID is useful evidence, but it cannot be the application's idempotency key, because a retry can receive a new provider ID while still representing the same user turn.

Use one stable operation ID across input screening, generation, output screening, and publication. Persist each verdict under a key such as `(operation_id, stage, policy_version)`, and make the visible assistant message unique on the operation ID. The final publish and its decision record belong in one database transaction. If the worker loses an acknowledgement after commit, replay should read the committed result rather than publish again.

Two writes are the danger.

Retries need the same discipline. Retry only outcomes classified as transient by the adapter, cap attempts, add jitter, and stop when the parent deadline expires. A policy denial is a completed decision, so retrying it is wrong. A schema-invalid response is indeterminate and should be observable as such; repeatedly asking until the classifier happens to say `allow` would turn retry policy into policy shopping.

This Go sketch keeps the external API behind a narrow interface. The store owns the important invariant: `CommitOnce` must enforce uniqueness in the same transaction that records the verdict and, at the output stage, publishes the response.

```go
package safetygate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"
)

type Stage string

const (
	InputStage  Stage = "input"
	OutputStage Stage = "output"
)

type Verdict struct {
	Allow      bool   `json:"allow"`
	Category   string `json:"category"`
	ReasonCode string `json:"reason_code"`
}

type Classifier interface {
	Classify(ctx context.Context, content, policyVersion string) (Verdict, error)
}

type Store interface {
	CommitOnce(ctx context.Context, operationID string, stage Stage, policyVersion, digest string, verdict Verdict) error
}

var allowedCategories = map[string]struct{}{
	"allowed": {},
	"abuse": {},
	"sexual": {},
	"violence": {},
	"self_harm": {},
}

func Screen(ctx context.Context, classifier Classifier, store Store, operationID string, stage Stage, content string) (Verdict, error) {
	if operationID == "" || content == "" {
		return Verdict{}, errors.New("invalid screening request")
	}

	callCtx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()

	const policyVersion = "basic-chat-v1"
	verdict, err := classifier.Classify(callCtx, content, policyVersion)
	if err != nil {
		return Verdict{}, errors.New("indeterminate screening result")
	}
	if _, known := allowedCategories[verdict.Category]; !known || verdict.ReasonCode == "" {
		return Verdict{}, errors.New("invalid screening verdict")
	}

	sum := sha256.Sum256([]byte(content))
	digest := hex.EncodeToString(sum[:])
	if err := store.CommitOnce(ctx, operationID, stage, policyVersion, digest, verdict); err != nil {
		return Verdict{}, err
	}
	return verdict, nil
}
```

The `1500ms` value is an example allocation, not a universal target. Derive the real value from the product's end-to-end latency objective, then reserve time for generation, the second screen, queueing, and commit. Your mileage may vary. What must not vary is cancellation propagation: once the parent operation is done, outbound work should stop and late results must not publish.

The category set above is illustrative too. A real policy needs definitions, examples, ownership, and a review path. Schema and policy versions should travel together; accepting an old category under a new prompt creates an audit record that looks precise while describing the wrong contract.

## Verify the gate before it controls traffic

Start with a replay corpus that contains synthetic cases and appropriately governed, redacted examples. Cover obvious violations, benign discussion of sensitive subjects, quotations, multilingual input, long repeated strings, and instructions that tell the classifier to ignore its task. Review disagreements. An old expected label is evidence, not an oracle.

Then shadow the new decision unit without changing what users see. Track invalid-schema results, indeterminate results, category distribution, false allows and false blocks found through review, and tail latency. Aggregate metrics shouldn't contain message bodies. Samples used for review need a collection purpose, access policy, and retention limit.

I use a four-column release check because it forces every happy-path claim to name its failure behavior:

| Control | Injection test | Required observation | Release blocker |
|---|---|---|---|
| Schema boundary | Missing field, unknown enum, extra field, truncated body | Result is `indeterminate` with policy and schema versions | Any malformed verdict reaches generation or publication |
| Deadline | Slow each stage independently | Cancellation reaches the adapter and no late publish occurs | Work continues beyond the parent operation |
| Idempotency | Replay the same operation before and after commit | One decision record per stage and one visible response | A duplicate response or conflicting verdict is committed |
| Audit | Look up a sampled decision from its operation ID | Operator can recover versions, stage, digest, and outcome | The decision cannot be reconstructed without broad raw-log access |

Test changes as a bundle: model identifier, screening instruction, JSON schema, category definitions, and application mapping. Changing one can alter the meaning of the others. A canary should therefore report the bundle version, not merely the model name.

Selection follows from this exercise. Favor an API surface that can request schema-constrained output, respects cancellation and deadlines through your client, returns traceable request metadata, and can be isolated behind a replaceable adapter. Measure those behaviors with the same concurrency, payload distribution, and timeout budget expected in production. Documentation is necessary; a replayable test is the acceptance criterion.

## Roll back the decision unit, not one prompt

Rollback must restore the prior model, instruction, schema, and policy mapping together. Preserve operation IDs during the change so queued and retried work remains deduplicated. Quarantine work whose recorded policy version is unavailable; don't reinterpret it under whichever version happens to be live.

The catch is availability coupling. With two synchronous screening calls, either one can consume the interaction's deadline, and fail-closed policy can stop chat when no valid verdict arrives. A queue can help an asynchronous workflow, but it is unsuitable when queued work will outlive an interactive request. Shed load visibly or retain a pending state. Never turn off validation as a quick rollback.

General-purpose LLM screening is also not suitable when regulation, child safety, emergency escalation, or a contractual control requires specialized detection or human review. In those cases, use the required dedicated system and an audited escalation path. Keep deterministic authorization outside the model in every case.

The operational go/no-go question is plain: can the team replay one logical turn, explain each stored decision, prove that retries cannot publish twice, and restore the prior decision unit within its recovery objective? If not, the API is not ready for this role, regardless of how clean its demo output looks.

## References

- OWASP, *Top 10 for Large Language Model Applications*: https://owasp.org/www-project-top-10-for-large-language-model-applications/
- OpenRouter documentation, included as an example of a documented multi-model API surface rather than a recommendation: https://openrouter.ai/docs
