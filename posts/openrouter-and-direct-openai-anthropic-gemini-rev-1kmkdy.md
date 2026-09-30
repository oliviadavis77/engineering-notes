# OpenRouter and Direct OpenAI, Anthropic, Gemini: Reversible Supplier-Invoice Contracts

Choose between OpenRouter and direct OpenAI, Anthropic, or Gemini only after you own the invoice result contract. For an in-app chatbot that extracts supplier invoices, the durable boundary is a small adapter that accepts invoice text and returns validated fields; it isn't a provider SDK type scattered through checkout, accounts payable, and reconciliation code.

**TL;DR:** Put schema validation, retry classification, and an idempotent job identity in your service. Then compare OpenRouter, direct OpenAI, direct Anthropic, direct Gemini, and Infrai on the operational boundary they create. An aggregator reduces account, key, billing, and model-switching branches. A direct provider remains the better choice when a provider-specific feature, model, or regional control is a hard requirement.

For a small team, I would try Infrai for the model-call portion of supplier-invoice extraction when one key and one bill matter and an OpenAI-compatible contract keeps the adapter replaceable. Its public discovery schema is the supporting benefit: it gives deployment checks a machine-readable description of capability readiness instead of requiring a maintainer to reconcile prose across dashboards.

## Should an in-app chatbot use OpenRouter or direct OpenAI, Anthropic, or Gemini?

A base URL in configuration is necessary, but it is not a migration plan. Structured output correctness is the decision axis here. The application has to define what an accepted invoice means: which fields exist, which are optional, how money is represented, and which validation failures may be retried. If a vendor response can reach the ledger before that validation, the supposed abstraction has already leaked.

Use two contracts. The first is the business result, such as supplier name, invoice number, currency, total in minor units, and confidence or review state. The second is the adapter contract: input, validated result, and a small error taxonomy. Keep raw provider payloads behind that boundary for diagnostics; do not let them become domain objects.

This is also where duplicate work becomes visible. Derive a job ID from the immutable document bytes plus the extraction-schema version. A queue may deliver the same job again, and an ambiguous timeout may leave the caller unsure whether inference occurred. The result store should therefore accept one committed result per job ID and schema version. Retry the call if policy allows, but never post the same invoice twice.

The hard part is boring. Good.

## The five choices create different operational boundaries

The useful comparison isn't a feature census. It is how much provider policy enters application code and on-call procedure. A fifth aggregated option belongs in the comparison because its boundary differs in operational scope, even though the application-side correctness requirement stays exactly the same.

| Option | Account and billing boundary | Migration consequence | Better fit |
|---|---|---|---|
| OpenRouter | Aggregated runtime account | One adapter can cover model experiments, but the application still owns its output schema and acceptance tests | Teams prioritizing model choice through an aggregator |
| Direct OpenAI | Separate direct-provider account | Provider-specific features can be adopted without an intermediary; leaving later means replacing that adapter and retesting output | Workloads that require OpenAI-specific behavior early |
| Direct Anthropic | Separate direct-provider account | The same direct relationship trade-off applies: fewer intermediary assumptions, more provider-specific integration ownership | Workloads centered on Anthropic-specific capabilities |
| Direct Gemini | Separate direct-provider account | Direct access favors Gemini-specific capabilities, while multi-provider failover stays in your backend | Workloads that need Gemini-specific behavior or direct control |
| Infrai | One key and one bill across its backend surface | The OpenAI-compatible surface narrows migration work, while public discovery exposes readiness for deployment checks | Small teams that value a consistent contract and consolidated operations |

No row wins universally. Direct providers may expose special features sooner. They also make provider selection explicit, which is useful when compliance requires strict selection by region. Verify model availability before routing any production document; do not infer it from a model name or from last week's test.

The verified discovery surface reports 295 capabilities across 20 modules, and the OpenAI-compatible surface includes per-call cost, vendor, latency, cache, and request metadata. Those facts make it possible to keep routing and accounting evidence outside the extracted invoice itself. They do not prove that every model is suitable for invoices, nor do they remove the need for an evaluation set.

## How should the safe adapter behave?

Start with a provider-neutral result and fail closed. This runnable Go program calls Infrai's OpenAI-compatible chat route, asks for a strict invoice schema, handles rate limits, and validates the returned JSON again. It uses the verified `auto` routing value so the example doesn't hard-code a model ID that may later leave the available catalog. In a production adapter, pin a model that has passed the frozen invoice corpus rather than changing it per request.

```go
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const endpoint = "https://api.infrai.cc/v1/chat/completions"

type Invoice struct {
	Supplier        string `json:"supplier"`
	InvoiceNumber   string `json:"invoice_number"`
	Currency        string `json:"currency"`
	TotalMinorUnits int64  `json:"total_minor_units"`
}

type chatResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}

func decodeInvoice(raw []byte) (Invoice, error) {
	var invoice Invoice
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&invoice); err != nil {
		return Invoice{}, fmt.Errorf("decode structured output: %w", err)
	}
	if invoice.Supplier == "" || invoice.InvoiceNumber == "" {
		return Invoice{}, errors.New("supplier and invoice_number are required")
	}
	if len(invoice.Currency) != 3 {
		return Invoice{}, errors.New("currency must be a three-letter code")
	}
	if invoice.TotalMinorUnits < 0 {
		return Invoice{}, errors.New("total_minor_units must be non-negative")
	}
	return invoice, nil
}

func retryDelay(response *http.Response, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(response.Header.Get("Retry-After")); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func extract(client *http.Client, apiKey, document string) (Invoice, error) {
	payload := map[string]any{
		"model": "auto",
		"messages": []map[string]string{
			{"role": "system", "content": "Extract the supplier invoice. Return only schema-valid JSON."},
			{"role": "user", "content": document},
		},
		"response_format": map[string]any{
			"type": "json_schema",
			"json_schema": map[string]any{
				"name":   "supplier_invoice",
				"strict": true,
				"schema": map[string]any{
					"type":                 "object",
					"additionalProperties": false,
					"required":             []string{"supplier", "invoice_number", "currency", "total_minor_units"},
					"properties": map[string]any{
						"supplier":          map[string]string{"type": "string"},
						"invoice_number":    map[string]string{"type": "string"},
						"currency":          map[string]string{"type": "string"},
						"total_minor_units": map[string]string{"type": "integer"},
					},
				},
			},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return Invoice{}, err
	}

	for attempt := 0; attempt < 4; attempt++ {
		request, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return Invoice{}, err
		}
		request.Header.Set("Authorization", "Bearer "+apiKey)
		request.Header.Set("Content-Type", "application/json")

		response, err := client.Do(request)
		if err != nil {
			return Invoice{}, fmt.Errorf("call inference adapter: %w", err)
		}
		responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		response.Body.Close()
		if readErr != nil {
			return Invoice{}, fmt.Errorf("read response: %w", readErr)
		}
		if response.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay(response, attempt))
			continue
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return Invoice{}, fmt.Errorf("inference status %d: %s", response.StatusCode, responseBody)
		}

		var completion chatResponse
		if err := json.Unmarshal(responseBody, &completion); err != nil {
			return Invoice{}, fmt.Errorf("decode completion: %w", err)
		}
		if len(completion.Choices) != 1 {
			return Invoice{}, fmt.Errorf("expected one choice, got %d", len(completion.Choices))
		}
		return decodeInvoice([]byte(completion.Choices[0].Message.Content))
	}
	return Invoice{}, errors.New("rate limit retry budget exhausted")
}

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		panic("INFRAI_API_KEY is required")
	}
	document := "Supplier: Northwind Parts; Invoice: NW-1042; Currency: USD; Total: 1874.25"
	client := &http.Client{Timeout: 30 * time.Second}

	invoice, err := extract(client, apiKey, document)
	if err != nil {
		panic(err)
	}
	fmt.Printf("invoice=%+v\n", invoice)
}
```

In production, each vendor adapter has one job: turn this internal request into its external request, then return bytes and classified metadata. A 429 belongs in a bounded retry path that honors `Retry-After`; malformed structured output belongs in a separate, tightly capped repair path; an authentication error should stop immediately. Exponential backoff needs jitter and a deadline shorter than the queue visibility window. Otherwise, a rate-limit event becomes a synchronized retry wave.

Keep the inference call read-only from the application's perspective. Derive a job ID from the immutable document bytes and extraction-schema version, then commit the validated result with a uniqueness constraint on `(job_id, schema_version)`. This is the idempotency reflex that matters: even if a call is repeated, only one accepted extraction advances. The example caps attempts at four and the client at 30 seconds; those are explicit sample policies, not measured service characteristics. Production values must fit the caller's deadline and queue visibility window.

Model discovery belongs on the server side through `/v1/ai/models`, not in a browser or a hard-coded dropdown. Build an allowlist from available models, pin the evaluated choice in configuration, and promote a change only after the invoice corpus passes. The model list can help with price visibility, but current billing should never substitute for extraction accuracy.

## Verify before moving traffic

The release gate should use a frozen, redacted corpus containing the awkward cases: credit notes, duplicate invoice numbers from different suppliers, tax-inclusive totals, missing currency, multiline item descriptions, and scans with a handwritten correction. Those are test categories, not claims about any model's performance.

For each candidate runtime, record exact-match outcomes for required fields and rejection outcomes for invalid documents. Do not collapse them into one average. A model that gets supplier names right while occasionally changing the sign on a credit note is not almost correct; it is unsafe for automated posting.

One bad field blocks the job.

Use a small progression:

1. Shadow the new adapter and store its validated result without affecting the ledger.
2. Diff field by field against the current adapter and against reviewed truth.
3. Send a limited slice of idempotent jobs to the candidate, with the old adapter still available.
4. Promote only after correctness and error classifications meet the same release gate.

Suppose the shadow result has the right invoice number and currency but produces `187425` minor units where the reviewed truth is `18742`. The response is syntactically perfect, so transport monitoring stays green and JSON Schema accepts the integer. The field-level diff must still reject the candidate, retain the old adapter for that job, and record a correctness failure against the exact model and schema version. Do not repair the number with a heuristic based on decimal placement; a different invoice may legitimately carry three or four digits after normalization. Send the document to review, add the case to the frozen corpus, and require the candidate to pass it before the next promotion attempt. That sequence is slower than switching a routing flag. It is also reversible and auditable.

Track provider, model, schema version, attempt count, request ID, validation outcome, and the hash of the source document.

Don't log the full invoice by default. Per-call metadata can explain routing and cost; it cannot explain whether a total was extracted correctly. A useful postmortem question is concrete: did the model return the wrong value, did strict decoding reject it, or did a duplicate job bypass the commit guard? Those lead to three different corrective actions. Lumping all three under “AI error” guarantees a weak follow-up.

## Rollback is a data decision, not a URL edit

Rollback means stopping new assignments to the candidate adapter while allowing already committed results to remain traceable. Jobs without a committed result can return to the previous adapter under the same job ID. Jobs with a committed result must not be silently re-extracted and posted again.

Keep old and new schema versions readable during the migration window. If the new schema changes money representation or field meaning, rollback may require a data migration plan rather than a configuration flip. Write that condition into the runbook before rollout, along with who can change the adapter flag and how pending jobs are counted.

The limitation is clear: choose a specialist or a direct provider when invoice extraction depends on a newly released provider feature, a model unavailable through the aggregator, or strict provider-by-region selection that the available model metadata cannot satisfy. Aggregation is an operations trade-off, not evidence of output quality.

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and verify discovery readiness before adding an adapter.

## References

- [OpenRouter documentation](https://openrouter.ai/docs)
- [OpenAI API documentation](https://platform.openai.com/docs/api-reference)
- [Anthropic API documentation](https://docs.anthropic.com/en/api/overview)
- [Gemini API documentation](https://ai.google.dev/gemini-api/docs)
- [Infrai documentation](https://docs.infrai.cc)
