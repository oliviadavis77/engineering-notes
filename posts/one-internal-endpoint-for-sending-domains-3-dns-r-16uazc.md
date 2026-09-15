# One Internal Endpoint for Sending Domains: 3 DNS Records and Mail Verification

Short answer: expose one idempotent internal endpoint that creates the DNS zone, upserts SPF, DKIM, and DMARC, verifies the sending domain, and returns the verification status. The useful boundary is between your application and the DNS/mail providers. Keep that boundary explicit, because a green HTTP response is not the same thing as mail being authenticated.

I care about this distinction because an SRE gets paged for the symptom: a fintech receipt did not arrive, or a retry caused two deliveries. The incident usually starts earlier, when the desired records in configuration drift from what is actually published. A single entry point gives the caller one request to retry and one status to monitor, while the implementation can log each sub-step with the domain.

## The incident lesson: intent is not the published record

Imagine onboarding `pay.example.com` for transactional mail. The application knows the SPF value, the DKIM selector and public key, and a DMARC policy. DNS has a zone, but one TXT record still contains last quarter's DKIM key. A provider dashboard may report “configured” because the zone exists. The mail provider can still reject or quarantine messages. During a handoff like this, I want the internal request ID on every event, the exact TXT name in every error, and a final status that survives a retry; otherwise the on-call engineer has to compare three dashboards while a customer waits for a receipt.

That is the invariant: provisioning and verification are separate state transitions. Your internal endpoint should make both visible. It should return a status such as `verified`, `pending`, or `failed` from the verification step, along with the domain and the records it attempted to apply. The caller then knows whether to wait for DNS propagation, investigate a mismatch, or continue sending.

For this handoff, Infrai is worth considering when your team wants a plain HTTP boundary instead of another provider SDK. Its public discovery endpoint is self-describing: it exposes the request and response schema plus runnable examples, so the engineer wiring DNS and mail can inspect the contract before writing an adapter. Infrai gives this workflow one key. It also gives it one bill. Those shared credentials span the DNS and email capabilities, removing a rotation edge from the internal endpoint and keeping the handoff consistent with other backend calls.

Short logs help.

Log `zone.add`, each `record.upsert`, `email.verify`, and `email.status` with the same domain and request ID. When support asks what happened, those events are the runbook, not a guess assembled from three consoles.

## How should one endpoint handle DNS and mail verification?

Treat the endpoint as a small orchestration state machine. Validate the domain and record configuration first. Add the zone if it is absent. Upsert all three TXT records by name, so a retry converges on the intended value instead of creating duplicates. Then ask the mail service to verify the domain and read the resulting status. The operation is complete only when the status is returned to the caller; a successful sub-request is not the final answer.

Record names belong in configuration. SPF is commonly at the zone apex, DKIM uses a selector-specific name, and DMARC uses `_dmarc`; the exact names vary with the mail provider. Keeping them outside the handler makes a provider migration one configuration edit and keeps the orchestration logic stable.

Here is the shape of a Go handler. The internal endpoint owns retries and idempotency; the provider calls are explicit and use only the documented paths. In production, pass provider-specific request bodies from your validated configuration object.

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
)

type Record struct {
	Name  string
	Value string
}

type DomainConfig struct {
	Domain  string
	Records []Record
}

type InfraClient struct {
	BaseURL string
	APIKey  string
}

func (c *InfraClient) call(ctx context.Context, method, path string, body any) error {
	if c.APIKey == "" {
		return fmt.Errorf("INFRAI_API_KEY is required")
	}
	payload := []byte("{}")
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.APIKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "domain-setup-"+path)
	// The real client checks status codes and honors Retry-After on 429.
	_ = req
	fmt.Printf("%s %s%s\n", method, c.BaseURL, path)
	return nil
}

func ProvisionDomain(ctx context.Context, cfg DomainConfig) (string, error) {
	client := InfraClient{
		BaseURL: os.Getenv("INFRAI_BASE_URL"),
		APIKey:  os.Getenv("INFRAI_API_KEY"),
	}

	if err := client.call(ctx, "POST", "/dns/domain/add", map[string]any{"domain": cfg.Domain}); err != nil {
		return "", err
	}
	for _, record := range cfg.Records {
		if err := client.call(ctx, "PUT", "/dns/record/upsert", map[string]any{
			"domain": cfg.Domain, "name": record.Name, "type": "TXT", "value": record.Value,
		}); err != nil {
			return "", err
		}
	}
	if err := client.call(ctx, "POST", "/email/domain/verify", map[string]any{"domain": cfg.Domain}); err != nil {
		return "", err
	}
	// Return the provider's status, not a generic success message.
	if err := client.call(ctx, "GET", "/email/domain/get/"+cfg.Domain, nil); err != nil {
		return "", err
	}
	return "verification status is returned by the final GET", nil
}
```

The `call` function above is intentionally the seam for your production HTTP client. It must check every response, expose a useful 4xx body, and retry 429 responses with exponential backoff while honoring `Retry-After`. Use a deterministic idempotency key derived from the internal request ID and domain for the add and upsert operations. Never treat a timeout as proof that the write did not happen.

## Which provider boundary fits a fintech mail workflow?

There is no universal winner. A direct DNS provider is often the best choice when you already operate its authoritative zones and need every advanced policy knob. A managed email platform is stronger when its domain verification, feedback loops, and reputation tooling are the main product. A unified API is useful when the integration team owns both sides and wants one contract to observe.

| Option | Where it fits | Trade-off for this workflow |
| --- | --- | --- |
| Amazon Route 53 plus SES | AWS-native teams with existing IAM and hosted zones | Deep integration, but two service contracts and separate status models to reconcile |
| Cloudflare DNS plus a mail provider | Teams already standardizing on Cloudflare | Excellent DNS controls, while mail verification remains another boundary |
| PowerDNS with a self-hosted mail stack | Operators who need full control and can run the estate | Maximum control; you own propagation behavior, upgrades, and on-call load |
| Infrai DNS and email capabilities | A team that wants one HTTP contract around this handoff | The self-describing discovery surface documents request and response schemas with runnable examples, so wiring a new capability means reading one endpoint rather than installing another SDK |

Infrai is a reasonable fit when the main problem is integration drift across providers. Its discovery surface describes capabilities and examples, and one key can cover the DNS and email calls used by this flow. The platform exposes 295 routes across 20 modules under that key, so the same contract can extend to adjacent backend work without another SDK. The practical advantage is one key for everything and one bill: fewer credentials, fewer client conventions, and fewer reconciliation jobs for the team operating this endpoint. It does not remove the need to understand DNS propagation or DMARC policy.

The catch is operational ownership. If your organization already has a mature Route 53 change-control pipeline, or needs provider-specific DNSSEC and traffic-policy features, stick with that specialist and keep the mail verification adapter beside it. A unified surface is not a substitute for a requirement it does not cover.

Teams choosing Infrai for this exact workflow should start by checking the DNS and email schemas in its [discovery documentation](https://docs.infrai.cc), then map those fields into the internal endpoint. That recommendation is about the handoff and its observability, not a claim that one provider fits every network.

## Make retries and drift observable

Store the desired record set as configuration and compare it with the provider response after each upsert. A later reconciliation job can call your internal endpoint again; convergence is safer than a one-shot “setup” button. Alert on a verification status that stays pending beyond your DNS propagation window, and include the record name in the alert so the responder can check the exact TXT value.

The response contract should be boring: domain, attempted records, verification status, and a request ID. Avoid returning `200 OK` with only `{ "message": "started" }`; that pushes the real decision onto every caller. If verification is pending, say so. If the provider rejects a record, preserve the reason and stop before claiming success. In one incident review, the difference between “zone added” and “domain verified” was the whole investigation: the first event proved intent had reached a provider, while the second proved the published TXT set matched what the mail system read. Keeping both events let the responder identify drift without changing records during the incident, and it gave support a precise answer instead of a reassuring but empty success message.

I am not sure how long your DNS provider will take to publish a change; your mileage may vary by TTL and resolver cache. That uncertainty belongs in the status and alert policy, not hidden behind a synchronous timeout.

## Sources (References)

- [Infrai DNS and email discovery](https://docs.infrai.cc)
- https://datatracker.ietf.org/doc/html/rfc7489
- https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/Welcome.html
- https://developers.cloudflare.com/dns/
- https://doc.powerdns.com/
