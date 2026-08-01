# Self-Hosted Metabase, Redash, Supabase Charts, or Metrics APIs for SaaS KPI Dashboards

Use Metabase or Redash when your team needs open-ended internal SQL analysis; otherwise use an application-owned metrics API for the stable SaaS KPIs customers see in the product. Supabase is a good Postgres foundation for that application path, but it does not remove the need to define metrics, protect tenant data, and operate the aggregation job.

Short answer: self-hosted BI is best for internal exploration, while a narrow API backed by aggregates or managed metrics is the safer way to show repeatable KPIs in an app.

I run cron and queue infrastructure in production. A dashboard that says everything is green while the completion worker is late is worse than no dashboard because people stop looking for evidence. Start with a definition and a reconciliation path back to the source records.

## What should a self-hosted Metabase, Redash, Supabase charts, or managed metrics API show?

The question contains two jobs that look alike on a slide. Internal operators need to join tables, alter a date range, and investigate one odd customer account. Product users need a small set of defined numbers: active seats, processed volume, conversion, and perhaps a latency percentile. One tool can serve both jobs, but the permission model and query behavior get unpleasant fast.

Metabase works well for internal teams that want both a visual query builder and native SQL. Redash is a sensible choice for a SQL-first team sharing saved queries and visualizations. Both can be self-hosted. That can be appropriate when you already have backups, identity integration, a reporting replica, and someone responsible for upgrades. The catch is real: an exploratory dashboard query must not become a noisy neighbor on the primary database.

Supabase gives an application a Postgres base where carefully designed views, RPC functions, or aggregate tables can power a customer-facing chart. Put a server boundary in front of those reads. I would not expose broad tables to a browser just because row-level security exists; the endpoint should enforce tenant scope and return only the series a chart needs.

A managed metrics API fits the operational side: request rates, queue age, retry counts, and latency distributions. Prometheus is a common model for these time-series signals, while Grafana is useful for visualizing them. Neither turns metrics labels into an account ledger. Keep revenue and durable business state in the database or warehouse that owns them.

| Approach | Best fit | Main cost | Avoid it when |
| --- | --- | --- | --- |
| Self-hosted Metabase | Internal business exploration | Operations and database capacity | The app needs a tightly controlled customer UI |
| Self-hosted Redash | SQL-first shared analysis | Query governance and maintenance | Non-SQL users need a curated workflow |
| Supabase plus app charts | Focused product KPIs | Engineering time for aggregate models and APIs | Metrics need long-range telemetry analysis |
| Managed metrics API with Grafana | Operational time series | Metered ingestion, retention, and queries | Users need arbitrary account records |

## How do you build a SaaS KPI dashboard in an app without creating an incident?

I make the dashboard read from a derived table or a metric stream, never from the write path. An hourly or five-minute worker aggregates immutable events by tenant and time bucket; the serving endpoint then performs a bounded lookup. Each bucket has a unique key such as `(tenant_id, metric_name, bucket_start)`, so a retry can upsert the same result. Duplicate delivery is normal. Duplicate KPI totals are not.

I learned this after a silent failure in a queue consumer: the call returned 200, the expected side effect never happened, and I found it 7 hours later when a customer total went flat. My runbook now records an input count, an output count, a watermark, and a last-success timestamp for every aggregation. A 200 only proves that one HTTP exchange ended politely.

The follow-up was more revealing than the page. The producer had accepted the event, the queue had acknowledged it, and the consumer's request metric had incremented, so three separate screens looked normal. The missing evidence was a durable completion record keyed to the original event, plus an alert that compared accepted work with completed work over a bounded window. I added that comparison, a retry count, and a query that sampled completed records against source events before changing the dashboard. That sequence matters because a chart is downstream of the contract: if the contract is vague, adding more panels only gives the ambiguity more colors. For customer KPIs, I also keep late arrivals visible as a freshness or backfill indicator instead of silently changing a historical bar after someone has exported it. It is mundane work, but it prevents an operator from treating a successful request as proof that business work happened.

Here is the shape I use when a Go service reads a bounded Prometheus range query before returning chart data. Keep the PromQL expression on the server or construct it from validated metric names; don't accept arbitrary expressions from the browser.

```go
package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

type queryRangeResponse struct {
	Status string `json:"status"`
	Data   struct {
		Result json.RawMessage `json:"result"`
	} `json:"data"`
}

func QueryRequests(ctx context.Context, baseURL, tenant string) (json.RawMessage, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	endpoint, err := url.Parse(baseURL + "/v1/metrics/query")
	if err != nil {
		return nil, err
	}
	q := endpoint.Query()
	q.Set("query", fmt.Sprintf(`sum(rate(http_requests_total{tenant=%q}[5m]))`, tenant))
	q.Set("start", time.Now().Add(-time.Hour).UTC().Format(time.RFC3339))
	q.Set("end", time.Now().UTC().Format(time.RFC3339))
	q.Set("step", "60")
	endpoint.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("metrics query returned %s", resp.Status)
	}

	var decoded queryRangeResponse
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return nil, err
	}
	if decoded.Status != "success" {
		return nil, fmt.Errorf("metrics query status: %s", decoded.Status)
	}
	return decoded.Data.Result, nil
}
```

Cache by tenant, query window, and dashboard version. Then show a freshness timestamp. Small details. Big consequences.

## Which costs and deployment choices change the cheapest dashboard option?

"Cheapest" is rarely the license line. Self-hosting can have low software cost while moving the bill into database headroom, on-call time, backups, security reviews, and dashboard refresh load. A managed metrics API makes more of that work usage-based through ingestion, retention, indexed dimensions, and query volume. Datadog's pricing page, for example, separates log ingestion and indexing considerations; read meter definitions rather than comparing a headline plan.

For a new SaaS product, calculate tenants, dashboard opens per day, points per chart, retention period, and label cardinality. A counter labeled by tenant, region, and status can be manageable. Adding user IDs or request IDs as labels changes the economics quickly. Your mileage may vary because real query patterns matter more than a synthetic benchmark.

Deployment needs the same discipline. Run the aggregation worker separately from the web process and give it a durable cursor or watermark. Test a rerun with the same input range, a partial range, and delayed events. I keep a sampled reconciliation query that compares the dashboard aggregate with the source of record, because a chart can be internally consistent while describing yesterday's data.

For frontend performance KPIs, define the statistic before building the graph. Core Web Vitals use the 75th percentile as the assessment threshold for LCP, INP, and CLS, so an average-only chart can hide the experience that matters. Show the window, population, and freshness next to the value.

## When should you keep BI internal and expose only a narrow product dashboard?

Keep Metabase or Redash internal when users need ad hoc analysis, metrics are changing weekly, or support needs to follow a question into raw rows. Their flexibility is the point. Add a read replica, a restricted database role, saved-query review, and an owner for each executive KPI before broad adoption.

Expose a narrow application dashboard when the product promise depends on repeatable numbers. Give every chart a definition, a timezone, a refresh expectation, and an empty-state policy. Avoid putting an internal dashboard in an iframe and calling it done; authentication, tenant filtering, and predictable performance are part of the product surface.

I'm not sure why teams still treat the chart as the hard part — it is usually the last ten percent. The hard part is choosing the authoritative event, preserving that choice through retries and backfills, and making late data visible without quietly rewriting history.

The practical recommendation is split ownership: use self-hosted BI for discovery when your team can operate it, and serve customer KPIs through a deliberately small API backed by aggregates or managed metrics. Stick with Metabase or Redash for exploration; choose a managed metrics API when telemetry volume and retention justify its operating model; use Supabase plus app charts for a focused product view with disciplined Postgres data. None is universally cheapest. The right one is the one whose failure modes you can detect before a customer does.

## References

- https://www.metabase.com/docs/latest/
- https://redash.io/help/
- https://supabase.com/docs
- https://prometheus.io/docs/prometheus/latest/querying/api/
- https://grafana.com/docs/grafana/latest/
- https://www.datadoghq.com/pricing/
- https://web.dev/articles/vitals
