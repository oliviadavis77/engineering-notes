# Node.js Object Storage Guide: Resume, Complete, or Abort Backup Uploads

A backup worker can disappear between any two network calls, so the safe choice is to treat a multipart upload as durable work with a checkpoint outside the worker. **Short answer: persist the upload ID and a fixed part plan, reconcile accepted parts after a restart, let one owner complete the ordered manifest, and abort only work that policy has declared abandoned.**

This applies to a large app backup file sent by a Node.js service to S3-compatible object storage. The client library changes the spelling of calls; it does not remove the need for an immutable source, idempotent transitions, or a restore test. A successful upload is only transport evidence.

## What makes a backup upload resumable rather than merely retryable?

Multipart upload reduces the retry unit from a whole backup to a part. The storage service assigns an upload ID, accepts numbered parts, and completes the object from an ordered list of accepted part identifiers. A process crash after a service accepts a part but before the worker records the response is normal enough to design for. On restart, list the accepted parts for the saved upload ID before sending anything new.

The checkpoint is the real contract. Keep it in durable, shared storage keyed by a backup run ID, with the bucket, object key, upload ID, immutable source identity, source size, part size, expected part count, state, and a revision or lease. The source identity can be a snapshot ID or a content digest. Without it, a restarted process can assemble ranges from two versions of the same live file.

Keep it boring.

There are two distinct duplicate-delivery hazards. Two workers might initiate separate uploads for one logical backup, or one worker might begin completion while another is still reconciling parts. Conditional checkpoint writes or a lease make the coordinator single-owner. A worker that loses the lease stops after its current request and observes state rather than trying to win by sending another mutation. This is less exciting than a clever retry loop, and much easier to explain during a restore drill.

Fixed part boundaries matter too. Record them when the run begins. If a process later chooses a different part size, the part numbers no longer describe the same byte ranges. Set part size and bounded concurrency from the target's documented multipart limits, disk throughput, memory budget, and the cost of retrying one range. There is no universal setting; the right value for a backup on a local volume can be wrong for a streamed snapshot. Your mileage may vary, so load-test with representative files and forced process exits.

The runbook needs a decision before it needs code. Start with a generated backup artifact that will not change while bytes are being read; assign a run ID before initiation; write the intended byte plan before the first part request; and make the worker prove it still owns the checkpoint before it changes phase. Then exercise the awkward boundaries deliberately. Stop a worker after the remote service has accepted a part but before the local write that records its entity tag. Deliver the same queue item to two workers. Stop a coordinator after it has changed state to `completing`, then begin a new deployment with the old message still visible. In each case, the next worker should read the checkpoint, list remote parts, and make a single conservative decision from evidence already persisted. It should never infer success from a timeout, never reuse an upload ID for a changed source, and never create a second upload merely because the last response was not observed. The same discipline applies to shutdown: stop taking new part work, finish or account for in-flight requests, release the lease, and leave a checkpoint that another worker can understand without local logs. This sounds strict because backup pipelines have a long tail of rare failures. The small amount of state is cheaper than having to reconstruct which ranges reached storage after a pager alert.

## How should a Node.js backup upload resume, complete, or abort safely?

On each delivery, load the checkpoint and compare the requested bucket, key, source identity, size, and part plan with the durable record. If they differ, do not attach the worker to that upload. Create a new immutable source before starting a fresh run. If they match, list the remote parts, merge their part numbers and entity tags into the checkpoint, and upload only absent fixed ranges.

The state transitions should be narrow: `initiated`, `uploading`, `completing`, `complete`, `aborting`, and `aborted`. A lost completion response is a read-before-write case: inspect the target object and checkpoint before issuing another complete request. A duplicate delivery seeing `completing` should observe, not initiate a replacement upload.

The Go example is intentionally independent of a particular SDK. A Node.js adapter can implement the same interface using its selected S3-compatible client. The important behavior is that the repository transition is conditional and completion is owned by one coordinator.

```go
package backup

import (
	"context"
	"fmt"
	"sort"
)

type Part struct {
	Number int32
	ETag   string
}

type Checkpoint struct {
	RunID, Bucket, Key, UploadID string
	SourceID                     string
	SourceSize, PartSize         int64
	State                        string
}

type Store interface {
	CreateMultipart(context.Context, string, string) (string, error)
	ListParts(context.Context, Checkpoint) ([]Part, error)
	UploadPart(context.Context, Checkpoint, int32, int64, int64) (Part, error)
	CompleteMultipart(context.Context, Checkpoint, []Part) error
	AbortMultipart(context.Context, Checkpoint) error
}

type Repository interface {
	SaveNew(context.Context, Checkpoint) error
	Transition(context.Context, string, string, string) error
}

func Reconcile(ctx context.Context, objects Store, repo Repository, cp Checkpoint, abort bool) error {
	if cp.UploadID == "" {
		id, err := objects.CreateMultipart(ctx, cp.Bucket, cp.Key)
		if err != nil {
			return err
		}
		cp.UploadID, cp.State = id, "initiated"
		if err := repo.SaveNew(ctx, cp); err != nil {
			return err
		}
	}

	if abort {
		if err := repo.Transition(ctx, cp.RunID, cp.State, "aborting"); err != nil {
			return err
		}
		if err := objects.AbortMultipart(ctx, cp); err != nil {
			return err
		}
		return repo.Transition(ctx, cp.RunID, "aborting", "aborted")
	}

	accepted, err := objects.ListParts(ctx, cp)
	if err != nil {
		return err
	}
	byNumber := make(map[int32]Part, len(accepted))
	for _, part := range accepted {
		byNumber[part.Number] = part
	}

	expected := int32((cp.SourceSize + cp.PartSize - 1) / cp.PartSize)
	for number := int32(1); number <= expected; number++ {
		if _, ok := byNumber[number]; ok {
			continue
		}
		offset := int64(number-1) * cp.PartSize
		length := min(cp.PartSize, cp.SourceSize-offset)
		part, err := objects.UploadPart(ctx, cp, number, offset, length)
		if err != nil {
			return err
		}
		byNumber[number] = part
	}

	manifest := make([]Part, 0, expected)
	for number := int32(1); number <= expected; number++ {
		part, ok := byNumber[number]
		if !ok || part.ETag == "" {
			return fmt.Errorf("part %d is not accepted", number)
		}
		manifest = append(manifest, part)
	}
	sort.Slice(manifest, func(i, j int) bool { return manifest[i].Number < manifest[j].Number })

	if err := repo.Transition(ctx, cp.RunID, cp.State, "completing"); err != nil {
		return err
	}
	if err := objects.CompleteMultipart(ctx, cp, manifest); err != nil {
		return err
	}
	return repo.Transition(ctx, cp.RunID, "completing", "complete")
}
```

The sketch streams ranges behind `UploadPart`; it does not require the whole backup in memory. Add a small, bounded pool around the missing-part loop only after the state model is tested. Completion stays serial, because an ordered manifest and a single transition are the two things that make a duplicate delivery harmless.

## Verification belongs before cleanup

Completion proves that the object-store control plane accepted a manifest. It does not prove an application can use the backup. Verify in layers: confirm the object exists at the expected bucket and key, compare its observed size with the checkpoint, use a checksum method supported by the workflow, then restore into an isolated environment and run application-level checks. Do not treat a multipart ETag as a portable whole-object content hash; its meaning depends on the implementation and upload method.

Capture run ID, state age, bytes accepted, missing-part count, operation retry count, and age of open multipart uploads. Keep upload telemetry separate from restore evidence. The alert that matters is a stalled state transition or a missed backup deadline, not a single retried range.

| Recovery choice | Durable evidence | Use it when | Do not use it when |
|---|---|---|---|
| Resume the upload | Upload ID, immutable source identity, and listed accepted parts | The source and byte plan still match | The backup artifact has changed |
| Complete the upload | Every planned part has one accepted identifier in order | One coordinator holds the transition lease | A missing or ambiguous part remains |
| Abort the upload | An explicit cancellation or abandonment decision | Recovery policy no longer permits resumption | A transient failure is the only signal |

Before deployment, use a disposable target and terminate the worker after initiation, during a part, after a part response, and around completion. Redeliver the same job. The expected result is one usable object, a checkpoint that reaches `complete`, and no second active upload for the run. Also test a deliberate cancellation and confirm that an `aborted` checkpoint cannot re-enter the runnable queue.

## When should an operator abort or roll back a multipart run?

Abort when a cancellation is explicit, a retention policy classifies the attempt as abandoned, or the immutable source no longer matches the checkpoint. Do not abort as the generic response to a transient failure, because it destroys progress that a later worker could reconcile. Keep a short audit record of the decision while removing the live upload ID from runnable state.

Rollback starts by stopping new coordinators and preserving checkpoints. Classify each in-progress run before altering it: resumable runs continue under the known-good worker version; incompatible or abandoned runs follow the owned abort policy. A mass cleanup operation is tempting when a deployment is under pressure. It is also how recoverable backups disappear.

The catch is that this design is not suitable when the source cannot remain stable for the upload lifetime, or when the team cannot operate a durable checkpoint store. Take an immutable snapshot first, choose a chunked backup format, or use a storage workflow whose recovery boundary the team can support. The restore test is the decision point, not the upload response.

## References

- Cloudflare R2 documentation: https://developers.cloudflare.com/r2/
- Cloudflare Workers documentation: https://developers.cloudflare.com/workers/
