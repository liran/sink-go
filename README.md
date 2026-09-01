# sink-go

`sink-go` is the typed, concurrency-safe Go client for the
[`liran/sink`](https://github.com/liran/sink) gRPC service. It covers the full
batch API: reads, puts, self-contained Lua merges, and hard deletes with synchronous
or durable asynchronous completion.

## Install

```shell
go get github.com/liran/sink-go
```

The client requires Go 1.27 or newer.

## Quick start

`Dial` uses TLS 1.2 or newer by default. The explicit insecure credentials in
this example are appropriate only for a trusted local endpoint.

```go
package main

import (
	"context"
	"log"
	"time"

	sink "github.com/liran/sink-go"
	"google.golang.org/grpc/credentials/insecure"
)

type Product struct {
	UID       string    `json:"uid" bson:"_id"`
	Name      string    `json:"name" bson:"name"`
	Stock     int       `json:"stock" bson:"stock"`
	UpdatedAt time.Time `json:"updated_at" bson:"updated_at"`
}

func main() {
	dialOptions := sink.DialOptions{
		TransportCredentials: insecure.NewCredentials(),
	}
	client, err := sink.Dial("127.0.0.1:8080", dialOptions)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	datasetOptions := sink.DatasetOptions{
		Store:     "primary",
		Namespace: "catalog",
		Dataset:   "products",
		Encoding:  sink.DocumentEncodingBSON,
	}
	products, err := sink.NewDataset(client, datasetOptions)
	if err != nil {
		log.Fatal(err)
	}
	value := Product{
		UID:       "product-42",
		Name:      "keyboard",
		Stock:     12,
		UpdatedAt: time.Now().UTC(),
	}
	record := sink.Record{
		Key:   sink.StringKey(value.UID),
		Value: value,
	}
	_, err = products.Upsert(
		context.Background(),
		sink.CompletionWaitUntilVisible,
		record,
	)
	if err != nil {
		log.Fatal(err)
	}
}
```

`Dataset` binds the stable store, namespace, dataset, and document encoding once.
Each mutation still receives an explicit completion mode because callers of the
same dataset can require different durability or visibility guarantees. Pass any
number of records to `Create`, `Replace`, or `Upsert`; the client validates and
encodes the complete collection before sending it, automatically splits large
collections by `ClientOptions.MaxOperations`, and preserves global operation
indexes:

```go
records := []sink.Record{
	{Key: sink.StringKey("product-42"), Value: firstProduct},
	{Key: sink.StringKey("product-43"), Value: secondProduct},
}
results, err := products.Upsert(
	context.Background(),
	sink.CompletionReturnAfterAccepted,
	records...,
)
```

Configure `DocumentEncodingBSON` for MongoDB; it applies `bson` tags and keeps
native values such as BSON datetimes. Configure `DocumentEncodingJSON` for
Elasticsearch and OpenSearch; it applies `json` tags and produces ordinary JSON
without Extended JSON `$date` wrappers. Sink rejects an encoding that does not
match the selected backend. The low-level API also requires callers to create an
explicitly encoded `Document` before `NewPut` or `NewMerge`.

Reads return a `Document` with `Encoding`, immutable `Payload`, and `Decode`
methods. `Decode` selects the matching JSON or BSON decoder automatically.

A merge rule is ordinary application source code. Construct its immutable
`LuaProgram` once and bind it to the `Dataset`; every `Merge` call reuses that
program while the client includes its SHA-256 digest automatically:

```go
source := []byte(`
return function(current, incoming)
    current = current or json.object()
    current.stock = incoming.stock
    current.updated_at = sink.v1.time.now()
    return current
end`)
program, err := sink.NewLuaProgram(source)
if err != nil {
	log.Fatal(err)
}
datasetOptions := sink.DatasetOptions{
	Store:        "search-main",
	Namespace:    "catalog",
	Dataset:      "products",
	Encoding:     sink.DocumentEncodingJSON,
	MergeProgram: &program,
}
products, err := sink.NewDataset(client, datasetOptions)
if err != nil {
	log.Fatal(err)
}
record := sink.Record{
	Key:   sink.StringKey("product-42"),
	Value: incomingProduct,
}
results, err := products.Merge(
	context.Background(),
	sink.CompletionWaitUntilVisible,
	sink.MissingDocumentCreate,
	record,
)
if err != nil {
	log.Fatal(err)
}
```

The stored current document and incoming records must use the Dataset's
encoding, and the merge result preserves it. A Dataset without `MergeProgram`
rejects `Merge` before sending an RPC.

The merge function receives only `current` and `incoming`. Sink provides
versioned `sink.v1` array, object, and retry-stable time helpers. See the
[Lua merge developer guide](https://github.com/liran/sink/blob/main/docs/lua-merge-guide.md)
for the complete function reference and reliability rules.

## API model

- `Dataset` is the primary mutation API. It binds routing, encoding, and one
  optional Lua program while keeping completion mode explicit on every call.
  `Create`, `Replace`, `Upsert`, and `Merge` accept one or many `Record` values,
  split large collections automatically, and return all per-record results.
- `Read(ctx, addresses...)` preserves request order and reports found,
  not-found, or failed results independently. `ReadAll` automatically splits a
  larger collection by the configured operation limit.
- `Write(ctx, completionMode, operations...)` supports mixed put and merge
  batches. Use `NewPut` and `NewMerge` to construct validated operations;
  `WriteAll` handles larger collections.
- `Delete(ctx, completionMode, addresses...)` performs hard deletes; deleting
  an absent record is successful. `DeleteAll` handles larger collections.
- String, int64, byte, and opaque legacy keys are supported.
- `CheckHealth` uses the standard gRPC health service.
- `Raw` exposes the generated `api/sink/v1` client for advanced use.

Use `CompletionWaitUntilApplied` for storage acknowledgement,
`CompletionWaitUntilVisible` when a following search read must observe the
mutation, and `CompletionReturnAfterAccepted` for durable asynchronous queueing.
For OpenSearch and Elasticsearch, visible completion maps to
`refresh=wait_for`; MongoDB is immediately visible after an acknowledged write.

Lua source travels with the merge intent in both synchronous and asynchronous
mode. Sink caches compilation by digest while using a fresh VM per execution,
so rules follow application releases without server-side profile management.
The client declares identical source only once per `Write` batch; Sink embeds
the full program into each asynchronous Kafka mutation so it remains replayable
without server-side rule state.

Each result includes its operation index. The client validates result counts,
indexes, statuses, documents, and failure details before returning a response.
Per-record failures are represented by `OperationError`, so one bad record does
not hide successful records from the same batch. Dataset mutations return all
available results and automatically expose operation failures as an
`errors.As`-compatible `BatchError`. If a later split batch also has a transport
failure, the returned error preserves both the earlier operation failures and
the transport error. Low-level results expose `Err()`, and `ReadResultsError`,
`WriteResultsError`, and `DeleteResultsError` collect failures explicitly.

## Reliability behavior

Reads retry transport-level `Unavailable` failures and retryable per-operation
failures with bounded exponential backoff and jitter. Only failed operations are
resubmitted after a partial batch response. The default is three attempts,
starting at 100 ms and capped at one second; `ClientOptions.ReadRetry` can tune
or disable retries by setting `MaxAttempts` to one.

Writes and deletes are never retried automatically. A transport error can arrive
after Sink has already applied a synchronous mutation or durably accepted an
asynchronous one, so automatic mutation retries could duplicate work. Callers
should retry only when their operation is safe under Sink's documented
at-least-once semantics.

The client limits batches to 1,000 operations by default, matching Sink's
default configuration.
Set `ClientOptions.MaxOperations` when the server is configured with a different
limit. Encoded requests and responses are limited to 64 MiB by default; use
`MaxSendMessageBytes` and `MaxReceiveMessageBytes` to match custom server
limits. A `Client` and its underlying gRPC connection are safe for concurrent
use.

## Compatibility and development

The generated protocol matches the current Sink server contract. CI
runs descriptor contract tests, race-enabled unit tests against an in-memory
gRPC server, malformed-response tests, static analysis, and an end-to-end
compatibility test against the current Sink main branch with MongoDB and Kafka.
The compatibility workflow also runs weekly so server-side drift is detected
without requiring a client commit.

```shell
make test
make test-race
make lint
make proto-check
```

To run the external compatibility test against an already running server:

```shell
SINK_INTEGRATION_ADDRESS=127.0.0.1:8080 make test-integration
```
