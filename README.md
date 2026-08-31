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

	address, err := sink.NewAddress(
		"primary",
		"catalog",
		"products",
		sink.StringKey("product-42"),
	)
	if err != nil {
		log.Fatal(err)
	}
	value := Product{
		UID:       "product-42",
		Name:      "keyboard",
		Stock:     12,
		UpdatedAt: time.Now().UTC(),
	}
	document, err := sink.NewDocument(value, sink.DocumentEncodingBSON)
	if err != nil {
		log.Fatal(err)
	}
	operation, err := sink.NewPut(address, document, sink.WriteUpsert)
	if err != nil {
		log.Fatal(err)
	}

	results, err := client.Write(
		context.Background(),
		sink.CompletionWaitUntilVisible,
		operation,
	)
	if err != nil {
		log.Fatal(err)
	}
	if results[0].Failure != nil {
		log.Fatal(results[0].Failure)
	}
}
```

Callers must create a `Document` with an explicit encoding before `NewPut` or
`NewMerge`. Use `DocumentEncodingBSON` for MongoDB; it applies `bson` tags and
keeps native values such as BSON datetimes. Use `DocumentEncodingJSON` for
Elasticsearch and OpenSearch; it applies `json` tags and produces ordinary JSON
without Extended JSON `$date` wrappers. Sink rejects an encoding that does not
match the selected backend.

Reads return a `Document` with `Encoding`, immutable `Payload`, and `Decode`
methods. `Decode` selects the matching JSON or BSON decoder automatically.

A merge rule is ordinary application source code. Construct it once and reuse
the immutable `LuaProgram` across operations; the client includes its SHA-256
digest automatically:

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
mergeOptions := sink.MergeOptions{
    Incoming:            incomingDocument,
    Program:             program,
    MissingDocumentMode: sink.MissingDocumentCreate,
}
operation, err := sink.NewMerge(address, mergeOptions)
if err != nil {
    log.Fatal(err)
}
```

Encode `incomingDocument` for the destination backend before constructing the
merge. The stored current document and incoming document must use the same
encoding, and the merge result preserves it.

The merge function receives only `current` and `incoming`. Sink provides
versioned `sink.v1` array, object, and retry-stable time helpers. See the
[Lua merge developer guide](https://github.com/liran/sink/blob/main/docs/lua-merge-guide.md)
for the complete function reference and reliability rules.

## API model

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
not hide successful records from the same batch. Each result exposes `Err()`,
and `ReadResultsError`, `WriteResultsError`, and `DeleteResultsError` collect all
operation failures into an `errors.As`-compatible `BatchError`.

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
