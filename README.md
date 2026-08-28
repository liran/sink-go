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

	sink "github.com/liran/sink-go"
	"google.golang.org/grpc/credentials/insecure"
)

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
	value := map[string]any{"name": "keyboard", "stock": 12}
	document, err := sink.JSONDocument(value)
	if err != nil {
		log.Fatal(err)
	}
	operation, err := sink.NewPut(address, document, sink.WriteUpsert)
	if err != nil {
		log.Fatal(err)
	}

	results, err := client.Write(
		context.Background(),
		sink.CompletionWaitUntilApplied,
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

Use `application/bson` documents with MongoDB-backed Sink deployments and
`application/json` with Elasticsearch or OpenSearch. `NewDocument` accepts
already encoded bytes without adding a storage-specific dependency to this
module. Lua merge operations currently require JSON objects.

A merge rule is ordinary application source code. Construct it once and reuse
the immutable `LuaProgram` across operations; the client includes its SHA-256
digest automatically:

```go
source := []byte(`
return function(current, incoming, context)
    current = current or json.object()
    current.stock = incoming.stock
    current.last_found_at = context.observed_at
    return current
end`)
program, err := sink.NewLuaProgram(source)
if err != nil {
    log.Fatal(err)
}
incoming, err := sink.JSONDocument(map[string]any{"stock": 12})
if err != nil {
    log.Fatal(err)
}
mergeOptions := sink.MergeOptions{
    IncomingDocument:    incoming,
    Program:             program,
    MissingDocumentMode: sink.MissingDocumentCreate,
}
operation, err := sink.NewMerge(address, mergeOptions)
if err != nil {
    log.Fatal(err)
}
```

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
