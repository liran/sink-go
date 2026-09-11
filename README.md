# sink-go

`sink-go` is the typed, concurrency-safe Go client for the
[`liran/sink`](https://github.com/liran/sink) gRPC service. It covers the full
batch API: reads, puts, self-contained Lua merges, and hard deletes with synchronous
or durable asynchronous completion.
Native `Execute` commands and resumable `Scan` pages use the
same Sink connection, while returned writes support atomic read-modify-write results.

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
	readResults, err := products.Read(context.Background(), sink.StringKey(value.UID))
	if err != nil {
		log.Fatal(err)
	}
	if readResults[0].Status == sink.ReadFound {
		var stored Product
		if err := readResults[0].Document.Decode(&stored); err != nil {
			log.Fatal(err)
		}
	}
}
```

`Dataset` binds the stable store, namespace, dataset, and document encoding once.
`Read` accepts one or many keys without reconstructing addresses. Each mutation
still receives an explicit completion mode because callers of the same dataset
can require different durability or visibility guarantees. Pass any number of
records to `Create`, `Replace`, or `Upsert`; the client validates and encodes the
complete collection before sending it, automatically splits large collections
by `ClientOptions.MaxOperations`, and preserves global operation indexes:

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

- `Dataset` is the primary record API. It binds routing, encoding, and one
  optional Lua program. `Read` accepts one or many keys; `Create`, `Replace`,
  `Upsert`, and `Merge` accept one or many `Record` values while keeping
  completion mode explicit per mutation. Every method splits large collections
  automatically and returns all per-record results.
- `Read(ctx, addresses...)` preserves request order and reports found,
  not-found, or failed results independently.
- `Write(ctx, completionMode, operations...)` supports mixed put and merge
  batches. Use `NewPut` and `NewMerge` to construct validated operations.
- `Delete(ctx, completionMode, addresses...)` performs hard deletes; deleting
  an absent record is successful.
- `Execute(ctx, request)` returns native BSON or HTTP payloads, status and headers
  for native queries, writes, and administration; MongoDB cursor/session commands
  are rejected.
- `Scan(ctx, request)` returns one live page of MongoDB documents or complete
  search hits, with an opaque `NextCursor` for resuming on any server.
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
not hide successful records from the same batch. Dataset methods return all
available results and automatically expose operation failures as an
`errors.As`-compatible `BatchError`. If a later split batch also has a transport
failure, the returned error preserves both the earlier operation failures and
the transport error. Low-level results expose `Err()`, and `ReadResultsError`,
`WriteResultsError`, and `DeleteResultsError` collect failures explicitly.

## Native commands, queries, counts, and scans

Construct ordered MongoDB commands without opening a database connection. A
struct, `bson.D`, or `bson.Raw` preserves the command field order; unordered maps
are rejected by `NewBSONCommand`. All native RPCs use the same `Command` fields
(store, namespace, method, path, query, headers, content type and payload). BSON values are retained through the RPC:

```go
commandValue := struct {
	Count string `bson:"count"`
}{Count: "products"}
command, err := sink.NewBSONCommand("primary", "catalog", commandValue)
if err != nil {
	return err
}
request := sink.ExecuteRequest{Command: command}
response, err := client.Execute(ctx, request)
if err != nil {
	var native *sink.NativeError
	if errors.As(err, &native) {
		// native.Response.Payload retains the database's complete BSON error.
	}
	return err
}
var count struct {
	N int64 `bson:"n"`
}
if err := response.Decode(&count); err != nil {
	return err
}
```

For search, pass the method, endpoint path, URL-encoded query parameters, and
original body. Sink uses its configured endpoint and credentials. For example:

```go
command := sink.Command{
	Store: "search-main",
	ContentType: "application/json",
	Method: "POST",
	Path:   "/products/_search",
	Query:  "track_total_hits=true",
	Payload: []byte(`{"query":{"match":{"name":"keyboard"}},"size":20}`),
}
request := sink.ExecuteRequest{Command: command}
response, err := client.Execute(ctx, request)
// response.Payload, StatusCode, and Headers remain available on NativeError.
```

MongoDB Execute on revision-protected Sink servers supports `insert`, `update`,
`delete`, and `findAndModify`, plus an explicit set of read/diagnostic and index
commands. Inserted/replacement documents, operator updates, and update pipelines
atomically receive fresh Sink revisions, so concurrent record Merges detect the
change and recompute. The server rejects metadata tampering, unsafe commands such
as `drop`/`renameCollection`, and unknown commands before execution; it does not
fall back to unrestricted passthrough. All writers to a collection must use the
same revision protocol and metadata field. This requires server-side support:
older unrestricted servers do not advance revisions for native writes, and a
client upgrade alone does not protect mixed native/record mutations.

Search Execute validates supported routes before forwarding native payloads.
Document writes, `_bulk`, queries, mapping updates and routine refresh/flush
operations remain available. Index lifecycle operations, alias changes,
settings/templates, lifecycle policies and unknown administrative/plugin write
routes return `INVALID_ARGUMENT` before reaching the backend. GET/HEAD/OPTIONS
can still inspect native endpoints. See the server's
[native access contract](https://github.com/liran/sink/blob/main/docs/native-access.md#elasticsearch-and-opensearch-endpoints)
for supported routes. This protection requires an updated server; upgrading
this SDK alone does not restrict older servers. External administration and
existing lifecycle policies must be coordinated with record clients, which must
discard old revisions and snapshots after an index change.
Native mutations do not run Lua merges or
participate in record batching or asynchronous completion modes. Neither the
server nor this SDK automatically retries or deduplicates native mutations.

MongoDB cursor commands (`find`, `aggregate`, `listIndexes`, `listCollections`,
`getMore`, `killCursors`, `parallelCollectionScan`, and cursor-returning
`bulkWrite`) are rejected by Execute. Use Scan for resumable find pages and Query for
independent find/aggregate pages.
Client-managed sessions and transactions are also unsupported. Search scrolls
can be managed explicitly through Execute, with cleanup owned by the caller.

`ExecuteResponse.Decode` handles JSON and BSON; other content types can use
`Payload` directly. Nonempty request payloads require `ContentType`; set it
directly instead of in Headers. `_msearch` uses `application/x-ndjson` with the
final newline intact.
Database failures return both the response and `*NativeError`. Transport errors
return no response. HTTP 2xx can still contain partial search failures, so inspect
the raw response when using `_msearch` or other partial-result queries.

Fetch independent pages with explicit sorting and projection, and request the
count separately when needed. Reuse the same native query Command for both:

```go
command := sink.Command{
 Store: "search-main",
 Method: "POST",
 Path: "/products/_search",
 ContentType: "application/json",
 Payload: []byte(`{"query":{"match":{"name":"keyboard"}}}`),
}
projection := &sink.Projection{Fields: []string{"name", "price", "uid"}}
request := sink.QueryRequest{
 Command: command,
 Page: 2,
 PageSize: 20,
 Sort: []sink.SortField{{Field: "price", Descending: true}, {Field: "uid"}},
 Projection: projection,
}
page, err := client.Query(ctx, request)
if err != nil {
 return err
}
// page.Documents contains complete native hits with projected _source fields.
// page.HasMore is determined by fetching one extra result, without counting.
countRequest := sink.CountRequest{Command: command}
count, err := client.Count(ctx, countRequest)
// count.Count is the total; count.Estimated identifies a metadata estimate.
```

Pages start at 1; zero defaults to page 1 and page size 100, with a maximum of
1000. Empty Sort preserves native ordering. Nil Projection preserves the native
projection; an explicit empty field list selects all fields. Set Exclude to
exclude listed fields. MongoDB uses native document paths and `_id` projection
rules; HTTP projection selects `_source` fields and retains hit metadata.

Query overrides find skip/limit or HTTP from/size. Explicit sort/projection replace
native settings; for aggregate, they apply after the supplied pipeline and before
pagination. Both Query and Count support find/read-only aggregate and HTTP _search.
Count ignores find/HTTP pagination; aggregate Count counts pipeline output. HTTP
Count counts matching documents before collapse. Use Execute for full native replies.

For MongoDB find predicates using `$where`, `$near` or `$nearSphere`, updated
servers count a native find cursor with a constant projection instead of
translating the filter into an aggregation. This retains native filter semantics
and returns `Estimated=false`, but transfers one small result per match and may
be slower. Timeouts and cursor failures return errors without a partial total.

No cursor is retained between Query calls. Stable sorting with a unique tie-breaker
is recommended; concurrent writes can shift pages and change a separately requested
count. Deep pages remain subject to backend offset costs and result-window limits,
including the extra result for HasMore. Use Scan for full traversal. MongoDB Count
automatically uses `EstimatedDocumentCount` for missing or empty find filters without options
requiring exact execution.
The response exposes `Count` and `Estimated`, indicating whether an estimate was
used. Filtered queries, aggregate pipelines and HTTP searches remain exact.
Incomplete counts and approximate HTTP search totals fail. Query and Count return gRPC failures rather than NativeError,
and the SDK retries neither operation.

`Dataset` also exposes `Execute`, `Query`, `Count` and `Scan` using the same request
and response types as Client. Store is bound automatically. BSON datasets bind the
database and collection; JSON datasets bind the index (the logical Namespace is
unused by HTTP search). Empty Query/Count commands select all records in the
Dataset. MongoDB Scan also accepts an empty Command; JSON Scan needs a body with
an explicit stable sort. Conflicting stores, namespaces or BSON collection targets fail.
The Dataset wrappers retain native error, no-retry and cursor semantics.

```go
opts := sink.DatasetOptions{
 Store: "primary", Namespace: "catalog", Dataset: "products",
 Encoding: sink.DocumentEncodingBSON,
}
products, err := sink.NewDataset(client, opts)
if err != nil {
 return err
}
// The helper inserts the command's first field and collection name.
arguments := bson.D{{Key: "filter", Value: bson.D{{Key: "active", Value: true}}}}
command, err := products.NewBSONCommand("find", arguments)
if err != nil {
 return err
}
projection := &sink.Projection{Fields: []string{"name", "price"}}
query := sink.QueryRequest{
 Command: command, Page: 2, PageSize: 20,
 Sort: []sink.SortField{{Field: "price"}, {Field: "_id"}}, Projection: projection,
}
page, err := products.Query(ctx, query)

// No command needed for the whole table; MongoDB automatically uses metadata.
countRequest := sink.CountRequest{}
count, err := products.Count(ctx, countRequest)

scanRequest := sink.ScanRequest{Command: command, BatchSize: 100}
scanPage, err := products.Scan(ctx, scanRequest)
// After processing scanPage.Documents, save scanPage.NextCursor for the next call.

// Collection commands use the same helper and Execute wrapper.
index := bson.D{{Key: "name", Value: "price"}, {Key: "key", Value: bson.D{{Key: "price", Value: 1}}}}
indexArguments := bson.D{{Key: "indexes", Value: bson.A{index}}}
indexCommand, err := products.NewBSONCommand("createIndexes", indexArguments)
if err != nil {
 return err
}
executeRequest := sink.ExecuteRequest{Command: indexCommand}
response, err := products.Execute(ctx, executeRequest)
```

An existing full BSON command can also be passed; its first value must match the
Dataset collection or be an empty string placeholder. Native BSON types and
ordered fields are preserved. For a JSON Dataset, supply an index-relative Path
such as `/_search`, `/_mapping` or `/_doc/id`; empty Execute Path selects the index
itself for inspection with GET/HEAD. Explicit index creation/deletion is rejected
by updated servers. Query/Count/Scan default to `POST /<index>/_search`. JSON ContentType is
inferred when omitted; specify NDJSON explicitly for bulk bodies. Use Client for
database, cluster and multi-index endpoints. Dataset binding is a convenience;
it does not restrict cross-collection operations inside native payloads.

Scan returns one page at a time and does not accumulate the complete result set:

```go
command := sink.Command{
 Store: "search-main",
 ContentType: "application/json",
 Method: "POST",
 Path: "/products/_search",
 Payload: []byte(`{"query":{"match_all":{}},"sort":[{"uid.keyword":"asc"}]}`),
}
request := sink.ScanRequest{Command: command, BatchSize: 100}
for {
 page, err := client.Scan(ctx, request)
 if err != nil {
  return err // Retain the last successfully processed checkpoint for retry.
 }
 for _, document := range page.Documents {
  var hit struct {
   ID string `json:"_id"`
   Source json.RawMessage `json:"_source"`
  }
  if err := document.Decode(&hit); err != nil {
   return err
  }
  if err := process(ctx, hit.ID, hit.Source); err != nil {
   return err
  }
 }
 if len(page.NextCursor) == 0 {
  break // Persist task completion; an empty cursor starts a new scan.
 }
 request.Cursor = page.NextCursor // Persist only after processing the whole page.
}
```

MongoDB Scan supports find queries in `_id` ascending order by default, or an
explicit `_id` descending sort, with simple collation. Projections can exclude
`_id`; Sink still uses the original ID in the opaque cursor. Other sorts,
aggregates, skip/limit and list commands are not supported by Scan. Query retains
its independent find/aggregate pagination behavior.

Search Scan requires an explicit stable, globally unique sort tuple using
ordinary fields with doc_values. The example assumes `uid.keyword` is a unique
immutable keyword field. `_id`, `_doc`, `_shard_doc`, `_score`, null sort values,
scripted sorts and URL sort are unsupported. Every document is a complete JSON
hit, including its sort values. Scan uses search_after without scroll or PIT.

Resend the same Command, including headers and native payload bytes, with
`Cursor` set to the previous `NextCursor`. Batch size may change. Cursors are
opaque continuation markers limited to 64 KiB and bound to the query; they are
not credentials. They do not expire and survive Sink server restarts. There is
no keep-alive, explicit close operation or database cursor retained between
requests. Each request still has the server's ordinary deadline and admission
limits; task deadlines and checkpoint retention belong to the application.

Only an empty NextCursor marks the end, even if a byte-limited page is short.
Scan reads live data: inserts before the checkpoint may be missed, later inserts
may appear, and updates/deletes can change results. Retries from the same cursor
can observe newer data. Neither Execute nor Scan retries automatically. A failed
Scan returns no page; reuse the last saved cursor and process idempotently.
Persist task completion separately so a completed task does not restart from an
empty cursor. A dataset recreation or remapping requires an explicit new scan.

## Return the result of a write

Set `Record.ReturnDocument` for Dataset `Create`, `Replace`, `Upsert`, or `Merge`,
or use `operation.WithReturnedDocument()` with the low-level Write API:

```go
record := sink.Record{
	Key:            sink.StringKey("daily-quota"),
	Value:          increment,
	ReturnDocument: true,
}
results, err := quotas.Merge(ctx, sink.CompletionWaitUntilApplied, sink.MissingDocumentCreate, record)
if err != nil {
	return err
}
var quota Quota
if err := results[0].Document.Decode(&quota); err != nil {
	return err
}
```

Bind an increment Lua program to `quotas`. Each successful result contains that
operation's logical document and revision. Sink takes it from the successful
commit candidate without a later Read. Each operation requesting a returned
document commits independently; other operations in the same-address chain
may still fold and share a revision. Failed operations have no
document. Backend-generated fields and ingest transformations are excluded;
Read is available for a later stored observation. Returning documents requires
synchronous completion and is rejected with `CompletionReturnAfterAccepted`.
A timeout can still leave a mutation's outcome unknown; this is not exactly-once
increment delivery or a multi-document transaction.

See the server's [native access contract](https://github.com/liran/sink/blob/main/docs/native-access.md)
for cursor restrictions, native write semantics, raw metadata, byte limits, and
scan deadlines.

## Reliability behavior

`Dial` defaults to `round_robin` across the addresses returned by the resolver.
In Kubernetes, use a headless Service selecting only Sink server pods and a
target such as `dns:///sink-headless.sink.svc.cluster.local:8080`. A normal
ClusterIP resolves to one virtual address and does not expose individual
replicas for per-RPC balancing. Resolver service configuration or explicit
`DialOptions.GRPCOptions` can override the default policy. For DNS targets,
`DialOptions.DNSRefreshInterval` controls the delay after a successful address
update before the next lookup while the channel is active. Zero defaults to
30 seconds, positive durations override it, and negative durations are rejected
by `Dial`. For example:

```go
dialOptions := sink.DialOptions{
	DNSRefreshInterval: 5 * time.Second,
}
client, err := sink.Dial("dns:///sink.example.com:8080", dialOptions)
```

Intervals below 30 seconds take effect without changing gRPC's process-global
resolution settings. Each scheduled refresh starts a new DNS resolver while
retaining the gRPC channel and unchanged backend connections. Slow or failed
lookups retain gRPC's DNS timeout and retry behavior; the next scheduled refresh
starts after an accepted update. Lookup delays and DNS-server caches can delay
discovery, so the configured interval is not an availability guarantee.
Literal IP targets perform no DNS queries and have no refresh timer. Explicit
resolvers supplied through `GRPCOptions` take precedence and control their own
refresh behavior. Channel shutdown or idleness stops the refresh timer; leaving
idle starts a new resolver. TLS and plaintext transport options are unchanged.

To query a particular DNS server directly, use a target such as
`dns://10.0.0.53:53/sink.example.com:8080`, replacing `10.0.0.53` with your DNS
server's IP. gRPC then uses Go's DNS resolver and directs queries to that server,
bypassing the operating system's native resolver cache path. The selected DNS
server, a local forwarding service, or an upstream resolver may still cache
answers. The client cannot force those servers to ignore their caches; that
requires choosing a DNS endpoint outside the cached path or changing the
DNS-server cache configuration.

During scale-in, allow enough time after endpoint removal for clients to refresh,
then gracefully drain the server. Abrupt termination can fail in-flight calls;
load balancing does not make mutating RPCs safe to replay.

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

`Read`, `Write`, and `Delete` split collections into batches of 1,000 operations
by default, matching Sink's default configuration. There are no separate `All`
variants for callers to choose between.
Set `ClientOptions.MaxOperations` when the server is configured with a different
limit. Encoded requests and responses are limited to 64 MiB by default; use
`MaxSendMessageBytes` and `MaxReceiveMessageBytes` to match custom server
limits. A `Client` and its underlying gRPC connection are safe for concurrent
use.

## Compatibility and development

Native access and returned documents require the matching Sink server update.
Upgrade the server before using these options. Older servers report Unimplemented
for native RPCs; a missing requested write document is a ProtocolError after the
write may already have been applied, so it must not trigger an automatic retry.

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
