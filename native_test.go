package sink_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	sink "github.com/liran/sink-go"
	sinkv1 "github.com/liran/sink-go/api/sink/v1"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type nativeRPCServer struct {
	sinkv1.UnimplementedSinkServer
	executeCalls  atomic.Int32
	scanCalls     atomic.Int32
	writeCalls    atomic.Int32
	transportFail bool
	blockScan     bool
	omitDocument  bool
	stopped       chan struct{}
	requests      chan *sinkv1.ExecuteRequest
}

func (s *nativeRPCServer) Execute(_ context.Context, req *sinkv1.ExecuteRequest) (*sinkv1.ExecuteResponse, error) {
	s.executeCalls.Add(1)
	if s.requests != nil {
		s.requests <- req
	}
	if s.transportFail {
		return nil, status.Error(codes.Unavailable, "connection lost")
	}
	warning := &sinkv1.Header{Name: "Warning", Values: []string{"first", "second"}}
	response := &sinkv1.ExecuteResponse{ContentType: "application/json", Payload: []byte("{\"error\":\"query\"}\n"), StatusCode: 429,
		Headers: []*sinkv1.Header{warning}}
	return response, nil
}

func (s *nativeRPCServer) Scan(ctx context.Context, req *sinkv1.ScanRequest) (*sinkv1.ScanResponse, error) {
	s.scanCalls.Add(1)
	if s.stopped != nil {
		defer close(s.stopped)
	}
	if s.blockScan {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if s.transportFail {
		return nil, status.Error(codes.Unavailable, "page response lost")
	}
	document := &sinkv1.Document{Encoding: sinkv1.DocumentEncoding_DOCUMENT_ENCODING_JSON, Payload: []byte(`{"_id":"1","_source":{"value":1}}`)}
	page := &sinkv1.ScanResponse{Documents: []*sinkv1.Document{document}}
	if len(req.Cursor) == 0 {
		page.NextCursor = []byte("next")
	}
	return page, nil
}

func (s *nativeRPCServer) Write(_ context.Context, req *sinkv1.WriteRequest) (*sinkv1.WriteResponse, error) {
	s.writeCalls.Add(1)
	response := &sinkv1.WriteResponse{}
	for index, operation := range req.GetOperations() {
		result := &sinkv1.WriteResult{OperationIndex: uint32(index), Status: sinkv1.WriteStatus_WRITE_STATUS_APPLIED}
		if operation.GetReturnDocument() && !s.omitDocument {
			result.Document = operation.GetPut().GetDocument()
		}
		response.Results = append(response.Results, result)
	}
	return response, nil
}

func sdkNativeRequest() sink.ExecuteRequest {
	command := sink.Command{Store: "search", Method: "POST", Path: "/products/_msearch", Query: "q=a&q=b",
		Headers: http.Header{"Accept": {"application/json"}}, ContentType: "application/x-ndjson", Payload: []byte("{}\n{}\n")}
	request := sink.ExecuteRequest{Command: command}
	return request
}

func TestExecuteRetainsNativeFailureAndRequestBytes(t *testing.T) {
	server := &nativeRPCServer{requests: make(chan *sinkv1.ExecuteRequest, 1)}
	opts := sink.ClientOptions{}
	client := startTestClient(t, server, opts)
	request := sdkNativeRequest()
	response, err := client.Execute(t.Context(), request)
	var nativeErr *sink.NativeError
	if !errors.As(err, &nativeErr) || response.StatusCode != 429 || !bytes.Equal(response.Payload, nativeErr.Response.Payload) || len(response.Headers.Values("Warning")) != 2 {
		t.Fatalf("response=%+v err=%v", response, err)
	}
	var document map[string]string
	if err := response.Decode(&document); err != nil || document["error"] != "query" {
		t.Fatalf("decode=%v err=%v", document, err)
	}
	captured := <-server.requests
	if !bytes.Equal(captured.GetCommand().GetPayload(), request.Command.Payload) || captured.GetCommand().GetQuery() != request.Command.Query || server.executeCalls.Load() != 1 {
		t.Fatalf("native request changed or retried: %v", captured)
	}
}

func TestExecuteNeverRetriesAmbiguousTransportFailure(t *testing.T) {
	server := &nativeRPCServer{transportFail: true}
	opts := sink.ClientOptions{}
	client := startTestClient(t, server, opts)
	request := sdkNativeRequest()
	_, err := client.Execute(t.Context(), request)
	if status.Code(err) != codes.Unavailable || server.executeCalls.Load() != 1 {
		t.Fatalf("calls=%d err=%v", server.executeCalls.Load(), err)
	}
}

func TestScanReturnsPageAndPreservesCheckpointOnFailure(t *testing.T) {
	server := &nativeRPCServer{}
	opts := sink.ClientOptions{}
	client := startTestClient(t, server, opts)
	request := sink.ScanRequest{Command: sdkNativeRequest().Command, BatchSize: 2}
	page, err := client.Scan(t.Context(), request)
	if err != nil || len(page.Documents) != 1 || string(page.NextCursor) != "next" {
		t.Fatalf("page=%+v err=%v", page, err)
	}
	request.Cursor = page.NextCursor
	page, err = client.Scan(t.Context(), request)
	if err != nil || len(page.Documents) != 1 || len(page.NextCursor) != 0 || string(request.Cursor) != "next" {
		t.Fatalf("page=%+v err=%v", page, err)
	}
}

func TestScanDoesNotRetryFailedPage(t *testing.T) {
	server := &nativeRPCServer{transportFail: true}
	opts := sink.ClientOptions{}
	client := startTestClient(t, server, opts)
	request := sink.ScanRequest{Command: sdkNativeRequest().Command, Cursor: []byte("checkpoint")}
	page, err := client.Scan(t.Context(), request)
	if status.Code(err) != codes.Unavailable || len(page.Documents) != 0 || len(page.NextCursor) != 0 || server.scanCalls.Load() != 1 || string(request.Cursor) != "checkpoint" {
		t.Fatalf("page=%+v calls=%d err=%v", page, server.scanCalls.Load(), err)
	}
}

func TestScanRequestCancellationReleasesServer(t *testing.T) {
	server := &nativeRPCServer{blockScan: true, stopped: make(chan struct{})}
	opts := sink.ClientOptions{}
	client := startTestClient(t, server, opts)
	request := sink.ScanRequest{Command: sdkNativeRequest().Command}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	if _, err := client.Scan(ctx, request); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("cancel=%v", err)
	}
	select {
	case <-server.stopped:
	case <-time.After(time.Second):
		t.Fatal("cancellation leaked server scan")
	}
}

func TestBSONCommandRequiresOrderAndPreservesBSON(t *testing.T) {
	unordered := map[string]any{"find": "products", "filter": map[string]any{}}
	for _, value := range []any{unordered, &unordered} {
		if _, err := sink.NewBSONCommand("primary", "catalog", value); err == nil {
			t.Fatal("accepted unordered command")
		}
	}
	ordered := bson.D{{Key: "find", Value: "products"}, {Key: "filter", Value: bson.D{{Key: "at", Value: bson.DateTime(1234)}}}}
	command, err := sink.NewBSONCommand("primary", "catalog", ordered)
	if err != nil {
		t.Fatal(err)
	}
	raw := bson.Raw(command.Payload)
	elements, err := raw.Elements()
	if err != nil || elements[0].Key() != "find" || raw.Lookup("filter", "at").Type != bson.TypeDateTime {
		t.Fatalf("command=%s err=%v", raw, err)
	}
}

func TestReturnedDocumentsReachDatasetAndRejectAsync(t *testing.T) {
	server := &nativeRPCServer{}
	opts := sink.ClientOptions{}
	client := startTestClient(t, server, opts)
	datasetOptions := sink.DatasetOptions{Store: "primary", Namespace: "catalog", Dataset: "products", Encoding: sink.DocumentEncodingJSON}
	dataset, err := sink.NewDataset(client, datasetOptions)
	if err != nil {
		t.Fatal(err)
	}
	record := sink.Record{Key: sink.StringKey("1"), Value: map[string]int{"count": 1}, ReturnDocument: true}
	results, err := dataset.Upsert(t.Context(), sink.CompletionWaitUntilApplied, record)
	if err != nil || len(results) != 1 {
		t.Fatalf("results=%v err=%v", results, err)
	}
	var returned map[string]int
	if err := results[0].Document.Decode(&returned); err != nil || returned["count"] != 1 {
		t.Fatalf("document=%v err=%v", returned, err)
	}
	_, err = dataset.Upsert(t.Context(), sink.CompletionReturnAfterAccepted, record)
	if err == nil || server.writeCalls.Load() != 1 {
		t.Fatalf("async returning request sent: calls=%d err=%v", server.writeCalls.Load(), err)
	}
}

func TestMissingRequestedDocumentIsProtocolFailure(t *testing.T) {
	server := &nativeRPCServer{omitDocument: true}
	opts := sink.ClientOptions{}
	client := startTestClient(t, server, opts)
	address := testAddress(t, sink.StringKey("1"))
	document := testDocument("value")
	operation, err := sink.NewPut(address, document, sink.WriteUpsert)
	if err != nil {
		t.Fatal(err)
	}
	operation = operation.WithReturnedDocument()
	_, err = client.Write(t.Context(), sink.CompletionWaitUntilApplied, operation)
	var protocolErr *sink.ProtocolError
	if !errors.As(err, &protocolErr) {
		t.Fatalf("missing requested document error=%v", err)
	}
}
