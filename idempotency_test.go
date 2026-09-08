package sink_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	sink "github.com/liran/sink-go"
	sinkv1 "github.com/liran/sink-go/api/sink/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type idempotentRPCServer struct {
	sinkv1.UnimplementedSinkServer
	mu         sync.Mutex
	requests   []*sinkv1.WriteRequest
	alwaysFail bool
	loseFirst  bool
}

func (s *idempotentRPCServer) WriteIdempotent(_ context.Context, request *sinkv1.WriteRequest) (*sinkv1.WriteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = append(s.requests, proto.Clone(request).(*sinkv1.WriteRequest))
	if s.alwaysFail || (s.loseFirst && len(s.requests) == 1) {
		return nil, status.Error(codes.Unavailable, "lost acknowledgment")
	}
	response := &sinkv1.WriteResponse{}
	for index, operation := range request.Operations {
		if operation.OperationId == "" {
			return nil, status.Error(codes.InvalidArgument, "missing ID")
		}
		revision := &sinkv1.RevisionToken{Data: []byte("receipt")}
		result := &sinkv1.WriteResult{OperationIndex: uint32(index), Status: sinkv1.WriteStatus_WRITE_STATUS_APPLIED, Revision: revision}
		response.Results = append(response.Results, result)
	}
	return response, nil
}

func TestSDKIdempotencyGeneratesBeforeSplitAndReusesAcrossRetries(t *testing.T) {
	server := &idempotentRPCServer{loseFirst: true}
	policy := sink.RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond}
	opts := sink.ClientOptions{IdempotentWrites: true, MaxOperations: 1, ReadRetry: policy}
	client := startTestClient(t, server, opts)
	address := testAddress(t, sink.StringKey("counter"))
	operation, err := sink.NewPut(address, testDocument("value"), sink.WriteUpsert)
	if err != nil {
		t.Fatal(err)
	}
	results, err := client.Write(t.Context(), sink.CompletionWaitUntilApplied, operation, operation)
	if err != nil || len(results) != 2 {
		t.Fatalf("write: %v %v", results, err)
	}
	server.mu.Lock()
	requests := append([]*sinkv1.WriteRequest(nil), server.requests...)
	server.mu.Unlock()
	if len(requests) != 3 || !proto.Equal(requests[0], requests[1]) {
		t.Fatal("transport retry regenerated request/ID")
	}
	if requests[1].Operations[0].OperationId == requests[2].Operations[0].OperationId {
		t.Fatal("distinct logical operations shared an ID")
	}
	if results[0].OperationID == "" || results[0].OperationID == results[1].OperationID {
		t.Fatal("receipt IDs missing")
	}
	if operation.OperationID() != "" {
		t.Fatal("SDK mutated caller's operation")
	}
	more, err := client.Write(t.Context(), sink.CompletionWaitUntilApplied, operation)
	if err != nil || more[0].OperationID == results[0].OperationID {
		t.Fatal("independent SDK calls reused auto ID")
	}
}

func TestSDKIdempotencyFailsClosedOnOldServer(t *testing.T) {
	server := &testSinkServer{}
	opts := sink.ClientOptions{IdempotentWrites: true}
	client := startTestClient(t, server, opts)
	address := testAddress(t, sink.StringKey("key"))
	operation, err := sink.NewPut(address, testDocument("value"), sink.WriteUpsert)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Write(t.Context(), sink.CompletionWaitUntilApplied, operation)
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("old server did not fail closed: %v", err)
	}
	_, writes, _ := server.counts()
	if writes != 0 {
		t.Fatal("SDK fell back to unprotected Write")
	}
}

func TestSDKIdempotencyPreservesPendingOperationsAfterUnknownOutcome(t *testing.T) {
	server := &idempotentRPCServer{alwaysFail: true}
	policy := sink.RetryPolicy{MaxAttempts: 2, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond}
	opts := sink.ClientOptions{IdempotentWrites: true, MaxOperations: 1, ReadRetry: policy}
	client := startTestClient(t, server, opts)
	datasetOpts := sink.DatasetOptions{Store: "primary", Namespace: "catalog", Dataset: "products", Encoding: sink.DocumentEncodingJSON}
	dataset, err := sink.NewDataset(client, datasetOpts)
	if err != nil {
		t.Fatal(err)
	}
	id, err := sink.OperationIDFor("event:1:step:2", time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	record := sink.Record{Key: sink.StringKey("key"), Value: map[string]int{"value": 1}, OperationID: id}
	_, err = dataset.Upsert(t.Context(), sink.CompletionWaitUntilApplied, record, record)
	var outcome *sink.WriteTransportError
	if !errors.As(err, &outcome) || len(outcome.Operations) != 2 {
		t.Fatalf("pending operations lost: %v", err)
	}
	if outcome.Operations[0].OperationID() != id || outcome.Operations[1].OperationID() != id {
		t.Fatal("explicit ID was overwritten")
	}
	server.mu.Lock()
	server.alwaysFail = false
	server.mu.Unlock()
	results, err := client.Write(t.Context(), sink.CompletionWaitUntilApplied, outcome.Operations...)
	if err != nil || results[0].OperationID != id {
		t.Fatalf("retry lost ID: %v %v", results, err)
	}
}

func TestOperationIDValidationAndStableDerivation(t *testing.T) {
	created := time.Now().Add(-time.Hour)
	first, err := sink.OperationIDFor("event", created)
	if err != nil {
		t.Fatal(err)
	}
	second, err := sink.OperationIDFor("event", created)
	if err != nil || first != second {
		t.Fatal("business ID derivation unstable")
	}
	if _, err := sink.OperationIDFor("event", time.Now().Add(-32*24*time.Hour)); err == nil {
		t.Fatal("expired operation accepted")
	}
	server := &idempotentRPCServer{}
	opts := sink.ClientOptions{}
	client := startTestClient(t, server, opts)
	address := testAddress(t, sink.StringKey("key"))
	operation, err := sink.NewPut(address, testDocument("value"), sink.WriteUpsert)
	if err != nil {
		t.Fatal(err)
	}
	operation = operation.WithOperationID("malformed")
	if _, err := client.Write(t.Context(), sink.CompletionWaitUntilApplied, operation); err == nil {
		t.Fatal("invalid ID reached transport")
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	if len(server.requests) != 0 {
		t.Fatal("invalid ID sent")
	}
}
