package sink_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	sink "github.com/liran/sink-go"
	sinkv1 "github.com/liran/sink-go/api/sink/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

const testBufferSize = 1024 * 1024

type responseMode uint8

const (
	responseNormal responseMode = iota
	responseCountMismatch
	responseDuplicateIndex
	responseOutOfRangeIndex
	responseNilResult
	responseMissingFailure
	responseMissingDocument
	responseInvalidStatus
)

type testSinkServer struct {
	sinkv1.UnimplementedSinkServer

	mu                 sync.Mutex
	readCalls          int
	writeCalls         int
	deleteCalls        int
	readFailures       int
	readResultFailures int
	writeFailures      int
	deleteFailures     int
	readFailureCode    codes.Code
	mode               responseMode
	readRequest        *sinkv1.ReadRequest
	writeRequest       *sinkv1.WriteRequest
	deleteRequest      *sinkv1.DeleteRequest
	readDocumentBytes  int
}

func (s *testSinkServer) Read(
	_ context.Context,
	request *sinkv1.ReadRequest,
) (*sinkv1.ReadResponse, error) {
	s.mu.Lock()
	s.readCalls++
	call := s.readCalls
	cloned := proto.Clone(request)
	s.readRequest, _ = cloned.(*sinkv1.ReadRequest)
	s.mu.Unlock()

	if call <= s.readFailures {
		failureCode := s.readFailureCode
		if failureCode == codes.OK {
			failureCode = codes.Unavailable
		}
		return nil, status.Error(failureCode, "temporary read failure")
	}
	results := make([]*sinkv1.ReadResult, len(request.GetOperations()))
	for index := range request.GetOperations() {
		data := []byte(fmt.Sprintf(`{"index":%d}`, index))
		if s.readDocumentBytes > 0 {
			data = bytes.Repeat([]byte("x"), s.readDocumentBytes)
		}
		document := &sinkv1.Document{
			ContentType: "text/plain",
			Data:        data,
		}
		revision := &sinkv1.RevisionToken{Data: []byte{byte(index + 1)}}
		result := &sinkv1.ReadResult{
			OperationIndex: uint32(index),
			Status:         sinkv1.ReadStatus_READ_STATUS_FOUND,
			Document:       document,
			Revision:       revision,
		}
		if call <= s.readResultFailures && index == 0 {
			failure := &sinkv1.Failure{
				Code:      sinkv1.FailureCode_FAILURE_CODE_UNAVAILABLE,
				Message:   "storage is temporarily unavailable",
				Retryable: true,
			}
			result.Status = sinkv1.ReadStatus_READ_STATUS_FAILED
			result.Document = nil
			result.Revision = nil
			result.Failure = failure
		}
		results[len(results)-1-index] = result
	}
	response := &sinkv1.ReadResponse{Results: results}
	s.applyReadMode(response)
	return response, nil
}

func (s *testSinkServer) Write(
	_ context.Context,
	request *sinkv1.WriteRequest,
) (*sinkv1.WriteResponse, error) {
	s.mu.Lock()
	s.writeCalls++
	call := s.writeCalls
	cloned := proto.Clone(request)
	s.writeRequest, _ = cloned.(*sinkv1.WriteRequest)
	s.mu.Unlock()

	if call <= s.writeFailures {
		return nil, status.Error(codes.Unavailable, "temporary write failure")
	}
	results := make([]*sinkv1.WriteResult, len(request.GetOperations()))
	for index := range request.GetOperations() {
		result := &sinkv1.WriteResult{
			OperationIndex: uint32(index),
			Status:         sinkv1.WriteStatus_WRITE_STATUS_APPLIED,
			Revision:       &sinkv1.RevisionToken{Data: []byte{byte(index + 1)}},
		}
		if index == 1 {
			failure := &sinkv1.Failure{
				Code:    sinkv1.FailureCode_FAILURE_CODE_PRECONDITION_FAILED,
				Message: "condition did not match",
			}
			result.Status = sinkv1.WriteStatus_WRITE_STATUS_PRECONDITION_FAILED
			result.Failure = failure
		}
		results[len(results)-1-index] = result
	}
	response := &sinkv1.WriteResponse{Results: results}
	s.applyWriteMode(response)
	return response, nil
}

func (s *testSinkServer) Delete(
	_ context.Context,
	request *sinkv1.DeleteRequest,
) (*sinkv1.DeleteResponse, error) {
	s.mu.Lock()
	s.deleteCalls++
	call := s.deleteCalls
	cloned := proto.Clone(request)
	s.deleteRequest, _ = cloned.(*sinkv1.DeleteRequest)
	s.mu.Unlock()

	if call <= s.deleteFailures {
		return nil, status.Error(codes.Unavailable, "temporary delete failure")
	}
	resultStatus := sinkv1.DeleteStatus_DELETE_STATUS_APPLIED
	if request.GetCompletionMode() == sinkv1.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED {
		resultStatus = sinkv1.DeleteStatus_DELETE_STATUS_ACCEPTED
	}
	results := make([]*sinkv1.DeleteResult, len(request.GetOperations()))
	for index := range request.GetOperations() {
		result := &sinkv1.DeleteResult{
			OperationIndex: uint32(index),
			Status:         resultStatus,
		}
		results[len(results)-1-index] = result
	}
	response := &sinkv1.DeleteResponse{Results: results}
	s.applyDeleteMode(response)
	return response, nil
}

func (s *testSinkServer) applyReadMode(response *sinkv1.ReadResponse) {
	switch s.mode {
	case responseCountMismatch:
		response.Results = response.Results[:1]
	case responseDuplicateIndex:
		response.Results[0].OperationIndex = response.Results[1].OperationIndex
	case responseOutOfRangeIndex:
		response.Results[0].OperationIndex = ^uint32(0)
	case responseNilResult:
		response.Results[0] = nil
	case responseMissingFailure:
		response.Results[0].Status = sinkv1.ReadStatus_READ_STATUS_FAILED
		response.Results[0].Failure = nil
	case responseMissingDocument:
		response.Results[0].Document = nil
	case responseInvalidStatus:
		response.Results[0].Status = sinkv1.ReadStatus_READ_STATUS_UNSPECIFIED
	}
}

func (s *testSinkServer) applyWriteMode(response *sinkv1.WriteResponse) {
	if s.mode == responseDuplicateIndex {
		response.Results[0].OperationIndex = response.Results[1].OperationIndex
	}
}

func (s *testSinkServer) applyDeleteMode(response *sinkv1.DeleteResponse) {
	if s.mode == responseDuplicateIndex {
		response.Results[0].OperationIndex = response.Results[1].OperationIndex
	}
}

func (s *testSinkServer) counts() (int, int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readCalls, s.writeCalls, s.deleteCalls
}

func startTestClient(
	t *testing.T,
	server *testSinkServer,
	opts sink.ClientOptions,
) *sink.Client {
	t.Helper()
	listener := bufconn.Listen(testBufferSize)
	grpcServer := grpc.NewServer()
	sinkv1.RegisterSinkServer(grpcServer, server)
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthv1.HealthCheckResponse_SERVING)
	healthv1.RegisterHealthServer(grpcServer, healthServer)
	go func() {
		_ = grpcServer.Serve(listener)
	}()

	dialer := func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}
	contextDialer := grpc.WithContextDialer(dialer)
	transportOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	connection, err := grpc.NewClient("passthrough:///bufnet", contextDialer, transportOption)
	if err != nil {
		grpcServer.Stop()
		_ = listener.Close()
		t.Fatalf("grpc.NewClient() error = %v", err)
	}
	client, err := sink.New(connection, opts)
	if err != nil {
		_ = connection.Close()
		grpcServer.Stop()
		_ = listener.Close()
		t.Fatalf("sink.New() error = %v", err)
	}
	t.Cleanup(func() {
		_ = connection.Close()
		grpcServer.Stop()
		_ = listener.Close()
	})
	return client
}

func testAddress(t *testing.T, key sink.Key) sink.Address {
	t.Helper()
	address, err := sink.NewAddress("primary", "catalog", "products", key)
	if err != nil {
		t.Fatalf("sink.NewAddress() error = %v", err)
	}
	return address
}

func testDocument(t *testing.T, value string) sink.Document {
	t.Helper()
	document, err := sink.NewDocument("text/plain", []byte(value))
	if err != nil {
		t.Fatalf("sink.NewDocument() error = %v", err)
	}
	return document
}

func TestClientCoversSinkContract(t *testing.T) {
	server := &testSinkServer{}
	var clientOptions sink.ClientOptions
	client := startTestClient(t, server, clientOptions)

	opaque, err := sink.OpaqueKey("mongodb/object-id", []byte("0123456789ab"))
	if err != nil {
		t.Fatalf("sink.OpaqueKey() error = %v", err)
	}
	addresses := []sink.Address{
		testAddress(t, sink.StringKey("alpha")),
		testAddress(t, sink.Int64Key(-42)),
		testAddress(t, sink.BytesKey([]byte{0, 1, 2})),
		testAddress(t, opaque),
	}

	readResults, err := client.Read(context.Background(), addresses...)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if len(readResults) != len(addresses) {
		t.Fatalf("Read() results = %d, want %d", len(readResults), len(addresses))
	}
	for index, result := range readResults {
		if result.OperationIndex != index || result.Status != sink.ReadFound {
			t.Fatalf("Read() result %d = %+v", index, result)
		}
		if len(result.Revision.Bytes()) == 0 {
			t.Fatalf("Read() result %d revision is empty", index)
		}
	}

	putDocument := testDocument(t, "put")
	put, err := sink.NewPut(addresses[0], putDocument, sink.WriteUpsert)
	if err != nil {
		t.Fatalf("sink.NewPut() error = %v", err)
	}
	profile, err := sink.NewMergeProfile("merge-product", 1)
	if err != nil {
		t.Fatalf("sink.NewMergeProfile() error = %v", err)
	}
	mergeOptions := sink.MergeOptions{
		IncomingDocument:    testDocument(t, "merge"),
		Profile:             profile,
		MissingDocumentMode: sink.MissingDocumentCreate,
	}
	merge, err := sink.NewMerge(addresses[1], mergeOptions)
	if err != nil {
		t.Fatalf("sink.NewMerge() error = %v", err)
	}
	writeResults, err := client.Write(context.Background(), sink.CompletionWaitUntilApplied, put, merge)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if writeResults[0].Status != sink.WriteApplied {
		t.Fatalf("Write() first status = %s", writeResults[0].Status)
	}
	if writeResults[1].Status != sink.WritePreconditionFailed || writeResults[1].Failure == nil {
		t.Fatalf("Write() second result = %+v", writeResults[1])
	}
	if writeResults[1].Failure.Code != sink.FailurePreconditionFailed {
		t.Fatalf("Write() failure code = %s", writeResults[1].Failure.Code)
	}

	deleteResults, err := client.Delete(
		context.Background(),
		sink.CompletionReturnAfterAccepted,
		addresses[:2]...,
	)
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	for index, result := range deleteResults {
		if result.OperationIndex != index || result.Status != sink.DeleteAccepted {
			t.Fatalf("Delete() result %d = %+v", index, result)
		}
	}
	if err := client.CheckHealth(context.Background()); err != nil {
		t.Fatalf("CheckHealth() error = %v", err)
	}
	if client.Raw() == nil {
		t.Fatal("Raw() returned nil")
	}

	server.mu.Lock()
	readRequest := server.readRequest
	writeRequest := server.writeRequest
	deleteRequest := server.deleteRequest
	server.mu.Unlock()
	if _, ok := readRequest.GetOperations()[0].GetAddress().GetKey().GetKind().(*sinkv1.RecordKey_StringValue); !ok {
		t.Fatalf("first key type = %T", readRequest.GetOperations()[0].GetAddress().GetKey().GetKind())
	}
	if _, ok := readRequest.GetOperations()[1].GetAddress().GetKey().GetKind().(*sinkv1.RecordKey_Int64Value); !ok {
		t.Fatalf("second key type = %T", readRequest.GetOperations()[1].GetAddress().GetKey().GetKind())
	}
	if _, ok := readRequest.GetOperations()[2].GetAddress().GetKey().GetKind().(*sinkv1.RecordKey_BytesValue); !ok {
		t.Fatalf("third key type = %T", readRequest.GetOperations()[2].GetAddress().GetKey().GetKind())
	}
	if _, ok := readRequest.GetOperations()[3].GetAddress().GetKey().GetKind().(*sinkv1.RecordKey_OpaqueValue); !ok {
		t.Fatalf("fourth key type = %T", readRequest.GetOperations()[3].GetAddress().GetKey().GetKind())
	}
	if writeRequest.GetOperations()[0].GetPut() == nil || writeRequest.GetOperations()[1].GetMerge() == nil {
		t.Fatalf("Write() actions = %T, %T", writeRequest.GetOperations()[0].GetAction(), writeRequest.GetOperations()[1].GetAction())
	}
	if deleteRequest.GetCompletionMode() != sinkv1.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED {
		t.Fatalf("Delete() completion mode = %s", deleteRequest.GetCompletionMode())
	}
}

func TestReadRetriesOnlyUnavailable(t *testing.T) {
	t.Run("unavailable", func(t *testing.T) {
		server := &testSinkServer{readFailures: 2, readFailureCode: codes.Unavailable}
		retry := sink.RetryPolicy{
			MaxAttempts:    3,
			InitialBackoff: time.Nanosecond,
			MaxBackoff:     2 * time.Nanosecond,
			Multiplier:     2,
		}
		clientOptions := sink.ClientOptions{ReadRetry: retry}
		client := startTestClient(t, server, clientOptions)
		address := testAddress(t, sink.StringKey("retry"))
		_, err := client.Read(context.Background(), address)
		if err != nil {
			t.Fatalf("Read() error = %v", err)
		}
		readCalls, _, _ := server.counts()
		if readCalls != 3 {
			t.Fatalf("Read() calls = %d, want 3", readCalls)
		}
	})

	t.Run("resource exhausted", func(t *testing.T) {
		server := &testSinkServer{readFailures: 3, readFailureCode: codes.ResourceExhausted}
		retry := sink.RetryPolicy{
			MaxAttempts:    3,
			InitialBackoff: time.Nanosecond,
			MaxBackoff:     time.Nanosecond,
			Multiplier:     1,
		}
		clientOptions := sink.ClientOptions{ReadRetry: retry}
		client := startTestClient(t, server, clientOptions)
		address := testAddress(t, sink.StringKey("quota"))
		_, err := client.Read(context.Background(), address)
		if status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("Read() code = %s, want ResourceExhausted", status.Code(err))
		}
		readCalls, _, _ := server.counts()
		if readCalls != 1 {
			t.Fatalf("Read() calls = %d, want 1", readCalls)
		}
	})
}

func TestClientAcceptsMessagesLargerThanGRPCDefault(t *testing.T) {
	const documentSize = 5 << 20
	server := &testSinkServer{readDocumentBytes: documentSize}
	var clientOptions sink.ClientOptions
	client := startTestClient(t, server, clientOptions)
	address := testAddress(t, sink.StringKey("large"))
	results, err := client.Read(context.Background(), address)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if got := len(results[0].Document.Bytes()); got != documentSize {
		t.Fatalf("document size = %d, want %d", got, documentSize)
	}
}

func TestReadRetriesOnlyRetryableOperationFailures(t *testing.T) {
	server := &testSinkServer{readResultFailures: 1}
	retry := sink.RetryPolicy{
		MaxAttempts:    2,
		InitialBackoff: time.Nanosecond,
		MaxBackoff:     time.Nanosecond,
		Multiplier:     1,
	}
	clientOptions := sink.ClientOptions{ReadRetry: retry}
	client := startTestClient(t, server, clientOptions)
	addresses := []sink.Address{
		testAddress(t, sink.StringKey("retry")),
		testAddress(t, sink.StringKey("success")),
	}
	results, err := client.Read(context.Background(), addresses...)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	for index, result := range results {
		if result.OperationIndex != index || result.Status != sink.ReadFound || result.Failure != nil {
			t.Fatalf("Read() result[%d] = %+v", index, result)
		}
	}
	readCalls, _, _ := server.counts()
	if readCalls != 2 {
		t.Fatalf("Read() calls = %d, want 2", readCalls)
	}
	server.mu.Lock()
	lastRequestCount := len(server.readRequest.GetOperations())
	server.mu.Unlock()
	if lastRequestCount != 1 {
		t.Fatalf("Read() retry operations = %d, want 1", lastRequestCount)
	}
}

func TestCollectionMethodsSplitBatchesAndRemapIndexes(t *testing.T) {
	server := &testSinkServer{}
	clientOptions := sink.ClientOptions{MaxOperations: 2}
	client := startTestClient(t, server, clientOptions)
	addresses := make([]sink.Address, 5)
	operations := make([]sink.WriteOperation, 5)
	document := testDocument(t, "value")
	for index := range addresses {
		addresses[index] = testAddress(t, sink.StringKey(fmt.Sprintf("record-%d", index)))
		operation, err := sink.NewPut(addresses[index], document, sink.WriteUpsert)
		if err != nil {
			t.Fatalf("NewPut() error = %v", err)
		}
		operations[index] = operation
	}
	readResults, err := client.ReadAll(context.Background(), addresses)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	writeResults, err := client.WriteAll(context.Background(), sink.CompletionWaitUntilApplied, operations)
	if err != nil {
		t.Fatalf("WriteAll() error = %v", err)
	}
	deleteResults, err := client.DeleteAll(context.Background(), sink.CompletionWaitUntilApplied, addresses)
	if err != nil {
		t.Fatalf("DeleteAll() error = %v", err)
	}
	for index := range addresses {
		if readResults[index].OperationIndex != index || writeResults[index].OperationIndex != index || deleteResults[index].OperationIndex != index {
			t.Fatalf("operation indexes at %d = %d, %d, %d", index, readResults[index].OperationIndex, writeResults[index].OperationIndex, deleteResults[index].OperationIndex)
		}
	}
	readCalls, writeCalls, deleteCalls := server.counts()
	if readCalls != 3 || writeCalls != 3 || deleteCalls != 3 {
		t.Fatalf("batch calls = read %d, write %d, delete %d", readCalls, writeCalls, deleteCalls)
	}
	if sink.WriteResultsError(writeResults) == nil || writeResults[1].Err() == nil || writeResults[3].Err() == nil {
		t.Fatalf("WriteResultsError() did not collect remapped failures: %+v", writeResults)
	}
}

func TestMutationsAreNotRetried(t *testing.T) {
	server := &testSinkServer{writeFailures: 2, deleteFailures: 2}
	var clientOptions sink.ClientOptions
	client := startTestClient(t, server, clientOptions)
	address := testAddress(t, sink.StringKey("no-retry"))
	document := testDocument(t, "value")
	put, err := sink.NewPut(address, document, sink.WriteUpsert)
	if err != nil {
		t.Fatalf("sink.NewPut() error = %v", err)
	}
	_, err = client.Write(context.Background(), sink.CompletionWaitUntilApplied, put)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("Write() code = %s, want Unavailable", status.Code(err))
	}
	_, err = client.Delete(context.Background(), sink.CompletionWaitUntilApplied, address)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("Delete() code = %s, want Unavailable", status.Code(err))
	}
	_, writeCalls, deleteCalls := server.counts()
	if writeCalls != 1 || deleteCalls != 1 {
		t.Fatalf("mutation calls = write %d, delete %d; want 1 each", writeCalls, deleteCalls)
	}
}

func TestReadRejectsMalformedResponses(t *testing.T) {
	tests := []struct {
		name string
		mode responseMode
	}{
		{name: "count mismatch", mode: responseCountMismatch},
		{name: "duplicate index", mode: responseDuplicateIndex},
		{name: "out of range index", mode: responseOutOfRangeIndex},
		{name: "nil result", mode: responseNilResult},
		{name: "missing failure", mode: responseMissingFailure},
		{name: "missing document", mode: responseMissingDocument},
		{name: "invalid status", mode: responseInvalidStatus},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := &testSinkServer{mode: test.mode}
			var clientOptions sink.ClientOptions
			client := startTestClient(t, server, clientOptions)
			addresses := []sink.Address{
				testAddress(t, sink.StringKey("one")),
				testAddress(t, sink.StringKey("two")),
			}
			_, err := client.Read(context.Background(), addresses...)
			var protocolErr *sink.ProtocolError
			if !errors.As(err, &protocolErr) {
				t.Fatalf("Read() error = %v, want ProtocolError", err)
			}
		})
	}
}

func TestMutationResponsesRejectDuplicateIndexes(t *testing.T) {
	server := &testSinkServer{mode: responseDuplicateIndex}
	var clientOptions sink.ClientOptions
	client := startTestClient(t, server, clientOptions)
	addresses := []sink.Address{
		testAddress(t, sink.StringKey("one")),
		testAddress(t, sink.StringKey("two")),
	}
	document := testDocument(t, "value")
	operations := make([]sink.WriteOperation, len(addresses))
	for index, address := range addresses {
		operation, err := sink.NewPut(address, document, sink.WriteUpsert)
		if err != nil {
			t.Fatalf("sink.NewPut() error = %v", err)
		}
		operations[index] = operation
	}
	_, err := client.Write(context.Background(), sink.CompletionWaitUntilApplied, operations...)
	var writeProtocolErr *sink.ProtocolError
	if !errors.As(err, &writeProtocolErr) {
		t.Fatalf("Write() error = %v, want ProtocolError", err)
	}
	_, err = client.Delete(context.Background(), sink.CompletionWaitUntilApplied, addresses...)
	var deleteProtocolErr *sink.ProtocolError
	if !errors.As(err, &deleteProtocolErr) {
		t.Fatalf("Delete() error = %v, want ProtocolError", err)
	}
}

func TestConstructorsValidateAndCopyInput(t *testing.T) {
	raw := []byte("original")
	document, err := sink.NewDocument("text/plain", raw)
	if err != nil {
		t.Fatalf("sink.NewDocument() error = %v", err)
	}
	raw[0] = 'X'
	firstCopy := document.Bytes()
	if string(firstCopy) != "original" {
		t.Fatalf("document bytes = %q", firstCopy)
	}
	firstCopy[0] = 'Y'
	if string(document.Bytes()) != "original" {
		t.Fatal("Document.Bytes() exposed mutable storage")
	}

	var emptyKey sink.Key
	_, err = sink.NewAddress("primary", "catalog", "products", emptyKey)
	if err == nil {
		t.Fatal("sink.NewAddress() accepted an empty key")
	}
	_, err = sink.OpaqueKey("", []byte("value"))
	if err == nil {
		t.Fatal("sink.OpaqueKey() accepted an empty type")
	}
	_, err = sink.NewMergeProfile("profile", 0)
	if err == nil {
		t.Fatal("sink.NewMergeProfile() accepted version zero")
	}

	jsonDocument, err := sink.JSONDocument(map[string]string{"name": "sink"})
	if err != nil {
		t.Fatalf("sink.JSONDocument() error = %v", err)
	}
	decoded := make(map[string]string)
	if err := jsonDocument.DecodeJSON(&decoded); err != nil {
		t.Fatalf("DecodeJSON() error = %v", err)
	}
	if decoded["name"] != "sink" {
		t.Fatalf("decoded name = %q", decoded["name"])
	}
}

func TestClientOptionsValidation(t *testing.T) {
	listener := bufconn.Listen(testBufferSize)
	dialer := func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}
	contextDialer := grpc.WithContextDialer(dialer)
	transportOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	connection, err := grpc.NewClient("passthrough:///unused", contextDialer, transportOption)
	if err != nil {
		t.Fatalf("grpc.NewClient() error = %v", err)
	}
	t.Cleanup(func() {
		_ = connection.Close()
		_ = listener.Close()
	})

	tests := []sink.ClientOptions{
		{MaxOperations: -1},
		{ReadRetry: sink.RetryPolicy{MaxAttempts: -1}},
		{ReadRetry: sink.RetryPolicy{InitialBackoff: time.Second, MaxBackoff: time.Millisecond}},
		{ReadRetry: sink.RetryPolicy{Multiplier: 0.5}},
		{ReadRetry: sink.RetryPolicy{Jitter: 1.1}},
		{MaxReceiveMessageBytes: -1},
		{MaxSendMessageBytes: -1},
	}
	for index, options := range tests {
		_, err := sink.New(connection, options)
		if err == nil {
			t.Fatalf("sink.New() options %d succeeded", index)
		}
	}
}
