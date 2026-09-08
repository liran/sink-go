package sink_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	sink "github.com/liran/sink-go"
	sinkv1 "github.com/liran/sink-go/api/sink/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type queryRPCServer struct {
	sinkv1.UnimplementedSinkServer
	queries     chan *sinkv1.QueryRequest
	counts      chan *sinkv1.CountRequest
	queryCalls  atomic.Int32
	countCalls  atomic.Int32
	fail        bool
	invalidPage bool
}

func (s *queryRPCServer) Query(_ context.Context, req *sinkv1.QueryRequest) (*sinkv1.QueryResponse, error) {
	s.queryCalls.Add(1)
	if s.fail {
		return nil, status.Error(codes.Unavailable, "connection lost")
	}
	if s.queries != nil {
		s.queries <- req
	}
	document := &sinkv1.Document{Encoding: sinkv1.DocumentEncoding_DOCUMENT_ENCODING_JSON, Payload: []byte(`{"_source":{"number":4}}`)}
	response := &sinkv1.QueryResponse{Documents: []*sinkv1.Document{document}, HasMore: true}
	if s.invalidPage {
		response.Documents = nil
	}
	return response, nil
}

func (s *queryRPCServer) Count(_ context.Context, req *sinkv1.CountRequest) (*sinkv1.CountResponse, error) {
	s.countCalls.Add(1)
	if s.fail {
		return nil, status.Error(codes.Unavailable, "connection lost")
	}
	if s.counts != nil {
		s.counts <- req
	}
	response := &sinkv1.CountResponse{Count: 1<<53 + 1}
	return response, nil
}

func TestQueryAndCountPreserveControlsAndExactIntegers(t *testing.T) {
	server := &queryRPCServer{queries: make(chan *sinkv1.QueryRequest, 1), counts: make(chan *sinkv1.CountRequest, 1)}
	opts := sink.ClientOptions{}
	client := startTestClient(t, server, opts)
	command := sdkNativeRequest().Command
	projection := &sink.Projection{Fields: []string{"number"}}
	request := sink.QueryRequest{Command: command, Page: 3, PageSize: 1,
		Sort: []sink.SortField{{Field: "number", Descending: true}, {Field: "uid"}}, Projection: projection}
	page, err := client.Query(t.Context(), request)
	if err != nil || len(page.Documents) != 1 || !page.HasMore {
		t.Fatalf("page=%+v err=%v", page, err)
	}
	captured := <-server.queries
	if captured.GetPage() != 3 || captured.GetPageSize() != 1 || len(captured.GetSort()) != 2 || !captured.GetSort()[0].GetDescending() || captured.GetSort()[1].GetField() != "uid" || captured.GetProjection().GetFields()[0] != "number" || string(captured.GetCommand().GetPayload()) != string(command.Payload) {
		t.Fatalf("query lost controls: %v", captured)
	}
	countRequest := sink.CountRequest{Command: command}
	count, err := client.Count(t.Context(), countRequest)
	if err != nil || count != 1<<53+1 || server.countCalls.Load() != 1 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	if captured := <-server.counts; captured.GetCommand().GetStore() != command.Store {
		t.Fatalf("count lost store: %v", captured)
	}
}

func TestQueryValidatesControlsAndRejectsMalformedPages(t *testing.T) {
	server := &queryRPCServer{invalidPage: true}
	opts := sink.ClientOptions{}
	client := startTestClient(t, server, opts)
	command := sdkNativeRequest().Command
	requests := []sink.QueryRequest{
		{Command: command, Page: -1},
		{Command: command, PageSize: 1001},
		{Command: command, Sort: []sink.SortField{{Field: "a"}, {Field: "a"}}},
		{Command: command, Sort: []sink.SortField{{Field: ""}}},
	}
	for _, request := range requests {
		if _, err := client.Query(t.Context(), request); err == nil || server.queryCalls.Load() != 0 {
			t.Fatalf("invalid query sent: %+v err=%v", request, err)
		}
	}
	request := sink.QueryRequest{Command: command, PageSize: 1}
	_, err := client.Query(t.Context(), request)
	var protocolErr *sink.ProtocolError
	if !errors.As(err, &protocolErr) {
		t.Fatalf("invalid has_more accepted: %v", err)
	}
}

func TestQueryAndCountDoNotRetryTransportFailures(t *testing.T) {
	server := &queryRPCServer{fail: true}
	opts := sink.ClientOptions{}
	client := startTestClient(t, server, opts)
	query := sink.QueryRequest{Command: sdkNativeRequest().Command}
	if _, err := client.Query(t.Context(), query); status.Code(err) != codes.Unavailable || server.queryCalls.Load() != 1 {
		t.Fatalf("query retried: %d %v", server.queryCalls.Load(), err)
	}
	count := sink.CountRequest{Command: query.Command}
	if _, err := client.Count(t.Context(), count); status.Code(err) != codes.Unavailable || server.countCalls.Load() != 1 {
		t.Fatalf("count retried: %d %v", server.countCalls.Load(), err)
	}
}
