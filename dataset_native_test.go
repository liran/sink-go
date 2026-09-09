package sink_test

import (
	"bytes"
	"context"
	"net/http"
	"testing"
	"time"

	sink "github.com/liran/sink-go"
	sinkv1 "github.com/liran/sink-go/api/sink/v1"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type datasetNativeServer struct {
	queryRPCServer
	executes chan *sinkv1.ExecuteRequest
	scans    chan *sinkv1.ScanRequest
}

func (s *datasetNativeServer) Execute(_ context.Context, req *sinkv1.ExecuteRequest) (*sinkv1.ExecuteResponse, error) {
	s.executes <- req
	response := &sinkv1.ExecuteResponse{Success: true, ContentType: "application/json", Payload: []byte(`{"ok":true}`)}
	return response, nil
}

func (s *datasetNativeServer) Scan(_ context.Context, req *sinkv1.ScanRequest) (*sinkv1.ScanResponse, error) {
	s.scans <- req
	document := &sinkv1.Document{Encoding: sinkv1.DocumentEncoding_DOCUMENT_ENCODING_JSON, Payload: []byte(`{"number":1}`)}
	response := &sinkv1.ScanResponse{Documents: []*sinkv1.Document{document}}
	return response, nil
}

func TestDatasetNativeMethodsBindScopeAndRetainControls(t *testing.T) {
	for _, encoding := range []sink.DocumentEncoding{sink.DocumentEncodingBSON, sink.DocumentEncodingJSON} {
		t.Run(encoding.String(), func(t *testing.T) {
			server := &datasetNativeServer{executes: make(chan *sinkv1.ExecuteRequest, 1), scans: make(chan *sinkv1.ScanRequest, 1)}
			server.queries = make(chan *sinkv1.QueryRequest, 1)
			server.counts = make(chan *sinkv1.CountRequest, 1)
			clientOptions := sink.ClientOptions{}
			client := startTestClient(t, server, clientOptions)
			opts := sink.DatasetOptions{Store: "primary", Namespace: "catalog", Dataset: "products", Encoding: encoding}
			dataset, err := sink.NewDataset(client, opts)
			if err != nil {
				t.Fatal(err)
			}
			projection := &sink.Projection{Fields: []string{"name"}}
			query := sink.QueryRequest{Page: 3, PageSize: 1, Sort: []sink.SortField{{Field: "name", Descending: true}}, Projection: projection}
			if _, err := dataset.Query(t.Context(), query); err != nil {
				t.Fatal(err)
			}
			captured := <-server.queries
			assertDatasetNativeScope(t, captured.Command, encoding)
			if captured.Page != 3 || captured.PageSize != 1 || !captured.Sort[0].Descending || captured.Projection.Fields[0] != "name" || query.Command.Store != "" || len(query.Command.Payload) != 0 {
				t.Fatalf("query controls or caller request changed: %v %+v", captured, query)
			}
			count := sink.CountRequest{}
			if result, err := dataset.Count(t.Context(), count); err != nil || result.Count != 1<<53+1 || result.Estimated {
				t.Fatalf("count=%+v err=%v", result, err)
			}
			countRequest := <-server.counts
			assertDatasetNativeScope(t, countRequest.Command, encoding)
			scan := sink.ScanRequest{BatchSize: 23, Cursor: []byte("checkpoint")}
			if page, err := dataset.Scan(t.Context(), scan); err != nil || len(page.Documents) != 1 {
				t.Fatalf("scan=%+v err=%v", page, err)
			}
			scanRequest := <-server.scans
			assertDatasetNativeScope(t, scanRequest.Command, encoding)
			if scanRequest.BatchSize != 23 || string(scanRequest.Cursor) != "checkpoint" {
				t.Fatal("batch size lost")
			}
			command := sink.Command{Method: "PUT", Path: "/_mapping", Query: "a=1&a=2", Payload: []byte(`{"properties":{}}`), Headers: http.Header{"Accept": {"application/json"}}}
			if encoding == sink.DocumentEncodingBSON {
				arguments := bson.D{{Key: "comment", Value: time.UnixMilli(123)}}
				command, err = dataset.NewBSONCommand("createIndexes", arguments)
				if err != nil {
					t.Fatal(err)
				}
			}
			execute := sink.ExecuteRequest{Command: command}
			if result, err := dataset.Execute(t.Context(), execute); err != nil || !result.Success {
				t.Fatalf("execute=%+v err=%v", result, err)
			}
			executed := (<-server.executes).Command
			if executed.Store != opts.Store || !bytes.Equal(executed.Payload, command.Payload) {
				t.Fatalf("execute lost command: %v", executed)
			}
			if encoding == sink.DocumentEncodingJSON && (executed.Path != "/products/_mapping" || executed.Query != command.Query || executed.ContentType != "application/json" || len(executed.Headers) != 1 || command.Path != "/_mapping") {
				t.Fatalf("HTTP scope or controls lost: %v", executed)
			}
			if encoding == sink.DocumentEncodingBSON && (bson.Raw(executed.Payload).Lookup("createIndexes").StringValue() != opts.Dataset || bson.Raw(executed.Payload).Lookup("comment").Type != bson.TypeDateTime) {
				t.Fatalf("BSON scope or type lost: %v", executed)
			}
		})
	}
}

func assertDatasetNativeScope(t *testing.T, command *sinkv1.Command, encoding sink.DocumentEncoding) {
	t.Helper()
	if command.Store != "primary" {
		t.Fatalf("store lost: %v", command)
	}
	if encoding == sink.DocumentEncodingBSON {
		if command.Namespace != "catalog" || command.ContentType != "application/bson" || bson.Raw(command.Payload).Lookup("find").StringValue() != "products" {
			t.Fatalf("BSON scope lost: %v", command)
		}
	} else if command.Namespace != "" || command.Method != "POST" || command.Path != "/products/_search" {
		t.Fatalf("search scope lost: %v", command)
	}
}

func TestDatasetNativeRejectsScopeConflictsAndInvalidCommands(t *testing.T) {
	server := &datasetNativeServer{}
	clientOptions := sink.ClientOptions{}
	client := startTestClient(t, server, clientOptions)
	for _, encoding := range []sink.DocumentEncoding{sink.DocumentEncodingBSON, sink.DocumentEncodingJSON} {
		opts := sink.DatasetOptions{Store: "primary", Namespace: "catalog", Dataset: "products", Encoding: encoding}
		dataset, err := sink.NewDataset(client, opts)
		if err != nil {
			t.Fatal(err)
		}
		commands := []sink.Command{{Store: "other"}, {Namespace: "other"}}
		if encoding == sink.DocumentEncodingBSON {
			other := bson.D{{Key: "find", Value: "other"}}
			payload, err := bson.Marshal(other)
			if err != nil {
				t.Fatal(err)
			}
			otherCommand := sink.Command{Payload: payload}
			invalid := sink.Command{Payload: []byte("invalid")}
			wrongEncoding := sink.Command{ContentType: "application/json"}
			commands = append(commands, otherCommand, invalid, wrongEncoding)
			duplicate := bson.D{{Key: "find", Value: "other"}}
			if _, err := dataset.NewBSONCommand("find", duplicate); err == nil {
				t.Fatal("builder allowed a second collection target")
			}
		} else {
			for _, path := range []string{"https://example.com/", "//other/_search", "../other", "/../other", "/%2e%2e/other", "/_search?q=x", "/_search#fragment", "/%zz"} {
				command := sink.Command{Path: path}
				commands = append(commands, command)
			}
		}
		for _, command := range commands {
			request := sink.QueryRequest{Command: command}
			if _, err := dataset.Query(t.Context(), request); err == nil {
				t.Fatalf("invalid scope accepted: %+v", command)
			}
		}
	}
	var dataset *sink.Dataset
	query := sink.QueryRequest{}
	count := sink.CountRequest{}
	execute := sink.ExecuteRequest{}
	scan := sink.ScanRequest{}
	if _, err := dataset.Query(t.Context(), query); err == nil {
		t.Fatal("nil Dataset Query accepted")
	}
	if _, err := dataset.Count(t.Context(), count); err == nil {
		t.Fatal("nil Dataset Count accepted")
	}
	if _, err := dataset.Execute(t.Context(), execute); err == nil {
		t.Fatal("nil Dataset Execute accepted")
	}
	if _, err := dataset.Scan(t.Context(), scan); err == nil {
		t.Fatal("nil Dataset Scan accepted")
	}
}

func TestCountPreservesAutomaticEstimateMetadata(t *testing.T) {
	for _, estimated := range []bool{false, true} {
		server := &queryRPCServer{estimated: estimated}
		opts := sink.ClientOptions{}
		client := startTestClient(t, server, opts)
		request := sink.CountRequest{Command: sdkNativeRequest().Command}
		result, err := client.Count(t.Context(), request)
		if err != nil || result.Estimated != estimated || result.Count != 1<<53+1 {
			t.Fatalf("count metadata lost: %+v %v", result, err)
		}
	}
}

func TestDatasetBSONPlaceholderPreservesTypesAndCallerBytes(t *testing.T) {
	server := &queryRPCServer{queries: make(chan *sinkv1.QueryRequest, 1)}
	clientOptions := sink.ClientOptions{}
	client := startTestClient(t, server, clientOptions)
	opts := sink.DatasetOptions{Store: "primary", Namespace: "catalog", Dataset: "products", Encoding: sink.DocumentEncodingBSON}
	dataset, err := sink.NewDataset(client, opts)
	if err != nil {
		t.Fatal(err)
	}
	id := bson.NewObjectID()
	filter := bson.D{{Key: "_id", Value: id}, {Key: "at", Value: time.UnixMilli(123)}, {Key: "n", Value: int64(1<<53 + 1)}}
	value := bson.D{{Key: "find", Value: ""}, {Key: "filter", Value: filter}}
	payload, err := bson.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	original := bytes.Clone(payload)
	command := sink.Command{Payload: payload}
	request := sink.QueryRequest{Command: command, PageSize: 1}
	if _, err := dataset.Query(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	captured := (<-server.queries).Command
	raw := bson.Raw(captured.Payload)
	if !bytes.Equal(payload, original) || raw.Lookup("find").StringValue() != "products" || raw.Lookup("filter", "_id").ObjectID() != id || raw.Lookup("filter", "at").DateTime() != 123 || raw.Lookup("filter", "n").Int64() != 1<<53+1 {
		t.Fatalf("placeholder binding lost native BSON or changed caller: %s", raw)
	}
}
