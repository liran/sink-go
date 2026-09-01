package sink_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	sink "github.com/liran/sink-go"
	sinkv1 "github.com/liran/sink-go/api/sink/v1"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type datasetProduct struct {
	UID   string `json:"uid" bson:"_id"`
	Name  string `json:"name" bson:"name"`
	Stock int    `json:"stock" bson:"stock"`
}

func TestNewDatasetValidatesAndCopiesOptions(t *testing.T) {
	server := &testSinkServer{suppressWriteError: true}
	var clientOptions sink.ClientOptions
	client := startTestClient(t, server, clientOptions)
	validOptions := sink.DatasetOptions{
		Store:     "search-main",
		Namespace: "catalog",
		Dataset:   "products",
		Encoding:  sink.DocumentEncodingJSON,
	}

	var nilClient *sink.Client
	_, err := sink.NewDataset(nilClient, validOptions)
	if err == nil {
		t.Fatal("NewDataset() accepted a nil client")
	}

	missingStore := validOptions
	missingStore.Store = ""
	missingNamespace := validOptions
	missingNamespace.Namespace = ""
	missingDataset := validOptions
	missingDataset.Dataset = ""
	missingEncoding := validOptions
	missingEncoding.Encoding = 0
	var emptyMergeProgram sink.LuaProgram
	emptyProgram := validOptions
	emptyProgram.MergeProgram = &emptyMergeProgram
	tests := []struct {
		name    string
		options sink.DatasetOptions
	}{
		{name: "missing store", options: missingStore},
		{name: "missing namespace", options: missingNamespace},
		{name: "missing dataset", options: missingDataset},
		{name: "missing encoding", options: missingEncoding},
		{name: "empty merge program", options: emptyProgram},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := sink.NewDataset(client, test.options)
			if err == nil {
				t.Fatal("NewDataset() accepted invalid options")
			}
		})
	}

	dataset, err := sink.NewDataset(client, validOptions)
	if err != nil {
		t.Fatalf("NewDataset() error = %v", err)
	}
	var nilDataset *sink.Dataset
	record := sink.Record{
		Key:   sink.StringKey("product-1"),
		Value: datasetProduct{UID: "product-1", Name: "keyboard"},
	}
	_, err = nilDataset.Upsert(t.Context(), sink.CompletionWaitUntilApplied, record)
	if err == nil {
		t.Fatal("Dataset.Upsert() accepted a nil dataset")
	}
	_, err = dataset.Upsert(t.Context(), 0, record)
	if err == nil {
		t.Fatal("Dataset.Upsert() accepted an invalid completion mode")
	}
}

func TestDatasetPutMethodsBindRoutingEncodingAndPerCallCompletion(t *testing.T) {
	server := &testSinkServer{suppressWriteError: true}
	var clientOptions sink.ClientOptions
	client := startTestClient(t, server, clientOptions)
	datasetOptions := sink.DatasetOptions{
		Store:     "search-main",
		Namespace: "product-search-engine",
		Dataset:   "product",
		Encoding:  sink.DocumentEncodingJSON,
	}
	dataset, err := sink.NewDataset(client, datasetOptions)
	if err != nil {
		t.Fatalf("NewDataset() error = %v", err)
	}

	type methodExpectation struct {
		name           string
		key            string
		completionMode sink.CompletionMode
		writeMode      sink.WriteMode
	}
	expectations := []methodExpectation{
		{
			name:           "create",
			key:            "product-create",
			completionMode: sink.CompletionWaitUntilApplied,
			writeMode:      sink.WriteCreate,
		},
		{
			name:           "replace",
			key:            "product-replace",
			completionMode: sink.CompletionWaitUntilVisible,
			writeMode:      sink.WriteReplace,
		},
		{
			name:           "upsert",
			key:            "product-upsert",
			completionMode: sink.CompletionReturnAfterAccepted,
			writeMode:      sink.WriteUpsert,
		},
	}
	createProduct := datasetProduct{UID: expectations[0].key, Name: expectations[0].name, Stock: 12}
	createRecord := sink.Record{Key: sink.StringKey(createProduct.UID), Value: createProduct}
	createResults, err := dataset.Create(t.Context(), sink.CompletionWaitUntilApplied, createRecord)
	if err != nil {
		t.Fatalf("Dataset.Create() error = %v", err)
	}
	replaceProduct := datasetProduct{UID: expectations[1].key, Name: expectations[1].name, Stock: 12}
	replaceRecord := sink.Record{Key: sink.StringKey(replaceProduct.UID), Value: replaceProduct}
	replaceResults, err := dataset.Replace(t.Context(), sink.CompletionWaitUntilVisible, replaceRecord)
	if err != nil {
		t.Fatalf("Dataset.Replace() error = %v", err)
	}
	upsertProduct := datasetProduct{UID: expectations[2].key, Name: expectations[2].name, Stock: 12}
	upsertRecord := sink.Record{Key: sink.StringKey(upsertProduct.UID), Value: upsertProduct}
	upsertResults, err := dataset.Upsert(t.Context(), sink.CompletionReturnAfterAccepted, upsertRecord)
	if err != nil {
		t.Fatalf("Dataset.Upsert() error = %v", err)
	}
	methodResults := [][]sink.WriteResult{createResults, replaceResults, upsertResults}
	for index, results := range methodResults {
		if len(results) != 1 || results[0].OperationIndex != 0 {
			t.Fatalf("Dataset.%s() results = %+v", expectations[index].name, results)
		}
	}

	server.mu.Lock()
	requests := append([]*sinkv1.WriteRequest(nil), server.writeRequests...)
	server.mu.Unlock()
	if len(requests) != len(expectations) {
		t.Fatalf("write requests = %d, want %d", len(requests), len(expectations))
	}
	for index, method := range expectations {
		request := requests[index]
		if request.GetCompletionMode() != method.completionMode {
			t.Fatalf("Dataset.%s() completion mode = %s", method.name, request.GetCompletionMode())
		}
		if len(request.GetOperations()) != 1 {
			t.Fatalf("Dataset.%s() operations = %d", method.name, len(request.GetOperations()))
		}
		operation := request.GetOperations()[0]
		address := operation.GetAddress()
		if address.GetStore() != datasetOptions.Store || address.GetNamespace() != datasetOptions.Namespace ||
			address.GetDataset() != datasetOptions.Dataset || address.GetKey().GetStringValue() != method.key {
			t.Fatalf("Dataset.%s() address = %+v", method.name, address)
		}
		put := operation.GetPut()
		if put == nil || put.GetMode() != method.writeMode {
			t.Fatalf("Dataset.%s() put = %+v", method.name, put)
		}
		if put.GetDocument().GetEncoding() != sinkv1.DocumentEncoding_DOCUMENT_ENCODING_JSON {
			t.Fatalf("Dataset.%s() encoding = %s", method.name, put.GetDocument().GetEncoding())
		}
		var value map[string]any
		if err := json.Unmarshal(put.GetDocument().GetPayload(), &value); err != nil {
			t.Fatalf("Dataset.%s() JSON decode error = %v", method.name, err)
		}
		if value["uid"] != method.key || value["_id"] != nil {
			t.Fatalf("Dataset.%s() document = %#v", method.name, value)
		}
	}
}

func TestDatasetUsesConfiguredBSONEncoding(t *testing.T) {
	server := &testSinkServer{suppressWriteError: true}
	var clientOptions sink.ClientOptions
	client := startTestClient(t, server, clientOptions)
	datasetOptions := sink.DatasetOptions{
		Store:     "mongo-main",
		Namespace: "catalog",
		Dataset:   "products",
		Encoding:  sink.DocumentEncodingBSON,
	}
	dataset, err := sink.NewDataset(client, datasetOptions)
	if err != nil {
		t.Fatalf("NewDataset() error = %v", err)
	}
	product := datasetProduct{UID: "product-bson", Name: "keyboard", Stock: 4}
	record := sink.Record{Key: sink.StringKey(product.UID), Value: product}
	_, err = dataset.Upsert(t.Context(), sink.CompletionWaitUntilApplied, record)
	if err != nil {
		t.Fatalf("Dataset.Upsert() error = %v", err)
	}

	server.mu.Lock()
	document := server.writeRequest.GetOperations()[0].GetPut().GetDocument()
	server.mu.Unlock()
	if document.GetEncoding() != sinkv1.DocumentEncoding_DOCUMENT_ENCODING_BSON {
		t.Fatalf("Dataset.Upsert() encoding = %s", document.GetEncoding())
	}
	raw := bson.Raw(document.GetPayload())
	if raw.Lookup("_id").StringValue() != product.UID || raw.Lookup("uid").Type != 0 {
		t.Fatalf("Dataset.Upsert() BSON = %v", raw)
	}
}

func TestDatasetMergeUsesOneBoundProgramForBatch(t *testing.T) {
	server := &testSinkServer{suppressWriteError: true}
	var clientOptions sink.ClientOptions
	client := startTestClient(t, server, clientOptions)
	mergeSource := []byte("return function(current, incoming) return incoming end")
	program, err := sink.NewLuaProgram(mergeSource)
	if err != nil {
		t.Fatalf("NewLuaProgram() error = %v", err)
	}
	datasetOptions := sink.DatasetOptions{
		Store:        "search-main",
		Namespace:    "product-search-engine",
		Dataset:      "product",
		Encoding:     sink.DocumentEncodingJSON,
		MergeProgram: &program,
	}
	dataset, err := sink.NewDataset(client, datasetOptions)
	if err != nil {
		t.Fatalf("NewDataset() error = %v", err)
	}
	var emptyProgram sink.LuaProgram
	program = emptyProgram
	records := []sink.Record{
		{Key: sink.StringKey("product-1"), Value: datasetProduct{UID: "product-1", Stock: 1}},
		{Key: sink.StringKey("product-2"), Value: datasetProduct{UID: "product-2", Stock: 2}},
	}
	results, err := dataset.Merge(
		t.Context(),
		sink.CompletionWaitUntilVisible,
		sink.MissingDocumentCreate,
		records...,
	)
	if err != nil {
		t.Fatalf("Dataset.Merge() error = %v", err)
	}
	if len(results) != len(records) {
		t.Fatalf("Dataset.Merge() results = %d, want %d", len(results), len(records))
	}

	server.mu.Lock()
	request := server.writeRequest
	server.mu.Unlock()
	if request.GetCompletionMode() != sinkv1.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE {
		t.Fatalf("Dataset.Merge() completion mode = %s", request.GetCompletionMode())
	}
	wantDigest := sha256.Sum256(mergeSource)
	if len(request.GetLuaPrograms()) != 1 ||
		!bytes.Equal(request.GetLuaPrograms()[0].GetSource(), mergeSource) ||
		!bytes.Equal(request.GetLuaPrograms()[0].GetSha256(), wantDigest[:]) {
		t.Fatalf("Dataset.Merge() programs = %+v", request.GetLuaPrograms())
	}
	for index, operation := range request.GetOperations() {
		merge := operation.GetMerge()
		if merge == nil || merge.GetMissingDocumentMode() != sinkv1.MissingDocumentMode_MISSING_DOCUMENT_MODE_CREATE {
			t.Fatalf("Dataset.Merge() operation %d = %+v", index, operation)
		}
		if len(merge.GetLuaProgram().GetSource()) != 0 ||
			!bytes.Equal(merge.GetLuaProgram().GetSha256(), wantDigest[:]) {
			t.Fatalf("Dataset.Merge() program reference %d = %+v", index, merge.GetLuaProgram())
		}
	}

	putOnlyOptions := datasetOptions
	putOnlyOptions.MergeProgram = nil
	putOnlyDataset, err := sink.NewDataset(client, putOnlyOptions)
	if err != nil {
		t.Fatalf("NewDataset(put only) error = %v", err)
	}
	_, err = putOnlyDataset.Merge(
		t.Context(),
		sink.CompletionWaitUntilApplied,
		sink.MissingDocumentFail,
		records[0],
	)
	if err == nil || !strings.Contains(err.Error(), "merge program is not configured") {
		t.Fatalf("Dataset.Merge() without program error = %v", err)
	}
}

func TestDatasetUpsertSplitsBatchesAndCollectsOperationFailures(t *testing.T) {
	server := &testSinkServer{}
	clientOptions := sink.ClientOptions{MaxOperations: 2}
	client := startTestClient(t, server, clientOptions)
	datasetOptions := sink.DatasetOptions{
		Store:     "search-main",
		Namespace: "product-search-engine",
		Dataset:   "product",
		Encoding:  sink.DocumentEncodingJSON,
	}
	dataset, err := sink.NewDataset(client, datasetOptions)
	if err != nil {
		t.Fatalf("NewDataset() error = %v", err)
	}
	records := make([]sink.Record, 5)
	for index := range records {
		uid := fmt.Sprintf("product-%d", index)
		product := datasetProduct{UID: uid, Stock: index}
		records[index] = sink.Record{Key: sink.StringKey(uid), Value: product}
	}
	results, err := dataset.Upsert(t.Context(), sink.CompletionReturnAfterAccepted, records...)
	if len(results) != len(records) {
		t.Fatalf("Dataset.Upsert() results = %d, want %d", len(results), len(records))
	}
	var batchError *sink.BatchError
	if !errors.As(err, &batchError) {
		t.Fatalf("Dataset.Upsert() error = %v, want BatchError", err)
	}
	if len(batchError.Failures) != 2 || batchError.Failures[0].OperationIndex != 1 ||
		batchError.Failures[1].OperationIndex != 3 {
		t.Fatalf("Dataset.Upsert() failures = %+v", batchError.Failures)
	}
	for _, index := range []int{1, 3} {
		if results[index].Failure == nil || results[index].Failure.OperationIndex != index {
			t.Fatalf("Dataset.Upsert() result %d = %+v", index, results[index])
		}
	}

	server.mu.Lock()
	requests := append([]*sinkv1.WriteRequest(nil), server.writeRequests...)
	server.mu.Unlock()
	wantBatchSizes := []int{2, 2, 1}
	if len(requests) != len(wantBatchSizes) {
		t.Fatalf("Dataset.Upsert() requests = %d, want %d", len(requests), len(wantBatchSizes))
	}
	for index, request := range requests {
		if len(request.GetOperations()) != wantBatchSizes[index] {
			t.Fatalf("Dataset.Upsert() request %d operations = %d", index, len(request.GetOperations()))
		}
		if request.GetCompletionMode() != sinkv1.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED {
			t.Fatalf("Dataset.Upsert() request %d completion = %s", index, request.GetCompletionMode())
		}
	}
}

func TestDatasetUpsertReturnsPartialResultsOnTransportFailure(t *testing.T) {
	server := &testSinkServer{writeFailureAt: 2}
	clientOptions := sink.ClientOptions{MaxOperations: 2}
	client := startTestClient(t, server, clientOptions)
	datasetOptions := sink.DatasetOptions{
		Store:     "search-main",
		Namespace: "product-search-engine",
		Dataset:   "product",
		Encoding:  sink.DocumentEncodingJSON,
	}
	dataset, err := sink.NewDataset(client, datasetOptions)
	if err != nil {
		t.Fatalf("NewDataset() error = %v", err)
	}
	records := []sink.Record{
		{Key: sink.StringKey("product-1"), Value: datasetProduct{UID: "product-1"}},
		{Key: sink.StringKey("product-2"), Value: datasetProduct{UID: "product-2"}},
		{Key: sink.StringKey("product-3"), Value: datasetProduct{UID: "product-3"}},
	}
	results, err := dataset.Upsert(t.Context(), sink.CompletionWaitUntilApplied, records...)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("Dataset.Upsert() error code = %s, want Unavailable", status.Code(err))
	}
	var batchError *sink.BatchError
	if !errors.As(err, &batchError) || len(batchError.Failures) != 1 ||
		batchError.Failures[0].OperationIndex != 1 {
		t.Fatalf("Dataset.Upsert() operation error = %v", err)
	}
	if len(results) != 2 || results[0].OperationIndex != 0 || results[1].OperationIndex != 1 {
		t.Fatalf("Dataset.Upsert() partial results = %+v", results)
	}
	_, writeCalls, _ := server.counts()
	if writeCalls != 2 {
		t.Fatalf("Dataset.Upsert() calls = %d, want 2", writeCalls)
	}
}

func TestDatasetRejectsInvalidRecordsBeforeWrite(t *testing.T) {
	server := &testSinkServer{suppressWriteError: true}
	var clientOptions sink.ClientOptions
	client := startTestClient(t, server, clientOptions)
	datasetOptions := sink.DatasetOptions{
		Store:     "search-main",
		Namespace: "product-search-engine",
		Dataset:   "product",
		Encoding:  sink.DocumentEncodingJSON,
	}
	dataset, err := sink.NewDataset(client, datasetOptions)
	if err != nil {
		t.Fatalf("NewDataset() error = %v", err)
	}
	var emptyKey sink.Key
	record := sink.Record{Key: emptyKey, Value: datasetProduct{UID: "product-1"}}
	_, err = dataset.Upsert(t.Context(), sink.CompletionWaitUntilApplied, record)
	if err == nil || !strings.Contains(err.Error(), "dataset upsert record 0") {
		t.Fatalf("Dataset.Upsert() invalid record error = %v", err)
	}
	_, writeCalls, _ := server.counts()
	if writeCalls != 0 {
		t.Fatalf("Dataset.Upsert() calls = %d, want 0", writeCalls)
	}
}
