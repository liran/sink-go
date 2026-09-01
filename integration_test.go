//go:build integration

package sink_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	sink "github.com/liran/sink-go"
	"google.golang.org/grpc/credentials/insecure"
)

type waitDocumentOptions struct {
	Dataset *sink.Dataset
	Key     sink.Key
	Name    string
	Stage   string
}

func TestSinkCompatibility(t *testing.T) {
	target := os.Getenv("SINK_INTEGRATION_ADDRESS")
	if target == "" {
		t.Skip("SINK_INTEGRATION_ADDRESS is not set")
	}
	retryPolicy := sink.RetryPolicy{
		MaxAttempts:    5,
		InitialBackoff: 20 * time.Millisecond,
		MaxBackoff:     200 * time.Millisecond,
		Multiplier:     2,
	}
	clientOptions := sink.ClientOptions{ReadRetry: retryPolicy}
	dialOptions := sink.DialOptions{
		Client:               clientOptions,
		TransportCredentials: insecure.NewCredentials(),
	}
	client, err := sink.Dial(target, dialOptions)
	if err != nil {
		t.Fatalf("sink.Dial() error = %v", err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := waitForHealth(ctx, client); err != nil {
		t.Fatalf("wait for Sink health: %v", err)
	}

	keyPrefix := fmt.Sprintf("sink-go-%d", time.Now().UnixNano())
	syncKey := sink.StringKey(keyPrefix + "-sync")
	asyncKey := sink.StringKey(keyPrefix + "-async")
	syncAddress := integrationAddress(t, keyPrefix+"-sync")
	asyncAddress := integrationAddress(t, keyPrefix+"-async")
	mergeSource := []byte("return function(current, incoming) return incoming end")
	program, err := sink.NewLuaProgram(mergeSource)
	if err != nil {
		t.Fatalf("sink.NewLuaProgram() error = %v", err)
	}
	datasetOptions := sink.DatasetOptions{
		Store:        "primary",
		Namespace:    "sink_go_client",
		Dataset:      "compatibility",
		Encoding:     sink.DocumentEncodingBSON,
		MergeProgram: &program,
	}
	dataset, err := sink.NewDataset(client, datasetOptions)
	if err != nil {
		t.Fatalf("sink.NewDataset() error = %v", err)
	}
	syncValue := integrationValueFor("sync", "applied")
	syncRecord := sink.Record{Key: syncKey, Value: syncValue}
	writeResults, err := dataset.Upsert(ctx, sink.CompletionWaitUntilVisible, syncRecord)
	if err != nil {
		t.Fatalf("Dataset.Upsert(sync) error = %v", err)
	}
	assertWriteStatus(t, writeResults, sink.WriteApplied)

	readResults, err := dataset.Read(ctx, syncKey)
	if err != nil {
		t.Fatalf("Dataset.Read(sync) error = %v", err)
	}
	assertReadDocument(t, readResults, "sync", "applied")

	writeResults, err = dataset.Create(ctx, sink.CompletionWaitUntilApplied, syncRecord)
	if err == nil {
		t.Fatal("Dataset.Create(duplicate) succeeded")
	}
	assertWriteFailure(t, writeResults, sink.WritePreconditionFailed, sink.FailurePreconditionFailed)

	writeResults, err = dataset.Merge(
		ctx,
		sink.CompletionWaitUntilVisible,
		sink.MissingDocumentFail,
		syncRecord,
	)
	if err != nil {
		t.Fatalf("Dataset.Merge() error = %v", err)
	}
	assertWriteStatus(t, writeResults, sink.WriteApplied)

	asyncValue := integrationValueFor("async", "accepted")
	asyncRecord := sink.Record{Key: asyncKey, Value: asyncValue}
	writeResults, err = dataset.Upsert(ctx, sink.CompletionReturnAfterAccepted, asyncRecord)
	if err != nil {
		t.Fatalf("Dataset.Upsert(async) error = %v", err)
	}
	assertWriteStatus(t, writeResults, sink.WriteAccepted)
	waitOptions := waitDocumentOptions{
		Dataset: dataset,
		Key:     asyncKey,
		Name:    "async",
		Stage:   "accepted",
	}
	if err := waitForDocument(ctx, waitOptions); err != nil {
		t.Fatalf("wait for asynchronous document: %v", err)
	}

	deleteResults, err := client.Delete(
		ctx,
		sink.CompletionWaitUntilVisible,
		syncAddress,
		asyncAddress,
	)
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	for index, result := range deleteResults {
		if result.Status != sink.DeleteApplied || result.Failure != nil {
			t.Fatalf("Delete() result %d = %+v", index, result)
		}
	}
	readResults, err = dataset.Read(ctx, syncKey, asyncKey)
	if err != nil {
		t.Fatalf("Dataset.Read(after delete) error = %v", err)
	}
	for index, result := range readResults {
		if result.Status != sink.ReadNotFound {
			t.Fatalf("Read(after delete) result %d = %+v", index, result)
		}
	}
}

func integrationAddress(t *testing.T, key string) sink.Address {
	t.Helper()
	address, err := sink.NewAddress("primary", "sink_go_client", "compatibility", sink.StringKey(key))
	if err != nil {
		t.Fatalf("sink.NewAddress() error = %v", err)
	}
	return address
}

type integrationValue struct {
	Name      string    `bson:"name"`
	Stage     string    `bson:"stage"`
	CreatedAt time.Time `bson:"created_at"`
}

func integrationDocument(t *testing.T, name string, stage string) sink.Document {
	t.Helper()
	value := integrationValueFor(name, stage)
	document, err := sink.NewDocument(value, sink.DocumentEncodingBSON)
	if err != nil {
		t.Fatalf("sink.NewDocument() error = %v", err)
	}
	return document
}

func integrationValueFor(name string, stage string) integrationValue {
	value := integrationValue{
		Name:      name,
		Stage:     stage,
		CreatedAt: integrationDateTime(),
	}
	return value
}

func integrationDateTime() time.Time {
	return time.Date(2026, time.August, 29, 4, 34, 56, 789000000, time.UTC)
}

func waitForHealth(ctx context.Context, client *sink.Client) error {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		attemptContext, cancel := context.WithTimeout(ctx, time.Second)
		err := client.CheckHealth(attemptContext)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %v", ctx.Err(), lastErr)
		case <-ticker.C:
		}
	}
}

func waitForDocument(
	ctx context.Context,
	opts waitDocumentOptions,
) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		results, err := opts.Dataset.Read(ctx, opts.Key)
		if err == nil && len(results) == 1 && results[0].Status == sink.ReadFound {
			var document integrationValue
			decodeErr := results[0].Document.Decode(&document)
			if decodeErr == nil && document.Name == opts.Name && document.Stage == opts.Stage &&
				document.CreatedAt.Equal(integrationDateTime()) {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func assertWriteStatus(t *testing.T, results []sink.WriteResult, want sink.WriteStatus) {
	t.Helper()
	if len(results) != 1 {
		t.Fatalf("Write() results = %d, want 1", len(results))
	}
	if results[0].Status != want || results[0].Failure != nil {
		t.Fatalf("Write() result = %+v, want status %s", results[0], want)
	}
}

func assertWriteFailure(
	t *testing.T,
	results []sink.WriteResult,
	wantStatus sink.WriteStatus,
	wantCode sink.FailureCode,
) {
	t.Helper()
	if len(results) != 1 || results[0].Failure == nil {
		t.Fatalf("Write() results = %+v, want one failure", results)
	}
	if results[0].Status != wantStatus || results[0].Failure.Code != wantCode {
		t.Fatalf(
			"Write() result = %+v, want status %s and code %s",
			results[0],
			wantStatus,
			wantCode,
		)
	}
}

func assertReadDocument(
	t *testing.T,
	results []sink.ReadResult,
	wantName string,
	wantStage string,
) {
	t.Helper()
	if len(results) != 1 || results[0].Status != sink.ReadFound {
		t.Fatalf("Read() results = %+v, want one found document", results)
	}
	var document integrationValue
	if err := results[0].Document.Decode(&document); err != nil {
		t.Fatalf("Read() decode document: %v", err)
	}
	if document.Name != wantName || document.Stage != wantStage || !document.CreatedAt.Equal(integrationDateTime()) {
		t.Fatalf("Read() document does not contain name %q and stage %q", wantName, wantStage)
	}
}
