//go:build integration

package sink_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"testing"
	"time"

	sink "github.com/liran/sink-go"
	"google.golang.org/grpc/credentials/insecure"
)

type waitDocumentOptions struct {
	Client  *sink.Client
	Address sink.Address
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
	syncAddress := integrationAddress(t, keyPrefix+"-sync")
	asyncAddress := integrationAddress(t, keyPrefix+"-async")
	syncDocument := integrationBSONDocument(t, "sync", "applied")
	asyncDocument := integrationBSONDocument(t, "async", "accepted")

	put, err := sink.NewPut(syncAddress, syncDocument, sink.WriteUpsert)
	if err != nil {
		t.Fatalf("sink.NewPut() error = %v", err)
	}
	writeResults, err := client.Write(ctx, sink.CompletionWaitUntilApplied, put)
	if err != nil {
		t.Fatalf("Write(sync) error = %v", err)
	}
	assertWriteStatus(t, writeResults, sink.WriteApplied)

	readResults, err := client.Read(ctx, syncAddress)
	if err != nil {
		t.Fatalf("Read(sync) error = %v", err)
	}
	assertReadDocument(t, readResults, "sync", "applied")

	create, err := sink.NewPut(syncAddress, syncDocument, sink.WriteCreate)
	if err != nil {
		t.Fatalf("sink.NewPut(create) error = %v", err)
	}
	writeResults, err = client.Write(ctx, sink.CompletionWaitUntilApplied, create)
	if err != nil {
		t.Fatalf("Write(create duplicate) error = %v", err)
	}
	assertWriteFailure(t, writeResults, sink.WritePreconditionFailed, sink.FailurePreconditionFailed)

	profile, err := sink.NewMergeProfile("missing-client-test-profile", 1)
	if err != nil {
		t.Fatalf("sink.NewMergeProfile() error = %v", err)
	}
	mergeOptions := sink.MergeOptions{
		IncomingDocument:    syncDocument,
		Profile:             profile,
		MissingDocumentMode: sink.MissingDocumentFail,
	}
	merge, err := sink.NewMerge(syncAddress, mergeOptions)
	if err != nil {
		t.Fatalf("sink.NewMerge() error = %v", err)
	}
	writeResults, err = client.Write(ctx, sink.CompletionWaitUntilApplied, merge)
	if err != nil {
		t.Fatalf("Write(merge) error = %v", err)
	}
	assertWriteFailure(t, writeResults, sink.WriteFailed, sink.FailureInvalidArgument)

	asyncPut, err := sink.NewPut(asyncAddress, asyncDocument, sink.WriteUpsert)
	if err != nil {
		t.Fatalf("sink.NewPut(async) error = %v", err)
	}
	writeResults, err = client.Write(ctx, sink.CompletionReturnAfterAccepted, asyncPut)
	if err != nil {
		t.Fatalf("Write(async) error = %v", err)
	}
	assertWriteStatus(t, writeResults, sink.WriteAccepted)
	waitOptions := waitDocumentOptions{
		Client:  client,
		Address: asyncAddress,
		Name:    "async",
		Stage:   "accepted",
	}
	if err := waitForDocument(ctx, waitOptions); err != nil {
		t.Fatalf("wait for asynchronous document: %v", err)
	}

	deleteResults, err := client.Delete(
		ctx,
		sink.CompletionWaitUntilApplied,
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
	readResults, err = client.Read(ctx, syncAddress, asyncAddress)
	if err != nil {
		t.Fatalf("Read(after delete) error = %v", err)
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

func integrationBSONDocument(t *testing.T, name string, stage string) sink.Document {
	t.Helper()
	elements := make([]byte, 0)
	elements = appendBSONString(elements, "name", name)
	elements = appendBSONString(elements, "stage", stage)
	encoded := make([]byte, 4, len(elements)+5)
	binary.LittleEndian.PutUint32(encoded, uint32(len(elements)+5))
	encoded = append(encoded, elements...)
	encoded = append(encoded, 0)
	document, err := sink.NewDocument(sink.ContentTypeBSON, encoded)
	if err != nil {
		t.Fatalf("sink.NewDocument() error = %v", err)
	}
	return document
}

func appendBSONString(destination []byte, key string, value string) []byte {
	destination = append(destination, 0x02)
	destination = append(destination, key...)
	destination = append(destination, 0)
	length := make([]byte, 4)
	binary.LittleEndian.PutUint32(length, uint32(len(value)+1))
	destination = append(destination, length...)
	destination = append(destination, value...)
	destination = append(destination, 0)
	return destination
}

func bsonContainsString(document []byte, key string, value string) bool {
	pattern := make([]byte, 0, len(key)+len(value)+7)
	pattern = appendBSONString(pattern, key, value)
	return bytes.Contains(document, pattern)
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
		results, err := opts.Client.Read(ctx, opts.Address)
		if err == nil && len(results) == 1 && results[0].Status == sink.ReadFound {
			document := results[0].Document.Bytes()
			if bsonContainsString(document, "name", opts.Name) &&
				bsonContainsString(document, "stage", opts.Stage) {
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
	document := results[0].Document.Bytes()
	if !bsonContainsString(document, "name", wantName) ||
		!bsonContainsString(document, "stage", wantStage) {
		t.Fatalf("Read() document does not contain name %q and stage %q", wantName, wantStage)
	}
}
