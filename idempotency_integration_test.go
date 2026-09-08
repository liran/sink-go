//go:build integration

package sink_test

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"

	sink "github.com/liran/sink-go"
	"google.golang.org/grpc/credentials/insecure"
)

func TestSinkIdempotencyCompatibility(t *testing.T) {
	target := os.Getenv("SINK_INTEGRATION_ADDRESS")
	if target == "" {
		t.Skip("SINK_INTEGRATION_ADDRESS is not set")
	}
	clientOptions := sink.ClientOptions{IdempotentWrites: true}
	dialOptions := sink.DialOptions{Client: clientOptions, TransportCredentials: insecure.NewCredentials()}
	client, err := sink.Dial(target, dialOptions)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if err := waitForHealth(t.Context(), client); err != nil {
		t.Fatal(err)
	}
	address := integrationAddress(t, fmt.Sprintf("protected-%d", time.Now().UnixNano()))
	document := integrationDocument(t, "protected", "initial")
	operation, err := sink.NewPut(address, document, sink.WriteCreate)
	if err != nil {
		t.Fatal(err)
	}
	id, err := sink.NewOperationID()
	if err != nil {
		t.Fatal(err)
	}
	operation = operation.WithOperationID(id).WithReturnedDocument()
	first, err := client.Write(t.Context(), sink.CompletionWaitUntilApplied, operation)
	if err != nil {
		t.Fatal(err)
	}
	assertWriteStatus(t, first, sink.WriteApplied)
	// Reconnect to discard any client process-local state.
	second, err := sink.Dial(target, dialOptions)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })
	replayed, err := second.Write(t.Context(), sink.CompletionWaitUntilApplied, operation)
	if err != nil {
		t.Fatal(err)
	}
	assertWriteStatus(t, replayed, sink.WriteApplied)
	if replayed[0].OperationID != id || !bytes.Equal(first[0].Revision.Bytes(), replayed[0].Revision.Bytes()) || !bytes.Equal(first[0].Document.Payload(), replayed[0].Document.Payload()) {
		t.Fatal("SDK lost original receipt")
	}
}
