//go:build integration

package sink_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	sink "github.com/liran/sink-go"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func testNativeCompatibility(t *testing.T, ctx context.Context, client *sink.Client) {
	collection := fmt.Sprintf("native_%d", time.Now().UnixNano())
	program, err := sink.NewLuaProgram([]byte(`return function(current, incoming)
    current = current or {count = 0}
    current.count = current.count + incoming.count
    current.updated_at = incoming.updated_at
    return current
end`))
	if err != nil {
		t.Fatal(err)
	}
	opts := sink.DatasetOptions{Store: "primary", Namespace: "sink_go_client", Dataset: collection,
		Encoding: sink.DocumentEncodingBSON, MergeProgram: &program}
	dataset, err := sink.NewDataset(client, opts)
	if err != nil {
		t.Fatal(err)
	}
	key := sink.StringKey("quota")
	address, err := sink.NewAddress(opts.Store, opts.Namespace, opts.Dataset, key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = client.Delete(cleanup, sink.CompletionWaitUntilApplied, address)
	})
	value := struct {
		Count     int       `bson:"count"`
		UpdatedAt time.Time `bson:"updated_at"`
	}{Count: 1, UpdatedAt: integrationDateTime()}
	record := sink.Record{Key: key, Value: value, ReturnDocument: true}
	results, err := dataset.Merge(ctx, sink.CompletionWaitUntilApplied, sink.MissingDocumentCreate, record, record)
	if err != nil || len(results) != 2 {
		t.Fatalf("returned Merge: %+v %v", results, err)
	}
	for index, result := range results {
		var stored struct {
			Count int `bson:"count"`
		}
		if err := result.Document.Decode(&stored); err != nil || stored.Count != index+1 {
			t.Fatalf("returned count=%d err=%v", stored.Count, err)
		}
	}
	indexDefinition := bson.D{{Key: "key", Value: bson.D{{Key: "count", Value: 1}}}, {Key: "name", Value: "count"}}
	indexCommand := bson.D{{Key: "createIndexes", Value: collection}, {Key: "indexes", Value: bson.A{indexDefinition}}}
	command, err := sink.NewMongoCommand(opts.Namespace, indexCommand)
	if err != nil {
		t.Fatal(err)
	}
	native := sink.ExecuteRequest{Store: opts.Store, MongoDB: &command}
	for range 2 {
		response, err := client.Execute(ctx, native)
		if err != nil || !response.Success || response.ContentType != "application/bson" {
			t.Fatalf("index setup: %+v %v", response, err)
		}
	}
	find := bson.D{{Key: "find", Value: collection}, {Key: "filter", Value: bson.D{{Key: "count", Value: 2}}}}
	command, err = sink.NewMongoCommand(opts.Namespace, find)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Execute(ctx, native); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Execute must reject cursor commands before execution: %v", err)
	}
	scan := sink.ScanRequest{Request: native, BatchSize: 1}
	seen := 0
	visit := func(document sink.Document) error {
		seen++
		var stored struct {
			Count     int       `bson:"count"`
			UpdatedAt time.Time `bson:"updated_at"`
		}
		if err := document.Decode(&stored); err != nil {
			return err
		}
		if stored.Count != 2 || !stored.UpdatedAt.Equal(integrationDateTime()) {
			return fmt.Errorf("scan lost document or BSON datetime: %+v", stored)
		}
		return nil
	}
	if err := client.Scan(ctx, scan, visit); err != nil || seen != 1 {
		t.Fatalf("scan seen=%d err=%v", seen, err)
	}
	invalid := bson.D{{Key: "count", Value: collection}, {Key: "unknownOption", Value: true}}
	command, err = sink.NewMongoCommand(opts.Namespace, invalid)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Execute(ctx, native)
	var failure *sink.NativeError
	if !errors.As(err, &failure) || response.Success || bson.Raw(failure.Response.Payload).Lookup("errmsg").Type != bson.TypeString {
		t.Fatalf("native error lost: %+v %v", response, err)
	}
	modify := bson.D{{Key: "findAndModify", Value: collection}, {Key: "query", Value: bson.D{{Key: "_id", Value: "quota"}}},
		{Key: "update", Value: bson.D{{Key: "$inc", Value: bson.D{{Key: "count", Value: 1}}}}}, {Key: "new", Value: true}}
	command, err = sink.NewMongoCommand(opts.Namespace, modify)
	if err != nil {
		t.Fatal(err)
	}
	response, err = client.Execute(ctx, native)
	if err != nil || !response.Success || bson.Raw(response.Payload).Lookup("value", "count").AsInt64() != 3 {
		t.Fatalf("native mutation response=%+v err=%v", response, err)
	}
}
