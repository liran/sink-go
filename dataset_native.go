package sink

import (
	"context"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"net/url"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// NewBSONCommand binds a collection command to this BSON Dataset. Arguments
// contain only the remaining command fields, e.g. filter, pipeline or indexes.
// Use bson.D to preserve argument order; native BSON types are retained.
func (d *Dataset) NewBSONCommand(operation string, arguments any) (Command, error) {
	var empty Command
	if err := d.validate("BSON command"); err != nil {
		return empty, err
	}
	if d.encoding != DocumentEncodingBSON || strings.TrimSpace(operation) == "" {
		return empty, errors.New("dataset BSON command requires BSON encoding and an operation")
	}
	command := bson.D{{Key: operation, Value: d.dataset}}
	if arguments != nil {
		payload, err := bson.Marshal(arguments)
		if err != nil {
			return empty, fmt.Errorf("encode dataset command arguments: %w", err)
		}
		var fields bson.D
		if err := bson.Unmarshal(payload, &fields); err != nil {
			return empty, fmt.Errorf("decode dataset command arguments: %w", err)
		}
		seen := map[string]bool{operation: true}
		for _, field := range fields {
			if seen[field.Key] {
				return empty, fmt.Errorf("duplicate dataset command field %q", field.Key)
			}
			seen[field.Key] = true
		}
		command = append(command, fields...)
	}
	return NewBSONCommand(d.store, d.namespace, command)
}

// Execute binds a native command to this Dataset. For BSON the first field must
// target this collection (an empty string is filled in). For JSON, Path is
// relative to the index, e.g. /_mapping; empty selects the index itself.
// Use Client.Execute for database, cluster or multi-index operations.
func (d *Dataset) Execute(ctx context.Context, req ExecuteRequest) (ExecuteResponse, error) {
	command, err := d.bindNativeCommand(req.Command, false)
	if err != nil {
		var empty ExecuteResponse
		return empty, err
	}
	req.Command = command
	return d.client.Execute(ctx, req)
}

// Query fetches a page from this Dataset. An empty Command selects all records.
// BSON uses find by default; JSON uses POST /<index>/_search. Query controls
// and native payload semantics are the same as Client.Query.
func (d *Dataset) Query(ctx context.Context, req QueryRequest) (QueryResponse, error) {
	command, err := d.bindNativeCommand(req.Command, true)
	if err != nil {
		var empty QueryResponse
		return empty, err
	}
	req.Command = command
	return d.client.Query(ctx, req)
}

// Count counts matches in this Dataset. An empty Command counts all records;
// ordinary unfiltered MongoDB counts automatically use collection metadata.
func (d *Dataset) Count(ctx context.Context, req CountRequest) (CountResponse, error) {
	command, err := d.bindNativeCommand(req.Command, true)
	if err != nil {
		var empty CountResponse
		return empty, err
	}
	req.Command = command
	return d.client.Count(ctx, req)
}

// Scan returns one live page scoped to this Dataset. Reuse the request with
// NextCursor to continue. JSON commands must provide a unique stable sort.
func (d *Dataset) Scan(ctx context.Context, req ScanRequest) (ScanResponse, error) {
	command, err := d.bindNativeCommand(req.Command, true)
	if err != nil {
		var empty ScanResponse
		return empty, err
	}
	req.Command = command
	return d.client.Scan(ctx, req)
}

func (d *Dataset) bindNativeCommand(command Command, query bool) (Command, error) {
	var empty Command
	if err := d.validate("native command"); err != nil {
		return empty, err
	}
	if command.Store != "" && command.Store != d.store {
		return empty, errors.New("native command store differs from Dataset")
	}
	if command.Namespace != "" && command.Namespace != d.namespace {
		return empty, errors.New("native command namespace differs from Dataset")
	}
	command.Store = d.store
	if d.encoding == DocumentEncodingBSON {
		command.Namespace = d.namespace
		if command.ContentType == "" {
			command.ContentType = "application/bson"
		}
		mediaType, _, err := mime.ParseMediaType(command.ContentType)
		if err != nil || mediaType != "application/bson" {
			return empty, errors.New("BSON Dataset requires application/bson commands")
		}
		if len(command.Payload) == 0 && query {
			find := bson.D{{Key: "find", Value: d.dataset}}
			command.Payload, err = bson.Marshal(find)
			if err != nil {
				return empty, err
			}
		}
		var document bson.D
		if err := bson.Unmarshal(command.Payload, &document); err != nil {
			return empty, fmt.Errorf("decode dataset command: %w", err)
		}
		if len(document) == 0 {
			return empty, errors.New("dataset command is empty")
		}
		collection, ok := document[0].Value.(string)
		if !ok || (collection != "" && collection != d.dataset) {
			return empty, errors.New("native command must target the Dataset collection; use Client for other scopes")
		}
		if collection == "" {
			document[0].Value = d.dataset
			command.Payload, err = bson.Marshal(document)
			if err != nil {
				return empty, err
			}
		}
		return command, nil
	}
	// Search record routing uses Dataset as the index; Namespace is logical only.
	command.Namespace = ""
	if command.ContentType == "" && len(command.Payload) > 0 {
		command.ContentType = "application/json"
	}
	if query {
		if command.Path == "" {
			command.Path = "/_search"
		}
		if command.Method == "" {
			command.Method = http.MethodPost
		}
	}
	if command.Path != "" {
		parsed, err := url.Parse(command.Path)
		if err != nil || !strings.HasPrefix(command.Path, "/") || strings.HasPrefix(command.Path, "//") || parsed.IsAbs() || parsed.Host != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
			return empty, errors.New("dataset command path must be relative to its index, with query in Command.Query")
		}
		for _, segment := range strings.Split(parsed.Path, "/") {
			if segment == "." || segment == ".." {
				return empty, errors.New("dataset command path cannot contain dot segments")
			}
		}
	}
	command.Path = "/" + url.PathEscape(d.dataset) + command.Path
	return command, nil
}
