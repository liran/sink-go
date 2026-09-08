package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"reflect"
	"sort"
	"strings"

	sinkv1 "github.com/liran/sink-go/api/sink/v1"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// MongoCommand carries an ordered BSON command. Use NewMongoCommand to encode
// a struct, bson.D, or bson.Raw without requiring a MongoDB connection.
type MongoCommand struct {
	Database string
	Command  []byte
}

func NewMongoCommand(database string, value any) (MongoCommand, error) {
	var empty MongoCommand
	if strings.TrimSpace(database) == "" || value == nil {
		return empty, errors.New("MongoDB command requires a database and ordered value")
	}
	valueType := reflect.TypeOf(value)
	for valueType.Kind() == reflect.Pointer {
		valueType = valueType.Elem()
	}
	if valueType.Kind() == reflect.Map {
		return empty, errors.New("MongoDB commands must preserve field order; use a struct, bson.D, or bson.Raw")
	}
	payload, err := bson.Marshal(value)
	if err != nil {
		return empty, fmt.Errorf("encode MongoDB command: %w", err)
	}
	command := MongoCommand{Database: database, Command: payload}
	return command, nil
}

// SearchCommand describes a request within a configured search endpoint. Sink
// owns authentication and transport headers; other valid headers are forwarded.
type SearchCommand struct {
	Method  string
	Path    string
	Query   string
	Headers http.Header
	Body    []byte
}

type ExecuteRequest struct {
	Store   string
	MongoDB *MongoCommand
	Search  *SearchCommand
}

type ExecuteResponse struct {
	ContentType string
	Payload     []byte
	Success     bool
	StatusCode  int
	Headers     http.Header
}

// NativeError retains the database's complete response, including errors not
// represented by the record API's storage-independent failure codes.
type NativeError struct {
	Response ExecuteResponse
}

func (e *NativeError) Error() string {
	if e.Response.StatusCode != 0 {
		return fmt.Sprintf("native database command returned HTTP %d", e.Response.StatusCode)
	}
	return "native database command failed; inspect its BSON response"
}

// Decode interprets a native JSON or BSON response. Payload remains available
// for responses in another content type and for byte-preserving forwarding.
func (r ExecuteResponse) Decode(destination any) error {
	contentType, _, err := mime.ParseMediaType(r.ContentType)
	if err != nil {
		return fmt.Errorf("decode native response content type: %w", err)
	}
	if contentType == "application/bson" {
		return bson.Unmarshal(r.Payload, destination)
	}
	if contentType == "application/json" || strings.HasSuffix(contentType, "+json") {
		return json.Unmarshal(r.Payload, destination)
	}
	return fmt.Errorf("native content type %q requires decoding Payload directly", r.ContentType)
}

func (r ExecuteRequest) toProto() (*sinkv1.ExecuteRequest, error) {
	if strings.TrimSpace(r.Store) == "" || (r.MongoDB == nil) == (r.Search == nil) {
		return nil, errors.New("native request requires a store and exactly one command")
	}
	request := &sinkv1.ExecuteRequest{Store: r.Store}
	if r.MongoDB != nil {
		if strings.TrimSpace(r.MongoDB.Database) == "" {
			return nil, errors.New("MongoDB database is required")
		}
		if err := bson.Raw(r.MongoDB.Command).Validate(); err != nil {
			return nil, fmt.Errorf("invalid BSON command: %w", err)
		}
		command := &sinkv1.MongoCommand{Database: r.MongoDB.Database, Command: bytes.Clone(r.MongoDB.Command)}
		wrapper := &sinkv1.ExecuteRequest_Mongodb{Mongodb: command}
		request.Command = wrapper
		return request, nil
	}
	command := &sinkv1.SearchCommand{Method: r.Search.Method, Path: r.Search.Path,
		Query: r.Search.Query, Body: bytes.Clone(r.Search.Body)}
	names := make([]string, 0, len(r.Search.Headers))
	for name := range r.Search.Headers {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		header := &sinkv1.Header{Name: name, Values: append([]string(nil), r.Search.Headers[name]...)}
		command.Headers = append(command.Headers, header)
	}
	wrapper := &sinkv1.ExecuteRequest_Search{Search: command}
	request.Command = wrapper
	return request, nil
}

// Execute makes one native command request. The SDK never retries it.
// MongoDB cursor and session commands are rejected; use Scan for cursor queries.
// Native writes follow database semantics independently of Sink's record API.
// A database error returns both its response and a *NativeError. Transport
// failures return no database response and must not be assumed unapplied.
func (c *Client) Execute(ctx context.Context, req ExecuteRequest) (ExecuteResponse, error) {
	var empty ExecuteResponse
	if c == nil || c.rpc == nil {
		return empty, errors.New("execute native command: client is required")
	}
	request, err := req.toProto()
	if err != nil {
		return empty, err
	}
	response, err := c.rpc.Execute(ctx, request, c.config.sinkCallOptions...)
	if err != nil {
		return empty, fmt.Errorf("execute native command: %w", err)
	}
	if response == nil {
		return empty, protocolError("Execute", "response is empty")
	}
	headers := make(http.Header)
	for _, header := range response.GetHeaders() {
		for _, value := range header.GetValues() {
			headers.Add(header.GetName(), value)
		}
	}
	result := ExecuteResponse{ContentType: response.GetContentType(), Payload: bytes.Clone(response.GetPayload()),
		Success: response.GetSuccess(), StatusCode: int(response.GetStatusCode()), Headers: headers}
	if !result.Success {
		failure := &NativeError{Response: result}
		return result, failure
	}
	return result, nil
}

type ScanRequest struct {
	Request   ExecuteRequest
	BatchSize int
}

// Scan visits MongoDB documents or complete search hits (including _source,
// _id, and sort values). It holds at most one received page, cancels the stream
// when visit fails, and never replays a partial stream. Callbacks must observe
// ctx when doing their own blocking work.
func (c *Client) Scan(ctx context.Context, req ScanRequest, visit func(Document) error) error {
	if c == nil || c.rpc == nil || visit == nil {
		return errors.New("scan requires a client and document callback")
	}
	if req.BatchSize < 0 || req.BatchSize > 1000 {
		return errors.New("scan batch size must be between 0 and 1000")
	}
	command, err := req.Request.toProto()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	request := &sinkv1.ScanRequest{Request: command, BatchSize: uint32(req.BatchSize)}
	stream, err := c.rpc.Scan(ctx, request, c.config.sinkCallOptions...)
	if err != nil {
		return fmt.Errorf("open scan: %w", err)
	}
	for {
		page, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("receive scan page (previous callbacks may have completed): %w", err)
		}
		if page == nil {
			return protocolError("Scan", "page is empty")
		}
		for _, raw := range page.GetDocuments() {
			if err := ctx.Err(); err != nil {
				return err
			}
			document, err := documentFromProto(raw)
			if err != nil {
				return protocolError("Scan", err.Error())
			}
			if err := visit(document); err != nil {
				return err
			}
		}
	}
}
