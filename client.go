package sink

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"time"

	sinkv1 "github.com/liran/sink-go/api/sink/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

const (
	defaultMaxOperations = 1000
	defaultReadAttempts  = 3
	defaultReadBackoff   = 100 * time.Millisecond
	defaultMaxBackoff    = time.Second
	defaultMultiplier    = 2
)

// RetryPolicy controls retries for transport-level Unavailable errors from
// Read. Mutating RPCs are never retried automatically.
type RetryPolicy struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Multiplier     float64
}

// ClientOptions controls request limits and safe read retries.
type ClientOptions struct {
	MaxOperations int
	ReadRetry     RetryPolicy
}

// DialOptions controls connection creation. TLS with a minimum version of 1.2
// is used when TransportCredentials is nil. Use insecure.NewCredentials only
// for a trusted plaintext development endpoint.
type DialOptions struct {
	Client               ClientOptions
	TransportCredentials credentials.TransportCredentials
	GRPCOptions          []grpc.DialOption
}

type clientConfig struct {
	maxOperations int
	readRetry     RetryPolicy
}

// Client is safe for concurrent use.
type Client struct {
	rpc        sinkv1.SinkClient
	health     healthv1.HealthClient
	connection *grpc.ClientConn
	config     clientConfig
}

// Dial creates a lazily connected gRPC client. Call CheckHealth when startup
// must prove the endpoint is reachable before processing work.
func Dial(target string, opts DialOptions) (*Client, error) {
	if strings.TrimSpace(target) == "" {
		return nil, errors.New("create Sink client: target is required")
	}
	transportCredentials := opts.TransportCredentials
	if transportCredentials == nil {
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
		transportCredentials = credentials.NewTLS(tlsConfig)
	}
	grpcOptions := append([]grpc.DialOption(nil), opts.GRPCOptions...)
	transportOption := grpc.WithTransportCredentials(transportCredentials)
	grpcOptions = append(grpcOptions, transportOption)
	connection, err := grpc.NewClient(target, grpcOptions...)
	if err != nil {
		return nil, fmt.Errorf("create Sink connection: %w", err)
	}
	client, err := New(connection, opts.Client)
	if err != nil {
		_ = connection.Close()
		return nil, err
	}
	client.connection = connection
	return client, nil
}

// New wraps an existing connection without taking ownership of it.
func New(connection grpc.ClientConnInterface, opts ClientOptions) (*Client, error) {
	if connection == nil {
		return nil, errors.New("create Sink client: connection is required")
	}
	config, err := newClientConfig(opts)
	if err != nil {
		return nil, err
	}
	client := &Client{
		rpc:    sinkv1.NewSinkClient(connection),
		health: healthv1.NewHealthClient(connection),
		config: config,
	}
	return client, nil
}

func newClientConfig(opts ClientOptions) (clientConfig, error) {
	var config clientConfig
	if opts.MaxOperations < 0 {
		return config, errors.New("create Sink client: max operations cannot be negative")
	}
	maxOperations := opts.MaxOperations
	if maxOperations == 0 {
		maxOperations = defaultMaxOperations
	}
	retry, err := normalizeRetryPolicy(opts.ReadRetry)
	if err != nil {
		return config, err
	}
	config = clientConfig{maxOperations: maxOperations, readRetry: retry}
	return config, nil
}

func normalizeRetryPolicy(policy RetryPolicy) (RetryPolicy, error) {
	var empty RetryPolicy
	if policy.MaxAttempts < 0 {
		return empty, errors.New("create Sink client: read retry max attempts cannot be negative")
	}
	if policy.InitialBackoff < 0 || policy.MaxBackoff < 0 {
		return empty, errors.New("create Sink client: read retry backoff cannot be negative")
	}
	if policy.Multiplier < 0 {
		return empty, errors.New("create Sink client: read retry multiplier cannot be negative")
	}
	if policy.MaxAttempts == 0 {
		policy.MaxAttempts = defaultReadAttempts
	}
	if policy.InitialBackoff == 0 {
		policy.InitialBackoff = defaultReadBackoff
	}
	if policy.MaxBackoff == 0 {
		policy.MaxBackoff = defaultMaxBackoff
	}
	if policy.Multiplier == 0 {
		policy.Multiplier = defaultMultiplier
	}
	if policy.MaxBackoff < policy.InitialBackoff {
		return empty, errors.New("create Sink client: read retry max backoff is less than initial backoff")
	}
	if policy.Multiplier < 1 {
		return empty, errors.New("create Sink client: read retry multiplier must be at least 1")
	}
	return policy, nil
}

// Close closes connections created by Dial. It is a no-op for clients created
// with New because the caller owns that connection.
func (c *Client) Close() error {
	if c == nil || c.connection == nil {
		return nil
	}
	return c.connection.Close()
}

// Raw returns the generated gRPC client for advanced or forward-compatible
// usage.
func (c *Client) Raw() sinkv1.SinkClient {
	if c == nil {
		return nil
	}
	return c.rpc
}

// CheckHealth verifies that the server's standard gRPC health service reports
// SERVING.
func (c *Client) CheckHealth(ctx context.Context) error {
	if c == nil || c.health == nil {
		return errors.New("check Sink health: client is nil")
	}
	request := &healthv1.HealthCheckRequest{}
	response, err := c.health.Check(ctx, request)
	if err != nil {
		return fmt.Errorf("check Sink health: %w", err)
	}
	if response == nil {
		return errors.New("check Sink health: server returned an empty response")
	}
	if response.GetStatus() != healthv1.HealthCheckResponse_SERVING {
		return fmt.Errorf("check Sink health: server status is %s", response.GetStatus())
	}
	return nil
}

// Read returns results in request order. Only transport-level Unavailable
// errors are retried, because reads are idempotent.
func (c *Client) Read(ctx context.Context, addresses ...Address) ([]ReadResult, error) {
	if err := c.validateBatch("read", len(addresses)); err != nil {
		return nil, err
	}
	operations := make([]*sinkv1.ReadOperation, len(addresses))
	for index, address := range addresses {
		if err := address.validate(); err != nil {
			return nil, fmt.Errorf("read operation %d: %w", index, err)
		}
		protoAddress := address.toProto()
		operation := &sinkv1.ReadOperation{Address: protoAddress}
		operations[index] = operation
	}
	request := &sinkv1.ReadRequest{Operations: operations}
	response, err := c.readWithRetry(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("read records: %w", err)
	}
	return decodeReadResponse(response, len(addresses))
}

// Write submits a mixed batch of put and merge operations. It deliberately
// does not retry transport failures because the server may already have
// applied or durably accepted the mutation.
func (c *Client) Write(
	ctx context.Context,
	completionMode CompletionMode,
	operations ...WriteOperation,
) ([]WriteResult, error) {
	if err := c.validateBatch("write", len(operations)); err != nil {
		return nil, err
	}
	if !validCompletionMode(completionMode) {
		return nil, errors.New("write request has an invalid completion mode")
	}
	protoOperations := make([]*sinkv1.WriteOperation, len(operations))
	for index, operation := range operations {
		if err := operation.validate(); err != nil {
			return nil, fmt.Errorf("write operation %d: %w", index, err)
		}
		protoOperations[index] = operation.toProto()
	}
	request := &sinkv1.WriteRequest{
		CompletionMode: completionMode,
		Operations:     protoOperations,
	}
	response, err := c.rpc.Write(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("write records: %w", err)
	}
	return decodeWriteResponse(response, len(operations))
}

// Delete permanently deletes records. Deleting an absent record is successful.
// Transport failures are not retried automatically.
func (c *Client) Delete(
	ctx context.Context,
	completionMode CompletionMode,
	addresses ...Address,
) ([]DeleteResult, error) {
	if err := c.validateBatch("delete", len(addresses)); err != nil {
		return nil, err
	}
	if !validCompletionMode(completionMode) {
		return nil, errors.New("delete request has an invalid completion mode")
	}
	operations := make([]*sinkv1.DeleteOperation, len(addresses))
	for index, address := range addresses {
		if err := address.validate(); err != nil {
			return nil, fmt.Errorf("delete operation %d: %w", index, err)
		}
		protoAddress := address.toProto()
		operation := &sinkv1.DeleteOperation{Address: protoAddress}
		operations[index] = operation
	}
	request := &sinkv1.DeleteRequest{
		CompletionMode: completionMode,
		Operations:     operations,
	}
	response, err := c.rpc.Delete(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("delete records: %w", err)
	}
	return decodeDeleteResponse(response, len(addresses))
}

func (c *Client) validateBatch(method string, count int) error {
	if c == nil || c.rpc == nil {
		return fmt.Errorf("%s records: client is nil", method)
	}
	if count == 0 {
		return fmt.Errorf("%s request must contain operations", method)
	}
	if count > c.config.maxOperations {
		return fmt.Errorf(
			"%s request contains %d operations; client maximum is %d",
			method,
			count,
			c.config.maxOperations,
		)
	}
	return nil
}

func validCompletionMode(mode CompletionMode) bool {
	return mode == CompletionWaitUntilApplied || mode == CompletionReturnAfterAccepted
}

func (c *Client) readWithRetry(
	ctx context.Context,
	request *sinkv1.ReadRequest,
) (*sinkv1.ReadResponse, error) {
	backoff := c.config.readRetry.InitialBackoff
	var lastErr error
	for attempt := 1; attempt <= c.config.readRetry.MaxAttempts; attempt++ {
		response, err := c.rpc.Read(ctx, request)
		if err == nil {
			return response, nil
		}
		lastErr = err
		if attempt == c.config.readRetry.MaxAttempts || status.Code(err) != codes.Unavailable {
			return nil, err
		}
		if err := waitForBackoff(ctx, backoff); err != nil {
			return nil, err
		}
		backoff = nextBackoff(backoff, c.config.readRetry)
	}
	return nil, lastErr
}

func waitForBackoff(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	select {
	case <-ctx.Done():
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func nextBackoff(current time.Duration, policy RetryPolicy) time.Duration {
	if current >= policy.MaxBackoff {
		return policy.MaxBackoff
	}
	maximumMultiplier := float64(policy.MaxBackoff) / float64(current)
	if policy.Multiplier >= maximumMultiplier {
		return policy.MaxBackoff
	}
	next := time.Duration(float64(current) * policy.Multiplier)
	return next
}
