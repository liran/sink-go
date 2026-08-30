package sink

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"errors"
	"fmt"
	"math/rand/v2"
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
	defaultMaxOperations   = 1000
	defaultReadAttempts    = 3
	defaultReadBackoff     = 100 * time.Millisecond
	defaultMaxBackoff      = time.Second
	defaultMultiplier      = 2
	defaultRetryJitter     = 0.2
	defaultMaxMessageBytes = 64 << 20
)

// RetryPolicy controls retries for transport-level Unavailable errors and
// retryable per-operation failures from Read. Mutating RPCs are never retried
// automatically.
type RetryPolicy struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Multiplier     float64
	Jitter         float64
}

// ClientOptions controls request limits and safe read retries.
type ClientOptions struct {
	MaxOperations          int
	ReadRetry              RetryPolicy
	MaxReceiveMessageBytes int
	MaxSendMessageBytes    int
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
	maxOperations     int
	readRetry         RetryPolicy
	sinkCallOptions   []grpc.CallOption
	healthCallOptions []grpc.CallOption
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
	if opts.MaxOperations < 0 || opts.MaxReceiveMessageBytes < 0 || opts.MaxSendMessageBytes < 0 {
		return config, errors.New("create Sink client: limits cannot be negative")
	}
	maxOperations := opts.MaxOperations
	if maxOperations == 0 {
		maxOperations = defaultMaxOperations
	}
	retry, err := normalizeRetryPolicy(opts.ReadRetry)
	if err != nil {
		return config, err
	}
	maxReceiveBytes := opts.MaxReceiveMessageBytes
	if maxReceiveBytes == 0 {
		maxReceiveBytes = defaultMaxMessageBytes
	}
	maxSendBytes := opts.MaxSendMessageBytes
	if maxSendBytes == 0 {
		maxSendBytes = defaultMaxMessageBytes
	}
	healthCallOptions := []grpc.CallOption{
		grpc.MaxCallRecvMsgSize(maxReceiveBytes),
		grpc.MaxCallSendMsgSize(maxSendBytes),
	}
	sinkCallOptions := append([]grpc.CallOption(nil), healthCallOptions...)
	vtCodec := newVTProtoCodec()
	sinkCallOptions = append(sinkCallOptions, grpc.ForceCodecV2(vtCodec))
	config = clientConfig{
		maxOperations:     maxOperations,
		readRetry:         retry,
		sinkCallOptions:   sinkCallOptions,
		healthCallOptions: healthCallOptions,
	}
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
	if policy.Jitter < 0 || policy.Jitter > 1 {
		return empty, errors.New("create Sink client: read retry jitter must be between 0 and 1")
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
	if policy.Jitter == 0 {
		policy.Jitter = defaultRetryJitter
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
	response, err := c.health.Check(ctx, request, c.config.healthCallOptions...)
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

// Read returns results in request order. Transport-level Unavailable errors and
// retryable per-operation failures are retried, because reads are idempotent.
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
	results, err := c.readOperationsWithRetry(ctx, operations)
	if err != nil {
		return nil, fmt.Errorf("read records: %w", err)
	}
	return results, nil
}

// ReadAll splits a larger collection into configured operation-count batches.
// Returned operation indexes refer to the original collection.
func (c *Client) ReadAll(ctx context.Context, addresses []Address) ([]ReadResult, error) {
	if err := c.validateCollection("read", len(addresses)); err != nil {
		return nil, err
	}
	results := make([]ReadResult, 0, len(addresses))
	for start := 0; start < len(addresses); start += c.config.maxOperations {
		end := min(start+c.config.maxOperations, len(addresses))
		batch, err := c.Read(ctx, addresses[start:end]...)
		if err != nil {
			return results, err
		}
		remapReadIndexes(batch, start)
		results = append(results, batch...)
	}
	return results, nil
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
	luaPrograms := make([]*sinkv1.LuaProgram, 0)
	seenLuaPrograms := make(map[[sha256.Size]byte]struct{})
	for index, operation := range operations {
		if err := operation.validate(); err != nil {
			return nil, fmt.Errorf("write operation %d: %w", index, err)
		}
		protoOperations[index] = operation.toProto()
		if operation.action == writeActionMerge {
			digest := operation.merge.program.sha256
			if _, exists := seenLuaPrograms[digest]; !exists {
				program := &sinkv1.LuaProgram{
					Source: operation.merge.program.Source(),
					Sha256: operation.merge.program.SHA256(),
				}
				luaPrograms = append(luaPrograms, program)
				seenLuaPrograms[digest] = struct{}{}
			}
		}
	}
	request := &sinkv1.WriteRequest{
		CompletionMode: completionMode,
		Operations:     protoOperations,
		LuaPrograms:    luaPrograms,
	}
	response, err := c.rpc.Write(ctx, request, c.config.sinkCallOptions...)
	if err != nil {
		return nil, fmt.Errorf("write records: %w", err)
	}
	return decodeWriteResponse(response, len(operations))
}

// WriteAll splits a larger collection into configured operation-count batches.
// Earlier batches may have completed when a later batch returns an RPC error.
func (c *Client) WriteAll(
	ctx context.Context,
	completionMode CompletionMode,
	operations []WriteOperation,
) ([]WriteResult, error) {
	if err := c.validateCollection("write", len(operations)); err != nil {
		return nil, err
	}
	results := make([]WriteResult, 0, len(operations))
	for start := 0; start < len(operations); start += c.config.maxOperations {
		end := min(start+c.config.maxOperations, len(operations))
		batch, err := c.Write(ctx, completionMode, operations[start:end]...)
		if err != nil {
			return results, err
		}
		remapWriteIndexes(batch, start)
		results = append(results, batch...)
	}
	return results, nil
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
	response, err := c.rpc.Delete(ctx, request, c.config.sinkCallOptions...)
	if err != nil {
		return nil, fmt.Errorf("delete records: %w", err)
	}
	return decodeDeleteResponse(response, len(addresses))
}

// DeleteAll splits a larger collection into configured operation-count batches.
// Earlier batches may have completed when a later batch returns an RPC error.
func (c *Client) DeleteAll(
	ctx context.Context,
	completionMode CompletionMode,
	addresses []Address,
) ([]DeleteResult, error) {
	if err := c.validateCollection("delete", len(addresses)); err != nil {
		return nil, err
	}
	results := make([]DeleteResult, 0, len(addresses))
	for start := 0; start < len(addresses); start += c.config.maxOperations {
		end := min(start+c.config.maxOperations, len(addresses))
		batch, err := c.Delete(ctx, completionMode, addresses[start:end]...)
		if err != nil {
			return results, err
		}
		remapDeleteIndexes(batch, start)
		results = append(results, batch...)
	}
	return results, nil
}

func (c *Client) validateCollection(method string, count int) error {
	if c == nil || c.rpc == nil {
		return fmt.Errorf("%s records: client is nil", method)
	}
	if count == 0 {
		return fmt.Errorf("%s request must contain operations", method)
	}
	return nil
}

func remapReadIndexes(results []ReadResult, offset int) {
	for index := range results {
		results[index].OperationIndex += offset
		if results[index].Failure != nil {
			results[index].Failure.OperationIndex += offset
		}
	}
}

func remapWriteIndexes(results []WriteResult, offset int) {
	for index := range results {
		results[index].OperationIndex += offset
		if results[index].Failure != nil {
			results[index].Failure.OperationIndex += offset
		}
	}
}

func remapDeleteIndexes(results []DeleteResult, offset int) {
	for index := range results {
		results[index].OperationIndex += offset
		if results[index].Failure != nil {
			results[index].Failure.OperationIndex += offset
		}
	}
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
	return mode == CompletionWaitUntilApplied || mode == CompletionReturnAfterAccepted ||
		mode == CompletionWaitUntilVisible
}

type pendingRead struct {
	index     int
	operation *sinkv1.ReadOperation
}

func (c *Client) readOperationsWithRetry(
	ctx context.Context,
	operations []*sinkv1.ReadOperation,
) ([]ReadResult, error) {
	results := make([]ReadResult, len(operations))
	pending := make([]pendingRead, 0, len(operations))
	for index, operation := range operations {
		work := pendingRead{index: index, operation: operation}
		pending = append(pending, work)
	}
	backoff := c.config.readRetry.InitialBackoff
	for attempt := 1; attempt <= c.config.readRetry.MaxAttempts; attempt++ {
		requestOperations := make([]*sinkv1.ReadOperation, 0, len(pending))
		for _, work := range pending {
			requestOperations = append(requestOperations, work.operation)
		}
		request := &sinkv1.ReadRequest{Operations: requestOperations}
		response, err := c.rpc.Read(ctx, request, c.config.sinkCallOptions...)
		if err != nil {
			if attempt == c.config.readRetry.MaxAttempts || status.Code(err) != codes.Unavailable {
				return nil, err
			}
			if err := waitForBackoff(ctx, jitteredBackoff(backoff, c.config.readRetry.Jitter)); err != nil {
				return nil, err
			}
			backoff = nextBackoff(backoff, c.config.readRetry)
			continue
		}
		decoded, err := decodeReadResponse(response, len(pending))
		if err != nil {
			return nil, err
		}
		next := make([]pendingRead, 0)
		for index, result := range decoded {
			work := pending[index]
			result.OperationIndex = work.index
			if result.Failure != nil {
				result.Failure.OperationIndex = work.index
			}
			results[work.index] = result
			if attempt < c.config.readRetry.MaxAttempts && result.Failure != nil && result.Failure.Retryable {
				next = append(next, work)
			}
		}
		if len(next) == 0 {
			return results, nil
		}
		if err := waitForBackoff(ctx, jitteredBackoff(backoff, c.config.readRetry.Jitter)); err != nil {
			return nil, err
		}
		pending = next
		backoff = nextBackoff(backoff, c.config.readRetry)
	}
	return results, nil
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

func jitteredBackoff(backoff time.Duration, jitter float64) time.Duration {
	if jitter <= 0 || backoff <= 1 {
		return backoff
	}
	minimum := float64(backoff) * (1 - jitter)
	spread := float64(backoff) * jitter * 2
	return time.Duration(minimum + rand.Float64()*spread)
}
