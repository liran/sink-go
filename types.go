package sink

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"

	sinkv1 "github.com/liran/sink-go/api/sink/v1"
)

type CompletionMode = sinkv1.CompletionMode

const (
	CompletionWaitUntilApplied    CompletionMode = sinkv1.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED
	CompletionReturnAfterAccepted CompletionMode = sinkv1.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED
	CompletionWaitUntilVisible    CompletionMode = sinkv1.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE
)

type WriteMode = sinkv1.WriteMode

const (
	WriteCreate  WriteMode = sinkv1.WriteMode_WRITE_MODE_CREATE
	WriteReplace WriteMode = sinkv1.WriteMode_WRITE_MODE_REPLACE
	WriteUpsert  WriteMode = sinkv1.WriteMode_WRITE_MODE_UPSERT
)

type MissingDocumentMode = sinkv1.MissingDocumentMode

const (
	MissingDocumentFail   MissingDocumentMode = sinkv1.MissingDocumentMode_MISSING_DOCUMENT_MODE_FAIL
	MissingDocumentCreate MissingDocumentMode = sinkv1.MissingDocumentMode_MISSING_DOCUMENT_MODE_CREATE
)

type ReadStatus = sinkv1.ReadStatus

const (
	ReadFound    ReadStatus = sinkv1.ReadStatus_READ_STATUS_FOUND
	ReadNotFound ReadStatus = sinkv1.ReadStatus_READ_STATUS_NOT_FOUND
	ReadFailed   ReadStatus = sinkv1.ReadStatus_READ_STATUS_FAILED
)

type WriteStatus = sinkv1.WriteStatus

const (
	WriteAccepted           WriteStatus = sinkv1.WriteStatus_WRITE_STATUS_ACCEPTED
	WriteApplied            WriteStatus = sinkv1.WriteStatus_WRITE_STATUS_APPLIED
	WritePreconditionFailed WriteStatus = sinkv1.WriteStatus_WRITE_STATUS_PRECONDITION_FAILED
	WriteFailed             WriteStatus = sinkv1.WriteStatus_WRITE_STATUS_FAILED
)

type DeleteStatus = sinkv1.DeleteStatus

const (
	DeleteAccepted DeleteStatus = sinkv1.DeleteStatus_DELETE_STATUS_ACCEPTED
	DeleteApplied  DeleteStatus = sinkv1.DeleteStatus_DELETE_STATUS_APPLIED
	DeleteFailed   DeleteStatus = sinkv1.DeleteStatus_DELETE_STATUS_FAILED
)

type FailureCode = sinkv1.FailureCode

const (
	FailureInvalidArgument    FailureCode = sinkv1.FailureCode_FAILURE_CODE_INVALID_ARGUMENT
	FailurePreconditionFailed FailureCode = sinkv1.FailureCode_FAILURE_CODE_PRECONDITION_FAILED
	FailureNotFound           FailureCode = sinkv1.FailureCode_FAILURE_CODE_NOT_FOUND
	FailureConflict           FailureCode = sinkv1.FailureCode_FAILURE_CODE_CONFLICT
	FailureResourceExhausted  FailureCode = sinkv1.FailureCode_FAILURE_CODE_RESOURCE_EXHAUSTED
	FailureUnavailable        FailureCode = sinkv1.FailureCode_FAILURE_CODE_UNAVAILABLE
	FailureDeadlineExceeded   FailureCode = sinkv1.FailureCode_FAILURE_CODE_DEADLINE_EXCEEDED
	FailureInternal           FailureCode = sinkv1.FailureCode_FAILURE_CODE_INTERNAL
)

type keyKind uint8

const (
	keyKindUnspecified keyKind = iota
	keyKindString
	keyKindInt64
	keyKindBytes
	keyKindOpaque
)

// Key is an immutable logical record key. Use one of the key constructors.
type Key struct {
	kind        keyKind
	stringValue string
	int64Value  int64
	bytesValue  []byte
	opaqueType  string
}

func StringKey(value string) Key {
	key := Key{kind: keyKindString, stringValue: value}
	return key
}

func Int64Key(value int64) Key {
	key := Key{kind: keyKindInt64, int64Value: value}
	return key
}

func BytesKey(value []byte) Key {
	key := Key{kind: keyKindBytes, bytesValue: bytes.Clone(value)}
	return key
}

func OpaqueKey(typeName string, value []byte) (Key, error) {
	var empty Key
	if typeName == "" {
		return empty, errors.New("opaque key type is required")
	}
	key := Key{
		kind:       keyKindOpaque,
		bytesValue: bytes.Clone(value),
		opaqueType: typeName,
	}
	return key, nil
}

func (k Key) validate() error {
	if k.kind == keyKindUnspecified {
		return errors.New("record key is required")
	}
	if k.kind == keyKindOpaque && k.opaqueType == "" {
		return errors.New("opaque key type is required")
	}
	return nil
}

// Address identifies a record through logical names rather than physical
// database terminology.
type Address struct {
	store     string
	namespace string
	dataset   string
	key       Key
}

func NewAddress(store string, namespace string, dataset string, key Key) (Address, error) {
	var address Address
	if store == "" {
		return address, errors.New("record address store is required")
	}
	if namespace == "" {
		return address, errors.New("record address namespace is required")
	}
	if dataset == "" {
		return address, errors.New("record address dataset is required")
	}
	if err := key.validate(); err != nil {
		return address, err
	}
	address = Address{
		store:     store,
		namespace: namespace,
		dataset:   dataset,
		key:       key,
	}
	return address, nil
}

func (a Address) Store() string {
	return a.store
}

func (a Address) Namespace() string {
	return a.namespace
}

func (a Address) Dataset() string {
	return a.dataset
}

func (a Address) validate() error {
	if a.store == "" {
		return errors.New("record address store is required")
	}
	if a.namespace == "" {
		return errors.New("record address namespace is required")
	}
	if a.dataset == "" {
		return errors.New("record address dataset is required")
	}
	return a.key.validate()
}

// Document contains an immutable JSON object returned by Sink.
type Document struct {
	json []byte
}

func documentFromValue(value any) (Document, error) {
	var document Document
	switch typed := value.(type) {
	case Document:
		return documentFromJSON(typed.json)
	case *Document:
		if typed == nil {
			return document, errors.New("document is required")
		}
		return documentFromJSON(typed.json)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return document, fmt.Errorf("encode JSON document: %w", err)
	}
	return documentFromJSON(encoded)
}

func documentFromJSON(encoded []byte) (Document, error) {
	var document Document
	trimmed := bytes.TrimSpace(encoded)
	if err := validateJSONObjectJSON(trimmed); err != nil {
		return document, err
	}
	document = Document{json: bytes.Clone(trimmed)}
	return document, nil
}

func validateJSONObjectJSON(encoded []byte) error {
	if len(encoded) < 2 || encoded[0] != '{' || encoded[len(encoded)-1] != '}' || !json.Valid(encoded) {
		return errors.New("document must be a valid JSON object")
	}
	return nil
}

func (d Document) JSON() []byte {
	return bytes.Clone(d.json)
}

func (d Document) Decode(destination any) error {
	if destination == nil {
		return errors.New("decode JSON document: destination is required")
	}
	if err := json.Unmarshal(d.json, destination); err != nil {
		return fmt.Errorf("decode JSON document: %w", err)
	}
	return nil
}

func (d Document) validate() error {
	return validateJSONObjectJSON(d.json)
}

// LuaProgram is an immutable, self-contained merge rule. Sink verifies the
// digest and caches the compiled program while executing each merge in a fresh
// VM.
type LuaProgram struct {
	source []byte
	sha256 [sha256.Size]byte
}

func NewLuaProgram(source []byte) (LuaProgram, error) {
	var program LuaProgram
	if len(source) == 0 {
		return program, errors.New("lua merge program source is required")
	}
	program.source = bytes.Clone(source)
	program.sha256 = sha256.Sum256(source)
	return program, nil
}

func (p LuaProgram) Source() []byte {
	return bytes.Clone(p.source)
}

func (p LuaProgram) SHA256() []byte {
	digest := make([]byte, sha256.Size)
	copy(digest, p.sha256[:])
	return digest
}

func (p LuaProgram) validate() error {
	if len(p.source) == 0 {
		return errors.New("lua merge program source is required")
	}
	if sha256.Sum256(p.source) != p.sha256 {
		return errors.New("lua merge program source was modified")
	}
	return nil
}

type writeAction uint8

const (
	writeActionUnspecified writeAction = iota
	writeActionPut
	writeActionMerge
)

// MergeOptions describes a Lua-driven read-modify-write operation.
type MergeOptions struct {
	Incoming            any
	Program             LuaProgram
	MissingDocumentMode MissingDocumentMode
}

type mergeOperation struct {
	incoming            Document
	program             LuaProgram
	missingDocumentMode MissingDocumentMode
}

// WriteOperation is either a put or merge operation. Use NewPut or NewMerge.
type WriteOperation struct {
	address Address
	action  writeAction
	put     Document
	mode    WriteMode
	merge   mergeOperation
}

func NewPut(address Address, value any, mode WriteMode) (WriteOperation, error) {
	var operation WriteOperation
	if err := address.validate(); err != nil {
		return operation, err
	}
	document, err := documentFromValue(value)
	if err != nil {
		return operation, err
	}
	if mode != WriteCreate && mode != WriteReplace && mode != WriteUpsert {
		return operation, errors.New("put operation has an invalid write mode")
	}
	operation = WriteOperation{
		address: address,
		action:  writeActionPut,
		put:     document,
		mode:    mode,
	}
	return operation, nil
}

func NewMerge(address Address, opts MergeOptions) (WriteOperation, error) {
	var operation WriteOperation
	if err := address.validate(); err != nil {
		return operation, err
	}
	incoming, err := documentFromValue(opts.Incoming)
	if err != nil {
		return operation, err
	}
	if err := opts.Program.validate(); err != nil {
		return operation, err
	}
	if opts.MissingDocumentMode != MissingDocumentFail && opts.MissingDocumentMode != MissingDocumentCreate {
		return operation, errors.New("merge operation has an invalid missing document mode")
	}
	operation = WriteOperation{
		address: address,
		action:  writeActionMerge,
		merge: mergeOperation{
			incoming:            incoming,
			program:             opts.Program,
			missingDocumentMode: opts.MissingDocumentMode,
		},
	}
	return operation, nil
}

func (o WriteOperation) validate() error {
	if err := o.address.validate(); err != nil {
		return err
	}
	switch o.action {
	case writeActionPut:
		if err := o.put.validate(); err != nil {
			return err
		}
		if o.mode != WriteCreate && o.mode != WriteReplace && o.mode != WriteUpsert {
			return errors.New("put operation has an invalid write mode")
		}
	case writeActionMerge:
		if err := o.merge.incoming.validate(); err != nil {
			return err
		}
		if err := o.merge.program.validate(); err != nil {
			return err
		}
		if o.merge.missingDocumentMode != MissingDocumentFail && o.merge.missingDocumentMode != MissingDocumentCreate {
			return errors.New("merge operation has an invalid missing document mode")
		}
	default:
		return errors.New("write action is required")
	}
	return nil
}

// RevisionToken is an opaque storage revision returned by Sink.
type RevisionToken struct {
	data []byte
}

func (r RevisionToken) Bytes() []byte {
	return bytes.Clone(r.data)
}

type ReadResult struct {
	OperationIndex int
	Status         ReadStatus
	Document       Document
	Revision       RevisionToken
	Failure        *OperationError
}

type WriteResult struct {
	OperationIndex int
	Status         WriteStatus
	Revision       RevisionToken
	Failure        *OperationError
}

type DeleteResult struct {
	OperationIndex int
	Status         DeleteStatus
	Failure        *OperationError
}

// OperationError reports a per-record failure without discarding successful
// results from the same batch.
type OperationError struct {
	OperationIndex int
	Code           FailureCode
	Message        string
	Retryable      bool
}

func (e *OperationError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("sink operation %d failed with %s: %s", e.OperationIndex, e.Code, e.Message)
}

func (r ReadResult) Err() error {
	if r.Failure == nil {
		return nil
	}
	return r.Failure
}

func (r WriteResult) Err() error {
	if r.Failure == nil {
		return nil
	}
	return r.Failure
}

func (r DeleteResult) Err() error {
	if r.Failure == nil {
		return nil
	}
	return r.Failure
}

// BatchError collects per-operation failures while preserving errors.Is and
// errors.As traversal through every failure.
type BatchError struct {
	Failures []*OperationError
}

func (e *BatchError) Error() string {
	if e == nil || len(e.Failures) == 0 {
		return ""
	}
	return fmt.Sprintf("sink batch contains %d failed operations", len(e.Failures))
}

func (e *BatchError) Unwrap() []error {
	if e == nil {
		return nil
	}
	errorsList := make([]error, 0, len(e.Failures))
	for _, failure := range e.Failures {
		errorsList = append(errorsList, failure)
	}
	return errorsList
}

func ReadResultsError(results []ReadResult) error {
	failures := make([]*OperationError, 0)
	for _, result := range results {
		if result.Failure != nil {
			failures = append(failures, result.Failure)
		}
	}
	return newBatchError(failures)
}

func WriteResultsError(results []WriteResult) error {
	failures := make([]*OperationError, 0)
	for _, result := range results {
		if result.Failure != nil {
			failures = append(failures, result.Failure)
		}
	}
	return newBatchError(failures)
}

func DeleteResultsError(results []DeleteResult) error {
	failures := make([]*OperationError, 0)
	for _, result := range results {
		if result.Failure != nil {
			failures = append(failures, result.Failure)
		}
	}
	return newBatchError(failures)
}

func newBatchError(failures []*OperationError) error {
	if len(failures) == 0 {
		return nil
	}
	batchError := &BatchError{Failures: failures}
	return batchError
}

// ProtocolError means the server returned a structurally invalid response.
type ProtocolError struct {
	Method  string
	Message string
}

func (e *ProtocolError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("sink %s response is invalid: %s", e.Method, e.Message)
}
