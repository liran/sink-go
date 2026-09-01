package sink

import (
	"context"
	"errors"
	"fmt"
)

// DatasetOptions binds the stable routing, encoding, and optional merge
// program shared by records in one logical dataset. Completion mode remains a
// per-call choice because callers of the same dataset can require different
// durability and visibility guarantees.
type DatasetOptions struct {
	Store        string
	Namespace    string
	Dataset      string
	Encoding     DocumentEncoding
	MergeProgram *LuaProgram
}

// Record pairs one logical key with the Go value to encode for a Dataset
// mutation.
type Record struct {
	Key   Key
	Value any
}

// Dataset provides validated, batch-native reads and mutations for one routing
// and encoding scope. Its methods return decoded results and a BatchError when
// any individual operation fails.
type Dataset struct {
	client          *Client
	store           string
	namespace       string
	dataset         string
	encoding        DocumentEncoding
	mergeProgram    LuaProgram
	hasMergeProgram bool
}

// NewDataset binds stable dataset settings to a Client. MergeProgram is
// optional and is copied when configured.
func NewDataset(client *Client, opts DatasetOptions) (*Dataset, error) {
	if client == nil || client.rpc == nil {
		return nil, errors.New("create dataset: client is required")
	}
	if opts.Store == "" {
		return nil, errors.New("create dataset: store is required")
	}
	if opts.Namespace == "" {
		return nil, errors.New("create dataset: namespace is required")
	}
	if opts.Dataset == "" {
		return nil, errors.New("create dataset: dataset is required")
	}
	if opts.Encoding != DocumentEncodingJSON && opts.Encoding != DocumentEncodingBSON {
		return nil, errors.New("create dataset: document encoding is required")
	}
	dataset := &Dataset{
		client:    client,
		store:     opts.Store,
		namespace: opts.Namespace,
		dataset:   opts.Dataset,
		encoding:  opts.Encoding,
	}
	if opts.MergeProgram != nil {
		if err := opts.MergeProgram.validate(); err != nil {
			return nil, fmt.Errorf("create dataset: %w", err)
		}
		program, err := NewLuaProgram(opts.MergeProgram.Source())
		if err != nil {
			return nil, fmt.Errorf("create dataset: copy merge program: %w", err)
		}
		dataset.mergeProgram = program
		dataset.hasMergeProgram = true
	}
	return dataset, nil
}

// Read fetches one or more records by key. It preserves key order, splits large
// collections automatically, and treats not-found results as successful reads.
func (d *Dataset) Read(ctx context.Context, keys ...Key) ([]ReadResult, error) {
	if err := d.validate("read"); err != nil {
		return nil, err
	}
	addresses := make([]Address, len(keys))
	for index, key := range keys {
		address, err := d.address(key)
		if err != nil {
			return nil, fmt.Errorf("dataset read key %d: %w", index, err)
		}
		addresses[index] = address
	}
	results, err := d.client.Read(ctx, addresses...)
	resultsErr := ReadResultsError(results)
	if err != nil {
		if resultsErr != nil {
			err = errors.Join(err, resultsErr)
		}
		return results, fmt.Errorf("dataset read: %w", err)
	}
	if resultsErr != nil {
		return results, fmt.Errorf("dataset read: %w", resultsErr)
	}
	return results, nil
}

// Create writes complete documents only when their keys do not already exist.
func (d *Dataset) Create(
	ctx context.Context,
	completionMode CompletionMode,
	records ...Record,
) ([]WriteResult, error) {
	opts := datasetPutOptions{
		completionMode: completionMode,
		writeMode:      WriteCreate,
		operation:      "create",
		records:        records,
	}
	return d.put(ctx, opts)
}

// Replace writes complete documents only when their keys already exist.
func (d *Dataset) Replace(
	ctx context.Context,
	completionMode CompletionMode,
	records ...Record,
) ([]WriteResult, error) {
	opts := datasetPutOptions{
		completionMode: completionMode,
		writeMode:      WriteReplace,
		operation:      "replace",
		records:        records,
	}
	return d.put(ctx, opts)
}

// Upsert writes complete documents whether or not their keys already exist.
func (d *Dataset) Upsert(
	ctx context.Context,
	completionMode CompletionMode,
	records ...Record,
) ([]WriteResult, error) {
	opts := datasetPutOptions{
		completionMode: completionMode,
		writeMode:      WriteUpsert,
		operation:      "upsert",
		records:        records,
	}
	return d.put(ctx, opts)
}

// Merge atomically applies the Dataset's bound Lua program to every incoming
// record. A Dataset without MergeProgram rejects Merge before sending an RPC.
func (d *Dataset) Merge(
	ctx context.Context,
	completionMode CompletionMode,
	missingDocumentMode MissingDocumentMode,
	records ...Record,
) ([]WriteResult, error) {
	if err := d.validate("merge"); err != nil {
		return nil, err
	}
	if !d.hasMergeProgram {
		return nil, errors.New("dataset merge: merge program is not configured")
	}
	operations := make([]WriteOperation, len(records))
	for index, record := range records {
		address, document, err := d.encodeRecord(record)
		if err != nil {
			return nil, fmt.Errorf("dataset merge record %d: %w", index, err)
		}
		mergeOptions := MergeOptions{
			Incoming:            document,
			Program:             d.mergeProgram,
			MissingDocumentMode: missingDocumentMode,
		}
		operation, err := NewMerge(address, mergeOptions)
		if err != nil {
			return nil, fmt.Errorf("dataset merge record %d: %w", index, err)
		}
		operations[index] = operation
	}
	return d.write(ctx, "merge", completionMode, operations)
}

type datasetPutOptions struct {
	completionMode CompletionMode
	writeMode      WriteMode
	operation      string
	records        []Record
}

func (d *Dataset) put(ctx context.Context, opts datasetPutOptions) ([]WriteResult, error) {
	if err := d.validate(opts.operation); err != nil {
		return nil, err
	}
	operations := make([]WriteOperation, len(opts.records))
	for index, record := range opts.records {
		address, document, err := d.encodeRecord(record)
		if err != nil {
			return nil, fmt.Errorf("dataset %s record %d: %w", opts.operation, index, err)
		}
		operation, err := NewPut(address, document, opts.writeMode)
		if err != nil {
			return nil, fmt.Errorf("dataset %s record %d: %w", opts.operation, index, err)
		}
		operations[index] = operation
	}
	return d.write(ctx, opts.operation, opts.completionMode, operations)
}

func (d *Dataset) validate(operation string) error {
	if d == nil || d.client == nil || d.client.rpc == nil {
		return fmt.Errorf("dataset %s: client is required", operation)
	}
	return nil
}

func (d *Dataset) encodeRecord(record Record) (Address, Document, error) {
	var emptyAddress Address
	var emptyDocument Document
	address, err := d.address(record.Key)
	if err != nil {
		return emptyAddress, emptyDocument, err
	}
	document, err := NewDocument(record.Value, d.encoding)
	if err != nil {
		return emptyAddress, emptyDocument, err
	}
	return address, document, nil
}

func (d *Dataset) address(key Key) (Address, error) {
	return NewAddress(d.store, d.namespace, d.dataset, key)
}

func (d *Dataset) write(
	ctx context.Context,
	operation string,
	completionMode CompletionMode,
	operations []WriteOperation,
) ([]WriteResult, error) {
	results, err := d.client.Write(ctx, completionMode, operations...)
	resultsErr := WriteResultsError(results)
	if err != nil {
		if resultsErr != nil {
			err = errors.Join(err, resultsErr)
		}
		return results, fmt.Errorf("dataset %s: %w", operation, err)
	}
	if resultsErr != nil {
		return results, fmt.Errorf("dataset %s: %w", operation, resultsErr)
	}
	return results, nil
}
