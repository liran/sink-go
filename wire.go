package sink

import (
	"bytes"
	"fmt"

	sinkv1 "github.com/liran/sink-go/api/sink/v1"
)

func (k Key) toProto() *sinkv1.RecordKey {
	key := &sinkv1.RecordKey{}
	switch k.kind {
	case keyKindString:
		kind := &sinkv1.RecordKey_StringValue{StringValue: k.stringValue}
		key.Kind = kind
	case keyKindInt64:
		kind := &sinkv1.RecordKey_Int64Value{Int64Value: k.int64Value}
		key.Kind = kind
	case keyKindBytes:
		kind := &sinkv1.RecordKey_BytesValue{BytesValue: bytes.Clone(k.bytesValue)}
		key.Kind = kind
	case keyKindOpaque:
		opaque := &sinkv1.OpaqueValue{
			Type: k.opaqueType,
			Data: bytes.Clone(k.bytesValue),
		}
		kind := &sinkv1.RecordKey_OpaqueValue{OpaqueValue: opaque}
		key.Kind = kind
	}
	return key
}

func (a Address) toProto() *sinkv1.RecordAddress {
	address := &sinkv1.RecordAddress{
		Store:     a.store,
		Namespace: a.namespace,
		Dataset:   a.dataset,
		Key:       a.key.toProto(),
	}
	return address
}

func (d Document) toProto() *sinkv1.Document {
	document := &sinkv1.Document{
		ContentType: d.contentType,
		Data:        bytes.Clone(d.data),
	}
	return document
}

func documentFromProto(document *sinkv1.Document) (Document, error) {
	var empty Document
	if document == nil {
		return empty, fmt.Errorf("document is missing")
	}
	return NewDocument(document.GetContentType(), document.GetData())
}

func revisionFromProto(revision *sinkv1.RevisionToken) RevisionToken {
	if revision == nil {
		var empty RevisionToken
		return empty
	}
	token := RevisionToken{data: bytes.Clone(revision.GetData())}
	return token
}

func (o WriteOperation) toProto() *sinkv1.WriteOperation {
	operation := &sinkv1.WriteOperation{Address: o.address.toProto()}
	switch o.action {
	case writeActionPut:
		put := &sinkv1.PutOperation{
			Document: o.put.toProto(),
			Mode:     o.mode,
		}
		action := &sinkv1.WriteOperation_Put{Put: put}
		operation.Action = action
	case writeActionMerge:
		program := &sinkv1.LuaProgram{
			Sha256: o.merge.Program.SHA256(),
		}
		merge := &sinkv1.MergeOperation{
			IncomingDocument:    o.merge.IncomingDocument.toProto(),
			LuaProgram:          program,
			MissingDocumentMode: o.merge.MissingDocumentMode,
		}
		action := &sinkv1.WriteOperation_Merge{Merge: merge}
		operation.Action = action
	}
	return operation
}
