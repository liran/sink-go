package sink

import (
	"testing"

	sinkv1 "github.com/liran/sink-go/api/sink/v1"
	vtgrpc "github.com/planetscale/vtprotobuf/codec/grpc"
	"google.golang.org/grpc"
)

type vtprotoMessage interface {
	MarshalVT() ([]byte, error)
	UnmarshalVT([]byte) error
}

var (
	_ vtprotoMessage = (*sinkv1.ReadRequest)(nil)
	_ vtprotoMessage = (*sinkv1.ReadResponse)(nil)
	_ vtprotoMessage = (*sinkv1.WriteRequest)(nil)
	_ vtprotoMessage = (*sinkv1.WriteResponse)(nil)
	_ vtprotoMessage = (*sinkv1.DeleteRequest)(nil)
	_ vtprotoMessage = (*sinkv1.DeleteResponse)(nil)
)

func TestClientConfigUsesVTProtoForSinkCalls(t *testing.T) {
	var options ClientOptions
	config, err := newClientConfig(options)
	if err != nil {
		t.Fatalf("newClientConfig() error = %v", err)
	}

	foundVTCodec := false
	for _, option := range config.sinkCallOptions {
		forceCodec, ok := option.(grpc.ForceCodecCallOption)
		if !ok {
			continue
		}
		if _, ok := forceCodec.Codec.(vtgrpc.Codec); !ok {
			t.Fatalf("Sink ForceCodec type = %T", forceCodec.Codec)
		}
		foundVTCodec = true
	}
	if !foundVTCodec {
		t.Fatal("Sink call options do not force the vtprotobuf codec")
	}

	for _, option := range config.healthCallOptions {
		if forceCodec, ok := option.(grpc.ForceCodecCallOption); ok {
			t.Fatalf("health ForceCodec type = %T", forceCodec.Codec)
		}
	}
}

func TestVTProtoCodecRoundTrip(t *testing.T) {
	request := &sinkv1.ReadRequest{
		Operations: []*sinkv1.ReadOperation{
			{
				Address: &sinkv1.RecordAddress{
					Store:     "primary",
					Namespace: "catalog",
					Dataset:   "products",
					Key: &sinkv1.RecordKey{
						Kind: &sinkv1.RecordKey_StringValue{StringValue: "sku-1"},
					},
				},
			},
		},
	}
	codec := vtgrpc.Codec{}
	encoded, err := codec.Marshal(request)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	decoded := &sinkv1.ReadRequest{}
	if err := codec.Unmarshal(encoded, decoded); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	address := decoded.GetOperations()[0].GetAddress()
	if address.GetStore() != "primary" || address.GetKey().GetStringValue() != "sku-1" {
		t.Fatalf("round-trip address = %+v", address)
	}
}
