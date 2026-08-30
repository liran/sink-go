package sink

import (
	"testing"

	sinkv1 "github.com/liran/sink-go/api/sink/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
)

type vtprotoMessage interface {
	MarshalToSizedBufferVT([]byte) (int, error)
	UnmarshalVT([]byte) error
	SizeVT() int
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
		forceCodec, ok := option.(grpc.ForceCodecV2CallOption)
		if !ok {
			continue
		}
		if _, ok := forceCodec.CodecV2.(*vtProtoCodec); !ok {
			t.Fatalf("Sink ForceCodecV2 type = %T", forceCodec.CodecV2)
		}
		foundVTCodec = true
	}
	if !foundVTCodec {
		t.Fatal("Sink call options do not force the vtprotobuf codec")
	}

	for _, option := range config.healthCallOptions {
		if forceCodec, ok := option.(grpc.ForceCodecV2CallOption); ok {
			t.Fatalf("health ForceCodecV2 type = %T", forceCodec.CodecV2)
		}
	}
}

func TestVTProtoCodecRoundTrip(t *testing.T) {
	request := testVTProtoRequest()
	codec := newVTProtoCodec()
	encoded, err := codec.Marshal(request)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	defer encoded.Free()
	decoded := &sinkv1.ReadRequest{}
	if err := codec.Unmarshal(encoded, decoded); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	address := decoded.GetOperations()[0].GetAddress()
	if address.GetStore() != "primary" || address.GetKey().GetStringValue() != "sku-1" {
		t.Fatalf("round-trip address = %+v", address)
	}
}

func TestVTProtoCodecDoesNotReplaceGlobalProtoCodec(t *testing.T) {
	globalCodec := encoding.GetCodecV2(vtProtoCodecName)
	if globalCodec == nil {
		t.Fatal("global proto CodecV2 is not registered")
	}
	var options ClientOptions
	if _, err := newClientConfig(options); err != nil {
		t.Fatalf("newClientConfig() error = %v", err)
	}
	if current := encoding.GetCodecV2(vtProtoCodecName); current != globalCodec {
		t.Fatalf("global proto CodecV2 changed from %T to %T", globalCodec, current)
	}
}

func BenchmarkVTProtoCodecMarshal(b *testing.B) {
	request := testVTProtoRequest()
	codec := newVTProtoCodec()
	b.ReportAllocs()
	for b.Loop() {
		encoded, err := codec.Marshal(request)
		if err != nil {
			b.Fatal(err)
		}
		encoded.Free()
	}
}

func BenchmarkStandardProtoCodecMarshal(b *testing.B) {
	request := testVTProtoRequest()
	codec := encoding.GetCodecV2(vtProtoCodecName)
	if codec == nil {
		b.Fatal("global proto CodecV2 is not registered")
	}
	b.ReportAllocs()
	for b.Loop() {
		encoded, err := codec.Marshal(request)
		if err != nil {
			b.Fatal(err)
		}
		encoded.Free()
	}
}

func testVTProtoRequest() *sinkv1.ReadRequest {
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
	return request
}
