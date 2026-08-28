package sink_test

import (
	"slices"
	"testing"

	sinkv1 "github.com/liran/sink-go/api/sink/v1"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestGeneratedSinkServiceContract(t *testing.T) {
	service := sinkv1.File_sink_sink_proto.Services().ByName(protoreflect.Name("Sink"))
	if service == nil {
		t.Fatal("Sink service descriptor is missing")
	}
	if service.FullName() != protoreflect.FullName("sink.v1.Sink") {
		t.Fatalf("Sink service full name = %q", service.FullName())
	}
	methods := service.Methods()
	names := make([]string, 0, methods.Len())
	for index := range methods.Len() {
		names = append(names, string(methods.Get(index).Name()))
	}
	want := []string{"Read", "Write", "Delete"}
	if !slices.Equal(names, want) {
		t.Fatalf("Sink methods = %v, want %v", names, want)
	}
}
