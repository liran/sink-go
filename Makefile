.PHONY: proto proto-check test test-race test-integration lint clean

BIN_DIR := $(CURDIR)/.bin
PROTOC_GEN_GO_VERSION := v1.36.6
PROTOC_GEN_GO_GRPC_VERSION := v1.5.1
PROTOC_GEN_GO_VTPROTO_VERSION := v0.6.1-0.20240319094008-0393e58bdf10
STATICCHECK_VERSION := v0.8.1

proto:
	@mkdir -p $(BIN_DIR)
	GOBIN=$(BIN_DIR) go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	GOBIN=$(BIN_DIR) go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)
	GOBIN=$(BIN_DIR) go install github.com/planetscale/vtprotobuf/cmd/protoc-gen-go-vtproto@$(PROTOC_GEN_GO_VTPROTO_VERSION)
	PATH="$(BIN_DIR):$$PATH" protoc \
		--proto_path=proto \
		--go_out=. --go_opt=module=github.com/liran/sink-go \
		--go-grpc_out=. --go-grpc_opt=module=github.com/liran/sink-go \
		--go-vtproto_out=. --go-vtproto_opt=module=github.com/liran/sink-go,features=marshal+unmarshal+size \
		proto/sink/sink.proto
	@perl -0pi -e 's/\treturn &sinkClient\{cc\}\n/\tclient := \&sinkClient{cc}\n\treturn client\n/' api/sink/v1/sink_grpc.pb.go
	@perl -0pi -e 's/^(\s*)m\.([A-Za-z]+) = append\(m\.\2, &([A-Za-z]+)\{\}\)\n/$$1item := \&$${3}{}\n$$1m.$$2 = append(m.$$2, item)\n/gm' api/sink/v1/sink_vtproto.pb.go
	@gofmt -w api/sink/v1/sink_grpc.pb.go api/sink/v1/sink_vtproto.pb.go

proto-check: proto
	git diff --exit-code -- api/sink/v1

test:
	go test ./... -count=1

test-race:
	go test -race ./... -count=1

test-integration:
	test -n "$$SINK_INTEGRATION_ADDRESS"
	go test -tags=integration ./... -count=1

lint:
	@test -z "$$(gofmt -l .)"
	go vet ./...
	go run honnef.co/go/tools/cmd/staticcheck@$(STATICCHECK_VERSION) -checks=all ./...

clean:
	rm -r $(BIN_DIR)
