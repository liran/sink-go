package sink

import (
	"fmt"

	"google.golang.org/grpc/mem"
)

const vtProtoCodecName = "proto"

type vtProtoCodecMessage interface {
	MarshalToSizedBufferVT([]byte) (int, error)
	UnmarshalVT([]byte) error
	SizeVT() int
}

type vtProtoCodec struct {
	pool mem.BufferPool
}

func newVTProtoCodec() *vtProtoCodec {
	codec := &vtProtoCodec{pool: mem.DefaultBufferPool()}
	return codec
}

func (*vtProtoCodec) Name() string {
	return vtProtoCodecName
}

func (c *vtProtoCodec) Marshal(value any) (mem.BufferSlice, error) {
	message, ok := value.(vtProtoCodecMessage)
	if !ok {
		return nil, fmt.Errorf("vtproto: message %T does not provide VT marshal helpers", value)
	}

	size := message.SizeVT()
	if mem.IsBelowBufferPoolingThreshold(size) {
		buffer := make([]byte, size)
		written, err := message.MarshalToSizedBufferVT(buffer)
		if err != nil {
			return nil, err
		}
		if written != size {
			return nil, fmt.Errorf("vtproto: marshaled %d bytes, expected %d", written, size)
		}
		encoded := mem.BufferSlice{mem.SliceBuffer(buffer)}
		return encoded, nil
	}

	pooled := c.pool.Get(size)
	buffer := (*pooled)[:size]
	written, err := message.MarshalToSizedBufferVT(buffer)
	if err != nil {
		c.pool.Put(pooled)
		return nil, err
	}
	if written != size {
		c.pool.Put(pooled)
		return nil, fmt.Errorf("vtproto: marshaled %d bytes, expected %d", written, size)
	}
	encoded := mem.BufferSlice{mem.NewBuffer(pooled, c.pool)}
	return encoded, nil
}

func (c *vtProtoCodec) Unmarshal(data mem.BufferSlice, value any) error {
	message, ok := value.(vtProtoCodecMessage)
	if !ok {
		return fmt.Errorf("vtproto: message %T does not provide VT unmarshal helpers", value)
	}
	buffer := data.MaterializeToBuffer(c.pool)
	defer buffer.Free()
	return message.UnmarshalVT(buffer.ReadOnlyData())
}
