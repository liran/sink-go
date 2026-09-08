package sink

import (
	"context"
	"errors"
	"fmt"
	"strings"

	sinkv1 "github.com/liran/sink-go/api/sink/v1"
)

type QueryRequest struct {
	Command    Command
	Page       int         // One-based; zero selects page 1.
	PageSize   int         // Zero selects 100; maximum 1000.
	Sort       []SortField // Ordered keys; empty preserves native sorting.
	Projection *Projection // Nil preserves the native projection.
}

type SortField struct {
	Field      string
	Descending bool
}

// Projection includes or excludes native document paths. An empty Fields list
// selects all fields. Search paths are relative to _source; hit metadata remains.
type Projection struct {
	Fields  []string
	Exclude bool
}

type QueryResponse struct {
	Documents []Document
	HasMore   bool
}

// Query fetches an independent page without retaining a cursor between calls.
// Use a stable native sort with a unique tie-breaker. Concurrent writes can shift
// pages; deep pages are subject to backend offset costs and result-window limits.
// HasMore uses one extra result. Query never automatically runs Count or retries.
func (c *Client) Query(ctx context.Context, req QueryRequest) (QueryResponse, error) {
	var empty QueryResponse
	if c == nil || c.rpc == nil {
		return empty, errors.New("query requires a client")
	}
	if req.Page < 0 || uint64(req.Page) > uint64(^uint32(0)) || req.PageSize < 0 || req.PageSize > 1000 {
		return empty, errors.New("query requires a nonnegative uint32 page and page size between 0 and 1000")
	}
	command, err := req.Command.toProto()
	if err != nil {
		return empty, err
	}
	request := &sinkv1.QueryRequest{Command: command, Page: uint32(req.Page), PageSize: uint32(req.PageSize)}
	seen := make(map[string]bool)
	for _, field := range req.Sort {
		if strings.TrimSpace(field.Field) == "" || seen[field.Field] {
			return empty, errors.New("sort fields must be nonempty and unique")
		}
		seen[field.Field] = true
		item := &sinkv1.SortField{Field: field.Field, Descending: field.Descending}
		request.Sort = append(request.Sort, item)
	}
	if req.Projection != nil {
		seen = make(map[string]bool)
		for _, field := range req.Projection.Fields {
			if strings.TrimSpace(field) == "" || seen[field] {
				return empty, errors.New("projection fields must be nonempty and unique")
			}
			seen[field] = true
		}
		request.Projection = &sinkv1.Projection{Fields: append([]string(nil), req.Projection.Fields...), Exclude: req.Projection.Exclude}
	}
	response, err := c.rpc.Query(ctx, request, c.config.sinkCallOptions...)
	if err != nil {
		return empty, fmt.Errorf("query native page: %w", err)
	}
	if response == nil {
		return empty, protocolError("Query", "response is empty")
	}
	pageSize := req.PageSize
	if pageSize == 0 {
		pageSize = 100
	}
	if len(response.GetDocuments()) > pageSize || (response.GetHasMore() && len(response.GetDocuments()) != pageSize) {
		return empty, protocolError("Query", "invalid page length or has_more")
	}
	result := QueryResponse{HasMore: response.GetHasMore()}
	for _, raw := range response.GetDocuments() {
		document, err := documentFromProto(raw)
		if err != nil {
			return empty, protocolError("Query", err.Error())
		}
		result.Documents = append(result.Documents, document)
	}
	return result, nil
}

type CountRequest struct {
	Command Command
}

type CountResponse struct {
	Count     uint64
	Estimated bool // True when the backend used collection metadata.
}

// Count counts matches before find/HTTP pagination, or after the supplied
// aggregate pipeline. MongoDB automatically uses metadata for ordinary empty
// find filters; other queries remain exact. Estimated identifies the strategy.
// Concurrent writes can make the count differ from a separately fetched page.
// The SDK never retries.
func (c *Client) Count(ctx context.Context, req CountRequest) (CountResponse, error) {
	var empty CountResponse
	if c == nil || c.rpc == nil {
		return empty, errors.New("count requires a client")
	}
	command, err := req.Command.toProto()
	if err != nil {
		return empty, err
	}
	request := &sinkv1.CountRequest{Command: command}
	response, err := c.rpc.Count(ctx, request, c.config.sinkCallOptions...)
	if err != nil {
		return empty, fmt.Errorf("count native query: %w", err)
	}
	if response == nil {
		return empty, protocolError("Count", "response is empty")
	}
	result := CountResponse{Count: response.GetCount(), Estimated: response.GetEstimated()}
	return result, nil
}
