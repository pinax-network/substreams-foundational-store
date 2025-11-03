package legacy

import (
	"context"
	"fmt"
	"time"

	pbmodel "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/model/v2"
	pbv1 "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/service/v1"
	pbv2 "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/service/v2"
	"github.com/streamingfast/substreams-foundational-store/store"
	"go.uber.org/zap"
)

// Server implements the legacy v1 Store gRPC service by delegating to the v2 store
// and adapting requests/responses.
//
// Note: We gate on head block the same way as the v2 server: if requested block
// has not been reached yet, we return block_reached=false and no per-key entries.
// v1 has an extra NOT_FOUND_BLOCK_NOT_REACHED code, but callers can rely on the
// block_reached flag; we keep semantics aligned with v2.
type Server struct {
	pbv1.UnimplementedStoreServer
	store            store.Store
	headBlockFetcher func() uint64
	logger           *zap.Logger
}

func NewServer(store store.Store, headBlockFetcher func() uint64, logger *zap.Logger) *Server {
	return &Server{store: store, headBlockFetcher: headBlockFetcher, logger: logger}
}

func (s *Server) Get(ctx context.Context, req *pbv1.GetRequest) (*pbv1.GetResponse, error) {
	headBlock := s.headBlockFetcher()
	if headBlock < req.BlockNumber {
		return &pbv1.GetResponse{BlockReached: false}, nil
	}

	v2req := &pbv2.GetRequest{
		BlockNumber: req.BlockNumber,
		BlockHash:   req.BlockHash,
		Key:         &pbmodel.Key{Bytes: req.Key},
	}

	v2resp, err := s.store.Get(v2req)
	if err != nil {
		return nil, fmt.Errorf("getting key from store (legacy): %w", err)
	}

	out := &pbv1.GetResponse{BlockReached: true}
	if v2resp.Entry != nil {
		code := v2resp.Entry.GetCode()
		// Map deletion semantics when omit_deleted is true
		if req.GetOmitDeleted() && code == pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND_FINALIZE {
			out.Code = pbv1.ResponseCode_RESPONSE_CODE_NOT_FOUND
			return out, nil
		}

		// Map codes 1:1 for shared values
		switch code {
		case pbmodel.ResponseCode_RESPONSE_CODE_FOUND:
			out.Code = pbv1.ResponseCode_RESPONSE_CODE_FOUND
			if ent := v2resp.Entry.GetEntry(); ent != nil {
				out.Value = ent.GetValue()
			}
		case pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND:
			out.Code = pbv1.ResponseCode_RESPONSE_CODE_NOT_FOUND
		case pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND_FINALIZE:
			out.Code = pbv1.ResponseCode_RESPONSE_CODE_NOT_FOUND_FINALIZE
		default:
			out.Code = pbv1.ResponseCode_RESPONSE_CODE_UNSPECIFIED
		}
	}

	return out, nil
}

func (s *Server) GetAll(ctx context.Context, req *pbv1.GetAllRequest) (*pbv1.GetAllResponse, error) {
	executionStart := time.Now()
	headBlock := s.headBlockFetcher()
	if headBlock < req.BlockNumber {
		return &pbv1.GetAllResponse{BlockReached: false}, nil
	}

	keys := make([]*pbmodel.Key, 0, len(req.Keys))
	for _, k := range req.Keys {
		keys = append(keys, &pbmodel.Key{Bytes: k})
	}
	v2req := &pbv2.GetAllRequest{
		BlockNumber: req.BlockNumber,
		BlockHash:   req.BlockHash,
		Keys:        keys,
	}

	v2resp, err := s.store.GetAll(v2req)
	if err != nil {
		return nil, fmt.Errorf("getting all keys from store (legacy): %w", err)
	}

	out := &pbv1.GetAllResponse{BlockReached: true}
	out.Entries = make([]*pbv1.ResponseEntry, 0, len(v2resp.GetEntries().GetEntries()))
	for _, qe := range v2resp.GetEntries().GetEntries() {
		resp := &pbv1.GetResponse{BlockReached: true}
		code := qe.GetCode()
		// Omit deleted mapping when requested
		if req.GetOmitDeleted() && code == pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND_FINALIZE {
			resp.Code = pbv1.ResponseCode_RESPONSE_CODE_NOT_FOUND
			// key for entry
			var keyBytes []byte
			if e := qe.GetEntry(); e != nil {
				keyBytes = e.GetKey().GetBytes()
			}
			out.Entries = append(out.Entries, &pbv1.ResponseEntry{Key: keyBytes, Response: resp})
			continue
		}
		switch code {
		case pbmodel.ResponseCode_RESPONSE_CODE_FOUND:
			resp.Code = pbv1.ResponseCode_RESPONSE_CODE_FOUND
		case pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND:
			resp.Code = pbv1.ResponseCode_RESPONSE_CODE_NOT_FOUND
		case pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND_FINALIZE:
			resp.Code = pbv1.ResponseCode_RESPONSE_CODE_NOT_FOUND_FINALIZE
		default:
			resp.Code = pbv1.ResponseCode_RESPONSE_CODE_UNSPECIFIED
		}
		var keyBytes []byte
		if e := qe.GetEntry(); e != nil {
			keyBytes = e.GetKey().GetBytes()
			resp.Value = e.GetValue()
		}
		out.Entries = append(out.Entries, &pbv1.ResponseEntry{Key: keyBytes, Response: resp})
	}

	// Minimal logging for legacy endpoint
	s.logger.Info("legacy v1 request stats",
		zap.Uint64("block_number", req.BlockNumber),
		zap.Uint64("head_block", headBlock),
		zap.Int("requested_keys", len(req.Keys)),
		zap.Int("found_entries", len(out.Entries)),
		zap.Duration("execution_time", time.Since(executionStart)),
		zap.Bool("keep", false),
	)

	return out, nil
}

// The v1 server has no need for mustEmbedUnimplementedStoreServer because we implement the full interface via registration.
// We do not expose Run/Shutdown here; the main v2 server owns the GRPC server lifecycle.

// Assert interface implementation at compile-time
var _ pbv1.StoreServer = (*Server)(nil)
