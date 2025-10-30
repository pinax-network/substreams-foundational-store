package grpc

import (
	"context"
	"fmt"
	"time"

	dgrpcServer "github.com/streamingfast/dgrpc/server"
	"github.com/streamingfast/dgrpc/server/factory"
	"github.com/streamingfast/shutter"
	pbservice "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/service/v1"
	"github.com/streamingfast/substreams-foundational-store/store"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// StoreServer implements the StoreKV gRPC service
type GrpcServer struct {
	*shutter.Shutter
	pbservice.UnimplementedStoreServer
	store            store.Store
	dgrpcServer      dgrpcServer.Server
	headBlockFetcher FetchHeadBlock
	logger           *zap.Logger
}

type FetchHeadBlock func() uint64

// NewStoreServer creates a new StoreServer with the given foundational-store
func NewStoreServer(store store.Store, headBlockFetcher FetchHeadBlock, logger *zap.Logger) *GrpcServer {
	return &GrpcServer{
		Shutter:          shutter.New(),
		store:            store,
		dgrpcServer:      nil,
		headBlockFetcher: headBlockFetcher,
		logger:           logger,
	}
}

// Get implements the Get method of the StoreKV service
func (s *GrpcServer) Get(ctx context.Context, req *pbservice.GetRequest) (*pbservice.GetResponse, error) {
	headBlock := s.headBlockFetcher()
	if headBlock < req.BlockNumber {
		return &pbservice.GetResponse{BlockReached: false}, nil
	}

	r, err := s.store.Get(req)
	if err != nil {
		return nil, fmt.Errorf("getting key from store: %w", err)
	}

	r.BlockReached = true
	return r, nil
}

// GetFirst implements the GetFirst method of the Store service
func (s *GrpcServer) GetFirst(ctx context.Context, req *pbservice.GetFirstRequest) (*pbservice.GetResponse, error) {
	headBlock := s.headBlockFetcher()
	if headBlock < req.BlockNumber {
		return &pbservice.GetResponse{BlockReached: false}, nil
	}

	r, err := s.store.GetFirst(req)
	if err != nil {
		return nil, fmt.Errorf("getting first key from store: %w", err)
	}
	// No block gating for GetFirst as request has no block fields
	return r, nil
}

// GetAll implements the GetAll method of the StoreKV service
func (s *GrpcServer) GetAll(ctx context.Context, req *pbservice.GetAllRequest) (*pbservice.GetAllResponse, error) {

	executionStart := time.Now()
	headBlock := s.headBlockFetcher()
	if headBlock < req.BlockNumber {
		return &pbservice.GetAllResponse{BlockReached: false}, nil
	}

	r, err := s.store.GetAll(req)
	if err != nil {
		return nil, fmt.Errorf("getting all keys from store: %w", err)
	}

	// Set BlockReached = true for top-level response
	r.BlockReached = true

	s.logger.Info("request stats",
		zap.Uint64("block_number", req.BlockNumber),
		zap.Uint64("head_block", headBlock),
		zap.Int("requested_keys", len(req.Keys)),
		zap.Int("found_keys", len(r.Entries.Entries)),
		zap.Duration("execution_time", time.Since(executionStart)),
		zap.Bool("keep", false),
	)
	return r, nil
}

func (s *GrpcServer) GetAllFirst(ctx context.Context, req *pbservice.GetAllRequest) (*pbservice.GetAllResponse, error) {

	executionStart := time.Now()
	headBlock := s.headBlockFetcher()
	if headBlock < req.BlockNumber {
		return &pbservice.GetAllResponse{BlockReached: false}, nil
	}

	r, err := s.store.GetAllFirst(req)
	if err != nil {
		return nil, fmt.Errorf("getting all first keys from store: %w", err)
	}

	// Set BlockReached = true for top-level response
	r.BlockReached = true

	s.logger.Info("request stats",
		zap.Uint64("block_number", req.BlockNumber),
		zap.Uint64("head_block", headBlock),
		zap.Int("requested_keys", len(req.Keys)),
		zap.Int("found_keys", len(r.Entries.Entries)),
		zap.Duration("execution_time", time.Since(executionStart)),
		zap.Bool("keep", false),
	)
	return r, nil
}

func (s *GrpcServer) Run(addr string, opts ...grpc.ServerOption) {
	// Create the dgrpc server with reduced per-call logging
	grpcLogger := s.logger.Named("grpc").WithOptions(zap.IncreaseLevel(zap.WarnLevel))
	s.dgrpcServer = factory.ServerFromOptions(
		dgrpcServer.WithLogger(grpcLogger),
		dgrpcServer.WithPlainTextServer(),
		dgrpcServer.WithGRPCServerOptions(opts...),
		dgrpcServer.WithRegisterService(func(gs *grpc.Server) {
			pbservice.RegisterStoreServer(gs, s)
		}),
		dgrpcServer.WithHealthCheck(dgrpcServer.HealthCheckOverGRPC|dgrpcServer.HealthCheckOverHTTP, healthCheck),
	)

	s.dgrpcServer.OnTerminated(func(err error) {
		s.Shutter.Shutdown(err)
	})

	s.dgrpcServer.Launch(addr)
}

func healthCheck(ctx context.Context) (isReady bool, out interface{}, err error) {
	// In your own code, you should tied the `isReady` value to the lifecycle of your application.
	// If your application is ready to accept requests, return `true`, otherwise return `false`.
	//
	// The `out` value that can anything is used by the HTTP health check (if configured in the `WithHealthCheck`
	// option by using for example `dgrpc.HealthCheckOverGRPC | dgrpc.HealthCheckOverHTTP`) will be serialized
	// in the body as JSON. It is **not** used by the GRPC health check because there is no such notion of
	// return payload.
	//
	// An error should be returned only in really rare cases, most of the time if there is an error it means
	// your application is not ready to accept requests.
	return true, nil, nil
}

func (s *GrpcServer) Shutdown(err error) {
	if server := s.dgrpcServer; server != nil {
		server.Shutdown(15 * time.Second)
	}

	s.Shutter.Shutdown(err)
}
