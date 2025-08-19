package server

import (
	"context"
	"fmt"
	"time"

	dgrpcServer "github.com/streamingfast/dgrpc/server"
	"github.com/streamingfast/dgrpc/server/factory"
	pbstore "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/v1"
	"github.com/streamingfast/substreams-foundational-store/sink"
	"github.com/streamingfast/substreams-foundational-store/store"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// StoreServer implements the StoreKV gRPC service
type StoreServer struct {
	pbstore.UnimplementedStoreKVServer
	store store.Store
}

// NewStoreServer creates a new StoreServer with the given foundational-store
func NewStoreServer(store store.Store) *StoreServer {
	return &StoreServer{
		store: store,
	}
}

// Get implements the Get method of the StoreKV service
func (s *StoreServer) Get(ctx context.Context, req *pbstore.GetRequest) (*pbstore.GetResponse, error) {
	start := time.Now()
	defer func() {
		sink.GRPCGetDuration.ObserveDuration(time.Since(start))
		sink.GRPCGetCount.Inc()
	}()

	r, err := s.store.Get(req)
	if err != nil {
		return nil, fmt.Errorf("getting from store: %w", err)
	}
	return r, nil
}

// GetAll implements the GetAll method of the StoreKV service
func (s *StoreServer) GetAll(ctx context.Context, req *pbstore.GetAllRequest) (*pbstore.GetAllResponse, error) {
	start := time.Now()
	defer func() {
		sink.GRPCGetAllDuration.ObserveDuration(time.Since(start))
		sink.GRPCGetAllCount.Inc()
	}()

	return s.store.GetAll(req)
}

// Serve starts the gRPC server on the given address
func Serve(addr string, store store.Store, logger *zap.Logger, opts ...grpc.ServerOption) error {

	// Create a channel to receive server errors
	errCh := make(chan error, 1)

	storeServer := NewStoreServer(store)

	// Create the dgrpc server with reduced per-call logging
	grpcLogger := logger.Named("grpc").WithOptions(zap.IncreaseLevel(zap.WarnLevel))
	grpcServer := factory.ServerFromOptions(
		dgrpcServer.WithLogger(grpcLogger),
		dgrpcServer.WithPlainTextServer(),
		dgrpcServer.WithGRPCServerOptions(opts...),
		dgrpcServer.WithRegisterService(func(gs *grpc.Server) {
			pbstore.RegisterStoreKVServer(gs, storeServer)
		}),
	)

	// Handle server termination
	grpcServer.OnTerminated(func(err error) {
		if err != nil {
			logger.Error("grpc server unexpected failure", zap.Error(err))
			errCh <- err
		}
	})

	// Launch the server
	go grpcServer.Launch(addr)

	// Wait for an error from the server
	// This will block until the server terminates with an error
	// or until the application is shut down
	return <-errCh
}
