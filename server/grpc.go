package server

import (
	"context"
	"fmt"

	dgrpcServer "github.com/streamingfast/dgrpc/server"
	"github.com/streamingfast/dgrpc/server/factory"
	pbstore "github.com/streamingfast/substreams-foundationnal-store/pb/store"
	"github.com/streamingfast/substreams-foundationnal-store/store"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// StoreServer implements the StoreKV gRPC service
type StoreServer struct {
	pbstore.UnimplementedStoreKVServer
	store store.Store
}

// NewStoreServer creates a new StoreServer with the given store
func NewStoreServer(store store.Store) *StoreServer {
	return &StoreServer{
		store: store,
	}
}

// Get implements the Get method of the StoreKV service
func (s *StoreServer) Get(ctx context.Context, req *pbstore.GetRequest) (*pbstore.GetResponse, error) {
	return s.store.Get(req)
}

// GetAll implements the GetAll method of the StoreKV service
func (s *StoreServer) GetAll(ctx context.Context, req *pbstore.GetAllRequest) (*pbstore.GetAllResponse, error) {
	return s.store.GetAll(req)
}

// Serve starts the gRPC server on the given address
func Serve(addr string, store store.Store, opts ...grpc.ServerOption) error {
	// Create a default logger
	logger := zap.NewNop()

	// Create a channel to receive server errors
	errCh := make(chan error, 1)

	storeServer := NewStoreServer(store)

	// Create the dgrpc server
	grpcServer := factory.ServerFromOptions(
		dgrpcServer.WithLogger(logger),
		dgrpcServer.WithPlainTextServer(),
		dgrpcServer.WithGRPCServerOptions(opts...),
		dgrpcServer.WithRegisterService(func(gs *grpc.Server) {
			pbstore.RegisterStoreKVServer(gs, storeServer)
		}),
	)

	// Handle server termination
	grpcServer.OnTerminated(func(err error) {
		if err != nil {
			logger.Error("gRPC server unexpected failure", zap.Error(err))
			errCh <- err
		}
	})

	// Launch the server
	go grpcServer.Launch(addr)

	fmt.Printf("Starting gRPC server on %s\n", addr)

	// Wait for an error from the server
	// This will block until the server terminates with an error
	// or until the application is shut down
	return <-errCh
}
