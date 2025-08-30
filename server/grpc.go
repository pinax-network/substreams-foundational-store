package server

import (
	"context"
	"fmt"
	"time"

	dgrpcServer "github.com/streamingfast/dgrpc/server"
	"github.com/streamingfast/dgrpc/server/factory"
	"github.com/streamingfast/shutter"
	pbstore "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/v1"
	"github.com/streamingfast/substreams-foundational-store/sink"
	"github.com/streamingfast/substreams-foundational-store/store"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// StoreServer implements the StoreKV gRPC service
type StoreServer struct {
	*shutter.Shutter
	pbstore.UnimplementedStoreKVServer
	store      store.Store
	grpcServer dgrpcServer.Server
}

// NewStoreServer creates a new StoreServer with the given foundational-store
func NewStoreServer(store store.Store) *StoreServer {
	return &StoreServer{
		Shutter:    shutter.New(),
		store:      store,
		grpcServer: nil,
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

func (s *StoreServer) Run(addr string, logger *zap.Logger, opts ...grpc.ServerOption) {
	// Create the dgrpc server with reduced per-call logging
	grpcLogger := logger.Named("grpc").WithOptions(zap.IncreaseLevel(zap.WarnLevel))
	s.grpcServer = factory.ServerFromOptions(
		dgrpcServer.WithLogger(grpcLogger),
		dgrpcServer.WithPlainTextServer(),
		dgrpcServer.WithGRPCServerOptions(opts...),
		dgrpcServer.WithRegisterService(func(gs *grpc.Server) {
			pbstore.RegisterStoreKVServer(gs, s)
		}),
	)

	s.grpcServer.OnTerminated(func(err error) {
		s.Shutter.Shutdown(err)
	})

	s.grpcServer.Launch(addr)
}

func (s *StoreServer) Shutdown(err error) {
	if server := s.grpcServer; server != nil {
		server.Shutdown(15 * time.Second)
	}

	s.Shutter.Shutdown(err)
}
