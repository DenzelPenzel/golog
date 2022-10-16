package server

import (
	"context"
	"time"

	api "github.com/denisschmidt/golog/api/v1"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	"go.opencensus.io/plugin/ocgrpc"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Config struct {
	CommitLog   CommitLog
	Authorizer  Authorizer
	GetServerer GetServerer
}

// refer to ACL policy table
const (
	objectWildcard = "*"
	produceAction  = "produce"
	consumeAction  = "consume"
)

type Authorizer interface {
	Authorize(subject, object, action string) error
}

// CommitLog - need for dependency inversion
// service depend on a log interface rather than on a concrete type
// service can use any log implementation that satisfies the slog interface
type CommitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

func newGRPCServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{
		Config: config,
	}
	return srv, nil
}

// create new record
func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	// check auth roles
	if err := s.Authorizer.Authorize(subject(ctx), objectWildcard, produceAction); err != nil {
		return nil, err
	}

	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil
}

// receive record
func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	if err := s.Authorizer.Authorize(subject(ctx), objectWildcard, produceAction); err != nil {
		return nil, err
	}

	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &api.ConsumeResponse{Record: record}, nil
}

// implements a bidirectional streaming RPC so the client can stream data into the server’s log
// and the server can tel the client whether each request succeeded
func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}
		if err = stream.Send(res); err != nil {
			return err
		}
	}
}

// implements a server-side streaming RPC so the
// client can tell the server where in the log to read records, and then the server
// will stream every record that follows—even records that aren’t in the log yet!

// When the server reaches the end of the log, the server will wait until someone
// appends a record to the log and then continue streaming records to the client
func (s *grpcServer) ConsumeStream(req *api.ConsumeRequest, stream api.Log_ConsumeStreamServer) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := s.Consume(stream.Context(), req)

			switch err.(type) {
			case nil:
			case api.ErrOffsetOutOfRange:
				continue
			default:
				return err
			}

			if err = stream.Send(res); err != nil {
				return err
			}

			req.Offset++
		}
	}
}

func NewGRPCServer(config *Config, opts ...grpc.ServerOption) (*grpc.Server, error) {
	logger := zap.L().Named("server")
	zapOpts := []grpc_zap.Option{
		grpc_zap.WithDurationField(
			func(duration time.Duration) zapcore.Field {
				return zap.Int64(
					"grpc.time_ns",
					duration.Nanoseconds(),
				)
			},
		),
	}

	/*
		metrics_traces, configure OpenCensus

		The default server views track stats on:
			• Received bytes per RPC
			• Sent bytes per RPC
			• Latency
			• Completed RPCs
			Now, change the grpcOpts af

	*/
	trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})
	err := view.Register(ocgrpc.DefaultServerViews...)
	if err != nil {
		return nil, err
	}

	/*
		- configure gRPC to apply the Zap interceptors that log the gRPC
			calls and attach OpenCensus as the server’s stat handler so that OpenCensus
			can record stats on the server’s request handling.

		- hook up our authenticate() interceptor to our gRPC server so that server
			identifies the subject of each RPC to kick off the authorization process
	*/
	opts = append(opts, grpc.StreamInterceptor(
		grpc_middleware.ChainStreamServer(

			grpc_ctxtags.StreamServerInterceptor(),

			grpc_zap.StreamServerInterceptor(logger, zapOpts...),

			grpc_auth.StreamServerInterceptor(authenticate),
		)), grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(

		grpc_ctxtags.UnaryServerInterceptor(),
		grpc_zap.UnaryServerInterceptor(logger, zapOpts...),

		grpc_auth.UnaryServerInterceptor(authenticate),
	)),
		grpc.StatsHandler(&ocgrpc.ServerHandler{}),
	)

	// create a gRPC server
	gsrv := grpc.NewServer(opts...)

	// instantiate your service
	srv, err := newGRPCServer(config)
	if err != nil {
		return nil, err
	}

	// register your service to gRPC server
	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}

type subjectContextKey struct{}

// interceptor that reads the subject out of the client’s cert and writes it to the RPC’s context
// with interceptors, you can intercept and modify the execution of each RPC call,
// allowing you to break the request handling into smaller, reusable chunks
func authenticate(ctx context.Context) (context.Context, error) {
	peer, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.New(
			codes.Unknown,
			"couldn't find peer info",
		).Err()
	}

	if peer.AuthInfo == nil {
		return context.WithValue(ctx, subjectContextKey{}, ""), nil
	}

	tlsInfo := peer.AuthInfo.(credentials.TLSInfo)
	subject := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
	ctx = context.WithValue(ctx, subjectContextKey{}, subject)

	return ctx, nil
}

func subject(ctx context.Context) string {
	return ctx.Value(subjectContextKey{}).(string)
}

type GetServerer interface {
	GetServers() ([]*api.Server, error)
}

func (s *grpcServer) GetServer(ctx context.Context, req *api.GetServersRequest) (*api.GetServersResponse, error) {
	servers, err := s.GetServerer.GetServers()
	if err != nil {
		return nil, err
	}
	return &api.GetServersResponse{Servers: servers}, nil
}
