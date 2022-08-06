package server

import (
	"context"

	api "github.com/denisschmidt/golog/api/v1"
	"google.golang.org/grpc"
)

type Config struct {
	CommitLog CommitLog
}

// need for dependency inversion
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
	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil
}

// receive record
func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
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

func NewGRPCServer(config *Config) (*grpc.Server, error) {
	// create a gRPC server
	gsrv := grpc.NewServer()
	// instantiate your service
	srv, err := newGRPCServer(config)
	if err != nil {
		return nil, err
	}
	// register your service to gRPC server
	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}
