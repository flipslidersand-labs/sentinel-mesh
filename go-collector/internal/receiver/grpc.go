package receiver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/flipslidersand/sentinel-mesh/internal/pb"
	"github.com/flipslidersand/sentinel-mesh/internal/registry"
	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

type server struct {
	pb.UnimplementedSentinelCollectorServer
	st  *store.Store
	reg *registry.Registry
	log *zap.Logger
}

// Register handles agent registration (unary RPC).
func (s *server) Register(_ context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	if err := s.reg.Register(req.NodeId, req.Hostname, req.Ip, req.Version); err != nil {
		return &pb.RegisterResponse{Ok: false, Message: err.Error()}, nil
	}
	s.log.Info("agent registered", zap.String("node_id", req.NodeId), zap.String("host", req.Hostname))
	return &pb.RegisterResponse{Ok: true, Message: "registered"}, nil
}

// StreamEvents receives a bidirectional stream of KernelEvents.
func (s *server) StreamEvents(stream pb.SentinelCollector_StreamEventsServer) error {
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		s.reg.Heartbeat(event.NodeId)

		if storeErr := s.saveEvent(event); storeErr != nil {
			s.log.Error("save event", zap.Error(storeErr))
		}

		if err := stream.Send(&pb.EventAck{Ok: true}); err != nil {
			return err
		}
	}
}

func (s *server) saveEvent(e *pb.KernelEvent) error {
	var typeName string
	var payload any

	switch e.Type {
	case pb.EventType_EXEC:
		typeName = "exec"
		if ex := e.GetExec(); ex != nil {
			payload = ex
		}
	case pb.EventType_TCP:
		typeName = "tcp"
		if t := e.GetTcp(); t != nil {
			payload = t
		}
	case pb.EventType_FILE:
		typeName = "file"
		if f := e.GetFile(); f != nil {
			payload = f
		}
	default:
		typeName = fmt.Sprintf("unknown(%d)", e.Type)
	}

	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	return s.st.SaveEvent(store.Event{
		EventID:   e.EventId,
		NodeID:    e.NodeId,
		Timestamp: time.Unix(0, e.Timestamp).UTC(),
		Type:      typeName,
		Payload:   raw,
	})
}

// Serve starts the gRPC server on addr.
func Serve(addr string, st *store.Store, reg *registry.Registry, log *zap.Logger) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	srv := grpc.NewServer()

	s := &server{st: st, reg: reg, log: log}
	pb.RegisterSentinelCollectorServer(srv, s)

	log.Info("gRPC server listening", zap.String("addr", addr))
	return srv.Serve(lis)
}
