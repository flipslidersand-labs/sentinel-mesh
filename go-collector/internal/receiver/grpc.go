package receiver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/flipslidersand/sentinel-mesh/internal/alerting"
	"github.com/flipslidersand/sentinel-mesh/internal/anomaly"
	"github.com/flipslidersand/sentinel-mesh/internal/otel"
	"github.com/flipslidersand/sentinel-mesh/internal/pb"
	"github.com/flipslidersand/sentinel-mesh/internal/registry"
	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

type server struct {
	pb.UnimplementedSentinelCollectorServer
	st       *store.Store
	reg      *registry.Registry
	engine   *alerting.Engine
	detector *anomaly.Detector
	metrics  *otel.MetricsProvider
	tracer   trace.Tracer
	log      *zap.Logger
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

		storedEvent := store.Event{
			EventID:   event.EventId,
			NodeID:    event.NodeId,
			Timestamp: time.Unix(0, event.Timestamp).UTC(),
			Type:      s.eventTypeName(event),
			Payload:   s.eventPayload(event),
		}

		if storeErr := s.st.SaveEvent(storedEvent); storeErr != nil {
			s.log.Error("save event", zap.Error(storeErr))
		}

		// Phase 6: record event metric
		if s.metrics != nil {
			s.metrics.RecordEvent(storedEvent.Type, storedEvent.NodeID)
		}

		// Phase 5: evaluate alerts
		if s.engine != nil {
			_, span := s.tracer.Start(stream.Context(), "evaluate_alerts",
				trace.WithAttributes(
					attribute.String("event.id", storedEvent.EventID),
					attribute.String("event.type", storedEvent.Type),
					attribute.String("node.id", storedEvent.NodeID),
				),
			)
			defer span.End()

			alerts := s.engine.Evaluate(storedEvent)
			for _, alert := range alerts {
				if err := s.st.SaveAlert(alert); err != nil {
					s.log.Error("save alert", zap.Error(err))
				}

				// Phase 6: record alert metric and trace
				if s.metrics != nil {
					s.metrics.RecordAlert(alert.RuleID, alert.Severity, alert.NodeID)
				}
				span.AddEvent("alert_triggered", trace.WithAttributes(
					attribute.String("rule.id", alert.RuleID),
					attribute.String("severity", alert.Severity),
					attribute.String("message", alert.Message),
				))
			}
		}

		// Phase 7: anomaly detection feed
		if s.detector != nil {
			s.detector.Feed(storedEvent.NodeID, storedEvent.Type)
		}

		if err := stream.Send(&pb.EventAck{Ok: true}); err != nil {
			return err
		}
	}
}

func (s *server) eventTypeName(e *pb.KernelEvent) string {
	switch e.Type {
	case pb.EventType_EXEC:
		return "exec"
	case pb.EventType_TCP:
		return "tcp"
	case pb.EventType_FILE:
		return "file"
	default:
		return fmt.Sprintf("unknown(%d)", e.Type)
	}
}

func (s *server) eventPayload(e *pb.KernelEvent) json.RawMessage {
	var payload any

	switch e.Type {
	case pb.EventType_EXEC:
		if ex := e.GetExec(); ex != nil {
			payload = ex
		}
	case pb.EventType_TCP:
		if t := e.GetTcp(); t != nil {
			payload = t
		}
	case pb.EventType_FILE:
		if f := e.GetFile(); f != nil {
			payload = f
		}
	}

	raw, _ := json.Marshal(payload)
	return raw
}

// Serve starts the gRPC server on addr.
func Serve(addr string, st *store.Store, reg *registry.Registry, engine *alerting.Engine,
	detector *anomaly.Detector, metrics *otel.MetricsProvider, tracer trace.Tracer, log *zap.Logger) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	srv := grpc.NewServer()

	s := &server{st: st, reg: reg, engine: engine, detector: detector, metrics: metrics, tracer: tracer, log: log}
	pb.RegisterSentinelCollectorServer(srv, s)

	log.Info("gRPC server listening", zap.String("addr", addr))
	return srv.Serve(lis)
}
