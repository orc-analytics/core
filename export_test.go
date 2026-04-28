// Bridge package that implements useful mocking functionality for
// Orca processors

package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/orca-telemetry/contract/go"
)

type mockOrcaProcessorServer struct {
	pb.UnimplementedOrcaProcessorServer
}

// ExecuteDagPart implements the streaming RPC for DAG execution
func (s *mockOrcaProcessorServer) ExecuteDagPart(req *pb.ExecutionRequest, stream pb.OrcaProcessor_ExecuteDagPartServer) error {
	slog.Debug("Received ExecuteDagPart request", "exec_id", req.GetExecId())

	// simulate processing each algorithm in the request
	for _, execution := range req.GetAlgorithmExecutions() {

		// create a mock result for this algorithm
		result := &pb.ExecutionResult{
			ExecId: req.GetExecId(),
			AlgorithmResult: &pb.AlgorithmResult{
				Algorithm: execution.GetAlgorithm(),
				Result: &pb.Result{
					Status: pb.ResultStatus_RESULT_STATUS_SUCEEDED,
					ResultData: &pb.Result_SingleValue{
						SingleValue: 0},
					Timestamp: req.GetWindow().GetTimeFrom().GetSeconds(),
				},
			},
		}

		// stream the result back
		if err := stream.Send(result); err != nil {
			slog.Error("error sending result", "error", err)
			return status.Errorf(codes.Internal, "failed to send result: %v", err)
		}

	}

	slog.Debug("completed ExecuteDagPart", "exec_id", req.GetExecId())
	return nil
}

// HealthCheck implements the health check RPC
func (s *mockOrcaProcessorServer) HealthCheck(ctx context.Context, req *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {
	slog.Debug("recieved HealthCheck request", "timestamp", req.GetTimestamp())

	response := &pb.HealthCheckResponse{
		Status:  pb.HealthCheckResponse_STATUS_SERVING,
		Message: "Mock processor is healthy and ready",
		Metrics: &pb.ProcessorMetrics{
			ActiveTasks:   0,
			MemoryBytes:   0,
			CpuPercent:    0.0,
			UptimeSeconds: 0,
		},
	}

	return response, nil
}

// StartMockOrcaProcessor starts a mock gRPC server implementing OrcaProcessor
func StartMockOrcaProcessor(port int) (*grpc.Server, net.Listener, error) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterOrcaProcessorServer(s, &mockOrcaProcessorServer{})

	go func() {
		slog.Debug("mock OrcaProcessor server listening", "port", port)
		if err := s.Serve(lis); err != nil {
			log.Printf("Server exited with error: %v", err)
		}
	}()

	// give the server a moment to start
	time.Sleep(100 * time.Millisecond)

	return s, lis, nil
}
