package transport

import (
	"net"

	"google.golang.org/grpc"

	"github.com/Faithful001/concord.git/internal/rpc"
	"github.com/Faithful001/concord.git/pkg/raft"
	"github.com/Faithful001/concord.git/pkg/raftpb"
)

// Serve registers the node's Raft gRPC service and starts accepting connections on
// addr. It blocks until the listener is closed.
func Serve(node *raft.Node, addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer()
	service := rpc.NewRPCService(node)
	raftpb.RegisterRaftServiceServer(grpcServer, service)

	return grpcServer.Serve(listener)
}