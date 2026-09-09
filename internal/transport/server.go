package transport

import (
	"net"
	"net/rpc"

	"github.com/Faithful001/concord.git/internal/raft"
	rpcSvc "github.com/Faithful001/concord.git/internal/rpc"
)

// Serve registers the node's RPC methods and starts accepting connections on
// addr.  It blocks until the listener is closed.
//
// A fresh rpc.Server is used (rather than the package-level default) so that
// multiple nodes can be started in the same process without "already
// registered" panics — useful for integration tests.
func Serve(node *raft.Node, addr string) error {
	srv := rpc.NewServer()
	service := rpcSvc.NewRPCService(node)
	if err := srv.Register(service); err != nil {
		return err
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	srv.Accept(listener) // blocks until listener is closed
	return nil
}