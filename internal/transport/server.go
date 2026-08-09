package transport

import (
	"net"
	"net/rpc"

	"github.com/Faithful001/concord.git/internal/raft"
	rpcPath "github.com/Faithful001/concord.git/internal/rpc"
)

// Serve registers and listens for incoming Raft RPCs on addr.
func Serve(node *raft.Node, addr string) error {
	service := rpcPath.NewRPCService(node)
	if err := rpc.Register(service); err != nil {
		return err
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	rpc.Accept(listener) // blocks, handling connections until listener (net.Listen) closes
	return nil
}