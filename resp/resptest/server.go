// Package resptest provides a RESP server for use in end-to-end
// tests: [NewServer] serves the handler of a test on a local address
// of the test, [Server.Client] dials the connections of its clients
// and [Server.Close] shuts it down at the cleanup of the test.
package resptest

import (
	"net"
	"net/url"
	"sync"
	"testing"

	"github.com/coding-socks/rdb/resp"
)

// Server is a RESP server for use in end-to-end tests. It serves the
// commands of its connections with its handler on a local address of
// the test, [Server.Client] dials the connections of its clients and
// [Server.Close] shuts it down. A Server is started by [NewServer],
// the cleanup of the test closes it.
type Server struct {
	// Addr is the host:port address the server serves on.
	Addr string

	srv     *resp.Server   // the server that serves the handler
	t       testing.TB     // the test the server serves for
	mu      sync.Mutex     // guards closed and clients
	closed  bool           // the server was closed
	clients []*resp.Client // the connections the clients of the test dialed
}

// NewServer starts and returns a new [Server] that serves the
// commands of its connections with the handler on a local address of
// the test. A nil handler replies to every command with the error of
// an unknown command. The cleanup of the test closes the server.
func NewServer(t testing.TB, handler resp.Handler) *Server {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("resptest: failed to listen on a port: ", err)
	}
	s := &Server{
		Addr: ln.Addr().String(),
		srv:  &resp.Server{Handler: handler},
		t:    t,
	}
	go s.srv.Serve(ln)
	t.Cleanup(func() {
		s.Close()
	})
	return s
}

// Client dials the server and returns the connection of a client,
// every call dials a connection of its own. [Server.Close] closes the
// connections it dialed.
func (s *Server) Client() *resp.Client {
	u, err := url.Parse("redis://" + s.Addr)
	if err != nil {
		s.t.Fatal(err)
	}
	conn, err := resp.Dial(s.t.Context(), u, 0)
	if err != nil {
		s.t.Fatal(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clients = append(s.clients, conn)
	return conn
}

// Close closes the server and the connections of the clients it
// dialed, the command a connection was serving runs to its end. The
// cleanup of the test closes the server behind the test, the second
// call leaves it closed.
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	clients := s.clients
	s.clients = nil
	s.mu.Unlock()
	s.srv.Close()
	for _, c := range clients {
		c.Close()
	}
	return nil
}
