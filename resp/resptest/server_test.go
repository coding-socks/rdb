package resptest

import (
	"net"
	"testing"

	"github.com/coding-socks/rdb/resp"
)

func TestNewServer(t *testing.T) {
	// The server serves the handler on a local address of the
	// test, the reply of a command comes from the handler the test
	// registered.
	mux := resp.NewServeMux()
	mux.HandleFunc("ECHO", func(w resp.ResponseWriter, cmd *resp.Command) {
		w.Proto().WriteToken(resp.String(string(cmd.Args[1])))
	})
	srv := NewServer(t, mux)
	if _, err := net.ResolveTCPAddr("tcp", srv.Addr); err != nil {
		t.Fatalf("Addr = %q, want a host:port address: %v", srv.Addr, err)
	}
	tok, err := srv.Client().Exec(t.Context(), []byte("ECHO"), []byte("hello"))
	if err != nil {
		t.Fatalf("Exec() error = %v", err)
	}
	if tok.Kind() != resp.KindString || tok.String() != "hello" {
		t.Errorf("ECHO reply = %s %v, want hello", tok.Kind(), tok.Val())
	}
}

func TestServer_Client(t *testing.T) {
	t.Run("every call dials a connection of its own", func(t *testing.T) {
		// The connections of the clients share the server, the
		// handler answers the command of each of them.
		mux := resp.NewServeMux()
		mux.HandleFunc("ECHO", func(w resp.ResponseWriter, cmd *resp.Command) {
			w.Proto().WriteToken(resp.String(string(cmd.Args[1])))
		})
		srv := NewServer(t, mux)
		c1, c2 := srv.Client(), srv.Client()
		if c1 == c2 {
			t.Error("Client() returned the same connection twice")
		}
		for _, c := range []*resp.Client{c1, c2} {
			tok, err := c.Exec(t.Context(), []byte("ECHO"), []byte("hello"))
			if err != nil {
				t.Fatalf("Exec() error = %v", err)
			}
			if tok.Kind() != resp.KindString || tok.String() != "hello" {
				t.Errorf("ECHO reply = %s %v, want hello", tok.Kind(), tok.Val())
			}
		}
	})
}

func TestServer_Close(t *testing.T) {
	t.Run("the server and the clients are closed", func(t *testing.T) {
		srv := NewServer(t, nil)
		c := srv.Client()
		if _, err := c.Exec(t.Context(), []byte("PING")); err != nil {
			t.Fatalf("Exec() error = %v", err)
		}
		srv.Close()
		// The connection of the client and the listener of the
		// server end with the close.
		if _, err := c.Exec(t.Context(), []byte("PING")); err == nil {
			t.Error("Exec() after Close() did not fail")
		}
		if nc, err := net.Dial("tcp", srv.Addr); err == nil {
			nc.Close()
			t.Error("the server accepted a connection after Close()")
		}
	})

	t.Run("the close of the cleanup closes a closed server", func(t *testing.T) {
		// The test closes the server itself, the cleanup of the
		// test repeats the close.
		srv := NewServer(t, nil)
		srv.Close()
		srv.Close()
	})
}
