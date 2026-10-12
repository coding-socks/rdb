package resp

import (
	"context"
	"crypto/tls"
	"errors"
	"log"
	"net"
	"sync"
	"sync/atomic"
)

// ErrServerClosed indicates that the server stopped serving because
// [Server.Close] closed it.
var ErrServerClosed = errors.New("resp: server closed")

// Server serves RESP connections.
type Server struct {
	// Handler serves the commands of every connection, typically a
	// [ServeMux]. A nil Handler replies to every command with the
	// error of an unknown command.
	Handler Handler

	// Addr is the address ListenAndServe listens on, in the
	// host:port form of net.Listen. An empty Addr listens on :6379,
	// the port of Redis.
	Addr string

	// Password is the password of the default user, the user AUTH
	// and the AUTH option of HELLO authenticate for. Every other
	// command of a connection that did not authenticate is answered
	// with NOAUTH while a password is set, an empty password
	// disables the authentication.
	Password string

	// ConnContext optionally wraps the context of a new connection,
	// the context its commands carry, to add the values the handlers
	// of the server share per connection. It is called once per
	// connection before its event loop starts.
	ConnContext func(ctx context.Context, nc net.Conn) context.Context

	// TLSConfig optionally provides the TLS configuration ServeTLS
	// builds on, its Certificates are used in addition to the ones
	// loaded from the certificate files of the call.
	TLSConfig *tls.Config

	// ErrorLog names the logger the panics of the handlers are
	// reported to, nil reports them to the standard logger.
	ErrorLog *log.Logger

	// connSeq counts the connections the server has served, the id
	// HELLO replies with.
	connSeq atomic.Uint64

	mu        sync.Mutex                // guards listeners and conns
	closed    atomic.Bool               // set by Close
	listeners map[net.Listener]struct{} // the listeners the server serves on
	conns     map[net.Conn]struct{}     // the connections the server serves
}

// Serve accepts connections on the listener and serves each of them
// on its own goroutine. It returns [ErrServerClosed] when
// [Server.Close] closed the server and the error of the listener
// when it stops accepting otherwise. The listener is closed once, by
// the return of Serve or by [Server.Close].
func (srv *Server) Serve(l net.Listener) error {
	ol := &onceCloseListener{Listener: l}
	defer ol.Close()
	if !srv.trackListener(ol, true) {
		return ErrServerClosed
	}
	defer srv.trackListener(ol, false)
	for {
		nc, err := ol.Accept()
		if err != nil {
			if srv.closed.Load() {
				return ErrServerClosed
			}
			return err
		}
		go srv.ServeConn(nc)
	}
}

// ServeConn serves one connection of the server. It runs the event
// loop of the connection until the client closes it, a command fails
// to decode or a write fails, and closes the connection before it
// returns, a connection a handler hijacked with
// [ResponseWriter.Hijack] is left open for its handler. It serves
// nothing and returns [ErrServerClosed] when [Server.Close] closed
// the server. The context of the commands of the connection is
// canceled when it returns.
func (srv *Server) ServeConn(nc net.Conn) error {
	if !srv.trackConn(nc, true) {
		return ErrServerClosed
	}
	defer srv.trackConn(nc, false)
	ctx := context.Background()
	if srv.ConnContext != nil {
		ctx = srv.ConnContext(ctx, nc)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	c := newConn(srv, nc, ctx)
	err := c.serve()
	if c.hijacked {
		return err
	}
	nc.Close()
	return err
}

// ServeTLS accepts TLS encrypted connections on the listener, the
// certificate is loaded from the named files and added to the
// certificates of [Server.TLSConfig]. It serves the connections like
// [Server.Serve] does.
func (srv *Server) ServeTLS(l net.Listener, certFile, keyFile string) error {
	cfg := srv.TLSConfig.Clone()
	if cfg == nil {
		cfg = &tls.Config{}
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return err
	}
	cfg.Certificates = append(cfg.Certificates, cert)
	return srv.Serve(tls.NewListener(l, cfg))
}

// Close closes the listeners the server serves on and the
// connections it serves. [Server.Serve] and [Server.ServeConn] return
// [ErrServerClosed] after it, the command a connection was serving
// runs to its end. A connection a handler hijacked is not closed, it
// belongs to its handler. Close returns the error of closing the
// listeners.
func (srv *Server) Close() error {
	srv.closed.Store(true)
	srv.mu.Lock()
	defer srv.mu.Unlock()
	var err error
	for l := range srv.listeners {
		if e := l.Close(); e != nil && err == nil {
			err = e
		}
		delete(srv.listeners, l)
	}
	for nc := range srv.conns {
		nc.Close()
		delete(srv.conns, nc)
	}
	return err
}

// handler returns the handler of the server, an empty mux when none
// is set, so every command receives the reply of an unknown command.
func (srv *Server) handler() Handler {
	if srv.Handler == nil {
		return &ServeMux{}
	}
	return srv.Handler
}

// nextConnID returns the id of the connection, the id HELLO replies
// with.
func (srv *Server) nextConnID() uint64 {
	return srv.connSeq.Add(1) - 1
}

// logf reports a panic of a handler to the error log of the server.
func (srv *Server) logf(format string, args ...any) {
	if srv.ErrorLog != nil {
		srv.ErrorLog.Printf(format, args...)
		return
	}
	log.Printf(format, args...)
}

// trackListener records the listener the server serves on or drops
// it. It reports false when a listener is added to a closed server.
func (srv *Server) trackListener(l net.Listener, add bool) bool {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if add {
		if srv.closed.Load() {
			return false
		}
		if srv.listeners == nil {
			srv.listeners = make(map[net.Listener]struct{})
		}
		srv.listeners[l] = struct{}{}
		return true
	}
	delete(srv.listeners, l)
	return true
}

// trackConn records the connection the server serves or drops it. It
// reports false when a connection is added to a closed server.
func (srv *Server) trackConn(nc net.Conn, add bool) bool {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if add {
		if srv.closed.Load() {
			return false
		}
		if srv.conns == nil {
			srv.conns = make(map[net.Conn]struct{})
		}
		srv.conns[nc] = struct{}{}
		return true
	}
	delete(srv.conns, nc)
	return true
}

// ListenAndServe listens on the TCP network address of the server,
// [Server.Addr], and serves the connections with it. An empty Addr
// listens on :6379, the port of Redis. It returns the error of the
// listen call when the address cannot be listened on and the error
// of [Server.Serve] otherwise.
func (srv *Server) ListenAndServe() error {
	addr := srv.Addr
	if addr == "" {
		addr = ":6379"
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return srv.Serve(ln)
}

// ListenAndServe listens on the TCP network address addr and serves
// the connections with the handler. It is a shortcut for a server
// with the handler and no other configuration, see
// [Server.ListenAndServe].
func ListenAndServe(addr string, handler Handler) error {
	srv := &Server{Addr: addr, Handler: handler}
	return srv.ListenAndServe()
}

// onceCloseListener wraps a net.Listener so it is closed once: the
// calls of Close behind the first return its error.
type onceCloseListener struct {
	net.Listener
	once     sync.Once
	closeErr error
}

func (oc *onceCloseListener) Close() error {
	oc.once.Do(oc.close)
	return oc.closeErr
}

func (oc *onceCloseListener) close() { oc.closeErr = oc.Listener.Close() }
