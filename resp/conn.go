package resp

import (
	"bufio"
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
)

// The server identity HELLO replies with.
const (
	serverName    = "rdbc"
	serverVersion = "0.1.0"
)

// defaultUser is the name of the user the AUTH command and the AUTH
// option of HELLO authenticate for.
const defaultUser = "default"

// ErrHijacked is returned by [ResponseWriter.Hijack] when the
// connection was already hijacked.
var ErrHijacked = errors.New("resp: connection has been hijacked")

// ErrNotFlushed indicates that [ResponseWriter.Hijack] was called
// while the reply of the command carries bytes that were written but
// not flushed. This error is only returned by the first Hijack call
// of a connection, the buffered bytes would be lost on a hijacked
// connection. The handler flushes the reply with
// [ResponseWriter.Flush] and calls Hijack again.
var ErrNotFlushed = errors.New("resp: reply not flushed before hijack")

// Conn is the server side of one RESP connection. It reads the
// commands of the client and serves them on a single event loop, one
// command at a time, the reply of a command is flushed before the
// next one is read.
type Conn struct {
	srv   *Server
	nc    net.Conn
	bw    *bufio.Writer
	proto *protocol

	// ctx is the context of the connection, it is canceled when the
	// connection is closed.
	ctx context.Context

	// authed reports whether the connection has authenticated with
	// the password of the server, it starts out true when no password
	// is set.
	authed bool

	// hijacked reports whether a handler took the connection over
	// with [ResponseWriter.Hijack], the event loop stops after the
	// command of the handler and the connection is left to it.
	hijacked bool
}

// newConn constructs the connection of a server over nc, it starts as
// a RESP2 session, the default of a Redis client.
func newConn(srv *Server, nc net.Conn, ctx context.Context) *Conn {
	bw := bufio.NewWriter(nc)
	return &Conn{
		srv:    srv,
		nc:     nc,
		bw:     bw,
		proto:  newProtocol2(nc, bw),
		ctx:    ctx,
		authed: srv.Password == "",
	}
}

// serve runs the event loop of the connection until the client closes
// it, a command fails to decode or a write fails. The connection is
// closed by the caller, a handler that hijacked the connection ends
// the loop with the connection left open for it.
func (c *Conn) serve() error {
	for {
		args, err := c.proto.readCommand()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return err
			}
			fmt.Fprintf(c.bw, "-ERR Protocol error: %s\r\n", err.Error())
			c.bw.Flush()
			return err
		}
		quit, err := c.dispatch(args)
		if err != nil {
			return err
		}
		// The reply of a hijacked command was flushed by its handler,
		// the commands that follow are never read.
		if c.hijacked {
			return nil
		}
		if err := c.proto.flush(); err != nil {
			return err
		}
		if quit {
			return nil
		}
	}
}

// dispatch serves one command, the reserved HELLO, AUTH, RESET and
// QUIT before the handler of the server. A panic of a handler is
// recovered and answered with an error reply, the connection stays
// open.
func (c *Conn) dispatch(args [][]byte) (quit bool, err error) {
	w := &writer{c: c}
	defer func() {
		if r := recover(); r != nil {
			c.srv.logf("resp: panic serving %v: %v", c.nc.RemoteAddr(), r)
			err = w.Proto().WriteToken(Error("ERR internal error"))
		}
	}()

	name := strings.ToUpper(string(args[0]))
	cmd := &Command{ctx: c.ctx, Args: args}
	switch name {
	case "HELLO":
		return false, c.hello(w, cmd)
	case "AUTH":
		return false, c.auth(w, cmd)
	case "RESET":
		return false, c.reset(w)
	case "QUIT":
		if _, err := c.bw.WriteString("+OK\r\n"); err != nil {
			return false, err
		}
		return true, nil
	}
	if c.srv.Password != "" && !c.authed {
		return false, w.Proto().WriteToken(Error("NOAUTH Authentication required"))
	}
	switch name {
	case "PING":
		if _, err := c.bw.WriteString("+PONG\r\n"); err != nil {
			return false, err
		}
		return false, nil
	}
	c.srv.handler().ServeRESP(w, cmd)
	return false, nil
}

// hello implements the HELLO command. Without an argument it replies
// with the information of the server in the protocol of the session.
// With a protocol version it switches the protocol of the connection
// before it replies, AUTH and SETNAME arguments are accepted, the
// AUTH arguments validate like the AUTH command.
func (c *Conn) hello(w *writer, cmd *Command) error {
	current := 2
	if _, ok := w.Proto().(*RESP3Encoder); ok {
		current = 3
	}
	proto := current
	if len(cmd.Args) > 1 {
		p, err := strconv.ParseInt(string(cmd.Args[1]), 10, 64)
		if err != nil || (p != 2 && p != 3) {
			return w.Proto().WriteToken(Error("NOPROTO unsupported protocol version"))
		}
		rest := cmd.Args[2:]
		for i := 0; i < len(rest); i++ {
			switch strings.ToUpper(string(rest[i])) {
			case "AUTH":
				if i+2 >= len(rest) {
					return w.Proto().WriteToken(Error("ERR syntax error in HELLO options"))
				}
				if err := w.Auth(string(rest[i+1]), string(rest[i+2])); err != nil {
					return w.Proto().WriteToken(Error(err.Error()))
				}
				i += 2
			case "SETNAME":
				if i+1 >= len(rest) {
					return w.Proto().WriteToken(Error("ERR syntax error in HELLO options"))
				}
				i++
			default:
				return w.Proto().WriteToken(Error("ERR syntax error in HELLO options"))
			}
		}
		proto = int(p)
	}
	if proto != current {
		if err := c.changeProto(proto); err != nil {
			return err
		}
	}
	return w.writeHelloReply(proto, c.srv.nextConnID())
}

// auth implements the AUTH command, the password of the default user
// alone, or the user and the password.
func (c *Conn) auth(w *writer, cmd *Command) error {
	var user, password string
	args := cmd.Args[1:]
	switch len(args) {
	case 1:
		user, password = defaultUser, string(args[0])
	case 2:
		user, password = string(args[0]), string(args[1])
	default:
		return w.Proto().WriteToken(Error("ERR wrong number of arguments for 'auth' command"))
	}
	if err := w.Auth(user, password); err != nil {
		return w.Proto().WriteToken(Error(err.Error()))
	}
	_, err := c.bw.WriteString("+OK\r\n")
	return err
}

// reset implements the RESET command, the protocol of the session
// goes back to RESP2 and the authentication is reset.
func (c *Conn) reset(w *writer) error {
	if err := c.changeProto(2); err != nil {
		return err
	}
	c.authed = c.srv.Password == ""
	_, err := c.bw.WriteString("+RESET\r\n")
	return err
}

// changeProto switches the protocol of the connection. The input the
// old protocol has already read but not consumed is carried over with
// a handoff, so the commands pipelined behind the switch are decoded
// by the new protocol.
func (c *Conn) changeProto(p int) error {
	if p != 2 && p != 3 {
		return fmt.Errorf("resp: unsupported protocol version %d", p)
	}
	r := c.proto.handoff()
	if p == 3 {
		c.proto = newProtocol3(r, c.bw)
		return nil
	}
	c.proto = newProtocol2(r, c.bw)
	return nil
}

// authenticate validates the user and its password against the
// password of the server and marks the connection authenticated on
// success. The only user of the server is default, the comparison of
// the password runs in constant time.
func (c *Conn) authenticate(user, password string) error {
	if c.srv.Password == "" {
		return errors.New("ERR Client sent AUTH, but no password is set")
	}
	if user != defaultUser {
		return errors.New("WRONGPASS invalid username-password pair or user is disabled")
	}
	if subtle.ConstantTimeCompare([]byte(password), []byte(c.srv.Password)) != 1 {
		return errors.New("WRONGPASS invalid username-password pair or user is disabled")
	}
	c.authed = true
	return nil
}

// writer is the [ResponseWriter] of one command, it writes through
// the protocol of the connection.
type writer struct {
	c *Conn
}

// Proto returns the encoder of the current protocol of the session,
// see [ResponseWriter.Proto].
func (w *writer) Proto() Encoder {
	return w.c.proto.encoder()
}

// ChangeProto switches the protocol of the session, see
// [ResponseWriter.ChangeProto].
func (w *writer) ChangeProto(p int) error {
	return w.c.changeProto(p)
}

// Auth authenticates the connection for the user, see
// [ResponseWriter.Auth].
func (w *writer) Auth(user, password string) error {
	return w.c.authenticate(user, password)
}

// Flush writes the buffered reply to the connection, see
// [ResponseWriter.Flush].
func (w *writer) Flush() error {
	return w.c.proto.flush()
}

// Hijack hands the connection over to the handler, see
// [ResponseWriter.Hijack]. The connection stops being tracked by the
// server, [Server.Close] leaves it to the handler.
func (w *writer) Hijack() (net.Conn, error) {
	if w.c.hijacked {
		return nil, ErrHijacked
	}
	if w.c.bw.Buffered() != 0 {
		return nil, ErrNotFlushed
	}
	w.c.hijacked = true
	w.c.srv.trackConn(w.c.nc, false)
	return w.c.nc, nil
}

// RemoteAddr returns the address of the peer of the connection, see
// [ResponseWriter.RemoteAddr].
func (w *writer) RemoteAddr() net.Addr {
	return w.c.nc.RemoteAddr()
}

// writeHelloReply writes the information map the HELLO command
// replies with, in the tokens of the protocol of the session.
func (w *writer) writeHelloReply(proto int, id uint64) error {
	fields := []struct {
		name  string
		value string
		num   int64
		isNum bool
	}{
		{"server", serverName, 0, false},
		{"version", serverVersion, 0, false},
		{"proto", "", int64(proto), true},
		{"id", "", int64(id), true},
		{"mode", "standalone", 0, false},
		{"role", "master", 0, false},
	}
	switch e := w.Proto().(type) {
	case *RESP2Encoder:
		if err := e.WriteToken(ArrayLen((len(fields) + 1) * 2)); err != nil {
			return err
		}
		for _, f := range fields {
			if err := e.WriteToken(Bytes([]byte(f.name))); err != nil {
				return err
			}
			if err := writeHelloValue2(e, f); err != nil {
				return err
			}
		}
		if err := e.WriteToken(Bytes([]byte("modules"))); err != nil {
			return err
		}
		return e.WriteToken(ArrayLen(0))
	case *RESP3Encoder:
		if err := e.WriteToken(MapLen(len(fields) + 1)); err != nil {
			return err
		}
		for _, f := range fields {
			if err := e.WriteToken(Bytes([]byte(f.name))); err != nil {
				return err
			}
			if err := writeHelloValue3(e, f); err != nil {
				return err
			}
		}
		if err := e.WriteToken(Bytes([]byte("modules"))); err != nil {
			return err
		}
		return e.WriteToken(ArrayLen(0))
	}
	panic("resp: unknown protocol encoder")
}

// writeHelloValue2 writes the value of one field of the HELLO reply,
// a bulk string or a RESP integer.
func writeHelloValue2(e *RESP2Encoder, f helloField) error {
	if f.isNum {
		return e.WriteToken(Int(f.num))
	}
	return e.WriteToken(Bytes([]byte(f.value)))
}

// writeHelloValue3 writes the value of one field of the HELLO reply,
// a blob string or a RESP number.
func writeHelloValue3(e *RESP3Encoder, f helloField) error {
	if f.isNum {
		return e.WriteToken(Int(f.num))
	}
	return e.WriteToken(Bytes([]byte(f.value)))
}

// helloField is one field of the information map HELLO replies with,
// its value is either the value string or the num integer.
type helloField struct {
	name  string
	value string
	num   int64
	isNum bool
}
