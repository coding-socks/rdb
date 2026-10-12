package resp

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/url"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/coding-socks/rdb"
)

// Client is a raw RESP connection to a Redis compatible server.
// The commands are written through the encoder and the replies are
// read with the decoder, the underlying network connection is exposed
// for the deadline and close handling of the caller.
type Client struct {
	// C is the underlying network connection.
	C net.Conn
	// E writes the commands of the connection.
	E Encoder
	// D reads the replies of the connection.
	D Decoder

	proto   int
	dialect rdb.Dialect
	u       *url.URL
}

type dialer interface {
	DialContext(ctx context.Context, network, addr string) (net.Conn, error)
}

var (
	schemes       = []string{"redis", "keydb", "valkey", "rediss", "keydbs", "valkeys"}
	secureSchemes = []string{"rediss", "keydbs", "valkeys"}
	dialects      = map[string]rdb.Dialect{
		"keydb":   rdb.KeyDBDialect{},
		"keydbs":  rdb.KeyDBDialect{},
		"redis":   rdb.RedisDialect{},
		"rediss":  rdb.RedisDialect{},
		"valkey":  rdb.ValkeyDialect{},
		"valkeys": rdb.ValkeyDialect{},
	}
)

// Dial connects to the server of the URL and switches the connection
// to the protocol p: 0 selects RESP2, the versions 2 and 3 select
// their own. The scheme determines whether the connection is TLS
// encrypted: redis://, keydb:// and valkey:// dial plain TCP, their s
// variants dial TLS. The query parameter insecure=true skips the
// verification of the TLS certificate of the server. The HELLO of
// the handshake authenticates the user of the url with the AUTH
// arguments, without a username it authenticates the default user,
// and names the connection rdbc. The context ends the read of the
// handshake reply. A failed dial closes the connection it opened.
func Dial(ctx context.Context, u *url.URL, p int) (*Client, error) {
	if p != 0 && p != 2 && p != 3 {
		return nil, fmt.Errorf("resp: unsupported protocol version %d", p)
	}
	if !slices.Contains(schemes, u.Scheme) {
		return nil, fmt.Errorf("resp: unsupported scheme %q", u.Scheme)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("resp: missing host in connection string %q", u.String())
	}
	if u.Port() == "" {
		u.Host = net.JoinHostPort(u.Hostname(), "6379")
	}

	if p == 0 {
		p = 2
	}
	var (
		raw    net.Conn
		err    error
		d      dialer
		secure = slices.Contains(secureSchemes, u.Scheme)
	)
	if secure {
		host, _, _ := net.SplitHostPort(u.Host)
		insecure := false
		if q := u.Query().Get("insecure"); q != "" {
			insecure, err = strconv.ParseBool(q)
			if err != nil {
				return nil, fmt.Errorf("resp: invalid insecure value %q", q)
			}
		}
		d = &tls.Dialer{
			NetDialer: &net.Dialer{Timeout: 5 * time.Second},
			Config: &tls.Config{
				ServerName:         host,
				InsecureSkipVerify: insecure,
			},
		}
	} else {
		d = &net.Dialer{Timeout: 5 * time.Second}
	}

	raw, err = d.DialContext(ctx, "tcp", u.Host)
	if err != nil {
		return nil, err
	}

	conn := &Client{proto: 2, u: u}
	conn.dialect = dialects[u.Scheme]
	conn.C = raw
	conn.D = NewRESP3Decoder(conn.C)
	conn.E = NewRESP3Encoder(conn.C)
	if err := conn.Hello(ctx, p, u.User); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

// Proto returns the protocol version the server switched the
// connection to in the handshake of the dial, 2 or 3.
func (c *Client) Proto() int {
	return c.proto
}

// URL returns the connection string the connection was dialed with.
func (c *Client) URL() *url.URL {
	return c.u
}

// Dialect returns the dialect of the server the connection was dialed
// on.
func (c *Client) Dialect() rdb.Dialect {
	return c.dialect
}

func (c *Client) SetDialect(d rdb.Dialect) {
	c.dialect = d
}

// Hello asks the server to switch the protocol of the connection to
// p, 2 or 3, and authenticates the user of the connection string with
// the AUTH arguments of HELLO, without a username AUTH authenticates
// the default user. The connection is named rdbc. An error reply or
// an unexpected reply of the server fails the handshake, the elements
// of the reply are read to the end. [Dial] runs it as the handshake
// of the connection it opens.
func (c *Client) Hello(ctx context.Context, p int, user *url.Userinfo) error {
	if p != 2 && p != 3 {
		return fmt.Errorf("resp: unsupported protocol version %d", p)
	}
	args := [][]byte{[]byte("HELLO"), strconv.AppendInt(nil, int64(p), 10)}
	if user != nil {
		if password, hasPassword := user.Password(); hasPassword {
			name := user.Username()
			if name == "" {
				name = "default"
			}
			args = append(args, []byte("AUTH"), []byte(name), []byte(password))
		}
	}
	args = append(args, []byte("SETNAME"), []byte("rdbc"))
	if err := c.Write(args...); err != nil {
		return err
	}
	t, err := c.ReadValue(ctx)
	if err != nil {
		return err
	}
	switch t.Kind() {
	case KindError:
		return t.Err()
	case KindMap, KindAttribute, KindArray:
		c.proto = p
		return SkipReply(c.D, t)
	}
	return errors.New("resp: unexpected HELLO reply")
}

// SkipReply reads the elements of an aggregate reply to the end, a
// nested aggregate is read whole, the parts of an unbound blob string
// included. A reply the caller ignores must be read to its end or the
// connection desyncs, the decoder is left on the boundary of the next
// reply.
func SkipReply(d Decoder, t Token) error {
	switch t.Kind() {
	case KindMap, KindAttribute, KindArray, KindSet, KindPush:
		next, errf := t.Iter(d)
		for e := range next {
			if err := SkipReply(d, e); err != nil {
				return err
			}
		}
		return errf()
	case KindBlobString:
		next, errf := t.Iter(d)
		for range next {
		}
		return errf()
	}
	return nil
}

func (c *Client) write(args ...[]byte) error {
	if err := c.E.WriteToken(ArrayLen(len(args))); err != nil {
		return err
	}
	for i := range args {
		if err := c.E.WriteToken(Bytes(args[i])); err != nil {
			return err
		}
	}
	return nil
}

// Write queues a command with the given arguments and flushes it.
func (c *Client) Write(args ...[]byte) error {
	if err := c.write(args...); err != nil {
		return err
	}
	return c.E.Flush()
}

// Exec writes the command of the given arguments to the connection
// and reads the first token of its reply. The elements of an
// aggregate reply are left on the connection, the caller reads them
// with [Token.Iter] or skips them with [SkipReply]. An error reply is
// the [KindError] token it is. The context ends the read of the
// reply.
func (c *Client) Exec(ctx context.Context, args ...[]byte) (Token, error) {
	if err := c.Write(args...); err != nil {
		return Token{}, err
	}
	return c.ReadValue(ctx)
}

// Pipe is the pipeline of a [Client], see [Client.Pipe].
type Pipe struct {
	c *Client
}

// Pipe returns a pipeline of the connection: the commands written to
// it are buffered, [Pipe.Flush] writes them in one round trip. The
// replies are read with [Client.ReadValue] in the order the commands
// were written.
func (c *Client) Pipe() *Pipe {
	return &Pipe{c: c}
}

// Write queues a command with the given arguments in the pipeline.
func (p *Pipe) Write(args ...[]byte) error {
	return p.c.write(args...)
}

// Flush writes the commands queued in the pipeline to the server.
func (p *Pipe) Flush() error {
	return p.c.E.Flush()
}

// ReadValue reads one message of the connection and returns it.
// The read is aborted when the context is done.
func (c *Client) ReadValue(ctx context.Context) (Token, error) {
	if err := ctx.Err(); err != nil {
		return Token{}, err
	}
	// A blocking read cannot be canceled directly, the watch aborts it
	// with a read deadline when the context is done.
	stop := AbortReadOn(ctx, c.C)
	t, err := c.D.ReadToken()
	stop()
	if netErr, ok := errors.AsType[net.Error](err); ok && netErr.Timeout() && ctx.Err() != nil {
		// The watch aborted the read because the context is done.
		return Token{}, ctx.Err()
	}
	return t, err
}

// AbortReadOn starts a goroutine that aborts a blocking read on c when
// the context is done. The returned function stops the watching, it
// blocks until the goroutine exited, so the caller can clear the read
// deadline without a race.
func AbortReadOn(ctx context.Context, c net.Conn) (stop func()) {
	stopCh := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			c.SetReadDeadline(time.Now())
		case <-stopCh:
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			close(stopCh)
			c.SetReadDeadline(time.Time{})
		})
	}
}

// Close closes the connection.
func (c *Client) Close() error {
	return c.C.Close()
}
