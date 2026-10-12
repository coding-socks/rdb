package resp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestToken_Iter(t *testing.T) {
	tests := []struct {
		name     string
		data     string
		kind     Kind
		elements []string
	}{
		{
			name:     "bound array",
			data:     "*3\r\n:1\r\n:2\r\n:3\r\n",
			kind:     KindArray,
			elements: []string{"1 number", "2 number", "3 number"},
		},
		{
			name:     "unbound array",
			data:     "*?\r\n:1\r\n:2\r\n.\r\n",
			kind:     KindArray,
			elements: []string{"1 number", "2 number"},
		},
		{
			name: "unbound empty array",
			data: "*?\r\n.\r\n",
			kind: KindArray,
		},
		{
			name:     "bound map",
			data:     "%2\r\n+a\r\n:1\r\n+b\r\n:2\r\n",
			kind:     KindMap,
			elements: []string{"a string", "1 number", "b string", "2 number"},
		},
		{
			name:     "unbound map",
			data:     "%?\r\n+a\r\n:1\r\n+b\r\n:2\r\n.\r\n",
			kind:     KindMap,
			elements: []string{"a string", "1 number", "b string", "2 number"},
		},
		{
			name:     "unbound set",
			data:     "~?\r\n+orange\r\n+apple\r\n.\r\n",
			kind:     KindSet,
			elements: []string{"orange string", "apple string"},
		},
		{
			name:     "unbound attribute",
			data:     "|?\r\n+key\r\n:1\r\n.\r\n",
			kind:     KindAttribute,
			elements: []string{"key string", "1 number"},
		},
		{
			name:     "bound push",
			data:     ">2\r\n+a\r\n+b\r\n",
			kind:     KindPush,
			elements: []string{"a string", "b string"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := NewRESP3Decoder(strings.NewReader(tt.data))
			tok, err := d.ReadToken()
			if err != nil {
				t.Fatalf("ReadToken() error = %v", err)
			}
			if tok.Kind() != tt.kind {
				t.Fatalf("ReadToken() kind = %s, want = %s", tok.Kind(), tt.kind)
			}
			var got []string
			next, errf := tok.Iter(d)
			for e := range next {
				if e.Kind() == KindStringPart {
					got = append(got, string(e.Bytes())+" "+e.Kind().String())
					continue
				}
				got = append(got, fmt.Sprintf("%v %s", e.Val(), e.Kind()))
			}
			if err := errf(); err != nil {
				t.Fatalf("Iter() error = %v", err)
			}
			if !reflect.DeepEqual(got, tt.elements) {
				t.Errorf("Iter() = %v, want = %v", got, tt.elements)
			}
		})
	}

	// The error of the iteration is carried by the function next to
	// the iterator, a read that hits the end of the stream reports
	// it after the loop.
	t.Run("read error", func(t *testing.T) {
		d := NewRESP3Decoder(strings.NewReader("*2\r\n:1\r\n"))
		tok, err := d.ReadToken()
		if err != nil {
			t.Fatalf("ReadToken() error = %v", err)
		}
		var got []string
		next, errf := tok.Iter(d)
		for e := range next {
			got = append(got, fmt.Sprintf("%v", e.Val()))
		}
		if err := errf(); err == nil {
			t.Error("Iter() error = nil, want the error of the missing element")
		}
		if len(got) != 1 {
			t.Errorf("Iter() = %v, want the one element of the stream", got)
		}
	})

	// Iter on a token that is not an aggregate panics, the elements
	// are read with the same decoder.
	t.Run("not iterable", func(t *testing.T) {
		d := NewRESP3Decoder(strings.NewReader(":1\r\n"))
		tok, err := d.ReadToken()
		if err != nil {
			t.Fatalf("ReadToken() error = %v", err)
		}
		defer func() {
			if recover() == nil {
				t.Error("Iter() on a number did not panic")
			}
		}()
		tok.Iter(d)
	})
}

func TestRESP3Decoder_Handoff(t *testing.T) {
	r := strings.NewReader("$5\r\nhello\r\n*1\r\n$4\r\nPING\r\n")
	d := NewRESP3Decoder(r)
	tok, err := d.ReadToken()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := tok.String(), "hello"; got != want {
		t.Fatalf("ReadToken() = %q, want %q", got, want)
	}

	d2 := NewRESP3Decoder(d.Handoff())
	tok, err = d2.ReadToken()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := tok.Int(), int64(1); got != want {
		t.Errorf("ReadToken() after Handoff = %d, want %d", got, want)
	}
	tok, err = d2.ReadToken()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := tok.String(), "PING"; got != want {
		t.Errorf("ReadToken() after Handoff = %q, want %q", got, want)
	}
	if _, err := d2.ReadToken(); err != io.EOF {
		t.Errorf("ReadToken() after Handoff err = %v, want %v", err, io.EOF)
	}
}

// recorder is a [ResponseWriter] writing through a RESP2 encoder into
// its buffer.
type recorder struct {
	buf bytes.Buffer
	e   *RESP2Encoder
}

func newRecorder() *recorder {
	w := &recorder{}
	w.e = NewRESP2Encoder(&w.buf)
	return w
}

func (w *recorder) Proto() Encoder { return w.e }

func (w *recorder) ChangeProto(int) error { return nil }

func (w *recorder) Flush() error { return w.e.Flush() }

// Auth authenticates the connection, the recorder accepts every
// user and password.
func (w *recorder) Auth(user, password string) error { return nil }

// Hijack fails, the recorder has no connection to hand over.
func (w *recorder) Hijack() (net.Conn, error) {
	return nil, ErrHijacked
}

// RemoteAddr has no address, the recorder serves no connection.
func (w *recorder) RemoteAddr() net.Addr { return nil }

func TestServeMux_Handle(t *testing.T) {
	newMux := func() (*ServeMux, *string) {
		mux := NewServeMux()
		var hit string
		reg := func(pattern, name string) {
			mux.HandleFunc(pattern, func(w ResponseWriter, cmd *Command) { hit = name })
		}
		reg("", "fallback")
		reg("GET", "GET")
		reg("CONFIG", "CONFIG")
		reg("CONFIG|GET", "CONFIG|GET")
		return mux, &hit
	}
	serve := func(mux *ServeMux, hit *string, name string, args ...string) string {
		*hit = ""
		cmdArgs := make([][]byte, 0, len(args)+1)
		cmdArgs = append(cmdArgs, []byte(name))
		for _, a := range args {
			cmdArgs = append(cmdArgs, []byte(a))
		}
		mux.ServeRESP(newRecorder(), &Command{Args: cmdArgs})
		return *hit
	}

	mux, hit := newMux()

	tests := []struct {
		name string
		args []string
		want string
	}{
		// The fallback serves the unregistered commands.
		{name: "DEL", args: []string{"DEL"}, want: "fallback"},
		// The name matches any arity.
		{name: "GET", args: []string{"GET"}, want: "GET"},
		{name: "GET a b", args: []string{"GET", "a", "b"}, want: "GET"},
		// The nested names match exactly and beat the name alone.
		{name: "CONFIG GET", args: []string{"CONFIG", "GET"}, want: "CONFIG|GET"},
		{name: "CONFIG get", args: []string{"CONFIG", "get"}, want: "CONFIG|GET"},
		{name: "CONFIG GET extra", args: []string{"CONFIG", "GET", "extra"}, want: "CONFIG|GET"},
		{name: "CONFIG SET", args: []string{"CONFIG", "SET"}, want: "CONFIG"},
		{name: "CONFIG", args: []string{"CONFIG"}, want: "CONFIG"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := serve(mux, hit, tt.args[0], tt.args[1:]...); got != tt.want {
				t.Errorf("ServeRESP() hit %q, want %q", got, tt.want)
			}
		})
	}

	t.Run("unknown command without fallback is an error reply", func(t *testing.T) {
		w := newRecorder()
		NewServeMux().ServeRESP(w, &Command{Args: [][]byte{[]byte("DEL")}})
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
		if got, want := w.buf.String(), "-ERR unknown command 'DEL'\r\n"; got != want {
			t.Errorf("ServeRESP() wrote %q, want %q", got, want)
		}
	})

	t.Run("duplicate name panics", func(t *testing.T) {
		mux := NewServeMux()
		h := HandlerFunc(func(w ResponseWriter, cmd *Command) {})
		mux.Handle("GET", h)
		defer func() {
			if recover() == nil {
				t.Error("Handle() on a registered name did not panic")
			}
		}()
		mux.Handle("GET", h)
	})

	t.Run("duplicate fallback panics", func(t *testing.T) {
		mux := NewServeMux()
		h := HandlerFunc(func(w ResponseWriter, cmd *Command) {})
		mux.Handle("", h)
		defer func() {
			if recover() == nil {
				t.Error("Handle() on a registered fallback did not panic")
			}
		}()
		mux.Handle("", h)
	})

	t.Run("pattern with spaces panics", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Error("Handle() with a spaced pattern did not panic")
			}
		}()
		NewServeMux().Handle("MSET a *", HandlerFunc(func(w ResponseWriter, cmd *Command) {}))
	})

	t.Run("pattern without a name panics", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Error("Handle() without a command name did not panic")
			}
		}()
		NewServeMux().Handle("|x", HandlerFunc(func(w ResponseWriter, cmd *Command) {}))
	})
}

// startServer starts the server on a local listener and returns its
// address, the listener is closed at the cleanup of the test.
func startServer(t *testing.T, srv *Server) net.Addr {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(l)
	t.Cleanup(func() { l.Close() })
	return l.Addr()
}

// dialClient dials the address and returns the connection with the
// deadline that keeps a broken server from hanging the test.
func dialClient(t *testing.T, addr net.Addr) net.Conn {
	t.Helper()
	nc, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nc.Close() })
	return nc
}

// readElems reads the n elements of a reply and returns the value of
// every element, a string or an int64, an empty array as "[]".
func readElems(t *testing.T, d *RESP3Decoder, n int) []any {
	t.Helper()
	out := make([]any, 0, n)
	for i := 0; i < n; i++ {
		tok, err := d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		switch tok.Kind() {
		case KindBlobString, KindString:
			out = append(out, tok.String())
		case KindNumber:
			out = append(out, tok.Int())
		case KindArray:
			if tok.Int() != 0 {
				t.Fatalf("ReadToken() read an array of %d elements, want 0", tok.Int())
			}
			out = append(out, "[]")
		default:
			t.Fatalf("ReadToken() read the kind %s", tok.Kind())
		}
	}
	return out
}

func TestConn_Hello(t *testing.T) {
	t.Run("switch carries the pipelined input over", func(t *testing.T) {
		mux := NewServeMux()
		nc := dialClient(t, startServer(t, &Server{Handler: mux}))
		fmt.Fprint(nc, "*1\r\n$5\r\nHELLO\r\n"+
			"*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n"+
			"*1\r\n$4\r\nPING\r\n")
		d := NewRESP3Decoder(nc)

		tok, err := d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := tok.Kind(), KindArray; got != want {
			t.Fatalf("HELLO kind = %s, want %s", got, want)
		}
		if got, want := tok.Int(), int64(14); got != want {
			t.Fatalf("HELLO length = %d, want %d", got, want)
		}
		elems := readElems(t, d, 14)
		if got, want := elems[4], "proto"; got != want {
			t.Errorf("HELLO field = %v, want %v", got, want)
		}
		if got, want := elems[5], int64(2); got != want {
			t.Errorf("HELLO proto = %v, want %v", got, want)
		}

		tok, err = d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := tok.Kind(), KindMap; got != want {
			t.Fatalf("HELLO 3 kind = %s, want %s", got, want)
		}
		if got, want := tok.Int(), int64(7); got != want {
			t.Fatalf("HELLO 3 length = %d, want %d", got, want)
		}
		elems = readElems(t, d, 14)
		if got, want := elems[4], "proto"; got != want {
			t.Errorf("HELLO 3 field = %v, want %v", got, want)
		}
		if got, want := elems[5], int64(3); got != want {
			t.Errorf("HELLO 3 proto = %v, want %v", got, want)
		}

		tok, err = d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := tok.String(), "PONG"; got != want {
			t.Errorf("PING reply = %q, want %q", got, want)
		}
	})

	t.Run("unsupported version is an error reply", func(t *testing.T) {
		mux := NewServeMux()
		nc := dialClient(t, startServer(t, &Server{Handler: mux}))
		fmt.Fprint(nc, "*2\r\n$5\r\nHELLO\r\n$1\r\n4\r\n*1\r\n$4\r\nPING\r\n")
		d := NewRESP3Decoder(nc)

		tok, err := d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := tok.Kind(), KindError; got != want {
			t.Fatalf("HELLO 4 kind = %s, want %s", got, want)
		}
		if got, want := tok.String(), "NOPROTO unsupported protocol version"; got != want {
			t.Errorf("HELLO 4 reply = %q, want %q", got, want)
		}
		tok, err = d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := tok.String(), "PONG"; got != want {
			t.Errorf("PING reply = %q, want %q", got, want)
		}
	})
}

func TestConn_Auth(t *testing.T) {
	type reqResp struct {
		cmd  []byte
		want []byte
	}
	tests := []struct {
		name   string
		server *Server
		rrs    []reqResp
	}{
		{
			name:   "no passwd",
			server: &Server{},
			rrs: []reqResp{
				{
					[]byte("*2\r\n$4\r\nAUTH\r\n$6\r\nsekret\r\n"),
					[]byte("-ERR Client sent AUTH, but no password is set\r\n"),
				},
				{
					[]byte("*1\r\n$4\r\nPING\r\n"),
					[]byte("+PONG\r\n"),
				},
			},
		},
		{
			name:   "passwd auth",
			server: &Server{Password: "sekret"},
			rrs: []reqResp{
				{
					[]byte("*1\r\n$4\r\nPING\r\n"),
					[]byte("-NOAUTH Authentication required\r\n"),
				},
				{
					[]byte("*2\r\n$4\r\nAUTH\r\n$3\r\nbad\r\n"),
					[]byte("-WRONGPASS invalid username-password pair or user is disabled\r\n"),
				},
				{
					[]byte("*2\r\n$4\r\nAUTH\r\n$6\r\nsekret\r\n"),
					[]byte("+OK\r\n"),
				},
				{
					[]byte("*1\r\n$4\r\nPING\r\n"),
					[]byte("+PONG\r\n"),
				},
			},
		},
		{
			name:   "passwd hello",
			server: &Server{Password: "sekret"},
			rrs: []reqResp{
				{
					[]byte("*1\r\n$4\r\nPING\r\n"),
					[]byte("-NOAUTH Authentication required\r\n"),
				},
				{
					[]byte("*5\r\n$5\r\nHELLO\r\n$1\r\n3\r\n$4\r\nAUTH\r\n$4\r\nuser\r\n$3\r\nbad\r\n"),
					[]byte("-WRONGPASS invalid username-password pair or user is disabled\r\n"),
				},
				{
					[]byte("*5\r\n$5\r\nHELLO\r\n$1\r\n3\r\n$4\r\nAUTH\r\n$7\r\ndefault\r\n$6\r\nsekret\r\n"),
					[]byte("%7\r\n$6\r\nserver\r\n$4\r\nrdbc\r\n$7\r\nversion\r\n$5\r\n0.1.0\r\n$5\r\nproto\r\n:3\r\n$2\r\nid\r\n:0\r\n$4\r\nmode\r\n$10\r\nstandalone\r\n$4\r\nrole\r\n$6\r\nmaster\r\n$7\r\nmodules\r\n*0\r\n"),
				},
				{
					[]byte("*1\r\n$4\r\nPING\r\n"),
					[]byte("+PONG\r\n"),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr := startServer(t, tt.server)
			nc, err := net.Dial("tcp", addr.String())
			if err != nil {
				t.Fatal(err)
			}
			defer nc.Close()
			nc.SetDeadline(time.Now().Add(10 * time.Second))
			buf := make([]byte, 1024)

			for _, rr := range tt.rrs {
				if _, err := nc.Write(rr.cmd); err != nil {
					t.Fatal(err)
				}
				n, err := nc.Read(buf)
				if err != nil {
					t.Fatal(err)
				}
				if got := buf[:n]; !bytes.Equal(got, rr.want) {
					t.Fatalf("Exec() = %s, want %s", got, rr.want)
				}
			}
		})
	}
}

func TestConn_Reset(t *testing.T) {
	t.Run("goes back to RESP2 and resets the authentication", func(t *testing.T) {
		mux := NewServeMux()
		nc := dialClient(t, startServer(t, &Server{Handler: mux, Password: "sekret"}))
		fmt.Fprint(nc,
			"*2\r\n$4\r\nAUTH\r\n$6\r\nsekret\r\n"+
				"*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n"+
				"*1\r\n$5\r\nRESET\r\n"+
				"*1\r\n$5\r\nHELLO\r\n"+
				"*1\r\n$3\r\nFOO\r\n")
		d := NewRESP3Decoder(nc)

		want := []struct {
			kind Kind
			text string
		}{
			{KindString, "OK"},
			{KindMap, ""},
			{KindString, "RESET"},
			{KindArray, ""},
			{KindError, "NOAUTH Authentication required"},
		}
		for i, w := range want {
			tok, err := d.ReadToken()
			if err != nil {
				t.Fatal(err)
			}
			if got := tok.Kind(); got != w.kind {
				t.Fatalf("reply %d kind = %s, want %s", i, got, w.kind)
			}
			if w.text != "" && tok.String() != w.text {
				t.Errorf("reply %d = %q, want %q", i, tok.String(), w.text)
			}
			switch w.kind {
			case KindMap:
				elems := readElems(t, d, 14)
				if got, want := elems[5], int64(3); got != want {
					t.Errorf("HELLO proto = %v, want %v", got, want)
				}
			case KindArray:
				elems := readElems(t, d, 14)
				if got, want := elems[5], int64(2); got != want {
					t.Errorf("HELLO proto = %v, want %v", got, want)
				}
			}
		}
	})
}

func TestConn_ChangeProto(t *testing.T) {
	mux := NewServeMux()
	mux.HandleFunc("HELLO3", func(w ResponseWriter, cmd *Command) {
		if err := w.ChangeProto(3); err != nil {
			w.Proto().WriteToken(Error(err.Error()))
			return
		}
		w.Proto().(*RESP3Encoder).WriteToken(String("SWITCHED"))
	})
	nc := dialClient(t, startServer(t, &Server{Handler: mux}))
	fmt.Fprint(nc, "*1\r\n$6\r\nHELLO3\r\n*1\r\n$4\r\nPING\r\n")
	d := NewRESP3Decoder(nc)

	tok, err := d.ReadToken()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := tok.String(), "SWITCHED"; got != want {
		t.Errorf("HELLO3 reply = %q, want %q", got, want)
	}
	tok, err = d.ReadToken()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := tok.String(), "PONG"; got != want {
		t.Errorf("PING reply = %q, want %q", got, want)
	}
}

func TestConn_Quit(t *testing.T) {
	mux := NewServeMux()
	nc := dialClient(t, startServer(t, &Server{Handler: mux}))
	fmt.Fprint(nc, "*1\r\n$4\r\nQUIT\r\n")
	d := NewRESP3Decoder(nc)

	tok, err := d.ReadToken()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := tok.String(), "OK"; got != want {
		t.Errorf("QUIT reply = %q, want %q", got, want)
	}
	if _, err := d.ReadToken(); err != io.EOF {
		t.Errorf("ReadToken() after QUIT err = %v, want %v", err, io.EOF)
	}
}

func TestConn_serve(t *testing.T) {
	t.Run("protocol error closes the connection", func(t *testing.T) {
		mux := NewServeMux()
		nc := dialClient(t, startServer(t, &Server{Handler: mux}))
		fmt.Fprint(nc, "*1\r\n:5\r\n")
		d := NewRESP3Decoder(nc)

		tok, err := d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := tok.Kind(), KindError; got != want {
			t.Fatalf("protocol error kind = %s, want %s", got, want)
		}
		if !strings.Contains(tok.String(), "Protocol error") {
			t.Errorf("protocol error reply = %q, want it to report the protocol error", tok.String())
		}
		if _, err := d.ReadToken(); err != io.EOF {
			t.Errorf("ReadToken() after a protocol error err = %v, want %v", err, io.EOF)
		}
	})
}

func TestServer_Serve(t *testing.T) {
	t.Run("unknown commands without a handler", func(t *testing.T) {
		nc := dialClient(t, startServer(t, &Server{}))
		fmt.Fprint(nc, "*1\r\n$3\r\nFOO\r\n")
		d := NewRESP3Decoder(nc)

		tok, err := d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := tok.String(), "ERR unknown command 'FOO'"; got != want {
			t.Errorf("FOO reply = %q, want %q", got, want)
		}
	})
}

// countCloseListener is a net.Listener wrapper that counts the
// number of the Close calls of the listener.
type countCloseListener struct {
	net.Listener
	closes atomic.Int32
}

func (cl *countCloseListener) Close() error {
	var err error
	if n := cl.closes.Add(1); n == 1 && cl.Listener != nil {
		err = cl.Listener.Close()
	}
	return err
}

func TestServer_Close(t *testing.T) {
	t.Run("a server that serves nothing closes twice", func(t *testing.T) {
		// The close touches no listener and no connection, the
		// second close is safe.
		var srv Server
		if err := srv.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
		if err := srv.Close(); err != nil {
			t.Fatalf("second Close() error = %v", err)
		}
	})

	t.Run("Serve reports the close of the server", func(t *testing.T) {
		// Serve on a closed server serves nothing and closes the
		// listener it was handed exactly once.
		srv := &Server{}
		if err := srv.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
		cl := &countCloseListener{Listener: localListener(t)}
		if err := srv.Serve(cl); !errors.Is(err, ErrServerClosed) {
			t.Errorf("Serve() error = %v, want %v", err, ErrServerClosed)
		}
		if got := cl.closes.Load(); got != 1 {
			t.Errorf("the listener was closed %d times, want 1", got)
		}
	})

	t.Run("the listener of a serving server is closed once", func(t *testing.T) {
		// The close of the server closes the listener of Serve and
		// the return of Serve closes it no second time.
		srv := &Server{}
		cl := &countCloseListener{Listener: localListener(t)}
		served := make(chan error, 1)
		go func() { served <- srv.Serve(cl) }()
		// The listener of the server may not be up yet, the dial
		// retries until it is.
		var nc net.Conn
		var err error
		for i := 0; i < 100; i++ {
			nc, err = net.DialTimeout("tcp", cl.Addr().String(), time.Second)
			if err == nil {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if nc == nil {
			t.Fatal("the server did not listen on its address")
		}
		defer nc.Close()
		if err := srv.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
		select {
		case err := <-served:
			if !errors.Is(err, ErrServerClosed) {
				t.Errorf("Serve() error = %v, want %v", err, ErrServerClosed)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Serve() did not return after Close()")
		}
		if got := cl.closes.Load(); got != 1 {
			t.Errorf("the listener was closed %d times, want 1", got)
		}
	})

	t.Run("the connections are closed", func(t *testing.T) {
		// The core library enforces this with its goroutine leak
		// check alone, the test asserts it on the wire instead: the
		// read of the client ends with the connection the close of
		// the server closed.
		srv := &Server{}
		l := localListener(t)
		served := make(chan error, 1)
		go func() { served <- srv.Serve(l) }()
		nc := dialClient(t, l.Addr())
		fmt.Fprint(nc, "*1\r\n$4\r\nPING\r\n")
		d := NewRESP3Decoder(nc)
		tok, err := d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := tok.String(), "PONG"; got != want {
			t.Errorf("PING reply = %q, want %q", got, want)
		}
		if err := srv.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
		if _, err := d.ReadToken(); err == nil {
			t.Error("ReadToken() after Close() did not fail")
		}
		select {
		case <-served:
		case <-time.After(5 * time.Second):
			t.Fatal("Serve() did not return after Close()")
		}
	})
}

// localListener listens on a local address and closes the listener
// at the cleanup of the test.
func localListener(t *testing.T) net.Listener {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

func TestCommand_Context(t *testing.T) {
	var served context.Context
	done := make(chan struct{})
	mux := NewServeMux()
	mux.HandleFunc("CTX", func(w ResponseWriter, cmd *Command) {
		served = cmd.Context()
		if served == nil {
			t.Error("Context() = nil")
		}
		close(done)
	})
	nc := dialClient(t, startServer(t, &Server{Handler: mux}))
	fmt.Fprint(nc, "*1\r\n$3\r\nCTX\r\n*1\r\n$4\r\nQUIT\r\n")
	d := NewRESP3Decoder(nc)

	tok, err := d.ReadToken()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := tok.String(), "OK"; got != want {
		t.Errorf("QUIT reply = %q, want %q", got, want)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("the command was not served")
	}
	select {
	case <-served.Done():
	case <-time.After(time.Second):
		t.Fatal("the context of the connection was not canceled")
	}
}

func TestServer_ServeConn_hijack(t *testing.T) {
	type hijackResult struct {
		nc  net.Conn
		err error
	}
	readResult := func(t *testing.T, hijacked <-chan hijackResult) hijackResult {
		t.Helper()
		select {
		case r := <-hijacked:
			return r
		case <-time.After(5 * time.Second):
			t.Fatal("the handler did not hijack the connection")
			return hijackResult{}
		}
	}

	t.Run("the connection is left to the handler", func(t *testing.T) {
		mux := NewServeMux()
		hijacked := make(chan hijackResult, 2)
		mux.HandleFunc("HIJACK", func(w ResponseWriter, cmd *Command) {
			nc, err := w.Hijack()
			hijacked <- hijackResult{nc, err}
			if err != nil {
				return
			}
			_, err = w.Hijack()
			hijacked <- hijackResult{nil, err}
		})
		srv := &Server{Handler: mux}
		addr := startServer(t, srv)
		nc := dialClient(t, addr)
		if _, err := fmt.Fprint(nc, "*1\r\n$6\r\nHIJACK\r\n"); err != nil {
			t.Fatal(err)
		}

		r := readResult(t, hijacked)
		if r.err != nil {
			t.Fatalf("Hijack() err = %v, want nil", r.err)
		}
		t.Cleanup(func() { r.nc.Close() })
		if err := readResult(t, hijacked).err; !errors.Is(err, ErrHijacked) {
			t.Errorf("the second Hijack() err = %v, want %v", err, ErrHijacked)
		}

		// The command behind the hijack is never read, the server
		// answers nothing.
		if _, err := fmt.Fprint(nc, "*1\r\n$4\r\nPING\r\n"); err != nil {
			t.Fatal(err)
		}
		nc.SetReadDeadline(time.Now().Add(250 * time.Millisecond))
		buf := make([]byte, 1)
		if n, err := nc.Read(buf); err == nil {
			t.Errorf("the server answered a command of a hijacked connection: %q", buf[:n])
		} else if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Errorf("Read() err = %v, want the deadline of the read", err)
		}

		// The server closes the connection of nobody but the handler,
		// the connection the hijack returned still reaches the client.
		if err := srv.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := fmt.Fprint(r.nc, "+still open\r\n"); err != nil {
			t.Fatal(err)
		}
		nc.SetReadDeadline(time.Now().Add(5 * time.Second))
		want := "+still open\r\n"
		got := make([]byte, len(want))
		if _, err := io.ReadFull(nc, got); err != nil {
			t.Fatal(err)
		}
		if string(got) != want {
			t.Errorf("the hijacked connection received %q, want %q", got, want)
		}
	})

	t.Run("a partial reply refuses the hijack", func(t *testing.T) {
		mux := NewServeMux()
		hijacked := make(chan hijackResult, 2)
		mux.HandleFunc("PARTIAL", func(w ResponseWriter, cmd *Command) {
			w.Proto().WriteToken(String("partial"))
			_, err := w.Hijack()
			hijacked <- hijackResult{nil, err}
			if !errors.Is(err, ErrNotFlushed) {
				return
			}
			if err := w.Flush(); err != nil {
				return
			}
			nc, err := w.Hijack()
			hijacked <- hijackResult{nc, err}
			if err == nil {
				nc.Close()
			}
		})
		nc := dialClient(t, startServer(t, &Server{Handler: mux}))
		if _, err := fmt.Fprint(nc, "*1\r\n$7\r\nPARTIAL\r\n"); err != nil {
			t.Fatal(err)
		}

		if err := readResult(t, hijacked).err; !errors.Is(err, ErrNotFlushed) {
			t.Errorf("Hijack() err = %v, want %v", err, ErrNotFlushed)
		}
		if err := readResult(t, hijacked).err; err != nil {
			t.Fatalf("Hijack() after Flush() err = %v, want nil", err)
		}
		// The refused reply reached the client with the flush of the
		// handler, the connection of the second hijack closed it.
		d := NewRESP2Decoder(nc)
		tok, err := d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := tok.String(), "partial"; got != want {
			t.Errorf("the refused reply = %q, want %q", got, want)
		}
		if _, err := d.ReadToken(); !errors.Is(err, io.EOF) {
			t.Errorf("ReadToken() after the close err = %v, want %v", err, io.EOF)
		}
	})
}

func TestServer_ConnContext(t *testing.T) {
	type key struct{}
	type state struct{ id int }
	mux := NewServeMux()
	var mu sync.Mutex
	var states []*state
	mux.HandleFunc("STATE", func(w ResponseWriter, cmd *Command) {
		st := cmd.Context().Value(key{}).(*state)
		mu.Lock()
		states = append(states, st)
		mu.Unlock()
		w.Proto().WriteToken(String("OK"))
	})
	srv := &Server{
		Handler: mux,
		ConnContext: func(ctx context.Context, nc net.Conn) context.Context {
			return context.WithValue(ctx, key{}, &state{})
		},
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(l)
	t.Cleanup(func() { l.Close() })

	for i := 0; i < 2; i++ {
		nc := dialClient(t, l.Addr())
		fmt.Fprint(nc, "*1\r\n$5\r\nSTATE\r\n")
		d := NewRESP3Decoder(nc)
		tok, err := d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := tok.String(), "OK"; got != want {
			t.Errorf("STATE reply = %q, want %q", got, want)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(states) != 2 {
		t.Fatalf("the handlers saw %d states, want 2", len(states))
	}
	if states[0] == states[1] {
		t.Error("the two connections share the state of their context")
	}
}

func TestServer_ListenAndServe(t *testing.T) {
	mux := NewServeMux()

	// A free port for the server, the listener of the probe is
	// closed before the server listens on the address.
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := probe.Addr().String()
	probe.Close()

	srv := &Server{Addr: addr, Handler: mux}
	go srv.ListenAndServe()

	// The listener of the server may not be up yet, the dial retries
	// until it is.
	var nc net.Conn
	for i := 0; i < 100; i++ {
		var err error
		nc, err = net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if nc == nil {
		t.Fatal("the server did not listen on its address")
	}
	defer nc.Close()
	if err := nc.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprint(nc, "*1\r\n$4\r\nPING\r\n"); err != nil {
		t.Fatal(err)
	}
	tok, err := NewRESP3Decoder(nc).ReadToken()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := tok.String(), "PONG"; got != want {
		t.Errorf("PING reply = %q, want %q", got, want)
	}
}

func TestListenAndServe(t *testing.T) {
	if err := ListenAndServe("bogus", nil); err == nil {
		t.Error("ListenAndServe() with an invalid address did not fail")
	}
}
