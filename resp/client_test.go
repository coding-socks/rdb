package resp

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

// serveReplies returns a client over a pipe whose commands are read by
// a server goroutine, the replies of the script are written to it in
// the order the commands arrive.
func serveReplies(t *testing.T, script ...[]Token) *Client {
	t.Helper()
	c1, c2 := net.Pipe()
	t.Cleanup(func() { c1.Close(); c2.Close() })
	go func() {
		defer c2.Close()
		d := NewRESP2Decoder(c2)
		e := NewRESP2Encoder(c2)
		for _, reply := range script {
			// The command of the client is read whole, the
			// elements of its array included, the next reply
			// follows it on the connection.
			cmd, err := d.ReadToken()
			if err != nil {
				return
			}
			if err := SkipReply(d, cmd); err != nil {
				return
			}
			for _, tok := range reply {
				if err := e.WriteToken(tok); err != nil {
					return
				}
			}
			if err := e.Flush(); err != nil {
				return
			}
		}
	}()
	return &Client{C: c1, D: NewRESP3Decoder(c1), E: NewRESP3Encoder(c1)}
}

func TestClient_Exec(t *testing.T) {
	t.Run("the reply of the command", func(t *testing.T) {
		// The elements of an aggregate reply are left on the
		// connection, the caller of Exec reads them or skips
		// them, the reply of the second command arrives behind
		// the skipped ones.
		c := serveReplies(t,
			[]Token{ArrayLen(2), String("a"), String("b")},
			[]Token{String("OK")},
		)
		tok, err := c.Exec(t.Context(), []byte("GET"), []byte("key"))
		if err != nil {
			t.Fatalf("Exec() error = %v", err)
		}
		if tok.Kind() != KindArray || tok.Int() != 2 {
			t.Errorf("Exec() = %s %v, want an array of two", tok.Kind(), tok.Val())
		}
		if err := SkipReply(c.D, tok); err != nil {
			t.Fatalf("SkipReply() error = %v", err)
		}
		tok, err = c.Exec(t.Context(), []byte("PING"))
		if err != nil {
			t.Fatalf("Exec() error = %v", err)
		}
		if tok.Kind() != KindString || tok.String() != "OK" {
			t.Errorf("Exec() = %s %v, want OK", tok.Kind(), tok.Val())
		}
	})

	t.Run("an error reply is the token it is", func(t *testing.T) {
		want := "ERR test"
		c := serveReplies(t, []Token{Error(want)})
		tok, err := c.Exec(t.Context(), []byte("PING"))
		if err != nil {
			t.Fatalf("Exec() error = %v", err)
		}
		if tok.Kind() != KindError || tok.Err().Error() != want {
			t.Errorf("Exec() = %s %v, want the error %s", tok.Kind(), tok.Val(), want)
		}
	})

	t.Run("a context that is done ends the read", func(t *testing.T) {
		// The server of the test answers nothing, the read of the
		// reply is aborted when the context is done.
		c1, c2 := net.Pipe()
		defer c1.Close()
		defer c2.Close()
		go func() {
			io.Copy(io.Discard, c2)
		}()
		c := &Client{C: c1, D: NewRESP3Decoder(c1), E: NewRESP3Encoder(c1)}
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		if _, err := c.Exec(ctx, []byte("PING")); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Exec() error = %v, want %v", err, context.DeadlineExceeded)
		}
	})
}

func Test_SkipReply(t *testing.T) {
	// The elements of an aggregate reply are read to the end, a
	// nested aggregate and the parts of an unbound blob string
	// included, the decoder is left on the boundary of the next
	// reply.
	const data = "*2\r\n" +
		"*2\r\n+a\r\n+b\r\n" +
		"%1\r\n+c\r\n$?\r\n;4\r\npart\r\n;0\r\n" +
		"$5\r\nhello\r\n"
	d := NewRESP3Decoder(strings.NewReader(data))
	tok, err := d.ReadToken()
	if err != nil {
		t.Fatalf("ReadToken() error = %v", err)
	}
	if err := SkipReply(d, tok); err != nil {
		t.Fatalf("SkipReply() error = %v", err)
	}
	next, err := d.ReadToken()
	if err != nil {
		t.Fatalf("ReadToken() error = %v", err)
	}
	if next.Kind() != KindBlobString || next.String() != "hello" {
		t.Errorf("ReadToken() = %s %v, want hello", next.Kind(), next.Val())
	}

	// The elements of a push are skipped like the ones of an
	// array, the decoder is left on the boundary of the next
	// reply.
	const pushes = ">2\r\n+a\r\n+b\r\n+OK\r\n"
	d = NewRESP3Decoder(strings.NewReader(pushes))
	tok, err = d.ReadToken()
	if err != nil {
		t.Fatalf("ReadToken() error = %v", err)
	}
	if err := SkipReply(d, tok); err != nil {
		t.Fatalf("SkipReply() error = %v", err)
	}
	next, err = d.ReadToken()
	if err != nil {
		t.Fatalf("ReadToken() error = %v", err)
	}
	if next.Kind() != KindString || next.String() != "OK" {
		t.Errorf("ReadToken() = %s %v, want OK", next.Kind(), next.Val())
	}
}
