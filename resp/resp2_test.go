package resp

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
)

func TestRESP2Decoder_ReadToken(t *testing.T) {
	data := "+OK\r\n" +
		"-Error message\r\n" +
		":1000\r\n" +
		"$6\r\nfoobar\r\n" +
		"$0\r\n\r\n" +
		"$-1\r\n" +
		"*0\r\n" +
		"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n" +
		"*-1\r\n" +
		"+hello world\r\n"
	wants := []struct {
		kind  Kind
		value any
	}{
		{kind: KindString, value: "OK"},
		{kind: KindError, value: errors.New("Error message")},
		{kind: KindInteger, value: int64(1000)},
		{kind: KindBulkString, value: []byte("foobar")},
		{kind: KindBulkString, value: []byte{}},
		{kind: KindBulkString, value: []byte(nil)},
		{kind: KindArray, value: int64(0)},
		{kind: KindArray, value: int64(2)},
		{kind: KindBulkString, value: []byte("foo")},
		{kind: KindBulkString, value: []byte("bar")},
		{kind: KindArray, value: int64(-1)},
		{kind: KindString, value: "hello world"},
	}
	r := NewRESP2Decoder(strings.NewReader(data))
	for _, want := range wants {
		got, err := r.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		if want := want.kind; got.Kind() != want {
			t.Errorf("ReadToken() got = %v, want = %v", got.Kind(), want)
		}
		if want := want.value; !reflect.DeepEqual(got.Val(), want) {
			t.Errorf("ReadValue() got = %v, want = %v", got.Val(), want)
		}
	}

	// A line too short to carry the two terminator bytes is an
	// error, the read must not panic.
	t.Run("invalid terminator", func(t *testing.T) {
		for _, data := range []string{"+\n", ":\n"} {
			d := NewRESP2Decoder(strings.NewReader(data))
			if _, err := d.ReadToken(); err == nil {
				t.Errorf("ReadToken() on %q did not fail", data)
			}
		}
	})
}

func TestRESP2Encoder_WriteToken(t *testing.T) {
	t.Run("round trip of a nil bulk string", func(t *testing.T) {
		// The decoder reports a nil bulk string with a raw token, its
		// raw bytes are a complete reply.
		d := NewRESP2Decoder(strings.NewReader("$-1\r\n"))
		tok, err := d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		var b strings.Builder
		e := NewRESP2Encoder(&b)
		if err := e.WriteToken(tok); err != nil {
			t.Fatal(err)
		}
		if err := e.Flush(); err != nil {
			t.Fatal(err)
		}
		if got, want := b.String(), "$-1\r\n"; got != want {
			t.Errorf("WriteToken() = %q, want %q", got, want)
		}
	})

	t.Run("payload is streamed", func(t *testing.T) {
		var b strings.Builder
		e := NewRESP2Encoder(&b)
		if err := e.WriteToken(Bytes([]byte("hello"))); err != nil {
			t.Fatal(err)
		}
		if err := e.Flush(); err != nil {
			t.Fatal(err)
		}
		if got, want := b.String(), "$5\r\nhello\r\n"; got != want {
			t.Errorf("WriteToken() = %q, want %q", got, want)
		}
	})
}

func TestRESP2Decoder_Handoff(t *testing.T) {
	r := strings.NewReader("$5\r\nhello\r\n*1\r\n$4\r\nPING\r\n")
	d := NewRESP2Decoder(r)
	tok, err := d.ReadToken()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := tok.String(), "hello"; got != want {
		t.Fatalf("ReadToken() = %q, want %q", got, want)
	}

	d2 := NewRESP2Decoder(d.Handoff())
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

func TestRESP2Encoder_WriteToken_streamed(t *testing.T) {
	t.Run("payload is streamed", func(t *testing.T) {
		var b strings.Builder
		e := NewRESP2Encoder(&b)
		tok := BulkString(&io.LimitedReader{R: strings.NewReader("0123456789trailing"), N: 10})
		if got, want := tok.Kind(), KindBulkString; got != want {
			t.Fatalf("Kind() = %s, want %s", got, want)
		}
		if err := e.WriteToken(tok); err != nil {
			t.Fatal(err)
		}
		if err := e.Flush(); err != nil {
			t.Fatal(err)
		}
		if got, want := b.String(), "$10\r\n0123456789\r\n"; got != want {
			t.Errorf("WriteToken() = %q, want %q", got, want)
		}
	})

	t.Run("zero length payload is streamed", func(t *testing.T) {
		var b strings.Builder
		e := NewRESP2Encoder(&b)
		if err := e.WriteToken(BulkString(&io.LimitedReader{R: strings.NewReader(""), N: 0})); err != nil {
			t.Fatal(err)
		}
		if err := e.Flush(); err != nil {
			t.Fatal(err)
		}
		if got, want := b.String(), "$0\r\n\r\n"; got != want {
			t.Errorf("WriteToken() = %q, want %q", got, want)
		}
	})

	t.Run("short payload fails the write", func(t *testing.T) {
		var b strings.Builder
		e := NewRESP2Encoder(&b)
		if err := e.WriteToken(BulkString(&io.LimitedReader{R: strings.NewReader("abc"), N: 10})); err == nil {
			t.Error("WriteToken() of a short payload did not fail")
		}
	})

	t.Run("nil reader panics", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Error("BulkString() with a nil reader did not panic")
			}
		}()
		BulkString(nil)
	})
}

func TestRESP2Decoder_ReadToken_streamed(t *testing.T) {
	t.Run("payload is streamed", func(t *testing.T) {
		d := NewRESP2Decoder(strings.NewReader("$16\r\n" + "0123456789abcdef\r\n+OK\r\n"))
		tok, err := d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := tok.Kind(), KindBulkString; got != want {
			t.Fatalf("Kind() = %s, want %s", got, want)
		}
		if tok.stream == nil {
			t.Fatal("ReadToken() did not stream the payload")
		}
		p, err := io.ReadAll(tok.stream)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := string(p), "0123456789abcdef\r\n"; got != want {
			t.Errorf("payload = %q, want %q", got, want)
		}
		tok2, err := d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := tok2.String(), "OK"; got != want {
			t.Errorf("ReadToken() after the payload = %q, want %q", got, want)
		}
	})

	t.Run("Bytes consumes the payload", func(t *testing.T) {
		d := NewRESP2Decoder(strings.NewReader("$16\r\n" + "0123456789abcdef" + "\r\n+OK\r\n"))
		tok, err := d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := string(tok.Bytes()), "0123456789abcdef"; got != want {
			t.Errorf("Bytes() = %q, want %q", got[:16]+"...", want[:16]+"...")
		}
		tok2, err := d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := tok2.String(), "OK"; got != want {
			t.Errorf("ReadToken() after Bytes = %q, want %q", got, want)
		}
	})

	t.Run("unconsumed payload is skipped", func(t *testing.T) {
		d := NewRESP2Decoder(strings.NewReader("$16\r\n" + "0123456789abcdef" + "\r\n+OK\r\n"))
		_, err := d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		tok2, err := d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := tok2.String(), "OK"; got != want {
			t.Errorf("ReadToken() after the unconsumed payload = %q, want %q", got, want)
		}
	})

	t.Run("a streamed token is written as it was read", func(t *testing.T) {
		wire := "$16\r\n" + "0123456789abcdef" + "\r\n"
		d := NewRESP2Decoder(strings.NewReader(wire))
		tok, err := d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		e := NewRESP2Encoder(&buf)
		if err := e.WriteToken(tok); err != nil {
			t.Fatal(err)
		}
		if err := e.Flush(); err != nil {
			t.Fatal(err)
		}
		if got := buf.String(); got != wire {
			t.Errorf("WriteToken() = %q, want %q", got, wire)
		}
	})

	t.Run("the payload of a consumed token is written as it was read", func(t *testing.T) {
		wire := "$16\r\n" + "0123456789abcdef" + "\r\n"
		d := NewRESP2Decoder(strings.NewReader(wire))
		tok, err := d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		_ = tok.Bytes()
		var buf bytes.Buffer
		e := NewRESP2Encoder(&buf)
		if err := e.WriteToken(tok); err != nil {
			t.Fatal(err)
		}
		if err := e.Flush(); err != nil {
			t.Fatal(err)
		}
		if got := buf.String(); got != wire {
			t.Errorf("WriteToken() = %q, want %q", got, wire)
		}
	})
}

func TestRESP2Decoder_ReadToken_eofMarked(t *testing.T) {
	const marker = "0123456789abcdef0123456789abcdef01234567"

	t.Run("payload ends at the marker", func(t *testing.T) {
		wire := "$EOF:" + marker + "\r\nhello world" + marker
		d := NewRESP2Decoder(strings.NewReader(wire + "+OK\r\n"))
		tok, err := d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := tok.Kind(), KindBulkString; got != want {
			t.Fatalf("Kind() = %s, want %s", got, want)
		}
		if got, want := string(tok.Bytes()), "hello world"; got != want {
			t.Errorf("Bytes() = %q, want %q", got, want)
		}
		tok2, err := d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := tok2.String(), "OK"; got != want {
			t.Errorf("ReadToken() after the payload = %q, want %q", got, want)
		}

		// the raw bytes of the token are replayed by the encoder
		tok, err = NewRESP2Decoder(strings.NewReader(wire)).ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		e := NewRESP2Encoder(&buf)
		if err := e.WriteToken(tok); err != nil {
			t.Fatal(err)
		}
		if err := e.Flush(); err != nil {
			t.Fatal(err)
		}
		if got := buf.String(); got != wire {
			t.Errorf("WriteToken() = %q, want %q", got, wire)
		}
	})

	t.Run("unconsumed payload is skipped", func(t *testing.T) {
		d := NewRESP2Decoder(strings.NewReader("$EOF:" + marker + "\r\nhello world" + marker + "+OK\r\n"))
		if _, err := d.ReadToken(); err != nil {
			t.Fatal(err)
		}
		tok2, err := d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := tok2.String(), "OK"; got != want {
			t.Errorf("ReadToken() after the unconsumed payload = %q, want %q", got, want)
		}
	})

	t.Run("a stream without the marker fails", func(t *testing.T) {
		d := NewRESP2Decoder(strings.NewReader("$EOF:" + marker + "\r\nhello world"))
		if _, err := d.ReadToken(); err != nil {
			t.Fatal(err)
		}
		if _, err := d.ReadToken(); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("ReadToken() error = %v, want %v", err, io.ErrUnexpectedEOF)
		}
	})

	t.Run("the payload of a consumed token is written as it was read", func(t *testing.T) {
		wire := "$EOF:" + marker + "\r\nhello" + marker
		d := NewRESP2Decoder(strings.NewReader(wire))
		tok, err := d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		_ = tok.Bytes()
		var buf bytes.Buffer
		e := NewRESP2Encoder(&buf)
		if err := e.WriteToken(tok); err != nil {
			t.Fatal(err)
		}
		if err := e.Flush(); err != nil {
			t.Fatal(err)
		}
		if got := buf.String(); got != wire {
			t.Errorf("WriteToken() = %q, want %q", got, wire)
		}
	})

	t.Run("a constructed token ends at the marker of the encoder", func(t *testing.T) {
		var buf bytes.Buffer
		e := NewRESP2Encoder(&buf)
		if err := e.WriteToken(BulkStringEOF(strings.NewReader("hello"))); err != nil {
			t.Fatal(err)
		}
		if err := e.Flush(); err != nil {
			t.Fatal(err)
		}
		wire := buf.String()
		if got, want := wire[:5], "$EOF:"; got != want {
			t.Fatalf("BulkStringEOF() header = %q, want %q", got, want)
		}
		if got, want := wire[47:], "hello"+wire[5:45]; got != want {
			t.Errorf("BulkStringEOF() payload = %q, want %q", got, want)
		}

		// the token is read back the way it was written
		rtok, err := NewRESP2Decoder(strings.NewReader(wire)).ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		var back bytes.Buffer
		e = NewRESP2Encoder(&back)
		if err := e.WriteToken(rtok); err != nil {
			t.Fatal(err)
		}
		if err := e.Flush(); err != nil {
			t.Fatal(err)
		}
		if got := back.String(); got != wire {
			t.Errorf("WriteToken() = %q, want %q", got, wire)
		}
	})
}
