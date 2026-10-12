package commands

import (
	"bytes"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coding-socks/rdb/resp"
)

func TestConn_ReadValue(t *testing.T) {
	t.Run("formats messages", func(t *testing.T) {
		tests := []struct {
			name    string
			data    string
			want    string
			wantErr bool
			// wantNext is the message left for the next read, a
			// message that arrived behind the one read.
			wantNext string
		}{
			{
				name: "resp2 blob string",
				data: "$5\r\nhello\r\n",
				want: "hello\n",
			},
			{
				name: "resp2 nil blob string",
				data: "$-1\r\n",
				want: "(nil)\n",
			},
			{
				name: "resp2 nil array",
				data: "*-1\r\n",
				want: "(nil)\n",
			},
			{
				name: "resp2 array",
				data: "*2\r\n$1\r\na\r\n$1\r\nb\r\n",
				want: "1) \"a\"\n2) \"b\"\n",
			},
			{
				name: "resp2 nested array",
				data: "*2\r\n*1\r\n$1\r\na\r\n$1\r\nb\r\n",
				want: "1) 1) \"a\"\n2) \"b\"\n",
			},
			{
				name: "resp2 error reply",
				data: "-ERR bad\r\n",
				want: "ERR bad",
			},
			{
				name: "resp3 blob string",
				data: "$5\r\nhello\r\n",
				want: "hello\n",
			},
			{
				name: "resp3 streamed blob string",
				data: "$?\r\n;4\r\nHell\r\n;5\r\no wor\r\n;2\r\nld\r\n;0\r\n",
				want: "Hello world\n",
			},
			{
				name: "resp3 simple string",
				data: "+OK\r\n",
				want: "OK\n",
			},
			{
				name: "resp3 error reply",
				data: "-ERR bad\r\n",
				want: "ERR bad",
			},
			{
				name: "resp3 map",
				data: "%1\r\n$1\r\na\r\n$1\r\nb\r\n",
				want: "1# \"a\" => \"b\"\n",
			},
			{
				name: "resp3 empty map",
				data: "%0\r\n",
				want: "(empty hash)\n",
			},
			{
				name: "resp3 map with multiline value",
				data: "%1\r\n$1\r\na\r\n*2\r\n$1\r\nb\r\n$1\r\nc\r\n",
				want: "1# \"a\" => \n   1) \"b\"\n   2) \"c\"\n",
			},
			{
				name: "resp3 map with single element array value",
				data: "%1\r\n$1\r\na\r\n*1\r\n$1\r\nb\r\n",
				want: "1# \"a\" => 1) \"b\"\n",
			},
			{
				name: "resp3 set",
				data: "~2\r\n$1\r\na\r\n$1\r\nb\r\n",
				want: "1~ \"a\"\n2~ \"b\"\n",
			},
			{
				name:     "resp3 push message",
				data:     ">3\r\n$7\r\nmessage\r\n$2\r\nch\r\n$5\r\nhello\r\n+PONG\r\n",
				want:     "> 1) \"message\"\n> 2) \"ch\"\n> 3) \"hello\"\n",
				wantNext: "PONG\n",
			},
			{
				name:     "resp3 push with a nested aggregate",
				data:     ">2\r\n$1\r\nx\r\n*1\r\n$1\r\ny\r\n+OK\r\n",
				want:     "> 1) \"x\"\n> 2) 1) \"y\"\n",
				wantNext: "OK\n",
			},
			{
				name: "resp3 double and boolean and null",
				data: ",1.5\r\n",
				want: "(dbl) 1.5\n",
			},
			{
				name: "resp3 null",
				data: "_\r\n",
				want: "(nil)\n",
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				client, server := net.Pipe()
				defer server.Close()
				go server.Write([]byte(tt.data))
				c := &resp.Client{C: client, D: resp.NewRESP3Decoder(strings.NewReader(tt.data))}
				tok, err := c.ReadValue(t.Context())
				if (err != nil) != tt.wantErr {
					t.Fatalf("ReadValue() error = %v, wantErr %v", err, tt.wantErr)
				}
				var buf bytes.Buffer
				if _, err := io.Copy(&buf, NewTokenReader(c, tok)); err != nil {
					t.Fatal(err)
				}
				if buf.String() != tt.want {
					t.Errorf("message = %q, want = %q", buf.String(), tt.want)
				}
				if tt.wantNext == "" {
					return
				}
				// The read of the message consumed nothing of what the
				// server sends afterwards.
				tok, err = c.ReadValue(t.Context())
				if err != nil {
					t.Fatalf("ReadValue() error = %v", err)
				}
				buf.Reset()
				if _, err := io.Copy(&buf, NewTokenReader(c, tok)); err != nil {
					t.Fatal(err)
				}
				if buf.String() != tt.wantNext {
					t.Errorf("next message = %q, want = %q", buf.String(), tt.wantNext)
				}
			})
		}
	})

	// The hello then blob subtest reads a HELLO like map reply followed
	// by a large blob string reply, the map is formatted in the
	// redis-cli like format and the blob is formatted raw.
	t.Run("hello then blob", func(t *testing.T) {
		var b bytes.Buffer
		b.WriteString("%2\r\n$6\r\nserver\r\n$5\r\nredis\r\n$7\r\nmodules\r\n*2\r\n")
		for i := range 2 {
			b.WriteString("%1\r\n$4\r\nname\r\n$2\r\nm" + strconv.Itoa(i) + "\r\n")
		}
		blob := strings.Repeat("x", 100000)
		b.WriteString("$" + strconv.Itoa(len(blob)) + "\r\n" + blob + "\r\n")

		client, server := net.Pipe()
		defer server.Close()
		go func() {
			server.Write(b.Bytes())
			time.Sleep(time.Second)
			server.Close()
		}()

		c := &resp.Client{C: client, D: resp.NewRESP3Decoder(bytes.NewReader(b.Bytes()))}
		tok, err := c.ReadValue(t.Context())
		if err != nil {
			t.Fatalf("ReadValue() error = %v", err)
		}
		var buf bytes.Buffer
		if _, err := io.Copy(&buf, NewTokenReader(c, tok)); err != nil {
			t.Fatal(err)
		}
		want := "1# \"server\" => \"redis\"\n2# \"modules\" => \n   1) 1# \"name\" => \"m0\"\n   2) 1# \"name\" => \"m1\"\n"
		if buf.String() != want {
			t.Errorf("message = %q, want = %q", buf.String(), want)
		}
		tok, err = c.ReadValue(t.Context())
		if err != nil {
			t.Fatalf("ReadValue() error = %v", err)
		}
		buf.Reset()
		if _, err := io.Copy(&buf, NewTokenReader(c, tok)); err != nil {
			t.Fatal(err)
		}
		if buf.String() != blob+"\n" {
			t.Errorf("the blob was not formatted raw, message = %d bytes", buf.Len())
		}
	})

	// The chunked blob reply subtest reads a large blob string reply
	// that arrives in chunks over a real TCP connection. The blob
	// header and its payload straddle the reads of the connection
	// buffer, a reply token must not alias it.
	t.Run("chunked blob reply over tcp", func(t *testing.T) {
		var b bytes.Buffer
		b.WriteString("%1\r\n$1\r\nk\r\n$1\r\nv\r\n")
		blob := strings.Repeat("x", 100000)
		b.WriteString("$" + strconv.Itoa(len(blob)) + "\r\n" + blob + "\r\n")

		ls, err := net.Listen("tcp", "localhost:0")
		if err != nil {
			t.Fatal(err)
		}
		defer ls.Close()
		go func() {
			c, err := ls.Accept()
			if err != nil {
				return
			}
			defer c.Close()
			wire := b.Bytes()
			for i := 0; i < len(wire); i += 16384 {
				end := min(i+16384, len(wire))
				if _, err := c.Write(wire[i:end]); err != nil {
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
			time.Sleep(time.Second)
		}()
		conn, err := net.Dial("tcp", ls.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		c := &resp.Client{C: conn, D: resp.NewRESP3Decoder(conn)}
		tok, err := c.ReadValue(t.Context())
		if err != nil {
			t.Fatalf("first ReadValue() error = %v", err)
		}
		var buf bytes.Buffer
		if _, err := io.Copy(&buf, NewTokenReader(c, tok)); err != nil {
			t.Fatal(err)
		}
		if want := "1# \"k\" => \"v\"\n"; buf.String() != want {
			t.Errorf("message = %q, want the map", buf.String())
		}
		tok, err = c.ReadValue(t.Context())
		if err != nil {
			t.Fatalf("second ReadValue() error = %v", err)
		}
		buf.Reset()
		if _, err := io.Copy(&buf, NewTokenReader(c, tok)); err != nil {
			t.Fatal(err)
		}
		if buf.String() != blob+"\n" {
			t.Errorf("the blob was not formatted raw, message = %d bytes", buf.Len())
		}
	})
}
