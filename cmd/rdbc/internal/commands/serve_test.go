package commands

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coding-socks/rdb/resp"
)

// dialReplica dials the address and returns the connection with the
// deadline that keeps a broken server from hanging the test.
func dialReplica(t *testing.T, addr net.Addr) net.Conn {
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

// assertSilence fails the test when the server writes anything to nc
// before the deadline d, a timeout of the read is the silence, an end
// of the connection is a close.
func assertSilence(t *testing.T, nc net.Conn, d time.Duration) {
	t.Helper()
	if err := nc.SetReadDeadline(time.Now().Add(d)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1)
	if n, err := nc.Read(buf); err == nil {
		t.Errorf("the server wrote %q to the replica", buf[:n])
	} else if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Errorf("Read() err = %v, want the deadline of the read", err)
	}
}

func Test_newReplicaServer(t *testing.T) {
	file := filepath.Join(t.TempDir(), "dump.rdb")
	if err := os.WriteFile(file, []byte("RDB"), 0o644); err != nil {
		t.Fatal(err)
	}
	replid := "0123456789abcdef0123"
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go newReplicaServer(file, replid, "").Serve(l)
	t.Cleanup(func() { l.Close() })
	addr := l.Addr()

	t.Run("sync offset is zero", func(t *testing.T) {
		nc := dialReplica(t, addr)
		if _, err := fmt.Fprint(nc, "*1\r\n$4\r\nSYNC\r\n"); err != nil {
			t.Fatal(err)
		}
		d := resp.NewRESP3Decoder(nc)
		tok, err := d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := tok.String(), "FULLRESYNC "+replid+" 0"; got != want {
			t.Errorf("FULLRESYNC = %q, want %q", got, want)
		}
		tok, err = d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := string(tok.Bytes()), "RDB"; got != want {
			t.Errorf("RDB payload = %q, want %q", got, want)
		}
	})

	t.Run("ping is answered before the sync", func(t *testing.T) {
		nc := dialReplica(t, addr)
		if _, err := fmt.Fprint(nc, "*1\r\n$4\r\nPING\r\n"); err != nil {
			t.Fatal(err)
		}
		tok, err := resp.NewRESP3Decoder(nc).ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := tok.String(), "PONG"; got != want {
			t.Errorf("PING reply = %q, want %q", got, want)
		}
	})

	t.Run("commands of a synced replica are ignored", func(t *testing.T) {
		nc := dialReplica(t, addr)
		if _, err := fmt.Fprint(nc, "*1\r\n$4\r\nSYNC\r\n"+
			"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n"+
			"*1\r\n$4\r\nPING\r\n"); err != nil {
			t.Fatal(err)
		}
		d := resp.NewRESP3Decoder(nc)
		if _, err := d.ReadToken(); err != nil { // FULLRESYNC
			t.Fatal(err)
		}
		if _, err := d.ReadToken(); err != nil { // the RDB payload
			t.Fatal(err)
		}
		// The REPLCONF ACK and the PING pipelined behind the sync are
		// read and discarded, the server answers neither.
		assertSilence(t, nc, 250*time.Millisecond)
	})

	t.Run("the connection stays open after the sync", func(t *testing.T) {
		nc := dialReplica(t, addr)
		if _, err := fmt.Fprint(nc, "*1\r\n$4\r\nSYNC\r\n"); err != nil {
			t.Fatal(err)
		}
		d := resp.NewRESP3Decoder(nc)
		if _, err := d.ReadToken(); err != nil { // FULLRESYNC
			t.Fatal(err)
		}
		if _, err := d.ReadToken(); err != nil { // the RDB payload
			t.Fatal(err)
		}
		// The replica is kept online, the PING of the link is read
		// and discarded and the server closes nothing.
		if _, err := fmt.Fprint(nc, "*1\r\n$4\r\nPING\r\n"); err != nil {
			t.Fatal(err)
		}
		assertSilence(t, nc, 250*time.Millisecond)
	})

	t.Run("protocol error closes the connection", func(t *testing.T) {
		nc := dialReplica(t, addr)
		if _, err := fmt.Fprint(nc, "*-5\r\n"); err != nil {
			t.Fatal(err)
		}
		d := resp.NewRESP3Decoder(nc)
		tok, err := d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(tok.String(), "Protocol error") {
			t.Errorf("protocol error reply = %q, want it to report the protocol error", tok.String())
		}
		if _, err := d.ReadToken(); err != io.EOF {
			t.Errorf("ReadToken() after a protocol error err = %v, want %v", err, io.EOF)
		}
	})

	t.Run("auth gates the commands when a password is set", func(t *testing.T) {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		go newReplicaServer(file, replid, "sekret").Serve(l)
		t.Cleanup(func() { l.Close() })

		nc := dialReplica(t, l.Addr())
		if _, err := fmt.Fprint(nc,
			"*1\r\n$4\r\nPING\r\n"+
				"*2\r\n$4\r\nAUTH\r\n$3\r\nbad\r\n"+
				"*2\r\n$4\r\nAUTH\r\n$6\r\nsekret\r\n"+
				"*1\r\n$4\r\nPING\r\n"); err != nil {
			t.Fatal(err)
		}
		d := resp.NewRESP3Decoder(nc)
		want := []struct {
			text string
		}{
			{"NOAUTH Authentication required"},
			{"WRONGPASS invalid username-password pair or user is disabled"},
			{"OK"},
			{"PONG"},
		}
		for i, w := range want {
			tok, err := d.ReadToken()
			if err != nil {
				t.Fatal(err)
			}
			if got := tok.String(); got != w.text {
				t.Errorf("reply %d = %q, want %q", i, got, w.text)
			}
		}
	})

	t.Run("a synced replica is pinged", func(t *testing.T) {
		replicaPingPeriod = 100 * time.Millisecond
		t.Cleanup(func() { replicaPingPeriod = 10 * time.Second })
		nc := dialReplica(t, addr)
		if _, err := fmt.Fprint(nc, "*1\r\n$4\r\nSYNC\r\n"); err != nil {
			t.Fatal(err)
		}
		d := resp.NewRESP3Decoder(nc)
		if _, err := d.ReadToken(); err != nil { // FULLRESYNC
			t.Fatal(err)
		}
		if _, err := d.ReadToken(); err != nil { // the RDB payload
			t.Fatal(err)
		}
		// The link carries the PING of the replication cron, a plain
		// command of one argument.
		tok, err := d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		if tok.Kind() != resp.KindArray || tok.Int() != 1 {
			t.Fatalf("PING = %s of %d, want an array of one", tok.Kind(), tok.Int())
		}
		tok, err = d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := tok.String(), "PING"; got != want {
			t.Errorf("PING argument = %q, want %q", got, want)
		}
	})
}
