package commands

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/coding-socks/rdb/resp"
)

func Serve(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), `usage: rdbc serve [-auth PASSWORD] [-logfile FILE] RDB_FILE [HOST[:PORT]]

Loads an RDB file and listens for a Redis compatible server to connect
as a replica. The address to listen on is HOST:PORT, :6379 when it is missing.
The replica handshake is answered: AUTH, HELLO, PING, REPLCONF, SYNC and PSYNC.
A replica is served a FULLRESYNC reply followed by the RDB file,
after which the connection is kept open: the commands of the replica
link are read and discarded and a PING is sent every 10 seconds, the
period a Redis master keeps a synced replica alive with.
`)
		fs.PrintDefaults()
	}
	auth := fs.String("auth", "", "enable password protection")
	logfile := fs.String("logfile", "", "target log file")
	fs.Parse(args)

	var addr string
	var file string
	switch fs.NArg() {
	default:
		fs.Usage()
		return errors.New("rdbc serve: missing RDB file")
	case 1:
		file = fs.Arg(0)
		addr = ":6379"
	case 2:
		file = fs.Arg(0)
		addr = fs.Arg(1)
		if _, _, err := net.SplitHostPort(addr); err != nil {
			addr += ":6379"
		}
	}
	_, err := os.Stat(file)
	if err != nil {
		return err
	}

	replid := make([]byte, 20)
	if _, err := rand.Read(replid); err != nil {
		return err
	}

	var logf *os.File
	if *logfile != "" {
		logf, err = os.OpenFile(*logfile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return err
		}
		defer logf.Close()
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		ln.Close()
	}()
	srv := newReplicaServer(file, hex.EncodeToString(replid), *auth)
	if logf != nil {
		srv.SetLogger(slog.New(slog.NewTextHandler(logf, nil)))
	} else {
		srv.SetLogger(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	}
	err = srv.Serve(ln)
	if err := ctx.Err(); err != nil {
		if !errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}
	return err
}

// replicaServer serves the RDB file of the serve command to the
// replicas that connect to it.
type replicaServer struct {
	srv    *resp.Server
	file   string
	replid string
	log    *slog.Logger
}

// SetLogger sets the logger the server writes with. The server logs
// nothing until it is called.
func (s *replicaServer) SetLogger(l *slog.Logger) {
	s.log = l
}

// newReplicaServer builds the server that answers the replica
// handshake of the serve command. The AUTH command is gated by the
// password, the mux serves REPLCONF, SYNC and PSYNC and the SYNC and
// PSYNC handlers take the connection over after the payload of the
// RDB file.
func newReplicaServer(file, replid, password string) *replicaServer {
	s := &replicaServer{
		file:   file,
		replid: replid,
		log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	mux := resp.NewServeMux()
	mux.HandleFunc("REPLCONF", s.replconf)
	mux.HandleFunc("SYNC", s.sync)
	mux.HandleFunc("PSYNC", s.sync)
	s.srv = &resp.Server{
		Handler:  mux,
		Password: password,
	}
	return s
}

func (s *replicaServer) Serve(ln net.Listener) error {
	s.log.Info("serving file", "file", s.file, "addr", ln.Addr())
	return s.srv.Serve(logListener{Listener: ln, log: s.log})
}

func (s *replicaServer) replconf(w resp.ResponseWriter, cmd *resp.Command) {
	w.Proto().WriteToken(resp.String("OK"))
}

// replicaPingPeriod is the period of the PING the sync handler sends
// to a synced replica, the default of the repl-ping-replica-period
// of Redis. A replica drops the link of a master that sends nothing
// within its repl-timeout.
var replicaPingPeriod = 10 * time.Second

func (s *replicaServer) sync(w resp.ResponseWriter, cmd *resp.Command) {
	f, err := os.Open(s.file)
	if err != nil {
		w.Proto().WriteToken(resp.Error("ERR " + err.Error()))
		return
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		w.Proto().WriteToken(resp.Error("ERR " + err.Error()))
		return
	}
	e := w.Proto()
	e.WriteToken(resp.String("FULLRESYNC " + s.replid + " 0"))
	e.WriteToken(resp.BlobString(&io.LimitedReader{R: f, N: fi.Size()}))
	if err := w.Flush(); err != nil {
		s.log.Error("sync interrupted", "addr", w.RemoteAddr(), "err", err)
		return
	}
	s.log.Info("sync complete", "addr", w.RemoteAddr(), "bytes", fi.Size())
	nc, err := w.Hijack()
	if err != nil {
		s.log.Error("hijack failed", "err", err)
		return
	}
	s.log.Info("replica link open", "addr", nc.RemoteAddr())
	defer nc.Close()
	// The connection is the handler's, the context of the command
	// closes it on the shutdown of the serve command, the read
	// deadline keeps the loop from blocking past the next PING.
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-cmd.Context().Done():
		case <-done:
		}
		nc.Close()
	}()
	buf := make([]byte, 64)
	for {
		if err := nc.SetReadDeadline(time.Now().Add(replicaPingPeriod)); err != nil {
			s.log.Info("replica link closed", "addr", nc.RemoteAddr(), "reason", err)
			return
		}
		if _, err := nc.Read(buf); errors.Is(err, os.ErrDeadlineExceeded) {
			if err := writePing(e); err != nil {
				s.log.Info("replica link closed", "addr", nc.RemoteAddr(), "reason", err)
				return
			}
		} else if err != nil {
			s.log.Info("replica link closed", "addr", nc.RemoteAddr(), "reason", err)
			return
		}
	}
}

// writePing writes the PING of the replication cron, a plain command
// of one argument.
func writePing(e resp.Encoder) error {
	if err := e.WriteToken(resp.ArrayLen(1)); err != nil {
		return err
	}
	if err := e.WriteToken(resp.Bytes([]byte("PING"))); err != nil {
		return err
	}
	return e.Flush()
}

type logListener struct {
	net.Listener
	log *slog.Logger
}

func (l logListener) Accept() (net.Conn, error) {
	nc, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.log.Info("connection established", "addr", nc.RemoteAddr())
	return logConn{Conn: nc, log: l.log}, nil
}

type logConn struct {
	net.Conn
	log *slog.Logger
}

func (c logConn) Close() error {
	c.log.Info("connection closed", "addr", c.RemoteAddr())
	return c.Conn.Close()
}
