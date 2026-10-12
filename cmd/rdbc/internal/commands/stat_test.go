package commands

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"charm.land/bubbles/v2/progress"
	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/exp/teatest/v2"
	"github.com/coding-socks/rdb/internal/chain"
	"github.com/coding-socks/rdb/resp"
	"github.com/coding-socks/rdb/resp/resptest"
)

// statKey is one key of the keyspace a test serves.
type statKey struct {
	typ    string
	length int64 // reply of the length command, unused when the type has none
	mem    uint64
}

// statServer serves the commands of a stat walk over the keyspace of
// a test on a [resptest.Server]. The handlers assert the shape of the
// commands they serve and count the executed verbs, a command with no
// handler fails the walk with the error of an unknown command.
type statServer struct {
	srv  *resptest.Server
	keys map[string]statKey

	mu    sync.Mutex
	verbs map[string]int
	scans []string
}

func newStatServer(t *testing.T, keys map[string]statKey) *statServer {
	t.Helper()
	s := &statServer{keys: keys, verbs: make(map[string]int)}
	mux := resp.NewServeMux()
	mux.HandleFunc("DBSIZE", func(w resp.ResponseWriter, cmd *resp.Command) {
		s.served("DBSIZE")
		w.Proto().WriteToken(resp.Int(int64(len(s.keys))))
	})
	mux.HandleFunc("SCAN", func(w resp.ResponseWriter, cmd *resp.Command) {
		args := argsOf(cmd)
		if len(args) != 4 || args[2] != "COUNT" {
			t.Errorf("the server served %v, want [SCAN cursor COUNT count]", args)
			return
		}
		s.served("SCAN")
		// The walk serves the whole keyspace in one page.
		s.mu.Lock()
		s.scans = append(s.scans, args[1])
		s.mu.Unlock()
		w.Proto().WriteToken(resp.ArrayLen(2))
		w.Proto().WriteToken(resp.Bytes([]byte("0")))
		w.Proto().WriteToken(resp.ArrayLen(len(s.keys)))
		for key := range s.keys {
			w.Proto().WriteToken(resp.Bytes([]byte(key)))
		}
	})
	mux.HandleFunc("TYPE", func(w resp.ResponseWriter, cmd *resp.Command) {
		args := argsOf(cmd)
		if len(args) != 2 {
			t.Errorf("the server served %v, want [TYPE key]", args)
			return
		}
		s.served("TYPE")
		k, ok := s.keys[args[1]]
		if !ok {
			k = statKey{typ: "none"}
		}
		w.Proto().WriteToken(resp.String(k.typ))
	})
	mux.HandleFunc("MEMORY|USAGE", func(w resp.ResponseWriter, cmd *resp.Command) {
		args := argsOf(cmd)
		if len(args) != 5 || args[3] != "SAMPLES" {
			t.Errorf("the server served %v, want [MEMORY USAGE key SAMPLES n]", args)
			return
		}
		s.served("MEMORY")
		k, ok := s.keys[args[2]]
		if !ok {
			w.Proto().WriteToken(resp.Null)
			return
		}
		w.Proto().WriteToken(resp.Int(int64(k.mem)))
	})
	for _, name := range []string{"STRLEN", "LLEN", "SCARD", "ZCARD", "HLEN", "XLEN", "VCARD"} {
		mux.HandleFunc(name, func(w resp.ResponseWriter, cmd *resp.Command) {
			args := argsOf(cmd)
			if len(args) != 2 {
				t.Errorf("the server served %v, want [%s key]", args, name)
				return
			}
			s.served(name)
			k, ok := s.keys[args[1]]
			if !ok || k.length < 0 {
				w.Proto().WriteToken(resp.Int(0))
				return
			}
			w.Proto().WriteToken(resp.Int(k.length))
		})
	}
	s.srv = resptest.NewServer(t, mux)
	return s
}

// served counts the command the mux served under the name the test
// knows it by.
func (s *statServer) served(verb string) {
	s.mu.Lock()
	s.verbs[verb]++
	s.mu.Unlock()
}

func (s *statServer) executed(verb string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.verbs[verb]
}

func (s *statServer) cursors() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.scans...)
}

// argsOf returns the arguments of the command the server served, the
// name of the command among them.
func argsOf(cmd *resp.Command) []string {
	args := make([]string, len(cmd.Args))
	for i, arg := range cmd.Args {
		args[i] = string(arg)
	}
	return args
}

// newTestStatScanner dials the server and constructs a scanner over
// it. The reader function returns the messages the scanner delivered
// through its send.
func newTestStatScanner(t *testing.T, s *statServer) (*statScanner, func() []any) {
	t.Helper()
	var msgs []any
	conn := s.srv.Client()
	return &statScanner{
		conn:  conn,
		send:  func(msg reportMsg) { msgs = append(msgs, msg) },
		bulk:  chain.NewBulk(conn, chain.WithCount(10)),
		stats: *newKeyStat(2),
		types: newTypeStats(),
	}, func() []any { return msgs }
}

func Test_statScanner_Scan(t *testing.T) {
	// The keyspace exercises every branch of the read: a string and a
	// hash with a length command, a key of an unknown type without one
	// and a key that vanishes before its TYPE.
	keys := map[string]statKey{
		"str:aaaa": {typ: "string", length: 8, mem: 100},
		"str:bb":   {typ: "string", length: 2, mem: 200},
		"h:cccc":   {typ: "hash", length: 4, mem: 300},
		"w:dd":     {typ: "wobble", mem: 50},
		"gone":     {typ: "none", mem: 0},
	}

	t.Run("the walk folds the keyspace and reports", func(t *testing.T) {
		s := newStatServer(t, keys)
		scan, msgs := newTestStatScanner(t, s)
		if err := scan.Scan(t.Context()); err != nil {
			t.Fatal(err)
		}

		delivered := msgs()
		final, ok := delivered[len(delivered)-1].(reportMsg)
		if !ok {
			t.Fatalf("the last message = %T, want a reportMsg", delivered[len(delivered)-1])
		}
		if final.percent != 1 {
			t.Errorf("the final percent = %v, want 1", final.percent)
		}
		if final.cursor != 0 {
			t.Errorf("the report cursor = %d, want 0", final.cursor)
		}
		var beats int
		for _, msg := range delivered[:len(delivered)-1] {
			beat, ok := msg.(reportMsg)
			if !ok {
				t.Errorf("the message %T is not a reportMsg", msg)
				continue
			}
			if beat.percent >= 1 {
				t.Errorf("the walk delivered a percent of %v before its end", beat.percent)
			}
			beats++
		}
		if beats == 0 {
			t.Error("the walk delivered no progress")
		}

		r := final.report
		if r.sampled != 4 {
			t.Errorf("the report sampled %d keys, want 4", r.sampled)
		}
		if len(r.top) != 2 || r.top[0].key != "h:cccc" || r.top[1].key != "str:bb" {
			t.Errorf("the top keys = %+v, want [h:cccc str:bb]", r.top)
		}
		for _, ts := range r.types {
			switch ts.name {
			case "string":
				if ts.count != 2 || ts.totalLen != 10 {
					t.Errorf("string = %+v, want 2 keys and a total length of 10", ts)
				}
			case "hash":
				if ts.count != 1 || ts.totalLen != 4 {
					t.Errorf("hash = %+v, want 1 key and a total length of 4", ts)
				}
			case "wobble":
				if ts.count != 1 {
					t.Errorf("wobble = %+v, want 1 key", ts)
				}
			default:
				t.Errorf("unexpected type %q in the report", ts.name)
			}
		}

		// A command the mux does not serve fails the walk with the
		// error of an unknown command, the multiplicity of the served
		// ones is the assertion here.
		if got := s.executed("TYPE"); got != len(keys) {
			t.Errorf("TYPE ran %d times, want %d", got, len(keys))
		}
		if got := s.executed("MEMORY"); got != len(keys) {
			t.Errorf("MEMORY ran %d times, want %d", got, len(keys))
		}
		if got := s.executed("STRLEN"); got != 2 {
			t.Errorf("STRLEN ran %d times, want 2", got)
		}
		if got := s.executed("HLEN"); got != 1 {
			t.Errorf("HLEN ran %d times, want 1", got)
		}
	})

	t.Run("the walk resumes at the given cursor", func(t *testing.T) {
		s := newStatServer(t, keys)
		scan, msgs := newTestStatScanner(t, s)
		chain.WithCursor(42)(scan.bulk)
		if err := scan.Scan(t.Context()); err != nil {
			t.Fatal(err)
		}
		if cursors := s.cursors(); len(cursors) == 0 || cursors[0] != "42" {
			t.Errorf("scan cursors = %q, want it to start at 42", cursors)
		}
		if _, ok := msgs()[len(msgs())-1].(reportMsg); !ok {
			t.Error("the walk delivered no report")
		}
	})

	t.Run("an empty keyspace reports without keys", func(t *testing.T) {
		s := newStatServer(t, map[string]statKey{})
		scan, msgs := newTestStatScanner(t, s)
		if err := scan.Scan(t.Context()); err != nil {
			t.Fatal(err)
		}
		delivered := msgs()
		if len(delivered) != 1 {
			t.Fatalf("the walk delivered %d messages, want the report alone", len(delivered))
		}
		report, ok := delivered[0].(reportMsg)
		if !ok {
			t.Fatalf("the message = %T, want a reportMsg", delivered[0])
		}
		if report.report.sampled != 0 {
			t.Errorf("the report sampled %d keys, want 0", report.report.sampled)
		}
		if got := s.executed("DBSIZE"); got != 1 {
			t.Errorf("DBSIZE ran %d times, want once before the walk", got)
		}
	})

	t.Run("an error reply fails the walk", func(t *testing.T) {
		mux := resp.NewServeMux()
		mux.HandleFunc("DBSIZE", func(w resp.ResponseWriter, cmd *resp.Command) {
			w.Proto().WriteToken(resp.Int(1))
		})
		mux.HandleFunc("SCAN", func(w resp.ResponseWriter, cmd *resp.Command) {
			e := w.Proto()
			if err := e.WriteToken(resp.ArrayLen(2)); err != nil {
				t.Error(err)
				return
			}
			if err := e.WriteToken(resp.Bytes([]byte("0"))); err != nil {
				t.Error(err)
				return
			}
			if err := e.WriteToken(resp.ArrayLen(1)); err != nil {
				t.Error(err)
				return
			}
			if err := e.WriteToken(resp.Bytes([]byte("k"))); err != nil {
				t.Error(err)
			}
		})
		mux.HandleFunc("TYPE", func(w resp.ResponseWriter, cmd *resp.Command) {
			if err := w.Proto().WriteToken(resp.Error("WRONGTYPE key is a list")); err != nil {
				t.Error(err)
			}
		})

		var msgs []any
		conn := resptest.NewServer(t, mux).Client()
		scan := &statScanner{
			conn:  conn,
			send:  func(msg reportMsg) { msgs = append(msgs, msg) },
			bulk:  chain.NewBulk(conn, chain.WithCount(10)),
			stats: *newKeyStat(2),
			types: newTypeStats(),
		}
		err := scan.Scan(t.Context())
		if err == nil || err.Error() != "WRONGTYPE key is a list" {
			t.Fatalf("Scan() = %v, want the WRONGTYPE error", err)
		}
		if len(msgs) != 0 {
			t.Errorf("the failed walk delivered %d messages, want none, the error is the return value", len(msgs))
		}
	})
}

func Test_statModel_Update(t *testing.T) {
	t.Run("the report ends the scan and ctrl+c quits", func(t *testing.T) {
		tm := teatest.NewTestModel(t, statModel{
			progress: progress.New(progress.WithDefaultBlend()),
		}, teatest.WithInitialTermSize(100, 60))
		tm.Send(reportMsg{percent: 1, report: keyReport{
			sampled:  1,
			totalMem: 100,
			types: []typeStat{{
				name:  "string",
				count: 1, totalMem: 100, totalLen: 8,
				biggestMem: 100, biggestMemKey: "k",
				biggestLen: 8, biggestLenKey: "k",
			}},
			top: []keyInfo{{key: "k", typ: "string", size: 100}},
		}, cursor: 0})

		tm.Send(tea.KeyPressMsg{Code: 'c', Mod: tea.ModCtrl})
		tm.WaitFinished(t, teatest.WithFinalTimeout(2*time.Second))
		fm := tm.FinalModel(t).(statModel)
		if fm.progress.Percent() != 1 {
			t.Errorf("the report = %+v, want the delivered keyReport", fm.progress.Percent())
		}
	})

	t.Run("an error prints and quits", func(t *testing.T) {
		tm := teatest.NewTestModel(t, statModel{
			progress: progress.New(progress.WithDefaultBlend()),
		}, teatest.WithInitialTermSize(100, 60))
		tm.Send(errorMsg(errors.New("boom")))
		tm.WaitFinished(t, teatest.WithFinalTimeout(2*time.Second))
		out, err := io.ReadAll(tm.Output())
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(out, []byte("boom")) {
			t.Errorf("the output = %q, want it to name the error", out)
		}
	})
}
