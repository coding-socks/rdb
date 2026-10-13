package chain

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/coding-socks/rdb"
	"github.com/coding-socks/rdb/aof"
	"github.com/coding-socks/rdb/clusters"
	"github.com/coding-socks/rdb/dump"
	"github.com/coding-socks/rdb/resp"
	"github.com/coding-socks/rdb/resp/resptest"
)

func TestSyncSrc_Run(t *testing.T) {
	// The payload of every node is a minimal RDB file, the SYNC
	// transfer of a node carries it behind the handshake. A record
	// of a string names its key and carries its value.
	encode := func(key string) []byte {
		var buf bytes.Buffer
		enc := rdb.NewFileEncoder(&buf, rdb.RedisDialect{})
		if err := enc.WriteToken(rdb.Op(rdb.RedisOpCodeString)); err != nil {
			t.Fatal(err)
		}
		if err := enc.WriteToken(rdb.Str(key)); err != nil {
			t.Fatal(err)
		}
		if err := enc.WriteToken(rdb.Str("value")); err != nil {
			t.Fatal(err)
		}
		if err := enc.WriteToken(rdb.Op(rdb.RedisOpCodeEndOfRDB)); err != nil {
			t.Fatal(err)
		}
		return buf.Bytes()
	}
	payload1, payload2 := encode("k1"), encode("k2")

	t.Run("cluster payload merge", func(t *testing.T) {
		// The SYNC handler of a node writes the handshake of the
		// sync and the payload of the node behind it.
		syncHandler := func(payload []byte) resp.Handler {
			mux := resp.NewServeMux()
			mux.HandleFunc("SYNC", func(w resp.ResponseWriter, cmd *resp.Command) {
				w.Proto().WriteToken(resp.String("FULLRESYNC 0123456789abcdef0123 0"))
				w.Proto().WriteToken(resp.Bytes(payload))
			})
			return mux
		}
		cl := resptest.NewCluster(t,
			resptest.Shard{ID: "node-0", Slots: []rdb.SlotRange{{Start: 0, End: 8191}}, Handler: syncHandler(payload1)},
			resptest.Shard{ID: "node-1", Slots: []rdb.SlotRange{{Start: 8192, End: 16383}}, Handler: syncHandler(payload2)},
		)
		c, err := clusters.Upgrade(t.Context(), cl.Node("node-0").Client())
		if err != nil {
			t.Fatalf("Upgrade() error = %v", err)
		}
		defer c.Close()
		src := NewSyncServerSrc(c)
		var got bytes.Buffer
		dst := NewRDBDst(&got)
		if err := src.Run(t.Context(), dst); !errors.Is(err, io.EOF) {
			t.Fatalf("Run() error = %v, want io.EOF", err)
		}
		// The payload of a node ends its tokens, the tokens of the
		// next node follow them in one stream, the header of the
		// file the encoder writes once and the checksum of the
		// stream it closes with.
		body := got.Bytes()[:len(got.Bytes())-8]
		want := append(bytes.Clone(payload1), payload2[9:len(payload2)-8]...)
		if !bytes.Equal(body, want) {
			t.Fatalf("the tokens = %#x, want %#x", body, want)
		}
		wantProgress := []ProgressReport{
			{I: uint64(len(payload1)), N: uint64(len(payload1)), Unit: "bytes"},
			{I: uint64(len(payload2)), N: uint64(len(payload2)), Unit: "bytes"},
		}
		if got := src.Progress(); !slices.Equal(got, wantProgress) {
			t.Errorf("Progress() = %v, want %v", got, wantProgress)
		}
	})

	t.Run("the handshake of a single instance", func(t *testing.T) {
		tests := []struct {
			name    string
			reply   string
			wantN   uint64
			wantErr bool
		}{
			{
				name:  "the payload follows the handshake",
				reply: "+FULLRESYNC 0123456789abcdef0123 0\r\n$" + strconv.Itoa(len(payload1)) + "\r\n" + string(payload1) + "\r\n",
				wantN: uint64(len(payload1)),
			},
			{
				name:  "the payload of the EOF-marked form ends at its marker",
				reply: "+FULLRESYNC 0123456789abcdef0123 0\r\n$EOF:0123456789abcdef0123456789abcdef01234567\r\n" + string(payload1) + "0123456789abcdef0123456789abcdef01234567",
			},
			{
				name:  "the server replies with the payload alone",
				reply: "$" + strconv.Itoa(len(payload1)) + "\r\n" + string(payload1) + "\r\n",
				wantN: uint64(len(payload1)),
			},
			{
				name:    "an error reply fails the transfer",
				reply:   "-ERR replication is not allowed\r\n",
				wantErr: true,
			},
			{
				name:    "a null bulk string carries no payload",
				reply:   "$-1\r\n",
				wantErr: true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				s := resptest.NewServer(t, func() resp.Handler {
					mux := resp.NewServeMux()
					mux.HandleFunc("SYNC", func(w resp.ResponseWriter, r *resp.Command) {
						conn, _ := w.Hijack()
						conn.Write([]byte(tt.reply))
					})
					return mux
				}())

				src := NewSyncServerSrc(clusters.Single(s.Client()))
				var got bytes.Buffer
				dst := NewRDBDst(&got)
				err := src.Run(t.Context(), dst)
				if tt.wantErr {
					if err == nil || errors.Is(err, io.EOF) {
						t.Fatalf("Run() error = %v, want an error", err)
					}
					return
				}
				if !errors.Is(err, io.EOF) {
					t.Fatalf("Run() error = %v, want io.EOF", err)
				}
				if !bytes.Equal(got.Bytes(), payload1) {
					t.Errorf("the payload = %q, want %q", got.Bytes(), payload1)
				}
				p := src.Progress()
				if len(p) != 1 || p[0].N != tt.wantN || p[0].I != uint64(len(payload1)) || p[0].Unit != "bytes" {
					t.Errorf("Progress() = %v, want %d of %d bytes", p, len(payload1), tt.wantN)
				}
			})
		}
	})
}

// reply returns the handler that writes the tokens of a static reply,
// the header of an aggregate and its elements arrive one after the
// other.
func reply(tokens ...resp.Token) resp.HandlerFunc {
	return func(w resp.ResponseWriter, cmd *resp.Command) {
		for _, tok := range tokens {
			if err := w.Proto().WriteToken(tok); err != nil {
				return
			}
		}
	}
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

func TestDumpSrc_Run(t *testing.T) {
	// The payload of a DUMP: the type of the value, its encoding,
	// the version of the file and the checksum.
	payload := string([]byte{0x00, 'v', 'a', 'l', 0x09, 0x00, 0, 0, 0, 0, 0, 0, 0, 0})

	t.Run("the keyspaces of the nodes follow one another", func(t *testing.T) {
		// The node of a key serves the page of the scan with the
		// key alone, every command of the scan names the key of
		// the node it reaches.
		node := func(key string) resp.Handler {
			mux := resp.NewServeMux()
			mux.HandleFunc("DBSIZE", func(w resp.ResponseWriter, cmd *resp.Command) {
				w.Proto().WriteToken(resp.Int(1))
			})
			mux.HandleFunc("SCAN", func(w resp.ResponseWriter, cmd *resp.Command) {
				if args := argsOf(cmd); !slices.Equal(args, []string{"SCAN", "0", "COUNT", "100"}) {
					t.Errorf("the server served %v, want [SCAN 0 COUNT 100]", args)
				}
				w.Proto().WriteToken(resp.ArrayLen(2))
				w.Proto().WriteToken(resp.Bytes([]byte("0")))
				w.Proto().WriteToken(resp.ArrayLen(1))
				w.Proto().WriteToken(resp.Bytes([]byte(key)))
			})
			mux.HandleFunc("TYPE", func(w resp.ResponseWriter, cmd *resp.Command) {
				if args := argsOf(cmd); !slices.Equal(args, []string{"TYPE", key}) {
					t.Errorf("the server served %v, want [TYPE %s]", args, key)
				}
				w.Proto().WriteToken(resp.String("string"))
			})
			mux.HandleFunc("DUMP", func(w resp.ResponseWriter, cmd *resp.Command) {
				if args := argsOf(cmd); !slices.Equal(args, []string{"DUMP", key}) {
					t.Errorf("the server served %v, want [DUMP %s]", args, key)
				}
				w.Proto().WriteToken(resp.Bytes([]byte(payload)))
			})
			mux.HandleFunc("PTTL", func(w resp.ResponseWriter, cmd *resp.Command) {
				if args := argsOf(cmd); !slices.Equal(args, []string{"PTTL", key}) {
					t.Errorf("the server served %v, want [PTTL %s]", args, key)
				}
				w.Proto().WriteToken(resp.Int(1000))
			})
			return mux
		}
		cl := resptest.NewCluster(t,
			resptest.Shard{ID: "node-0", Slots: []rdb.SlotRange{{Start: 0, End: 8191}}, Handler: node("k1")},
			resptest.Shard{ID: "node-1", Slots: []rdb.SlotRange{{Start: 8192, End: 16383}}, Handler: node("k2")},
		)
		c, err := clusters.Upgrade(t.Context(), cl.Node("node-0").Client())
		if err != nil {
			t.Fatalf("Upgrade() error = %v", err)
		}
		defer c.Close()

		src := NewDumpServerSrc(c)
		rec := NewRecorderDst()
		if err := src.Run(t.Context(), rec); !errors.Is(err, io.EOF) {
			t.Fatalf("Run() error = %v, want io.EOF", err)
		}
		toks := rec.Received()
		if len(toks) != 3 {
			t.Fatalf("the stream produced %d tokens, want the head and two keys", len(toks))
		}
		head, ok := toks[0].(Head)
		if !ok || head.Kind != KindDump {
			t.Fatalf("the first token = %v, want the dump head", toks[0])
		}
		for i, key := range []string{"k1", "k2"} {
			dt, ok := toks[i+1].(dump.Token)
			if !ok {
				t.Fatalf("the token %d = %T, want a dump token", i+1, toks[i+1])
			}
			if string(dt.Key) != key {
				t.Errorf("the key = %q, want %q", dt.Key, key)
			}
		}
		want := []ProgressReport{
			{I: 1, N: 1, Unit: "keys"},
			{I: 1, N: 1, Unit: "keys"},
		}
		if got := src.Progress(); !slices.Equal(got, want) {
			t.Errorf("Progress() = %v, want %v", got, want)
		}
	})
}

func TestRDBFileSrc_Progress(t *testing.T) {
	data := writeRDB(t, rdb.ValkeyDialect{V: 80},
		rdb.Op(rdb.RedisOpCodeString),
		rdb.Str("foo"), rdb.Str("bar"),
		rdb.Op(rdb.RedisOpCodeEndOfRDB),
	).(*bytes.Buffer).Bytes()

	t.Run("the size of the file is the total", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "dump.rdb")
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		src := NewRDBFileSrc(f, nil)
		if err := src.Run(t.Context(), NewRecorderDst()); !errors.Is(err, io.EOF) {
			t.Fatalf("Run() error = %v, want io.EOF", err)
		}
		ps := src.Progress()
		if len(ps) != 1 || ps[0].N != uint64(len(data)) || ps[0].I != uint64(len(data)) || ps[0].Unit != "bytes" {
			t.Errorf("Progress() = %v, want %d of %d bytes", ps, len(data), len(data))
		}
	})

	t.Run("a reader that is not a file has no total", func(t *testing.T) {
		src := NewRDBFileSrc(bytes.NewReader(data), nil)
		if err := src.Run(t.Context(), NewRecorderDst()); !errors.Is(err, io.EOF) {
			t.Fatalf("Run() error = %v, want io.EOF", err)
		}
		ps := src.Progress()
		if len(ps) != 1 || ps[0].N != 0 || ps[0].I != uint64(len(data)) || ps[0].Unit != "bytes" {
			t.Errorf("Progress() = %v, want %d bytes of unknown total", ps, len(data))
		}
	})
}

func TestAOFFileSrc_Progress(t *testing.T) {
	data := []byte("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")

	t.Run("the size of the file is the total", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "appendonly.aof")
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		src := NewAOFFileSrc(f, nil)
		if err := src.Run(t.Context(), NewRecorderDst()); !errors.Is(err, io.EOF) {
			t.Fatalf("Run() error = %v, want io.EOF", err)
		}
		ps := src.Progress()
		if len(ps) != 1 || ps[0].N != uint64(len(data)) || ps[0].I != uint64(len(data)) || ps[0].Unit != "bytes" {
			t.Errorf("Progress() = %v, want %d of %d bytes", ps, len(data), len(data))
		}
	})

	t.Run("a reader that is not a file has no total", func(t *testing.T) {
		src := NewAOFFileSrc(bytes.NewReader(data), nil)
		if err := src.Run(t.Context(), NewRecorderDst()); !errors.Is(err, io.EOF) {
			t.Fatalf("Run() error = %v, want io.EOF", err)
		}
		ps := src.Progress()
		if len(ps) != 1 || ps[0].N != 0 || ps[0].I != uint64(len(data)) || ps[0].Unit != "bytes" {
			t.Errorf("Progress() = %v, want %d bytes of unknown total", ps, len(data))
		}
	})
}

func TestManifestSrc_Progress(t *testing.T) {
	base := writeRDB(t, rdb.ValkeyDialect{V: 80},
		rdb.Op(rdb.RedisOpCodeString),
		rdb.Str("foo"), rdb.Str("bar"),
		rdb.Op(rdb.RedisOpCodeEndOfRDB),
	).(*bytes.Buffer).Bytes()
	incr := []byte("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
	archive := bytes.NewBuffer(writeArchive(t,
		manifestEntry{"appendonly.aof.1.base.rdb", base},
		manifestEntry{"appendonly.aof.1.incr.aof", incr},
		manifestEntry{"appendonly.aof.2.incr.aof", incr},
	))

	src := NewManifestSrc(archive, nil)
	if err := src.Run(t.Context(), NewRecorderDst()); !errors.Is(err, io.EOF) {
		t.Fatalf("Run() error = %v, want io.EOF", err)
	}
	want := []ProgressReport{
		{I: uint64(len(base)), N: uint64(len(base)), Unit: "bytes"},
		{I: uint64(len(incr)), N: uint64(len(incr)), Unit: "bytes"},
		{I: uint64(len(incr)), N: uint64(len(incr)), Unit: "bytes"},
	}
	if got := src.Progress(); !slices.Equal(got, want) {
		t.Errorf("Progress() = %v, want %v", got, want)
	}
}

// writeArchive writes the files of a manifest archive in the order
// they are given.
func writeArchive(t *testing.T, entries ...manifestEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, e := range entries {
		if err := tw.WriteHeader(&tar.Header{Name: e.name, Mode: 0o644, Size: int64(len(e.data))}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(e.data); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// TestAOFServerSrc reads the keyspace of a real server in a Docker
// container with testcontainers: the commands the read produces are
// compared with the commands that store the keys, the PEXPIREAT of
// an expiring key carries the expiry the seed fixed in time. The
// container needs Docker, the test is skipped when it is not usable
// and under the short flag.
func TestAOFServerSrc(t *testing.T) {
	if testing.Short() {
		t.Skip("the test runs a server in a Docker container")
	}

	cont := resptest.RunTestcontainer(t)

	seed := cont.Client()
	started := time.Now().UnixMilli()
	table := seed.Dialect().Commands()

	want := map[string][]aof.Token{
		"str:small": {
			{[]byte("UNLINK"), []byte("str:small")},
			{[]byte("APPEND"), []byte("str:small"), []byte("v")},
		},
		"str:big": {
			{[]byte("UNLINK"), []byte("str:big")},
			{[]byte("APPEND"), []byte("str:big"), bytes.Repeat([]byte("0123456789"), readStringMax/10)},
			{[]byte("APPEND"), []byte("str:big"), []byte("the tail")},
		},
		"list:small": {
			{[]byte("UNLINK"), []byte("list:small")},
			{[]byte("RPUSH"), []byte("list:small"), []byte("a")},
			{[]byte("RPUSH"), []byte("list:small"), []byte("b")},
			{[]byte("RPUSH"), []byte("list:small"), []byte("c")},
		},
		"ttl:str": {
			{[]byte("UNLINK"), []byte("ttl:str")},
			{[]byte("APPEND"), []byte("ttl:str"), []byte("x")},
			{[]byte("PEXPIREAT"), []byte("ttl:str"), []byte(strconv.FormatInt(started+1000_000, 10))},
		},
		"ttl:hash": {
			{[]byte("UNLINK"), []byte("ttl:hash")},
			{[]byte("HSET"), []byte("ttl:hash"), []byte("f"), []byte("v")},
			{[]byte("PEXPIREAT"), []byte("ttl:hash"), []byte(strconv.FormatInt(started+5000_000, 10))},
		},
		"ttl:field": {
			{[]byte("UNLINK"), []byte("ttl:field")},
			{[]byte("HSET"), []byte("ttl:field"), []byte("f"), []byte("v")},
			{[]byte("HPEXPIREAT"), []byte("ttl:field"), []byte(strconv.FormatInt(started+2500_000, 10)), []byte("FIELDS"), []byte("1"), []byte("f")},
		},
		"list:big": {
			{[]byte("UNLINK"), []byte("list:big")},
		},
		"set:big": {
			{[]byte("UNLINK"), []byte("set:big")},
		},
		"zset:big": {
			{[]byte("UNLINK"), []byte("zset:big")},
		},
		"hash:big": {
			{[]byte("UNLINK"), []byte("hash:big")},
		},
		"stream:big": {
			{[]byte("UNLINK"), []byte("stream:big")},
		},
		"bf:key": {
			{[]byte("BF.LOADCHUNK"), []byte("bf:key"), []byte("1"), gz64(t, "H4sIAEhSx2oC/2NigABGIGYFYiYgngAVa2CB0ExQfrXIOveHVSX2L9v0N5nyqTlwAMVSoHIMAGwjqH1JAAAA")},
			{[]byte("BF.LOADCHUNK"), []byte("bf:key"), []byte("145"), gz64(t, "H4sIAEhSx2oC/1NgQAMNDAIMjCgiTOhKNDCFEIAFiB2QBQTAQkjAAcNGTgQHABwcK0WQAAAA")},
		},
		"cf:key": {
			{[]byte("CF.LOADCHUNK"), []byte("cf:key"), []byte("1"), gz64(t, "H4sIAEhSx2oC/2NkgAImBhTACBcWAbIB7WXZxSYAAAA=")},
			{[]byte("CF.LOADCHUNK"), []byte("cf:key"), []byte("1025"), gz64(t, "H4sIAEhSx2oC/2NgGAWjYBQMDPg84C4AAFUtzdQABAAA")},
		},
		"ts:key": {
			{[]byte("UNLINK"), []byte("ts:key")},
		},
		"ts:dst": {
			{[]byte("UNLINK"), []byte("ts:dst")},
		},
	}
	for i := range 37 {
		want["list:big"] = append(want["list:big"],
			aof.Token{[]byte("RPUSH"), []byte("list:big"), []byte(fmt.Sprintf("m%02d", i))})
		want["set:big"] = append(want["set:big"],
			aof.Token{[]byte("SADD"), []byte("set:big"), []byte(fmt.Sprintf("m%02d", i))})
		want["zset:big"] = append(want["zset:big"],
			aof.Token{[]byte("ZADD"), []byte("zset:big"), []byte(strconv.Itoa(i)), []byte(fmt.Sprintf("m%02d", i))})
		want["hash:big"] = append(want["hash:big"],
			aof.Token{[]byte("HSET"), []byte("hash:big"), []byte(fmt.Sprintf("f%02d", i)), []byte(fmt.Sprintf("v%02d", i))})
	}
	for i := range 3 {
		want["stream:big"] = append(want["stream:big"], aof.Token{
			[]byte("XADD"), []byte("stream:big"),
			[]byte(fmt.Sprintf("%d-1", i+1)), []byte("field"), []byte(fmt.Sprintf("v%02d", i)),
		})
	}
	want["stream:big"] = append(want["stream:big"],
		aof.Token{[]byte("XGROUP"), []byte("CREATE"), []byte("stream:big"), []byte("g1"), []byte("3-1"), []byte("MKSTREAM"), []byte("ENTRIESREAD"), []byte("3")},
		aof.Token{[]byte("XGROUP"), []byte("CREATE"), []byte("stream:big"), []byte("g2"), []byte("1-0"), []byte("MKSTREAM")},
	)
	want["json:key"] = []aof.Token{
		{[]byte("JSON.SET"), []byte("json:key"), []byte("$"), []byte(`{"a":1,"b":[1,2]}`)},
	}
	want["ts:key"] = []aof.Token{
		{[]byte("TS.CREATE"), []byte("ts:key"), []byte("RETENTION"), []byte("10000"), []byte("CHUNK_SIZE"), []byte("128"), []byte("DUPLICATE_POLICY"), []byte("block"), []byte("LABELS"), []byte("k"), []byte("v")},
		{[]byte("TS.ADD"), []byte("ts:key"), []byte("1000"), []byte("1.5")},
		{[]byte("TS.ADD"), []byte("ts:key"), []byte("2000"), []byte("2.5")},
		{[]byte("TS.CREATERULE"), []byte("ts:key"), []byte("ts:dst"), []byte("AGGREGATION"), []byte("AVG"), []byte("1000")},
	}
	want["ts:dst"] = []aof.Token{
		{[]byte("TS.CREATE"), []byte("ts:dst"), []byte("CHUNK_SIZE"), []byte("4096"), []byte("DUPLICATE_POLICY"), []byte("block")},
	}

	pipe := seed.Pipe()
	for _, k := range slices.Sorted(maps.Keys(want)) {
		for _, cmd := range want[k] {
			if err := pipe.Write(cmd...); err != nil {
				t.Fatalf("failed to write command to pipe: %v", err)
			}
		}
	}
	if err := pipe.Flush(); err != nil {
		t.Fatalf("failed to flush pipe: %v", err)
	}
	for _, cmds := range want {
		for range cmds {
			reply, err := seed.ReadValue(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if reply.Kind() == resp.KindError {
				t.Fatalf("the seed failed: %v", reply.Err())
			}
			if err := resp.SkipReply(seed.D, reply); err != nil {
				t.Fatalf("failed to skip reply: %v", err)
			}
		}
	}

	cl := clusters.Single(seed)
	defer cl.Close()
	rec := NewRecorderDst()
	if err := NewAOFServerSrc(cl).Run(t.Context(), rec); !errors.Is(err, io.EOF) {
		t.Fatalf("Run() error = %v, want io.EOF", err)
	}

	got := map[string][]aof.Token{}
	for _, tok := range rec.Received() {
		cmd, ok := tok.(aof.Token)
		if !ok {
			continue
		}
		entry, err := table.Lookup(cmd)
		if err != nil {
			t.Fatalf("Lookup(%v) error = %v", cmd, err)
		}
		keys, err := entry.ExtractKeys(cmd)
		if err != nil {
			t.Fatalf("ExtractKeys(%v) error = %v", cmd, err)
		}
		key := string(keys[0])
		got[key] = append(got[key], cmd)
	}
	if len(got) != len(want) {
		t.Errorf("the read stored %d keys, want %d", len(got), len(want))
	}
	for key, wantCmds := range want {
		gotCmds, ok := got[key]
		if !ok {
			t.Errorf("the read misses %q", key)
			continue
		}
		if len(gotCmds) != len(wantCmds) {
			t.Errorf("%q produced %d commands, want %d", key, len(gotCmds), len(wantCmds))
			continue
		}
		for i := range wantCmds {
			if !slices.EqualFunc(gotCmds[i], wantCmds[i], bytes.Equal) {
				t.Errorf("the command %d of %q = %v, want %v", i, key, gotCmds[i], wantCmds[i])
			}
		}
	}
}

// gz64 decodes a chunk of the filters of TestAOFServerSrc, base64 encoded
// gzip, the encoding keeps the zeros of the chunks out of the source.
func gz64(t *testing.T, s string) []byte {
	t.Helper()
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	z, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	out, err := io.ReadAll(z)
	if err != nil {
		t.Fatal(err)
	}
	return out
}
