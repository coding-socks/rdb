package chain

import (
	"bytes"
	"errors"
	"slices"
	"testing"

	"github.com/coding-socks/rdb"
	"github.com/coding-socks/rdb/aof"
	"github.com/coding-socks/rdb/clusters"
	"github.com/coding-socks/rdb/dump"
	"github.com/coding-socks/rdb/resp"
	"github.com/coding-socks/rdb/resp/resptest"
)

func Test_RDBDst_Process(t *testing.T) {
	t.Run("the head names the dialect of the file", func(t *testing.T) {
		var buf bytes.Buffer
		d := NewRDBDst(&buf)
		if err := d.Process(Head{Kind: KindRDB, Dialect: rdb.ValkeyDialect{V: 80}}); err != nil {
			t.Fatal(err)
		}
		if err := d.Process(rdb.Op(rdb.RedisOpCodeEndOfRDB)); err != nil {
			t.Fatal(err)
		}
		if err := d.Finish(); err != nil {
			t.Fatal(err)
		}
		b := buf.Bytes()
		if !bytes.HasPrefix(b, []byte("VALKEY080")) {
			t.Errorf("the file starts with %q, want VALKEY080", b[:min(len(b), 10)])
		}
	})

	tests := []struct {
		name  string
		token Token
	}{
		{name: "a head of another kind fails", token: Head{Kind: KindJSON, Dialect: rdb.RedisDialect{}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := NewRDBDst(&bytes.Buffer{})
			if err := d.Process(tt.token); err == nil {
				t.Error("Process() did not fail")
			}
		})
	}
}

func Test_ManifestDst_Process(t *testing.T) {
	t.Run("the base snapshot is written in the dialect of the head", func(t *testing.T) {
		var buf bytes.Buffer
		d := NewManifestDst(&buf)
		if err := d.Process(Head{Kind: KindRDB, Dialect: rdb.ValkeyDialect{V: 80}}); err != nil {
			t.Fatal(err)
		}
		if err := d.Process(rdb.Op(rdb.RedisOpCodeEndOfRDB)); err != nil {
			t.Fatal(err)
		}
		if err := d.Finish(); err != nil {
			t.Fatal(err)
		}
		base, _, err := manifestRead(buf.Bytes())
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.HasPrefix(base, []byte("VALKEY080")) {
			t.Errorf("the base snapshot starts with %q, want VALKEY080", base[:min(len(base), 10)])
		}
	})

	t.Run("a head of the base and the increments is accepted", func(t *testing.T) {
		var buf bytes.Buffer
		d := NewManifestDst(&buf)
		if err := d.Process(Head{Kind: KindRDB | KindAOF, Dialect: rdb.RedisDialect{}}); err != nil {
			t.Errorf("Process() with a mixed head error = %v", err)
		}
	})

	tests := []struct {
		name  string
		token Token
	}{
		{name: "a head of another kind fails", token: Head{Kind: KindJSON, Dialect: rdb.RedisDialect{}}},
		{name: "a head of only the increments fails", token: Head{Kind: KindAOF, Dialect: rdb.RedisDialect{}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := NewManifestDst(&bytes.Buffer{})
			if err := d.Process(tt.token); err == nil {
				t.Error("Process() did not fail")
			}
		})
	}

	t.Run("an end without a base snapshot fails", func(t *testing.T) {
		var buf bytes.Buffer
		d := NewManifestDst(&buf)
		if err := d.Finish(); err == nil {
			t.Error("Finish() without a base snapshot did not fail")
		}
	})
}

func Test_JSONDst_Process(t *testing.T) {
	var buf bytes.Buffer
	d := NewJSONDst(&buf)
	if err := d.Process(Head{Kind: KindJSON, Dialect: rdb.RedisDialect{}}); err != nil {
		t.Errorf("Process() with the JSON head error = %v", err)
	}
	if err := d.Process(Head{Kind: KindRDB, Dialect: rdb.RedisDialect{}}); err == nil {
		t.Error("Process() with an RDB head did not fail")
	}
	if err := d.Finish(); err != nil {
		t.Errorf("Finish() error = %v", err)
	}
}

func Test_AOFDst_Process(t *testing.T) {
	var buf bytes.Buffer
	d := NewAOFDst(&buf)
	if err := d.Process(Head{Kind: KindAOF, Dialect: rdb.RedisDialect{}}); err != nil {
		t.Errorf("Process() with the AOF head error = %v", err)
	}
	if err := d.Process(Head{Kind: KindRDB, Dialect: rdb.RedisDialect{}}); err == nil {
		t.Error("Process() with an RDB head did not fail")
	}
	if err := d.Finish(); err != nil {
		t.Errorf("Finish() error = %v", err)
	}
	d.Abort(errors.New("boom"))
}

func Test_ServerDst_Process(t *testing.T) {
	server := resptest.NewServer(t, nil)
	client := server.Client()
	client.SetDialect(rdb.ValkeyDialect{})
	d := NewServerDst(t.Context(), clusters.Single(client), KindDump)
	for _, tt := range []struct {
		name    string
		arg     Head
		wantErr bool
	}{
		{name: "matching", arg: Head{Kind: KindDump, Dialect: rdb.ValkeyDialect{}}},
		{name: "another family", arg: Head{Kind: KindDump, Dialect: rdb.RedisDialect{}}, wantErr: true},
		{name: "another kind", arg: Head{Kind: KindJSON, Dialect: rdb.ValkeyDialect{}}, wantErr: true},
		{name: "a kind the strategy does not take", arg: Head{Kind: KindAOF, Dialect: rdb.ValkeyDialect{}}, wantErr: true},
	} {
		t.Run("a head of "+tt.name+" fails", func(t *testing.T) {
			if err := d.Process(tt.arg); (err != nil) != tt.wantErr {
				t.Errorf("Process() err = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

func Test_ServerDst_route(t *testing.T) {
	// The node of a key is the one that serves its slot, the keys
	// of the test route to the two nodes of the cluster. The
	// RESTORE of a node names the key the node serves.
	restore := func(key string) resp.HandlerFunc {
		return func(w resp.ResponseWriter, cmd *resp.Command) {
			if args := argsOf(cmd); !slices.Equal(args, []string{"RESTORE", key, "0", "payload", "REPLACE"}) {
				t.Errorf("the server served %v, want [RESTORE %s 0 payload REPLACE]", args, key)
			}
			w.Proto().WriteToken(resp.String("OK"))
		}
	}
	node0 := func() resp.Handler {
		mux := resp.NewServeMux()
		mux.HandleFunc("RESTORE", restore("bar"))
		return mux
	}()
	node1 := func() resp.Handler {
		mux := resp.NewServeMux()
		mux.HandleFunc("RESTORE", restore("foo"))
		return mux
	}()
	cl := resptest.NewCluster(t,
		resptest.Shard{ID: "node-0", Slots: []rdb.SlotRange{{Start: 0, End: 5460}}, Handler: node0},
		resptest.Shard{ID: "node-1", Slots: []rdb.SlotRange{{Start: 12000, End: 13000}}, Handler: node1},
	)
	c, err := clusters.Upgrade(t.Context(), cl.Node("node-0").Client())
	if err != nil {
		t.Fatalf("Upgrade() error = %v", err)
	}
	defer c.Close()
	d := NewServerDst(t.Context(), c, KindDump)
	if err := d.Process(Head{Kind: KindDump, Dialect: rdb.RedisDialect{}}); err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	// The slot of the key bar is 5061, the one of foo is 12182.
	for _, key := range []string{"bar", "foo"} {
		if err := d.Process(dump.Token{Key: []byte(key), Payload: []byte("payload"), TTL: -1}); err != nil {
			t.Fatalf("Process() error = %v", err)
		}
	}
	// A key of an unassigned slot names no node, it fails the
	// transfer. The slot of the key zap is 6469, the one of the
	// ranges of the test carries no slot of it.
	if err := d.Process(dump.Token{Key: []byte("zap")}); err == nil {
		t.Error("Process() with a key of an unassigned slot did not fail")
	}
	if err := d.Finish(); err != nil {
		t.Errorf("Finish() error = %v", err)
	}
}

func Test_ServerDst_errorReply(t *testing.T) {
	// The server of the test rejects the RESTORE of a key, the
	// reply of it fails the flush of the batch.
	server := resptest.NewServer(t, func() resp.Handler {
		mux := resp.NewServeMux()
		mux.HandleFunc("RESTORE", reply(resp.Error("ERR invalid payload")))
		return mux
	}())
	c := clusters.Single(server.Client())
	d := NewServerDst(t.Context(), c, KindDump)
	if err := d.Process(Head{Kind: KindDump, Dialect: rdb.RedisDialect{}}); err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	err := d.Process(dump.Token{Key: []byte("bar"), Payload: []byte("payload")})
	err = d.Finish()
	if err == nil {
		t.Errorf("Process() error = %v, want it to contain %q", err, "invalid payload")
	}
}

func Test_ServerDst_strategyKind(t *testing.T) {
	// The strategy of the destination names the kinds it takes, a
	// token of another kind fails the transfer before the server
	// reads one command of it.
	server := resptest.NewServer(t, nil)
	client := server.Client()
	client.SetDialect(rdb.RedisDialect{})

	t.Run("a restore server takes no aof command", func(t *testing.T) {
		d := NewServerDst(t.Context(), clusters.Single(client), KindDump)
		if err := d.Process(Head{Kind: KindDump, Dialect: rdb.RedisDialect{}}); err != nil {
			t.Fatalf("Process() error = %v", err)
		}
		if err := d.Process(aof.Token{[]byte("SET"), []byte("foo"), []byte("bar")}); err == nil {
			t.Error("Process() with an aof command did not fail")
		}
	})

	t.Run("an aof server takes no key of a scan", func(t *testing.T) {
		d := NewServerDst(t.Context(), clusters.Single(client), KindAOF)
		if err := d.Process(Head{Kind: KindAOF, Dialect: rdb.RedisDialect{}}); err != nil {
			t.Fatalf("Process() error = %v", err)
		}
		if err := d.Process(dump.Token{Key: []byte("foo"), Payload: []byte("payload"), TTL: -1}); err == nil {
			t.Error("Process() with a key of a scan did not fail")
		}
	})

	t.Run("a restore+aof server takes both", func(t *testing.T) {
		d := NewServerDst(t.Context(), clusters.Single(client), KindDump|KindAOF)
		if err := d.Process(Head{Kind: KindDump | KindAOF, Dialect: rdb.RedisDialect{}}); err != nil {
			t.Fatalf("Process() error = %v", err)
		}
	})
}
