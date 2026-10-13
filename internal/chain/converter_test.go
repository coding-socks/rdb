package chain

import (
	"bytes"
	"encoding/json/jsontext"
	"strings"
	"testing"
	"time"

	"github.com/coding-socks/rdb"
	"github.com/coding-socks/rdb/aof"
	"github.com/coding-socks/rdb/dump"
)

func Test_ToRDB(t *testing.T) {
	// The DUMP of the key foo, the value bar.
	tok, err := dump.NewEncoder(rdb.RedisDialect{}).Encode(
		rdb.Op(rdb.RedisOpCodeString), rdb.Str("foo"), rdb.Str("bar"))
	if err != nil {
		t.Fatal(err)
	}
	rec := NewRecorderDst()
	e := ToRDB()(rec)
	if err := e.Process(Head{Kind: KindDump, Dialect: rdb.RedisDialect{}}); err != nil {
		t.Fatal(err)
	}
	if err := e.Process(tok); err != nil {
		t.Fatal(err)
	}
	if err := e.Finish(); err != nil {
		t.Fatal(err)
	}
	toks := rec.Received()
	if !rec.Finished() {
		t.Error("the conversion did not pass the end of the stream on")
	}
	if len(toks) != 5 {
		t.Fatalf("the conversion produced %d tokens, want the head, three and the tail", len(toks))
	}
	h, ok := toks[0].(Head)
	if !ok || h.Kind != KindRDB {
		t.Fatalf("the head = %v, want the RDB head", toks[0])
	}
	if _, ok := h.Dialect.(rdb.RedisDialect); !ok {
		t.Fatalf("the head = %v, want the RDB head of the redis dialect", toks[0])
	}
	if op, ok := toks[4].(rdb.Token); !ok || op.OpCode() != rdb.RedisOpCodeEndOfRDB {
		t.Fatalf("the tail token = %T, want the end of file opcode", toks[4])
	}

	// A payload of a server of a fork with its own file format
	// carries the version of the fork, the dialect the head names
	// gates the op codes the payload decodes with.
	t.Run("valkey version", func(t *testing.T) {
		tok, err := dump.NewEncoder(rdb.ValkeyDialect{}).Encode(
			rdb.Op(rdb.ValkeyOpCodeString), rdb.Str("foo"), rdb.Str("bar"))
		if err != nil {
			t.Fatal(err)
		}
		rec := NewRecorderDst()
		e := ToRDB()(rec)
		if err := e.Process(Head{Kind: KindDump, Dialect: rdb.ValkeyDialect{}}); err != nil {
			t.Fatal(err)
		}
		if err := e.Process(tok); err != nil {
			t.Fatal(err)
		}
		toks := rec.Received()
		if len(toks) != 4 {
			t.Fatalf("the conversion produced %d tokens, want the head and three", len(toks))
		}
		if op := toks[1].(rdb.Token); op.OpCode() != rdb.ValkeyOpCodeString {
			t.Errorf("the first token = %v, want the valkey string opcode", op.OpCode())
		}
	})

	t.Run("a key before the head fails", func(t *testing.T) {
		e := ToRDB()(NewRecorderDst())
		if err := e.Process(dump.Token{Key: []byte("foo")}); err == nil {
			t.Error("Process() with a key before the head did not fail")
		}
	})

	// A payload that lost a byte of its content fails the checksum
	// of the decode, the conversion fails instead of storing a
	// value the server did not store.
	t.Run("a corrupt payload fails", func(t *testing.T) {
		tok, err := dump.NewEncoder(rdb.RedisDialect{}).Encode(
			rdb.Op(rdb.RedisOpCodeString), rdb.Str("foo"), rdb.Str("bar"))
		if err != nil {
			t.Fatal(err)
		}
		tok.Payload[len(tok.Payload)-1] ^= 0xff
		e := ToRDB()(NewRecorderDst())
		if err := e.Process(Head{Kind: KindDump, Dialect: rdb.RedisDialect{}}); err != nil {
			t.Fatal(err)
		}
		if err := e.Process(tok); err == nil {
			t.Error("Process() with a corrupt payload did not fail")
		}
	})
}

func Test_ToDump(t *testing.T) {
	// The plain values of an RDB file become the keys of a scan, the
	// way the DUMP command returns them, the expiry becomes their
	// time to live.
	want, err := dump.NewEncoder(rdb.RedisDialect{}).Encode(
		rdb.Op(rdb.RedisOpCodeString), rdb.Str("foo"), rdb.Str("bar"))
	if err != nil {
		t.Fatal(err)
	}
	rec := NewRecorderDst()
	e := ToDump()(rec)
	if err := e.Process(Head{Kind: KindRDB, Dialect: rdb.RedisDialect{}}); err != nil {
		t.Fatal(err)
	}
	for _, tok := range []rdb.Token{
		rdb.Op(rdb.RedisOpCodeString), rdb.Str("foo"), rdb.Str("bar"),
		rdb.Op(rdb.RedisOpCodeEndOfRDB),
	} {
		if err := e.Process(tok); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.Finish(); err != nil {
		t.Fatal(err)
	}
	toks := rec.Received()
	if !rec.Finished() {
		t.Error("the conversion did not pass the end of the stream on")
	}
	// A scan has no tail, the head and the key of a scan are all
	// the conversion produces.
	if len(toks) != 2 {
		t.Fatalf("the conversion produced %d tokens, want the head and the key of a scan", len(toks))
	}
	h, ok := toks[0].(Head)
	if !ok || h.Kind != KindDump {
		t.Fatalf("the head = %v, want the dump head", toks[0])
	}
	if _, ok := h.Dialect.(rdb.RedisDialect); !ok {
		t.Fatalf("the head = %v, want the dump head of the redis dialect", toks[0])
	}
	tok, ok := toks[1].(dump.Token)
	if !ok {
		t.Fatalf("the token = %T, want a dump token", toks[1])
	}
	if string(tok.Key) != "foo" || tok.TTL != -1 {
		t.Errorf("key = %q, ttl = %d, want foo without an expiry", tok.Key, tok.TTL)
	}
	if !bytes.Equal(tok.Payload, want.Payload) {
		t.Errorf("payload = %x, want %x", tok.Payload, want.Payload)
	}

	// The expiry of a record becomes the time to live of the key
	// of a scan.
	t.Run("an expiring key keeps its time to live", func(t *testing.T) {
		want, err := dump.NewEncoder(rdb.RedisDialect{}).Encode(
			rdb.Op(rdb.RedisOpCodeString), rdb.Str("foo"), rdb.Str("bar"))
		if err != nil {
			t.Fatal(err)
		}
		rec := NewRecorderDst()
		e := ToDump()(rec)
		if err := e.Process(Head{Kind: KindRDB, Dialect: rdb.RedisDialect{}}); err != nil {
			t.Fatal(err)
		}
		for _, tok := range []rdb.Token{
			rdb.Op(rdb.RedisOpCodeExpiryMilli), rdb.Expiry(uint64(time.Now().Add(time.Hour).UnixMilli())),
			rdb.Op(rdb.RedisOpCodeString), rdb.Str("foo"), rdb.Str("bar"),
			rdb.Op(rdb.RedisOpCodeEndOfRDB),
		} {
			if err := e.Process(tok); err != nil {
				t.Fatal(err)
			}
		}
		toks := rec.Received()
		if len(toks) != 2 {
			t.Fatalf("the conversion produced %d tokens, want the head and the key of a scan", len(toks))
		}
		tok, ok := toks[1].(dump.Token)
		if !ok {
			t.Fatalf("the token = %T, want a dump token", toks[1])
		}
		if tok.TTL <= 0 || tok.TTL > int64(time.Hour) {
			t.Errorf("ttl = %d, want the hour the expiry names", tok.TTL)
		}
		if !bytes.Equal(tok.Payload, want.Payload) {
			t.Errorf("payload = %x, want %x", tok.Payload, want.Payload)
		}
	})

	// A key that expired between the file and the conversion is
	// not stored.
	t.Run("an expired key is dropped", func(t *testing.T) {
		rec := NewRecorderDst()
		e := ToDump()(rec)
		if err := e.Process(Head{Kind: KindRDB, Dialect: rdb.RedisDialect{}}); err != nil {
			t.Fatal(err)
		}
		for _, tok := range []rdb.Token{
			rdb.Op(rdb.RedisOpCodeExpiryMilli), rdb.Expiry(0),
			rdb.Op(rdb.RedisOpCodeString), rdb.Str("foo"), rdb.Str("bar"),
			rdb.Op(rdb.RedisOpCodeEndOfRDB),
		} {
			if err := e.Process(tok); err != nil {
				t.Fatal(err)
			}
		}
		toks := rec.Received()
		if len(toks) != 1 {
			t.Fatalf("the conversion produced %d tokens, want the head alone", len(toks))
		}
	})
}

func Test_ToAOF(t *testing.T) {
	dumped, err := dump.NewEncoder(rdb.RedisDialect{}).Encode(
		rdb.Op(rdb.RedisOpCodeString), rdb.Str("foo"), rdb.Str("bar"))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name        string
		in          []Token
		finish      bool
		want        [][]string // the commands the conversion produces, the head apart
		wantDialect rdb.Dialect
		wantErr     bool
	}{
		{
			name:        "the head of an rdb stream becomes an aof head",
			in:          []Token{Head{Kind: KindRDB, Dialect: rdb.RedisDialect{}}},
			finish:      true,
			wantDialect: rdb.RedisDialect{},
		},
		{
			name: "the records of an rdb stream become the commands that re-create them",
			in: []Token{
				Head{Kind: KindRDB, Dialect: rdb.RedisDialect{}},
				rdb.Op(rdb.RedisOpCodeString),
				rdb.Str("foo"),
				rdb.Str("bar"),
			},
			finish:      true,
			want:        [][]string{{"SET", "foo", "bar"}},
			wantDialect: rdb.RedisDialect{},
		},
		{
			name: "the keys of a scan become the commands that re-create them",
			in: []Token{
				Head{Kind: KindDump, Dialect: rdb.RedisDialect{}},
				dump.Token{Key: []byte("foo"), Payload: dumped.Payload, TTL: -1},
			},
			finish:      true,
			want:        [][]string{{"SET", "foo", "bar"}},
			wantDialect: rdb.RedisDialect{},
		},
		{
			name:        "mixed kind",
			in:          []Token{Head{Kind: KindRDB | KindAOF, Dialect: rdb.RedisDialect{}}},
			finish:      true,
			wantDialect: rdb.RedisDialect{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := NewRecorderDst()
			e := ToAOF()(rec)
			var err error
			for _, tok := range tt.in {
				if err = e.Process(tok); err != nil {
					break
				}
			}
			if err == nil && tt.finish {
				err = e.Finish()
			}
			if tt.wantErr {
				if err == nil {
					t.Error("the conversion did not fail")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			// The end of the stream is passed on for the destination
			// to commit.
			if !rec.Finished() {
				t.Error("the conversion did not pass the end of the stream on")
			}
			toks := rec.Received()
			if len(toks) != len(tt.want)+1 {
				t.Fatalf("the conversion produced %d tokens, want the head and %d commands", len(toks), len(tt.want))
			}
			// The converted head keeps the dialect of the head it
			// received.
			head, ok := toks[0].(Head)
			if !ok || head.Kind != KindAOF || head.Dialect != tt.wantDialect {
				t.Fatalf("the head = %v, want the AOF head of the %v dialect", toks[0], tt.wantDialect)
			}
			for i, want := range tt.want {
				args, ok := toks[i+1].(aof.Token)
				if !ok {
					t.Fatalf("the token %d = %T, want a command", i+1, toks[i+1])
				}
				if len(args) != len(want) {
					t.Fatalf("command %d = %v, want %v", i+1, args, want)
				}
				for j := range args {
					if string(args[j]) != want[j] {
						t.Errorf("command %d = %v, want %v", i+1, args, want)
					}
				}
			}
		})
	}
}

func Test_ToJSON(t *testing.T) {
	dumped, err := dump.NewEncoder(rdb.RedisDialect{}).Encode(
		rdb.Op(rdb.RedisOpCodeString), rdb.Str("foo"), rdb.Str("bar"))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		in      []Token
		finish  bool
		want    string // the JSON the received tokens render, the head apart
		wantErr bool
	}{
		{
			name:   "the head of an rdb stream becomes a json head",
			in:     []Token{Head{Kind: KindRDB, Dialect: rdb.RedisDialect{}}},
			finish: true,
		},
		{
			name:   "the head of a scan becomes a json head",
			in:     []Token{Head{Kind: KindDump, Dialect: rdb.RedisDialect{}}},
			finish: true,
		},
		{
			name: "the records of an rdb stream become the values of the json stream",
			in: []Token{
				Head{Kind: KindRDB, Dialect: rdb.RedisDialect{}},
				rdb.Op(rdb.RedisOpCodeString),
				rdb.Str("foo"),
				rdb.Str("bar"),
			},
			finish: true,
			want:   `[{"foo":"bar"}]`,
		},
		{
			name: "the keys of a scan become the values of the json stream",
			in: []Token{
				Head{Kind: KindDump, Dialect: rdb.RedisDialect{}},
				dump.Token{Key: []byte("foo"), Payload: dumped.Payload, TTL: -1},
			},
			finish: true,
			want:   `[{"foo":"bar"}]`,
		},
		{
			name:   "the end of an empty stream stays empty",
			finish: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := NewRecorderDst()
			e := ToJSON()(rec)
			var err error
			for _, tok := range tt.in {
				if err = e.Process(tok); err != nil {
					break
				}
			}
			if err == nil && tt.finish {
				err = e.Finish()
			}
			if tt.wantErr {
				if err == nil {
					t.Error("the conversion did not fail")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			// The end of the stream is passed on for the destination
			// to commit.
			if !rec.Finished() {
				t.Error("the conversion did not pass the end of the stream on")
			}
			var buf bytes.Buffer
			enc := jsontext.NewEncoder(&buf)
			for i, tok := range rec.Received() {
				if head, ok := tok.(Head); ok {
					if i != 0 || head.Kind != KindJSON || head.Dialect != (rdb.RedisDialect{}) {
						t.Errorf("the head = %v, want the JSON head of the redis dialect", tok)
					}
					continue
				}
				if err := enc.WriteToken(tok.(jsontext.Token)); err != nil {
					t.Fatal(err)
				}
			}
			if got := strings.TrimSpace(buf.String()); got != tt.want {
				t.Errorf("json = %q, want %q", got, tt.want)
			}
		})
	}
}
