package chain

import (
	"bytes"
	"slices"
	"strings"
	"testing"

	"github.com/coding-socks/rdb"
	"github.com/coding-socks/rdb/aof"
	"github.com/coding-socks/rdb/dump"
)

// sameTokens compares the tokens by their kind and value, the tokens
// of a filter are the clones of the ones it received; the heads
// compare by the kinds they name.
func sameTokens(t *testing.T, got []Token, want []Token) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("tokens = %d, want %d", len(got), len(want))
	}
	for i, w := range want {
		switch w := w.(type) {
		case Head:
			head, ok := got[i].(Head)
			if !ok || head.Kinds != w.Kinds {
				t.Errorf("token %d = %v, want the head of %v", i, got[i], w.Kinds)
			}
		case rdb.Token:
			g, ok := got[i].(rdb.Token)
			if !ok {
				t.Errorf("token %d = %T, want an rdb token", i, got[i])
				continue
			}
			if g.Kind() != w.Kind() {
				t.Errorf("token %d kind = %s, want = %s", i, g.Kind(), w.Kind())
				continue
			}
			switch w.Kind() {
			case rdb.KindOpCode:
				if g.OpCode() != w.OpCode() {
					t.Errorf("token %d = %d, want = %d", i, g.OpCode(), w.OpCode())
				}
			case rdb.KindString:
				if g.String() != w.String() {
					t.Errorf("token %d = %q, want = %q", i, g.String(), w.String())
				}
			case rdb.KindUint:
				if g.Uint64() != w.Uint64() {
					t.Errorf("token %d = %d, want = %d", i, g.Uint64(), w.Uint64())
				}
			case rdb.KindExpiry:
				if !g.Time().Equal(w.Time()) {
					t.Errorf("token %d = %s, want = %s", i, g.Time(), w.Time())
				}
			}
		case aof.Token:
			g, ok := got[i].(aof.Token)
			if !ok {
				t.Errorf("token %d = %T, want a command", i, got[i])
				continue
			}
			if !slices.EqualFunc(g, w, func(a, b []byte) bool { return bytes.Equal(a, b) }) {
				t.Errorf("token %d = %v, want = %v", i, g, w)
			}
		case dump.Token:
			g, ok := got[i].(dump.Token)
			if !ok {
				t.Errorf("token %d = %T, want a scan token", i, got[i])
				continue
			}
			if !bytes.Equal(g.Key, w.Key) || !bytes.Equal(g.Payload, w.Payload) || g.TTL != w.TTL {
				t.Errorf("token %d = %v, want = %v", i, g, w)
			}
		default:
			t.Errorf("token %d: unexpected want of type %T", i, w)
		}
	}
}

func Test_Filter(t *testing.T) {
	tests := []struct {
		name    string
		keeps   func(string) bool // the predicate of the Filter under test
		in      []Token           // the head and the tokens the filter reads
		want    []Token           // the tokens the filter produces
		wantErr bool
	}{
		{
			name:  "include keeps the matching keys and their metadata",
			keeps: func(key string) bool { return key == "bar" },
			in: []Token{
				Head{Kinds: KindRDB, Dialect: rdb.RedisDialect{}},
				rdb.Op(rdb.RedisOpCodeAux), rdb.Str("redis-ver"), rdb.Str("7"),
				rdb.Op(rdb.RedisOpCodeSelectDB), rdb.Uint(0),
				rdb.Op(rdb.RedisOpCodeString), rdb.Str("foo"), rdb.Str("v1"),
				rdb.Op(rdb.RedisOpCodeExpiryMilli), rdb.Expiry(1234),
				rdb.Op(rdb.RedisOpCodeString), rdb.Str("bar"), rdb.Str("v2"),
				rdb.Op(rdb.RedisOpCodeEndOfRDB),
			},
			want: []Token{
				Head{Kinds: KindRDB, Dialect: rdb.RedisDialect{}},
				rdb.Op(rdb.RedisOpCodeSelectDB), rdb.Uint(0),
				rdb.Op(rdb.RedisOpCodeExpiryMilli), rdb.Expiry(1234),
				rdb.Op(rdb.RedisOpCodeString), rdb.Str("bar"), rdb.Str("v2"),
				rdb.Op(rdb.RedisOpCodeEndOfRDB),
			},
		},
		{
			name:  "exclude drops the matching keys",
			keeps: func(key string) bool { return key != "foo" },
			in: []Token{
				Head{Kinds: KindRDB, Dialect: rdb.RedisDialect{}},
				rdb.Op(rdb.RedisOpCodeAux), rdb.Str("redis-ver"), rdb.Str("7"),
				rdb.Op(rdb.RedisOpCodeSelectDB), rdb.Uint(0),
				rdb.Op(rdb.RedisOpCodeString), rdb.Str("foo"), rdb.Str("v1"),
				rdb.Op(rdb.RedisOpCodeExpiryMilli), rdb.Expiry(1234),
				rdb.Op(rdb.RedisOpCodeString), rdb.Str("bar"), rdb.Str("v2"),
				rdb.Op(rdb.RedisOpCodeEndOfRDB),
			},
			want: []Token{
				Head{Kinds: KindRDB, Dialect: rdb.RedisDialect{}},
				rdb.Op(rdb.RedisOpCodeAux), rdb.Str("redis-ver"), rdb.Str("7"),
				rdb.Op(rdb.RedisOpCodeSelectDB), rdb.Uint(0),
				rdb.Op(rdb.RedisOpCodeExpiryMilli), rdb.Expiry(1234),
				rdb.Op(rdb.RedisOpCodeString), rdb.Str("bar"), rdb.Str("v2"),
				rdb.Op(rdb.RedisOpCodeEndOfRDB),
			},
		},
		{
			name:  "a filter drops the keys it does not include",
			keeps: func(key string) bool { return strings.HasPrefix(key, "user:") },
			in: []Token{
				Head{Kinds: KindRDB, Dialect: rdb.RedisDialect{}},
				rdb.Op(rdb.RedisOpCodeAux), rdb.Str("redis-ver"), rdb.Str("7"),
				rdb.Op(rdb.RedisOpCodeSelectDB), rdb.Uint(0),
				rdb.Op(rdb.RedisOpCodeString), rdb.Str("foo"), rdb.Str("v1"),
				rdb.Op(rdb.RedisOpCodeExpiryMilli), rdb.Expiry(1234),
				rdb.Op(rdb.RedisOpCodeString), rdb.Str("bar"), rdb.Str("v2"),
				rdb.Op(rdb.RedisOpCodeEndOfRDB),
			},
			want: []Token{
				Head{Kinds: KindRDB, Dialect: rdb.RedisDialect{}},
				rdb.Op(rdb.RedisOpCodeSelectDB), rdb.Uint(0),
				rdb.Op(rdb.RedisOpCodeEndOfRDB),
			},
		},
		{
			name:  "the keys of a command come from the table of the dialect",
			keeps: func(key string) bool { return key == "baz" },
			in: []Token{
				Head{Kinds: KindAOF, Dialect: rdb.RedisDialect{}},
				aof.Token{[]byte("SELECT"), []byte("0")},
				aof.Token{[]byte("FLUSHALL")},
				aof.Token{[]byte("SET"), []byte("foo"), []byte("bar")},
				// The keys of an MSET sit at the second and the fourth
				// argument, one key of it matches and one does not.
				aof.Token{[]byte("MSET"), []byte("foo"), []byte("1"), []byte("baz"), []byte("2")},
				aof.Token{[]byte("SET"), []byte("baz"), []byte("qux")},
			},
			want: []Token{
				Head{Kinds: KindAOF, Dialect: rdb.RedisDialect{}},
				aof.Token{[]byte("SELECT"), []byte("0")},
				aof.Token{[]byte("FLUSHALL")},
				aof.Token{[]byte("SET"), []byte("baz"), []byte("qux")},
			},
		},
		{
			name:  "the key of a scan token is its name",
			keeps: func(key string) bool { return strings.HasPrefix(key, "only:") },
			in: []Token{
				Head{Kinds: KindDump, Dialect: rdb.RedisDialect{}},
				dump.Token{Key: []byte("only:a")},
				dump.Token{Key: []byte("skip:b")},
			},
			want: []Token{
				Head{Kinds: KindDump, Dialect: rdb.RedisDialect{}},
				dump.Token{Key: []byte("only:a")},
			},
		},
		{
			name:  "a head of a kind the filter does not take fails",
			keeps: func(key string) bool { return true },
			in: []Token{
				Head{Kinds: KindJSON},
			},
			wantErr: true,
		},
		{
			name:  "the tokens take a head",
			keeps: func(key string) bool { return true },
			in: []Token{
				dump.Token{Key: []byte("foo")},
			},
			wantErr: true,
		},
		{
			name:  "a filter follows the kinds of the head of a stream",
			keeps: func(key string) bool { return key == "baz" },
			// The head of a manifest names the records of the base
			// snapshot and the commands of the incremental files, the
			// filter follows both kinds in one stream.
			in: []Token{
				Head{Kinds: KindRDB | KindAOF, Dialect: rdb.RedisDialect{}},
				rdb.Op(rdb.RedisOpCodeString), rdb.Str("foo"), rdb.Str("v1"),
				aof.Token{[]byte("SET"), []byte("foo"), []byte("bar")},
				aof.Token{[]byte("SET"), []byte("baz"), []byte("qux")},
			},
			want: []Token{
				Head{Kinds: KindRDB | KindAOF, Dialect: rdb.RedisDialect{}},
				aof.Token{[]byte("SET"), []byte("baz"), []byte("qux")},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := NewRecorderDst()
			f := Filter(tt.keeps)(rec)
			var perr error
			for _, tok := range tt.in {
				if err := f.Process(tok); err != nil {
					perr = err
					break
				}
			}
			if tt.wantErr {
				if perr == nil {
					t.Error("Process() did not fail")
				}
				return
			}
			if perr != nil {
				t.Fatalf("Process() error = %v", perr)
			}
			if err := f.Finish(); err != nil {
				t.Fatal(err)
			}
			if !rec.Finished() {
				t.Error("the filter did not pass the end of the stream on")
			}
			sameTokens(t, rec.Received(), tt.want)
		})
	}
}
