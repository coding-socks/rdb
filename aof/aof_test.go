package aof

import (
	"bytes"
	"testing"

	"github.com/coding-socks/rdb"
	"github.com/coding-socks/rdb/resp"
)

func Test_Converter_Convert(t *testing.T) {
	convert := func(c *Converter, tokens ...rdb.Token) []Token {
		t.Helper()
		var got []Token
		for _, tok := range tokens {
			toks, err := c.Convert(tok)
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, toks...)
		}
		return got
	}

	// A plain key is stored with the command that recreates it.
	t.Run("plain key commands", func(t *testing.T) {
		got := convert(NewConverter(),
			rdb.Op(rdb.RedisOpCodeString), rdb.Str("key"), rdb.Str("value"),
			rdb.Op(rdb.RedisOpCodeEndOfRDB),
		)
		if len(got) != 1 {
			t.Fatalf("commands = %d, want one", len(got))
		}
		want := Token{[]byte("SET"), []byte("key"), []byte("value")}
		if len(got[0]) != len(want) {
			t.Fatalf("command = %v, want %v", got[0], want)
		}
		for i := range want {
			if string(got[0][i]) != string(want[i]) {
				t.Errorf("argument %d = %q, want %q", i, got[0][i], want[i])
			}
		}
	})

	// The members of a sorted set arrive before their scores, the
	// pairs of a ZADD are score first.
	t.Run("a sorted set swaps its pairs", func(t *testing.T) {
		got := convert(NewConverter(),
			rdb.Op(rdb.RedisOpCodeZset2), rdb.Str("key"), rdb.Uint(2),
			rdb.Str("m1"), rdb.Float64(1.5),
			rdb.Str("m2"), rdb.Float64(-0.25),
			rdb.Op(rdb.RedisOpCodeEndOfRDB),
		)
		if len(got) != 1 {
			t.Fatalf("commands = %d, want one", len(got))
		}
		want := Token{
			[]byte("ZADD"), []byte("key"),
			[]byte("1.5"), []byte("m1"),
			[]byte("-0.25"), []byte("m2"),
		}
		if len(got[0]) != len(want) {
			t.Fatalf("command = %v, want %v", got[0], want)
		}
		for i := range want {
			if string(got[0][i]) != string(want[i]) {
				t.Errorf("argument %d = %q, want %q", i, got[0][i], want[i])
			}
		}
	})

	// The expiry of a key becomes a PEXPIREAT of the command that
	// recreated it.
	t.Run("an expiry becomes a PEXPIREAT", func(t *testing.T) {
		got := convert(NewConverter(),
			rdb.Op(rdb.RedisOpCodeExpiryMilli), rdb.Expiry(1500),
			rdb.Op(rdb.RedisOpCodeString), rdb.Str("key"), rdb.Str("value"),
			rdb.Op(rdb.RedisOpCodeEndOfRDB),
		)
		if len(got) != 2 {
			t.Fatalf("commands = %d, want two", len(got))
		}
		want := Token{[]byte("PEXPIREAT"), []byte("key"), []byte("1500")}
		if len(got[1]) != len(want) {
			t.Fatalf("command = %v, want %v", got[1], want)
		}
		for i := range want {
			if string(got[1][i]) != string(want[i]) {
				t.Errorf("argument %d = %q, want %q", i, got[1][i], want[i])
			}
		}
	})
}

func Test_Reader_Next(t *testing.T) {
	var buf bytes.Buffer
	e := resp.NewRESP3Encoder(&buf)
	write := func(args ...string) {
		if err := e.WriteToken(resp.ArrayLen(len(args))); err != nil {
			t.Fatal(err)
		}
		for _, arg := range args {
			if err := e.WriteToken(resp.Bytes([]byte(arg))); err != nil {
				t.Fatal(err)
			}
		}
	}
	write("SELECT", "0")
	write("SET", "foo", "bar")
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}

	r := NewReader(bytes.NewReader(buf.Bytes()))
	tok, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if len(tok) != 2 || string(tok[0]) != "SELECT" || string(tok[1]) != "0" {
		t.Errorf("command = %v, want SELECT 0", tok)
	}
	tok, err = r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if len(tok) != 3 || string(tok[0]) != "SET" || string(tok[1]) != "foo" || string(tok[2]) != "bar" {
		t.Errorf("command = %v, want SET foo bar", tok)
	}
	if _, err := r.Next(); err == nil {
		t.Error("Next() did not end with an error")
	}
}
