package dump

import (
	"bytes"
	"testing"
	"time"

	"github.com/coding-socks/rdb"
)

func Test_Encode_Decode(t *testing.T) {
	enc := NewEncoder(rdb.RedisDialect{})
	dec := NewDecoder(rdb.RedisDialect{})

	// A packed value has no command representation, a key of a scan
	// carries it the way it carries every value.
	ziplist := append(rdb.LenBytes(15),
		0x0f, 0, 0, 0, // the total length
		0x0a, 0, 0, 0, // the offset of the first entry
		0x01, 0, // the number of entries
		0x00, 0x02, 'a', 'b', // the entry
		0xff, // the end
	)
	tests := []struct {
		name   string
		tokens []rdb.Token // the record of a key, the expiry of an expiring key the clock decides
	}{
		{
			name: "the record of a string key",
			tokens: []rdb.Token{
				rdb.Op(rdb.RedisOpCodeString), rdb.Str("foo"), rdb.Str("bar"),
			},
		},
		{
			name: "the record of a list of strings",
			tokens: []rdb.Token{
				rdb.Op(rdb.RedisOpCodeList), rdb.Str("list"), rdb.Uint(2),
				rdb.Str("one"), rdb.Str("two"),
			},
		},
		{
			name: "the record of an expiring packed value",
			tokens: []rdb.Token{
				rdb.Op(rdb.RedisOpCodeExpiryMilli), rdb.Expiry(uint64(time.Now().Add(time.Hour).UnixMilli())),
				rdb.Op(rdb.RedisOpCodeListQuicklist), rdb.Str("key"), rdb.Uint(1), rdb.Packed(rdb.KindQuickList, ziplist),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tok, err := enc.Encode(tt.tokens...)
			if err != nil {
				t.Fatal(err)
			}
			got, err := dec.Decode(tok)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != len(tt.tokens) {
				t.Fatalf("the round trip produced %d tokens, want %d", len(got), len(tt.tokens))
			}
			for i, want := range tt.tokens {
				if got[i].Kind() != want.Kind() {
					t.Errorf("the token %d = %s, want %s", i, got[i].Kind(), want.Kind())
					continue
				}
				if want.Kind() == rdb.KindExpiry {
					// The expiry of the round trip is the one of
					// the time to live, the time between the
					// encode and the decode is a moment.
					if d := got[i].Time().Sub(want.Time()); d > time.Minute || d < -time.Minute {
						t.Errorf("the token %d = %s, want the time to live from now", i, got[i].Time())
					}
					continue
				}
				if !bytes.Equal(got[i].Bytes(), want.Bytes()) {
					t.Errorf("the token %d = %x, want %x", i, got[i].Bytes(), want.Bytes())
				}
			}
		})
	}

	// The buffers of the encoder and the decoder are reused, the
	// tokens of the first key stay intact while the second one
	// passes through.
	var packed []byte
	for _, tt := range tests {
		tok, err := enc.Encode(tt.tokens...)
		if err != nil {
			t.Fatal(err)
		}
		got, err := dec.Decode(tok)
		if err != nil {
			t.Fatal(err)
		}
		for _, t := range got {
			if t.Kind() == rdb.KindQuickList {
				packed = t.Bytes()
			}
		}
	}
	tok, err := enc.Encode(
		rdb.Op(rdb.RedisOpCodeString), rdb.Str("other"), rdb.Str("value"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dec.Decode(tok); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(packed, ziplist) {
		t.Errorf("the packed value = %x, want %x", packed, ziplist)
	}
}
