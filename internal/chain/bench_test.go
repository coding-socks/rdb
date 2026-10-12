package chain

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"testing"

	"github.com/coding-socks/rdb"
)

// benchRDB encodes an RDB file with n string keys of size-byte
// values, the file carries no metadata: a round trip through the dump
// conversion produces the same bytes.
func benchRDB(b *testing.B, n, size int, dialect rdb.Dialect) []byte {
	b.Helper()
	var buf bytes.Buffer
	enc := rdb.NewFileEncoder(&buf, dialect)
	val := make([]byte, size)
	for i := 0; i < n; i++ {
		for j := range val {
			val[j] = byte(i*31 + j)
		}
		toks := []rdb.Token{
			rdb.Op(rdb.RedisOpCodeString),
			rdb.Str("key:" + strconv.Itoa(i)),
			rdb.Str(string(val)),
		}
		for _, tok := range toks {
			if err := enc.WriteToken(tok); err != nil {
				b.Fatal(err)
			}
		}
	}
	if err := enc.WriteToken(rdb.Op(rdb.RedisOpCodeEndOfRDB)); err != nil {
		b.Fatal(err)
	}
	return buf.Bytes()
}

// discardWriter receives the bytes of a destination and drops them,
// the benchmark measures the chain, not the writer.
type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// BenchmarkChain_Exec runs the alternating conversion of a transfer
// between two servers, the keys of an RDB file become the payloads of
// a scan and back, twice: RDB -> dump -> RDB -> dump -> RDB. The
// conversions are inverses of each other, the output of the chain is
// the file it started with, the first run compares the bytes.
func BenchmarkChain_Exec(b *testing.B) {
	dialect := rdb.RedisDialect{}
	for _, keys := range []int{1000, 10000} {
		b.Run(fmt.Sprintf("keys=%d", keys), func(b *testing.B) {
			file := benchRDB(b, keys, 64, dialect)
			run := func(out io.Writer) {
				src := NewRDBFileSrc(bytes.NewReader(file), nil)
				end := Dst(NewRDBDst(out))
				for _, mw := range []Middleware{ToDump(), ToRDB(), ToDump(), ToRDB()} {
					end = mw(end)
				}
				if err := Run(context.Background(), src, end); err != nil {
					b.Fatal(err)
				}
			}
			// The conversions are inverses of each other, the
			// bytes of the second RDB are the bytes of the first.
			var out bytes.Buffer
			run(&out)
			if !bytes.Equal(out.Bytes(), file) {
				i := 0
				for i < len(file) && i < out.Len() && file[i] == out.Bytes()[i] {
					i++
				}
				b.Fatalf("the round trip is not the same: in %d bytes, out %d bytes, the first difference at %d", len(file), out.Len(), i)
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(file)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				run(&discardWriter{})
			}
		})
	}
}
