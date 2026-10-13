package chain

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/coding-socks/rdb"
)

func Test_Run(t *testing.T) {
	t.Run("the tokens of the source reach the destination", func(t *testing.T) {
		src := NewSliceSrc(Head{}, []Msg{{T: 1}, {T: 2}, {T: 3}})
		dst := NewRecorderDst()
		if err := Run(t.Context(), src, dst); err != nil {
			t.Fatalf("Run() error = %v", err)
		}
		want := []Token{Head{}, 1, 2, 3}
		if got := dst.Received(); !reflect.DeepEqual(got, want) {
			t.Errorf("dst received %v, want %v", got, want)
		}
		if !dst.Finished() {
			t.Error("the stream did not end gracefully")
		}
		if err := dst.Aborted(); err != nil {
			t.Errorf("dst.Aborted() = %v, want none", err)
		}
	})

	t.Run("the head of a source starts the chain", func(t *testing.T) {
		data := writeRDB(t, rdb.ValkeyDialect{V: 80},
			rdb.Op(rdb.RedisOpCodeString),
			rdb.Str("foo"), rdb.Str("bar"),
			rdb.Op(rdb.RedisOpCodeEndOfRDB),
		)
		src := NewRDBFileSrc(data, nil)
		dst := NewRecorderDst()
		if err := Run(t.Context(), src, dst); err != nil {
			t.Fatalf("Run() error = %v", err)
		}
		if len(dst.Received()) != 5 {
			t.Errorf("dst received %d, want at least one token", len(dst.Received()))
		}
		if !dst.Finished() {
			t.Error("the stream did not end gracefully")
		}
		tt := dst.Received()[0]
		head, ok := tt.(Head)
		if !ok || head.Kind != KindRDB {
			t.Errorf("the first token = %v, want the RDB head", dst.Received()[0])
		}
		_, ver := head.Dialect.Header()
		if _, ok := head.Dialect.(rdb.ValkeyDialect); !ok || ver != 80 {
			t.Errorf("the first token = %v, want the RDB head of the valkey dialect", dst.Received()[0])
		}
	})

	t.Run("a head the destination does not take fails the chain", func(t *testing.T) {
		src := NewSliceSrc(Head{}, []Msg{{T: 1}})
		var buf bytes.Buffer
		dst := NewJSONDst(&buf)
		err := Run(t.Context(), src, dst)
		if err == nil {
			t.Errorf("Run() error = %q, wantErr %v", err, true)
		}
	})

	t.Run("a flow the destination does not take is rejected", func(t *testing.T) {
		src := NewAOFFileSrc(strings.NewReader(""), nil)
		var buf bytes.Buffer
		dst := NewJSONDst(&buf)
		err := Run(t.Context(), src, dst)
		if err == nil || !strings.Contains(err.Error(), "invalid kind") {
			t.Error("Run() did not reject the commands of an AOF source")
		}
	})

	t.Run("an error of the source aborts the chain", func(t *testing.T) {
		boom := errors.New("boom")
		src := NewSliceSrc(Head{}, []Msg{{Err: boom}})
		dst := NewRecorderDst()
		err := Run(t.Context(), src, dst)
		if !errors.Is(err, boom) {
			t.Fatalf("Run() error = %v, want %v", err, boom)
		}
		if err := dst.Aborted(); !errors.Is(err, boom) {
			t.Errorf("dst.Aborted() = %v, want %v", err, boom)
		}
		if dst.Finished() {
			t.Error("the stream ended gracefully")
		}
	})

	t.Run("an ended context aborts the chain without an error", func(t *testing.T) {
		src := NewSliceSrc(Head{}, []Msg{{T: 1}})
		dst := NewRecorderDst()
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if err := Run(ctx, src, dst); err != nil {
			t.Fatalf("Run() error = %v, want none", err)
		}
		if err := dst.Aborted(); !errors.Is(err, context.Canceled) {
			t.Errorf("dst.Aborted() = %v, want %v", err, context.Canceled)
		}
		if dst.Finished() {
			t.Error("the stream ended gracefully")
		}
	})
}

// writeRDB encodes the tokens the way a FileEncoder writes them, the
// dialect names the header of the file.
func writeRDB(t *testing.T, dialect rdb.Dialect, tokens ...rdb.Token) io.Reader {
	t.Helper()
	var buf bytes.Buffer
	enc := rdb.NewFileEncoder(&buf, dialect)
	err := enc.WriteTokens(tokens...)
	if err != nil {
		t.Fatal(err)
	}
	return &buf
}
