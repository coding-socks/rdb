// Package chain wires the stages of a transfer: a [Src] produces the
// tokens and pushes them into a [Dst], the middlewares between them
// receive and transform them.
package chain

import (
	"context"
	"errors"
	"io"
	"strings"

	"github.com/coding-socks/rdb"
)

// Token is the unit the stages of a chain exchange: [rdb.Token] for
// an RDB stream, dump.Token for a scan, aof.Token for the commands
// of an AOF stream, or jsontext.Token for a JSON stream. The first
// token of a stream is a [Head].
type Token = any

// Kind is the kind of the tokens a chain carries.
type Kind uint16

const (
	// KindRDB is the kind of the tokens of an RDB file.
	KindRDB Kind = 1 << iota
	// KindAOF is the kind of the commands of an AOF file.
	KindAOF
	// KindDump is the kind of the keys of a scan.
	KindDump
	// KindJSON is the kind of the tokens of a JSON stream.
	KindJSON
)

// String returns the textual representation of a kind.
func (k Kind) String() string {
	var kinds []string
	if k&KindRDB > 0 {
		kinds = append(kinds, "RDB")
	}
	if k&KindAOF > 0 {
		kinds = append(kinds, "AOF")
	}
	if k&KindDump > 0 {
		kinds = append(kinds, "DUMP")
	}
	if k&KindJSON > 0 {
		kinds = append(kinds, "JSON")
	}
	if len(kinds) == 0 {
		return "unknown"
	}
	return strings.Join(kinds, "+")
}

// Head is the first token of a stream. It names the [Kind] values of
// the tokens that follow it and the [rdb.Dialect] they are written
// in; the dialect is only set for the [KindRDB] and [KindDump] kinds,
// and a stream that changes its kind names all of them, the sections
// of a manifest archive among them.
type Head struct {
	Kind    Kind
	Dialect rdb.Dialect
}

// Src produces the tokens of a chain and pushes them into the
// destination: its first token is a [Head], the tokens follow it in
// the kinds and the dialect the head names. Run returns no error, or
// io.EOF, when the stream ended cleanly, and the error of the stream
// when it failed. The caller of the chain opens the resources of a
// source and closes them after [Run] returned; a source opens nothing
// and closes nothing.
type Src interface {
	// Run pushes the tokens of the stream into dst.
	Run(ctx context.Context, dst Dst) error
}

// Dst receives the tokens of a chain.
//
// The caller of the chain opens the resources of a destination and
// closes them after [Run] returned; a destination opens nothing and
// closes nothing.
type Dst interface {
	// Process receives one token of the chain. It returns an error
	// when it cannot receive the token, which fails the transfer.
	// Process may be called again after it returned an error; it
	// returns without effect then.
	Process(t Token) error
	// Finish ends the stream. It flushes what the destination
	// holds back and commits the tokens it received.
	// [Run] calls it when the source ended cleanly.
	Finish() error
	// Abort releases the destination without a commit: the stream
	// failed. [Run] calls it after an error of the source, of the
	// context, or of the chain; the destination has released
	// itself then, and Abort returns without effect.
	Abort(err error)
}

// Middleware wraps a [Dst] in another Dst. The constructors of the
// converters and the filters return one: the configuration of the
// transformation is bound to the constructor, the destination it
// wraps is the argument. The caller of [Run] applies the middlewares
// to the destination, from the destination up, so the middleware it
// applies first passes its tokens first.
type Middleware func(Dst) Dst

// Run pushes the tokens of src into dst: the first token of the
// stream is the [Head] of the source, the middlewares of the
// transformation are the wrapping of dst the caller applied.
//
// A source that ended cleanly, it returned no error or io.EOF,
// commits the destination with [Dst.Finish]; a source that failed, or
// whose context ended, aborts it with [Dst.Abort]. Run returns the
// first error of the transfer, nil when the context ended. The
// resources of the ends belong to the caller: it opens them before
// Run and closes them after it returned.
func Run(ctx context.Context, src Src, dst Dst) error {
	err := src.Run(ctx, dst)
	switch {
	case err == nil || errors.Is(err, io.EOF):
		return dst.Finish()
	case ctx.Err() != nil:
		dst.Abort(err)
		return nil
	default:
		dst.Abort(err)
		return err
	}
}
