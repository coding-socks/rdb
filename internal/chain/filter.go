package chain

import (
	"errors"
	"fmt"

	"github.com/coding-socks/rdb"
	"github.com/coding-socks/rdb/aof"
	"github.com/coding-socks/rdb/dump"
)

// Filter keeps the keys that match its predicate. It filters the
// kinds that carry keys, the records of an RDB file, the commands of
// an AOF file and the keys of a scan; the tokens of the other kinds
// pass through. The key of a command comes from the key specs of the
// dialect of the head, the key of a scan token is its name; the key
// of an RDB record is held back until its first value token arrives,
// the records that start with another kind carry no key. The head
// that names a kind the filter does not take fails the stream, the
// commands of the [KindAOF] kind take a dialect, and a command the
// table of the dialect does not know passes unfiltered, its keys
// cannot be decided.
func Filter(keeps func(string) bool) Middleware {
	return func(next Dst) Dst {
		return &filter{
			Dst:   next,
			keeps: keeps,
		}
	}
}

// filter is the [Dst] the Filter middleware wraps: it passes the
// keys it keeps to the wrapped destination and drops the others.
// Finish and Abort are promoted.
type filter struct {
	Dst

	keeps   func(string) bool
	dialect rdb.Dialect // the dialect of the head, its table extracts the keys of a command
	mode    uint8       // the state of the RDB record being filtered
	pending []rdb.Token
}

func (f *filter) Process(t Token) error {
	if head, ok := t.(Head); ok {
		if head.Kinds&(KindRDB|KindAOF|KindDump) == 0 {
			return fmt.Errorf("rdbc transfer: invalid kinds %v", head.Kinds)
		}
		f.dialect = head.Dialect
		return f.Dst.Process(head)
	}
	if f.dialect == nil {
		return errors.New("rdbc transfer: missing dialect")
	}
	switch tt := t.(type) {
	case rdb.Token:
		return f.processRDB(tt)
	case aof.Token:
		return f.processAOF(tt)
	case dump.Token:
		if !f.keeps(string(tt.Key)) {
			return nil
		}
		return f.Dst.Process(tt)
	default:
		return f.Dst.Process(t)
	}
}

// processAOF filters the commands of an AOF stream. The keys of a
// command come from the table of the dialect, a command passes when
// the predicate keeps every one of them; a command the table does
// not know passes unfiltered, its keys cannot be decided.
func (f *filter) processAOF(tt aof.Token) error {
	cmd, err := f.dialect.Commands().Lookup(tt)
	if err != nil {
		return f.Dst.Process(tt)
	}
	keys, err := cmd.ExtractKeys(tt)
	if err != nil {
		return f.Dst.Process(tt)
	}
	for _, key := range keys {
		if !f.keeps(string(key)) {
			return nil
		}
	}
	return f.Dst.Process(tt)
}

// processRDB filters the tokens of an RDB stream. A record is held
// back until its key is decided, and the records of a dropped key
// are skipped until the next op code.
func (f *filter) processRDB(tt rdb.Token) error {
	if tt.Kind() == rdb.KindOpCode {
		if tt.OpCode() == rdb.RedisOpCodeEndOfRDB || tt.OpCode() == rdb.RedisOpCodeSelectDB {
			// The end of the file closes the transfer of a
			// dropped key as well, the database of the following
			// keys always passes.
			out := f.pending
			f.pending = nil
			f.mode = filterPass
			for _, pt := range out {
				if err := f.Dst.Process(pt); err != nil {
					return err
				}
			}
			return f.Dst.Process(tt)
		}
		// A new record starts, it is held back until the
		// kind of its first token is known.
		f.mode = filterHold
		f.pending = append(f.pending, tt.Clone())
		return nil
	}
	switch f.mode {
	case filterSkip:
		return nil
	case filterHold:
		// The first string of a record may be the key of a value,
		// the aux and the function records start with one as
		// well, dropping them is harmless. The tokens of the
		// records held back stay undecided until a key arrives.
		if tt.Kind() == rdb.KindString && !f.keeps(tt.String()) {
			f.pending = nil
			f.mode = filterSkip
			return nil
		}
		if tt.Kind() != rdb.KindString {
			f.pending = append(f.pending, tt.Clone())
			return nil
		}
		out := f.pending
		f.pending = nil
		f.mode = filterPass
		for _, pt := range out {
			if err := f.Dst.Process(pt); err != nil {
				return err
			}
		}
		return f.Dst.Process(tt)
	default:
		return f.Dst.Process(tt)
	}
}

const (
	filterHold uint8 = iota // the record is undecided
	filterPass              // the record passed
	filterSkip              // the record is dropped
)
