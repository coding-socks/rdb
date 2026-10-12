// Package aof turns the keys of an RDB file into the AOF commands
// that recreate them on a server.
package aof

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"strconv"
	"time"

	"github.com/coding-socks/rdb"
	"github.com/coding-socks/rdb/resp"
)

// Token is one command of an AOF file: the name of the command and
// its arguments.
type Token [][]byte

// Reader reads the commands of an AOF stream.
type Reader struct {
	dec *resp.RESP3Decoder
}

// NewReader reads the commands of an AOF stream.
func NewReader(r io.Reader) *Reader {
	return &Reader{dec: resp.NewRESP3Decoder(r)}
}

// Next returns the next command of the stream, io.EOF at the end.
func (r *Reader) Next() (Token, error) {
	t, err := r.dec.ReadToken()
	if err != nil {
		return nil, err
	}
	if t.Kind() != resp.KindArray {
		return nil, fmt.Errorf("aof: unexpected AOF token %s", t.Kind())
	}
	n := int(t.Int())
	token := make(Token, 0, n)
	for i := 0; i < n; i++ {
		arg, err := r.dec.ReadToken()
		if err != nil {
			return nil, err
		}
		if arg.Kind() != resp.KindBlobString {
			return nil, fmt.Errorf("aof: unexpected AOF argument %s", arg.Kind())
		}
		token = append(token, bytes.Clone(arg.Bytes()))
	}
	return token, nil
}

// Converter turns the tokens of an RDB file into the commands of an
// AOF file: the SET, RPUSH, SADD, ZADD and HSET commands recreate the
// keys, with PEXPIREAT for the expiring keys and SELECT for the
// database of the following keys. Values that have no command
// representation are skipped.
type Converter struct {
	skipping bool

	db        int64
	key       []byte
	kind      byte     // s string, l list, S set, z zset, h hash
	cur       [][]byte // the arguments of the command being assembled
	expiry    time.Time
	hasExpiry bool
	remain    uint64 // value tokens still expected
	i         uint64 // value token index within the key
	member    []byte // the member of a zset pair, held until its score arrives

	pending uint8

	out []Token
}

// The pending kind of the tokens that follow an op code.
const (
	pendingNone uint8 = iota
	pendingSelect
	pendingExpiry
)

// NewConverter starts a conversion of the tokens of an RDB file to
// the commands of an AOF file.
func NewConverter() *Converter {
	return &Converter{}
}

// Convert turns one token of an RDB file into the commands that
// became complete with it.
func (c *Converter) Convert(t rdb.Token) ([]Token, error) {
	c.out = c.out[:0]
	if t.Kind() != rdb.KindOpCode {
		if err := c.convert(t); err != nil {
			return nil, err
		}
		return c.out, nil
	}
	if err := c.flush(); err != nil {
		return nil, err
	}
	switch op := t.OpCode(); op {
	case rdb.RedisOpCodeEndOfRDB:
		// The keys are done, the commands written so far are the
		// conversion.
	case rdb.RedisOpCodeSelectDB:
		c.pending = pendingSelect
	case rdb.RedisOpCodeExpirySeconds, rdb.RedisOpCodeExpiryMilli:
		c.pending = pendingExpiry
	case rdb.RedisOpCodeIdle, rdb.RedisOpCodeFreq:
		// The idle time and the frequency of a key are modifiers of
		// a RESTORE command, no command of the conversion carries
		// them. Their tokens are dropped until the next op code.
		c.skipping = true
	case rdb.RedisOpCodeString,
		rdb.RedisOpCodeList,
		rdb.RedisOpCodeSet,
		rdb.RedisOpCodeZset,
		rdb.RedisOpCodeZset2,
		rdb.RedisOpCodeHash:
		c.start(op)
	case rdb.RedisOpCodeModule2,
		rdb.RedisOpCodeModuleAux,
		rdb.RedisOpCodeModulePreGa,
		rdb.RedisOpCodeFunc,
		rdb.RedisOpCodeFunc2:
		c.skipping = true
	case rdb.RedisOpCodeAux,
		rdb.RedisOpCodeResizeDB,
		rdb.RedisOpCodeSlotInfo,
		rdb.RedisOpCodeRamLru,
		rdb.RedisOpCodeSlotNum,
		rdb.RedisOpCodeKeyMeta,
		rdb.RedisOpCodeHashTemplate:
		// The metadata records of the file: they carry no key and
		// have no command representation. Their tokens are dropped
		// until the next op code.
		c.skipping = true
	default:
		// The packed encodings, the streams and everything else
		// with no command representation: their tokens are
		// dropped until the next op code.
		c.skipping = true
	}
	return c.out, nil
}

// convert consumes one token of the payload of an op code.
func (c *Converter) convert(t rdb.Token) error {
	switch c.pending {
	case pendingSelect:
		c.db = int64(t.Uint64())
		if err := c.command([][]byte{[]byte("SELECT"), []byte(strconv.FormatInt(c.db, 10))}); err != nil {
			return err
		}
		c.pending = pendingNone
	case pendingExpiry:
		c.expiry, c.hasExpiry = t.Time(), true
		c.pending = pendingNone
	default:
		if c.skipping {
			return nil
		}
		if err := c.value(t); err != nil {
			return err
		}
	}
	return nil
}

// start begins a new key, its name arrives with the next token.
func (c *Converter) start(op rdb.OpCode) {
	c.skipping = false
	c.key = nil
	c.cur = nil
	c.member = nil
	c.i, c.remain = 0, 0
	switch op {
	case rdb.RedisOpCodeString:
		c.kind = 's'
	case rdb.RedisOpCodeList:
		c.kind = 'l'
	case rdb.RedisOpCodeSet:
		c.kind = 'S'
	case rdb.RedisOpCodeZset, rdb.RedisOpCodeZset2:
		c.kind = 'z'
	case rdb.RedisOpCodeHash:
		c.kind = 'h'
	}
}

// value consumes one token of the current key and appends it to the
// command of the key as it arrives.
func (c *Converter) value(t rdb.Token) error {
	if c.key == nil {
		// The first token of a key is its name.
		c.key = []byte(t.String())
		switch c.kind {
		case 's':
			if err := c.header("SET"); err != nil {
				return err
			}
			c.remain = 1
		case 'l', 'S', 'z', 'h':
			c.remain = ^uint64(0) // the count token follows
		}
		return nil
	}
	if c.remain == ^uint64(0) {
		// The second token of an aggregate is the count of its
		// elements, a hash and a zset count pairs.
		n := t.Uint64()
		if c.kind == 'z' || c.kind == 'h' {
			n *= 2
		}
		switch c.kind {
		case 'l':
			if err := c.headerArgs(int(2+n), "RPUSH"); err != nil {
				return err
			}
		case 'S':
			if err := c.headerArgs(int(2+n), "SADD"); err != nil {
				return err
			}
		case 'z':
			if err := c.headerArgs(int(2+n), "ZADD"); err != nil {
				return err
			}
		case 'h':
			if err := c.headerArgs(int(2+n), "HSET"); err != nil {
				return err
			}
		}
		c.remain = n
		return nil
	}
	if c.remain > 0 {
		c.remain--
		switch {
		case c.kind == 'z' && c.i&1 == 0:
			// The members of a zset arrive before their scores, the
			// pairs of ZADD are score first.
			c.member = []byte(t.String())
		case c.kind == 'z':
			c.cur = append(c.cur, zaddScore(t.Float64()), c.member)
		default:
			c.cur = append(c.cur, []byte(t.String()))
		}
		c.i++
	}
	if c.remain == 0 {
		if err := c.emit(); err != nil {
			return err
		}
		return c.flush()
	}
	return nil
}

// header opens the command of a key with its name and the key.
func (c *Converter) header(name string) error {
	c.cur = [][]byte{[]byte(name), c.key}
	return nil
}

// headerArgs opens the command of an aggregate, the count of its
// arguments is known before they arrive.
func (c *Converter) headerArgs(n int, name string) error {
	c.cur = make([][]byte, 0, n)
	c.cur = append(c.cur, []byte(name), c.key)
	return nil
}

// emit completes the command of the current key.
func (c *Converter) emit() error {
	if len(c.cur) == 0 {
		return nil
	}
	c.out = append(c.out, Token(append([][]byte(nil), c.cur...)))
	c.cur = nil
	return nil
}

// command writes a complete command.
func (c *Converter) command(args [][]byte) error {
	c.out = append(c.out, args)
	return nil
}

// flush finishes the current key: it writes the command of the key
// and the PEXPIREAT of an expiring key. An incomplete key is dropped.
func (c *Converter) flush() error {
	if c.key == nil {
		return nil
	}
	key, hasExpiry, expiry := c.key, c.hasExpiry, c.expiry
	c.key = nil
	c.skipping = false
	c.cur = nil
	c.hasExpiry = false
	c.expiry = time.Time{}
	if c.remain != 0 {
		return nil
	}
	if hasExpiry {
		ms := strconv.FormatInt(expiry.UnixMilli(), 10)
		return c.command([][]byte{[]byte("PEXPIREAT"), key, []byte(ms)})
	}
	return nil
}

// zaddScore formats the score of a sorted set member the way the ZADD
// command parses it back.
func zaddScore(f float64) []byte {
	switch {
	case math.IsInf(f, 1):
		return []byte("+inf")
	case math.IsInf(f, -1):
		return []byte("-inf")
	case math.IsNaN(f):
		return []byte("nan")
	}
	return []byte(strconv.FormatFloat(f, 'f', -1, 64))
}
