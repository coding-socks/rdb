package rdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"strconv"

	"github.com/coding-socks/rdb/internal/crc64jones"
)

// NOTE: The logic for decoding is complicated by the fact that the records
// of the RDB format do not declare how many tokens they produce: an op code
// dispatches a handler that reads the tokens of the record one by one.
//
// The file framing lives in [FileDecoder]: it reads the header of the
// file, detects the dialect behind it and verifies the checksum at its
// end. The [Decoder] reads the records behind the header.
//
// This file is structured in the following way:
//
//   - FileDecoder.readHeader reads the header of the file,
//     FileDecoder.validateChecksum verifies the checksum when the end
//     of file token ends the file.
//
//   - Decoder.loadXXX methods append the next element of the grammar
//     to Decoder.buf, so the raw bytes of a token span from its start
//     to the current end of the buffer.
//
//   - The XXXDecoder types implement [TokenDecoder]: Handle reads the
//     next token of the record and returns the handler for the token
//     after it, or nil when the record is complete.
//     The handlers that span a whole key (e.g., [KeyDecoder]) chain to
//     a value handler with their Next field.
//
//   - The read position of the handlers is the opCode pointer of the
//     Decoder: it holds the op code of the record being read while the
//     handler is active and is cleared when it completes.

// The shared token handlers compare and dispatch on the op codes of the
// RDB format itself. Every dialect uses the same values for them.
const (
	opCodeEndOfRDB OpCode = 0xff

	opCodeExpirySeconds OpCode = 0xfd
	opCodeExpiryMilli   OpCode = 0xfc

	opCodeZset  OpCode = 3
	opCodeZset2 OpCode = 5

	opCodeStreamListpacks2 OpCode = 19
	opCodeStreamListpacks3 OpCode = 21
	opCodeStreamListpacks4 OpCode = 26
	opCodeStreamListpacks5 OpCode = 27
)

var opCodeExpiryLength = map[OpCode]int{
	opCodeExpirySeconds: 4,
	opCodeExpiryMilli:   8,
}

// TokenDecoder reads the tokens of the value an op code starts.
// Handle reads the next token from d and returns the handler that reads
// the token after it: a handler returns itself to continue a sequence,
// or nil when the value is complete.
type TokenDecoder interface {
	Handle(d *Decoder) (TokenDecoder, Token, error)
}

// Decoder is a streaming decoder for the records of an RDB file.
// It is used to read the records of a file token by token.
//
// The tokens of a record are read in the order the file stores them:
// the op code of a record starts it, the tokens of its key and value
// follow it, see [TokenDecoder]. The header of the file and the
// checksum at its end are the business of [FileDecoder].
//
// A Token returned by [Decoder.ReadToken] points into the internal buffer
// of the Decoder and is only valid until the next call to any method on
// that Decoder. Call [Token.Clone] to keep it.
type Decoder struct {
	r io.Reader

	dialect Dialect

	buf []byte

	opCode  *OpCode
	handler TokenDecoder

	templates map[uint64]uint64
}

// NewDecoder constructs a new decoder reading the records of an RDB
// file from r. The dialect dispatches the op codes of the records and
// gates the versions they were introduced in. The zero dialect selects
// the [RedisDialect] in its newest version.
func NewDecoder(r io.Reader, dialect Dialect) *Decoder {
	if dialect == nil {
		dialect = RedisDialect{}
	}
	return &Decoder{
		r:       r,
		buf:     make([]byte, 0, DefaultBufferLen),
		dialect: dialect,
	}
}

// FileDecoder reads an RDB file: the header of the file, the records
// behind it, and the checksum at its end. The header names the dialect
// of the file and binds it to the version of the file, the checksum is
// verified when the end of file token ends the file.
//
// A Token returned by [FileDecoder.ReadToken] points into the internal
// buffer of the [Decoder] behind it and is only valid until the next
// call to any method on that FileDecoder. Call [Token.Clone] to keep it.
type FileDecoder struct {
	rraw io.Reader // the source, the reads of the checksum bypass the tee
	d    *Decoder  // the record decoder over the tee, nil until the header was read
	hash hash.Hash64

	dialect Dialect // the dialect of the file, nil until the header was read
	ver     int     // the version of the header, the gate of the checksum reads it
	done    bool    // the end of the file is read and verified
}

// NewFileDecoder constructs a new streaming decoder reading the file
// from r. The checksum of the file is verified transparently while
// reading. The dialect forces the dispatch of the op codes when it is
// set, a nil dialect detects it from the magic string of the header,
// see [RegisterDialect] to add support for a fork of Redis.
func NewFileDecoder(r io.Reader, dialect Dialect) *FileDecoder {
	return &FileDecoder{
		rraw:    r,
		hash:    crc64jones.New(),
		dialect: dialect,
	}
}

// Dialect returns the dialect of the file, the dialect the header names
// bound to the version of the header. A dialect given to
// [NewFileDecoder] skips the detection and binds the version of the
// header. It reports the header errors ([ErrMissingMagicString],
// [ErrInvalidVersion], [ErrUnsupportedVersion]) when the file does not
// start a supported dialect.
func (f *FileDecoder) Dialect() (Dialect, error) {
	if err := f.readHeader(); err != nil {
		return nil, err
	}
	return f.dialect, nil
}

// ReadToken reads the next [Token] of the file, advancing the read
// offset. The end of file token verifies the checksum and is returned
// with its error, the reads behind a verified end report io.EOF.
// The returned token is only valid until the next call to any method on
// that FileDecoder, see [Token.Clone].
func (f *FileDecoder) ReadToken() (Token, error) {
	if f.done {
		return Token{}, io.EOF
	}
	if err := f.readHeader(); err != nil {
		return Token{}, err
	}
	t, err := f.d.ReadToken()
	if err == nil && t.Kind() == KindOpCode && t.OpCode() == opCodeEndOfRDB {
		f.done = true
		if err := f.validateChecksum(); err != nil {
			return t, err
		}
	}
	return t, err
}

// readHeader reads the header of the file and constructs the record
// decoder behind it. The bytes of the header pass through the tee, the
// checksum of the file covers them.
func (f *FileDecoder) readHeader() error {
	if f.d != nil {
		return nil
	}
	tee := io.TeeReader(f.rraw, f.hash)
	var hdr [9]byte
	if _, err := io.ReadFull(tee, hdr[:]); err != nil {
		return err
	}
	// The header is nine bytes: the magic string followed by the version
	// number. The version is the trailing run of digits, its width depends
	// on the length of the magic string.
	i := 0
	for i < len(hdr) && (hdr[i] < '0' || hdr[i] > '9') {
		i++
	}
	if i == 0 || i == len(hdr) {
		return ErrMissingMagicString
	}
	magic := string(hdr[:i])
	ver, err := strconv.Atoi(string(hdr[i:]))
	if err != nil {
		return ErrInvalidVersion
	}
	f.ver = ver
	if f.dialect == nil {
		f.dialect = detectDialect(magic)
	}
	if f.dialect == nil {
		return ErrUnsupportedVersion
	}
	if _, maxVer := f.dialect.Header(); ver < 1 || ver > maxVer {
		return fmt.Errorf("%w: %d is not a version of the %s dialect", ErrUnsupportedVersion, ver, f.dialect)
	}
	f.dialect = f.dialect.WithVersion(ver)
	f.d = &Decoder{
		r:       tee,
		buf:     make([]byte, 0, DefaultBufferLen),
		dialect: f.dialect,
	}
	return nil
}

var disabledChecksum = []byte{0, 0, 0, 0, 0, 0, 0, 0}

// validateChecksum reads the checksum at the end of the file and
// compares it with the sum of the bytes the FileDecoder read. An
// all-zero checksum passes the validation, the way Redis disables it.
func (f *FileDecoder) validateChecksum() error {
	if f.ver < 5 {
		// Checksum was added in version 5
		return nil
	}
	var buf [8]byte
	if _, err := io.ReadFull(f.rraw, buf[:]); err != nil {
		return err
	}
	if bytes.Equal(buf[:], disabledChecksum) {
		return nil
	}
	sum := f.hash.Sum(nil)
	if !bytes.Equal(sum, buf[:]) {
		return ErrInvalidChecksum
	}
	return nil
}

// ReadToken reads the next [Token] of the file, advancing the read offset.
// It returns io.EOF when there are no more tokens.
// The returned token is only valid until the next call to any method on
// that Decoder, see [Token.Clone].
func (d *Decoder) ReadToken() (Token, error) {
	d.resetBuf()

	if d.handler != nil {
		var (
			token Token
			err   error
		)
		d.handler, token, err = d.handler.Handle(d)
		if d.handler == nil {
			d.opCode = nil
		}
		return token, err
	}

	if _, err := d.loadN(1); err != nil {
		return Token{}, err
	}
	opCode := OpCode(d.buf[0])

	d.opCode = &opCode

	if opCode == opCodeEndOfRDB {
		return Token{k: KindOpCode, raw: d.buf[:]}, nil
	}

	handler, err := d.dialect.Decoder(opCode)
	if err != nil {
		return Token{}, err
	}
	d.handler = handler
	return Token{k: KindOpCode, raw: d.buf[:]}, nil
}

func (d *Decoder) resetBuf() {
	d.buf = d.buf[:0]
}

func (d *Decoder) loadN(n int) (int, error) {
	prev := len(d.buf)
	if cap(d.buf) < (prev + n) {
		newCap := cap(d.buf) * 2
		for newCap < prev+n {
			newCap = newCap * 2
		}
		buf := make([]byte, newCap)
		copy(buf, d.buf[:])
		d.buf = buf
	}
	d.buf = d.buf[:prev+n]
	if _, err := io.ReadFull(d.r, d.buf[prev:prev+n]); err != nil {
		return prev, err
	}
	return prev, nil
}

func (d *Decoder) loadString() error {
	start := len(d.buf)
	if err := d.loadLen(); err != nil {
		return err
	}
	l, encoded, _ := decodeLen(d.buf[start:])
	if encoded {
		switch uint8(l) {
		case enc8bit:
			if _, err := d.loadN(1); err != nil {
				return err
			}
			return nil
		case enc16bit:
			if _, err := d.loadN(2); err != nil {
				return err
			}
			return nil
		case enc32bit:
			if _, err := d.loadN(4); err != nil {
				return err
			}
			return nil
		case encLZF:
			start := len(d.buf)
			if err := d.loadLen(); err != nil {
				return err
			}
			l := decodeUint64(d.buf[start:])
			if err := d.loadLen(); err != nil {
				return err
			}
			if _, err := d.loadN(int(l)); err != nil {
				return err
			}
			return nil
		}
	}
	if _, err := d.loadN(int(l)); err != nil {
		return err
	}
	return nil
}

func (d *Decoder) loadLen() error {
	pos, err := d.loadN(1)
	if err != nil {
		return err
	}
	switch d.buf[pos] >> 6 {
	case lenSpecial, len6bit:
		return nil
	case len14bit:
		if _, err := d.loadN(1); err != nil {
			return err
		}
		return nil
	}
	switch d.buf[pos] {
	case len32bit:
		if _, err := d.loadN(4); err != nil {
			return err
		}
		return nil
	case len64bit:
		if _, err := d.loadN(8); err != nil {
			return err
		}
		return nil
	}
	return fmt.Errorf("unsupported")
}

// SkipValue is equivalent to calling [Decoder.ReadToken] and discarding
// the result until the current value is complete, except that memory is
// not wasted trying to hold the entire value.
func (*Decoder) SkipValue() error {
	return errors.New("not implemented")
}

// ExpiryTokenDecoder reads the expiry timestamp of a key.
type ExpiryTokenDecoder struct{}

func (h ExpiryTokenDecoder) Handle(d *Decoder) (TokenDecoder, Token, error) {
	if _, err := d.loadN(opCodeExpiryLength[*d.opCode]); err != nil {
		return nil, Token{}, err
	}
	return nil, Token{k: KindExpiry, opCode: *d.opCode, raw: d.buf[:]}, nil
}

// StringTokensDecoder reads N strings.
type StringTokensDecoder struct {
	// N is the number of strings the value stores.
	N uint64
	i uint64
}

func (h StringTokensDecoder) Handle(d *Decoder) (TokenDecoder, Token, error) {
	if err := d.loadString(); err != nil {
		return nil, Token{}, err
	}
	if h.i >= (h.N - 1) {
		return nil, Token{k: KindString, opCode: *d.opCode, raw: d.buf[:]}, nil
	}
	h.i++
	return h, Token{k: KindString, opCode: *d.opCode, raw: d.buf[:]}, nil
}

// UintTokensDecoder reads N length encoded values.
type UintTokensDecoder struct {
	// N is the number of values the record stores.
	N uint64
	i uint64
}

func (h UintTokensDecoder) Handle(d *Decoder) (TokenDecoder, Token, error) {
	if err := d.loadLen(); err != nil {
		return nil, Token{}, err
	}
	if h.i >= (h.N - 1) {
		return nil, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
	}
	h.i++
	return h, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
}

// ByteTokenDecoder reads N raw bytes.
type ByteTokenDecoder struct {
	// N is the number of bytes the value stores.
	N int
}

func (h ByteTokenDecoder) Handle(d *Decoder) (TokenDecoder, Token, error) {
	if _, err := d.loadN(h.N); err != nil {
		return nil, Token{}, err
	}
	return nil, Token{k: KindByte, raw: d.buf[:]}, nil
}

// KeyDecoder reads a key name and continues with Next.
type KeyDecoder struct {
	// Next is the handler of the value that follows the key.
	Next TokenDecoder
}

func (h KeyDecoder) Handle(d *Decoder) (TokenDecoder, Token, error) {
	if err := d.loadString(); err != nil {
		return nil, Token{}, err
	}
	return h.Next, Token{k: KindString, opCode: *d.opCode, raw: d.buf[:]}, nil
}

// ListDecoder reads the entries of a list or a set.
type ListDecoder struct {
	step uint8
	n, i uint64
}

func (h ListDecoder) Handle(d *Decoder) (TokenDecoder, Token, error) {
	switch h.step {
	default:
		return nil, Token{}, fmt.Errorf("unimplemented list step: %d", h.step)
	case 0:
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		h.n = decodeUint64(d.buf)
		h.step++
		return h, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 1:
		if err := d.loadString(); err != nil {
			return nil, Token{}, err
		}
		if h.i >= (h.n - 1) {
			return nil, Token{k: KindString, opCode: *d.opCode, raw: d.buf[:]}, nil
		}
		h.i++
		return h, Token{k: KindString, opCode: *d.opCode, raw: d.buf[:]}, nil
	}
}

// ZsetDecoder reads the members and scores of a sorted set.
type ZsetDecoder struct {
	step uint8
	n, i uint64
}

func (h ZsetDecoder) Handle(d *Decoder) (TokenDecoder, Token, error) {
	switch h.step {
	default:
		return nil, Token{}, fmt.Errorf("unimplemented zset step: %d", h.step)
	case 0:
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		h.n = decodeUint64(d.buf) * 2
		h.step++
		return h, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 1:
		var token Token
		switch h.i & 1 {
		case 0:
			if err := d.loadString(); err != nil {
				return nil, Token{}, err
			}
			token = Token{k: KindString, opCode: *d.opCode, raw: d.buf[:]}
		case 1:
			if err := loadZsetFloat(d, *d.opCode); err != nil {
				return nil, Token{}, err
			}
			token = Token{k: KindFloat64, opCode: *d.opCode, raw: d.buf[:]}
		}
		if h.i >= (h.n - 1) {
			return nil, token, nil
		}
		h.i++
		return h, token, nil
	}
}

func loadZsetFloat(d *Decoder, opCode OpCode) error {
	switch opCode {
	case opCodeZset:
		pos, err := d.loadN(1)
		if err != nil {
			return err
		}
		switch d.buf[pos] {
		case 0xff, 0xfe, 0xfd:
			return nil
		default:
			_, err := d.loadN(int(d.buf[pos]))
			return err
		}
	case opCodeZset2:
		_, err := d.loadN(8)
		return err
	default:
		panic(fmt.Sprintf("unimplemented version: %s", d.dialect.OpCodeInfo(opCode)))
	}
}

// HashDecoder reads the field count of a hash and continues with the field
// and value strings.
type HashDecoder struct{}

func (h HashDecoder) Handle(d *Decoder) (TokenDecoder, Token, error) {
	if err := d.loadLen(); err != nil {
		return nil, Token{}, err
	}
	l := decodeUint64(d.buf)
	return StringTokensDecoder{N: l * 2}, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
}

// ZipPackDecoder reads a key and a single value, e.g. a zipmap.
type ZipPackDecoder struct{ step uint8 }

func (h ZipPackDecoder) Handle(d *Decoder) (TokenDecoder, Token, error) {
	switch h.step {
	case 0:
		if err := d.loadString(); err != nil {
			return nil, Token{}, err
		}
		h.step++
		return h, Token{k: KindString, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 1:
		if err := d.loadString(); err != nil {
			return nil, Token{}, err
		}
		return nil, Token{k: KindZipMap, opCode: *d.opCode, raw: d.buf[:]}, nil
	}
	return nil, Token{}, fmt.Errorf("unimplemented zip pack step: %d", h.step)
}

// ZipListDecoder reads a key and a single listpack.
type ZipListDecoder struct{ step uint8 }

func (h ZipListDecoder) Handle(d *Decoder) (TokenDecoder, Token, error) {
	switch h.step {
	case 0:
		if err := d.loadString(); err != nil {
			return nil, Token{}, err
		}
		h.step++
		return h, Token{k: KindString, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 1:
		if err := d.loadString(); err != nil {
			return nil, Token{}, err
		}
		return nil, Token{k: KindZipList, opCode: *d.opCode, raw: d.buf[:]}, nil
	}
	return nil, Token{}, fmt.Errorf("unimplemented zip list step: %d", h.step)
}

// SetIntSetDecoder reads a key and a single set blob.
type SetIntSetDecoder struct{ step uint8 }

func (h SetIntSetDecoder) Handle(d *Decoder) (TokenDecoder, Token, error) {
	switch h.step {
	case 0:
		if err := d.loadString(); err != nil {
			return nil, Token{}, err
		}
		h.step++
		return h, Token{k: KindString, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 1:
		if err := d.loadString(); err != nil {
			return nil, Token{}, err
		}
		return nil, Token{k: KindSetIntSet, opCode: *d.opCode, raw: d.buf[:]}, nil
	}
	return nil, Token{}, fmt.Errorf("unimplemented set int set step: %d", h.step)
}

// ZsetListDecoder reads a key and a single sorted set blob.
type ZsetListDecoder struct{ step uint8 }

func (h ZsetListDecoder) Handle(d *Decoder) (TokenDecoder, Token, error) {
	switch h.step {
	case 0:
		if err := d.loadString(); err != nil {
			return nil, Token{}, err
		}
		h.step++
		return h, Token{k: KindString, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 1:
		if err := d.loadString(); err != nil {
			return nil, Token{}, err
		}
		return nil, Token{k: KindZsetList, opCode: *d.opCode, raw: d.buf[:]}, nil
	}
	return nil, Token{}, fmt.Errorf("unimplemented zset list step: %d", h.step)
}

// QuickListDecoder reads the nodes of a quicklist.
type QuickListDecoder struct {
	step uint8
	n, i uint64
}

func (h QuickListDecoder) Handle(d *Decoder) (TokenDecoder, Token, error) {
	switch h.step {
	default:
		return nil, Token{}, fmt.Errorf("unimplemented quick list step: %d", h.step)
	case 0:
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		h.step++
		h.n = decodeUint64(d.buf)
		return h, Token{k: KindUint, raw: d.buf[:]}, nil
	case 1:
		if err := d.loadString(); err != nil {
			return nil, Token{}, err
		}
		if h.i >= (h.n - 1) {
			return nil, Token{k: KindQuickList, opCode: *d.opCode, raw: d.buf[:]}, nil
		}
		h.i++
		return h, Token{k: KindQuickList, opCode: *d.opCode, raw: d.buf[:]}, nil
	}
}

// QuickList2Decoder reads the nodes of a quicklist, where each node carries
// its container type.
type QuickList2Decoder struct {
	step uint8
	n, i uint64
}

func (h QuickList2Decoder) Handle(d *Decoder) (TokenDecoder, Token, error) {
	switch h.step {
	default:
		return nil, Token{}, fmt.Errorf("unimplemented quick list 2 step: %d", h.step)
	case 0:
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		h.step++
		h.n = decodeUint64(d.buf)
		return h, Token{k: KindUint, raw: d.buf[:]}, nil
	case 1:
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		if err := d.loadString(); err != nil {
			return nil, Token{}, err
		}
		if h.i >= (h.n - 1) {
			return nil, Token{k: KindQuickList, opCode: *d.opCode, raw: d.buf[:]}, nil
		}
		h.i++
		return h, Token{k: KindQuickList, opCode: *d.opCode, raw: d.buf[:]}, nil
	}
}

// StreamListDecoder reads a stream value of any of the StreamListpacks op
// codes, including its metadata, its consumer groups and their IDMP and NACK
// zone data.
type StreamListDecoder struct {
	step uint8
	n, i uint64
	sub  TokenDecoder
}

func (h StreamListDecoder) Handle(d *Decoder) (TokenDecoder, Token, error) {
	switch h.step {
	default:
		return nil, Token{}, fmt.Errorf("unimplemented stream list step: %d", h.step)
	case 0: // number of listpacks
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		h.n = decodeUint64(d.buf) * 2 // each entry is a stream ID and a listpack
		h.i = 0
		h.step = 1
		if h.n == 0 {
			h.step = 2
			h.n = streamMetaLen(*d.opCode)
			h.i = 0
		}
		return h, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 1: // listpack entries
		var k Kind
		if h.i&1 == 0 {
			k = KindStreamID
		} else {
			k = KindString
		}
		if err := d.loadString(); err != nil {
			return nil, Token{}, err
		}
		h.i++
		if h.i >= h.n {
			h.step = 2
			h.n = streamMetaLen(*d.opCode)
			h.i = 0
		}
		return h, Token{k: k, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 2: // stream metadata
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		h.i++
		if h.i >= h.n {
			h.step = 3
		}
		return h, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 3: // number of consumer groups
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		h.n = decodeUint64(d.buf)
		h.i = 0
		if h.n == 0 {
			if *d.opCode >= opCodeStreamListpacks4 {
				h.step = 6
				h.sub = StreamIdmpDecoder{}
			} else {
				return nil, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
			}
		} else {
			h.step = 4
			h.sub = StreamGroupDecoder{}
		}
		return h, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 4: // consumer groups
		next, token, err := h.sub.Handle(d)
		if err != nil {
			return nil, Token{}, err
		}
		if next == nil {
			h.sub = nil
			h.i++
			if h.i >= h.n {
				if *d.opCode >= opCodeStreamListpacks4 {
					h.step = 6
					h.sub = StreamIdmpDecoder{}
				} else {
					return nil, token, nil
				}
			} else {
				h.sub = StreamGroupDecoder{}
			}
			return h, token, nil
		}
		h.sub = next
		return h, token, nil
	case 6: // IDMP (Idempotent Message Producer) data
		next, token, err := h.sub.Handle(d)
		if err != nil {
			return nil, Token{}, err
		}
		if next == nil {
			return nil, token, nil
		}
		h.sub = next
		return h, token, nil
	}
}

// streamMetaLen returns the number of length encoded fields in the metadata
// of a stream.
func streamMetaLen(opCode OpCode) uint64 {
	if opCode >= opCodeStreamListpacks2 {
		return 8 // length, last ID, first ID, max deleted ID, added count
	}
	return 3 // length, last ID
}

// StreamGroupDecoder reads a stream consumer group: its pending entries, its
// consumers and its NACK zone.
type StreamGroupDecoder struct {
	step uint8
	n, i uint64
	sub  TokenDecoder
}

func (h StreamGroupDecoder) Handle(d *Decoder) (TokenDecoder, Token, error) {
	switch h.step {
	default:
		return nil, Token{}, fmt.Errorf("unimplemented stream group step: %d", h.step)
	case 0: // group name
		if err := d.loadString(); err != nil {
			return nil, Token{}, err
		}
		h.step = 1
		return h, Token{k: KindString, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 1: // last ID
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		h.step = 2
		return h, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 2: // last ID sequence
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		if *d.opCode >= opCodeStreamListpacks2 {
			h.step = 3
		} else {
			h.step = 4
		}
		return h, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 3: // entries read
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		h.step = 4
		return h, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 4: // number of pending entries
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		h.n = decodeUint64(d.buf)
		h.i = 0
		if h.n == 0 {
			h.step = 8
		} else {
			h.step = 5
		}
		return h, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 5: // pending entry stream ID
		if _, err := d.loadN(16); err != nil {
			return nil, Token{}, err
		}
		h.step++
		return h, Token{k: KindStreamID, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 6: // pending entry delivery time
		if _, err := d.loadN(8); err != nil {
			return nil, Token{}, err
		}
		h.step++
		return h, Token{k: KindTime, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 7: // pending entry delivery count
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		h.i++
		if h.i >= h.n {
			h.step = 8
		} else {
			h.step = 5
		}
		return h, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 8: // number of consumers
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		h.n = decodeUint64(d.buf)
		h.i = 0
		if h.n == 0 {
			if *d.opCode >= opCodeStreamListpacks5 {
				h.step = 10
			} else {
				return nil, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
			}
		} else {
			h.step = 9
			h.sub = StreamConsumerDecoder{}
		}
		return h, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 9: // consumers
		next, token, err := h.sub.Handle(d)
		if err != nil {
			return nil, Token{}, err
		}
		if next == nil {
			h.sub = nil
			h.i++
			if h.i >= h.n {
				if *d.opCode >= opCodeStreamListpacks5 {
					h.step = 10
				} else {
					return nil, token, nil
				}
			} else {
				h.sub = StreamConsumerDecoder{}
			}
			return h, token, nil
		}
		h.sub = next
		return h, token, nil
	case 10: // NACK zone entry count
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		h.n = decodeUint64(d.buf)
		h.i = 0
		if h.n == 0 {
			return nil, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
		}
		h.step = 11
		return h, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 11: // NACK zone entry stream ID
		if _, err := d.loadN(16); err != nil {
			return nil, Token{}, err
		}
		h.i++
		if h.i >= h.n {
			return nil, Token{k: KindStreamID, opCode: *d.opCode, raw: d.buf[:]}, nil
		}
		return h, Token{k: KindStreamID, opCode: *d.opCode, raw: d.buf[:]}, nil
	}
}

// StreamConsumerDecoder reads a stream consumer: the times it was seen and
// active and its pending entries.
type StreamConsumerDecoder struct {
	step uint8
	n, i uint64
}

func (h StreamConsumerDecoder) Handle(d *Decoder) (TokenDecoder, Token, error) {
	switch h.step {
	default:
		return nil, Token{}, fmt.Errorf("unimplemented stream consumer step: %d", h.step)
	case 0: // consumer name
		if err := d.loadString(); err != nil {
			return nil, Token{}, err
		}
		if *d.opCode >= opCodeStreamListpacks3 {
			h.step = 1
		} else {
			h.step = 2
		}
		return h, Token{k: KindString, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 1: // seen time
		if _, err := d.loadN(8); err != nil {
			return nil, Token{}, err
		}
		h.step = 2
		return h, Token{k: KindTime, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 2: // active time
		if _, err := d.loadN(8); err != nil {
			return nil, Token{}, err
		}
		h.step = 3
		return h, Token{k: KindTime, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 3: // number of pending entries
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		h.n = decodeUint64(d.buf)
		h.i = 0
		if h.n == 0 {
			return nil, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
		}
		h.step = 4
		return h, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 4: // pending entry stream ID
		if _, err := d.loadN(16); err != nil {
			return nil, Token{}, err
		}
		h.i++
		if h.i >= h.n {
			return nil, Token{k: KindStreamID, opCode: *d.opCode, raw: d.buf[:]}, nil
		}
		return h, Token{k: KindStreamID, opCode: *d.opCode, raw: d.buf[:]}, nil
	}
}

// StreamIdmpDecoder reads the IDMP (Idempotent Message Producer) data of a
// stream: its configuration, its producers with their entries and the stats.
type StreamIdmpDecoder struct {
	step  uint8
	p, pi uint64 // producers
	n, i  uint64 // entries of the current producer
}

func (h StreamIdmpDecoder) Handle(d *Decoder) (TokenDecoder, Token, error) {
	switch h.step {
	default:
		return nil, Token{}, fmt.Errorf("unimplemented stream IDMP step: %d", h.step)
	case 0: // duration
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		h.step = 1
		return h, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 1: // maximum entries
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		h.step = 2
		return h, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 2: // number of producers
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		h.p = decodeUint64(d.buf)
		h.pi = 0
		if h.p == 0 {
			h.step = 8
		} else {
			h.step = 3
		}
		return h, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 3: // producer ID
		if err := d.loadString(); err != nil {
			return nil, Token{}, err
		}
		h.step = 4
		return h, Token{k: KindString, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 4: // number of entries of the current producer
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		h.n = decodeUint64(d.buf)
		h.i = 0
		if h.n == 0 {
			h.pi++
			if h.pi >= h.p {
				h.step = 8
			} else {
				h.step = 3
			}
		} else {
			h.step = 5
		}
		return h, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 5: // entry IID
		if err := d.loadString(); err != nil {
			return nil, Token{}, err
		}
		h.step = 6
		return h, Token{k: KindString, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 6: // entry stream ID
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		h.step = 7
		return h, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 7: // entry stream ID sequence
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		h.i++
		if h.i < h.n {
			h.step = 5
		} else {
			h.pi++
			if h.pi < h.p {
				h.step = 3
			} else {
				h.step = 8
			}
		}
		return h, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 8: // all-time count of added IIDs
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		h.step = 9
		return h, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 9: // all-time count of duplicate IIDs
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		return nil, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
	}
}

// KeyMetaDecoder reads key metadata: the number of metadata classes and for
// each class its specification and module values.
type KeyMetaDecoder struct {
	step    uint8
	classes uint64
}

func (h KeyMetaDecoder) Handle(d *Decoder) (TokenDecoder, Token, error) {
	switch h.step {
	default:
		return nil, Token{}, fmt.Errorf("unimplemented key meta step: %d", h.step)
	case 0:
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		h.classes = decodeUint64(d.buf)
		if h.classes == 0 {
			return nil, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
		}
		h.step = 1
		return h, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 1:
		if _, err := d.loadN(4); err != nil {
			return nil, Token{}, err
		}
		h.step = 2
		return h, Token{k: KindByte, raw: d.buf[:]}, nil
	case 2:
		token, eof, err := loadModuleValue(d)
		if err != nil {
			return nil, Token{}, err
		}
		if !eof {
			return h, token, nil
		}
		h.classes--
		if h.classes == 0 {
			return nil, token, nil
		}
		h.step = 1
		return h, token, nil
	}
}

// loadModuleValue reads a single module value and reports whether it was the
// EOF module op code.
func loadModuleValue(d *Decoder) (Token, bool, error) {
	if _, err := d.loadN(1); err != nil {
		return Token{}, false, err
	}
	moduleOpCode := int(d.buf[0])
	switch moduleOpCode {
	case ModuleOpCodeEOF:
		return Token{k: KindOpCode, format: formatModule, raw: d.buf[:]}, true, nil
	case ModuleOpCodeSInt, ModuleOpCodeUInt:
		if err := d.loadLen(); err != nil {
			return Token{}, false, err
		}
	case ModuleOpCodeFloat:
		if _, err := d.loadN(4); err != nil {
			return Token{}, false, err
		}
	case ModuleOpCodeDouble:
		if _, err := d.loadN(8); err != nil {
			return Token{}, false, err
		}
	case ModuleOpCodeString:
		if err := d.loadString(); err != nil {
			return Token{}, false, err
		}
	default:
		return Token{}, false, fmt.Errorf("unimplemented module token: %d", moduleOpCode)
	}
	return Token{k: kindModuleMap[moduleOpCode], format: formatModule, raw: d.buf[:]}, false, nil
}

// Module2Decoder reads a module value: the key name, the module ID and the
// module values. Module auxiliary values omit the key name. They start from
// the module ID, so construct them with Step 1.
type Module2Decoder struct {
	// Step is 0 for a module value and 1 for a module auxiliary value.
	Step uint8
}

func (h Module2Decoder) Handle(d *Decoder) (TokenDecoder, Token, error) {
	switch h.Step {
	case 0:
		if err := d.loadString(); err != nil {
			return nil, Token{}, err
		}
		h.Step++
		return h, Token{k: KindString, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 1:
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		h.Step++
		return h, Token{k: KindString, format: formatModuleID, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 2:
		token, eof, err := loadModuleValue(d)
		if err != nil {
			return nil, Token{}, err
		}
		if eof {
			return nil, token, nil
		}
		return h, token, nil
	}
	return nil, Token{}, fmt.Errorf("unimplemented module 2 step: %d", h.Step)
}

// HashMetadataDecoder reads a hash with per field expiry: the minimum expiry
// of the fields, the field count and the fields with their expiry. The
// pre-release format has no minimum expiry, so construct it with Step 1.
type HashMetadataDecoder struct {
	// Step is 0 for the released format and 1 for the pre-release format
	// without the minimum expiry.
	Step uint8
	n    uint64
	sub  TokenDecoder
}

func (h HashMetadataDecoder) Handle(d *Decoder) (TokenDecoder, Token, error) {
	switch h.Step {
	default:
		return nil, Token{}, fmt.Errorf("unimplemented hash metadata step: %d", h.Step)
	case 0: // minimum expiry of the fields
		if _, err := d.loadN(8); err != nil {
			return nil, Token{}, err
		}
		h.Step = 1
		return h, Token{k: KindExpiry, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 1: // number of fields
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		h.n = decodeUint64(d.buf)
		if h.n == 0 {
			return nil, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
		}
		h.Step = 2
		return h, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 2: // field expiry
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		h.sub = StringTokensDecoder{N: 2}
		h.Step = 3
		return h, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 3: // field and value
		next, token, err := h.sub.Handle(d)
		if err != nil {
			return nil, Token{}, err
		}
		if next == nil {
			h.sub = nil
			h.n--
			if h.n == 0 {
				return nil, token, nil
			}
			h.Step = 2
			return h, token, nil
		}
		h.sub = next
		return h, token, nil
	}
}

// HashListpackExDecoder reads the minimum expiry of the fields and the
// entries of a hash with per field expiry stored as a listpack.
type HashListpackExDecoder struct{ step uint8 }

func (h HashListpackExDecoder) Handle(d *Decoder) (TokenDecoder, Token, error) {
	switch h.step {
	case 0:
		if _, err := d.loadN(8); err != nil {
			return nil, Token{}, err
		}
		h.step++
		return h, Token{k: KindExpiry, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 1:
		if err := d.loadString(); err != nil {
			return nil, Token{}, err
		}
		return nil, Token{k: KindZipList, opCode: *d.opCode, raw: d.buf[:]}, nil
	}
	return nil, Token{}, fmt.Errorf("unimplemented hash listpack ex step: %d", h.step)
}

// HashTemplateDecoder reads a hash template record and registers its field
// count, so hash values can reference it by ID.
type HashTemplateDecoder struct {
	step uint8
	id   uint64
	n, i uint64
}

func (h HashTemplateDecoder) Handle(d *Decoder) (TokenDecoder, Token, error) {
	switch h.step {
	case 0:
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		h.id = decodeUint64(d.buf)
		h.step++
		return h, Token{k: KindUint, raw: d.buf[:]}, nil
	case 1:
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		h.n = decodeUint64(d.buf)
		if h.n == 0 {
			return nil, Token{}, fmt.Errorf("rdb: hash template %d has zero fields", h.id)
		}
		if _, ok := d.templates[h.id]; ok {
			return nil, Token{}, fmt.Errorf("rdb: duplicate hash template id %d", h.id)
		}
		if d.templates == nil {
			d.templates = make(map[uint64]uint64)
		}
		d.templates[h.id] = h.n
		h.step++
		return h, Token{k: KindUint, raw: d.buf[:]}, nil
	case 2:
		if err := d.loadString(); err != nil {
			return nil, Token{}, err
		}
		if h.i >= (h.n - 1) {
			return nil, Token{k: KindString, raw: d.buf[:]}, nil
		}
		h.i++
		return h, Token{k: KindString, raw: d.buf[:]}, nil
	}
	return nil, Token{}, fmt.Errorf("unimplemented hash template step: %d", h.step)
}

// HashTmplArrayRefDecoder reads the template ID of an array referenced hash
// and continues with as many value strings as the template has fields.
type HashTmplArrayRefDecoder struct{}

func (h HashTmplArrayRefDecoder) Handle(d *Decoder) (TokenDecoder, Token, error) {
	if err := d.loadLen(); err != nil {
		return nil, Token{}, err
	}
	n, ok := d.templates[decodeUint64(d.buf)]
	if !ok {
		return nil, Token{}, fmt.Errorf("rdb: unknown hash template id")
	}
	return StringTokensDecoder{N: n}, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
}

// HashTmplSelfDecoder reads a self-contained template encoded hash, which
// carries its field names inline. LP selects the listpack value encoding.
type HashTmplSelfDecoder struct {
	// LP selects the listpack encoding of the hash values.
	LP   bool
	step uint8
	n, i uint64
}

func (h HashTmplSelfDecoder) Handle(d *Decoder) (TokenDecoder, Token, error) {
	switch h.step {
	case 0:
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		switch decodeUint64(d.buf) {
		case 0:
			h.step = 1
		case 1:
			h.step = 2
		default:
			return nil, Token{}, fmt.Errorf("rdb: unknown hash template fields format")
		}
		return h, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 1:
		if err := d.loadString(); err != nil {
			return nil, Token{}, err
		}
		n, err := lpCountEntries(d.buf)
		if err != nil {
			return nil, Token{}, err
		}
		h.n = n
		h.i = 0
		if h.LP {
			h.step = 4
		} else {
			h.step = 5
		}
		return h, Token{k: KindZipList, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 2:
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		h.n = decodeUint64(d.buf)
		if h.n == 0 {
			return nil, Token{}, fmt.Errorf("rdb: hash template has zero fields")
		}
		h.step = 3
		return h, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 3:
		if err := d.loadString(); err != nil {
			return nil, Token{}, err
		}
		if h.i >= (h.n - 1) {
			if h.LP {
				h.step = 4
			} else {
				h.step = 5
			}
			h.i = 0
		} else {
			h.i++
		}
		return h, Token{k: KindString, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 4:
		if err := d.loadString(); err != nil {
			return nil, Token{}, err
		}
		return nil, Token{k: KindZipList, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 5:
		if err := d.loadString(); err != nil {
			return nil, Token{}, err
		}
		if h.i >= (h.n - 1) {
			return nil, Token{k: KindString, opCode: *d.opCode, raw: d.buf[:]}, nil
		}
		h.i++
		return h, Token{k: KindString, opCode: *d.opCode, raw: d.buf[:]}, nil
	}
	return nil, Token{}, fmt.Errorf("unimplemented hash template self step: %d", h.step)
}

// ArrayDecoder reads a sparse array: the element count, the optional insert
// index and the tagged elements.
type ArrayDecoder struct {
	step uint8
	n, i uint64
	tag  uint64
}

func (h ArrayDecoder) Handle(d *Decoder) (TokenDecoder, Token, error) {
	switch h.step {
	default:
		return nil, Token{}, fmt.Errorf("unimplemented array step: %d", h.step)
	case 0:
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		h.n = decodeUint64(d.buf)
		if h.n == 0 {
			return nil, Token{}, fmt.Errorf("rdb: array count == 0 is invalid")
		}
		h.step = 1
		return h, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 1:
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		flag := decodeUint64(d.buf)
		if flag > 1 {
			return nil, Token{}, fmt.Errorf("rdb: invalid array insert index flag %d", flag)
		}
		if flag == 1 {
			h.step = 2
		} else {
			h.step = 3
		}
		return h, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 2:
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		h.step = 3
		return h, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 3:
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		if _, encoded, _ := decodeLen(d.buf); encoded {
			return nil, Token{}, fmt.Errorf("rdb: invalid array index (special encoding)")
		}
		h.step = 4
		return h, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 4:
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		h.tag = decodeUint64(d.buf)
		if h.tag > 3 {
			return nil, Token{}, fmt.Errorf("rdb: unknown array element type tag %d", h.tag)
		}
		h.step = 5
		return h, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 5:
		var k Kind
		switch h.tag {
		case 0, 3:
			if err := d.loadString(); err != nil {
				return nil, Token{}, err
			}
			k = KindString
		case 1:
			if _, err := d.loadN(8); err != nil {
				return nil, Token{}, err
			}
			k = KindInt
		case 2:
			if _, err := d.loadN(8); err != nil {
				return nil, Token{}, err
			}
			k = KindFloat64
		}
		h.i++
		if h.i >= h.n {
			return nil, Token{k: k, opCode: *d.opCode, raw: d.buf[:]}, nil
		}
		h.step = 3
		return h, Token{k: k, opCode: *d.opCode, raw: d.buf[:]}, nil
	}
}

// CronDecoder reads a KeyDB cron job: the script, the start time, the
// interval and the key and value arguments.
type CronDecoder struct {
	step uint8
	n, i uint64
}

func (h CronDecoder) Handle(d *Decoder) (TokenDecoder, Token, error) {
	switch h.step {
	default:
		return nil, Token{}, fmt.Errorf("unimplemented cron step: %d", h.step)
	case 0:
		if err := d.loadString(); err != nil {
			return nil, Token{}, err
		}
		h.step = 1
		return h, Token{k: KindString, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 1, 2:
		if _, err := d.loadN(8); err != nil {
			return nil, Token{}, err
		}
		h.step++
		return h, Token{k: KindTime, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 3:
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		h.n = decodeUint64(d.buf)
		h.i = 0
		if h.n == 0 {
			h.step = 5
		} else {
			h.step = 4
		}
		return h, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 4:
		if err := d.loadString(); err != nil {
			return nil, Token{}, err
		}
		h.i++
		if h.i >= h.n {
			h.step = 5
		}
		return h, Token{k: KindString, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 5:
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		h.n = decodeUint64(d.buf)
		h.i = 0
		if h.n == 0 {
			return nil, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
		}
		h.step = 6
		return h, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 6:
		if err := d.loadString(); err != nil {
			return nil, Token{}, err
		}
		h.i++
		if h.i >= h.n {
			return nil, Token{k: KindString, opCode: *d.opCode, raw: d.buf[:]}, nil
		}
		return h, Token{k: KindString, opCode: *d.opCode, raw: d.buf[:]}, nil
	}
}

// lpCountEntries counts the entries of a listpack blob. The raw bytes of a
// string token include the string encoding, which is stripped here.
func lpCountEntries(raw []byte) (uint64, error) {
	l, encoded, n := decodeLen(raw)
	if encoded {
		return 0, fmt.Errorf("%w: compressed listpack is not supported", ErrCorruptPayload)
	}
	blob := raw[n : n+int(l)]
	if len(blob) < 7 || blob[len(blob)-1] != 0xff {
		return 0, fmt.Errorf("%w: invalid listpack", ErrCorruptPayload)
	}
	total := int(binary.LittleEndian.Uint32(blob[0:4]))
	if total > len(blob) {
		return 0, fmt.Errorf("%w: invalid listpack", ErrCorruptPayload)
	}
	p, count := 6, uint64(0)
	for p < total-1 {
		b := blob[p]
		var entryLen int
		switch {
		case b < 0x80:
			entryLen = 1
		case b < 0xC0:
			entryLen = 1 + int(b&0x3f)
		case b < 0xE0:
			entryLen = 2
		case b < 0xF0:
			if p+2 > len(blob) {
				return 0, fmt.Errorf("%w: invalid listpack", ErrCorruptPayload)
			}
			entryLen = 2 + int(b&0xf)<<8 | int(blob[p+1])
		case b == 0xF0:
			if p+5 > len(blob) {
				return 0, fmt.Errorf("%w: invalid listpack", ErrCorruptPayload)
			}
			entryLen = 5 + int(binary.LittleEndian.Uint32(blob[p+1:p+5]))
		case b == 0xF1:
			entryLen = 3
		case b == 0xF2:
			entryLen = 4
		case b == 0xF3:
			entryLen = 5
		case b == 0xF4:
			entryLen = 9
		default:
			return 0, fmt.Errorf("%w: invalid listpack entry encoding %x", ErrCorruptPayload, b)
		}
		p += entryLen + lpBacklenBytes(entryLen)
		count++
	}
	return count, nil
}

// lpBacklenBytes returns the number of bytes used to encode the backlen of a
// listpack entry of the given size, see lpEncodeBacklenBytes.
func lpBacklenBytes(l int) int {
	switch {
	case l <= 127:
		return 1
	case l <= 16383:
		return 2
	case l <= 2097151:
		return 3
	case l <= 268435455:
		return 4
	default:
		return 5
	}
}

// Hash2Decoder reads a Valkey hash with per field expiry: the number of
// entries and for each entry the field, the value and the expiry. Valkey
// stores it under the value type Redis uses for HashMetadataPreGa.
type Hash2Decoder struct {
	step uint8
	n, i uint64
}

func (h Hash2Decoder) Handle(d *Decoder) (TokenDecoder, Token, error) {
	switch h.step {
	default:
		return nil, Token{}, fmt.Errorf("unimplemented hash2 step: %d", h.step)
	case 0:
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		h.n = decodeUint64(d.buf)
		h.i = 0
		if h.n == 0 {
			return nil, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
		}
		h.step = 1
		return h, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 1: // field
		if err := d.loadString(); err != nil {
			return nil, Token{}, err
		}
		h.step = 2
		return h, Token{k: KindString, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 2: // value
		if err := d.loadString(); err != nil {
			return nil, Token{}, err
		}
		h.step = 3
		return h, Token{k: KindString, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 3: // expiry
		if _, err := d.loadN(8); err != nil {
			return nil, Token{}, err
		}
		h.i++
		if h.i >= h.n {
			return nil, Token{k: KindTime, opCode: *d.opCode, raw: d.buf[:]}, nil
		}
		h.step = 1
		return h, Token{k: KindTime, opCode: *d.opCode, raw: d.buf[:]}, nil
	}
}

// SlotImportDecoder reads the slot import state: the job name, the number of
// slot ranges and the ranges themselves.
type SlotImportDecoder struct {
	step uint8
	n, i uint64
}

func (h SlotImportDecoder) Handle(d *Decoder) (TokenDecoder, Token, error) {
	switch h.step {
	default:
		return nil, Token{}, fmt.Errorf("unimplemented slot import step: %d", h.step)
	case 0: // job name
		if err := d.loadString(); err != nil {
			return nil, Token{}, err
		}
		h.step = 1
		return h, Token{k: KindString, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 1: // number of slot ranges
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		h.n = decodeUint64(d.buf)
		h.i = 0
		if h.n == 0 {
			return nil, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
		}
		h.step = 2
		return h, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 2: // start slot
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		h.step = 3
		return h, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
	case 3: // end slot
		if err := d.loadLen(); err != nil {
			return nil, Token{}, err
		}
		h.i++
		if h.i >= h.n {
			return nil, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
		}
		h.step = 2
		return h, Token{k: KindUint, opCode: *d.opCode, raw: d.buf[:]}, nil
	}
}
