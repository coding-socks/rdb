package rdb

import (
	"fmt"
	"hash"
	"io"

	"github.com/coding-socks/rdb/internal/crc64jones"
)

// TokenEncoder writes the tokens of the value an op code starts.
// Handle writes t and returns the encoder that writes the token after
// it: an encoder returns itself to continue a sequence, or nil when
// the value is complete.
type TokenEncoder interface {
	Handle(e *Encoder, t Token) (TokenEncoder, error)
}

// Encoder is a streaming encoder for the records of an RDB file.
// It is used to write the records of a file token by token.
//
// Every value record starts with an op code token that selects the
// handler behind it, the tokens of its key and value follow it, see
// [TokenEncoder]. The header of the file and the checksum at its end
// are the business of [FileEncoder].
//
// The dialect of the records is fixed when the Encoder is constructed,
// see [NewEncoder].
type Encoder struct {
	w io.Writer

	dialect Dialect

	opCode  *OpCode
	handler TokenEncoder

	templates map[uint64]uint64
}

// NewEncoder constructs a new encoder writing the records of an RDB
// file to w. The dialect dispatches the op codes of the records and
// gates the versions they were introduced in. The zero dialect selects
// the [RedisDialect] in its newest version.
func NewEncoder(w io.Writer, dialect Dialect) *Encoder {
	if dialect == nil {
		dialect = RedisDialect{}
	}
	return &Encoder{
		w:       w,
		dialect: dialect,
	}
}

// FileEncoder writes an RDB file: the header of the file, the records
// behind it, and the checksum at its end. The header carries the magic
// string and the version of the dialect, the end of file token closes
// the file with the checksum of the written bytes. The versions before
// 5 carry no checksum, the way Redis writes none for them. The tokens
// behind an end of file token write on, a further end of file token
// writes another checksum, the merged payloads of a sync transfer
// among them.
type FileEncoder struct {
	w    io.Writer // the destination, the write of the checksum bypasses the multi writer
	e    *Encoder  // the record encoder over the multi writer, nil until the header was written
	hash hash.Hash

	dialect Dialect // the dialect of the file
	ver     int     // the version of the header, the gate of the checksum writes it
}

// NewFileEncoder constructs a new streaming encoder writing the file
// to w. The dialect is the dialect of the output file: the header
// carries the version the dialect represents and the file may only
// contain the op codes of the version and the ones before it. The zero
// dialect selects the [RedisDialect] in its newest version.
//
// The header of the file is written with the first token and the
// checksum with the end of file token.
func NewFileEncoder(w io.Writer, dialect Dialect) *FileEncoder {
	if dialect == nil {
		dialect = RedisDialect{}
	}
	return &FileEncoder{
		w:       w,
		hash:    crc64jones.New(),
		dialect: dialect,
	}
}

// WriteToken writes the next token of the file and advances the
// internal write offset. The provided token must be consistent with
// the grammar of the RDB format: an op code token starts a record,
// the tokens of its value follow it, and the end of file token closes
// the file with its checksum. A version the dialect does not support
// fails the first token with [ErrInvalidVersion], the bytes of the
// header are written behind it.
func (f *FileEncoder) WriteToken(t Token) error {
	if err := f.writeHeader(); err != nil {
		return err
	}
	if err := f.e.WriteToken(t); err != nil {
		return err
	}
	if t.Kind() == KindOpCode && t.OpCode() == opCodeEndOfRDB {
		return f.writeChecksum()
	}
	return nil
}

// WriteTokens writes the tokens of the file in the order they are
// given.
func (f *FileEncoder) WriteTokens(t ...Token) error {
	for _, t := range t {
		if err := f.WriteToken(t); err != nil {
			return err
		}
	}
	return nil
}

func (f *FileEncoder) writeHeader() error {
	if f.e != nil {
		return nil
	}
	magic, ver := f.dialect.Header()
	// A version the dialect does not support fails the write, the
	// zero version of the probe names the newest one it supports.
	if _, maxVer := f.dialect.WithVersion(0).Header(); ver < 1 || ver > maxVer {
		return fmt.Errorf("%w: %d is not a version of the %T dialect", ErrInvalidVersion, ver, f.dialect)
	}
	mw := io.MultiWriter(f.w, f.hash)
	if _, err := fmt.Fprintf(mw, "%s%0*d", magic, 9-len(magic), ver); err != nil {
		return fmt.Errorf("rdb: failed to write header: %w", err)
	}
	f.ver = ver
	f.e = &Encoder{
		w:       mw,
		dialect: f.dialect,
	}
	return nil
}

func (f *FileEncoder) writeChecksum() error {
	if f.ver < 5 {
		// Checksum was added in version 5
		return nil
	}
	sum := f.hash.Sum(nil)
	if _, err := f.w.Write(sum); err != nil {
		return fmt.Errorf("rdb: failed to write checksum: %w", err)
	}
	return nil
}

// WriteTokens writes the tokens of the records in the order they are
// given.
func (e *Encoder) WriteTokens(t ...Token) error {
	for _, t := range t {
		if err := e.WriteToken(t); err != nil {
			return err
		}
	}
	return nil
}

// WriteToken writes the next token of the record and advances the
// internal write offset. The provided token must be consistent with
// the grammar of the RDB format: an op code token starts a record,
// the tokens of its value follow it, the end of file token is written
// verbatim, the checksum of the file is the business of
// [FileEncoder].
func (e *Encoder) WriteToken(t Token) error {
	if e.handler != nil {
		var err error
		e.handler, err = e.handler.Handle(e, t)
		if e.handler == nil {
			e.opCode = nil
		}
		return err
	}

	if t.Kind() != KindOpCode {
		return fmt.Errorf("%w: invalid token kind %d", ErrUnexpectedToken, t.Kind())
	}

	opCode := t.OpCode()
	e.opCode = &opCode

	if opCode == opCodeEndOfRDB {
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return err
		}
		return nil
	}

	handler, err := e.dialect.Encoder(opCode)
	if err != nil {
		return err
	}
	e.handler = handler
	if _, err := e.w.Write(t.AsBytes()); err != nil {
		return err
	}
	return nil
}

// ExpiryTokenEncoder writes the expiry timestamp of a key.
type ExpiryTokenEncoder struct{}

func (h ExpiryTokenEncoder) Handle(e *Encoder, t Token) (TokenEncoder, error) {
	if t.k != KindExpiry {
		return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindExpiry)
	}
	if _, err := e.w.Write(t.AsBytes()); err != nil {
		return nil, err
	}
	return nil, nil
}

// StringTokensEncoder writes N strings.
type StringTokensEncoder struct {
	// N is the number of strings the value stores.
	N uint64
	i uint64
}

func (h StringTokensEncoder) Handle(e *Encoder, t Token) (TokenEncoder, error) {
	if t.k != KindString {
		return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindString)
	}
	if _, err := e.w.Write(t.AsBytes()); err != nil {
		return nil, err
	}
	if h.i >= (h.N - 1) {
		return nil, nil
	}
	h.i++
	return h, nil
}

// UintTokensEncoder writes N length encoded values.
type UintTokensEncoder struct {
	// N is the number of values the record stores.
	N uint64
	i uint64
}

func (h UintTokensEncoder) Handle(e *Encoder, t Token) (TokenEncoder, error) {
	if t.k != KindUint {
		return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
	}
	if _, err := e.w.Write(t.AsBytes()); err != nil {
		return nil, err
	}
	if h.i >= (h.N - 1) {
		return nil, nil
	}
	h.i++
	return h, nil
}

// ByteTokenEncoder writes N raw bytes.
type ByteTokenEncoder struct {
	// N is the number of bytes the value stores.
	N int
}

func (h ByteTokenEncoder) Handle(e *Encoder, t Token) (TokenEncoder, error) {
	if t.k != KindByte {
		return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindByte)
	}
	if _, err := e.w.Write(t.AsBytes()); err != nil {
		return nil, err
	}
	return nil, nil
}

// KeyTokenEncoder writes a key name and continues with Next.
type KeyTokenEncoder struct {
	// Next is the encoder of the value that follows the key.
	Next TokenEncoder
}

func (h KeyTokenEncoder) Handle(e *Encoder, t Token) (TokenEncoder, error) {
	if t.k != KindString {
		return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindString)
	}
	if _, err := e.w.Write(t.AsBytes()); err != nil {
		return nil, err
	}
	return h.Next, nil
}

// ListEncoder writes the entries of a list or a set.
type ListEncoder struct {
	step uint8
	n, i uint64
}

func (h ListEncoder) Handle(e *Encoder, t Token) (TokenEncoder, error) {
	switch h.step {
	default:
		return nil, fmt.Errorf("unimplemented list step: %d", h.step)
	case 0:
		if t.k != KindUint {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.n = t.Uint64()
		h.step++
		return h, nil
	case 1:
		if t.k != KindString {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindString)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		if h.i >= (h.n - 1) {
			return nil, nil
		}
		h.i++
		return h, nil
	}
}

// ZsetEncoder writes the members and scores of a sorted set.
type ZsetEncoder struct {
	step uint8
	n, i uint64
}

func (h ZsetEncoder) Handle(e *Encoder, t Token) (TokenEncoder, error) {
	switch h.step {
	default:
		return nil, fmt.Errorf("unimplemented zset step: %d", h.step)
	case 0:
		if t.k != KindUint {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.n = t.Uint64() * 2
		h.step++
		return h, nil
	case 1:
		k := KindFloat64
		if h.i&1 == 0 {
			k = KindString
		}
		if t.k != k {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, k)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		if h.i >= (h.n - 1) {
			return nil, nil
		}
		h.i++
		return h, nil
	}
}

// HashEncoder writes the field count of a hash and continues with the field
// and value strings.
type HashEncoder struct{}

func (h HashEncoder) Handle(e *Encoder, t Token) (TokenEncoder, error) {
	if t.k != KindUint {
		return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
	}
	if _, err := e.w.Write(t.AsBytes()); err != nil {
		return nil, err
	}
	return StringTokensEncoder{N: t.Uint64() * 2}, nil
}

// ZipPackEncoder writes a key and a single value, e.g. a zipmap.
type ZipPackEncoder struct{ step uint8 }

func (h ZipPackEncoder) Handle(e *Encoder, t Token) (TokenEncoder, error) {
	switch h.step {
	case 0:
		if t.k != KindString {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindString)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.step++
		return h, nil
	case 1:
		if t.k != KindZipMap {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindZipMap)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		return nil, nil
	}
	return nil, fmt.Errorf("unimplemented zip pack step: %d", h.step)
}

// ZipListEncoder writes a key and a single listpack.
type ZipListEncoder struct{ step uint8 }

func (h ZipListEncoder) Handle(e *Encoder, t Token) (TokenEncoder, error) {
	switch h.step {
	case 0:
		if t.k != KindString {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindString)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.step++
		return h, nil
	case 1:
		if t.k != KindZipList {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindZipList)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		return nil, nil
	}
	return nil, fmt.Errorf("unimplemented zip list step: %d", h.step)
}

// SetIntSetEncoder writes a key and a single set blob.
type SetIntSetEncoder struct{ step uint8 }

func (h SetIntSetEncoder) Handle(e *Encoder, t Token) (TokenEncoder, error) {
	switch h.step {
	case 0:
		if t.k != KindString {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindString)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.step++
		return h, nil
	case 1:
		if t.k != KindSetIntSet {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindSetIntSet)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		return nil, nil
	}
	return nil, fmt.Errorf("unimplemented set int set step: %d", h.step)
}

// ZsetListEncoder writes a key and a single sorted set blob.
type ZsetListEncoder struct{ step uint8 }

func (h ZsetListEncoder) Handle(e *Encoder, t Token) (TokenEncoder, error) {
	switch h.step {
	case 0:
		if t.k != KindString {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindString)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.step++
		return h, nil
	case 1:
		if t.k != KindZsetList {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindZsetList)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		return nil, nil
	}
	return nil, fmt.Errorf("unimplemented zset list step: %d", h.step)
}

// QuickListEncoder writes the nodes of a quicklist.
type QuickListEncoder struct {
	step uint8
	n, i uint64
}

func (h QuickListEncoder) Handle(e *Encoder, t Token) (TokenEncoder, error) {
	switch h.step {
	default:
		return nil, fmt.Errorf("unimplemented quick list step: %d", h.step)
	case 0:
		if t.k != KindUint {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.n = t.Uint64()
		h.step++
		return h, nil
	case 1:
		if t.k != KindQuickList {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindQuickList)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		if h.i >= (h.n - 1) {
			return nil, nil
		}
		h.i++
		return h, nil
	}
}

// QuickList2Encoder writes the nodes of a quicklist, where each node carries
// its container type.
type QuickList2Encoder struct {
	step uint8
	n, i uint64
}

func (h QuickList2Encoder) Handle(e *Encoder, t Token) (TokenEncoder, error) {
	switch h.step {
	default:
		return nil, fmt.Errorf("unimplemented quick list 2 step: %d", h.step)
	case 0:
		if t.k != KindUint {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.n = t.Uint64()
		h.step++
		return h, nil
	case 1:
		if t.k != KindQuickList {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindQuickList)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		if h.i >= (h.n - 1) {
			return nil, nil
		}
		h.i++
		return h, nil
	}
}

// StreamListEncoder writes a stream value of any of the StreamListpacks op
// codes, including its metadata, its consumer groups and their IDMP and NACK
// zone data.
type StreamListEncoder struct {
	step uint8
	n, i uint64
	sub  TokenEncoder
}

func (h StreamListEncoder) Handle(e *Encoder, t Token) (TokenEncoder, error) {
	switch h.step {
	default:
		return nil, fmt.Errorf("unimplemented stream list step: %d", h.step)
	case 0: // number of listpacks
		if t.k != KindUint {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.n = t.Uint64() * 2 // each entry is a stream ID and a listpack
		h.i = 0
		h.step = 1
		if h.n == 0 {
			h.step = 2
			h.n = streamMetaLen(*e.opCode)
			h.i = 0
		}
		return h, nil
	case 1: // listpack entries
		k := KindString
		if h.i&1 == 0 {
			k = KindStreamID
		}
		if t.k != k {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, k)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.i++
		if h.i >= h.n {
			h.step = 2
			h.n = streamMetaLen(*e.opCode)
			h.i = 0
		}
		return h, nil
	case 2: // stream metadata
		if t.k != KindUint {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.i++
		if h.i >= h.n {
			h.step = 3
		}
		return h, nil
	case 3: // number of consumer groups
		if t.k != KindUint {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.n = t.Uint64()
		h.i = 0
		if h.n == 0 {
			if *e.opCode >= opCodeStreamListpacks4 {
				h.step = 6
				h.sub = StreamIdmpEncoder{}
			} else {
				return nil, nil
			}
		} else {
			h.step = 4
			h.sub = StreamGroupEncoder{}
		}
		return h, nil
	case 4: // consumer groups
		next, err := h.sub.Handle(e, t)
		if err != nil {
			return nil, err
		}
		if next == nil {
			h.sub = nil
			h.i++
			if h.i >= h.n {
				if *e.opCode >= opCodeStreamListpacks4 {
					h.step = 6
					h.sub = StreamIdmpEncoder{}
				} else {
					return nil, nil
				}
			} else {
				h.sub = StreamGroupEncoder{}
			}
			return h, nil
		}
		h.sub = next
		return h, nil
	case 6: // IDMP (Idempotent Message Producer) data
		next, err := h.sub.Handle(e, t)
		if err != nil {
			return nil, err
		}
		if next == nil {
			return nil, nil
		}
		h.sub = next
		return h, nil
	}
}

// StreamGroupEncoder writes a stream consumer group: its pending entries, its
// consumers and its NACK zone.
type StreamGroupEncoder struct {
	step uint8
	n, i uint64
	sub  TokenEncoder
}

func (h StreamGroupEncoder) Handle(e *Encoder, t Token) (TokenEncoder, error) {
	switch h.step {
	default:
		return nil, fmt.Errorf("unimplemented stream group step: %d", h.step)
	case 0: // group name
		if t.k != KindString {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindString)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.step = 1
		return h, nil
	case 1: // last ID
		if t.k != KindUint {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.step = 2
		return h, nil
	case 2: // last ID sequence
		if t.k != KindUint {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		if *e.opCode >= opCodeStreamListpacks2 {
			h.step = 3
		} else {
			h.step = 4
		}
		return h, nil
	case 3: // entries read
		if t.k != KindUint {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.step = 4
		return h, nil
	case 4: // number of pending entries
		if t.k != KindUint {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.n = t.Uint64()
		h.i = 0
		if h.n == 0 {
			h.step = 8
		} else {
			h.step = 5
		}
		return h, nil
	case 5: // pending entry stream ID
		if t.k != KindStreamID {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindStreamID)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.step++
		return h, nil
	case 6: // pending entry delivery time
		if t.k != KindTime {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindTime)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.step++
		return h, nil
	case 7: // pending entry delivery count
		if t.k != KindUint {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.i++
		if h.i >= h.n {
			h.step = 8
		} else {
			h.step = 5
		}
		return h, nil
	case 8: // number of consumers
		if t.k != KindUint {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.n = t.Uint64()
		h.i = 0
		if h.n == 0 {
			if *e.opCode >= opCodeStreamListpacks5 {
				h.step = 10
			} else {
				return nil, nil
			}
		} else {
			h.step = 9
			h.sub = StreamConsumerEncoder{}
		}
		return h, nil
	case 9: // consumers
		next, err := h.sub.Handle(e, t)
		if err != nil {
			return nil, err
		}
		if next == nil {
			h.sub = nil
			h.i++
			if h.i >= h.n {
				if *e.opCode >= opCodeStreamListpacks5 {
					h.step = 10
				} else {
					return nil, nil
				}
			} else {
				h.sub = StreamConsumerEncoder{}
			}
			return h, nil
		}
		h.sub = next
		return h, nil
	case 10: // NACK zone entry count
		if t.k != KindUint {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.n = t.Uint64()
		h.i = 0
		if h.n == 0 {
			return nil, nil
		}
		h.step = 11
		return h, nil
	case 11: // NACK zone entry stream ID
		if t.k != KindStreamID {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindStreamID)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.i++
		if h.i >= h.n {
			return nil, nil
		}
		return h, nil
	}
}

// StreamConsumerEncoder writes a stream consumer: the times it was seen and
// active and its pending entries.
type StreamConsumerEncoder struct {
	step uint8
	n, i uint64
}

func (h StreamConsumerEncoder) Handle(e *Encoder, t Token) (TokenEncoder, error) {
	switch h.step {
	default:
		return nil, fmt.Errorf("unimplemented stream consumer step: %d", h.step)
	case 0: // consumer name
		if t.k != KindString {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindString)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		if *e.opCode >= opCodeStreamListpacks3 {
			h.step = 1
		} else {
			h.step = 2
		}
		return h, nil
	case 1: // seen time
		if t.k != KindTime {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindTime)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.step = 2
		return h, nil
	case 2: // active time
		if t.k != KindTime {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindTime)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.step = 3
		return h, nil
	case 3: // number of pending entries
		if t.k != KindUint {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.n = t.Uint64()
		h.i = 0
		if h.n == 0 {
			return nil, nil
		}
		h.step = 4
		return h, nil
	case 4: // pending entry stream ID
		if t.k != KindStreamID {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindStreamID)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.i++
		if h.i >= h.n {
			return nil, nil
		}
		return h, nil
	}
}

// StreamIdmpEncoder writes the IDMP (Idempotent Message Producer) data of a
// stream: its configuration, its producers with their entries and the stats.
type StreamIdmpEncoder struct {
	step  uint8
	p, pi uint64 // producers
	n, i  uint64 // entries of the current producer
}

func (h StreamIdmpEncoder) Handle(e *Encoder, t Token) (TokenEncoder, error) {
	switch h.step {
	default:
		return nil, fmt.Errorf("unimplemented stream IDMP step: %d", h.step)
	case 0: // duration
		if t.k != KindUint {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.step = 1
		return h, nil
	case 1: // maximum entries
		if t.k != KindUint {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.step = 2
		return h, nil
	case 2: // number of producers
		if t.k != KindUint {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.p = t.Uint64()
		h.pi = 0
		if h.p == 0 {
			h.step = 8
		} else {
			h.step = 3
		}
		return h, nil
	case 3: // producer ID
		if t.k != KindString {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindString)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.step = 4
		return h, nil
	case 4: // number of entries of the current producer
		if t.k != KindUint {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.n = t.Uint64()
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
		return h, nil
	case 5: // entry IID
		if t.k != KindString {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindString)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.step = 6
		return h, nil
	case 6: // entry stream ID
		if t.k != KindUint {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.step = 7
		return h, nil
	case 7: // entry stream ID sequence
		if t.k != KindUint {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
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
		return h, nil
	case 8: // all-time count of added IIDs
		if t.k != KindUint {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.step = 9
		return h, nil
	case 9: // all-time count of duplicate IIDs
		if t.k != KindUint {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		return nil, nil
	}
}

// KeyMetaEncoder writes key metadata: the number of metadata classes and for
// each class its specification and module values.
type KeyMetaEncoder struct {
	step    uint8
	classes uint64
}

func (h KeyMetaEncoder) Handle(e *Encoder, t Token) (TokenEncoder, error) {
	switch h.step {
	default:
		return nil, fmt.Errorf("unimplemented key meta step: %d", h.step)
	case 0:
		if t.k != KindUint {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.classes = t.Uint64()
		if h.classes == 0 {
			return nil, nil
		}
		h.step = 1
		return h, nil
	case 1:
		if t.k != KindByte {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindByte)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.step = 2
		return h, nil
	case 2:
		eof, err := writeModuleValue(e, t)
		if err != nil {
			return nil, err
		}
		if !eof {
			return h, nil
		}
		h.classes--
		if h.classes == 0 {
			return nil, nil
		}
		h.step = 1
		return h, nil
	}
}

// writeModuleValue writes a single module value and reports whether it was
// the EOF module op code.
func writeModuleValue(e *Encoder, t Token) (bool, error) {
	if t.k == KindOpCode {
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return true, err
		}
		return true, nil
	}
	if t.format != formatModule {
		return false, fmt.Errorf("unexpected format %d, expected %d", t.format, formatModule)
	}
	if _, err := e.w.Write(t.AsBytes()); err != nil {
		return false, err
	}
	return false, nil
}

// Module2Encoder writes a module value: the key name, the module ID and the
// module values. Module auxiliary values omit the key name. They start from
// the module ID, so construct them with Step 1.
type Module2Encoder struct {
	// Step is 0 for a module value and 1 for a module auxiliary value.
	Step uint8
}

func (h Module2Encoder) Handle(e *Encoder, t Token) (TokenEncoder, error) {
	switch h.Step {
	case 0:
		if t.k != KindString {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindString)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.Step++
		return h, nil
	case 1:
		if t.k != KindString || t.format != formatModuleID {
			return nil, fmt.Errorf("unexpected token %s, expected %d format", t.k, formatModuleID)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.Step++
		return h, nil
	case 2:
		eof, err := writeModuleValue(e, t)
		if err != nil {
			return nil, err
		}
		if eof {
			return nil, nil
		}
		return h, nil
	}
	return nil, fmt.Errorf("unimplemented module 2 step: %d", h.Step)
}

// HashMetadataEncoder writes a hash with per field expiry: the minimum expiry
// of the fields, the field count and the fields with their expiry. The
// pre-release format has no minimum expiry, so construct it with Step 1.
type HashMetadataEncoder struct {
	// Step is 0 for the released format and 1 for the pre-release format
	// without the minimum expiry.
	Step uint8
	n    uint64
	sub  TokenEncoder
}

func (h HashMetadataEncoder) Handle(e *Encoder, t Token) (TokenEncoder, error) {
	switch h.Step {
	default:
		return nil, fmt.Errorf("unimplemented hash metadata step: %d", h.Step)
	case 0: // minimum expiry of the fields
		if t.k != KindExpiry {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindExpiry)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.Step = 1
		return h, nil
	case 1: // number of fields
		if t.k != KindUint {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.n = t.Uint64()
		if h.n == 0 {
			return nil, nil
		}
		h.Step = 2
		return h, nil
	case 2: // field expiry
		if t.k != KindUint {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.sub = StringTokensEncoder{N: 2}
		h.Step = 3
		return h, nil
	case 3: // field and value
		next, err := h.sub.Handle(e, t)
		if err != nil {
			return nil, err
		}
		if next == nil {
			h.sub = nil
			h.n--
			if h.n == 0 {
				return nil, nil
			}
			h.Step = 2
			return h, nil
		}
		h.sub = next
		return h, nil
	}
}

// HashListpackExEncoder writes the minimum expiry of the fields and the
// entries of a hash with per field expiry stored as a listpack.
type HashListpackExEncoder struct{ step uint8 }

func (h HashListpackExEncoder) Handle(e *Encoder, t Token) (TokenEncoder, error) {
	switch h.step {
	case 0:
		if t.k != KindExpiry {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindExpiry)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.step++
		return h, nil
	case 1:
		if t.k != KindZipList {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindZipList)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		return nil, nil
	}
	return nil, fmt.Errorf("unimplemented hash listpack ex step: %d", h.step)
}

// HashTemplateEncoder writes a hash template record and registers its field
// count, so hash values can reference it by ID.
type HashTemplateEncoder struct {
	step uint8
	id   uint64
	n, i uint64
}

func (h HashTemplateEncoder) Handle(e *Encoder, t Token) (TokenEncoder, error) {
	switch h.step {
	case 0:
		if t.k != KindUint {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.id = t.Uint64()
		h.step++
		return h, nil
	case 1:
		if t.k != KindUint {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.n = t.Uint64()
		if h.n == 0 {
			return nil, fmt.Errorf("rdb: hash template %d has zero fields", h.id)
		}
		if e.templates == nil {
			e.templates = make(map[uint64]uint64)
		}
		e.templates[h.id] = h.n
		h.step++
		return h, nil
	case 2:
		if t.k != KindString {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindString)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		if h.i >= (h.n - 1) {
			return nil, nil
		}
		h.i++
		return h, nil
	}
	return nil, fmt.Errorf("unimplemented hash template step: %d", h.step)
}

// HashTmplArrayRefEncoder writes the template ID of an array referenced hash
// and continues with as many value strings as the template has fields.
type HashTmplArrayRefEncoder struct{}

func (h HashTmplArrayRefEncoder) Handle(e *Encoder, t Token) (TokenEncoder, error) {
	if t.k != KindUint {
		return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
	}
	if _, err := e.w.Write(t.AsBytes()); err != nil {
		return nil, err
	}
	n, ok := e.templates[t.Uint64()]
	if !ok {
		return nil, fmt.Errorf("rdb: unknown hash template id")
	}
	return StringTokensEncoder{N: n}, nil
}

// HashTmplSelfEncoder writes a self-contained template encoded hash, which
// carries its field names inline. LP selects the listpack value encoding.
type HashTmplSelfEncoder struct {
	LP   bool
	step uint8
	n, i uint64
}

func (h HashTmplSelfEncoder) Handle(e *Encoder, t Token) (TokenEncoder, error) {
	switch h.step {
	case 0:
		if t.k != KindUint {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		switch t.Uint64() {
		case 0:
			h.step = 1
		case 1:
			h.step = 2
		default:
			return nil, fmt.Errorf("rdb: unknown hash template fields format")
		}
		return h, nil
	case 1:
		if t.k != KindZipList {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindZipList)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		n, err := lpCountEntries(t.AsBytes())
		if err != nil {
			return nil, err
		}
		h.n = n
		h.i = 0
		if h.LP {
			h.step = 4
		} else {
			h.step = 5
		}
		return h, nil
	case 2:
		if t.k != KindUint {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.n = t.Uint64()
		h.step = 3
		return h, nil
	case 3:
		if t.k != KindString {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindString)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
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
		return h, nil
	case 4:
		if t.k != KindZipList {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindZipList)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		return nil, nil
	case 5:
		if t.k != KindString {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindString)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		if h.i >= (h.n - 1) {
			return nil, nil
		}
		h.i++
		return h, nil
	}
	return nil, fmt.Errorf("unimplemented hash template self step: %d", h.step)
}

// ArrayEncoder writes a sparse array: the element count, the optional insert
// index and the tagged elements.
type ArrayEncoder struct {
	step uint8
	n, i uint64
	tag  uint64
}

func (h ArrayEncoder) Handle(e *Encoder, t Token) (TokenEncoder, error) {
	switch h.step {
	default:
		return nil, fmt.Errorf("unimplemented array step: %d", h.step)
	case 0:
		if t.k != KindUint {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.n = t.Uint64()
		h.step = 1
		return h, nil
	case 1:
		if t.k != KindUint {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		if t.Uint64() == 1 {
			h.step = 2
		} else {
			h.step = 3
		}
		return h, nil
	case 2:
		if t.k != KindUint {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.step = 3
		return h, nil
	case 3:
		if t.k != KindUint {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.step = 4
		return h, nil
	case 4:
		if t.k != KindUint {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.tag = t.Uint64()
		if h.tag > 3 {
			return nil, fmt.Errorf("rdb: unknown array element type tag %d", h.tag)
		}
		h.step = 5
		return h, nil
	case 5:
		k := KindString
		switch h.tag {
		case 1:
			k = KindInt
		case 2:
			k = KindFloat64
		}
		if t.k != k {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, k)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.i++
		if h.i >= h.n {
			return nil, nil
		}
		h.step = 3
		return h, nil
	}
}

// CronEncoder writes a KeyDB cron job: the script, the start time, the
// interval and the key and value arguments.
type CronEncoder struct {
	step uint8
	n, i uint64
}

func (h CronEncoder) Handle(e *Encoder, t Token) (TokenEncoder, error) {
	switch h.step {
	default:
		return nil, fmt.Errorf("unimplemented cron step: %d", h.step)
	case 0:
		if t.k != KindString {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindString)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.step = 1
		return h, nil
	case 1, 2:
		if t.k != KindTime {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindTime)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.step++
		return h, nil
	case 3:
		if t.k != KindUint {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.n = t.Uint64()
		h.i = 0
		if h.n == 0 {
			h.step = 5
		} else {
			h.step = 4
		}
		return h, nil
	case 4:
		if t.k != KindString {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindString)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.i++
		if h.i >= h.n {
			h.step = 5
		}
		return h, nil
	case 5:
		if t.k != KindUint {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.n = t.Uint64()
		h.i = 0
		if h.n == 0 {
			return nil, nil
		}
		h.step = 6
		return h, nil
	case 6:
		if t.k != KindString {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindString)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.i++
		if h.i >= h.n {
			return nil, nil
		}
		return h, nil
	}
}

// Hash2Encoder writes a Valkey hash with per field expiry: the number of
// entries and for each entry the field, the value and the expiry. Valkey
// stores it under the value type Redis uses for HashMetadataPreGa.
type Hash2Encoder struct {
	step uint8
	n, i uint64
}

func (h Hash2Encoder) Handle(e *Encoder, t Token) (TokenEncoder, error) {
	switch h.step {
	default:
		return nil, fmt.Errorf("unimplemented hash2 step: %d", h.step)
	case 0:
		if t.k != KindUint {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.n = t.Uint64()
		h.i = 0
		if h.n == 0 {
			return nil, nil
		}
		h.step = 1
		return h, nil
	case 1: // field
		if t.k != KindString {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindString)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.step = 2
		return h, nil
	case 2: // value
		if t.k != KindString {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindString)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.step = 3
		return h, nil
	case 3: // expiry
		if t.k != KindTime {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindTime)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.i++
		if h.i >= h.n {
			return nil, nil
		}
		h.step = 1
		return h, nil
	}
}

// SlotImportEncoder writes the slot import state: the job name, the number
// of slot ranges and the ranges themselves. Valkey stores it under the op
// code Redis uses for KeyMeta.
type SlotImportEncoder struct {
	step uint8
	n, i uint64
}

func (h SlotImportEncoder) Handle(e *Encoder, t Token) (TokenEncoder, error) {
	switch h.step {
	default:
		return nil, fmt.Errorf("unimplemented slot import step: %d", h.step)
	case 0: // job name
		if t.k != KindString {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindString)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.step = 1
		return h, nil
	case 1: // number of slot ranges
		if t.k != KindUint {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.n = t.Uint64()
		h.i = 0
		if h.n == 0 {
			return nil, nil
		}
		h.step = 2
		return h, nil
	case 2: // start slot
		if t.k != KindUint {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.step = 3
		return h, nil
	case 3: // end slot
		if t.k != KindUint {
			return nil, fmt.Errorf("unexpected token %s, expected %s", t.k, KindUint)
		}
		if _, err := e.w.Write(t.AsBytes()); err != nil {
			return nil, err
		}
		h.i++
		if h.i >= h.n {
			return nil, nil
		}
		h.step = 2
		return h, nil
	}
}
