package rdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coding-socks/rdb/commands"
	"github.com/coding-socks/rdb/internal/lzf"
)

const (
	kB = 1024

	// DefaultBufferLen is the initial size of the buffer
	// each Decoder allocates.
	DefaultBufferLen = 8 * kB
)

var (
	// ErrMissingMagicString indicates that a file could not be decoded
	// because it does not start with a magic string followed by a
	// version number.
	ErrMissingMagicString = errors.New("rdb: missing magic string")
	// ErrInvalidVersion indicates that a version is not a version of
	// the RDB format: the digits of a header do not parse as a version
	// number, or an encoder is given a version its dialect does not
	// support.
	ErrInvalidVersion = errors.New("rdb: invalid version")
	// ErrUnsupportedVersion indicates that a file could not be decoded
	// because no dialect in the registry accepts its header.
	// See [RegisterDialect] to add support for a fork of Redis.
	ErrUnsupportedVersion = errors.New("rdb: unsupported version")
	// ErrInvalidChecksum indicates that a file could not be decoded
	// because the checksum at its end does not match its content.
	// An all-zero checksum passes the validation, the way Redis
	// disables it.
	ErrInvalidChecksum = errors.New("rdb: invalid checksum")
	// ErrUnexpectedToken indicates that a token could not be encoded
	// because it cannot appear at the current position of the stream:
	// every record starts with an op code.
	ErrUnexpectedToken = errors.New("rdb: unexpected token")
	// ErrInvalidOpCode indicates that a token could not be encoded or
	// decoded because the dialect of the file assigns no meaning to
	// its op code.
	ErrInvalidOpCode = errors.New("rdb: invalid opcode")
	// ErrOpCodeTooNew indicates that a record could not be encoded or
	// decoded because its op code was introduced in a version of the
	// RDB format newer than the version of the dialect.
	ErrOpCodeTooNew = errors.New("rdb: op code newer than the file version")
	// ErrCorruptPayload indicates that a value could not be decoded
	// because a packed encoding inside it is malformed.
	ErrCorruptPayload = errors.New("rdb: corrupt payload")
)

// SlotRange is the inclusive range of the hash slots a node of a
// cluster serves.
type SlotRange struct {
	// Start is the first slot of the range.
	Start uint16
	// End is the last slot of the range, the range of a single
	// slot carries the slot in Start and End.
	End uint16
}

// Contains reports whether the range serves the slot.
func (r SlotRange) Contains(slot uint16) bool {
	return slot >= r.Start && slot <= r.End
}

const (
	// enc8bit indicates an 8-bit signed integer.
	enc8bit uint8 = iota
	// enc16bit indicates a 16-bit signed integer.
	enc16bit
	// enc32bit indicates a 32-bit signed integer.
	enc32bit
	// encLZF indicates a string compressed with FASTLZ.
	encLZF
)

const (
	// len6bit indicates an 8 bit length.
	len6bit uint8 = iota
	// len14bit indicates a 16 bit length.
	len14bit
	// len32plusBit indicates a 32 bit length. See len32bit and len64bit
	len32plusBit
	// lenSpecial indicates a special handling.
	lenSpecial

	len32bit = 0x80
	len64bit = 0x81
)

// OpCode is one byte of an RDB file that dispatches the record it
// starts. Its meaning depends on the [Dialect] of the file.
type OpCode byte

// Kind tells which of the accessors of a Token carry its value:
//
//   - [Token.Uint64] for the counts and lengths of a record
//   - [Token.Int64] for the numeric strings of a record
//   - [Token.Float64] for the scores of a sorted set
//   - [Token.String] for the strings of a record
//   - [Token.OpCode] for the op code of a record
//   - [Token.Time] for the expiry timestamps and pending entry times
//   - [Token.StreamID] for the stream IDs of a stream
//   - [Token.Bytes] for the raw bytes of a packed value
//
// A Kind without an accessor carries the raw bytes only. Every kind but
// [KindUnknown], the zero value, has a constructor.
type Kind uint16

const (
	KindUnknown Kind = iota
	KindExpiry
	KindOpCode
	KindByte
	KindUint
	KindInt
	KindFloat32
	KindFloat64
	KindString

	KindQuickList
	KindZipMap
	KindZipList
	KindSetIntSet
	KindZsetList

	KindStreamID
	KindTime
)

// String returns the name of the kind.
func (k Kind) String() string {
	kk := k
	switch kk {
	case KindExpiry:
		return "Expiry"
	case KindOpCode:
		return "OpCode"
	case KindByte:
		return "Byte"
	case KindUint:
		return "Uint"
	case KindInt:
		return "Int"
	case KindFloat32:
		return "Float32"
	case KindFloat64:
		return "Float64"
	case KindString:
		return "String"
	case KindQuickList:
		return "QuickList"
	case KindZipMap:
		return "ZipMap"
	case KindZipList:
		return "ZipList"
	case KindSetIntSet:
		return "SetIntSet"
	case KindZsetList:
		return "ZsetZipList"
	case KindStreamID:
		return "StreamID"
	case KindTime:
		return "Time"
	default:
		return "Unknown"
	}
}

// The op codes of a module value.
const (
	ModuleOpCodeEOF = iota
	ModuleOpCodeSInt
	ModuleOpCodeUInt
	ModuleOpCodeFloat
	ModuleOpCodeDouble
	ModuleOpCodeString
)

var kindModuleMap = map[int]Kind{
	ModuleOpCodeSInt:   KindInt,
	ModuleOpCodeUInt:   KindUint,
	ModuleOpCodeFloat:  KindFloat32,
	ModuleOpCodeDouble: KindFloat64,
	ModuleOpCodeString: KindString,
}

const (
	formatModuleID format = iota - 2
	formatModule
	formatDefault
)

// format represents the different formats of a kind. As an example the zset
// and zset2 op codes use different formats for storing a float.
type format int8

// Token is the unit of data a [Decoder] reads and an [Encoder] writes.
//
// A Token stores its value in one of two forms:
//
//   - As raw bytes: the verbatim encoding of the value, produced by
//     [Decoder.ReadToken] and by the [Blob] and [Packed] constructors.
//     A token read from a stream is backed by the internal buffer of
//     the [Decoder] and is only valid until the next call to any method
//     on that Decoder; call [Token.Clone] to copy the raw bytes into an
//     independent allocation that persists beyond subsequent [Decoder]
//     calls.
//
//   - As a typed Go value: a self-contained representation produced by
//     the other constructors (e.g., [Str], [Uint], [Float64]).
//     Such tokens are valid indefinitely and do not need to be cloned.
//
// Both forms provide the same view: the value methods return the value
// wherever it is stored, and [Token.Bytes] returns the wire format.
type Token struct {
	// Tokens can exist in either a "raw" or an "exact" form.
	// Tokens produced by the Decoder are in the "raw" form.
	// Tokens returned by constructors are usually in the "exact" form,
	// [Blob] and [Packed] hold the raw bytes to write and are in the
	// "raw" form.
	// The Encoder accepts Tokens in either the "raw" or "exact" form.
	//
	// The following chart shows the possible values for each Token type:
	//	╔══════════════════╦════════════╤════════════╤════════════╗
	//	║ Token type       ║ raw field  │ num field  │ str field  ║
	//	╠══════════════════╬════════════╪════════════╪════════════╣
	//	║ opcode   (raw)   ║ non-empty  │ 0          │ nil        ║
	//	║ opcode   (exact) ║ nil        │ 0          │ nil        ║
	//	║ expiry   (raw)   ║ non-empty  │ 0          │ nil        ║
	//	║ expiry   (exact) ║ nil        │ non-zero   │ nil        ║
	//	║ byte     (raw)   ║ non-empty  │ 0          │ nil        ║
	//	║ byte     (exact) ║ nil        │ 0          │ non-empty  ║
	//	║ uint     (raw)   ║ non-empty  │ 0          │ nil        ║
	//	║ uint     (exact) ║ nil        │ non-zero   │ nil        ║
	//	║ int      (raw)   ║ non-empty  │ 0          │ nil        ║
	//	║ int      (exact) ║ nil        │ non-zero   │ nil        ║
	//	║ float32  (raw)   ║ non-empty  │ 0          │ nil        ║
	//	║ float32  (exact) ║ nil        │ non-zero   │ nil        ║
	//	║ float64  (raw)   ║ non-empty  │ 0          │ nil        ║
	//	║ float64  (exact) ║ nil        │ non-zero   │ nil        ║
	//	║ string   (raw)   ║ non-empty  │ 0          │ nil        ║
	//	║ string   (exact) ║ nil        │ 0          │ non-empty  ║
	//	║ streamID (raw)   ║ non-empty  │ 0          │ nil        ║
	//	║ streamID (exact) ║ nil        │ 0          │ non-empty  ║
	//	║ time     (raw)   ║ non-empty  │ 0          │ nil        ║
	//	║ time     (exact) ║ nil        │ non-zero   │ nil        ║
	//	╚══════════════════╩════════════╧════════════╧════════════╝
	//
	// Notes:
	//   - For tokens stored in "raw" form, the raw field holds everything
	//     the value occupies in the file, the length prefix of a string
	//     included. The value methods decode the value from it on demand.
	//   - The packed kinds (i.e., KindQuickList, KindZipMap, KindZipList,
	//     KindSetIntSet, and KindZsetList) are always in the "raw" form.
	//   - The opCode field holds the op code of a KindOpCode token in the
	//     "exact" form. For KindExpiry and KindFloat64 tokens, it selects
	//     the encoding the value is written with.
	//   - The format field is formatModuleID for a KindString token whose
	//     string is a module ID and formatModule for a module value.

	// k is the kind of the token.
	k Kind

	// format selects the encoding of a KindString token: formatModuleID
	// for a module ID and formatModule for a module value.
	format format

	// opCode is the op code of a KindOpCode token in the "exact" form.
	// For KindExpiry and KindFloat64 tokens, it selects the encoding
	// the value is written with.
	opCode OpCode

	// raw is the verbatim encoding of the value read from a stream.
	// If non-nil, then its value takes precedence over num and str.
	raw []byte

	// num is a uint64, int64, float32, or float64 stored as a uint64
	// value. For floating-point values, it stores the raw IEEE-754
	// bit-pattern. It is non-zero for any number or timestamp in the
	// "exact" form.
	num uint64

	// str holds the content of a KindString token, the bytes of a
	// KindByte token, and the 16 ID bytes of a KindStreamID token.
	str []byte
}

// Clone returns a copy of the token with raw bytes that are not backed by
// the [Decoder] buffer and therefore remain valid past subsequent
// [Decoder] calls. It has no effect on tokens produced by the constructors,
// since those are already self-contained: such a token is returned
// unchanged.
func (t Token) Clone() Token {
	if t.raw == nil {
		return t
	}
	t.raw = bytes.Clone(t.raw)
	return t
}

// Kind returns the kind of the token, see [Kind] for the accessors
// that carry its value.
func (t Token) Kind() Kind {
	return t.k
}

// OpCode returns the op code of a record token.
// It panics if the token kind is not [KindOpCode].
func (t Token) OpCode() OpCode {
	if t.Kind() != KindOpCode {
		panic("rdb: unimplemented")
	}
	if t.raw == nil {
		return t.opCode
	}
	return OpCode(t.raw[0])
}

// Time returns the timestamp of an expiry or a pending entry time,
// in the timezone of the local machine like time.Unix does.
// It panics if the token kind is neither [KindExpiry] nor [KindTime].
func (t Token) Time() time.Time {
	switch t.Kind() {
	case KindExpiry:
		if t.raw == nil {
			if t.opCode == RedisOpCodeExpirySeconds {
				return time.Unix(int64(t.num), 0)
			}
			return time.UnixMilli(int64(t.num))
		}
		if len(t.raw) == 4 {
			return time.Unix(int64(int32(binary.LittleEndian.Uint32(t.raw))), 0)
		}
		if len(t.raw) == 8 {
			return time.UnixMilli(int64(binary.LittleEndian.Uint64(t.raw)))
		}
	case KindTime:
		if t.raw == nil {
			return time.UnixMilli(int64(t.num))
		}
		if len(t.raw) == 8 {
			return time.UnixMilli(int64(binary.LittleEndian.Uint64(t.raw)))
		}
	}
	panic("rdb: unknown time format")
}

// Uint64 returns the unsigned integer value of a count or a length.
// It panics if the token kind is not [KindUint].
func (t Token) Uint64() uint64 {
	if t.Kind() != KindUint {
		panic("rdb: unknown uint64 format")
	}
	if t.raw == nil {
		return t.num
	}
	return decodeUint64(t.raw)
}

// String returns the string value of the token.
// It panics if the token kind is not [KindString].
func (t Token) String() string {
	if t.Kind() != KindString {
		panic("rdb: unknown string format")
	}
	if t.raw == nil {
		return string(t.str)
	}
	return decodeString(t.raw, t.format)
}

// Int64 returns the integer value of a numeric string, the way Redis
// stores the members of an intset and the metadata counters.
// It panics if the token does not carry a numeric string.
func (t Token) Int64() int64 {
	if t.Kind() != KindString || t.format != formatDefault {
		panic("rdb: unknown int64 format")
	}
	if t.raw == nil {
		return int64(t.num)
	}
	return decodeStringAsInt64(t.raw)
}

// Float64 returns the score of a sorted set member.
// It panics if the token kind is not [KindFloat64].
func (t Token) Float64() float64 {
	if t.Kind() != KindFloat64 {
		panic("rdb: unknown float64 format")
	}
	if t.raw == nil {
		return math.Float64frombits(t.num)
	}
	return decodeFloat64(t.raw, t.opCode)
}

// StreamID returns the milliseconds time and the sequence number of a
// stream entry ID.
// It panics if the token kind is not [KindStreamID].
func (t Token) StreamID() (uint64, uint64) {
	if t.Kind() != KindStreamID {
		panic("rdb: unknown float64 format")
	}
	if t.raw == nil {
		return binary.BigEndian.Uint64(t.str[:8]), binary.BigEndian.Uint64(t.str[8:])
	}
	if len(t.raw) == 17 {
		s := []byte(decodeString(t.raw, t.format))
		return binary.BigEndian.Uint64(s[:8]), binary.BigEndian.Uint64(s[8:])
	} else {
		ms, _, n := decodeLen(t.raw)
		seq, _, _ := decodeLen(t.raw[n:])
		return ms, seq
	}
}

// Bytes returns the raw bytes of the token. If the token carries no raw
// bytes, e.g. it was not read from a stream, the wire format is built from
// the available values. The bytes of a token read from a stream are only
// valid until the next [Decoder] call, see [Token.Clone].
func (t Token) Bytes() []byte {
	if t.raw != nil {
		return t.raw
	}
	switch t.k {
	default:
		panic("rdb: unknown token kind: " + t.k.String())
	case KindOpCode:
		return []byte{byte(t.opCode)}
	case KindByte:
		return t.str
	case KindUint:
		return encodeLenBytes(t.num)
	case KindInt:
		return encodeLenBytes(uint64(int64(t.num)))
	case KindFloat32:
		b := make([]byte, 4)
		binary.LittleEndian.PutUint32(b, uint32(t.num))
		return b
	case KindFloat64:
		if t.opCode == RedisOpCodeZset {
			return encodeZsetFloat64Bytes(math.Float64frombits(t.num))
		}
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, t.num)
		return b
	case KindString:
		if t.format == formatModuleID {
			id, ok := moduleIDBytes(string(t.str))
			if !ok {
				panic("rdb: invalid module ID: " + string(t.str))
			}
			return encodeLenBytes(id)
		}
		return append(encodeLenBytes(uint64(len(t.str))), t.str...)
	case KindExpiry:
		if t.opCode == RedisOpCodeExpirySeconds {
			b := make([]byte, 4)
			binary.LittleEndian.PutUint32(b, uint32(t.num))
			return b
		}
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, t.num)
		return b
	case KindTime:
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, t.num)
		return b
	case KindStreamID:
		return append([]byte{0x10}, t.str...)
	}
}

// encodeLenBytes encodes a value in its canonical length encoding, using the
// shortest possible form.
func encodeLenBytes(v uint64) []byte {
	switch {
	case v <= 63:
		return []byte{byte(v)}
	case v <= 16383:
		return []byte{len14bit<<6 | byte(v>>8), byte(v)}
	case v <= math.MaxUint32:
		b := make([]byte, 5)
		b[0] = len32bit
		binary.BigEndian.PutUint32(b[1:], uint32(v))
		return b
	default:
		b := make([]byte, 9)
		b[0] = len64bit
		binary.BigEndian.PutUint64(b[1:], v)
		return b
	}
}

// encodeZsetFloat64Bytes encodes a sorted set score in the text format of
// the zset op code, see decodeFloat64.
func encodeZsetFloat64Bytes(v float64) []byte {
	switch {
	case math.IsNaN(v):
		return []byte{0xfd}
	case math.IsInf(v, 1):
		return []byte{0xfe}
	case math.IsInf(v, -1):
		return []byte{0xff}
	}
	s := strconv.FormatFloat(v, 'g', 17, 64)
	return append([]byte{byte(len(s))}, s...)
}

// moduleIDBytes encodes a 9 character module ID as the wire value, the
// inverse of the module ID decoding, see decodeString.
func moduleIDBytes(name string) (uint64, bool) {
	if len(name) != 9 {
		return 0, false
	}
	var id uint64
	for i := 0; i < len(name); i++ {
		idx := strings.IndexByte(moduleIDCharset, name[i])
		if idx < 0 {
			return 0, false
		}
		id |= uint64(idx) << (6 * (len(name) - 1 - i))
	}
	return id << 10, true
}

func decodeUint64(raw []byte) uint64 {
	switch raw[0] >> 6 {
	case len6bit:
		return uint64(raw[0] & 0x3f)
	case len14bit:
		return uint64(binary.BigEndian.Uint16([]byte{raw[0] & 0x3f, raw[1]}))
	case len32plusBit:
		switch raw[0] {
		case len32bit:
			return uint64(binary.BigEndian.Uint32(raw[1:5]))
		case len64bit:
			return binary.BigEndian.Uint64(raw[1:9])
		}
	case lenSpecial:
		return uint64(raw[0] & 0x3f)
	}
	panic("rdb: unimplemented uint64 format")
}

func decodeLen(raw []byte) (l uint64, encoded bool, n int) {
	switch raw[0] >> 6 {
	case len6bit:
		return uint64(raw[0] & 0x3f), false, 1
	case len14bit:
		return uint64(binary.BigEndian.Uint16([]byte{raw[0] & 0x3f, raw[1]})), false, 2
	case len32plusBit:
		switch raw[0] {
		case len32bit:
			return uint64(binary.BigEndian.Uint32(raw[1:5])), false, 5
		case len64bit:
			return binary.BigEndian.Uint64(raw[1:9]), false, 9
		}
	case lenSpecial:
		return uint64(raw[0] & 0x3f), true, 1
	}
	panic("rdb: unimplemented len format")
}

const moduleIDCharset = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"

func decodeString(raw []byte, form format) string {
	switch form {
	default:
		panic("rdb: unimplemented string format")
	case formatModuleID:
		u := decodeUint64(raw)
		b := make([]byte, 9)
		u >>= 10
		for i := len(b); i > 0; i-- {
			b[i-1] = moduleIDCharset[u&0x3f]
			u >>= 6
		}
		return string(b)
	case formatDefault:
		l, encoded, n := decodeLen(raw)
		if encoded {
			switch uint8(l) {
			default:
				panic("rdb: unimplemented string format")
			case enc8bit:
				return strconv.FormatInt(int64(int8(raw[len(raw)-1])), 10)
			case enc16bit:
				return strconv.FormatInt(int64(int16(binary.LittleEndian.Uint16(raw[len(raw)-2:]))), 10)
			case enc32bit:
				return strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(raw[len(raw)-4:]))), 10)
			case encLZF:
				start := n
				_, _, m := decodeLen(raw[start:])
				start += m
				l, _, m := decodeLen(raw[start:])
				decompressed := make([]byte, l)
				start += m
				if _, err := lzf.Decompress(raw[start:], decompressed); err != nil {
					panic("rdb: unable to decompress lzf: " + err.Error())
				}
				return string(decompressed)
			}
		}
		return string(raw[len(raw)-int(l):])
	}
}

func decodeStringAsInt64(raw []byte) int64 {
	l, encoded, _ := decodeLen(raw)
	if encoded {
		switch uint8(l) {
		case enc8bit:
			return int64(int8(raw[len(raw)-1]))
		case enc16bit:
			return int64(int16(binary.LittleEndian.Uint16(raw[len(raw)-2:])))
		case enc32bit:
			return int64(int32(binary.LittleEndian.Uint32(raw[len(raw)-4:])))
		}
	}
	panic("rdb: unknown numeric string format")
}

func decodeFloat64(raw []byte, code OpCode) float64 {
	switch code {
	default:
		panic("rdb: unknown float format")
	case RedisOpCodeZset:
		switch raw[0] {
		case 0xff:
			return math.Inf(-1)
		case 0xfe:
			return math.Inf(1)
		case 0xfd:
			return math.NaN()
		}
		f, err := strconv.ParseFloat(string(raw[1:]), 64)
		if err != nil {
			panic("rdb: unable to decode float: " + err.Error())
		}
		return f
	case RedisOpCodeZset2:
		return math.Float64frombits(binary.LittleEndian.Uint64(raw))
	}
}

// Dialect abstracts the differences between implementations of the RDB
// format. Every dialect owns its op code dispatch, the token handlers they
// return are shared. Decoders detect the dialect of a file from its header
// using the registry, Encoders write the header of the dialect they are
// constructed with.
//
// The dialects are version aware: a dialect value represents the format of
// one version, every op code was introduced in a version of the format,
// files of an earlier version cannot contain it.
type Dialect interface {
	// Header returns the magic string and the version the dialect
	// represents. The header is padded to nine bytes.
	Header() (magic string, version int)
	// OpCodeString returns the name of the op code in this dialect.
	OpCodeString(op OpCode) string
	// WithVersion returns the dialect bound to the version. A version
	// the dialect does not support falls back to the newest one it
	// supports.
	WithVersion(version int) Dialect
	// Commands returns the command table of the dialect.
	Commands() commands.Table
	// Decoder returns the token decoder of the op code. It fails with
	// [ErrInvalidOpCode] when the dialect assigns no meaning to the op
	// code and with [ErrOpCodeTooNew] when the op code was introduced
	// in a version newer than the one of the dialect.
	Decoder(op OpCode) (TokenDecoder, error)
	// Encoder is the write side of [Dialect.Decoder].
	Encoder(op OpCode) (TokenEncoder, error)
}

// checkDecoder returns the handler of the op code for the dialect of
// the version ver, introduced is the version that introduced the op
// code. It fails with [ErrOpCodeTooNew] when introduced is newer than
// ver.
func checkDecoder(d Dialect, op OpCode, handler TokenDecoder, introduced, ver int) (TokenDecoder, error) {
	if introduced > ver {
		return nil, errOpCodeTooNew(d, op, introduced, ver)
	}
	return handler, nil
}

// checkEncoder is the write side of [checkDecoder].
func checkEncoder(d Dialect, op OpCode, handler TokenEncoder, introduced, ver int) (TokenEncoder, error) {
	if introduced > ver {
		return nil, errOpCodeTooNew(d, op, introduced, ver)
	}
	return handler, nil
}

// errInvalidOpCode returns the error of an op code the dialect assigns
// no meaning to.
func errInvalidOpCode(d Dialect, op OpCode) error {
	return fmt.Errorf("%w: %s (%x) in the %T dialect", ErrInvalidOpCode, d.OpCodeString(op), op, d)
}

// errOpCodeTooNew returns the error of an op code that was introduced
// in a version of the RDB format newer than the version of the
// dialect.
func errOpCodeTooNew(d Dialect, op OpCode, introduced, version int) error {
	return fmt.Errorf("%w: %s (%x) was introduced in version %d, the dialect is version %d", ErrOpCodeTooNew, d.OpCodeString(op), op, introduced, version)
}

var (
	dialectsMu sync.RWMutex
	// dialects is the registry Decoders use to detect the dialect of a
	// file. Redis comes first, its files are a superset of the compatible
	// forks that reuse its header. The dialects of the registry are
	// unbound, they represent the newest version they support.
	dialects = []Dialect{
		RedisDialect{},
		ValkeyDialect{},
	}
)

// RegisterDialect adds d to the registry Decoders use to detect the dialect
// of a file from its header. It allows supporting forks of Redis that ship
// their own header or assign different meanings to op codes. The dialects
// of the registry are unbound, a bound one only detects the files of the
// version it represents.
func RegisterDialect(d Dialect) {
	dialectsMu.Lock()
	defer dialectsMu.Unlock()
	dialects = append(dialects, d)
}

// detectDialect returns the first dialect in the registry that writes
// the magic string of the header, or nil when no dialect does. The
// version of the header is the business of the dialect, the decoder
// rejects the versions it does not support.
func detectDialect(magic string) Dialect {
	dialectsMu.RLock()
	defer dialectsMu.RUnlock()
	for _, d := range dialects {
		if m, _ := d.Header(); magic == m {
			return d
		}
	}
	return nil
}

// Op constructs a Token representing an op code.
func Op(op OpCode) Token { return Token{k: KindOpCode, opCode: op} }

// Str constructs a Token representing a length prefixed string,
// e.g. a key name or a string value.
func Str(s string) Token { return Token{k: KindString, str: []byte(s)} }

// Packed constructs a Token of a packed value encoding, e.g. a quick
// list, a zip list or an int set. The kind tells the encoding,
// the raw bytes are written verbatim.
func Packed(kind Kind, b []byte) Token { return Token{k: kind, raw: b} }

// Byte constructs a Token representing a byte string written verbatim,
// e.g. the specification bytes of a key metadata class.
func Byte(b []byte) Token { return Token{k: KindByte, str: b} }

// Uint constructs a Token representing a length encoded unsigned integer.
func Uint(n uint64) Token { return Token{k: KindUint, num: n} }

// Int constructs a Token representing a signed integer stored as its
// two's complement bit pattern.
func Int(n int64) Token { return Token{k: KindInt, num: uint64(n)} }

// Float32 constructs a Token representing a 32-bit floating-point
// number stored as its IEEE-754 bit pattern.
func Float32(f float32) Token { return Token{k: KindFloat32, num: uint64(math.Float32bits(f))} }

// Float64 constructs a Token representing a 64-bit floating-point
// number stored as its IEEE-754 bit pattern. The scores of a sorted
// set record that stores them as text carry the op code of the record
// on the token, which the wire format selects on, see [Token.Bytes].
func Float64(f float64) Token { return Token{k: KindFloat64, num: math.Float64bits(f)} }

// Expiry constructs a Token representing an expiry timestamp
// in milliseconds since the epoch.
func Expiry(ms uint64) Token { return Token{k: KindExpiry, num: ms, opCode: RedisOpCodeExpiryMilli} }

// StreamID constructs a Token representing a stream entry ID: the
// milliseconds time and the sequence number of the entry.
func StreamID(ms, seq uint64) Token {
	str := make([]byte, 16)
	binary.BigEndian.PutUint64(str[:8], ms)
	binary.BigEndian.PutUint64(str[8:], seq)
	return Token{k: KindStreamID, str: str}
}

// Time constructs a Token representing a milliseconds timestamp, the
// times of the stream metadata and its pending entries among others.
func Time(t time.Time) Token { return Token{k: KindTime, num: uint64(t.UnixMilli())} }

// LenBytes returns the canonical length encoding of v,
// using the shortest possible form.
func LenBytes(v uint64) []byte { return encodeLenBytes(v) }
