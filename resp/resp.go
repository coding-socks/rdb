// Package resp implements a RESP server for Redis compatible clients
// with the shape of [net/http]: a [Server] accepts connections, a
// [Handler] serves the commands of a [Conn] and a [ResponseWriter]
// writes the reply of a command.
//
// # Handlers and commands
//
// A [Command] is one command of a client, the name of the command and
// its arguments. The commands are served by the [Handler] of the server, usually a
// [ServeMux] dispatching on the command patterns it was given. The
// reserved HELLO, AUTH, RESET and QUIT are served by the connection
// itself, HELLO switches the protocol of the session without losing
// the input the old decoder has already read.
//
// # Replies
//
// The commands are decoded and the replies encoded with the streaming
// decoders and encoders of the package, one adapter per protocol
// version: [RESP2Decoder]/[RESP2Encoder] and [RESP3Decoder]/[RESP3Encoder].
// A handler writes its reply through the [ResponseWriter] of the
// command: the [ResponseWriter.Proto] method returns the encoder of
// the current protocol, the tokens of the reply are written with the
// constructors of the package. [ResponseWriter.ChangeProto] switches
// the protocol of the session, the input the old decoder has already
// read survives the switch.
//
// # Serving
//
// The event loop of a connection is single threaded, like the one of
// Redis: it reads a command, dispatches it to the handler and flushes
// the reply before it reads the next one.
package resp

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"iter"
	"math"
	"math/big"
	"net"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Command is one command read from a [Conn].
type Command struct {
	// Args are the arguments of the command, the name of the command
	// first, the names of a nested command behind it, e.g. CONFIG and
	// GET for a CONFIG GET. They are copied out of the buffer of the
	// connection, they stay valid after the command has been served.
	Args [][]byte

	// ctx is the context of the connection the command was read from,
	// see [Command.Context].
	ctx context.Context
}

// Context returns the context of the connection the command was read
// from. It is canceled when the connection is closed, the handlers of
// a connection run on its event loop and can hand it to the
// goroutines they start.
func (c *Command) Context() context.Context {
	return c.ctx
}

// Handler serves the commands of a [Server].
type Handler interface {
	// ServeRESP serves one command of a [Conn], writing its reply with
	// w. It runs on the event loop of the connection, no other command
	// of the connection is read or served until it returns.
	ServeRESP(w ResponseWriter, cmd *Command)
}

// The HandlerFunc type is an adapter to allow the use of ordinary
// functions as [Handler] interfaces.
type HandlerFunc func(w ResponseWriter, cmd *Command)

// ServeRESP calls f(w, cmd).
func (f HandlerFunc) ServeRESP(w ResponseWriter, cmd *Command) {
	f(w, cmd)
}

// ResponseWriter writes the reply of a command. Its methods must only
// be called by the [Handler] of the command.
type ResponseWriter interface {
	// Proto returns the encoder of the current protocol of the
	// session, a *[RESP2Encoder] or a *[RESP3Encoder]. The tokens of
	// the reply are written with the constructors of the package, the
	// bytes are buffered until [ResponseWriter.Flush] writes them to
	// the connection.
	Proto() Encoder

	// ChangeProto switches the protocol of the session to p, 2 or 3.
	// The input the old decoder has already read is carried over to
	// the new one, so the commands pipelined behind the switch are
	// decoded by it. It returns an error for a version other than
	// 2 or 3.
	ChangeProto(p int) error

	// Auth authenticates the connection for the user and its
	// password. The user of the server is default, the comparison of
	// the password runs in constant time. It marks the connection
	// authenticated and returns nil on success, it returns an error
	// for a user or a password the server does not know and when the
	// server has no password.
	Auth(user, password string) error

	// Flush writes the buffered reply to the connection. The reply of
	// a command is flushed when its handler returns, Flush is only
	// needed to end a message before the reply continues, a push
	// between the messages of a reply for example.
	Flush() error

	// Hijack lets the caller take over the connection. After the call
	// the server does nothing else with the connection, it becomes
	// the caller's responsibility to manage and close it, the
	// commands that follow the command of the handler are never
	// read.
	//
	// The reply of the command must be flushed before the call, a
	// hijack that carries bytes written but not flushed returns
	// [ErrNotFlushed]. The input the decoder of the session has
	// already read but not consumed is dropped, a decoder the caller
	// wraps around the returned connection reads the connection
	// itself. The writer of the command must not be used after the
	// call. Hijack returns [ErrHijacked] when the connection was
	// already hijacked.
	Hijack() (net.Conn, error)

	// RemoteAddr returns the address of the peer of the connection
	// the command arrived on.
	RemoteAddr() net.Addr
}

const (
	KindString         Kind = '+'
	KindBlobString     Kind = '$'
	KindBulkString          = KindBlobString
	KindVerbatimString Kind = '='
	KindError          Kind = '-'
	KindBlobError      Kind = '!'
	KindNumber         Kind = ':'
	KindInteger             = KindNumber
	KindBigNumber      Kind = '('
	KindDouble         Kind = ','
	KindBoolean        Kind = '#'
	KindNull           Kind = '_'
	KindArray          Kind = '*'
	KindMap            Kind = '%'
	KindSet            Kind = '~'
	KindAttribute      Kind = '|'
	KindEnd            Kind = '.'
	KindPush           Kind = '>'
	KindStringPart     Kind = ';'
)

// Kind represents each possible RESP token kind with a single byte:
//
//   - `+`: string
//   - `$`: blob string
//   - `=`: verbatim string
//   - `-`: error
//   - `!`: blob error
//   - `:`: number
//   - `(`: big number
//   - `,`: double
//   - `#`: boolean
//   - `_`: null
//   - `*`: array len
//   - `%`: map len
//   - `~`: set len
//   - `|`: attribute
//   - `.`: end
//   - `>`: push
//   - `;`: string part
//
// An invalid kind is usually represented using 0,
// but may be non-zero due to invalid RESP data.
type Kind byte

// String returns the name of the kind.
func (k Kind) String() string {
	switch k {
	case '+':
		return "string"
	case '$':
		return "blob string"
	case '=':
		return "verbatim string"
	case '-':
		return "error"
	case '!':
		return "blob error"
	case ':':
		return "number"
	case '(':
		return "big number"
	case ',':
		return "double"
	case '#':
		return "boolean"
	case '_':
		return "null"
	case '*':
		return "array len"
	case '%':
		return "map len"
	case '~':
		return "set len"
	case '|':
		return "attribute"
	case '.':
		return "end"
	case '>':
		return "push"
	case ';':
		return "string part"
	default:
		return "<invalid resp.Kind: " + quoteRune(rune(k)) + ">"
	}
}

type raw struct {
	b []byte
}

// Token is the unit of data a [Decoder] reads and an [Encoder] writes.
//
// A Token stores data in one of two forms:
//
//   - As raw RESP text: backed by the internal buffer of the [Decoder]
//     and only ever produced by [Decoder.ReadToken].
//     Such a token is only valid until the next call to any method on
//     that [Decoder]. Call [Token.Clone] to copy the raw text into an
//     independent allocation that persists beyond subsequent
//     [Decoder] calls.
//
//   - As a typed Go value: a self-contained representation produced by
//     the constructors (e.g., [String], [Int], [Bytes]).
//     Such tokens are valid indefinitely and do not need to be cloned.
//
// Both forms provide the same view: the value methods return the value
// wherever it is stored.
type Token struct {
	// Tokens can exist in either a "raw" or an "exact" form.
	// Tokens produced by the Decoder are in the "raw" form.
	// Tokens returned by constructors are usually in the "exact" form.
	// The Encoder accepts Tokens in either the "raw" or "exact" form.
	//
	// The following chart shows the possible values for each Token type:
	//	╔══════════════════╦════════════╤════════════╤════════════╤════════════╗
	//	║ Token type       ║ raw field  │ str field  │ num field  │ stream     ║
	//	╠══════════════════╬════════════╪════════════╪════════════╪════════════╣
	//	║ string   (raw)   ║ non-empty  │ nil        │ 0          │ nil        ║
	//	║ string   (exact) ║ nil        │ non-empty  │ 0          │ nil        ║
	//	║ big num  (raw)   ║ non-empty  │ nil        │ 0          │ nil        ║
	//	║ big num  (exact) ║ nil        │ non-empty  │ 0          │ nil        ║
	//	║ error    (raw)   ║ non-empty  │ nil        │ 0          │ nil        ║
	//	║ error    (exact) ║ nil        │ non-empty  │ 0          │ nil        ║
	//	║ blob str (raw)   ║ non-empty  │ nil        │ 0          │ nil        ║
	//	║ blob str (exact) ║ nil        │ non-empty  │ length     │ nil        ║
	//	║ blob str (stream)║ non-empty  │ nil        │ length     │ non-nil    ║
	//	║ verbatim (exact) ║ nil        │ non-empty  │ length     │ nil        ║
	//	║ verbatim (stream)║ non-empty  │ nil        │ length     │ non-nil    ║
	//	║ blob err (exact) ║ nil        │ non-empty  │ length     │ nil        ║
	//	║ blob err (stream)║ non-empty  │ nil        │ length     │ non-nil    ║
	//	║ str part (raw)   ║ non-empty  │ nil        │ 0          │ nil        ║
	//	║ str part (exact) ║ nil        │ non-empty  │ length     │ nil        ║
	//	║ str part (stream)║ non-empty  │ nil        │ length     │ non-nil    ║
	//	║ number   (raw)   ║ non-empty  │ nil        │ 0          │ nil        ║
	//	║ number   (exact) ║ nil        │ first byte │ non-zero   │ nil        ║
	//	║ push     (raw)   ║ non-empty  │ nil        │ 0          │ nil        ║
	//	║ push     (exact) ║ nil        │ first byte │ non-zero   │ nil        ║
	//	║ double   (raw)   ║ non-empty  │ nil        │ 0          │ nil        ║
	//	║ double   (exact) ║ nil        │ first byte │ non-zero   │ nil        ║
	//	║ array    (raw)   ║ non-empty  │ nil        │ 0          │ nil        ║
	//	║ array    (exact) ║ nil        │ first byte │ length     │ nil        ║
	//	║ map      (raw)   ║ non-empty  │ nil        │ 0          │ nil        ║
	//	║ map      (exact) ║ nil        │ first byte │ length     │ nil        ║
	//	║ set      (raw)   ║ non-empty  │ nil        │ 0          │ nil        ║
	//	║ set      (exact) ║ nil        │ first byte │ length     │ nil        ║
	//	║ attr     (raw)   ║ non-empty  │ nil        │ 0          │ nil        ║
	//	║ attr     (exact) ║ nil        │ first byte │ length     │ nil        ║
	//	║ null     (raw)   ║ non-empty  │ nil        │ 0          │ nil        ║
	//	║ boolean  (raw)   ║ non-empty  │ nil        │ 0          │ nil        ║
	//	║ end      (raw)   ║ non-empty  │ nil        │ 0          │ nil        ║
	//	║ unbound  (raw)   ║ non-empty  │ nil        │ 0          │ nil        ║
	//	║ unbound  (stream)║ non-empty  │ nil        │ 0          │ non-nil    ║
	//	╚══════════════════╩════════════╧════════════╧════════════╧════════════╝
	//
	// Notes:
	//   - For a token that is not streamed, the raw field holds the
	//     complete wire representation, the terminator and the length
	//     prefix of a blob included. For a streamed token it holds only
	//     the header line of the blob.
	//   - The null, the booleans, the end marker and the unbound
	//     aggregates are always in the "raw" form.
	//   - The exact zero value of a string, a number or a double has
	//     ambiguous representation, the constructors return the shared
	//     tokens in the "raw" form for it.

	// raw contains a reference to the raw wire bytes.
	// If non-nil, then its value takes precedence over str and num.
	raw *raw

	// str is the RESP value without its terminator. The first byte is
	// the kind of the value, the rest is the byte representation.
	str []byte

	// num is a float64, int64, or uint64 stored as a uint64 value.
	// For floating-point values, it stores the raw IEEE-754 bit-pattern.
	// It is non-zero for any RESP number in the "exact" form.
	num uint64

	// stream carries the payload of a streamed blob: the payload and
	// the terminator of a sized transfer, the payload and the marker
	// of an EOF-marked one, the framed string parts of an unbounded
	// one. It is set by [BlobString], [BlobStringEOF],
	// [BlobStringUnbounded] and [Decoder.ReadToken], it is nil for
	// every other token. The num field of a streamed token holds the
	// payload length, zero for an EOF-marked blob string and for an
	// unbounded one. The remainder is read through [Token.Reader], an
	// unconsumed remainder is skipped with the next
	// [Decoder.ReadToken], the encoder replays it when the token is
	// written. The payload of a token serves one consumer, see
	// [Token.Reader].
	stream io.Reader

	// notEqual guarantees that two Token will never be equal to
	// each other.
	notEqual func()
}

var (
	Null  = rawToken("_\r\n")
	True  = rawToken("#t\r\n")
	False = rawToken("#f\r\n")

	End = rawToken(".\r\n")

	zeroString     = rawToken("+\r\n")
	zeroInt        = rawToken(":0\r\n")
	zeroFloat      = rawToken(",0\r\n")
	zeroBytes      = rawToken("$-1\r\n")
	zeroArray      = rawToken("*-1\r\n")
	zeroStringPart = rawToken(";0\r\n")

	UnboundArray  = rawToken("*?\r\n")
	UnboundMap    = rawToken("%?\r\n")
	UnboundSet    = rawToken("~?\r\n")
	UnboundAttr   = rawToken("|?\r\n")
	UnboundString = rawToken("$?\r\n")

	nanDouble  = rawToken(",nan\r\n")
	pinfDouble = rawToken(",inf\r\n")
	ninfDouble = rawToken(",-inf\r\n")
)

func rawToken(s string) Token {
	return Token{raw: &raw{b: []byte(s)}}
}

// Bool constructs a Token representing a RESP boolean.
func Bool(b bool) Token {
	if b {
		return True
	}
	return False
}

// String constructs a Token representing a RESP string.
func String(s string) Token {
	if len(s) == 0 {
		return zeroString
	}
	return Token{str: append([]byte{'+'}, strings.Map(stringSanitizer, s)...)}
}

// Int constructs a Token representing a RESP number.
func Int(n int64) Token {
	if n == 0 {
		return zeroInt
	}
	return Token{str: []byte{':'}, num: uint64(n)}
}

// Float constructs a Token representing a RESP double.
// The values NaN, +Inf, and -Inf will be represented
// as a RESP double with the values "nan", "inf", and "-inf".
func Float(n float64) Token {
	switch {
	case math.Float64bits(n) == 0:
		return zeroFloat
	case math.IsNaN(n):
		return nanDouble
	case math.IsInf(n, +1):
		return pinfDouble
	case math.IsInf(n, -1):
		return ninfDouble
	}
	return Token{str: []byte{','}, num: math.Float64bits(n)}
}

// Error constructs a Token representing a RESP error.
func Error(s string) Token {
	return Token{str: append([]byte{'-'}, strings.Map(stringSanitizer, s)...)}
}

var stringSanitizer = func(r rune) rune {
	if r == '\r' || r == '\n' {
		return ' '
	}
	return r
}

// BlobError constructs a Token representing a RESP blob error.
func BlobError(err error) Token {
	s := err.Error()
	return Token{str: append([]byte{'!'}, s...), num: uint64(len(s))}
}

// Bytes constructs a Token representing a RESP blob string.
// A nil slice constructs the null blob string.
func Bytes(p []byte) Token {
	if p == nil {
		return zeroBytes
	}
	return Token{str: append([]byte{'$'}, p...), num: uint64(len(p))}
}

// BulkString is an alias for [BlobString].
func BulkString(r *io.LimitedReader) Token {
	return BlobString(r)
}

// BlobString constructs a Token representing a blob string whose
// payload is the bytes the limited reader still holds: its N is the
// length of the token. The encoder writes the header of the token and
// streams the remainder behind it when the token is written, a reader
// that ends before its N bytes fails the write.
// It panics if the reader is nil.
func BlobString(r *io.LimitedReader) Token {
	if r == nil {
		panic("resp: nil payload reader")
	}
	hdr := append([]byte{'$'}, strconv.AppendInt(nil, r.N, 10)...)
	hdr = append(hdr, '\r', '\n')
	return Token{
		raw:    &raw{b: hdr},
		num:    uint64(r.N),
		stream: io.MultiReader(r, bytes.NewReader([]byte("\r\n"))),
	}
}

// BulkStringEOF is an alias for [BlobStringEOF].
func BulkStringEOF(r io.Reader) Token {
	return BlobStringEOF(r)
}

// BlobStringEOF constructs a Token representing an EOF-marked blob
// string, a blob string whose payload ends at a marker instead of a
// length: the form the RDB transfer of a replication session announces
// with $EOF:<marker>. The marker is generated for the token. The
// encoder writes the header of the token and streams the remainder
// behind it when the token is written, the payload is the bytes of the
// reader up to its end.
// It panics if the reader is nil.
func BlobStringEOF(r io.Reader) Token {
	if r == nil {
		panic("resp: nil payload reader")
	}
	marker := newMarker()
	hdr := append([]byte("$EOF:"), marker...)
	hdr = append(hdr, '\r', '\n')
	return Token{
		raw:    &raw{b: hdr},
		stream: io.MultiReader(r, bytes.NewReader(marker)),
	}
}

// BlobStringUnbounded constructs a Token representing an unbound blob
// string, a blob string whose payload arrives in string parts of at
// most chunk bytes announced with their ;<len>\r\n headers and
// terminated by a part of zero length. The encoder writes the header
// of the token and streams the parts from the reader when the token is
// written.
// It panics if the reader is nil or the chunk is not positive.
func BlobStringUnbounded(chunk uint, r io.Reader) Token {
	if r == nil {
		panic("resp: nil payload reader")
	}
	if chunk == 0 {
		panic("resp: chunk must be positive")
	}
	return Token{
		raw:    UnboundString.raw,
		stream: newPartFramer(r, int(chunk)),
	}
}

// newMarker returns an unguessable marker of 40 hexadecimal digits,
// the size the replica side of a replication session expects.
func newMarker() []byte {
	var b [20]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("resp: cannot generate an EOF marker: " + err.Error())
	}
	return []byte(hex.EncodeToString(b[:]))
}

// partsStream reads the data of a streamed unbound blob string: the
// payload of its string parts without their ;<len>\r\n framing, ended
// by the part of zero length. The parts are read with the decoder d
// over the reader of the token, a constructed token streams its
// framed parts behind the header the encoder writes.
// The payload of a token serves one consumer, an encoder cannot write
// a token whose parts were handed out.
type partsStream struct {
	d   *RESP3Decoder // the decoder of the parts
	buf *bytes.Buffer
	err error
}

func newPartsStream(d *RESP3Decoder) *partsStream {
	return &partsStream{d: d, buf: bytes.NewBuffer(nil)}
}

func (s *partsStream) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	if s.err != nil {
		return 0, s.err
	}
	if s.buf.Len() > 0 {
		return s.buf.Read(b)
	}
	t, err := s.d.ReadToken()
	if err != nil {
		s.err = err
		return 0, err
	}
	if t.Kind() != KindStringPart {
		s.err = fmt.Errorf("resp: unexpected token kind %v", t.Kind())
		return 0, s.err
	}
	if t.raw == zeroStringPart.raw {
		s.err = io.EOF
		return 0, io.EOF
	}
	if _, err := s.buf.ReadFrom(dropLastNReader(t.stream, 2)); err != nil {
		s.err = err
		return 0, err
	}
	return s.buf.Read(b)
}

// partFramer frames the data of its reader into streamed string parts.
type partFramer struct {
	r     io.Reader
	chunk int
	buf   []byte // the data of the part being read
	pend  []byte // the bytes of the current part waiting for delivery
	done  bool
	err   error
}

// newPartFramer returns a reader that frames the data of r into
// streamed string parts of at most chunk bytes: every part is
// announced with a ;<len>\r\n header and ends with its \r\n, a part of
// zero length ends the stream.
func newPartFramer(r io.Reader, chunk int) io.Reader {
	return &partFramer{r: r, chunk: chunk, buf: make([]byte, chunk)}
}

func (f *partFramer) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	for {
		if len(f.pend) > 0 {
			n := copy(b, f.pend)
			f.pend = f.pend[n:]
			return n, nil
		}
		if f.done {
			return 0, io.EOF
		}
		if f.err != nil {
			return 0, f.err
		}

		n, err := f.r.Read(f.buf)
		if err != nil && !errors.Is(err, io.EOF) {
			f.err = err
			if n == 0 {
				return 0, err
			}
		}
		if n > 0 {
			// a part carries its data and the \r\n behind it
			f.pend = append(f.pend[:0], ';')
			f.pend = strconv.AppendInt(f.pend, int64(n), 10)
			f.pend = append(f.pend, '\r', '\n')
			f.pend = append(f.pend, f.buf[:n]...)
			f.pend = append(f.pend, '\r', '\n')
			continue
		}
		if errors.Is(err, io.EOF) {
			f.done = true
			f.pend = append(f.pend[:0], ";0\r\n"...)
		}
	}
}

// PartBytes constructs a Token representing a RESP string part of a
// streamed blob string. An empty slice constructs the terminating part.
func PartBytes(p []byte) Token {
	if p == nil || len(p) == 0 {
		return zeroStringPart
	}
	return Token{str: append([]byte{';'}, p...), num: uint64(len(p))}
}

// BigInt constructs a Token representing a RESP big number.
func BigInt(i *big.Int) Token {
	return Token{str: append([]byte{'('}, i.String()...)}
}

// VerbatimString constructs a Token representing a RESP verbatim string.
// The format is a three character hint such as [VerbatimStringTXT].
func VerbatimString(i VerbatimStr) Token {
	s := i.Format + ":" + i.String
	return Token{str: append([]byte{'='}, s...), num: uint64(len(s))}
}

// Push constructs a Token representing a RESP push length.
func Push(n int64) Token {
	if n == 0 {
		return zeroInt
	}
	return Token{str: []byte{'>'}, num: uint64(n)}
}

// ArrayLen constructs a Token representing a RESP array length.
// A negative length constructs the null array.
func ArrayLen(l int) Token {
	if l < 0 {
		return zeroArray
	}
	return Token{str: []byte{'*'}, num: uint64(l)}
}

// MapLen constructs a Token representing a RESP map length, which
// counts the pairs of the map.
// It panics on a negative length, a map has no null form.
func MapLen(l int) Token {
	if l < 0 {
		panic("resp: len must be non-negative")
	}
	return Token{str: []byte{'%'}, num: uint64(l)}
}

// SetLen constructs a Token representing a RESP set length.
// It panics on a negative length, a set has no null form.
func SetLen(l int) Token {
	if l < 0 {
		panic("resp: len must be non-negative")
	}
	return Token{str: []byte{'~'}, num: uint64(l)}
}

// AttributeLen constructs a Token representing a RESP attribute length,
// which counts the pairs of the attribute.
// It panics on a negative length, an attribute has no null form.
func AttributeLen(l int) Token {
	if l < 0 {
		panic("resp: len must be non-negative")
	}
	return Token{str: []byte{'|'}, num: uint64(l)}
}

// Kind returns the kind of the token, which is the first byte of its
// wire representation.
func (t *Token) Kind() Kind {
	if t.raw != nil {
		return Kind(t.raw.b[0])
	}
	return Kind(t.str[0])
}

// Val returns the value of the token as a Go value, the way the
// accessors do: a string for a RESP string, an error for a RESP error
// and blob error, an int64 for a RESP number and the length kinds, a
// []byte for a blob string and a string part, and so on. An unbound
// blob string or aggregate and an EOF-marked blob string return their
// [Token.Term], a RESP null returns nil.
// It panics if the token kind is not a RESP kind.
func (t *Token) Val() any {
	switch t.Kind() {
	case '+':
		return t.String()
	case ';':
		return t.Bytes()
	case '$':
		switch t := t.Term().(type) {
		case TermUnbound:
			return t
		case TermByte:
			return t
		}
		return t.Bytes()
	case '=':
		return t.VerbatimString()
	case '-', '!':
		return t.Err()
	case ':':
		return t.Int()
	case '(':
		return t.BigInt()
	case ',':
		return t.Float()
	case '#':
		return t.Bool()
	case '_':
		return nil
	case '*', '%', '~', '|':
		if t.IsUnbound() {
			return t.Term()
		}
		return t.Int()
	case '.':
		return nil
	case '>':
		return t.Int()
	}
	panic("invalid RESP token kind: " + t.Kind().String())
}

// Bool returns the value of a RESP boolean.
// It panics if the token kind is not a RESP boolean.
func (t *Token) Bool() bool {
	switch t.raw {
	case True.raw:
		return true
	case False.raw:
		return false
	default:
		panic("invalid RESP token kind: " + t.Kind().String())
	}
}

// Err returns the error of a RESP error or blob error, nil for every
// other kind. The payload of a streamed token is read out of its
// reader, the reader is consumed by the call.
func (t *Token) Err() error {
	switch t.Kind() {
	case '-':
		if t.raw != nil {
			return errors.New(string(t.raw.b[1 : len(t.raw.b)-2]))
		}
		return errors.New(string(t.str[1:]))
	case '!':
		if t.stream != nil {
			return errors.New(string(t.replayReader()))
		}
		if t.raw != nil {
			return errors.New(string(t.raw.b[len(t.raw.b)-2-int(t.num) : len(t.raw.b)-2]))
		}
		return errors.New(string(t.str[1:]))
	default:
		return nil
	}
}

// Bytes returns the bytes of a RESP string, error, blob string, string
// part, blob error or verbatim string without their length or kind,
// nil for the null blob string. The payload of a streamed token is
// read out of its reader, the reader is consumed by the call.
// It panics if the token kind is none of the aforementioned or the
// token is an unbound blob string.
func (t *Token) Bytes() []byte {
	switch t.Kind() {
	case '+', '-':
		if t.raw != nil {
			return t.raw.b[1 : len(t.raw.b)-2]
		}
		return t.str[1:]
	case '$', ';', '!', '=':
		if t.stream != nil {
			return t.replayReader()
		}
		if t.IsUnbound() {
			panic("resp: unbound blob string is not supported")
		}
		if t.IsNil() || t.raw == zeroStringPart.raw {
			return nil
		}
		if t.raw != nil {
			return t.raw.b[len(t.raw.b)-2-int(t.num) : len(t.raw.b)-2]
		}
		return t.str[1:]
	default:
		panic("invalid RESP token kind: " + t.Kind().String())
	}
}

// String returns the content of a RESP string, error or blob string
// without its kind byte, an empty string for the null blob string.
// The payload of a streamed token is read out of its reader, the
// reader is consumed by the call.
// For other kinds it returns the raw RESP representation, so a Token
// satisfies [fmt.Stringer].
// It panics if the token kind is not a RESP kind or the token is an
// unbound blob string.
func (t *Token) String() string {
	switch t.Kind() {
	case '+', '-':
		if t.raw != nil {
			return string(t.raw.b[1 : len(t.raw.b)-2])
		}
		return string(t.str[1:])
	case '$':
		if t.stream != nil {
			return string(t.replayReader())
		}
		if t.IsUnbound() {
			panic("resp: unbound blob string is not supported")
		}
		if t.IsNil() {
			return ""
		}
		if t.raw != nil {
			return string(t.raw.b[len(t.raw.b)-2-int(t.num) : len(t.raw.b)-2])
		}
		return string(t.str[1:])
	default:
		panic("invalid RESP token kind: " + t.Kind().String())
	}
}

// VerbatimString returns the format hint and the content of a RESP
// verbatim string.
// It panics if the token kind is not a RESP verbatim string.
func (t *Token) VerbatimString() VerbatimStr {
	switch t.Kind() {
	case '=':
		if t.stream != nil {
			p := t.replayReader()
			return VerbatimStr{Format: string(p[:3]), String: string(p[4:])}
		}
		if t.raw != nil {
			payload := t.raw.b[len(t.raw.b)-2-int(t.num) : len(t.raw.b)-2]
			return VerbatimStr{Format: string(payload[:3]), String: string(payload[4:])}
		}
		return VerbatimStr{Format: string(t.str[1:4]), String: string(t.str[5:])}
	default:
		panic("invalid RESP token kind: " + t.Kind().String())
	}
}

// Int returns the integer value of a RESP number, the lengths of the
// aggregate kinds and a push, the length of a blob string, and -1 for
// the null blob string and null array.
// It panics if the token kind is none of the aforementioned or the
// token is an unbound blob string.
func (t *Token) Int() int64 {
	switch t.Kind() {
	case '$', '*', '%', '~', '|', '>':
		if t.raw != nil {
			if t.IsNil() {
				return -1
			}
			if t.Kind() == '$' && t.IsUnbound() {
				panic("resp: unbound blob string is not supported")
			}
			i, _ := strconv.ParseInt(string(t.raw.b[1:len(t.raw.b)-2]), 10, 64)
			return i
		}
		return int64(t.num)
	case ':':
		if t.raw != nil {
			i, _ := strconv.ParseInt(string(t.raw.b[1:len(t.raw.b)-2]), 10, 64)
			return i
		}
		return int64(t.num)
	default:
		panic("invalid RESP token kind: " + t.Kind().String())
	}
}

// BigInt returns the value of a RESP big number, nil when its content
// does not parse as one.
// It panics if the token kind is not a RESP big number.
func (t *Token) BigInt() *big.Int {
	switch t.Kind() {
	case '(':
		if t.raw != nil {
			i, _ := big.NewInt(0).SetString(string(t.raw.b[1:len(t.raw.b)-2]), 10)
			return i
		}
		i, _ := big.NewInt(0).SetString(string(t.str[1:]), 10)
		return i
	default:
		panic("invalid RESP token kind: " + t.Kind().String())
	}
}

// Float returns the value of a RESP double.
// It panics if the token kind is not a RESP double.
func (t *Token) Float() float64 {
	switch t.Kind() {
	case ',':
		switch t.raw {
		case nanDouble.raw:
			return math.NaN()
		case pinfDouble.raw:
			return math.Inf(1)
		case ninfDouble.raw:
			return math.Inf(-1)
		}
		if t.raw != nil {
			f, _ := strconv.ParseFloat(string(t.raw.b[1:len(t.raw.b)-2]), 64)
			return f
		}
		return math.Float64frombits(t.num)
	default:
		panic("invalid RESP token kind: " + t.Kind().String())
	}
}

// IsNil reports whether the token is the null blob string, the null
// array or the RESP null.
func (t *Token) IsNil() bool {
	switch k := t.Kind(); k {
	case '$', '*':
		return t.raw == zeroBytes.raw || t.raw == zeroArray.raw
	case '_':
		return true
	}
	return false
}

// IsUnbound reports whether the token is an unbound aggregate: an
// aggregate written with an unknown length whose elements are
// terminated by [KindEnd], or a blob string whose payload arrives in
// string parts terminated by a [KindStringPart] of zero length.
func (t *Token) IsUnbound() bool {
	switch t.Term().(type) {
	case TermUnbound:
		return true
	}
	return false
}

// Reader returns the payload of the token as a reader: the payload of
// a blob string, a blob error, a verbatim string or a string part
// without its header and terminator, without its marker for an
// EOF-marked one, and without the framing of its string parts for an
// unbound one. For a token whose payload stays in the buffer the
// reader is over the buffered bytes. The reader is the one
// [Token.Bytes] and [Token.String] consume, reading it directly leaves
// those accessors with an empty payload, and the payload of a token
// serves one consumer: an encoder cannot write a token whose payload
// was handed out, an unconsumed payload is skipped with the next
// [Decoder.ReadToken].
// It panics if the token kind carries no payload or the token is an
// unbound blob string.
func (t *Token) Reader() io.Reader {
	switch t.Kind() {
	case KindBlobString, KindBlobError, KindVerbatimString, KindStringPart:
	default:
		panic("invalid RESP token kind: " + t.Kind().String())
	}
	if t.stream == nil {
		if t.IsUnbound() {
			panic("resp: unbound blob string is not supported")
		}
		if t.IsNil() || t.raw == zeroStringPart.raw {
			return bytes.NewReader(nil)
		}
		if t.raw != nil {
			return bytes.NewReader(t.raw.b[len(t.raw.b)-2-int(t.num) : len(t.raw.b)-2])
		}
		return bytes.NewReader(t.str[1:])
	}
	switch term := t.Term().(type) {
	case TermUnbound:
		return newPartsStream(NewRESP3Decoder(t.stream))
	case TermByte:
		return dropLastNReader(t.stream, len(term))
	case TermLen:
		return dropLastNReader(t.stream, 2)
	}
	return nil
}

// Term returns the terminator of the token: a [TermLen] for the sized
// kinds, a [TermByte] for a line or the marker of an EOF-marked blob
// string, and a [TermUnbound] for the unbound blob string and the
// unbound aggregates.
func (t *Token) Term() Term {
	switch k := t.Kind(); k {
	case '$':
		if t.raw != nil {
			if len(t.raw.b) > 7 && bytes.Equal(t.raw.b[:5], []byte("$EOF:")) {
				return TermByte(bytes.Clone(t.raw.b[5 : len(t.raw.b)-2]))
			}
			if len(t.raw.b) == 4 && t.raw.b[1] == '?' {
				return TermUnbound(zeroStringPart)
			}
		}
		return TermLen(t.num)
	case '*', '%', '~', '|':
		if t.raw != nil && len(t.raw.b) == 4 && t.raw.b[1] == '?' {
			return TermUnbound(End)
		}
		return TermLen(t.Int())
	}
	return TermByte(eol)
}

// replayReader returns the payload of a streamed token as bytes: the
// reader of the token is drained through a [Replay], the payload is
// replayable after the call. It panics when the payload cannot be
// read.
func (t *Token) replayReader() []byte {
	r := ReplayReader(t.stream)
	t.stream = r.Clone()
	p, err := io.ReadAll(t.Reader())
	t.stream = r.Clone()
	if err != nil {
		panic("resp: cannot read the streamed payload: " + err.Error())
	}
	return p
}

// copyStream writes the payload of a streamed token to w: a payload
// that carries fewer bytes than the header of the token announces
// fails the write with io.ErrUnexpectedEOF. The segment of a decoded
// token carries the terminator of its line behind the payload, the
// stream of a constructed one carries the payload alone. The payload
// of an EOF-marked token and of an unbound one ends without a length,
// it is written as the reader delivers it.
func copyStream(w io.Writer, t Token) error {
	n, err := io.Copy(w, t.stream)
	if err != nil {
		return err
	}
	switch t.Term().(type) {
	case TermUnbound, TermByte:
		return nil
	}
	want := int64(t.num)
	if t.raw != nil {
		want += 2
	}
	if n != want {
		return io.ErrUnexpectedEOF
	}
	return nil
}

// Clone returns a copy of the token with a value that is not backed by
// the [Decoder] buffer and therefore remains valid past subsequent
// [Decoder] calls. It has no effect on tokens produced by constructor
// functions, since those are already self-contained.
func (t Token) Clone() Token {
	if t.raw == nil && t.stream != nil {
		return t
	}
	if len(t.raw.b) <= 6 {
		switch t.raw {
		case Null.raw:
			return Null
		case True.raw:
			return True
		case False.raw:
			return False
		case End.raw:
			return End
		case zeroString.raw:
			return zeroString
		case zeroInt.raw:
			return zeroInt
		case zeroFloat.raw:
			return zeroFloat
		case zeroBytes.raw:
			return zeroBytes
		case zeroArray.raw:
			return zeroArray
		case zeroStringPart.raw:
			return zeroStringPart
		case UnboundArray.raw:
			return UnboundArray
		case UnboundMap.raw:
			return UnboundMap
		case UnboundSet.raw:
			return UnboundSet
		case UnboundAttr.raw:
			return UnboundAttr
		case nanDouble.raw:
			return nanDouble
		case pinfDouble.raw:
			return pinfDouble
		case ninfDouble.raw:
			return ninfDouble
		}
	}
	if t.stream != nil {
		r := ReplayReader(t.stream)
		t.stream = r.Clone()
		return Token{raw: &raw{b: bytes.Clone(t.raw.b)}, num: t.num, str: t.str, stream: r.Clone()}
	}
	return Token{raw: &raw{b: bytes.Clone(t.raw.b)}, num: t.num, str: t.str}
}

// Iter returns an iterator over the elements of an aggregate token: an
// array, a set, a map, an attribute or a push. The elements are read
// from the decoder, the decoder is left behind the last element. The
// array, the set, the map and the attribute iterate their bound and
// unbound form, a push cannot be unbound, its bound elements are read
// like the ones of an array. The string parts of an unbound blob
// string are read with the iterator of the blob string too, every part
// is yielded as a token, a bound blob string yields nothing, its
// payload is read through [Token.Reader].
// It panics if the token kind is none of the aforementioned.
// The iterator carries no error of its own, the function returned next
// to it reports the iteration: it returns the error the iteration hit
// when it is called after the loop, nil when there was none.
func (t *Token) Iter(d Decoder) (iter.Seq[Token], func() error) {
	switch t.Kind() {
	case KindArray, KindSet, KindMap, KindAttribute, KindBlobString, KindPush:
	default:
		panic("resp: not an iterable token: " + t.Kind().String())
	}
	n := int64(0)
	if t.Kind() != KindBlobString {
		n = t.Int()
		if t.Kind() == KindMap || t.Kind() == KindAttribute {
			// The header of a map and an attribute counts the pairs, their
			// elements are the names and the values.
			n *= 2
		}
	}
	var iterErr error
	seq := func(yield func(Token) bool) {
		switch t.Kind() {
		case KindBlobString:
			if !t.IsUnbound() {
				return
			}
			for {
				e, err := d.ReadToken()
				if err != nil {
					iterErr = err
					return
				}
				if e.Kind() != KindStringPart {
					iterErr = fmt.Errorf("resp3: unexpected %s in a streamed blob string", e.Kind())
					return
				}
				if e.raw == zeroStringPart.raw {
					return
				}
				if !yield(e) {
					return
				}
			}
		case KindArray, KindSet, KindMap, KindAttribute, KindPush:
			if t.IsUnbound() {
				for {
					e, err := d.ReadToken()
					if err != nil {
						iterErr = err
						return
					}
					if e.Kind() == KindEnd {
						return
					}
					if !yield(e) {
						return
					}
				}
			}
			for i := n; i > 0; i-- {
				e, err := d.ReadToken()
				if err != nil {
					iterErr = err
					return
				}
				if !yield(e) {
					return
				}
			}
		}
	}
	return seq, func() error { return iterErr }
}

// The format hints of a RESP verbatim string.
const (
	VerbatimStringTXT = "txt"
	VerbatimStringMKD = "mkd"
)

func quoteRune(r rune) string {
	if utf8.ValidRune(r) {
		return strconv.QuoteRune(r)
	}
	return `'\x` + strconv.FormatUint(uint64(r), 16) + `'`
}

// VerbatimStr is the content of a RESP verbatim string: the three
// character format hint and the text behind it.
type VerbatimStr struct {
	Format string
	String string
}
