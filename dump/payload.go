package dump

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/coding-socks/rdb"
	"github.com/coding-socks/rdb/internal/crc64jones"
)

// ErrInvalidChecksum indicates that a payload could not be decoded
// because the checksum at its end does not match its content.
var ErrInvalidChecksum = errors.New("dump: invalid checksum")

// Encoder encodes the records of an RDB file to the keys of a scan:
// the payload of a key of a scan is the value the DUMP command
// returns, the way the RESTORE command reads it back. The encoder
// reuses the buffers of the records it encodes, one Encoder serves a
// whole transfer.
type Encoder struct {
	dialect rdb.Dialect  // the dialect of the records, the version of its header the payload carries
	rec     bytes.Buffer // the record the tokens encode into, reset by every encode
}

// NewEncoder constructs an Encoder encoding the records of the
// dialect. The payload of an encoded key carries the version the
// dialect writes its files with, the verifier of a server rejects a
// payload of a version above its own.
func NewEncoder(dialect rdb.Dialect) *Encoder {
	return &Encoder{dialect: dialect}
}

// Encode returns the key of a scan that stores the record of a key of
// an RDB file. The tokens are the record of a key in the order of a
// file, the op code of the value, its key and the tokens of the value,
// an expiry timestamp of the record ahead of them. The time to live of
// the returned token is the one of the expiry in milliseconds from
// now, -1 when the record carries none; a payload of a DUMP carries no
// expiry of its own, the time to live of a key is the ttl argument of
// the RESTORE command.
//
// An op code the dialect does not take fails the encode with
// [rdb.ErrInvalidOpCode] or [rdb.ErrOpCodeTooNew], a version the
// dialect does not support with [rdb.ErrInvalidVersion]. The returned
// key and payload own their bytes, the tokens of the record may stay
// volatile.
func (e *Encoder) Encode(tokens ...rdb.Token) (Token, error) {
	ttl := int64(-1)
	if len(tokens) >= 2 && tokens[0].Kind() == rdb.KindOpCode {
		switch tokens[0].OpCode() {
		case rdb.RedisOpCodeExpirySeconds, rdb.RedisOpCodeExpiryMilli:
			if tokens[1].Kind() != rdb.KindExpiry {
				return Token{}, fmt.Errorf("dump: expected an expiry, got %s", tokens[1].Kind())
			}
			ttl = tokens[1].Time().UnixMilli() - time.Now().UnixMilli()
			tokens = tokens[2:]
		}
	}
	if len(tokens) < 3 {
		return Token{}, fmt.Errorf("dump: the record of a key carries no value")
	}
	if tokens[0].Kind() != rdb.KindOpCode {
		return Token{}, fmt.Errorf("dump: expected an op code, got %s", tokens[0].Kind())
	}
	if tokens[1].Kind() != rdb.KindString {
		return Token{}, fmt.Errorf("dump: expected a key name, got %s", tokens[1].Kind())
	}
	_, ver := e.dialect.Header()
	if _, maxVer := e.dialect.WithVersion(0).Header(); ver < 1 || ver > maxVer {
		return Token{}, fmt.Errorf("%w: %d is not a version of the %T dialect", rdb.ErrInvalidVersion, ver, e.dialect)
	}

	e.rec.Reset()
	enc := rdb.NewEncoder(&e.rec, e.dialect)
	for _, tok := range tokens {
		if err := enc.WriteToken(tok); err != nil {
			return Token{}, err
		}
	}
	// The payload of a DUMP is the record of the key without its
	// name: the op code of the value, the value, the version and the
	// checksum. The record encoder wrote the name the payload drops,
	// its framing tells where the value begins.
	encoded := e.rec.Bytes()
	key := []byte(tokens[1].String())
	klen := len(rdb.LenBytes(uint64(len(key))))
	payload := make([]byte, 0, 1+len(encoded))
	payload = append(payload, encoded[0])
	payload = append(payload, encoded[1+klen+len(key):]...)
	payload = binary.LittleEndian.AppendUint16(payload, uint16(ver))
	crc := crc64jones.New()
	crc.Write(payload)
	payload = crc.Sum(payload)
	return Token{
		Key:     key,
		Payload: payload,
		TTL:     ttl,
	}, nil
}

// Decoder decodes the keys of a scan to the records of an RDB file:
// the payload of the DUMP command decodes like the value of a file,
// the way the RESTORE command reads it back. The decoder reuses the
// buffers of the records it decodes, one Decoder serves a whole
// transfer.
type Decoder struct {
	dialect rdb.Dialect  // the dialect of the records, the version of the payload gates its op codes
	rec     bytes.Buffer // the record the payload and the key frame, reset by every decode
}

// NewDecoder constructs a Decoder decoding the records of the
// dialect. The version of a payload gates the op codes the records
// decode with, a payload of a version the dialect does not support
// fails the decode.
func NewDecoder(dialect rdb.Dialect) *Decoder {
	return &Decoder{dialect: dialect}
}

// Decode returns the tokens of the record of a key of a scan. They are
// the tokens of a record in the order of a file: the expiry timestamp
// of an expiring key, the op code of the value, its key and the tokens
// of the value. The time to live of the key becomes the expiry
// timestamp of the record, the way a file stores one. The returned
// tokens own their bytes.
//
// A version the dialect does not support fails the decode with
// [rdb.ErrUnsupportedVersion], a payload that lost a byte of its
// content with [ErrInvalidChecksum].
func (d *Decoder) Decode(t Token) ([]rdb.Token, error) {
	if len(t.Payload) < 11 {
		return nil, fmt.Errorf("dump: invalid payload of the key %q", t.Key)
	}
	// The checksum of a payload covers the type of the value, the
	// value and the version, the bytes the server verified when it
	// stored the key.
	body := t.Payload[:len(t.Payload)-8]
	var want [8]byte
	copy(want[:], t.Payload[len(t.Payload)-8:])
	crc := crc64jones.New()
	crc.Write(body)
	if !bytes.Equal(crc.Sum(nil), want[:]) {
		return nil, ErrInvalidChecksum
	}
	ver := int(binary.LittleEndian.Uint16(t.Payload[len(t.Payload)-10 : len(t.Payload)-8]))
	if _, maxVer := d.dialect.Header(); ver < 1 || ver > maxVer {
		return nil, fmt.Errorf("%w: %d is not a version of the %s dialect", rdb.ErrUnsupportedVersion, ver, d.dialect)
	}

	// The payload of a DUMP is the record of the key without its
	// name, the name of the token frames the record the decoder
	// reads. The version of the payload is behind the value, the
	// record carries the value alone.
	d.rec.Reset()
	d.rec.WriteByte(body[0])
	d.rec.Write(rdb.LenBytes(uint64(len(t.Key))))
	d.rec.Write(t.Key)
	d.rec.Write(body[1 : len(body)-2])
	dec := rdb.NewDecoder(&d.rec, d.dialect.WithVersion(ver))
	var toks []rdb.Token
	if t.TTL >= 0 {
		toks = append(toks,
			rdb.Op(rdb.RedisOpCodeExpiryMilli),
			rdb.Expiry(uint64(time.Now().UnixMilli()+t.TTL)),
		)
	}
	for {
		tok, err := dec.ReadToken()
		if errors.Is(err, io.EOF) {
			return toks, nil
		}
		if err != nil {
			return nil, err
		}
		toks = append(toks, tok.Clone())
	}
}
