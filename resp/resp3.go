package resp

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
)

// RESP3Encoder is a streaming encoder to raw RESP tokens.
// It is used to write the elements of a command or a reply one by one.
//
// The tokens are written into an internal buffer, [Encoder.Flush] writes
// it to the underlying writer.
type RESP3Encoder struct {
	w *bufio.Writer

	buf []byte
}

// NewRESP3Encoder constructs a new streaming encoder writing to w.
// An [io.Writer] that is already a [bufio.Writer] is used directly,
// every other writer is wrapped in one.
func NewRESP3Encoder(w io.Writer) *RESP3Encoder {
	ww, ok := w.(*bufio.Writer)
	if !ok {
		ww = bufio.NewWriter(w)
	}
	return &RESP3Encoder{w: ww}
}

// WriteToken writes the wire representation of t.
// It panics if the token kind is not a RESP kind.
func (e *RESP3Encoder) WriteToken(t Token) error {
	if t.raw != nil {
		if _, err := e.w.Write(t.raw.b); err != nil {
			return err
		}
		if t.stream != nil {
			return copyStream(e.w, t)
		}
		return nil
	}

	k := t.Kind()
	e.buf = e.buf[:0]
	switch k {
	case '+', '-', '(':
		e.buf = append(e.buf, t.str...)
	case '$', ';', '!', '=':
		e.buf = append(e.buf, t.str[0])
		e.buf = strconv.AppendInt(e.buf, int64(t.num), 10)
		if t.stream == nil && int64(t.num) != -1 {
			e.buf = append(e.buf, '\r', '\n')
			e.buf = append(e.buf, t.str[1:]...)
		}
	case ':', '*', '%', '~', '|', '>':
		e.buf = append(e.buf, t.str[0])
		e.buf = strconv.AppendInt(e.buf, int64(t.num), 10)
	case ',':
		e.buf = append(e.buf, t.str[0])
		e.buf = strconv.AppendFloat(e.buf, t.Float(), 'f', -1, 64)
	default:
		return fmt.Errorf("resp3: invalid RESP token kind: %s", t.Kind().String())
	}
	e.buf = append(e.buf, '\r', '\n')
	_, err := e.w.Write(e.buf)
	if t.stream != nil {
		if err != nil {
			return err
		}
		return copyStream(e.w, t)
	}
	return err
}

// Flush writes any buffered data to the underlying writer.
func (e *RESP3Encoder) Flush() error {
	return e.w.Flush()
}

// RESP3Decoder is a streaming decoder for raw RESP tokens.
// It is used to read the elements of a reply or a command one by one.
// The elements of an unbound aggregate are read with [Token.Iter].
type RESP3Decoder struct {
	r *AdvanceReader

	buf *bytes.Buffer
}

// NewRESP3Decoder constructs a new streaming decoder reading from r.
func NewRESP3Decoder(r io.Reader) *RESP3Decoder {
	return &RESP3Decoder{r: NewAdvanceReader(r), buf: bytes.NewBuffer(nil)}
}

// ReadToken reads the next [Token] of the stream.
// It returns io.EOF when the stream ends at a token boundary.
// The returned token is only valid until the next call to any method on
// that Decoder, see [Token.Clone]. The payload of a large blob string
// or blob error and of an EOF-marked blob string is read from the
// reader of the returned token instead of the buffer, a payload that
// was not consumed is skipped with the next call, [Token.Bytes] and
// [Token.String] consume it. The payload of an EOF-marked blob string
// ends at its marker, the marker is consumed with it. An unbound blob
// string is returned as the [UnboundString] token, its string parts
// are read from the decoder with [Token.Iter].
func (d *RESP3Decoder) ReadToken() (Token, error) {
	if err := d.r.Advance(TermByte(eol)); err != nil {
		return Token{}, err
	}
	d.buf.Reset()
	if _, err := d.buf.ReadFrom(d.r); err != nil {
		return Token{}, err
	}
	buf := d.buf.Bytes()
	if len(buf) < 3 {
		return Token{}, fmt.Errorf("resp3: invalid line %q", buf)
	}

	if len(buf) <= 9 {
		switch string(buf) {
		case "_\r\n":
			return Null, nil
		case "#t\r\n":
			return True, nil
		case "#f\r\n":
			return False, nil

		case ".\r\n":
			return End, nil

		case "+\r\n":
			return zeroString, nil
		case ":0\r\n":
			return zeroInt, nil
		case ",0\r\n":
			return zeroFloat, nil

		case "*-1\r\n":
			return zeroArray, nil
		case "$-1\r\n":
			return zeroBytes, nil
		case ";0\r\n":
			return zeroStringPart, nil

		case "*?\r\n":
			return UnboundArray, nil
		case "~?\r\n":
			return UnboundSet, nil
		case "%?\r\n":
			return UnboundMap, nil
		case "|?\r\n":
			return UnboundAttr, nil
		case "$?\r\n":
			return UnboundString, nil

		case ",inf\r\n":
			return pinfDouble, nil
		case ",-inf\r\n":
			return ninfDouble, nil
		case ",nan\r\n":
			return nanDouble, nil
		}
	}

	switch k := buf[0]; k {
	case '+', '-', ':', '(',
		',', '#', '_', '*',
		'%', '~', '|', '.', '>':
		return Token{raw: &raw{b: buf}}, nil
	case '$', ';', '!', '=':
		if len(buf) > 7 && bytes.Equal(buf[:5], []byte("$EOF:")) {
			raw := &raw{b: bytes.Clone(buf)}
			d.r.Advance(TermByte(raw.b[5 : len(raw.b)-2]))
			return Token{raw: raw, stream: d.r}, nil
		}
		l, err := strconv.ParseInt(string(buf[1:len(buf)-2]), 10, 64)
		if err != nil {
			return Token{}, err
		}
		d.r.Advance(TermLen(int(l) + 2))
		return Token{raw: &raw{b: bytes.Clone(buf)}, num: uint64(l), stream: d.r}, nil
	}

	return Token{}, fmt.Errorf("resp3: invalid token %q", string(buf))
}

// Handoff returns the bytes the decoder has read from its reader but
// not consumed yet, the content of its buffer, as an [io.Reader]. A
// successor decoder reads them with an [io.MultiReader] over it and
// the reader of the connection, the decoder itself must not be used
// to decode afterwards. It hands the input of a session over when the
// protocol of a connection changes.
func (d *RESP3Decoder) Handoff() *AdvanceReader {
	return d.r
}
