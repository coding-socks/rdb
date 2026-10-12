package resp

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
)

// RESP2Encoder is a streaming encoder to raw RESP tokens.
// It is used to write the elements of a command or a reply one by one.
//
// The tokens are written into an internal buffer, [Encoder.Flush] writes
// it to the underlying writer.
type RESP2Encoder struct {
	w *bufio.Writer

	buf []byte
}

// NewRESP2Encoder constructs a new streaming encoder writing to w.
// An [io.Writer] that is already a [bufio.Writer] is used directly,
// every other writer is wrapped in one.
func NewRESP2Encoder(w io.Writer) *RESP2Encoder {
	ww, ok := w.(*bufio.Writer)
	if !ok {
		ww = bufio.NewWriter(w)
	}
	return &RESP2Encoder{w: ww}
}

// WriteToken writes the wire representation of t.
// It panics if the token kind is not a RESP kind.
func (e *RESP2Encoder) WriteToken(t Token) error {
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
	case '+', '-':
		e.buf = append(e.buf, t.str...)
	case ':', '*':
		e.buf = append(e.buf, t.str[0])
		e.buf = strconv.AppendInt(e.buf, int64(t.num), 10)
	case '$':
		e.buf = append(e.buf, t.str[0])
		e.buf = strconv.AppendInt(e.buf, int64(t.num), 10)
		if t.stream == nil && int64(t.num) != -1 {
			e.buf = append(e.buf, '\r', '\n')
			e.buf = append(e.buf, t.str[1:]...)
		}
	default:
		return fmt.Errorf("resp2: invalid RESP token kind: %s", t.Kind().String())
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
func (e *RESP2Encoder) Flush() error {
	return e.w.Flush()
}

// RESP2Decoder is a streaming decoder for raw RESP tokens.
// It is used to read the elements of a reply or a command one by one.
type RESP2Decoder struct {
	r *AdvanceReader

	buf *bytes.Buffer
}

// NewRESP2Decoder constructs a new streaming decoder reading from r.
func NewRESP2Decoder(r io.Reader) *RESP2Decoder {
	return &RESP2Decoder{r: NewAdvanceReader(r), buf: bytes.NewBuffer(nil)}
}

// ReadToken reads the next [Token] of the stream.
// It returns io.EOF when the stream ends at a token boundary.
// The returned token is only valid until the next call to any method on
// that Decoder, see [Token.Clone].
func (d *RESP2Decoder) ReadToken() (Token, error) {
	if err := d.r.Advance(TermByte(eol)); err != nil {
		return Token{}, err
	}
	d.buf.Reset()
	if _, err := d.buf.ReadFrom(d.r); err != nil {
		return Token{}, err
	}
	buf := d.buf.Bytes()
	if len(buf) < 3 {
		return Token{}, fmt.Errorf("resp2: invalid line %q", buf)
	}

	if len(buf) <= 5 {
		switch string(buf) {
		case "+\r\n":
			return zeroString, nil
		case ":0\r\n":
			return zeroInt, nil

		case "*-1\r\n":
			return zeroArray, nil
		case "$-1\r\n":
			return zeroBytes, nil
		}
	}

	switch k := buf[0]; k {
	case '+', '-', ':', '*':
		return Token{raw: &raw{buf}}, nil
	case '$':
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

	return Token{}, fmt.Errorf("resp2: invalid token %q", string(buf))
}

// Handoff returns the bytes the decoder has read from its reader but
// not consumed yet, the content of its buffer, as an [io.Reader]. A
// successor decoder reads them with an [io.MultiReader] over it and
// the reader of the connection, the decoder itself must not be used
// to decode afterwards. It hands the input of a session over when the
// protocol of a connection changes.
func (d *RESP2Decoder) Handoff() *AdvanceReader {
	return d.r
}
