package resp

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

// Decoder reads the tokens of a RESP stream. A decoder of a session
// hands its unread input over with [Decoder.Handoff] when the protocol
// of the connection changes.
type Decoder interface {
	ReadToken() (Token, error)
	Handoff() *AdvanceReader
}

// Encoder writes the tokens of a RESP stream, the bytes are buffered
// until [Encoder.Flush] writes them to the connection.
type Encoder interface {
	WriteToken(t Token) error
	Flush() error
}

// protocol adapts a session to one of the two protocols: it reads the
// commands and writes the replies with the decoder and encoder of the
// protocol version the session runs.
type protocol struct {
	d Decoder
	e Encoder
}

// newProtocol2 constructs the adapter for a RESP2 session reading
// from r and writing to the buffered writer w.
func newProtocol2(r io.Reader, w io.Writer) *protocol {
	return &protocol{d: NewRESP2Decoder(r), e: NewRESP2Encoder(w)}
}

// newProtocol3 constructs the adapter for a RESP3 session reading
// from r and writing to the buffered writer w.
func newProtocol3(r io.Reader, w io.Writer) *protocol {
	return &protocol{d: NewRESP3Decoder(r), e: NewRESP3Encoder(w)}
}

// readCommand reads the array of bulk strings of one command.
func (p *protocol) readCommand() ([][]byte, error) {
	t, err := p.d.ReadToken()
	if err != nil {
		return nil, err
	}
	if t.Kind() != KindArray {
		return nil, fmt.Errorf("resp: expected array, got %s", t.Kind())
	}
	if t.IsUnbound() {
		return nil, errors.New("resp: unbound array is not a command")
	}
	n := t.Int()
	if n < 0 {
		return nil, errors.New("resp: null array is not a command")
	}
	if n == 0 {
		return nil, errors.New("resp: empty array is not a command")
	}
	args := make([][]byte, n)
	for i := range args {
		t, err := p.d.ReadToken()
		if err != nil {
			return nil, err
		}
		if t.Kind() != KindBlobString {
			return nil, fmt.Errorf("resp: expected bulk string, got %s", t.Kind())
		}
		args[i] = bytes.Clone(t.Bytes())
	}
	return args, nil
}

func (p *protocol) encoder() Encoder {
	return p.e
}

func (p *protocol) flush() error {
	return p.e.Flush()
}

func (p *protocol) handoff() *AdvanceReader {
	return p.d.Handoff()
}
