package resp

import (
	"bytes"
	"errors"
	"io"
)

var eol = []byte{'\r', '\n'}

// Term is the terminator of a segment of an [AdvanceReader]: a
// [TermByte] terminator or the length of a [TermLen] segment.
// [TermUnbound] marks the unbound tokens of the protocol, it arms no
// segment, [AdvanceReader.Advance] rejects it.
type Term interface {
	isTerm()
}

// TermByte is a terminator of a segment, the segment ends with its
// first occurrence in the stream.
type TermByte []byte

func (TermByte) isTerm() {}

// TermLen is the length of a segment, the segment ends after its n
// bytes.
type TermLen int

func (TermLen) isTerm() {}

// TermUnbound marks a token of an unknown length: an aggregate whose
// elements are terminated by [KindEnd] or a blob string whose string
// parts are terminated by a [KindStringPart] of zero length.
type TermUnbound Token

func (TermUnbound) isTerm() {}

// AdvanceReader reads a stream as a sequence of segments: a segment
// is the bytes up to and including a terminator or of a fixed length,
// the reads of a segment return io.EOF at its end and every segment,
// the first included, is armed with Advance. The end of the reader is
// not a segment end, Advance reports it with false.
type AdvanceReader struct {
	r    io.Reader
	buf  []byte // the read-ahead window
	pos  int    // the bytes of the window delivered so far
	term []byte // the terminator of the armed segment, nil for a length segment
	n    int    // the bytes of the armed segment still to deliver
	err  error  // io.EOF once the segment is served, ErrUnexpectedEOF or a stream error otherwise
}

// NewAdvanceReader constructs a reader reading from r without an
// armed segment, the first one is armed with Advance.
func NewAdvanceReader(r io.Reader) *AdvanceReader {
	if r, ok := r.(*AdvanceReader); ok {
		return r
	}
	return &AdvanceReader{
		r:   r,
		buf: make([]byte, 0, 512),
	}
}

// Read delivers the bytes of the armed segment, a terminator-bounded
// segment is delivered with its terminator. It returns io.EOF when the
// segment is served whole: a terminator-bounded segment behind its
// terminator, a segment of a fixed length after its n bytes. A stream
// that ends before the segment is served whole fails the reads with
// io.ErrUnexpectedEOF. A reader without an armed segment reports
// io.EOF.
func (r *AdvanceReader) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	if r.err == nil {
		if r.term != nil {
			return r.readTerm(b)
		}
		return r.readLen(b)
	}
	// the read-ahead of the next segment is served behind the end
	// of the served one, a handoff drains it this way
	if a := min(len(r.buf)-r.pos, len(b)); a > 0 {
		n := copy(b, r.buf[r.pos:r.pos+a])
		r.pos += n
		return n, nil
	}
	return 0, r.err
}

// readTerm delivers the bytes of the segment up to its terminator,
// the terminator is served with the data through readLen.
func (r *AdvanceReader) readTerm(b []byte) (int, error) {
	for {
		if i := bytes.Index(r.buf[r.pos:], r.term); i >= 0 {
			// the terminator ends the segment, its bytes are
			// served with the data of the segment
			r.term, r.n = nil, i+len(r.term)
			return r.readLen(b)
		}
		// a partial terminator at the tail of the window is
		// withheld, the terminator may span the reads
		if a := max(0, len(r.buf)-r.pos-(len(r.term)-1)); a > 0 {
			n := copy(b, r.buf[r.pos:r.pos+a])
			r.pos += n
			return n, nil
		}
		if !r.load() {
			if r.err == nil {
				r.err = io.ErrUnexpectedEOF
			}
			return 0, r.err
		}
	}
}

// readLen delivers the next bytes of the segment of a fixed length,
// the segment ends when its n bytes are served.
func (r *AdvanceReader) readLen(b []byte) (int, error) {
	for {
		if r.n == 0 {
			r.err = io.EOF
			return 0, io.EOF
		}
		if a := min(len(r.buf)-r.pos, r.n); a > 0 {
			n := copy(b, r.buf[r.pos:r.pos+a])
			r.pos += n
			r.n -= n
			return n, nil
		}
		// the window is drained, a length segment needs no
		// lookahead, the remaining bytes are read straight from
		// the reader capped at the end of the segment
		m, err := r.r.Read(b[:min(len(b), r.n)])
		r.n -= m
		if m > 0 {
			return m, nil
		}
		if err == nil {
			continue
		}
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		r.err = err
		return 0, err
	}
}

// Advance ends the current segment and arms the next one with term:
// the rest of the segment and its terminator are discarded, the reads
// deliver the bytes up to the next occurrence of a [TermByte]
// terminator or the next n bytes of a [TermLen] segment. It reports
// whether the reader has input for the new segment, the end of the
// reader is reported with false. The [TermByte] terminator is not
// copied, it must stay valid until the next Advance.
// It panics if the term is a [TermUnbound].
func (r *AdvanceReader) Advance(term Term) error {
	if err := r.discard(); err != nil {
		return err
	}
	switch t := term.(type) {
	case TermByte:
		r.term, r.n = t, 0
	case TermLen:
		r.term, r.n = nil, int(t)
	default:
		panic("resp: cannot advance to an unbound terminator")
	}
	r.err = nil
	if cap(r.buf) < len(r.term) {
		buf := make([]byte, len(r.buf), max(512, len(r.term)))
		copy(buf, r.buf)
		r.buf = buf
	}
	return r.probe()
}

// discard consumes the rest of the armed segment, its terminator
// included. It reports whether the segment ended whole.
func (r *AdvanceReader) discard() error {
	if r.err != nil {
		if errors.Is(r.err, io.EOF) {
			return nil
		}
		return r.err
	}
	_, err := io.Copy(io.Discard, r)
	if err != nil {
		// a truncated segment or a stream error fails the advance
		r.err = err
		return r.err
	}
	// io.Copy consumed the io.EOF of the segment, the reads of the
	// served segment keep reporting it
	r.err = io.EOF
	return nil
}

// probe loads the first bytes of the armed segment: it reports
// whether the reader has input for it, the end of the reader sets the
// error of the reader to io.EOF.
func (r *AdvanceReader) probe() error {
	if r.pos < len(r.buf) {
		return nil
	}
	if !r.load() {
		if r.err == nil {
			r.err = io.EOF
		}
		return r.err
	}
	return nil
}

// load pulls the next bytes of the reader into the window, the
// delivered bytes at its front are dropped when it is full. It
// reports whether bytes arrived, an end of the reader is not an
// error, the reads report it as io.ErrUnexpectedEOF.
func (r *AdvanceReader) load() bool {
	if r.pos > 0 && len(r.buf) == cap(r.buf) {
		// the window is compacted instead of grown, the
		// terminator always fits it since Advance
		r.buf = r.buf[:copy(r.buf, r.buf[r.pos:])]
		r.pos = 0
	}
	m, err := r.r.Read(r.buf[len(r.buf):cap(r.buf)])
	r.buf = r.buf[:len(r.buf)+m]
	switch {
	case m > 0:
		return true
	case err == nil:
		return true
	case errors.Is(err, io.EOF):
		return false
	default:
		r.err = err
		return false
	}
}

// Replay reads the bytes of a stream as many times as its clones read
// them: the stream is drained once and the reads deliver the captured
// bytes, the clones share the buffer behind an independent position.
// An error of the drained stream is replayed behind its bytes, only a
// drained stream without one reports io.EOF.
type Replay struct {
	pos int
	buf []byte
	err error
}

// ReplayReader drains r into a replay, a replay is cloned with
// [Replay.Clone].
func ReplayReader(r io.Reader) *Replay {
	if rr, ok := r.(*Replay); ok {
		return rr.Clone()
	}
	var buf bytes.Buffer
	_, err := buf.ReadFrom(r)
	return &Replay{
		buf: buf.Bytes(),
		err: err,
	}
}

func (r *Replay) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.buf) {
		if r.err == nil {
			r.err = io.EOF
		}
		return 0, r.err
	}
	n = copy(p, r.buf[r.pos:])
	r.pos += n
	if r.pos == len(r.buf) {
		if r.err == nil {
			return n, io.EOF
		}
		return n, nil
	}
	return n, nil
}

func (r *Replay) Clone() *Replay {
	return &Replay{
		buf: r.buf,
		err: r.err,
	}
}

// dropLastNReader returns a reader over the bytes of r without the
// last n bytes: the reads withhold the bytes of the tail and swallow
// them at the end of the stream. A stream that is not longer than n
// delivers nothing.
func dropLastNReader(r io.Reader, n int) io.Reader {
	return &dropLastN{r: r, n: n, buf: make([]byte, 0, max(512, n+1))}
}

// dropLastN reads the bytes of a reader without the last n bytes of
// its stream: the reads withhold the bytes of the tail and swallow
// them at the end of the stream.
type dropLastN struct {
	r   io.Reader
	n   int
	buf []byte // the bytes read but not delivered, the tail stays withheld
	err error
}

func (s *dropLastN) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	for len(s.buf) <= s.n && s.err == nil {
		m, err := s.r.Read(s.buf[len(s.buf):cap(s.buf)])
		s.buf = s.buf[:len(s.buf)+m]
		s.err = err
	}
	if len(s.buf) < s.n {
		if errors.Is(s.err, io.EOF) {
			s.err = io.ErrUnexpectedEOF
		}
		return 0, s.err
	}
	m := copy(b, s.buf[:len(s.buf)-s.n])
	s.buf = s.buf[:copy(s.buf, s.buf[m:])]
	if len(s.buf) > s.n {
		return m, nil
	}
	return m, s.err
}
