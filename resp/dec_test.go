package resp

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

// chunkReader reads from r one byte at a time, the reads of the payload
// split the marker the way a slow server delivers it.
type chunkReader struct {
	r io.Reader
}

func (c *chunkReader) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	return c.r.Read(b[:1])
}

func Test_dropLastNReader(t *testing.T) {
	type args struct {
		r io.Reader
		n int
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "base",
			args: args{
				r: strings.NewReader("hello world\r\n"),
				n: 2,
			},
			want: []byte("hello world"),
		},
		{
			name: "full",
			args: args{
				r: strings.NewReader("0123456789"),
				n: 10,
			},
			want: []byte(""),
		},
		{
			name: "delayed err",
			args: args{
				r: &chunkReader{r: strings.NewReader("hello")},
				n: 3,
			},
			want: []byte("he"),
		},
		{
			name: "early err",
			args: args{
				r: strings.NewReader("hello"),
				n: 10,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := dropLastNReader(tt.args.r, tt.args.n)
			got, err := io.ReadAll(s)
			if (err != nil) != tt.wantErr {
				t.Fatalf("NewEOFData() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !bytes.Equal(got, tt.want) {
				t.Errorf("NewEOFData() = %s, want %s", got, tt.want)
			}
		})
	}
}

// failingReader fails every read with its error.
type failingReader struct {
	err error
}

func (r *failingReader) Read([]byte) (int, error) {
	return 0, r.err
}

func TestNewAdvanceReader(t *testing.T) {
	type args struct {
		r    io.Reader
		term Term
	}
	tests := []struct {
		name    string
		args    args
		before  func(*AdvanceReader)
		want    []byte
		wantErr bool
	}{
		{
			name: "base",
			args: args{
				r:    strings.NewReader("hello\r\n"),
				term: TermByte("\r\n"),
			},
			want: []byte("hello\r\n"),
		},
		{
			name: "base",
			args: args{
				r:    strings.NewReader("hello\r\nbye"),
				term: TermByte("\r\n"),
			},
			want: []byte("hello\r\n"),
		},
		{
			name: "unexpected end",
			args: args{
				r:    strings.NewReader("hel"),
				term: TermByte("\r\n"),
			},
			want:    []byte("he"),
			wantErr: true,
		},
		{
			name: "small reads",
			args: args{
				r:    &chunkReader{r: strings.NewReader("hello\r\nbye")},
				term: TermByte("\r\n"),
			},
			want: []byte("hello\r\n"),
		},
		{
			name: "next term",
			args: args{
				r:    strings.NewReader("hello\r\nbye\r\n"),
				term: TermByte("\r\n"),
			},
			before: func(ar *AdvanceReader) {
				_ = ar.Advance(TermByte("\r\n"))
			},
			want: []byte("bye\r\n"),
		},
		{
			name: "next two term",
			args: args{
				r:    strings.NewReader("hello\r\nterm\r\nbye\r\n"),
				term: TermByte("\r\n"),
			},
			before: func(ar *AdvanceReader) {
				_ = ar.Advance(TermByte("\r\n"))
				_ = ar.Advance(TermByte("\r\n"))
			},
			want: []byte("bye\r\n"),
		},
		{
			name: "next len",
			args: args{
				r:    strings.NewReader("hello\r\n1234567890"),
				term: TermByte("\r\n"),
			},
			before: func(ar *AdvanceReader) {
				_ = ar.Advance(TermLen(4))
			},
			want: []byte("1234"),
		},
		{
			name: "next two len",
			args: args{
				r:    strings.NewReader("hello\r\n1234567890"),
				term: TermByte("\r\n"),
			},
			before: func(ar *AdvanceReader) {
				_ = ar.Advance(TermLen(4))
				_ = ar.Advance(TermLen(4))
			},
			want: []byte("5678"),
		},
		{
			name: "long term",
			args: args{
				r:    &chunkReader{r: strings.NewReader(strings.Repeat("a", 600) + strings.Repeat("x", 600) + "tail")},
				term: TermByte(strings.Repeat("x", 600)),
			},
			want: []byte(strings.Repeat("a", 600) + strings.Repeat("x", 600)),
		},
		{
			name: "long len",
			args: args{
				r:    strings.NewReader("hello\r\n" + strings.Repeat("x", 2000)),
				term: TermByte(eol),
			},
			before: func(ar *AdvanceReader) {
				_ = ar.Advance(TermLen(2000))
			},
			want: []byte(strings.Repeat("x", 2000)),
		},
		{
			name: "next long len",
			args: args{
				r:    strings.NewReader("hello\r\n" + strings.Repeat("x", 600) + "next\r\n"),
				term: TermByte(eol),
			},
			before: func(ar *AdvanceReader) {
				_ = ar.Advance(TermLen(600))
				_ = ar.Advance(TermByte(eol))
			},
			want: []byte("next\r\n"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ar := NewAdvanceReader(tt.args.r)
			if err := ar.Advance(tt.args.term); err != nil {
				t.Fatalf("Advance() error = %v, want nil", err)
			}
			if tt.before != nil {
				tt.before(ar)
			}
			got, err := io.ReadAll(ar)
			if (err != nil) != tt.wantErr {
				t.Fatalf("NewAdvanceReader() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !bytes.Equal(got, tt.want) {
				t.Errorf("NewAdvanceReader() = %q, want %q", got, tt.want)
			}
		})
	}

	// An unbound terminator arms no segment, Advance panics on it.
	t.Run("unbound term", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Error("Advance() with an unbound term did not panic")
			}
		}()
		ar := NewAdvanceReader(strings.NewReader("hello\r\n"))
		ar.Advance(TermUnbound(End))
	})
}

func TestAdvanceReader_Advance(t *testing.T) {
	type args struct {
		term Term
	}
	tests := []struct {
		name    string
		reader  func() *AdvanceReader
		args    args
		wantErr bool
	}{
		{
			name: "empty",
			reader: func() *AdvanceReader {
				ar := NewAdvanceReader(strings.NewReader(""))
				ar.Advance(TermByte(eol))
				return ar
			},
			args:    args{term: TermByte(eol)},
			wantErr: true,
		},
		{
			name: "eof",
			reader: func() *AdvanceReader {
				ar := NewAdvanceReader(strings.NewReader("hello world\r\n"))
				ar.Advance(TermByte(eol))
				return ar
			},
			args:    args{term: TermByte(eol)},
			wantErr: true,
		},
		{
			name: "next term exists",
			reader: func() *AdvanceReader {
				ar := NewAdvanceReader(strings.NewReader("hello world\r\nnext\r\n"))
				ar.Advance(TermByte(eol))
				return ar
			},
			args:    args{term: TermByte(eol)},
			wantErr: false,
		},
		{
			name: "next term missing",
			reader: func() *AdvanceReader {
				ar := NewAdvanceReader(strings.NewReader("hello world\r\nnext"))
				ar.Advance(TermByte(eol))
				return ar
			},
			args:    args{term: TermByte(eol)},
			wantErr: false,
		},
		{
			name: "next term exists",
			reader: func() *AdvanceReader {
				ar := NewAdvanceReader(strings.NewReader("hello world\r\nnext\r\n"))
				ar.Advance(TermByte(eol))
				ar.Advance(TermByte(eol))
				return ar
			},
			args:    args{term: TermByte(eol)},
			wantErr: true,
		},
		{
			name: "next term exists",
			reader: func() *AdvanceReader {
				ar := NewAdvanceReader(strings.NewReader("hello world\r\nnext\r\nbye"))
				ar.Advance(TermByte(eol))
				ar.Advance(TermByte(eol))
				return ar
			},
			args:    args{term: TermByte(eol)},
			wantErr: false,
		},
		{
			name: "len eof",
			reader: func() *AdvanceReader {
				ar := NewAdvanceReader(strings.NewReader("\r\n"))
				ar.Advance(TermByte(eol))
				return ar
			},
			args:    args{term: TermLen(2)},
			wantErr: true,
		},
		{
			name: "len unexpected eof",
			reader: func() *AdvanceReader {
				ar := NewAdvanceReader(strings.NewReader(""))
				ar.Advance(TermByte(eol))
				return ar
			},
			args:    args{term: TermLen(2)},
			wantErr: true,
		},
		{
			name: "len second short",
			reader: func() *AdvanceReader {
				ar := NewAdvanceReader(strings.NewReader("\r\n1234"))
				ar.Advance(TermByte(eol))
				ar.Advance(TermLen(4))
				return ar
			},
			args:    args{term: TermLen(2)},
			wantErr: true,
		},
		{
			name: "len second long",
			reader: func() *AdvanceReader {
				ar := NewAdvanceReader(strings.NewReader("\r\n123456"))
				ar.Advance(TermByte(eol))
				ar.Advance(TermLen(4))
				return ar
			},
			args:    args{term: TermLen(2)},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.reader().Advance(tt.args.term)
			if (err != nil) != tt.wantErr {
				t.Errorf("AdvanceReader.Advance() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
