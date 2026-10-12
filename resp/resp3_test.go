package resp

import (
	"bytes"
	"errors"
	"io"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestRESP3Decoder_ReadToken2(t *testing.T) {
	type Want struct {
		kind  Kind
		value any
	}
	tests := []struct {
		name string
		data string
		want Want
	}{
		{
			name: "blob string",
			data: "$11\r\nhello world\r\n",
			want: Want{kind: KindBlobString, value: []byte("hello world")},
		},
		{
			name: "empty blob string",
			data: "$0\r\n\r\n",
			want: Want{kind: KindBlobString, value: []byte("")},
		},
		{
			name: "simple string",
			data: "+hello world\r\n",
			want: Want{kind: KindString, value: "hello world"},
		},
		{
			name: "error",
			data: "-ERR this is the error description\r\n",
			want: Want{kind: KindError, value: errors.New("ERR this is the error description")},
		},
		{
			name: "number",
			data: ":1234\r\n",
			want: Want{kind: KindNumber, value: int64(1234)},
		},
		{
			name: "double",
			data: ",1.23\r\n",
			want: Want{kind: KindDouble, value: 1.23},
		},
		{
			name: "double of an integer",
			data: ",123\r\n",
			want: Want{kind: KindDouble, value: float64(123)},
		},
		{
			name: "double inf",
			data: ",inf\r\n",
			want: Want{kind: KindDouble, value: math.Inf(1)},
		},
		{
			name: "double -inf",
			data: ",-inf\r\n",
			want: Want{kind: KindDouble, value: math.Inf(-1)},
		},
		{
			name: "double nan",
			data: ",nan\r\n",
			want: Want{kind: KindDouble, value: math.NaN()},
		},
		{
			name: "boolean true",
			data: "#t\r\n",
			want: Want{kind: KindBoolean, value: true},
		},
		{
			name: "boolean false",
			data: "#f\r\n",
			want: Want{kind: KindBoolean, value: false},
		},
		{
			name: "blob error",
			data: "!21\r\nSYNTAX invalid syntax\r\n",
			want: Want{kind: KindBlobError, value: errors.New("SYNTAX invalid syntax")},
		},
		{
			name: "verbatim string",
			data: "=15\r\ntxt:Some string\r\n",
			want: Want{kind: KindVerbatimString, value: VerbatimStr{Format: "txt", String: "Some string"}},
		},
		{
			name: "big number",
			data: "(3492890328409238509324850943850943825024385\r\n",
			want: Want{kind: KindBigNumber, value: mustParseBig("3492890328409238509324850943850943825024385")},
		},
		{
			name: "array",
			data: "*3\r\n",
			want: Want{kind: KindArray, value: int64(3)},
		},
		{
			name: "map",
			data: "%2\r\n",
			want: Want{kind: KindMap, value: int64(2)},
		},
		{
			name: "set",
			data: "~5\r\n",
			want: Want{kind: KindSet, value: int64(5)},
		},
		{
			name: "attribute",
			data: "|1\r\n",
			want: Want{kind: KindAttribute, value: int64(1)},
		},
		{
			name: "push",
			data: ">3\r\n",
			want: Want{kind: KindPush, value: int64(3)},
		},
		{
			name: "unbound blob string",
			data: "$?\r\n",
			want: Want{kind: KindBlobString, value: TermUnbound(zeroStringPart)},
		},
		{
			name: "string part",
			data: ";4\r\nHell\r\n",
			want: Want{kind: KindStringPart, value: []byte("Hell")},
		},
		{
			name: "end string part",
			data: ";0\r\n",
			want: Want{kind: KindStringPart, value: []byte(nil)},
		},
		{
			name: "unbound map",
			data: "%?\r\n",
			want: Want{kind: KindMap, value: TermUnbound(End)},
		},
		{
			name: "null",
			data: "_\r\n",
			want: Want{kind: KindNull, value: nil},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			e := NewRESP3Encoder(&buf)
			d := NewRESP3Decoder(strings.NewReader(tt.data))
			got, err := d.ReadToken()
			if err != nil {
				t.Fatalf("ReadToken() got err = %v", err)
			}
			if want := tt.want.kind; got.Kind() != want {
				t.Errorf("ReadToken() got = %v, want = %v", got.Kind(), want)
			}
			switch tt.want.kind {
			default:
				val := got.Val()
				if want := tt.want.value; !reflect.DeepEqual(val, want) {
					t.Errorf("ReadValue() got = %[1]T %[1]s, want = %[2]T %[2]s", val, want)
				}
			case KindDouble:
				val := got.Val()
				if want := tt.want.value; !reflect.DeepEqual(val, want) && !bothNaN(got.Kind(), val, want) {
					t.Errorf("ReadValue() got = %v, want = %v", val, want)
				}
			}
			e.WriteToken(got)
			e.Flush()
			if got, want := buf.String(), tt.data; got != want {
				t.Fatalf("ReadToken() got = %v, want %v", got, want)
			}
		})
	}
}

func TestRESP3Decoder_ReadToken(t *testing.T) {
	data := "$11\r\nhello world\r\n" +
		"$0\r\n\r\n" +
		"+hello world\r\n" +
		"-ERR this is the error description\r\n" +
		":1234\r\n" +
		",1.23\r\n" +
		",123\r\n" +
		",inf\r\n" +
		",-inf\r\n" +
		",nan\r\n" +
		"#t\r\n" +
		"#f\r\n" +
		"!21\r\nSYNTAX invalid syntax\r\n" +
		"=15\r\ntxt:Some string\r\n" +
		"(3492890328409238509324850943850943825024385\r\n" +
		"*3\r\n:1\r\n:2\r\n:3\r\n" +
		"%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n" +
		"~5\r\n+orange\r\n+apple\r\n#t\r\n:100\r\n:999\r\n" +
		"|1\r\n+key-popularity\r\n%2\r\n$1\r\na\r\n,0.1923\r\n$1\r\nb\r\n,0.0012\r\n" +
		">3\r\n+message\r\n+somechannel\r\n+this is the message\r\n" +
		"$?\r\n;4\r\nHell\r\n;5\r\no wor\r\n;2\r\nld\r\n;0\r\n" +
		"%?\r\n+a\r\n:1\r\n+b\r\n:2\r\n.\r\n" +
		"_\r\n"
	wants := []struct {
		kind  Kind
		value any
	}{
		{kind: KindBlobString, value: []byte("hello world")},
		{kind: KindBlobString, value: []byte("")},
		{kind: KindString, value: "hello world"},
		{kind: KindError, value: errors.New("ERR this is the error description")},
		{kind: KindNumber, value: int64(1234)},
		{kind: KindDouble, value: 1.23},
		{kind: KindDouble, value: float64(123)},
		{kind: KindDouble, value: math.Inf(1)},
		{kind: KindDouble, value: math.Inf(-1)},
		{kind: KindDouble, value: math.NaN()},
		{kind: KindBoolean, value: true},
		{kind: KindBoolean, value: false},
		{kind: KindBlobError, value: errors.New("SYNTAX invalid syntax")},
		{kind: KindVerbatimString, value: VerbatimStr{Format: "txt", String: "Some string"}},
		{kind: KindBigNumber, value: mustParseBig("3492890328409238509324850943850943825024385")},

		{kind: KindArray, value: int64(3)},
		{kind: KindNumber, value: int64(1)},
		{kind: KindNumber, value: int64(2)},
		{kind: KindNumber, value: int64(3)},

		{kind: KindMap, value: int64(2)},
		{kind: KindString, value: "first"},
		{kind: KindNumber, value: int64(1)},
		{kind: KindString, value: "second"},
		{kind: KindNumber, value: int64(2)},

		{kind: KindSet, value: int64(5)},
		{kind: KindString, value: "orange"},
		{kind: KindString, value: "apple"},
		{kind: KindBoolean, value: true},
		{kind: KindNumber, value: int64(100)},
		{kind: KindNumber, value: int64(999)},

		{kind: KindAttribute, value: int64(1)},
		{kind: KindString, value: "key-popularity"},
		{kind: KindMap, value: int64(2)},
		{kind: KindBlobString, value: []byte("a")},
		{kind: KindDouble, value: 0.1923},
		{kind: KindBlobString, value: []byte("b")},
		{kind: KindDouble, value: 0.0012},

		{kind: KindPush, value: int64(3)},
		{kind: KindString, value: "message"},
		{kind: KindString, value: "somechannel"},
		{kind: KindString, value: "this is the message"},

		{kind: KindBlobString, value: TermUnbound(zeroStringPart)},
		{kind: KindStringPart, value: []byte("Hell")},
		{kind: KindStringPart, value: []byte("o wor")},
		{kind: KindStringPart, value: []byte("ld")},
		{kind: KindStringPart, value: []byte(nil)},

		{kind: KindMap, value: TermUnbound(End)},
		{kind: KindString, value: "a"},
		{kind: KindNumber, value: int64(1)},
		{kind: KindString, value: "b"},
		{kind: KindNumber, value: int64(2)},
		{kind: KindEnd, value: nil},

		{kind: KindNull, value: nil},
	}

	var buf bytes.Buffer
	e := NewRESP3Encoder(&buf)
	d := NewRESP3Decoder(strings.NewReader(data))
	for i, want := range wants {
		got, err := d.ReadToken()
		if err != nil {
			t.Fatalf("ReadToken()#%d got err = %v", i, err)
		}
		if want := want.kind; got.Kind() != want {
			t.Errorf("ReadToken()#%d got = %v, want = %v", i, got.Kind(), want)
		}
		switch want.kind {
		default:
			val := got.Val()
			if want := want.value; !reflect.DeepEqual(val, want) {
				t.Errorf("ReadValue()#%d got = %[2]T %[2]s, want = %[3]T %[3]s", i, val, want)
			}
		case KindDouble:
			val := got.Val()
			if want := want.value; !reflect.DeepEqual(val, want) && !bothNaN(got.Kind(), val, want) {
				t.Errorf("ReadValue()#%d got = %v, want = %v", i, val, want)
			}
		}
		if err := e.WriteToken(got); err != nil {
			t.Fatalf("WriteToken()#%d got err = %v", i, err)
		}
	}
	if err := e.Flush(); err != nil {
		t.Fatalf("Flush() got err = %v", err)
	}
	if got := buf.String(); got != data {
		t.Errorf("Flush() got = %v, want = %v", got, data)
	}

	tokens := []Token{
		Bytes([]byte("hello world")),
		Bytes([]byte("")),
		String("hello world"),
		Error("ERR this is the error description"),
		Int(1234),
		Float(1.23),
		Float(123),
		Float(math.Inf(1)),
		Float(math.Inf(-1)),
		Float(math.NaN()),
		Bool(true),
		Bool(false),
		BlobError(errors.New("SYNTAX invalid syntax")),
		VerbatimString(VerbatimStr{Format: "txt", String: "Some string"}),
		BigInt(mustParseBig("3492890328409238509324850943850943825024385")),

		ArrayLen(3), Int(1), Int(2), Int(3),

		MapLen(2), String("first"), Int(1), String("second"), Int(2),

		SetLen(5), String("orange"), String("apple"), Bool(true), Int(100), Int(999),

		AttributeLen(1), String("key-popularity"), MapLen(2), Bytes([]byte("a")), Float(0.1923), Bytes([]byte("b")), Float(0.0012),

		Push(3), String("message"), String("somechannel"), String("this is the message"),

		rawToken("$?\r\n"), PartBytes([]byte("Hell")), PartBytes([]byte("o wor")), PartBytes([]byte("ld")), PartBytes(nil),

		UnboundMap, String("a"), Int(1), String("b"), Int(2), End,

		Null,
	}

	buf.Reset()
	for i, tok := range tokens {
		if err := e.WriteToken(tok); err != nil {
			t.Fatalf("WriteToken()#%d got err = %v", i, err)
		}
	}
	if err := e.Flush(); err != nil {
		t.Fatalf("Flush() got err = %v", err)
	}
	if got := buf.String(); got != data {
		t.Errorf("Flush() got = %v, want = %v", got, data)
	}

	// A line too short to carry the two terminator bytes is an
	// error, the read must not panic.
	t.Run("invalid terminator", func(t *testing.T) {
		for _, data := range []string{"+\n", ":\n"} {
			d := NewRESP3Decoder(strings.NewReader(data))
			if _, err := d.ReadToken(); err == nil {
				t.Errorf("ReadToken() on %q did not fail", data)
			}
		}
	})
}

func bothNaN(kind Kind, got any, want any) bool {
	if kind != KindDouble {
		return false
	}
	return math.IsNaN(got.(float64)) && math.IsNaN(want.(float64))
}

func mustParseBig(s string) *big.Int {
	f, _ := big.NewInt(0).SetString(s, 10)
	return f
}

func TestRESP3Encoder_WriteToken_streamed(t *testing.T) {
	t.Run("payload is streamed", func(t *testing.T) {
		var b strings.Builder
		e := NewRESP3Encoder(&b)
		tok := BlobString(&io.LimitedReader{R: strings.NewReader("0123456789trailing"), N: 10})
		if got, want := tok.Kind(), KindBlobString; got != want {
			t.Fatalf("Kind() = %s, want %s", got, want)
		}
		if err := e.WriteToken(tok); err != nil {
			t.Fatal(err)
		}
		if err := e.Flush(); err != nil {
			t.Fatal(err)
		}
		if got, want := b.String(), "$10\r\n0123456789\r\n"; got != want {
			t.Errorf("WriteToken() = %q, want %q", got, want)
		}
	})

	t.Run("zero length payload is streamed", func(t *testing.T) {
		var b strings.Builder
		e := NewRESP3Encoder(&b)
		if err := e.WriteToken(BlobString(&io.LimitedReader{R: strings.NewReader(""), N: 0})); err != nil {
			t.Fatal(err)
		}
		if err := e.Flush(); err != nil {
			t.Fatal(err)
		}
		if got, want := b.String(), "$0\r\n\r\n"; got != want {
			t.Errorf("WriteToken() = %q, want %q", got, want)
		}
	})

	t.Run("short payload fails the write", func(t *testing.T) {
		var b strings.Builder
		e := NewRESP3Encoder(&b)
		if err := e.WriteToken(BlobString(&io.LimitedReader{R: strings.NewReader("abc"), N: 10})); err == nil {
			t.Error("WriteToken() of a short payload did not fail")
		}
	})

	t.Run("nil reader panics", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Error("BlobString() with a nil reader did not panic")
			}
		}()
		BlobString(nil)
	})
}

func TestRESP3Decoder_ReadToken_streamed(t *testing.T) {
	payload := strings.Repeat("0123456789abcdef", 64)

	t.Run("payload is streamed", func(t *testing.T) {
		d := NewRESP3Decoder(strings.NewReader("$" + strconv.Itoa(len(payload)) + "\r\n" + payload + "\r\n+OK\r\n"))
		tok, err := d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := tok.Kind(), KindBlobString; got != want {
			t.Fatalf("Kind() = %s, want %s", got, want)
		}
		if tok.stream == nil {
			t.Fatal("ReadToken() did not stream the payload")
		}
		p, err := io.ReadAll(tok.stream)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := string(p), payload+"\r\n"; got != want {
			t.Errorf("payload = %q, want %q", got[:16]+"...", want[:16]+"...")
		}
		tok2, err := d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := tok2.String(), "OK"; got != want {
			t.Errorf("ReadToken() after the payload = %q, want %q", got, want)
		}
	})

	t.Run("Bytes consumes the payload", func(t *testing.T) {
		d := NewRESP3Decoder(strings.NewReader("$" + strconv.Itoa(len(payload)) + "\r\n" + payload + "\r\n+OK\r\n"))
		tok, err := d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := string(tok.Bytes()), payload; got != want {
			t.Errorf("Bytes() = %q, want %q", got[:16]+"...", want[:16]+"...")
		}
		tok2, err := d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := tok2.String(), "OK"; got != want {
			t.Errorf("ReadToken() after Bytes = %q, want %q", got, want)
		}
	})

	t.Run("unconsumed payload is skipped", func(t *testing.T) {
		d := NewRESP3Decoder(strings.NewReader("$" + strconv.Itoa(len(payload)) + "\r\n" + payload + "\r\n+OK\r\n"))
		tok, err := d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		tok2, err := d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := tok2.String(), "OK"; got != want {
			t.Errorf("ReadToken() after the unconsumed payload = %q, want %q", got, want)
		}
		if _, ok := tok.stream.(*AdvanceReader); !ok {
			t.Fatalf("ReadToken() streamed a %T", tok.stream)
		}
	})
}

func TestRESP3Decoder_ReadToken_streamed_blobError(t *testing.T) {
	payload := strings.Repeat("ERR ", 160)

	t.Run("blob error is streamed", func(t *testing.T) {
		d := NewRESP3Decoder(strings.NewReader("!" + strconv.Itoa(len(payload)) + "\r\n" + payload + "\r\n+OK\r\n"))
		tok, err := d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := tok.Kind(), KindBlobError; got != want {
			t.Fatalf("Kind() = %s, want %s", got, want)
		}
		if tok.stream == nil {
			t.Fatal("ReadToken() did not stream the payload")
		}
		if got, want := string(tok.Bytes()), payload; got != want {
			t.Errorf("Bytes() = %q, want %q", got[:16]+"...", payload[:16]+"...")
		}
		tok2, err := d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := tok2.String(), "OK"; got != want {
			t.Errorf("ReadToken() after the payload = %q, want %q", got, want)
		}
	})

	t.Run("blob error Err consumes the payload", func(t *testing.T) {
		d := NewRESP3Decoder(strings.NewReader("!" + strconv.Itoa(len(payload)) + "\r\n" + payload + "\r\n+OK\r\n"))
		tok, err := d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := tok.Err().Error(), payload; got != want {
			t.Errorf("Err() = %q, want %q", got[:16]+"...", payload[:16]+"...")
		}
		tok2, err := d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := tok2.String(), "OK"; got != want {
			t.Errorf("ReadToken() after Err = %q, want %q", got, want)
		}
	})
}

func TestRESP3Decoder_ReadToken_eofMarked(t *testing.T) {
	const marker = "0123456789abcdef0123456789abcdef01234567"

	t.Run("payload ends at the marker", func(t *testing.T) {
		d := NewRESP3Decoder(strings.NewReader("$EOF:" + marker + "\r\nhello world" + marker + "+OK\r\n"))
		tok, err := d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := tok.Kind(), KindBlobString; got != want {
			t.Fatalf("Kind() = %s, want %s", got, want)
		}
		if got, want := tok.Int(), int64(0); got != want {
			t.Errorf("Int() = %d, want %d", got, want)
		}
		if got, want := string(tok.Bytes()), "hello world"; got != want {
			t.Errorf("Bytes() = %q, want %q", got, want)
		}
		tok2, err := d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := tok2.String(), "OK"; got != want {
			t.Errorf("ReadToken() after the payload = %q, want %q", got, want)
		}
	})

	t.Run("unconsumed payload is skipped", func(t *testing.T) {
		d := NewRESP3Decoder(strings.NewReader("$EOF:" + marker + "\r\nhello world" + marker + "+OK\r\n"))
		if _, err := d.ReadToken(); err != nil {
			t.Fatal(err)
		}
		tok2, err := d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := tok2.String(), "OK"; got != want {
			t.Errorf("ReadToken() after the unconsumed payload = %q, want %q", got, want)
		}
	})

	t.Run("a stream without the marker fails", func(t *testing.T) {
		d := NewRESP3Decoder(strings.NewReader("$EOF:" + marker + "\r\nhello world"))
		if _, err := d.ReadToken(); err != nil {
			t.Fatal(err)
		}
		if _, err := d.ReadToken(); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("ReadToken() error = %v, want %v", err, io.ErrUnexpectedEOF)
		}
	})

	t.Run("an EOF-marked token can be written", func(t *testing.T) {
		d := NewRESP3Decoder(strings.NewReader("$EOF:" + marker + "\r\nhello" + marker))
		tok, err := d.ReadToken()
		if err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		if err := NewRESP3Encoder(&buf).WriteToken(tok); err != nil {
			t.Error("WriteToken() of an EOF-marked token failed")
		}
	})
}
