package lzf

import (
	"bytes"
	"testing"
)

var testData = bytes.Repeat([]byte(`Excerpt from Alice’s Adventures in Wonderland
“Cheshire Puss,” she began, rather timidly,
as she did not at all know whether it would
like the name : however, it only grinned a little
wider. “ Come, it ’s pleased so far,” thought
Alice, and she went on, “Would you tell me,
please, which way I ought to walk from here ?”
    “That depends a good deal on where you
want to get to,” said the Cat.
    “I don ’t much care where——” said Alice.
    “Then it doesn’t matter which way you
walk,” said the Cat.
    “ ——so long as I get somewhere,” Alice
added as an explanation.
    “Oh, you ’re sure to do that,” said the Cat,
“if you only walk long enough.”
`), 100)

func Test_Decompress(t *testing.T) {
	tests := []struct {
		name   string
		flavor flavor
	}{
		{name: "fast", flavor: FlavorFast},
		{name: "very fast", flavor: FlavorVeryFast},
		{name: "ultra fast", flavor: FlavorUltraFast},
	}
	//=== RUN   TestLZF/fast
	//lzf_test.go:45: compressed len: 2228
	//=== RUN   TestLZF/very_fast
	//lzf_test.go:45: compressed len: 2343
	//=== RUN   TestLZF/ultra_fast
	//lzf_test.go:45: compressed len: 3135
	t.Logf("test data len: %d", len(testData))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := New(14, tt.flavor)
			buf := make([]byte, len(testData)-1)
			l, err := c.Compress(testData, buf)
			if err != nil {
				t.Fatalf("Compress() err = %v", err)
			}
			compressed := buf[:l]
			t.Logf("compressed len: %d", l)
			if bytes.Equal(compressed, testData) {
				t.Fatalf("Compress() compressed = %v", compressed)
			}
			buf = make([]byte, len(testData))
			l, err = c.Decompress(compressed, buf)
			if err != nil {
				t.Fatalf("Decompress() err = %v", err)
			}
			if got := buf[:l]; !bytes.Equal(got, testData) {
				t.Fatalf("Decompress()\ngot  = %q,\nwant = %q", got, testData)
			}
		})
	}
}

func FuzzLZF(f *testing.F) {
	f.Fuzz(func(t *testing.T, in []byte) {
		c := New(16, FlavorVeryFast)
		bsize := max(len(in), 100)
		bsize = bsize * 104 / 100
		buf := make([]byte, bsize)
		l, err := c.Compress(in, buf)
		if err != nil {
			t.Logf("Compress() err = %v", err)
			return
		}
		compressed := buf[:l]
		if len(in) > 0 && bytes.Equal(compressed, in) {
			t.Fatalf("Compress() compressed = %v", compressed)
		}
		buf = make([]byte, len(in))
		l, err = c.Decompress(compressed, buf)
		if err != nil {
			t.Fatalf("Decompress() err = %v", err)
		}
		if got := buf[:l]; !bytes.Equal(got, in) {
			t.Fatalf("Decompress()\ngot  = %q,\nwant = %q", got, in)
		}
	})
}

func benchmarkLZF_Compress(b *testing.B, hlog uint8, flavor flavor) {
	c := New(hlog, flavor)
	buf := make([]byte, len(testData)-1)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := c.Compress(testData, buf)
		if err != nil {
			b.Fatalf("Compress() err = %v", err)
		}
	}
}

func BenchmarkLZF_CompressFast12(b *testing.B)     { benchmarkLZF_Compress(b, 12, FlavorFast) }
func BenchmarkLZF_CompressFast16(b *testing.B)     { benchmarkLZF_Compress(b, 16, FlavorFast) }
func BenchmarkLZF_CompressFast20(b *testing.B)     { benchmarkLZF_Compress(b, 20, FlavorFast) }
func BenchmarkLZF_CompressVeryFast12(b *testing.B) { benchmarkLZF_Compress(b, 12, FlavorVeryFast) }
func BenchmarkLZF_CompressVeryFast16(b *testing.B) { benchmarkLZF_Compress(b, 16, FlavorVeryFast) }
func BenchmarkLZF_CompressVeryFast20(b *testing.B) { benchmarkLZF_Compress(b, 20, FlavorVeryFast) }
func BenchmarkLZF_CompressUltraFast12(b *testing.B) {
	benchmarkLZF_Compress(b, 12, FlavorUltraFast)
}
func BenchmarkLZF_CompressUltraFast16(b *testing.B) {
	benchmarkLZF_Compress(b, 16, FlavorUltraFast)
}
func BenchmarkLZF_CompressUltraFast20(b *testing.B) {
	benchmarkLZF_Compress(b, 20, FlavorUltraFast)
}

func BenchmarkLZF_Decompress(b *testing.B) {
	c := New(16, FlavorVeryFast)
	buf := make([]byte, len(testData)-1)
	l, err := c.Compress(testData, buf)
	if err != nil {
		b.Fatalf("Compress() err = %v", err)
	}
	compressed := buf[:l]
	buf = make([]byte, len(testData))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		l, err = c.Decompress(compressed, buf)
		if err != nil {
			b.Fatalf("Decompress() err = %v", err)
		}
	}
}
