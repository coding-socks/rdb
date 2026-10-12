//go:generate go run make_compress.go

// Package lzf implements the LZF (FASTLZ) compression the RDB format
// compresses its strings with. Only decompression is needed to read
// the files, compression is provided for writing them.
//
// compress.go is generated code, see make_compress.go.
package lzf

import (
	"errors"
)

// The errors the compression and decompression report.
var (
	// Err2BIG indicates that the output buffer is not large enough
	// for the compressed or decompressed data.
	Err2BIG = errors.New("lzf: the output buffer is not large enough")
	// ErrINVAL indicates that the compressed data is malformed.
	ErrINVAL = errors.New("lzf: an error in the compressed data is detected")
)

// The flavors of the compressor, in increasing order of speed and
// decreasing order of compression.
const (
	FlavorFast flavor = iota
	FlavorVeryFast
	FlavorUltraFast
)

type flavor uint8

const (
	maxOff = 1 << 13
	maxRef = (1 << 8) + (1 << 3)
	maxLit = 1 << 5
)

// Default is the compressor the package level functions use.
var Default = New(16, FlavorFast)

// LZF is an LZF compressor with a hash table of a size the constructor
// selects. The zero value is not usable, see [New].
type LZF struct {
	hlog  uint8
	hsize uint32

	flavor flavor
}

// New constructs a compressor with a hash table of 2^hlog entries and
// the compression flavor selected. The flavors trade compression for
// speed, see [FlavorFast].
// It panics when hlog is greater than 22.
func New(hlog uint8, flavor flavor) LZF {
	if hlog > 22 {
		panic("lzf: hlog cannot be greater than 22")
	}
	return LZF{
		hlog:  hlog,
		hsize: 1 << hlog,

		flavor: flavor,
	}
}

// Compress compresses input into output with the [Default] compressor.
// It returns the number of bytes written and [Err2BIG] when output is
// too small.
func Compress(input []byte, output []byte) (int, error) {
	return Default.Compress(input, output)
}

// Compress compresses input into output. It returns the number of
// bytes written and [Err2BIG] when output is too small.
func (c LZF) Compress(input []byte, output []byte) (int, error) {
	switch c.flavor {
	case FlavorUltraFast:
		return c.compressUltraFast(input, output)
	case FlavorVeryFast:
		return c.compressVeryFast(input, output)
	default:
		return c.compressFast(input, output)
	}
}

// Decompress decompresses input into output with the [Default]
// decompressor. It returns the number of bytes written, [Err2BIG] when
// output is too small and [ErrINVAL] when the compressed data is
// malformed.
func Decompress(input, output []byte) (int, error) {
	return Default.Decompress(input, output)
}

// Decompress decompresses input into output. It returns the number of
// bytes written, [Err2BIG] when output is too small and [ErrINVAL]
// when the compressed data is malformed.
func (c LZF) Decompress(input, output []byte) (int, error) {
	var (
		lin, lout = len(input), len(output)
		iin, iout int
	)

	for iin < lin {
		ctrl := int(input[iin])
		iin++

		if ctrl < (1 << 5) { // literal run
			ctrl++

			if (iout + ctrl) > lout {
				return 0, Err2BIG
			}

			if (iin + ctrl) > lin {
				return 0, ErrINVAL
			}

			copy(output[iout:iout+ctrl], input[iin:iin+ctrl])
			iout += ctrl
			iin += ctrl
		} else { // back reference
			l := ctrl >> 5
			iref := iout - ((ctrl & 0x1f) << 8) - 1

			if iin >= lin {
				return 0, ErrINVAL
			}

			if l == 7 {
				l += int(input[iin])
				iin++
				if iin >= lin {
					return 0, ErrINVAL
				}
			}

			iref -= int(input[iin])
			iin++

			l += 2
			if (iout + l) > lout {
				return 0, Err2BIG
			}

			if iref < 0 {
				return 0, ErrINVAL
			}

			if iout >= (iref + l) { // disjunct areas
				copy(output[iout:iout+l], output[iref:iref+l])
				iout += l
			} else { // overlapping, use octet by octet copying
				for ok := true; ok; ok = l > 0 {
					output[iout] = output[iref]
					iout++
					iref++
					l--
				}
			}
		}
	}

	return iout, nil
}

func frst(input []byte, i int) uint32 {
	return (uint32(input[i]) << 8) | uint32(input[i+1])
}

func next(v uint32, input []byte, i int) uint32 {
	return (v << 8) | uint32(input[i+2])
}
