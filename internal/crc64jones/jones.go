// Package crc64jones implements the CRC-64 checksum with the Jones
// polynomial, the one the RDB format closes its files with. It mirrors
// hash/crc64, whose predefined polynomials do not include it.
package crc64jones

import (
	"hash"
	"hash/crc64"
	"sync"
)

// Predefined polynomials.
const (
	// The Jones polynomial, defined probably nowhere.
	Jones = 0x95ac9329ac4bc9b5
)

var table = crc64.MakeTable(Jones)

var (
	slicing8TablesBuildOnce sync.Once
	slicing8TableJones      *[8]crc64.Table
)

func buildSlicing8TablesOnce() {
	slicing8TablesBuildOnce.Do(buildSlicing8Tables)
}

func buildSlicing8Tables() {
	slicing8TableJones = makeSlicingBy8Table(table)
}

func makeSlicingBy8Table(t *crc64.Table) *[8]crc64.Table {
	var helperTable [8]crc64.Table
	helperTable[0] = *t
	for i := range 256 {
		crc := t[i]
		for j := 1; j < 8; j++ {
			crc = t[crc&0xff] ^ (crc >> 8)
			helperTable[j][i] = crc
		}
	}
	return &helperTable
}

// digest represents the partial evaluation of a checksum.
type digest struct {
	crc uint64
	tab *crc64.Table
}

// New creates a new hash.Hash64 computing the CRC-64 checksum using the
// Jones polynomial. Its Sum method will lay the value out in little-endian
// byte order.
func New() hash.Hash64 { return &digest{0, table} }

func (d *digest) Size() int { return crc64.Size }

func (d *digest) BlockSize() int { return 1 }

func (d *digest) Reset() { d.crc = 0 }

func update(crc uint64, tab *crc64.Table, p []byte) uint64 {
	buildSlicing8TablesOnce()
	// Table comparison is somewhat expensive, so avoid it for small sizes
	for len(p) >= 64 {
		var helperTable *[8]crc64.Table
		if *tab == slicing8TableJones[0] {
			helperTable = slicing8TableJones
		} else if len(p) >= 2048 {
			// According to the tests between various x86 and arm CPUs, 2k is a reasonable
			// threshold for now. This may change in the future.
			helperTable = makeSlicingBy8Table(tab)
		} else {
			break
		}
		// Update using slicing-by-8
		for len(p) > 8 {
			crc ^= uint64(p[0]) | uint64(p[1])<<8 | uint64(p[2])<<16 | uint64(p[3])<<24 |
				uint64(p[4])<<32 | uint64(p[5])<<40 | uint64(p[6])<<48 | uint64(p[7])<<56
			crc = helperTable[7][crc&0xff] ^
				helperTable[6][(crc>>8)&0xff] ^
				helperTable[5][(crc>>16)&0xff] ^
				helperTable[4][(crc>>24)&0xff] ^
				helperTable[3][(crc>>32)&0xff] ^
				helperTable[2][(crc>>40)&0xff] ^
				helperTable[1][(crc>>48)&0xff] ^
				helperTable[0][crc>>56]
			p = p[8:]
		}
	}
	// For reminders or small sizes
	for _, v := range p {
		crc = tab[byte(crc)^v] ^ (crc >> 8)
	}
	return crc
}

// Update returns the result of adding the bytes in p to the crc.
func Update(crc uint64, tab *crc64.Table, p []byte) uint64 {
	return update(crc, tab, p)
}

func (d *digest) Write(p []byte) (n int, err error) {
	d.crc = update(d.crc, d.tab, p)
	return len(p), nil
}

func (d *digest) Sum64() uint64 { return d.crc }

func (d *digest) Sum(in []byte) []byte {
	s := d.Sum64()
	// Compared to the core hash functions we return in little endian byte order.
	return append(in, byte(s), byte(s>>8), byte(s>>16), byte(s>>24), byte(s>>32), byte(s>>40), byte(s>>48), byte(s>>56))
}

// Checksum returns the CRC-64 checksum of data
// using the polynomial represented by the [crc64.Table].
func Checksum(data []byte, tab *crc64.Table) uint64 { return update(0, tab, data) }
