package rdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFileDecoder_ReadToken(t *testing.T) {
	tests := []struct {
		name        string
		wantVersion int
		wantErr     bool
	}{
		{name: "100_lists", wantVersion: 11},
		{name: "array_v14_basic", wantVersion: 14},
		{name: "array_v14_insert_idx_boundary", wantVersion: 14},
		{name: "array_v14_insert_idx_none", wantVersion: 14},
		{name: "array_v14_mixed_types", wantVersion: 14},
		{name: "array_v14_with_insert_idx", wantVersion: 14},
		{name: "cluster_slot_info", wantVersion: 12},
		{name: "empty", wantVersion: 11},
		{name: "function", wantVersion: 11},
		{name: "function2", wantVersion: 12},
		{name: "future_v19", wantVersion: 99, wantErr: true},
		{name: "hash", wantVersion: 3},
		{name: "hash_lp_v11", wantVersion: 11},
		{name: "hash_lp_with_hexpire_v12", wantVersion: 12},
		{name: "hash_v3", wantVersion: 7},
		{name: "hash_with_expire_v12", wantVersion: 12},
		{name: "hash_zl_v6", wantVersion: 9},
		{name: "hash_zm_v2", wantVersion: 2},
		{name: "hash_template_array_v15", wantVersion: 15},
		{name: "hash_template_arrayref_min_v15", wantVersion: 15},
		{name: "hash_template_self_array_v15", wantVersion: 15},
		{name: "hash_template_self_arraylp_v15", wantVersion: 15},
		{name: "hash_template_self_lp_v15", wantVersion: 15},
		{name: "hash_template_self_lpraw_v15", wantVersion: 15},
		{name: "hash_template_v15", wantVersion: 15},
		{name: "hash_template_values_v15", wantVersion: 15},
		{name: "invalid_chksum_v8", wantVersion: 8, wantErr: true},
		{name: "mem_policy_lfu", wantVersion: 11},
		{name: "mem_policy_lru", wantVersion: 11},
		{name: "misc_with_stream", wantVersion: 9},
		{name: "module", wantVersion: 11},
		{name: "module_aux", wantVersion: 11},
		{name: "module_aux_empty", wantVersion: 12},
		{name: "module_aux_v12", wantVersion: 12},
		{name: "multiple_dbs", wantVersion: 11},
		{name: "multiple_lists_strings", wantVersion: 11},
		{name: "multiple_types_v15", wantVersion: 15},
		{name: "plain_hash_v3", wantVersion: 7},
		{name: "plain_list_v6", wantVersion: 6},
		{name: "plain_set_v6", wantVersion: 6},
		{name: "plain_zset_2_v11", wantVersion: 11},
		{name: "plain_zset_v6", wantVersion: 6},
		{name: "quicklist", wantVersion: 8},
		{name: "quicklist2_v11", wantVersion: 11},
		{name: "redis_ent_opcode_ram_lru", wantVersion: 12},
		{name: "set_expired_v11", wantVersion: 11},
		{name: "set_is_v11", wantVersion: 11},
		{name: "set_lp_v11", wantVersion: 11},
		{name: "set_not_expired_v11", wantVersion: 11},
		{name: "single_key", wantVersion: 11},
		{name: "stream_v11", wantVersion: 11},
		{name: "stream_v13", wantVersion: 13},
		{name: "stream_v13_idmp", wantVersion: 14},
		{name: "string_int_encoded", wantVersion: 11},
		{name: "string_lzf", wantVersion: 11},
		{name: "ziplist_v3", wantVersion: 3},
		{name: "zset_lp_v11", wantVersion: 11},
		{name: "zset_zl_v6", wantVersion: 6},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want, err := os.ReadFile(filepath.Join("testdata/rdb", tt.name+".rdb"))
			if err != nil {
				t.Fatal(err)
			}
			dec := NewFileDecoder(bytes.NewReader(want), nil)
			dial, dialErr := dec.Dialect()
			var got bytes.Buffer
			enc := NewFileEncoder(&got, dial)
			var decErr error
			for {
				tok, err := dec.ReadToken()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					decErr = err
					break
				}
				if err := enc.WriteToken(tok); err != nil {
					t.Fatalf("could not write token: %v", err)
				}
			}
			err = dialErr
			if err == nil {
				err = decErr
			}
			if (err != nil) != tt.wantErr {
				t.Fatalf("round trip error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			_, version := dial.Header()
			if version != tt.wantVersion {
				t.Fatalf("the dialect = %d, want = %d", version, tt.wantVersion)
			}
			// The file the encoder writes carries the checksum the
			// fixture carries, the versions before 5 write none and
			// carry none.
			gotBytes := got.Bytes()
			if version >= 5 && bytes.Equal(want[len(want)-8:], disabledChecksum) {
				// The fixture disables its checksum, the written one
				// is compared as content.
				gotBytes = gotBytes[:len(gotBytes)-8]
				want = want[:len(want)-8]
			}
			if !bytes.Equal(gotBytes, want) {
				for i := 0; i < min(len(gotBytes), len(want)); i++ {
					if gotBytes[i] != want[i] {
						t.Fatalf("content mismatch from %d:\ngot  %x\nwant %x", i, gotBytes[i:], want[i:])
					}
				}
			}
		})
	}

	// The op codes of a newer version are not the records of an older
	// file: the hash field expiry types were introduced in the version
	// 12, a file of the version 9 cannot carry one.
	t.Run("op code newer than the file", func(t *testing.T) {
		dec := NewFileDecoder(bytes.NewReader(append([]byte("REDIS0009"), byte(RedisOpCodeHashListpackEx))), nil)
		_, err := dec.ReadToken()
		if !errors.Is(err, ErrOpCodeTooNew) {
			t.Fatalf("ReadToken() error = %v, want = %v", err, ErrOpCodeTooNew)
		}

		// The dispatch returns the token of the op code, the tokens of
		// the record follow it.
		dec = NewFileDecoder(bytes.NewReader(append([]byte("REDIS0012"), byte(RedisOpCodeHashListpackEx))), nil)
		if _, err := dec.ReadToken(); err != nil {
			t.Fatalf("ReadToken() error = %v", err)
		}
		if _, err := dec.ReadToken(); !errors.Is(err, io.EOF) {
			t.Fatalf("ReadToken() error = %v, want = %v", err, io.EOF)
		}
	})
}

// TestFileDecoder_Dialect decodes the KeyDB and Valkey fixtures with
// their dialect enabled: the value types of the dialects are only
// supported with it. The fixtures are the test assets of KeyDB and
// Valkey themselves.
func TestFileDecoder_Dialect(t *testing.T) {
	t.Run("keydb", func(t *testing.T) {
		tests := []struct {
			name        string
			wantVersion int
			wantErr     bool
		}{
			{name: "corrupt_empty_keys", wantVersion: 9, wantErr: true},
			{name: "corrupt_ziplist", wantVersion: 7},
			{name: "encodings", wantVersion: 4},
			{name: "hash-zipmap", wantVersion: 3},
			{name: "redis-module-save", wantVersion: 9},
			{name: "redis-save", wantVersion: 9},
			{name: "keydb_cron_v9", wantVersion: 9},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				data, err := os.ReadFile(filepath.Join("testdata/keydb", tt.name+".rdb"))
				if err != nil {
					t.Fatal(err)
				}
				dec := NewFileDecoder(bytes.NewReader(data), KeyDBDialect{})
				dial, err := dec.Dialect()
				if err != nil {
					t.Fatalf("Dialect() error = %v", err)
				}
				_, version := dial.Header()
				if version != tt.wantVersion {
					t.Fatalf("the dialect = %d, want = %d", version, tt.wantVersion)
				}
				for {
					if _, err := dec.ReadToken(); errors.Is(err, io.EOF) {
						return
					} else if err != nil {
						if !tt.wantErr {
							t.Errorf("could not read token: %v", err)
						}
						return
					}
				}
			})
		}
	})

	// The Valkey dialect: value type 22 is a hash with per field
	// expiry, op code 243 is the slot import state and the KeyDB
	// specific values are not supported.
	t.Run("valkey", func(t *testing.T) {
		tests := []struct {
			name        string
			wantVersion int
			wantErr     bool
			wantDecErr  error
		}{
			{name: "corrupt_empty_keys", wantVersion: 10, wantDecErr: io.ErrUnexpectedEOF},
			{name: "corrupt_ziplist", wantVersion: 7},
			{name: "encodings", wantVersion: 4},
			{name: "encodings-rdb12", wantVersion: 12},
			{name: "encodings-rdb75-unknown-types", wantVersion: 75, wantErr: true},
			{name: "encodings-rdb987", wantErr: true},
			{name: "encodings-rdb987-unknown-types", wantErr: true},
			{name: "hash-ziplist", wantVersion: 9},
			{name: "hash-zipmap", wantVersion: 3},
			{name: "list-quicklist", wantVersion: 8},
			{name: "scriptbackup", wantVersion: 10},
			{name: "zset-ziplist", wantVersion: 10},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				data, err := os.ReadFile(filepath.Join("testdata/valkey", tt.name+".rdb"))
				if err != nil {
					t.Fatal(err)
				}
				dec := NewFileDecoder(bytes.NewReader(data), nil)
				dial, dialErr := dec.Dialect()
				if (dialErr != nil) != tt.wantErr {
					t.Fatalf("Dialect() error = %v, wantErr %v", dialErr, tt.wantErr)
				}
				if dialErr != nil {
					return
				}
				_, version := dial.Header()
				if version != tt.wantVersion {
					t.Fatalf("the dialect = %d, want = %d", version, tt.wantVersion)
				}
				for {
					if _, err := dec.ReadToken(); errors.Is(err, io.EOF) {
						return
					} else if err != nil {
						if !errors.Is(err, tt.wantDecErr) {
							t.Errorf("could not read token: %v", err)
						}
						return
					}
				}
			})
		}

		t.Run("hash2", func(t *testing.T) {
			tokens := []Token{
				{k: KindOpCode, opCode: ValkeyOpCodeHash2},
				{k: KindString, str: []byte("hash:ttl")},
				{k: KindUint, num: 1},
				{k: KindString, str: []byte("field")},
				{k: KindString, str: []byte("value")},
				{k: KindTime, num: 1700000000000},
			}

			var buf bytes.Buffer
			enc := NewFileEncoder(&buf, ValkeyDialect{})
			for _, tok := range tokens {
				if err := enc.WriteToken(tok); err != nil {
					t.Fatalf("WriteToken(%s) error = %v", tok.Kind(), err)
				}
			}
			if got, want := string(buf.Bytes()[:9]), "VALKEY080"; got != want {
				t.Fatalf("header = %q, want = %q", got, want)
			}

			dec := NewFileDecoder(&buf, nil)
			for i, want := range tokens {
				got, err := dec.ReadToken()
				if err != nil {
					t.Fatalf("ReadToken(%d) error = %v", i, err)
				}
				if got.Kind() != want.Kind() {
					t.Fatalf("ReadToken(%d) kind = %s, want = %s", i, got.Kind(), want.Kind())
				}
				switch want.Kind() {
				case KindString:
					if got.String() != want.String() {
						t.Fatalf("ReadToken(%d) = %q, want = %q", i, got.String(), want.String())
					}
				case KindUint:
					if got.Uint64() != want.Uint64() {
						t.Fatalf("ReadToken(%d) = %d, want = %d", i, got.Uint64(), want.Uint64())
					}
				case KindTime:
					if !got.Time().Equal(want.Time()) {
						t.Fatalf("ReadToken(%d) = %s, want = %s", i, got.Time(), want.Time())
					}
				}
			}
		})

		t.Run("slot import", func(t *testing.T) {
			tokens := []Token{
				{k: KindOpCode, opCode: ValkeyOpCodeSlotImport},
				{k: KindString, str: []byte("job-0001")},
				{k: KindUint, num: 1},
				{k: KindUint, num: 0},
				{k: KindUint, num: 16383},
			}

			var buf bytes.Buffer
			enc := NewFileEncoder(&buf, ValkeyDialect{})
			for _, tok := range tokens {
				if err := enc.WriteToken(tok); err != nil {
					t.Fatalf("WriteToken(%s) error = %v", tok.Kind(), err)
				}
			}
			if got, want := string(buf.Bytes()[:9]), "VALKEY080"; got != want {
				t.Fatalf("header = %q, want = %q", got, want)
			}

			dec := NewFileDecoder(&buf, nil)
			for i, want := range tokens {
				got, err := dec.ReadToken()
				if err != nil {
					t.Fatalf("ReadToken(%d) error = %v", i, err)
				}
				if got.Kind() != want.Kind() {
					t.Fatalf("ReadToken(%d) kind = %s, want = %s", i, got.Kind(), want.Kind())
				}
				switch want.Kind() {
				case KindString:
					if got.String() != want.String() {
						t.Fatalf("ReadToken(%d) = %q, want = %q", i, got.String(), want.String())
					}
				case KindUint:
					if got.Uint64() != want.Uint64() {
						t.Fatalf("ReadToken(%d) = %d, want = %d", i, got.Uint64(), want.Uint64())
					}
				case KindTime:
					if !got.Time().Equal(want.Time()) {
						t.Fatalf("ReadToken(%d) = %s, want = %s", i, got.Time(), want.Time())
					}
				}
			}
		})

		t.Run("unsupported op codes", func(t *testing.T) {
			var buf bytes.Buffer
			enc := NewEncoder(&buf, ValkeyDialect{})
			if err := enc.WriteToken(Token{k: KindOpCode, opCode: RedisOpCodeHashMetadata}); err == nil {
				t.Error("WriteToken(RedisOpCodeHashMetadata) = nil error, want an unsupported error")
			}
			if err := enc.WriteToken(Token{k: KindOpCode, opCode: KeyDBOpCodeCron}); err == nil {
				t.Error("WriteToken(KeyDBOpCodeCron) = nil error, want an unsupported error")
			}

			buf.Reset()
			buf.WriteString("VALKEY080")
			buf.WriteByte(byte(RedisOpCodeHashTemplate))
			dec := NewFileDecoder(&buf, nil)
			if _, err := dec.ReadToken(); err == nil {
				t.Error("ReadToken(RedisOpCodeHashTemplate) = nil error, want an unsupported error")
			}

			buf.Reset()
			buf.WriteString("VALKEY080")
			buf.WriteByte(byte(KeyDBOpCodeCron))
			dec = NewFileDecoder(&buf, nil)
			if _, err := dec.ReadToken(); err == nil {
				t.Error("ReadToken(KeyDBOpCodeCron) = nil error, want an unsupported error")
			}
		})
	})

	t.Run("register dialect", func(t *testing.T) {
		RegisterDialect(forkDialect{})
		var buf bytes.Buffer
		buf.WriteString("FORK00001")
		buf.WriteByte(byte(RedisOpCodeString))
		buf.WriteByte(0x02)
		buf.WriteString("hi")
		dec := NewFileDecoder(&buf, nil)
		dial, err := dec.Dialect()
		if err != nil {
			t.Fatalf("Dialect() error = %v", err)
		}
		if _, ver := dial.Header(); ver != 1 {
			t.Fatalf("the dialect = %d, want 1", ver)
		}
		if _, err := dec.ReadToken(); err != nil {
			t.Fatalf("ReadToken() error = %v", err)
		}
		tok, err := dec.ReadToken()
		if err != nil {
			t.Fatalf("ReadToken() error = %v", err)
		}
		if got := tok.String(); got != "hi" {
			t.Fatalf("ReadToken() = %q, want = %q", got, "hi")
		}
	})
}

// forkDialect is a minimal custom dialect to test the registry.
type forkDialect struct {
	RedisDialect
}

func (forkDialect) Header() (string, int) { return "FORK", 1 }

func BenchmarkFileDecoder(b *testing.B) {
	f, err := os.ReadFile(filepath.Join("testdata/rdb", "hash.rdb"))
	if err != nil {
		b.Fatalf("could not read file: %v", err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r := bytes.NewReader(f)
		var gotBuf bytes.Buffer
		enc := NewEncoder(&gotBuf, nil)
		var wantBuf bytes.Buffer
		dec := NewFileDecoder(io.TeeReader(r, &wantBuf), nil)
		for {
			token, err := dec.ReadToken()
			if err == io.EOF {
				break
			} else if err != nil {
				b.Errorf("could not read token: %v", err)
				break
			}
			if err := enc.WriteToken(token); err != nil {
				b.Errorf("could not write token: %v", err)
				break
			}
		}
	}
}

func TestToken_Bytes(t *testing.T) {
	tests := []struct {
		name      string
		token     Token
		want      []byte
		wantPanic bool
	}{
		// A token that carries raw bytes always returns them.
		{name: "raw precedence", token: Token{k: KindUint, raw: []byte{0x2a}, num: 100}, want: []byte{0x2a}},
		{name: "raw empty blob", token: Token{k: KindZipList, raw: []byte{0x01}}, want: []byte{0x01}},

		// KindUnknown has no representation.
		{name: "unknown", token: Token{k: KindUnknown}, wantPanic: true},

		// KindOpCode builds from the op code.
		{name: "opcode", token: Token{k: KindOpCode, opCode: RedisOpCodeSelectDB}, want: []byte{0xfe}},
		{name: "opcode value type", token: Token{k: KindOpCode, opCode: RedisOpCodeHashListpack}, want: []byte{0x10}},

		// KindByte builds from the bytes.
		{name: "byte", token: Token{k: KindByte, str: []byte{0x01, 0x02, 0x03}}, want: []byte{0x01, 0x02, 0x03}},

		// KindUint builds the canonical length encoding.
		{name: "uint 6 bit", token: Token{k: KindUint, num: 10}, want: []byte{0x0a}},
		{name: "uint 14 bit", token: Token{k: KindUint, num: 300}, want: []byte{0x41, 0x2c}},
		{name: "uint 32 bit", token: Token{k: KindUint, num: 70000}, want: []byte{0x80, 0x00, 0x01, 0x11, 0x70}},
		{name: "uint 64 bit", token: Token{k: KindUint, num: 1 << 40}, want: []byte{0x81, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00}},

		// KindInt builds the canonical length encoding of the signed value.
		{name: "int positive", token: Token{k: KindInt, num: 42}, want: []byte{0x2a}},
		{name: "int negative", token: Token{k: KindInt, num: ^uint64(0)}, want: []byte{0x81, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},

		// KindFloat32 builds 4 bytes in little endian format.
		{name: "float32", token: Token{k: KindFloat32, num: uint64(math.Float32bits(1.5))}, want: []byte{0x00, 0x00, 0xc0, 0x3f}},

		// KindFloat64 builds 8 bytes in little endian format, unless the
		// score belongs to the zset op code, which stores scores as text.
		{name: "float64 binary", token: Token{k: KindFloat64, num: math.Float64bits(1.5)}, want: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf8, 0x3f}},
		{name: "float64 zset2 binary", token: Token{k: KindFloat64, opCode: RedisOpCodeZset2, num: math.Float64bits(1.5)}, want: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf8, 0x3f}},
		{name: "float64 zset text", token: Token{k: KindFloat64, opCode: RedisOpCodeZset, num: math.Float64bits(1.5)}, want: append([]byte{0x03}, []byte("1.5")...)},
		{name: "float64 zset positive inf", token: Token{k: KindFloat64, opCode: RedisOpCodeZset, num: math.Float64bits(math.Inf(1))}, want: []byte{0xfe}},
		{name: "float64 zset negative inf", token: Token{k: KindFloat64, opCode: RedisOpCodeZset, num: math.Float64bits(math.Inf(-1))}, want: []byte{0xff}},
		{name: "float64 zset nan", token: Token{k: KindFloat64, opCode: RedisOpCodeZset, num: math.Float64bits(math.NaN())}, want: []byte{0xfd}},

		// KindString builds a plain string, or the length encoded module ID.
		{name: "string", token: Token{k: KindString, str: []byte("value")}, want: append([]byte{0x05}, []byte("value")...)},
		{name: "string empty", token: Token{k: KindString}, want: []byte{0x00}},
		{name: "module ID", token: Token{k: KindString, format: formatModuleID, str: []byte("test__rdb")}, want: []byte{0x81, 0xb5, 0xeb, 0x2d, 0xff, 0xfa, 0xdd, 0x6c, 0x00}},

		// The constructors build the exact form of their kind.
		{name: "byte constructor", token: Byte([]byte{0x01, 0x02, 0x03}), want: []byte{0x01, 0x02, 0x03}},
		{name: "int constructor", token: Int(-1), want: []byte{0x81, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
		{name: "float32 constructor", token: Float32(1.5), want: []byte{0x00, 0x00, 0xc0, 0x3f}},
		{name: "float64 constructor", token: Float64(1.5), want: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf8, 0x3f}},
		{name: "stream ID constructor", token: StreamID(1, 2), want: []byte{0x10, 0, 0, 0, 0, 0, 0, 0, 0x01, 0, 0, 0, 0, 0, 0, 0, 0x02}},
		{name: "time constructor", token: Time(time.UnixMilli(1700000000000)), want: []byte{0x00, 0x68, 0xe5, 0xcf, 0x8b, 0x01, 0x00, 0x00}},

		// KindExpiry builds a little endian timestamp, seconds or milliseconds
		// depending on the op code.
		{name: "expiry seconds", token: Token{k: KindExpiry, opCode: RedisOpCodeExpirySeconds, num: 1700000000}, want: []byte{0x00, 0xf1, 0x53, 0x65}},
		{name: "expiry milliseconds", token: Token{k: KindExpiry, opCode: RedisOpCodeExpiryMilli, num: 1700000000000}, want: []byte{0x00, 0x68, 0xe5, 0xcf, 0x8b, 0x01, 0x00, 0x00}},
		{name: "expiry default milliseconds", token: Token{k: KindExpiry, num: 1700000000000}, want: []byte{0x00, 0x68, 0xe5, 0xcf, 0x8b, 0x01, 0x00, 0x00}},

		// KindTime builds 8 bytes in little endian format.
		{name: "time", token: Token{k: KindTime, num: 1700000000000}, want: []byte{0x00, 0x68, 0xe5, 0xcf, 0x8b, 0x01, 0x00, 0x00}},

		// KindStreamID builds the 17 byte stream node key form.
		{name: "stream ID", token: Token{k: KindStreamID, str: bytes.Repeat([]byte{0x01}, 16)}, want: append([]byte{0x10}, bytes.Repeat([]byte{0x01}, 16)...)},

		// KindQuickList, KindZipMap, KindZipList, KindSetIntSet and
		// KindZsetList have no values to build the wire format from.
		{name: "quick list", token: Token{k: KindQuickList}, wantPanic: true},
		{name: "zip map", token: Token{k: KindZipMap}, wantPanic: true},
		{name: "zip list", token: Token{k: KindZipList}, wantPanic: true},
		{name: "set int set", token: Token{k: KindSetIntSet}, wantPanic: true},
		{name: "zset list", token: Token{k: KindZsetList}, wantPanic: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.wantPanic {
				defer func() {
					if recover() == nil {
						t.Fatalf("Bytes() = %v, want panic", tt.token.raw)
					}
				}()
				tt.token.Bytes()
				return
			}
			got := tt.token.Bytes()
			if !bytes.Equal(got, tt.want) {
				t.Fatalf("Bytes() = %x, want = %x", got, tt.want)
			}
		})
	}
	// Bytes and the accessors agree: a token built from values
	// produces the same bytes that the decoder reads for the same
	// values.
	t.Run("round trip", func(t *testing.T) {
		tests := []struct {
			name  string
			token Token
			check func(t *testing.T, token Token, b []byte)
		}{
			{
				name:  "uint",
				token: Token{k: KindUint, num: 300},
				check: func(t *testing.T, token Token, b []byte) {
					if got, want := decodeUint64(b), token.num; got != want {
						t.Fatalf("decodeUint64(%x) = %d, want = %d", b, got, want)
					}
				},
			},
			{
				name:  "string",
				token: Token{k: KindString, str: []byte("hello")},
				check: func(t *testing.T, token Token, b []byte) {
					if got, want := decodeString(b, formatDefault), string(token.str); got != want {
						t.Fatalf("decodeString(%x) = %q, want = %q", b, got, want)
					}
				},
			},
			{
				name:  "stream ID",
				token: Token{k: KindStreamID, str: []byte{0, 0, 1, 0x8a, 0xcc, 0x95, 0x39, 0x84, 0, 0, 0, 0, 0, 0, 0, 1}},
				check: func(t *testing.T, token Token, b []byte) {
					ms, seq := Token{k: KindStreamID, raw: b}.StreamID()
					want := token.str
					if got := binary.BigEndian.Uint64(want[:8]); ms != got {
						t.Fatalf("StreamID() ms = %d, want = %d", ms, got)
					}
					if got := binary.BigEndian.Uint64(want[8:]); seq != got {
						t.Fatalf("StreamID() seq = %d, want = %d", seq, got)
					}
				},
			},
			{
				name:  "time",
				token: Token{k: KindTime, num: 1700000000000},
				check: func(t *testing.T, token Token, b []byte) {
					got := Token{k: KindTime, raw: b}.Time()
					if want := token.Time(); !got.Equal(want) {
						t.Fatalf("Time() = %s, want = %s", got, want)
					}
				},
			},
			{
				name:  "module ID",
				token: Token{k: KindString, format: formatModuleID, str: []byte("test__rdb")},
				check: func(t *testing.T, token Token, b []byte) {
					if got, want := decodeString(b, formatModuleID), string(token.str); got != want {
						t.Fatalf("decodeString(%x) = %q, want = %q", b, got, want)
					}
				},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				b := tt.token.Bytes()
				tt.check(t, tt.token, b)
			})
		}
	})
}

func TestToken_Clone(t *testing.T) {
	t.Run("constructor token unchanged", func(t *testing.T) {
		token := Str("value")
		if got := token.Clone(); !bytes.Equal(got.Bytes(), token.Bytes()) {
			t.Fatalf("Clone().Bytes() = %q, want = %q", got.Bytes(), token.Bytes())
		}
	})
	t.Run("raw bytes copied", func(t *testing.T) {
		raw := []byte{0x2a}
		token := Token{k: KindUint, raw: raw}
		cloned := token.Clone()
		if got, want := cloned.Bytes(), token.Bytes(); !bytes.Equal(got, want) {
			t.Fatalf("Clone().Bytes() = %x, want = %x", got, want)
		}
		// The clone owns its bytes: the original raw slice can be
		// overwritten the way the next Decoder read reuses its buffer.
		clear(raw)
		if got, want := cloned.Bytes(), []byte{0x2a}; !bytes.Equal(got, want) {
			t.Fatalf("Clone().Bytes() = %x, want = %x", got, want)
		}
	})
}

func TestSlotRange_Contains(t *testing.T) {
	// The ends of the range are served, the slots behind it are
	// not.
	tests := []struct {
		name string
		slot uint16
		want bool
	}{
		{"the first slot of the range", 5, true},
		{"a slot of the range", 7, true},
		{"the last slot of the range", 10, true},
		{"the slot before the range", 4, false},
		{"the slot behind the range", 11, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := SlotRange{Start: 5, End: 10}
			if got := r.Contains(tt.slot); got != tt.want {
				t.Errorf("Contains(%d) = %v, want %v", tt.slot, got, tt.want)
			}
		})
	}
}
