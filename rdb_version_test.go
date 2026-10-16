package rdb

import (
	"bytes"
	"errors"
	"testing"
)

func TestRedisDialect_Decoder(t *testing.T) {
	tests := []struct {
		name    string
		op      OpCode
		version int
		wantErr bool
	}{
		{name: "SelectDB", op: RedisOpCodeSelectDB, version: 1},
		{name: "ExpirySeconds", op: RedisOpCodeExpirySeconds, version: 1},
		{name: "String", op: RedisOpCodeString, version: 1},
		{name: "List", op: RedisOpCodeList, version: 1},
		{name: "Set", op: RedisOpCodeSet, version: 1},
		{name: "Zset", op: RedisOpCodeZset, version: 1},
		{name: "Hash", op: RedisOpCodeHash, version: 1},
		{name: "RamLru", op: RedisOpCodeRamLru, version: 1},
		{name: "SlotNum", op: RedisOpCodeSlotNum, version: 1},

		{name: "HashZipmap", op: RedisOpCodeHashZipmap, version: 2},
		{name: "ListZiplist", op: RedisOpCodeListZiplist, version: 2},
		{name: "SetIntset", op: RedisOpCodeSetIntset, version: 2},
		{name: "ZsetZiplist", op: RedisOpCodeZsetZiplist, version: 2},

		{name: "ExpiryMilli", op: RedisOpCodeExpiryMilli, version: 3},
		{name: "HashZiplist", op: RedisOpCodeHashZiplist, version: 4},

		{name: "ListQuicklist", op: RedisOpCodeListQuicklist, version: 7},
		{name: "Aux", op: RedisOpCodeAux, version: 7},
		{name: "ResizeDB", op: RedisOpCodeResizeDB, version: 7},

		{name: "Zset2", op: RedisOpCodeZset2, version: 8},
		{name: "Module2", op: RedisOpCodeModule2, version: 8},

		{name: "StreamListpacks", op: RedisOpCodeStreamListpacks, version: 9},
		{name: "Freq", op: RedisOpCodeFreq, version: 9},
		{name: "Idle", op: RedisOpCodeIdle, version: 9},
		{name: "ModuleAux", op: RedisOpCodeModuleAux, version: 9},

		{name: "Func2", op: RedisOpCodeFunc2, version: 10},
		{name: "HashListpack", op: RedisOpCodeHashListpack, version: 10},
		{name: "ZsetListpack", op: RedisOpCodeZsetListpack, version: 10},
		{name: "ListQuicklist2", op: RedisOpCodeListQuicklist2, version: 10},
		{name: "StreamListpacks2", op: RedisOpCodeStreamListpacks2, version: 10},

		{name: "SetListpack", op: RedisOpCodeSetListpack, version: 11},
		{name: "StreamListpacks3", op: RedisOpCodeStreamListpacks3, version: 11},

		{name: "HashMetadataPreGa", op: RedisOpCodeHashMetadataPreGa, version: 12},
		{name: "HashListpackExPreGa", op: RedisOpCodeHashListpackExPreGa, version: 12},
		{name: "HashMetadata", op: RedisOpCodeHashMetadata, version: 12},
		{name: "HashListpackEx", op: RedisOpCodeHashListpackEx, version: 12},
		{name: "SlotInfo", op: RedisOpCodeSlotInfo, version: 12},

		{name: "KeyMeta", op: RedisOpCodeKeyMeta, version: 13},
		{name: "StreamListpacks4", op: RedisOpCodeStreamListpacks4, version: 13},

		{name: "StreamListpacks5", op: RedisOpCodeStreamListpacks5, version: 14},
		{name: "Array", op: RedisOpCodeArray, version: 14},

		{name: "HashTemplate", op: RedisOpCodeHashTemplate, version: 15},
		{name: "HashTmplLP", op: RedisOpCodeHashTmplLP, version: 15},
		{name: "HashTmplLpRef", op: RedisOpCodeHashTmplLpRef, version: 15},
		{name: "HashTmplArray", op: RedisOpCodeHashTmplArray, version: 15},
		{name: "HashTmplArrayRef", op: RedisOpCodeHashTmplArrayRef, version: 15},

		{name: "unknown", op: OpCode(8), wantErr: true},
		{name: "Cron", op: KeyDBOpCodeCron, wantErr: true},
	}
	dialect := RedisDialect{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := dialect.WithVersion(tt.version).Decoder(tt.op)
			if (err != nil) != tt.wantErr {
				t.Errorf("Decoder(%s) at version %d error = %v, wantErr %v", dialect.OpCodeInfo(tt.op), tt.version, err, tt.wantErr)
			}
		})
	}
}

func TestRedisDialect_WithVersion(t *testing.T) {
	tests := []struct {
		name    string
		version int
		want    int
	}{
		{name: "zero", version: 0, want: redisMaxVersion},
		{name: "9", version: 9, want: 9},
		{name: "newest", version: redisMaxVersion, want: redisMaxVersion},
		{name: "negative", version: -1, want: redisMaxVersion},
		// A version above the newest one is reported as it is, the
		// read or the write that carries it fails.
		{name: "above the newest", version: redisMaxVersion + 1, want: redisMaxVersion + 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			magic, version := (RedisDialect{}).WithVersion(tt.version).Header()
			if magic != redisMagic || version != tt.want {
				t.Errorf("WithVersion(%d).Header() = (%q, %d), want = (%q, %d)", tt.version, magic, version, redisMagic, tt.want)
			}
		})
	}
}

func TestValkeyDialect_Decoder(t *testing.T) {
	tests := []struct {
		name    string
		op      OpCode
		version int
		wantErr bool
	}{
		{name: "SelectDB", op: ValkeyOpCodeSelectDB, version: 1},
		{name: "ExpirySeconds", op: ValkeyOpCodeExpirySeconds, version: 1},
		{name: "ExpiryMilli", op: ValkeyOpCodeExpiryMilli, version: 3},
		{name: "HashZipmap", op: ValkeyOpCodeHashZipmap, version: 2},
		{name: "ListZiplist", op: ValkeyOpCodeListZiplist, version: 2},
		{name: "SetIntset", op: ValkeyOpCodeSetIntset, version: 2},
		{name: "ZsetZiplist", op: ValkeyOpCodeZsetZiplist, version: 2},
		{name: "HashZiplist", op: ValkeyOpCodeHashZiplist, version: 4},
		{name: "ListQuicklist", op: ValkeyOpCodeListQuicklist, version: 7},
		{name: "Aux", op: ValkeyOpCodeAux, version: 7},
		{name: "ResizeDB", op: ValkeyOpCodeResizeDB, version: 7},
		{name: "Module2", op: ValkeyOpCodeModule2, version: 8},
		{name: "StreamListpacks", op: ValkeyOpCodeStreamListpacks, version: 9},
		{name: "Freq", op: ValkeyOpCodeFreq, version: 9},
		{name: "Idle", op: ValkeyOpCodeIdle, version: 9},
		{name: "ModuleAux", op: ValkeyOpCodeModuleAux, version: 9},
		{name: "Func2", op: ValkeyOpCodeFunc2, version: 10},
		{name: "HashListpack", op: ValkeyOpCodeHashListpack, version: 10},
		{name: "ZsetListpack", op: ValkeyOpCodeZsetListpack, version: 10},
		{name: "ListQuicklist2", op: ValkeyOpCodeListQuicklist2, version: 10},
		{name: "StreamListpacks2", op: ValkeyOpCodeStreamListpacks2, version: 10},
		{name: "SetListpack", op: ValkeyOpCodeSetListpack, version: 11},
		{name: "StreamListpacks3", op: ValkeyOpCodeStreamListpacks3, version: 11},

		{name: "HashMetadata", op: RedisOpCodeHashMetadata, wantErr: true},
		{name: "StreamListpacks4", op: RedisOpCodeStreamListpacks4, wantErr: true},
		{name: "HashTemplate", op: RedisOpCodeHashTemplate, wantErr: true},
		{name: "Func", op: ValkeyOpCodeFunc, wantErr: true},
		{name: "Cron", op: KeyDBOpCodeCron, wantErr: true},

		{name: "Hash2", op: ValkeyOpCodeHash2, version: 80},
		{name: "SlotImport", op: ValkeyOpCodeSlotImport, version: 80},
		{name: "SlotInfo", op: ValkeyOpCodeSlotInfo, version: 80},
	}
	dialect := ValkeyDialect{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := dialect.WithVersion(tt.version).Decoder(tt.op)
			if (err != nil) != tt.wantErr {
				t.Errorf("Decoder(%s) at version %d error = %v, wantErr %v", dialect.OpCodeInfo(tt.op), tt.version, err, tt.wantErr)
			}
		})
	}
}

func TestValkeyDialect_WithVersion(t *testing.T) {
	tests := []struct {
		name    string
		version int
		want    int
	}{
		{name: "zero", version: 0, want: valkeyMaxVersion},
		{name: "80", version: 80, want: 80},
		// A version above the newest one is reported as it is, the
		// read or the write that carries it fails.
		{name: "above the newest", version: valkeyMaxVersion + 1, want: valkeyMaxVersion + 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			magic, version := (ValkeyDialect{}).WithVersion(tt.version).Header()
			if magic != valkeyMagic || version != tt.want {
				t.Errorf("WithVersion(%d).Header() = (%q, %d), want = (%q, %d)", tt.version, magic, version, valkeyMagic, tt.want)
			}
		})
	}
}

func TestKeyDBDialect_Decoder(t *testing.T) {
	tests := []struct {
		name    string
		op      OpCode
		version int
		wantErr bool
	}{
		{name: "Cron", op: KeyDBOpCodeCron, version: 9},

		{name: "SelectDB", op: KeyDBOpCodeSelectDB, version: 1},
		{name: "ExpirySeconds", op: KeyDBOpCodeExpirySeconds, version: 1},
		{name: "ExpiryMilli", op: KeyDBOpCodeExpiryMilli, version: 3},
		{name: "HashZipmap", op: KeyDBOpCodeHashZipmap, version: 2},
		{name: "ListZiplist", op: KeyDBOpCodeListZiplist, version: 2},
		{name: "SetIntset", op: KeyDBOpCodeSetIntset, version: 2},
		{name: "ZsetZiplist", op: KeyDBOpCodeZsetZiplist, version: 2},
		{name: "HashZiplist", op: KeyDBOpCodeHashZiplist, version: 4},
		{name: "ListQuicklist", op: KeyDBOpCodeListQuicklist, version: 7},
		{name: "Aux", op: KeyDBOpCodeAux, version: 7},
		{name: "ResizeDB", op: KeyDBOpCodeResizeDB, version: 7},
		{name: "Module2", op: KeyDBOpCodeModule2, version: 8},
		{name: "StreamListpacks", op: KeyDBOpCodeStreamListpacks, version: 9},
		{name: "Freq", op: KeyDBOpCodeFreq, version: 9},
		{name: "Idle", op: KeyDBOpCodeIdle, version: 9},
		{name: "ModuleAux", op: KeyDBOpCodeModuleAux, version: 9},

		{name: "StreamListpacks2", op: RedisOpCodeStreamListpacks2, wantErr: true},
	}
	dialect := KeyDBDialect{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := dialect.WithVersion(tt.version).Decoder(tt.op)
			if (err != nil) != tt.wantErr {
				t.Errorf("Decoder(%s) at version %d error = %v, wantErr %v", dialect.OpCodeInfo(tt.op), tt.version, err, tt.wantErr)
			}
		})
	}
}

func TestFileEncoder_WriteToken(t *testing.T) {
	t.Run("op code newer than the dialect", func(t *testing.T) {
		var buf bytes.Buffer
		enc := NewFileEncoder(&buf, RedisDialect{V: 9})
		err := enc.WriteToken(Op(RedisOpCodeHashListpackEx))
		if !errors.Is(err, ErrOpCodeTooNew) {
			t.Fatalf("WriteToken() error = %v, want = %v", err, ErrOpCodeTooNew)
		}
		// The header carries the version, the record does not follow it.
		if got, want := buf.String(), "REDIS0009"; got != want {
			t.Fatalf("WriteToken() wrote %q, want = %q", got, want)
		}
	})

	t.Run("op code of the dialect", func(t *testing.T) {
		var buf bytes.Buffer
		enc := NewFileEncoder(&buf, RedisDialect{V: 12})
		if err := enc.WriteToken(Op(RedisOpCodeHashListpackEx)); err != nil {
			t.Fatalf("WriteToken() error = %v", err)
		}
		if got, want := buf.String(), "REDIS0012"+string([]byte{byte(RedisOpCodeHashListpackEx)}); got != want {
			t.Fatalf("WriteToken() wrote %q, want = %q", got, want)
		}
	})

	t.Run("op code of the newest version", func(t *testing.T) {
		var buf bytes.Buffer
		enc := NewFileEncoder(&buf, nil)
		if err := enc.WriteToken(Op(RedisOpCodeHashTemplate)); err != nil {
			t.Fatalf("WriteToken() error = %v", err)
		}
		if got, want := buf.String()[:9], "REDIS0015"; got != want {
			t.Fatalf("header = %q, want = %q", got, want)
		}
	})

	t.Run("version out of range fails", func(t *testing.T) {
		var buf bytes.Buffer
		enc := NewFileEncoder(&buf, RedisDialect{V: redisMaxVersion + 1})
		err := enc.WriteToken(Op(RedisOpCodeString))
		if !errors.Is(err, ErrInvalidVersion) {
			t.Fatalf("WriteToken() error = %v, want = %v", err, ErrInvalidVersion)
		}
		if buf.Len() != 0 {
			t.Fatalf("WriteToken() wrote %x, want nothing", buf.Bytes())
		}
	})

	t.Run("version below one falls back to the newest", func(t *testing.T) {
		for _, version := range []int{0, -1} {
			var buf bytes.Buffer
			enc := NewFileEncoder(&buf, RedisDialect{V: version})
			if err := enc.WriteToken(Op(RedisOpCodeHashListpackEx)); err != nil {
				t.Fatalf("WriteToken() error = %v", err)
			}
			if got, want := buf.String()[:9], "REDIS0015"; got != want {
				t.Fatalf("header = %q, want = %q", got, want)
			}
		}
	})
}

func TestEncoder_WriteToken(t *testing.T) {
	t.Run("op code of another dialect", func(t *testing.T) {
		var buf bytes.Buffer
		enc := NewEncoder(&buf, ValkeyDialect{})
		if err := enc.WriteToken(Op(RedisOpCodeHashMetadata)); !errors.Is(err, ErrInvalidOpCode) {
			t.Errorf("WriteToken(RedisOpCodeHashMetadata) error = %v, want = %v", err, ErrInvalidOpCode)
		}
		if err := enc.WriteToken(Op(KeyDBOpCodeCron)); !errors.Is(err, ErrInvalidOpCode) {
			t.Errorf("WriteToken(KeyDBOpCodeCron) error = %v, want = %v", err, ErrInvalidOpCode)
		}
	})
}
