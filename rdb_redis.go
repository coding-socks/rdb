package rdb

import "github.com/coding-socks/rdb/commands"

// RedisDialect is the dialect of Redis. It is the default of the [Encoder]
// and accepts every file with a Redis header this package understands.
type RedisDialect struct {
	// V is the version of the RDB format the dialect represents.
	// The zero version selects the newest one the dialect supports.
	V int
}

const (
	redisMagic      = "REDIS"
	redisMaxVersion = 15
)

// The op codes of the Redis dialect.
const (
	// RedisOpCodeEndOfRDB indicates the end of an RDB file.
	RedisOpCodeEndOfRDB OpCode = 0xff - iota
	// RedisOpCodeSelectDB indicates a new database.
	RedisOpCodeSelectDB
	// RedisOpCodeExpirySeconds indicates an expiry time in seconds.
	RedisOpCodeExpirySeconds
	// RedisOpCodeExpiryMilli indicates an expiry time in milliseconds.
	RedisOpCodeExpiryMilli
	// RedisOpCodeResizeDB indicates a hash table resize.
	RedisOpCodeResizeDB
	// RedisOpCodeAux indicates an auxiliary field.
	RedisOpCodeAux
	// RedisOpCodeFreq indicates an LFU frequency.
	RedisOpCodeFreq
	// RedisOpCodeIdle indicates an LRU idle time.
	RedisOpCodeIdle
	// RedisOpCodeModuleAux indicates module auxiliary data.
	RedisOpCodeModuleAux
	// RedisOpCodeFunc indicates function library data for 7.0 release candidate.
	//
	// Deprecated: RedisOpCodeFunc exists for historical compatibility.
	RedisOpCodeFunc
	// RedisOpCodeFunc2 indicates function library data.
	RedisOpCodeFunc2
	// RedisOpCodeSlotInfo indicates a slot ID and size, cluster mode only.
	RedisOpCodeSlotInfo
	// RedisOpCodeKeyMeta indicates key metadata.
	RedisOpCodeKeyMeta
	// RedisOpCodeHashTemplate indicates a hash template record (v15).
	RedisOpCodeHashTemplate
)

const (
	RedisOpCodeString OpCode = iota
	RedisOpCodeList
	RedisOpCodeSet
	RedisOpCodeZset
	RedisOpCodeHash
	RedisOpCodeZset2
	// RedisOpCodeModulePreGa indicates module data for 4.0 release candidate.
	//
	// Deprecated: RedisOpCodeModulePreGa exists for historical compatibility.
	RedisOpCodeModulePreGa
	RedisOpCodeModule2
	_
	RedisOpCodeHashZipmap
	RedisOpCodeListZiplist
	RedisOpCodeSetIntset
	RedisOpCodeZsetZiplist
	RedisOpCodeHashZiplist
	RedisOpCodeListQuicklist
	RedisOpCodeStreamListpacks
	RedisOpCodeHashListpack
	RedisOpCodeZsetListpack
	RedisOpCodeListQuicklist2
	RedisOpCodeStreamListpacks2
	RedisOpCodeSetListpack
	RedisOpCodeStreamListpacks3
	// RedisOpCodeHashMetadataPreGa indicates a hash with a minimum expiry of
	// the fields for the 7.4 release candidate.
	//
	// Deprecated: RedisOpCodeHashMetadataPreGa exists for historical
	// compatibility, use RedisOpCodeHashMetadata instead.
	RedisOpCodeHashMetadataPreGa
	// RedisOpCodeHashListpackExPreGa indicates a hash with per field expiry
	// for the 7.4 release candidate.
	//
	// Deprecated: RedisOpCodeHashListpackExPreGa exists for historical
	// compatibility, use RedisOpCodeHashListpackEx instead.
	RedisOpCodeHashListpackExPreGa
	RedisOpCodeHashMetadata
	RedisOpCodeHashListpackEx
	RedisOpCodeStreamListpacks4
	RedisOpCodeStreamListpacks5
	RedisOpCodeArray
	RedisOpCodeHashTmplLP
	RedisOpCodeHashTmplLpRef
	RedisOpCodeHashTmplArray
	RedisOpCodeHashTmplArrayRef
	RedisOpCodeGCRA
)

// Redis Enterprise specific op codes.
const (
	RedisOpCodeRamLru  OpCode = 107
	RedisOpCodeSlotNum OpCode = 121
)

// redisOpCodeVersion holds the version of the RDB format that introduced
// every op code of the dialect. The versions follow from the release
// history of Redis: a file of a version cannot contain the op codes of a
// later one, the dispatch of the dialect rejects them. The versions 4 and
// 5 introduced no op codes, the checksum is the addition of the version 5.
// Redis Enterprise writes its op codes in files of every version the
// dialect understands.
var redisOpCodeVersion = map[OpCode]int{
	// RDB version 1, Redis 1.0 through 2.2.
	RedisOpCodeEndOfRDB:      1,
	RedisOpCodeSelectDB:      1,
	RedisOpCodeExpirySeconds: 1,
	RedisOpCodeString:        1,
	RedisOpCodeList:          1,
	RedisOpCodeSet:           1,
	RedisOpCodeZset:          1,
	RedisOpCodeHash:          1,
	RedisOpCodeRamLru:        1,
	RedisOpCodeSlotNum:       1,

	// RDB version 2, Redis 2.4: the packed encodings split from the
	// types of Redis 1.0.
	RedisOpCodeHashZipmap:  2,
	RedisOpCodeListZiplist: 2,
	RedisOpCodeSetIntset:   2,
	RedisOpCodeZsetZiplist: 2,

	// RDB version 3, a development version between Redis 2.4 and 2.6.
	RedisOpCodeExpiryMilli: 3,

	// RDB version 4, the development builds of the cycle between
	// Redis 2.4 and 2.6 carry the hash ziplist encoding from here
	// on, no released Redis wrote the versions 3 to 5. The encoding
	// shipped with the version 6, the files of the versions 2 to 4
	// in the wild carry it as well.
	RedisOpCodeHashZiplist: 4,

	// RDB version 7, Redis 3.2.
	RedisOpCodeListQuicklist: 7,
	RedisOpCodeAux:           7,
	RedisOpCodeResizeDB:      7,

	// RDB version 8, Redis 4.0: the modules.
	RedisOpCodeZset2:       8,
	RedisOpCodeModulePreGa: 8,
	RedisOpCodeModule2:     8,

	// RDB version 9, Redis 5.0 through 6.2: the streams and the
	// memory policy of a key.
	RedisOpCodeStreamListpacks: 9,
	RedisOpCodeFreq:            9,
	RedisOpCodeIdle:            9,
	RedisOpCodeModuleAux:       9,

	// RDB version 10, Redis 7.0: the functions and the listpack
	// encodings.
	RedisOpCodeFunc:             10,
	RedisOpCodeFunc2:            10,
	RedisOpCodeHashListpack:     10,
	RedisOpCodeZsetListpack:     10,
	RedisOpCodeListQuicklist2:   10,
	RedisOpCodeStreamListpacks2: 10,

	// RDB version 11, Redis 7.2.
	RedisOpCodeSetListpack:      11,
	RedisOpCodeStreamListpacks3: 11,

	// RDB version 12, Redis 7.4 and 8.0 through 8.4: the expiry of
	// the hash fields and the slot sizes.
	RedisOpCodeHashMetadataPreGa:   12,
	RedisOpCodeHashListpackExPreGa: 12,
	RedisOpCodeHashMetadata:        12,
	RedisOpCodeHashListpackEx:      12,
	RedisOpCodeSlotInfo:            12,

	// RDB version 13, Redis 8.6: the key metadata and the
	// idempotency of the streams.
	RedisOpCodeKeyMeta:          13,
	RedisOpCodeStreamListpacks4: 13,

	// RDB version 14, Redis 8.8.
	RedisOpCodeStreamListpacks5: 14,
	RedisOpCodeArray:            14,

	// RDB version 15, Redis 8.10: the hash templates. GCRA never
	// shipped below, Redis keeps it behind a compile time flag and
	// moved its number when the templates took the types 29 to 32.
	RedisOpCodeHashTemplate:     15,
	RedisOpCodeHashTmplLP:       15,
	RedisOpCodeHashTmplLpRef:    15,
	RedisOpCodeHashTmplArray:    15,
	RedisOpCodeHashTmplArrayRef: 15,
	RedisOpCodeGCRA:             15,
}

// Header returns the magic string of Redis and the version of the
// dialect. The zero V selects the newest one the dialect
// supports.
func (d RedisDialect) Header() (string, int) {
	if d.V <= 0 {
		return redisMagic, redisMaxVersion
	}
	return redisMagic, d.V
}

// WithVersion returns the dialect bound to the version. The zero
// version binds the newest one the dialect supports.
func (d RedisDialect) WithVersion(version int) Dialect {
	return RedisDialect{V: version}
}

// introduced returns the version of the RDB format that introduced the
// op code, see [redisOpCodeVersion].
func (RedisDialect) introduced(op OpCode) int {
	return redisOpCodeVersion[op]
}

func (d RedisDialect) Commands() commands.Table {
	return commands.RedisCommands
}

// OpCodeString returns the name of the op code in the Redis dialect.
func (RedisDialect) OpCodeString(op OpCode) string {
	switch op {
	case RedisOpCodeEndOfRDB:
		return "EndOfRDB"
	case RedisOpCodeSelectDB:
		return "SelectDB"
	case RedisOpCodeExpirySeconds:
		return "ExpirySeconds"
	case RedisOpCodeExpiryMilli:
		return "ExpiryMilli"
	case RedisOpCodeResizeDB:
		return "ResizeDB"
	case RedisOpCodeAux:
		return "Aux"
	case RedisOpCodeFreq:
		return "Freq"
	case RedisOpCodeIdle:
		return "Idle"
	case RedisOpCodeModuleAux:
		return "ModuleAux"
	case RedisOpCodeFunc:
		return "Func (deprecated)"
	case RedisOpCodeFunc2:
		return "Func2"
	case RedisOpCodeSlotInfo:
		return "SlotInfo"
	case RedisOpCodeKeyMeta:
		return "KeyMeta"
	case RedisOpCodeHashTemplate:
		return "HashTemplate"

	case RedisOpCodeString:
		return "String"
	case RedisOpCodeList:
		return "List"
	case RedisOpCodeSet:
		return "Set"
	case RedisOpCodeZset:
		return "Zset"
	case RedisOpCodeHash:
		return "Hash"
	case RedisOpCodeZset2:
		return "Zset2"
	case RedisOpCodeModulePreGa:
		return "ModulePreGa"
	case RedisOpCodeModule2:
		return "Module2"
	case RedisOpCodeHashZipmap:
		return "HashZipmap"
	case RedisOpCodeListZiplist:
		return "ListZiplist"
	case RedisOpCodeSetIntset:
		return "SetIntset"
	case RedisOpCodeZsetZiplist:
		return "ZsetZiplist"
	case RedisOpCodeHashZiplist:
		return "HashZiplist"
	case RedisOpCodeListQuicklist:
		return "ListQuicklist"
	case RedisOpCodeStreamListpacks:
		return "StreamListpacks"
	case RedisOpCodeHashListpack:
		return "HashListpack"
	case RedisOpCodeZsetListpack:
		return "ZsetListpack"
	case RedisOpCodeListQuicklist2:
		return "ListQuicklist2"
	case RedisOpCodeStreamListpacks2:
		return "StreamListpacks2"
	case RedisOpCodeSetListpack:
		return "SetListpack"
	case RedisOpCodeStreamListpacks3:
		return "StreamListpacks3"
	case RedisOpCodeHashMetadataPreGa:
		return "HashMetadataPreGa (deprecated)"
	case RedisOpCodeHashListpackExPreGa:
		return "HashListpackExPreGa (deprecated)"
	case RedisOpCodeHashMetadata:
		return "HashMetadata"
	case RedisOpCodeHashListpackEx:
		return "HashListpackEx"
	case RedisOpCodeStreamListpacks4:
		return "StreamListpacks4"
	case RedisOpCodeStreamListpacks5:
		return "StreamListpacks5"
	case RedisOpCodeArray:
		return "Array"
	case RedisOpCodeHashTmplLP:
		return "HashTmplLP"
	case RedisOpCodeHashTmplLpRef:
		return "HashTmplLpRef"
	case RedisOpCodeHashTmplArray:
		return "HashTmplArray"
	case RedisOpCodeHashTmplArrayRef:
		return "HashTmplArrayRef"
	case RedisOpCodeGCRA:
		return "GCRA"
	case RedisOpCodeRamLru:
		return "RamLru"
	case RedisOpCodeSlotNum:
		return "SlotNum"
	}
	return "Unknown"
}

// Decoder returns the token decoder of the Redis op code. It fails
// with [ErrInvalidOpCode] when the dialect assigns no meaning to the
// op code and with [ErrOpCodeTooNew] when the op code was introduced
// in a version newer than the one of the dialect.
func (d RedisDialect) Decoder(op OpCode) (TokenDecoder, error) {
	handler, ok := redisDecoder(op)
	if !ok {
		return nil, errInvalidOpCode(d, op)
	}
	_, ver := d.Header()
	return checkDecoder(d, op, handler, d.introduced(op), ver)
}

// redisDecoder returns the token decoder of the op code, ok reports
// whether the dialect supports it.
func redisDecoder(op OpCode) (TokenDecoder, bool) {
	switch op {
	case RedisOpCodeExpirySeconds,
		RedisOpCodeExpiryMilli:
		return ExpiryTokenDecoder{}, true
	case RedisOpCodeFunc2:
		return StringTokensDecoder{N: 1}, true
	case RedisOpCodeAux,
		RedisOpCodeString:
		return StringTokensDecoder{N: 2}, true
	case RedisOpCodeSelectDB,
		RedisOpCodeIdle:
		return UintTokensDecoder{N: 1}, true
	case RedisOpCodeResizeDB:
		return UintTokensDecoder{N: 2}, true
	case RedisOpCodeSlotInfo:
		return UintTokensDecoder{N: 3}, true
	case RedisOpCodeFreq:
		return ByteTokenDecoder{N: 1}, true
	case RedisOpCodeRamLru,
		RedisOpCodeSlotNum:
		// Redis Enterprise specific op codes. The value is irrelevant outside
		// of Redis Enterprise.
		return UintTokensDecoder{N: 1}, true

	case RedisOpCodeKeyMeta:
		return KeyMetaDecoder{}, true
	case RedisOpCodeHashTemplate:
		return HashTemplateDecoder{}, true

	case RedisOpCodeModule2:
		return Module2Decoder{}, true
	case RedisOpCodeModuleAux:
		return Module2Decoder{Step: 1}, true

	case RedisOpCodeList,
		RedisOpCodeSet:
		return KeyDecoder{ListDecoder{}}, true
	case RedisOpCodeZset,
		RedisOpCodeZset2:
		return KeyDecoder{ZsetDecoder{}}, true
	case RedisOpCodeHash:
		return KeyDecoder{HashDecoder{}}, true
	case RedisOpCodeListQuicklist:
		return KeyDecoder{QuickListDecoder{}}, true
	case RedisOpCodeListQuicklist2:
		return KeyDecoder{QuickList2Decoder{}}, true

	case RedisOpCodeHashZipmap:
		return ZipPackDecoder{}, true
	case RedisOpCodeListZiplist,
		RedisOpCodeHashListpackExPreGa,
		RedisOpCodeHashTmplLpRef:
		return ZipListDecoder{}, true
	case RedisOpCodeSetIntset,
		RedisOpCodeSetListpack:
		return SetIntSetDecoder{}, true
	case RedisOpCodeZsetZiplist,
		RedisOpCodeZsetListpack:
		return ZsetListDecoder{}, true
	case RedisOpCodeHashZiplist,
		RedisOpCodeHashListpack:
		return ZipPackDecoder{}, true

	case RedisOpCodeStreamListpacks,
		RedisOpCodeStreamListpacks2,
		RedisOpCodeStreamListpacks3,
		RedisOpCodeStreamListpacks4,
		RedisOpCodeStreamListpacks5:
		return KeyDecoder{StreamListDecoder{}}, true

	case RedisOpCodeHashMetadataPreGa:
		return KeyDecoder{HashMetadataDecoder{Step: 1}}, true
	case RedisOpCodeHashMetadata:
		return KeyDecoder{HashMetadataDecoder{}}, true
	case RedisOpCodeHashListpackEx:
		return KeyDecoder{HashListpackExDecoder{}}, true

	case RedisOpCodeHashTmplLP:
		return KeyDecoder{HashTmplSelfDecoder{LP: true}}, true
	case RedisOpCodeHashTmplArray:
		return KeyDecoder{HashTmplSelfDecoder{}}, true
	case RedisOpCodeHashTmplArrayRef:
		return KeyDecoder{HashTmplArrayRefDecoder{}}, true

	case RedisOpCodeArray:
		return KeyDecoder{ArrayDecoder{}}, true
	}
	return nil, false
}

// Encoder is the write side of [RedisDialect.Decoder].
func (d RedisDialect) Encoder(op OpCode) (TokenEncoder, error) {
	handler := redisEncoder(op)
	if handler == nil {
		return nil, errInvalidOpCode(d, op)
	}
	_, ver := d.Header()
	return checkEncoder(d, op, handler, d.introduced(op), ver)
}

// redisEncoder returns the token encoder of the op code, ok reports
// whether the dialect supports it.
func redisEncoder(op OpCode) TokenEncoder {
	switch op {
	default:
		return nil

	case RedisOpCodeExpirySeconds,
		RedisOpCodeExpiryMilli:
		return ExpiryTokenEncoder{}
	case RedisOpCodeFunc2:
		return StringTokensEncoder{N: 1}
	case RedisOpCodeAux,
		RedisOpCodeString:
		return StringTokensEncoder{N: 2}
	case RedisOpCodeSelectDB,
		RedisOpCodeIdle:
		return UintTokensEncoder{N: 1}
	case RedisOpCodeResizeDB:
		return UintTokensEncoder{N: 2}
	case RedisOpCodeSlotInfo:
		return UintTokensEncoder{N: 3}
	case RedisOpCodeFreq:
		return ByteTokenEncoder{N: 1}
	case RedisOpCodeRamLru,
		RedisOpCodeSlotNum:
		return UintTokensEncoder{N: 1}

	case RedisOpCodeKeyMeta:
		return KeyMetaEncoder{}
	case RedisOpCodeHashTemplate:
		return HashTemplateEncoder{}

	case RedisOpCodeModule2:
		return Module2Encoder{}
	case RedisOpCodeModuleAux:
		return Module2Encoder{Step: 1}

	case RedisOpCodeList,
		RedisOpCodeSet:
		return KeyTokenEncoder{ListEncoder{}}
	case RedisOpCodeZset,
		RedisOpCodeZset2:
		return KeyTokenEncoder{ZsetEncoder{}}
	case RedisOpCodeHash:
		return KeyTokenEncoder{HashEncoder{}}
	case RedisOpCodeListQuicklist:
		return KeyTokenEncoder{QuickListEncoder{}}
	case RedisOpCodeListQuicklist2:
		return KeyTokenEncoder{QuickList2Encoder{}}

	case RedisOpCodeHashZipmap:
		return ZipPackEncoder{}
	case RedisOpCodeListZiplist,
		RedisOpCodeHashListpackExPreGa,
		RedisOpCodeHashTmplLpRef:
		return ZipListEncoder{}
	case RedisOpCodeSetIntset,
		RedisOpCodeSetListpack:
		return SetIntSetEncoder{}
	case RedisOpCodeZsetZiplist,
		RedisOpCodeZsetListpack:
		return ZsetListEncoder{}
	case RedisOpCodeHashZiplist,
		RedisOpCodeHashListpack:
		return ZipPackEncoder{}

	case RedisOpCodeStreamListpacks,
		RedisOpCodeStreamListpacks2,
		RedisOpCodeStreamListpacks3,
		RedisOpCodeStreamListpacks4,
		RedisOpCodeStreamListpacks5:
		return KeyTokenEncoder{StreamListEncoder{}}

	case RedisOpCodeHashMetadataPreGa:
		return KeyTokenEncoder{HashMetadataEncoder{Step: 1}}
	case RedisOpCodeHashMetadata:
		return KeyTokenEncoder{HashMetadataEncoder{}}
	case RedisOpCodeHashListpackEx:
		return KeyTokenEncoder{HashListpackExEncoder{}}

	case RedisOpCodeHashTmplLP:
		return KeyTokenEncoder{HashTmplSelfEncoder{LP: true}}
	case RedisOpCodeHashTmplArray:
		return KeyTokenEncoder{HashTmplSelfEncoder{}}
	case RedisOpCodeHashTmplArrayRef:
		return KeyTokenEncoder{HashTmplArrayRefEncoder{}}

	case RedisOpCodeArray:
		return KeyTokenEncoder{ArrayEncoder{}}
	}
}
