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

// redisOpCodeInfo holds the version of the RDB format that introduced
// every op code of the dialect. The versions follow from the release
// history of Redis: a file of a version cannot contain the op codes of a
// later one, the dispatch of the dialect rejects them. The versions 4 and
// 5 introduced no op codes, the checksum is the addition of the version 5.
// Redis Enterprise writes its op codes in files of every version the
// dialect understands.
var redisOpCodeInfo = map[OpCode]OpCodeInfo{
	// RDB version 1, Redis 1.0.
	RedisOpCodeEndOfRDB:      {Ver: 1, Name: "EndOfRDB"},
	RedisOpCodeSelectDB:      {Ver: 1, Name: "SelectDB"},
	RedisOpCodeExpirySeconds: {Ver: 1, Name: "ExpirySeconds"},
	RedisOpCodeString:        {Ver: 1, Name: "String"},
	RedisOpCodeList:          {Ver: 1, Name: "List"},
	RedisOpCodeSet:           {Ver: 1, Name: "Set"},
	RedisOpCodeZset:          {Ver: 1, Name: "Zset"},
	RedisOpCodeHash:          {Ver: 1, Name: "Hash"},
	RedisOpCodeRamLru:        {Ver: 1, Name: "RamLru"},
	RedisOpCodeSlotNum:       {Ver: 1, Name: "SlotNum"},

	// RDB version 2, Redis 2.4.
	RedisOpCodeHashZipmap:  {Ver: 2, Name: "HashZipmap"},
	RedisOpCodeListZiplist: {Ver: 2, Name: "ListZiplist"},
	RedisOpCodeSetIntset:   {Ver: 2, Name: "SetIntset"},
	RedisOpCodeZsetZiplist: {Ver: 2, Name: "ZsetZiplist"},

	// RDB version 3, Redis 2.4.
	RedisOpCodeExpiryMilli: {Ver: 3, Name: "ExpiryMilli"},

	// RDB version 4, Redis 2.4.
	RedisOpCodeHashZiplist: {Ver: 4, Name: "HashZiplist"},

	// RDB version 7, Redis 3.2.
	RedisOpCodeListQuicklist: {Ver: 7, Name: "ListQuicklist"},
	RedisOpCodeAux:           {Ver: 7, Name: "Aux"},
	RedisOpCodeResizeDB:      {Ver: 7, Name: "ResizeDB"},

	// RDB version 8, Redis 4.0.
	RedisOpCodeZset2:       {Ver: 8, Name: "Zset2"},
	RedisOpCodeModulePreGa: {Ver: 8, Name: "ModulePreGa"},
	RedisOpCodeModule2:     {Ver: 8, Name: "Module2"},

	// RDB version 9, Redis 5.0.
	RedisOpCodeStreamListpacks: {Ver: 9, Name: "StreamListpacks"},
	RedisOpCodeFreq:            {Ver: 9, Name: "Freq"},
	RedisOpCodeIdle:            {Ver: 9, Name: "Idle"},
	RedisOpCodeModuleAux:       {Ver: 9, Name: "ModuleAux"},

	// RDB version 10, Redis 7.0.
	RedisOpCodeFunc:             {Ver: 10, Name: "Func"},
	RedisOpCodeFunc2:            {Ver: 10, Name: "Func2"},
	RedisOpCodeHashListpack:     {Ver: 10, Name: "HashListpack"},
	RedisOpCodeZsetListpack:     {Ver: 10, Name: "ZsetListpack"},
	RedisOpCodeListQuicklist2:   {Ver: 10, Name: "ListQuicklist2"},
	RedisOpCodeStreamListpacks2: {Ver: 10, Name: "StreamListpacks2"},

	// RDB version 11, Redis 7.2.
	RedisOpCodeSetListpack:      {Ver: 11, Name: "SetListpack"},
	RedisOpCodeStreamListpacks3: {Ver: 11, Name: "StreamListpacks3"},

	// RDB version 12, Redis 7.4.
	RedisOpCodeHashMetadataPreGa:   {Ver: 12, Name: "HashMetadataPreGa"},
	RedisOpCodeHashListpackExPreGa: {Ver: 12, Name: "HashListpackExPreGa"},
	RedisOpCodeHashMetadata:        {Ver: 12, Name: "HashMetadata"},
	RedisOpCodeHashListpackEx:      {Ver: 12, Name: "HashListpackEx"},
	RedisOpCodeSlotInfo:            {Ver: 12, Name: "SlotInfo"},

	// RDB version 13, Redis 8.6.
	RedisOpCodeKeyMeta:          {Ver: 13, Name: "KeyMeta"},
	RedisOpCodeStreamListpacks4: {Ver: 13, Name: "StreamListpacks4"},

	// RDB version 14, Redis 8.8.
	RedisOpCodeStreamListpacks5: {Ver: 14, Name: "StreamListpacks5"},
	RedisOpCodeArray:            {Ver: 14, Name: "Array"},

	// RDB version 15, Redis 8.10.
	RedisOpCodeHashTemplate:     {Ver: 15, Name: "HashTemplate"},
	RedisOpCodeHashTmplLP:       {Ver: 15, Name: "HashTmplLP"},
	RedisOpCodeHashTmplLpRef:    {Ver: 15, Name: "HashTmplLpRef"},
	RedisOpCodeHashTmplArray:    {Ver: 15, Name: "HashTmplArray"},
	RedisOpCodeHashTmplArrayRef: {Ver: 15, Name: "HashTmplArrayRef"},
	RedisOpCodeGCRA:             {Ver: 15, Name: "GCRA"},
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

func (d RedisDialect) Commands() commands.Table {
	return commands.RedisCommands
}

func (RedisDialect) OpCodeInfo(op OpCode) OpCodeInfo {
	info, ok := redisOpCodeInfo[op]
	if !ok {
		return OpCodeInfo{Name: "Unknown"}
	}
	return info
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
	return checkDecoder(d, op, handler, d.OpCodeInfo(op), ver)
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
	return checkEncoder(d, op, handler, d.OpCodeInfo(op), ver)
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
