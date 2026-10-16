package rdb

import "github.com/coding-socks/rdb/commands"

// ValkeyDialect is the dialect of Valkey. Valkey 9.0 restarted the RDB
// version numbering at 80 under its own magic string, the versions 12 to
// 79 are reserved for the Redis forks the format does not share, and the
// versions up to 11 were written with the REDIS magic, which the Redis
// dialect detects. The Valkey dialect gives a different meaning to value
// type 22 and op code 243 than Redis, value types and op codes Redis
// introduced after Valkey forked are not supported.
type ValkeyDialect struct {
	// V is the version of the RDB format the dialect represents.
	// The zero version selects the newest one the dialect supports.
	V int
}

const (
	valkeyMagic      = "VALKEY"
	valkeyMaxVersion = 80
)

// The op codes of the Valkey dialect.
const (
	// ValkeyOpCodeEndOfRDB indicates the end of an RDB file.
	ValkeyOpCodeEndOfRDB OpCode = 0xff - iota
	// ValkeyOpCodeSelectDB indicates a new database.
	ValkeyOpCodeSelectDB
	// ValkeyOpCodeExpirySeconds indicates expiry time in seconds.
	ValkeyOpCodeExpirySeconds
	// ValkeyOpCodeExpiryMilli indicates expiry time in ms.
	ValkeyOpCodeExpiryMilli
	// ValkeyOpCodeResizeDB indicates Hash table resize.
	ValkeyOpCodeResizeDB
	// ValkeyOpCodeAux indicates RDB aux field.
	ValkeyOpCodeAux
	// ValkeyOpCodeFreq indicates LFU frequency.
	ValkeyOpCodeFreq
	// ValkeyOpCodeIdle indicates LRU idle time.
	ValkeyOpCodeIdle
	// ValkeyOpCodeModuleAux indicates Module auxiliary data.
	ValkeyOpCodeModuleAux
	// ValkeyOpCodeFunc indicates function library data for 7.0 release candidate.
	//
	// Deprecated: ValkeyOpCodeFunc exists for historical compatibility.
	ValkeyOpCodeFunc
	// ValkeyOpCodeFunc2 indicates function library data.
	ValkeyOpCodeFunc2
	// ValkeyOpCodeSlotInfo indicates slot id and size (cluster mode only).
	ValkeyOpCodeSlotInfo
	// ValkeyOpCodeSlotImport stores the state of a slot import.
	ValkeyOpCodeSlotImport
)

const (
	ValkeyOpCodeString OpCode = iota
	ValkeyOpCodeList
	ValkeyOpCodeSet
	ValkeyOpCodeZset
	ValkeyOpCodeHash
	ValkeyOpCodeZset2
	_ // 6 is the Redis ModulePreGa
	ValkeyOpCodeModule2
	_ // 8
	ValkeyOpCodeHashZipmap
	ValkeyOpCodeListZiplist
	ValkeyOpCodeSetIntset
	ValkeyOpCodeZsetZiplist
	ValkeyOpCodeHashZiplist
	ValkeyOpCodeListQuicklist
	ValkeyOpCodeStreamListpacks
	ValkeyOpCodeHashListpack
	ValkeyOpCodeZsetListpack
	ValkeyOpCodeListQuicklist2
	ValkeyOpCodeStreamListpacks2
	ValkeyOpCodeSetListpack
	ValkeyOpCodeStreamListpacks3

	// ValkeyOpCodeHash2 is a hash with per field expiry.
	ValkeyOpCodeHash2
)

// valkeyOpCodeInfo holds the version of the RDB format that
// introduced every op code of the dialect. The op codes Valkey shares
// with Redis carry the versions of the Redis dialect up to the fork of
// Valkey 7.2 (the version 11), the op codes of the split the version
// restart of Valkey 9.0. The dispatch of the dialect rejects the op
// codes the map does not carry.
var valkeyOpCodeInfo = map[OpCode]OpCodeInfo{
	// RDB version 1, Redis 1.0.
	ValkeyOpCodeSelectDB:      {Ver: 1, Name: "SelectDB"},
	ValkeyOpCodeExpirySeconds: {Ver: 1, Name: "ExpirySeconds"},
	ValkeyOpCodeString:        {Ver: 1, Name: "String"},
	ValkeyOpCodeList:          {Ver: 1, Name: "List"},
	ValkeyOpCodeSet:           {Ver: 1, Name: "Set"},
	ValkeyOpCodeZset:          {Ver: 1, Name: "Zset"},
	ValkeyOpCodeHash:          {Ver: 1, Name: "Hash"},

	// RDB version 2, Redis 2.4.
	ValkeyOpCodeHashZipmap:  {Ver: 2, Name: "HashZipmap"},
	ValkeyOpCodeListZiplist: {Ver: 2, Name: "ListZiplist"},
	ValkeyOpCodeSetIntset:   {Ver: 2, Name: "SetIntset"},
	ValkeyOpCodeZsetZiplist: {Ver: 2, Name: "ZsetZiplist"},

	// RDB version 3, Redis 2.4.
	ValkeyOpCodeExpiryMilli: {Ver: 3, Name: "ExpiryMilli"},

	// RDB version 4, Redis 2.4.
	ValkeyOpCodeHashZiplist: {Ver: 4, Name: "HashZiplist"},

	// RDB version 7, Redis 3.2.
	ValkeyOpCodeListQuicklist: {Ver: 7, Name: "ListQuicklist"},
	ValkeyOpCodeAux:           {Ver: 7, Name: "Aux"},
	ValkeyOpCodeResizeDB:      {Ver: 7, Name: "ResizeDB"},

	// RDB version 8, Redis 4.0.
	ValkeyOpCodeZset2:   {Ver: 8, Name: "Zset2"},
	ValkeyOpCodeModule2: {Ver: 8, Name: "Module2"},

	// RDB version 9, Redis 5.0.
	ValkeyOpCodeStreamListpacks: {Ver: 9, Name: "StreamListpacks"},
	ValkeyOpCodeFreq:            {Ver: 9, Name: "Freq"},
	ValkeyOpCodeIdle:            {Ver: 9, Name: "Idle"},
	ValkeyOpCodeModuleAux:       {Ver: 9, Name: "ModuleAux"},

	// RDB version 10, Redis 7.0.
	ValkeyOpCodeFunc:             {Ver: 10, Name: "Func"},
	ValkeyOpCodeFunc2:            {Ver: 10, Name: "Func2"},
	ValkeyOpCodeHashListpack:     {Ver: 10, Name: "HashListpack"},
	ValkeyOpCodeZsetListpack:     {Ver: 10, Name: "ZsetListpack"},
	ValkeyOpCodeListQuicklist2:   {Ver: 10, Name: "ListQuicklist2"},
	ValkeyOpCodeStreamListpacks2: {Ver: 10, Name: "StreamListpacks2"},

	// RDB version 11, Redis 7.2.
	ValkeyOpCodeSetListpack:      {Ver: 11, Name: "SetListpack"},
	ValkeyOpCodeStreamListpacks3: {Ver: 11, Name: "StreamListpacks3"},

	// RDB version 80, Valkey 9.0.
	ValkeyOpCodeHash2:      {Ver: 80, Name: "Hash2"},
	ValkeyOpCodeSlotImport: {Ver: 80, Name: "SlotImport"},
	ValkeyOpCodeSlotInfo:   {Ver: 80, Name: "SlotInfo"},
}

// Header returns the magic string of Valkey and the version of the
// dialect. The zero V selects the newest one the dialect
// supports.
func (d ValkeyDialect) Header() (string, int) {
	if d.V <= 0 {
		return valkeyMagic, valkeyMaxVersion
	}
	return valkeyMagic, d.V
}

// WithVersion returns the dialect bound to the version. The zero
// version binds the newest one the dialect supports.
func (d ValkeyDialect) WithVersion(version int) Dialect {
	return ValkeyDialect{V: version}
}

func (d ValkeyDialect) Commands() commands.Table {
	return commands.ValkeyCommands
}

func (ValkeyDialect) OpCodeInfo(op OpCode) OpCodeInfo {
	info, ok := valkeyOpCodeInfo[op]
	if !ok {
		return OpCodeInfo{Name: "Unknown"}
	}
	return info
}

// Decoder returns the token decoder of the Valkey op code. It fails
// with [ErrInvalidOpCode] when the dialect assigns no meaning to the
// op code and with [ErrOpCodeTooNew] when the op code was introduced
// in a version newer than the one of the dialect.
func (d ValkeyDialect) Decoder(op OpCode) (TokenDecoder, error) {
	handler, ok := valkeyDecoder(op)
	if !ok {
		return nil, errInvalidOpCode(d, op)
	}
	_, ver := d.Header()
	return checkDecoder(d, op, handler, d.OpCodeInfo(op), ver)
}

// valkeyDecoder returns the token decoder of the op code, ok reports
// whether the dialect supports it.
func valkeyDecoder(op OpCode) (TokenDecoder, bool) {
	switch op {
	case ValkeyOpCodeExpirySeconds,
		ValkeyOpCodeExpiryMilli:
		return ExpiryTokenDecoder{}, true
	case ValkeyOpCodeFunc2:
		return StringTokensDecoder{N: 1}, true
	case ValkeyOpCodeAux,
		ValkeyOpCodeString:
		return StringTokensDecoder{N: 2}, true
	case ValkeyOpCodeSelectDB,
		ValkeyOpCodeIdle:
		return UintTokensDecoder{N: 1}, true
	case ValkeyOpCodeResizeDB:
		return UintTokensDecoder{N: 2}, true
	case ValkeyOpCodeSlotInfo:
		return UintTokensDecoder{N: 3}, true
	case ValkeyOpCodeFreq:
		return ByteTokenDecoder{N: 1}, true
	case ValkeyOpCodeSlotImport:
		return SlotImportDecoder{}, true
	case ValkeyOpCodeModuleAux:
		return Module2Decoder{Step: 1}, true
	case ValkeyOpCodeModule2:
		return Module2Decoder{}, true

	case ValkeyOpCodeList,
		ValkeyOpCodeSet:
		return KeyDecoder{ListDecoder{}}, true
	case ValkeyOpCodeZset,
		ValkeyOpCodeZset2:
		return KeyDecoder{ZsetDecoder{}}, true
	case ValkeyOpCodeHash:
		return KeyDecoder{HashDecoder{}}, true
	case ValkeyOpCodeListQuicklist:
		return KeyDecoder{QuickListDecoder{}}, true
	case ValkeyOpCodeListQuicklist2:
		return KeyDecoder{QuickList2Decoder{}}, true

	case ValkeyOpCodeHashZipmap:
		return ZipPackDecoder{}, true
	case ValkeyOpCodeListZiplist:
		return ZipListDecoder{}, true
	case ValkeyOpCodeSetIntset,
		ValkeyOpCodeSetListpack:
		return SetIntSetDecoder{}, true
	case ValkeyOpCodeZsetZiplist,
		ValkeyOpCodeZsetListpack:
		return ZsetListDecoder{}, true
	case ValkeyOpCodeHashZiplist,
		ValkeyOpCodeHashListpack:
		return ZipPackDecoder{}, true

	case ValkeyOpCodeStreamListpacks,
		ValkeyOpCodeStreamListpacks2,
		ValkeyOpCodeStreamListpacks3:
		return KeyDecoder{StreamListDecoder{}}, true

	case ValkeyOpCodeHash2:
		return KeyDecoder{Hash2Decoder{}}, true
	}
	return nil, false
}

// Encoder is the write side of [ValkeyDialect.Decoder].
func (d ValkeyDialect) Encoder(op OpCode) (TokenEncoder, error) {
	handler, ok := valkeyEncoder(op)
	if !ok {
		return nil, errInvalidOpCode(d, op)
	}
	_, ver := d.Header()
	return checkEncoder(d, op, handler, d.OpCodeInfo(op), ver)
}

// valkeyEncoder returns the token encoder of the op code, ok reports
// whether the dialect supports it.
func valkeyEncoder(op OpCode) (TokenEncoder, bool) {
	switch op {
	case ValkeyOpCodeExpirySeconds,
		ValkeyOpCodeExpiryMilli:
		return ExpiryTokenEncoder{}, true
	case ValkeyOpCodeFunc2:
		return StringTokensEncoder{N: 1}, true
	case ValkeyOpCodeAux,
		ValkeyOpCodeString:
		return StringTokensEncoder{N: 2}, true
	case ValkeyOpCodeSelectDB,
		ValkeyOpCodeIdle:
		return UintTokensEncoder{N: 1}, true
	case ValkeyOpCodeResizeDB:
		return UintTokensEncoder{N: 2}, true
	case ValkeyOpCodeSlotInfo:
		return UintTokensEncoder{N: 3}, true
	case ValkeyOpCodeFreq:
		return ByteTokenEncoder{N: 1}, true
	case ValkeyOpCodeSlotImport:
		return SlotImportEncoder{}, true
	case ValkeyOpCodeModuleAux:
		return Module2Encoder{Step: 1}, true
	case ValkeyOpCodeModule2:
		return Module2Encoder{}, true

	case ValkeyOpCodeList,
		ValkeyOpCodeSet:
		return KeyTokenEncoder{ListEncoder{}}, true
	case ValkeyOpCodeZset,
		ValkeyOpCodeZset2:
		return KeyTokenEncoder{ZsetEncoder{}}, true
	case ValkeyOpCodeHash:
		return KeyTokenEncoder{HashEncoder{}}, true
	case ValkeyOpCodeListQuicklist:
		return KeyTokenEncoder{QuickListEncoder{}}, true
	case ValkeyOpCodeListQuicklist2:
		return KeyTokenEncoder{QuickList2Encoder{}}, true

	case ValkeyOpCodeHashZipmap:
		return ZipPackEncoder{}, true
	case ValkeyOpCodeListZiplist:
		return ZipListEncoder{}, true
	case ValkeyOpCodeSetIntset,
		ValkeyOpCodeSetListpack:
		return SetIntSetEncoder{}, true
	case ValkeyOpCodeZsetZiplist,
		ValkeyOpCodeZsetListpack:
		return ZsetListEncoder{}, true
	case ValkeyOpCodeHashZiplist,
		ValkeyOpCodeHashListpack:
		return ZipPackEncoder{}, true

	case ValkeyOpCodeStreamListpacks,
		ValkeyOpCodeStreamListpacks2,
		ValkeyOpCodeStreamListpacks3:
		return KeyTokenEncoder{StreamListEncoder{}}, true

	case ValkeyOpCodeHash2:
		return KeyTokenEncoder{Hash2Encoder{}}, true
	}
	return nil, false
}
