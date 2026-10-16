package rdb

import "github.com/coding-socks/rdb/commands"

// KeyDBDialect is the dialect of KeyDB. KeyDB files share the Redis magic
// string, so the dialect cannot be detected from a header and has to be set
// explicitly on the [Decoder].
type KeyDBDialect struct {
	// V is the version of the RDB format the dialect represents.
	// The zero version selects the newest one the dialect supports.
	V int
}

const (
	keydbMagic      = redisMagic
	keydbMaxVersion = 9
)

// The op codes of the KeyDB dialect.
const (
	// KeyDBOpCodeEndOfRDB indicates the end of an RDB file.
	KeyDBOpCodeEndOfRDB OpCode = 0xff - iota
	// KeyDBOpCodeSelectDB indicates a new database.
	KeyDBOpCodeSelectDB
	// KeyDBOpCodeExpirySeconds indicates an expiry time in seconds.
	KeyDBOpCodeExpirySeconds
	// KeyDBOpCodeExpiryMilli indicates an expiry time in milliseconds.
	KeyDBOpCodeExpiryMilli
	// KeyDBOpCodeResizeDB indicates a hash table resize.
	KeyDBOpCodeResizeDB
	// KeyDBOpCodeAux indicates an auxiliary field.
	KeyDBOpCodeAux
	// KeyDBOpCodeFreq indicates an LFU frequency.
	KeyDBOpCodeFreq
	// KeyDBOpCodeIdle indicates an LRU idle time.
	KeyDBOpCodeIdle
	// KeyDBOpCodeModuleAux indicates module auxiliary data.
	KeyDBOpCodeModuleAux
)

const (
	KeyDBOpCodeString OpCode = iota
	KeyDBOpCodeList
	KeyDBOpCodeSet
	KeyDBOpCodeZset
	KeyDBOpCodeHash
	KeyDBOpCodeZset2
	// KeyDBOpCodeModulePreGa indicates module data for 4.0 release candidate.
	//
	// Deprecated: KeyDBOpCodeModulePreGa exists for historical compatibility.
	KeyDBOpCodeModulePreGa
	KeyDBOpCodeModule2
	_ // 8
	KeyDBOpCodeHashZipmap
	KeyDBOpCodeListZiplist
	KeyDBOpCodeSetIntset
	KeyDBOpCodeZsetZiplist
	KeyDBOpCodeHashZiplist
	KeyDBOpCodeListQuicklist
	KeyDBOpCodeStreamListpacks
)

// KeyDB specific op codes.
const (
	// KeyDBOpCodeCron stores a cron job.
	KeyDBOpCodeCron OpCode = 64
)

// keyDBOpCodeInfo holds the info of the RDB format that introduced
// every op code of the dialect. The versions follow from the release
// history of Redis: a file of a version cannot contain the op codes of a
// later one, the dispatch of the dialect rejects them. The versions 4 and
// 5 introduced no op codes, the checksum is the addition of the version 5.
// Redis Enterprise writes its op codes in files of every version the
// dialect understands.
var keyDBOpCodeInfo = map[OpCode]OpCodeInfo{
	// RDB version 1, Redis 1.0.
	KeyDBOpCodeEndOfRDB:      {Ver: 1, Name: "EndOfRDB"},
	KeyDBOpCodeSelectDB:      {Ver: 1, Name: "SelectDB"},
	KeyDBOpCodeExpirySeconds: {Ver: 1, Name: "ExpirySeconds"},
	KeyDBOpCodeString:        {Ver: 1, Name: "String"},
	KeyDBOpCodeList:          {Ver: 1, Name: "List"},
	KeyDBOpCodeSet:           {Ver: 1, Name: "Set"},
	KeyDBOpCodeZset:          {Ver: 1, Name: "Zset"},
	KeyDBOpCodeHash:          {Ver: 1, Name: "Hash"},

	// RDB version 2, Redis 2.4.
	KeyDBOpCodeHashZipmap:  {Ver: 2, Name: "HashZipmap"},
	KeyDBOpCodeListZiplist: {Ver: 2, Name: "ListZiplist"},
	KeyDBOpCodeSetIntset:   {Ver: 2, Name: "SetIntset"},
	KeyDBOpCodeZsetZiplist: {Ver: 2, Name: "ZsetZiplist"},

	// RDB version 3, Redis 2.4.
	KeyDBOpCodeExpiryMilli: {Ver: 3, Name: "ExpiryMilli"},

	// RDB version 4, Redis 2.4.
	KeyDBOpCodeHashZiplist: {Ver: 4, Name: "HashZiplist"},

	// RDB version 7, Redis 3.2.
	KeyDBOpCodeListQuicklist: {Ver: 7, Name: "ListQuicklist"},
	KeyDBOpCodeAux:           {Ver: 7, Name: "Aux"},
	KeyDBOpCodeResizeDB:      {Ver: 7, Name: "ResizeDB"},

	// RDB version 8, Redis 4.0.
	KeyDBOpCodeZset2:       {Ver: 8, Name: "Zset2"},
	KeyDBOpCodeModulePreGa: {Ver: 8, Name: "ModulePreGa"},
	KeyDBOpCodeModule2:     {Ver: 8, Name: "Module2"},

	// RDB version 9, Redis 5.0.
	KeyDBOpCodeStreamListpacks: {Ver: 9, Name: "StreamListpacks"},
	KeyDBOpCodeFreq:            {Ver: 9, Name: "Freq"},
	KeyDBOpCodeIdle:            {Ver: 9, Name: "Idle"},
	KeyDBOpCodeModuleAux:       {Ver: 9, Name: "ModuleAux"},

	// KeyDB added a Cron type.
	KeyDBOpCodeCron: {Ver: 9, Name: "Cron"},
}

// Header returns the magic string of KeyDB and the version of the
// dialect. The zero V selects the newest one the dialect
// supports.
func (d KeyDBDialect) Header() (string, int) {
	if d.V <= 0 {
		return keydbMagic, keydbMaxVersion
	}
	return keydbMagic, d.V
}

// WithVersion returns the dialect bound to the version. The zero
// version binds the newest one the dialect supports.
func (d KeyDBDialect) WithVersion(version int) Dialect {
	return KeyDBDialect{V: version}
}

func (d KeyDBDialect) Commands() commands.Table {
	return commands.KeyDBCommands
}

func (KeyDBDialect) OpCodeInfo(op OpCode) OpCodeInfo {
	info, ok := keyDBOpCodeInfo[op]
	if !ok {
		return OpCodeInfo{Name: "Unknown"}
	}
	return info
}

// Decoder returns the token decoder of the KeyDB op code. It fails
// with [ErrInvalidOpCode] when the dialect assigns no meaning to the
// op code and with [ErrOpCodeTooNew] when the op code was introduced
// in a version newer than the one of the dialect.
func (d KeyDBDialect) Decoder(op OpCode) (TokenDecoder, error) {
	handler, ok := keydbDecoder(op)
	if !ok {
		return nil, errInvalidOpCode(d, op)
	}
	_, ver := d.Header()
	return checkDecoder(d, op, handler, d.OpCodeInfo(op), ver)
}

// keydbDecoder returns the token decoder of the op code, ok reports
// whether the dialect supports it.
func keydbDecoder(op OpCode) (TokenDecoder, bool) {
	switch op {
	case KeyDBOpCodeExpirySeconds,
		KeyDBOpCodeExpiryMilli:
		return ExpiryTokenDecoder{}, true
	case KeyDBOpCodeAux,
		KeyDBOpCodeString:
		return StringTokensDecoder{N: 2}, true
	case KeyDBOpCodeSelectDB,
		KeyDBOpCodeIdle:
		return UintTokensDecoder{N: 1}, true
	case KeyDBOpCodeResizeDB:
		return UintTokensDecoder{N: 2}, true
	case KeyDBOpCodeFreq:
		return ByteTokenDecoder{N: 1}, true
	case KeyDBOpCodeModuleAux:
		return Module2Decoder{Step: 1}, true
	case KeyDBOpCodeModule2:
		return Module2Decoder{}, true

	case KeyDBOpCodeList,
		KeyDBOpCodeSet:
		return KeyDecoder{ListDecoder{}}, true
	case KeyDBOpCodeZset,
		KeyDBOpCodeZset2:
		return KeyDecoder{ZsetDecoder{}}, true
	case KeyDBOpCodeHash:
		return KeyDecoder{HashDecoder{}}, true
	case KeyDBOpCodeListQuicklist:
		return KeyDecoder{QuickListDecoder{}}, true

	case KeyDBOpCodeHashZipmap:
		return ZipPackDecoder{}, true
	case KeyDBOpCodeListZiplist:
		return ZipListDecoder{}, true
	case KeyDBOpCodeSetIntset:
		return SetIntSetDecoder{}, true
	case KeyDBOpCodeZsetZiplist:
		return ZsetListDecoder{}, true
	case KeyDBOpCodeHashZiplist:
		return ZipPackDecoder{}, true
	case KeyDBOpCodeStreamListpacks:
		return KeyDecoder{StreamListDecoder{}}, true

	case KeyDBOpCodeCron:
		return KeyDecoder{CronDecoder{}}, true
	}
	return nil, false
}

// Encoder is the write side of [KeyDBDialect.Decoder].
func (d KeyDBDialect) Encoder(op OpCode) (TokenEncoder, error) {
	handler, ok := keydbEncoder(op)
	if !ok {
		return nil, errInvalidOpCode(d, op)
	}
	_, ver := d.Header()
	return checkEncoder(d, op, handler, d.OpCodeInfo(op), ver)
}

// keydbEncoder returns the token encoder of the op code, ok reports
// whether the dialect supports it.
func keydbEncoder(op OpCode) (TokenEncoder, bool) {
	switch op {
	case KeyDBOpCodeExpirySeconds,
		KeyDBOpCodeExpiryMilli:
		return ExpiryTokenEncoder{}, true
	case KeyDBOpCodeAux,
		KeyDBOpCodeString:
		return StringTokensEncoder{N: 2}, true
	case KeyDBOpCodeSelectDB,
		KeyDBOpCodeIdle:
		return UintTokensEncoder{N: 1}, true
	case KeyDBOpCodeResizeDB:
		return UintTokensEncoder{N: 2}, true
	case KeyDBOpCodeFreq:
		return ByteTokenEncoder{N: 1}, true
	case KeyDBOpCodeModuleAux:
		return Module2Encoder{Step: 1}, true
	case KeyDBOpCodeModule2:
		return Module2Encoder{}, true

	case KeyDBOpCodeList,
		KeyDBOpCodeSet:
		return KeyTokenEncoder{ListEncoder{}}, true
	case KeyDBOpCodeZset,
		KeyDBOpCodeZset2:
		return KeyTokenEncoder{ZsetEncoder{}}, true
	case KeyDBOpCodeHash:
		return KeyTokenEncoder{HashEncoder{}}, true
	case KeyDBOpCodeListQuicklist:
		return KeyTokenEncoder{QuickListEncoder{}}, true

	case KeyDBOpCodeHashZipmap:
		return ZipPackEncoder{}, true
	case KeyDBOpCodeListZiplist:
		return ZipListEncoder{}, true
	case KeyDBOpCodeSetIntset:
		return SetIntSetEncoder{}, true
	case KeyDBOpCodeZsetZiplist:
		return ZsetListEncoder{}, true
	case KeyDBOpCodeHashZiplist:
		return ZipPackEncoder{}, true
	case KeyDBOpCodeStreamListpacks:
		return KeyTokenEncoder{StreamListEncoder{}}, true

	case KeyDBOpCodeCron:
		return KeyTokenEncoder{CronEncoder{}}, true
	}
	return nil, false
}
