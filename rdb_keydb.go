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
	_ // 6 is the Redis ModulePreGa
	KeyDBOpCodeModule2
	_ // 8
	KeyDBOpCodeHashZipmap
	KeyDBOpCodeListZiplist
	KeyDBOpCodeSetIntset
	KeyDBOpCodeZsetZiplist
	KeyDBOpCodeHashZiplist
	KeyDBOpCodeListQuicklist
	KeyDBOpCodeStreamListpacks
	KeyDBOpCodeHashListpack
	KeyDBOpCodeZsetListpack
)

// KeyDB specific op codes.
const (
	// KeyDBOpCodeCron stores a cron job.
	KeyDBOpCodeCron OpCode = 64
)

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

// introduced returns the version of the RDB format that introduced the
// op code: the cron op code is the one of the fork, the shared ones
// carry the versions of the Redis dialect.
func (d KeyDBDialect) introduced(op OpCode) int {
	if op == KeyDBOpCodeCron {
		return keydbMaxVersion
	}
	return redisOpCodeVersion[op]
}

func (d KeyDBDialect) Commands() commands.Table {
	return commands.KeyDBCommands
}

// OpCodeString returns the name of the op code in the KeyDB dialect.
func (KeyDBDialect) OpCodeString(op OpCode) string {
	switch op {
	case KeyDBOpCodeEndOfRDB:
		return "EndOfRDB"
	case KeyDBOpCodeSelectDB:
		return "SelectDB"
	case KeyDBOpCodeExpirySeconds:
		return "ExpirySeconds"
	case KeyDBOpCodeExpiryMilli:
		return "ExpiryMilli"
	case KeyDBOpCodeResizeDB:
		return "ResizeDB"
	case KeyDBOpCodeAux:
		return "Aux"
	case KeyDBOpCodeFreq:
		return "Freq"
	case KeyDBOpCodeIdle:
		return "Idle"
	case KeyDBOpCodeModuleAux:
		return "ModuleAux"

	case KeyDBOpCodeString:
		return "String"
	case KeyDBOpCodeList:
		return "List"
	case KeyDBOpCodeSet:
		return "Set"
	case KeyDBOpCodeZset:
		return "Zset"
	case KeyDBOpCodeHash:
		return "Hash"
	case KeyDBOpCodeZset2:
		return "Zset2"
	case KeyDBOpCodeModule2:
		return "Module2"
	case KeyDBOpCodeHashZipmap:
		return "HashZipmap"
	case KeyDBOpCodeListZiplist:
		return "ListZiplist"
	case KeyDBOpCodeSetIntset:
		return "SetIntset"
	case KeyDBOpCodeZsetZiplist:
		return "ZsetZiplist"
	case KeyDBOpCodeHashZiplist:
		return "HashZiplist"
	case KeyDBOpCodeListQuicklist:
		return "ListQuicklist"
	case KeyDBOpCodeStreamListpacks:
		return "StreamListpacks"
	case KeyDBOpCodeHashListpack:
		return "HashListpack"
	case KeyDBOpCodeZsetListpack:
		return "ZsetListpack"
	case KeyDBOpCodeCron:
		return "Cron"
	}
	return "Unknown"
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
	return checkDecoder(d, op, handler, d.introduced(op), ver)
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
	return checkEncoder(d, op, handler, d.introduced(op), ver)
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
