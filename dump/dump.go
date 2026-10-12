// Package dump provides the tokens and the codec of a scan: a Token
// names a key of the keyspace, carries the value the DUMP command
// returns and the time the key lives, and the Encoder and the Decoder
// convert it into the record of a key of an RDB file.
package dump

// Token is one key of a scan: its name, the value the DUMP command
// returns and the time the key lives.
type Token struct {
	// Key is the name of the key.
	Key []byte
	// Payload is the value the DUMP command returns:
	// [type][value][version 2][crc 8].
	Payload []byte
	// TTL is the time to live of the key in milliseconds, -1 when
	// the key does not expire.
	TTL int64
}

// TypeOK reports whether keys of a type have an RDB representation
// here.
func TypeOK(typ string) (string, bool) {
	switch typ {
	case "string", "list", "set", "zset", "hash", "stream":
		return typ, true
	}
	return "", false
}
