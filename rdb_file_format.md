# RDB File Format

This document describes RDB (Redis Database), a binary file format used by Redis and other compatible technologies, like [Valkey](https://github.com/valkey-io/valkey), [KeyDB](https://github.com/Snapchat/KeyDB), [DragonflyDB](https://github.com/dragonflydb/dragonfly).

The goal of this document is to define the format of RDB files in an informal way. It was created for myself based on the source code of Redis. Any kind of contribution is greatly appreciated.

A typical RDB file has the following structure:

```
Magic String "REDIS"
RDB Version Number as ASCII string. e.g "0003"
Auxiliary field
  + Key
  + Value
Database selector
  + Value type (Op code)
    + Key
    + Value
Database selector
...
End of RDB file indicator
CRC64 checksum of the entire file.
```

## The RDB file

All the data inside the Redis instance can be dumbed in the form of an RDB file.

### The Database Header

The first 9 bytes of the database file are the database file header.

| Offset | Size | Description                        |
|--------|------|------------------------------------|
| 0      | 5    | The header string: "REDIS"         |
| 5      | 4    | RDB Version Number as ASCII string |

#### Magic Header String

Every valid RDB file begins with the following 5 bytes (in hex): 0x52 0x45 0x44 0x49 0x53. This byte sequence corresponds to the UTF-8 string "REDIS".

```
0x52 0x45 0x44 0x49 0x53  # "REDIS"
```

#### Version Number

The 4 bytes value beginning at offset 5 determines RDB version number used to generating the file. It is stored as an ASCII string and always padded with leading zeroes to ensure exactly 4 bytes.

```
0x30 0x30 0x30 0x33 # "0003" => Version 3
```

If you are wondering which Redis versions introduced which RDB version, here is a list.

| Redis version | RDB version |
|---------------|-------------|
| 8.10          | 15          |
| 8.8           | 14          |
| 8.6           | 13          |
| 7.4           | 12          |
| 7.2           | 11          |
| 7.0           | 10          |
| 5.0           | 9           |
| 4.0           | 8           |
| 3.2           | 7           |
| 2.6           | 6           |
| 2.4           | 5           |

### Dialects

The format described in this document follows Redis. The compatible technologies mentioned at the top diverge from it, and from each other, in ways that can make their files unreadable for each other. The subsections document the known incompatibilities of the Valkey and KeyDB dialects against Redis.

#### Valkey

Valkey 7.x and 8.x write `REDIS` files of version `11` that are indistinguishable from Redis. With Valkey 9.0 the dialects split:

- **Magic string and version.** Valkey 9.0 writes the magic string `VALKEY` followed by a three digit version number, for example `VALKEY080`, where Redis requires `REDIS`. The version numbering is offset as well: Valkey jumped to version `80` and reserves versions `12`-`79` for the incompatible Redis formats. With the default strict version check Valkey accepts `REDIS` files of version `1`-`11` and `VALKEY` files of version `80` and higher; it rejects a `REDIS` file of version `12` or higher, which includes every Redis 7.4 and 8.x file. Redis cannot read `VALKEY` files at all. `DUMP` payloads carry a version but no magic string; in strict mode Valkey accepts only versions `1`-`11` and `80`+ there.
- **Missing features.** Valkey has no equivalent for the Redis value types `22`-`243`.
- **Aux fields.** The version aux field is written as `valkey-ver` instead of `redis-ver`.

As a safeguard, Valkey rejects every type and op code in the range `22`-`243` while loading a `REDIS` file.

The Hash2 layout (hashtable encoding only, a listpack encoded hash is saved as `16` HashListpack):

1. The first token is the OpCode `0x16`.
2. The key name in string encoded form.
3. The number of entries in length encoded form.
4. Entries:
    1. The field name in string encoded form.
    2. The value in string encoded form.
    3. The field expiry in milliseconds stored using 8 bytes in Little endian format. A field without expiry is stored as `0xFFFFFFFFFFFFFFFF`.

The SlotImport layout:

1. The first token is the OpCode `0xF3`.
2. The job name in string encoded form (40 bytes).
3. The number of slot ranges in length encoded form.
4. Slot ranges:
    1. The start slot in length encoded form.
    2. The end slot in length encoded form.

#### KeyDB

KeyDB writes `REDIS` files of version `9` and shares the layout of every type and op code it supports with Redis. The dialect differences are:

- **Value type `64` Cron** (documented [below](#64-cron)). The number is outside the valid type range of both Redis (0-33) and Valkey (0-22), so both reject such files.
- **Missing features.** KeyDB only knows value types `0`-`7`, `9`-`15` and `64`. The types `16`-`33` are unknown to it, as are the op codes `242`-`246` (HashTemplate, KeyMeta, SlotInfo, Func2 and the pre-GA Func).
- **Per-field expiration via aux fields.** KeyDB predates the hash field expiration types of Redis (`24`/`25`) and Valkey (`22`). It persists the expiry of a hash field as a pair of aux fields that follow the key they belong to: `keydb-subexpire-key` with the field name and `keydb-subexpire-when` with the expiry in milliseconds, both as strings. Redis skips unknown aux fields, so it reads such files but silently drops the field expiries.
- **MVCC timestamp aux field.** In active replica mode KeyDB writes the aux field `mvcc-tstamp` (decimal string) before the type, key name and value of a key.

### Encoding

The RDB file uses a variable byte encoding designed to use as few bytes as possible. The first bytes indicate the structure and sometimes only that single byte is used.

| First two bits | Unused bits | Usable Bits | Representation                                                                            |
|----------------|-------------|-------------|-------------------------------------------------------------------------------------------|
| 00             | 0           | 6           | 00xx xxxx                                                                                 |
| 01             | 0           | 14          | 01xx xxxx xxxx xxxx                                                                       |
| 10             | 5           | 32          | 10-- ---0 xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx                                         |
| 10             | 5           | 64          | 10-- ---1 xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx |
| 11             | 0           | ?           | 11-- ----                                                                                 |

When the first two bit is `11` the data is stored in a special encoding. The remaining 6 bits indicate the exact structure.

| First byte | Description                                        |
|------------|----------------------------------------------------|
| 1100 0000  | An 8 bit integer follows in Little endian format.  |
| 1100 0001  | An 16 bit integer follows in Little endian format. |
| 1100 0010  | An 32 bit integer follows in Little endian format. |
| 1100 0011  | An LZF compressed string follows.                  |

An LZF compressed string has a special format. First the compressed length, the uncompressed length, then the compressed data.

### Module ID

Module ID charset "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"

The decoded ID always has 9 byte length. Each char is stored using 6 bites. This also means, that the last 10 bits are not used.

```
0                   1
0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7
0 0 0 0 0 0 1 1 1 1 1 1 2 2 2 2 2 2

    2                   3
8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5
3 3 3 3 3 3 4 4 4 4 4 4 5 5 5 5 5 5

        4                   5
6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3
6 6 6 6 6 6 7 7 7 7 7 7 8 8 8 8 8 8

            6
4 5 6 7 8 9 0 1 2 3
- - - - - - - - - -
```

(It feels like bas64 encoding.)

### Special op codes

Each part after the initial header is introduced by a special op code.

0xFF EndOfRDB indicates the end of an RDB file.
0xFE SelectDB indicates a new database.
0xFD ExpirySeconds indicates expiry time in seconds.
0xFC ExpiryMilli indicates expiry time in ms.
0xFB ResizeDB indicates Hash table resize.
0xFA Aux indicates RDB aux field.
0xF9 Freq indicates LFU frequency.
0xF8 Idle indicates LRU idle time.
0xF7 ModuleAux indicates Module auxiliary data.
0xF6 Func indicates function library data for 7.0 rc1 and rc2.
0xF5 Func2 indicates function library data.
0xF4 SlotInfo indicates slot id and size (cluster mode only).
0xF3 KeyMeta indicates key metadata (module metadata classes).
0xF2 HashTemplate indicates a hash template record (v15).

#### `255` EndOfRDB

A `0xFF` op code indicates the end of the RDB file. It is followed by an 8 byte checksum or 8 bytes of zero.

The checksum was added in version 5.

#### `254` SelectDB

A `0xFE` op code is followed by a variable length field which indicates the database number.

```
0xFE<length encoding : database number>
```

See [Length encoding](#Length-encoding).

#### `253` ExpirySeconds

A `0xFD` op code is followed by a Unix timestamp with second precision stored using 4 bytes in Little endian format.

```
0xFD<4 bytes>
```

See [year 2038 problem](https://en.wikipedia.org/wiki/Year_2038_problem).

#### `252` ExpiryMilli

A `0xFC` op code is followed by a Unix timestamp with millisecond precision stored using 8 bytes in Little endian format.

```
0xFC<8 bytes>
```

#### `251` ResizeDB

1. The first token is the OpCode `0xFB`.
2. The key count in length encoded form.
3. The expiry count in length encoded form.

A `0xFB` op code is followed by a length encoded key count and a length encoded expiry count.

```
0xFB<length encoding : key count><length encoding : expiry count>
```

#### `250` Aux

1. The first token is the OpCode `0xFA`.
2. The key name in string encoded form.
3. The value in string encoded form.

Possible key values:

- redis-ver: The Redis Version that wrote the RDB
- redis-bits: Bit architecture of the system that wrote the RDB, either 32 or 64
- ctime: Creation time of the RDB
- used-mem: Used memory of the instance that wrote the RDB
- repl-stream-db: ?
- repl-id: ?
- repl-offset: ?
- aof-base: ?

#### `249` Freq

A `0xF9` op code is followed by the LFU frequency stored using 1 byte.

```
0xF9<1 byte>
```

#### `248` Idle

A `0xF8` op code is followed by the LRU idle time stored using 1 byte.

```
0xF8<length encoding : LRU idle time>
```

#### `247` ModuleAux

```
0xF7<length encoding : Module ID>
```

Load module data that is not related to the Redis key space. Such data can be potentially be stored both before and after the RDB keys-values section.

#### `246` Func

Deprecated: Pre-release function format.

#### `245` Func2

1. The first token is the OpCode `0xF5`.
2. The function in string encoded form.

The payload requires additional decoding.

#### `244` SlotInfo

1. The first token is the OpCode `0xF4`.
2. The slot ID in length encoded form.
3. The slot size in length encoded form.
4. The expires slot size in length encoded form.

#### `243` KeyMeta

Key metadata for module metadata classes. The metadata belongs to the key that follows it.

1. The first token is the OpCode `0xF3`.
2. The number of metadata classes in length encoded form.
3. Metadata classes:
    1. The class specification stored using 4 raw bytes.
    2. A sequence of module values, each introduced by a module op code in length encoded form, terminated by the EOF op code:
        1. `0` EOF terminates the class.
        2. `1` signed integer in length encoded form.
        3. `2` unsigned integer in length encoded form.
        4. `3` single precision float stored using 4 bytes.
        5. `4` double precision float stored using 8 bytes.
        6. `5` string in string encoded form.

#### `242` HashTemplate

A single hash template record (v15). Templates define a shared, strictly ascending list of hash field names. Hash values stored with the template-based types reference a template by its ID. Template IDs are registered when the record is parsed; a duplicate ID or a zero field count is invalid, and a referencing type with an unknown ID is invalid.

1. The first token is the OpCode `0xF2`.
2. The template ID in length encoded form.
3. The number of fields in length encoded form. It must be greater than zero.
4. Fields:
    1. The field name in string encoded form.

The field names must be strictly ascending when compared by length first, then bytes.

### Value encodings

The value type indicates the encoding of the value. These keys always start with an op code, followed by a key name as string and the data. The format of the data depends on the op code.

#### `0` String

1. The first token is the OpCode `0x00`.
2. The key name in string encoded form.
3. The value in string encoded form.

#### `1` List

1. The first token is the OpCode `0x01`.
2. The key name in string encoded form.
3. The number of entries in length encoded form.
4. Entries:
    1. The entry value in string encoded form.

#### `2` Set

1. The first token is the OpCode `0x02`.
2. The key name in string encoded form.
3. The number of entries in length encoded form.
4. Entries:
    1. The entry value in string encoded form.

#### `3` ZSet

1. The first token is the OpCode `0x03`.
2. The key name in string encoded form.
3. The number of entries in length encoded form.
4. Entries:
    1. The entry key in length encoded form.
    2. The entry (float64) value in string encoded form.

If the len of the value is `0xff`, `0xfe`, or `0xfd`, then use the following table:

| value len | float value |
|-----------|-------------|
| 0xff      | -Inf        |
| 0xfe      | +Inf        |
| 0xfd      | NaN         |

#### `4` Hash

1. The first token is the OpCode `0x04`.
2. The key name in string encoded form.
3. The number of entries in length encoded form.
4. Entries:
    1. The entry member in string encoded form.
    2. The entry value in string encoded form.

#### `5` Zset2

1. The first token is the OpCode `0x05`.
2. The key name in string encoded form.
3. The number of entries in length encoded form.
4. Entries:
    1. The entry member in length encoded form.
    2. The entry score (float64) in raw 8-byte format.

#### `6` ModulePreGa

Deprecated: Pre-release (4.0 RC) module format.

#### `7` Module2

1. The first token is the OpCode `0x07`.
2. The key name in string encoded form.
3. The module ID in module ID form.

#### `9` HashZipmap

1. The first token is the OpCode `0x09`.
2. The key name in string encoded form.
3. The entries as a single value in string encoded form.

#### `10` ListZiplist

1. The first token is the OpCode `0x0A`.
2. The key name in string encoded form.
3. The entries as a single value in string encoded form.

#### `11` SetIntset

An Intset is a specially encoded set of integers. It is stored as a single string.

1. The first token is the OpCode `0x0B`.
2. The key name in string encoded form.
3. The entries as a single value in string encoded form.

#### `12` ZsetZiplist

1. The first token is the OpCode `0x0C`.
2. The key name in string encoded form.
3. The entries as a single value in string encoded form.

#### `13` HashZiplist

1. The first token is the OpCode `0x0D`.
2. The key name in string encoded form.
3. The entries as a single value in string encoded form.

#### `14` ListQuicklist

1. The first token is the OpCode `0x0E`.
2. The key name in string encoded form.
3. The number of entries in length encoded form.
4. Entries:
    1. The entry in string encoded form.

#### `15` StreamListpacks

1. The first token is the OpCode `0x0F`.
2. The key name in string encoded form.
3. The number of entries in length encoded form.
4. The entries them self.
    1. The stream ID in string encoded form (17 bytes).
    2. The stream entry in string encoded form.
5. The stream length in length encoded form.
6. The last stream ID in two length encoded fields.
7. Stream Groups:
    1. The group name in string encoded form.
    2. The last stream ID delivered to the group in two length encoded fields.
    3. The number of pending entries in length encoded form.
    4. Pending entries:
        1. The stream ID stored using 16 raw bytes.
        2. The delivery time in milliseconds stored using 8 bytes in Little endian format.
        3. The delivery count in length encoded form.
    5. The number of consumers in length encoded form.
    6. Consumers:
        1. The consumer name in string encoded form.
        2. The last seen time in milliseconds stored using 8 bytes in Little endian format.
        3. The number of pending entries in length encoded form.
        4. Pending entries:
            1. The stream ID stored using 16 raw bytes.

#### `16` HashListpack

1. The first token is the OpCode `0x10`.
2. The key name in string encoded form.
3. The entries as a single value in string encoded form.

#### `17` ZsetListpack

1. The first token is the OpCode `0x11`.
2. The key name in string encoded form.
3. The entries as a single value in string encoded form.

#### `18` ListQuicklist2

1. The first token is the OpCode `0x12`.
2. The key name in string encoded form.
3. The number of entries in length encoded form.
4. Entries:
    1. The entry type ID in length encoded form.
    2. The list entry in string encoded form.

The entry type can be "plain" (1) or "packed" (2).

#### `19` StreamListpacks2

1. The first token is the OpCode `0x13`.
2. The key name in string encoded form.
3. The number of entries in length encoded form.
4. Entries:
    1. The stream ID in string encoded form (17 bytes).
    2. The stream entry in string encoded form.
5. The stream length in length encoded form.
6. The last stream ID in two length encoded fields.
7. The first stream ID in two length encoded fields.
8. The max deleted ID in two length encoded fields.
9. The added count in length encoded form.
10. The number of stream groups in length encoded form.
11. Stream Groups:
    1. The group name in string encoded form.
    2. The last stream ID delivered to the group in two length encoded fields.
    3. The group entries-read counter in length encoded form.
    4. The number of pending entries in length encoded form.
    5. Pending entries:
        1. The stream ID stored using 16 raw bytes.
        2. The delivery time in milliseconds stored using 8 bytes in Little endian format.
        3. The delivery count in length encoded form.
    6. The number of consumers in length encoded form.
    7. Consumers:
        1. The consumer name in string encoded form.
        2. The last seen time in milliseconds stored using 8 bytes in Little endian format.
        3. The number of pending entries in length encoded form.
        4. Pending entries:
            1. The stream ID stored using 16 raw bytes.

#### `20` SetListpack

1. The first token is the OpCode `0x14`.
2. The key name in string encoded form.
3. The entries as a single value in string encoded form.

#### `21` StreamListpacks3

1. The first token is the OpCode `0x15`.
2. The key name in string encoded form.
3. The number of entries in length encoded form.
4. Entries:
    1. The stream ID in string encoded form (17 bytes).
    2. The stream entry in string encoded form.
5. The stream length in length encoded form.
6. The last stream ID in two length encoded fields.
7. The first stream ID in two length encoded fields.
8. The max deleted ID in two length encoded fields.
9. The added count in length encoded form.
10. The number of stream groups in length encoded form.
11. Stream Groups:
    1. The group name in string encoded form.
    2. The last stream ID delivered to the group in two length encoded fields.
    3. The group entries-read counter in length encoded form.
    4. The number of pending entries in length encoded form.
    5. Pending entries:
        1. The stream ID stored using 16 raw bytes.
        2. The delivery time in milliseconds stored using 8 bytes in Little endian format.
        3. The delivery count in length encoded form.
    6. The number of consumers in length encoded form.
    7. Consumers:
        1. The consumer name in string encoded form.
        2. The last seen time in milliseconds stored using 8 bytes in Little endian format.
        3. The last active time in milliseconds stored using 8 bytes in Little endian format.
        4. The number of pending entries in length encoded form.
        5. Pending entries:
            1. The stream ID stored using 16 raw bytes.

#### `22` HashMetadataPreGa

Deprecated: Pre-release (7.4 RC) format of HashMetadata.

Valkey incompatibility: Valkey 9.0 assigns type `22` to Hash2, a hash with field-level expiration with a different layout, see [Valkey](#valkey).

#### `23` HashListpackExPreGa

Deprecated: Pre-release (7.4 RC) format of HashListpackEx.

#### `24` HashMetadata

1. The first token is the OpCode `0x18`.
2. The key name in string encoded form.
3. The minimum expiry of the fields in milliseconds stored using 8 bytes in Little endian format.
4. The number of fields in length encoded form.
5. Entries:
    1. The field expiry in length encoded form. `0` means no expiry, otherwise the value is relative to the minimum expiry with an offset of `1`.
    2. The field name in string encoded form.
    3. The field value in string encoded form.

#### `25` HashListpackEx

1. The first token is the OpCode `0x19`.
2. The key name in string encoded form.
3. The minimum expiry of the fields in milliseconds stored using 8 bytes in Little endian format.
4. The entries as a single value in listpack encoded form. The listpack stores a triple of field, value and expiry. The expiry is `0` for no expiry, otherwise the value is relative to the minimum expiry with an offset of `1`.

#### `26` StreamListpacks4

Stream with IDMP (Idempotent Message Producer) support, appended after all the stream groups.

1. The first token is the OpCode `0x1A`.
2. The key name in string encoded form.
3. The number of entries in length encoded form.
4. Entries:
    1. The stream ID in string encoded form (17 bytes).
    2. The stream entry in string encoded form.
5. The stream length in length encoded form.
6. The last stream ID in two length encoded fields.
7. The first stream ID in two length encoded fields.
8. The max deleted ID in two length encoded fields.
9. The added count in length encoded form.
10. The number of stream groups in length encoded form.
11. Stream Groups:
    1. The group name in string encoded form.
    2. The last stream ID delivered to the group in two length encoded fields.
    3. The group entries-read counter in length encoded form.
    4. The number of pending entries in length encoded form.
    5. Pending entries:
        1. The stream ID stored using 16 raw bytes.
        2. The delivery time in milliseconds stored using 8 bytes in Little endian format.
        3. The delivery count in length encoded form.
    6. The number of consumers in length encoded form.
    7. Consumers:
        1. The consumer name in string encoded form.
        2. The last seen time in milliseconds stored using 8 bytes in Little endian format.
        3. The last active time in milliseconds stored using 8 bytes in Little endian format.
        4. The number of pending entries in length encoded form.
        5. Pending entries:
            1. The stream ID stored using 16 raw bytes.
12. IDMP data:
    1. The IDMP duration in length encoded form.
    2. The IDMP maximum entries in length encoded form.
    3. The number of producers in length encoded form.
    4. Producers:
        1. The producer PID in string encoded form.
        2. The number of entries in length encoded form.
        3. Entries:
            1. The entry IID in string encoded form.
            2. The stream ID in two length encoded fields.
    5. The all-time count of added IIDs in length encoded form.
    6. The all-time count of duplicate IIDs in length encoded form.

#### `27` StreamListpacks5

Stream with XNACK support. Each consumer group stores a NACK zone after the consumers.

1. The first token is the OpCode `0x1B`.
2. The key name in string encoded form.
3. The number of entries in length encoded form.
4. Entries:
    1. The stream ID in string encoded form (17 bytes).
    2. The stream entry in string encoded form.
5. The stream length in length encoded form.
6. The last stream ID in two length encoded fields.
7. The first stream ID in two length encoded fields.
8. The max deleted ID in two length encoded fields.
9. The added count in length encoded form.
10. The number of stream groups in length encoded form.
11. Stream Groups:
    1. The group name in string encoded form.
    2. The last stream ID delivered to the group in two length encoded fields.
    3. The group entries-read counter in length encoded form.
    4. The number of pending entries in length encoded form.
    5. Pending entries:
        1. The stream ID stored using 16 raw bytes.
        2. The delivery time in milliseconds stored using 8 bytes in Little endian format.
        3. The delivery count in length encoded form.
    6. The number of consumers in length encoded form.
    7. Consumers:
        1. The consumer name in string encoded form.
        2. The last seen time in milliseconds stored using 8 bytes in Little endian format.
        3. The last active time in milliseconds stored using 8 bytes in Little endian format.
        4. The number of pending entries in length encoded form.
        5. Pending entries:
            1. The stream ID stored using 16 raw bytes.
    8. The NACK zone entry count in length encoded form.
    9. NACK zone entries:
        1. The stream ID stored using 16 raw bytes.

#### `28` Array

A sparse array (v14). Elements are stored sparsely, each tagged with its index and type.

1. The first token is the OpCode `0x1C`.
2. The key name in string encoded form.
3. The number of elements in length encoded form. It must be greater than zero.
4. The insert index flag in length encoded form. If it is `1`, the insert index follows:
    1. The insert index in length encoded form.
5. Elements:
    1. The element index in length encoded form. Special encodings are not allowed.
    2. The element type tag in length encoded form:
        1. `0` string in string encoded form.
        2. `1` signed 64-bit integer stored using 8 bytes in Little endian format.
        3. `2` double stored using 8 bytes in Little endian format.
        4. `3` short string in string encoded form.

#### `29` HashTmplLP

A template-encoded hash (v15) that carries its field names inline, with the values stored as a single listpack.

1. The first token is the OpCode `0x1D`.
2. The key name in string encoded form.
3. The field names format in length encoded form:
    1. `0` field names stored as a single listpack. Integer entries decode to their decimal string.
    2. `1` explicit field list:
        1. The number of fields in length encoded form. It must be greater than zero.
        2. Fields:
            1. The field name in string encoded form.

The field names must be strictly ascending when compared by length first, then bytes.

4. The values as a single listpack, whose first entry is the template ID (ignored here) and the remaining entries are the values in field order. The number of values must match the field count.

#### `30` HashTmplLpRef

A template-encoded hash (v15) that references a template registered by a [HashTemplate](#242-hashtemplate) record, with the values stored as a single listpack.

1. The first token is the OpCode `0x1E`.
2. The key name in string encoded form.
3. The values as a single listpack, whose first entry is the referenced template ID and the remaining entries are the values in field order. The template ID must be a non-negative integer, and the number of values must match the template's field count.

#### `31` HashTmplArray

A template-encoded hash (v15) that carries its field names inline, with the values stored as raw strings.

1. The first token is the OpCode `0x1F`.
2. The key name in string encoded form.
3. The field names format in length encoded form:
    1. `0` field names stored as a single listpack. Integer entries decode to their decimal string.
    2. `1` explicit field list:
        1. The number of fields in length encoded form. It must be greater than zero.
        2. Fields:
            1. The field name in string encoded form.

The field names must be strictly ascending when compared by length first, then bytes.

4. Values:
    1. The value in string encoded form, in field order. The number of values must match the field count.

#### `32` HashTmplArrayRef

A template-encoded hash (v15) that references a template registered by a [HashTemplate](#242-hashtemplate) record, with the values stored as raw strings.

1. The first token is the OpCode `0x20`.
2. The key name in string encoded form.
3. The template ID in length encoded form.
4. Values:
    1. The value in string encoded form, in field order. The number of values must match the template's field count.

Redis unstable additionally defines `RDB_TYPE_GCRA` (33) for the GCRA object. It is not covered by librdb and its layout is not documented here.

https://github.com/redis/redis/blob/unstable/src/rdb.h

#### `64` Cron

A KeyDB cron job (`RDB_TYPE_CRON`, KeyDB specific). The job executes a script together with key and value arguments, which map to the `KEYS` and `ARGV` of the script invocation.

1. The first token is the OpCode `0x40`.
2. The key name in string encoded form.
3. The script in string encoded form.
4. The start time in milliseconds stored using 8 bytes in Little endian format.
5. The interval in milliseconds stored using 8 bytes in Little endian format.
6. The number of key arguments in length encoded form.
7. Key arguments:
    1. The argument in string encoded form.
8. The number of value arguments in length encoded form.
9. Value arguments:
    1. The argument in string encoded form.

https://github.com/Snapchat/KeyDB/blob/603ebb27fb82a27fb98b0feb6749b0f7661a1c4b/src/rdb.cpp
