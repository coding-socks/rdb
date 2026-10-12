// Package rdb implements reading and writing of the RDB dump file format
// of Redis and its compatible forks.
//
// An RDB file is a sequence of records: a header with a magic string and
// a version, metadata records such as the selected database, the expiry
// timestamps and the auxiliary fields, and the key value records
// themselves. The [FileDecoder] and [FileEncoder] types read and write
// an RDB file: the header names the dialect and the version of the
// file, the records follow it, and the checksum at its end is verified
// or written. The [Encoder] and [Decoder] types are used to write or
// read the records of a file.
//
// # Tokens and Op Codes
//
// A [Token] is the unit of data a Decoder reads and an Encoder writes.
// Every value record starts with an [OpCode] that dispatches the shape of
// the tokens that follow it, the handlers behind the op code produce them
// one by one: the key of a record, the number of the entries of a list,
// the entries themselves, and so on.
//
// A token stores its value in one of two forms. Tokens read from a stream
// hold the raw bytes of the value, which stay volatile until [Token.Clone]
// copies them out. Tokens built by the constructors (e.g., [Str], [Uint])
// hold their values instead, and the [Blob] and [Packed] constructors hold
// the raw bytes to write verbatim. Both provide the same view: the value
// methods return the value wherever it is stored, and [Token.Bytes] returns
// the wire format.
//
// # Dialects
//
// Compatible forks of Redis reuse the file format but drift in their op
// codes and headers. A [Dialect] abstracts those differences: a
// [FileDecoder] detects the dialect of a file from its header using a
// registry, a [FileEncoder] writes the header of the dialect it is
// constructed with.
// [RegisterDialect] adds support for a fork the package does not ship.
//
// The dialects are version aware. A dialect value represents the format
// of one version, [Dialect.WithVersion] binds another one, and a version
// out of range falls back to the newest one the dialect supports. The
// dispatch of the op codes fails for the op codes the dialect does not
// support and for the ones introduced after the version of the dialect:
// a decoder rejects the records a file of its version cannot carry, an
// encoder rejects them when the file is written with an earlier one.
package rdb
