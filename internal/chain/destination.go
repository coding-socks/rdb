package chain

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"strconv"

	"github.com/coding-socks/rdb"
	"github.com/coding-socks/rdb/aof"
	"github.com/coding-socks/rdb/clusters"
	"github.com/coding-socks/rdb/dump"
	"github.com/coding-socks/rdb/resp"
)

// RecorderDst records the tokens it receives, the end of the stream,
// and the error of an abort. The tests of the chain assert on it.
type RecorderDst struct {
	received []Token
	finished bool
	aborted  error
}

// NewRecorderDst constructs an empty RecorderDst.
func NewRecorderDst() *RecorderDst {
	return &RecorderDst{}
}

// Process records the token it receives.
func (d *RecorderDst) Process(t Token) error {
	d.received = append(d.received, t)
	return nil
}

// Finish records the graceful end of the stream.
func (d *RecorderDst) Finish() error {
	d.finished = true
	return nil
}

// Abort records the error of the abort.
func (d *RecorderDst) Abort(err error) {
	d.aborted = err
}

// Received returns the tokens the destination received.
func (d *RecorderDst) Received() []Token {
	return d.received
}

// Finished reports whether the stream ended gracefully.
func (d *RecorderDst) Finished() bool {
	return d.finished
}

// Aborted returns the error of the abort, nil when the chain was not
// aborted.
func (d *RecorderDst) Aborted() error {
	return d.aborted
}

// RDBDst writes the tokens of an RDB stream to a writer. The dialect
// of the head is the dialect of the file it writes.
type RDBDst struct {
	out     io.Writer
	dialect rdb.Dialect
	enc     *rdb.FileEncoder
}

// NewRDBDst constructs an RDBDst writing to w. The head of the chain
// names the dialect of the file the destination writes.
func NewRDBDst(w io.Writer) *RDBDst {
	return &RDBDst{out: w}
}

// Process writes one token of the RDB stream. The head names the
// dialect of the encoder it constructs.
func (d *RDBDst) Process(t Token) error {
	switch tt := t.(type) {
	default:
		return fmt.Errorf("rdbc transfer: invalid token %T", t)
	case Head:
		if tt.Kinds != KindRDB {
			return fmt.Errorf("rdbc transfer: invalid kinds %v", tt.Kinds)
		}
		d.dialect = tt.Dialect
		return nil
	case rdb.Token:
		if d.enc == nil {
			d.enc = rdb.NewFileEncoder(d.out, d.dialect)
		}
		return d.enc.WriteToken(tt)
	}
}

// Finish has nothing to flush: the encoder writes every token
// through.
func (d *RDBDst) Finish() error {
	return nil
}

// Abort does nothing, the caller of the chain owns the writer.
func (d *RDBDst) Abort(err error) {}

// JSONDst writes the tokens of a JSON stream to a writer.
type JSONDst struct {
	out io.Writer
	enc *jsontext.Encoder
}

// NewJSONDst constructs a JSONDst writing to w.
func NewJSONDst(w io.Writer) *JSONDst {
	return &JSONDst{out: w}
}

// Process writes one token of the JSON stream. The head must name
// the [KindJSON] kind.
func (d *JSONDst) Process(t Token) error {
	switch t := t.(type) {
	default:
		return fmt.Errorf("rdbc transfer: invalid token %T", t)
	case Head:
		if t.Kinds != KindJSON {
			return fmt.Errorf("rdbc transfer: invalid kinds %v", t.Kinds)
		}
		return nil
	case jsontext.Token:
		if d.enc == nil {
			d.enc = jsontext.NewEncoder(d.out, jsontext.WithIndent("  "))
		}
		return d.enc.WriteToken(t)
	}
}

// Finish has nothing to flush: the jsontext encoder writes every
// token through.
func (d *JSONDst) Finish() error {
	return nil
}

// Abort does nothing, the caller of the chain owns the writer.
func (d *JSONDst) Abort(err error) {}

// AOFDst writes the commands of an AOF stream to a writer.
type AOFDst struct {
	out io.Writer
	enc *resp.RESP3Encoder
}

// NewAOFDst constructs an AOFDst writing to w.
func NewAOFDst(w io.Writer) *AOFDst {
	return &AOFDst{out: w}
}

// Process writes one command of the AOF stream. The head must name
// the [KindAOF] kind.
func (d *AOFDst) Process(t Token) error {
	switch tt := t.(type) {
	default:
		return fmt.Errorf("rdbc transfer: invalid token %T", t)
	case Head:
		if tt.Kinds != KindAOF {
			return fmt.Errorf("rdbc transfer: invalid kinds %v", tt.Kinds)
		}
		return nil
	case aof.Token:
		if d.enc == nil {
			d.enc = resp.NewRESP3Encoder(d.out)
		}
		if err := d.enc.WriteToken(resp.ArrayLen(len(tt))); err != nil {
			return err
		}
		for _, arg := range tt {
			if err := d.enc.WriteToken(resp.Bytes(arg)); err != nil {
				return err
			}
		}
		return nil
	}
}

// Finish flushes the commands the encoder holds back. The end of the
// stream commits them.
func (d *AOFDst) Finish() error {
	if d.enc == nil {
		return nil
	}
	return d.enc.Flush()
}

// Abort does nothing, the caller of the chain owns the writer.
func (d *AOFDst) Abort(err error) {}

// ManifestDst writes a manifest archive. The tokens of the [KindRDB]
// kind are its base snapshot, and the commands of the [KindAOF] kind
// are its incremental file. The base snapshot is written in the
// dialect of its head.
type ManifestDst struct {
	out io.Writer

	dialect rdb.Dialect
	base    bytes.Buffer
	incr    bytes.Buffer
	baseEnc *rdb.FileEncoder
	incrEnc *resp.RESP3Encoder
}

// NewManifestDst constructs a ManifestDst writing to w.
func NewManifestDst(w io.Writer) *ManifestDst {
	return &ManifestDst{out: w}
}

// Process receives one token of the archive. The head of the base
// snapshot binds its dialect.
func (d *ManifestDst) Process(t Token) error {
	switch tt := t.(type) {
	default:
		return fmt.Errorf("rdbc transfer: invalid token %T, want RDB or AOF", tt)
	case Head:
		if tt.Kinds&(KindRDB|KindAOF) == 0 {
			return fmt.Errorf("rdbc transfer: invalid kinds %s", tt.Kinds)
		}
		d.dialect = tt.Dialect
		return nil
	case rdb.Token:
		if d.baseEnc == nil {
			// The dialect of the head is bound to the version of the
			// base snapshot, the destination writes it as it
			// received it.
			d.baseEnc = rdb.NewFileEncoder(&d.base, d.dialect)
		}
		return d.baseEnc.WriteToken(tt)
	case aof.Token:
		if d.incrEnc == nil {
			d.incrEnc = resp.NewRESP3Encoder(&d.incr)
		}
		if err := d.incrEnc.WriteToken(resp.ArrayLen(len(tt))); err != nil {
			return err
		}
		for _, arg := range tt {
			if err := d.incrEnc.WriteToken(resp.Bytes(arg)); err != nil {
				return err
			}
		}
		return nil
	}
}

// Finish writes the archive. It returns an error when the stream
// carried no base snapshot.
func (d *ManifestDst) Finish() error {
	if d.baseEnc == nil {
		return errors.New("rdbc transfer: premature end, missing RDB snapshot")
	}
	var incrs [][]byte
	if d.incr.Len() > 0 {
		if err := d.incrEnc.Flush(); err != nil {
			return err
		}
		incrs = [][]byte{d.incr.Bytes()}
	}
	return manifestWriter(d.out, d.base.Bytes(), incrs)
}

// Abort does nothing, the caller of the chain owns the writer.
func (d *ManifestDst) Abort(err error) {}

// ServerDst stores the keys in the nodes of a cluster: the slot of
// every key selects the node that receives its command. The keys of
// a scan become the RESTORE commands of their payloads, the commands
// of an AOF stream are sent as they are and routed by the key the
// table of the cluster extracts, a command whose keys cannot be
// decided goes to one of the nodes. Every node batches the commands
// of its keys, the replies are read after each batch.
type ServerDst struct {
	ctx context.Context
	c   *clusters.Cluster // the cluster the keys are routed in

	batchN  int
	batches map[*resp.Client]int // the commands of a batch, one batch per connection
}

// NewServerDst starts the destination on the nodes of the cluster. The
// cluster owns the connections of its nodes, and the context bounds
// the reads of the replies.
func NewServerDst(ctx context.Context, c *clusters.Cluster) *ServerDst {
	return &ServerDst{
		ctx:     ctx,
		batchN:  500,
		c:       c,
		batches: make(map[*resp.Client]int),
	}
}

// SetBatchN sets the number of commands a batch sends. It defaults
// to 500.
func (d *ServerDst) SetBatchN(n int) {
	d.batchN = n
}

func (d *ServerDst) Process(t Token) error {
	switch tt := t.(type) {
	default:
		return fmt.Errorf("rdbc transfer: invalid token %T", t)
	case Head:
		if tt.Kinds&(KindDump|KindAOF) == 0 {
			return fmt.Errorf("rdbc transfer: invalid kinds %v", tt.Kinds)
		}
		if tt.Dialect.WithVersion(0) != d.c.Dialect().WithVersion(0) {
			return fmt.Errorf("rdbc transfer: invalid dialect %T", tt.Dialect)
		}
		return nil
	case dump.Token:
		n, err := d.c.Target(tt.Key)
		if err != nil {
			return err
		}
		d.batches[n.Conn]++
		if err := n.Conn.Write(restoreCommand(tt)...); err != nil {
			return err
		}
		if d.batches[n.Conn] >= d.batchN {
			return d.flush(n.Conn)
		}
		return nil
	case aof.Token:
		keys, err := d.c.Keys(tt...)
		if err != nil {
			return err
		}
		var n clusters.Node
		if len(keys) == 0 {
			nodes := d.c.All()
			n = nodes[rand.IntN(len(nodes))]
		} else {
			var err error
			n, err = d.c.Target(keys[0])
			if err != nil {
				return err
			}
		}
		d.batches[n.Conn]++
		if err := n.Conn.Write(tt...); err != nil {
			return err
		}
		if d.batches[n.Conn] >= d.batchN {
			return d.flush(n.Conn)
		}
		return nil
	}
}

// Finish sends the commands of the batches of the nodes. The
// connections of the nodes are closed by the cluster of the chain.
func (d *ServerDst) Finish() error {
	for _, n := range d.c.All() {
		if err := d.flush(n.Conn); err != nil {
			return err
		}
	}
	return nil
}

// Abort releases nothing. The keys the batches sent stay stored, and
// the connections of the nodes are closed by the caller of the chain.
func (d *ServerDst) Abort(err error) {}

// restoreCommand builds the RESTORE command of a key of a scan. The
// time to live of the key becomes the ttl argument of the command; a
// key that does not expire is stored with a ttl of zero.
func restoreCommand(t dump.Token) aof.Token {
	ttl := 0
	if t.TTL > 0 {
		ttl = int(t.TTL)
	}
	return aof.Token{
		[]byte("RESTORE"),
		t.Key,
		[]byte(strconv.Itoa(ttl)),
		t.Payload,
		[]byte("REPLACE"),
	}
}

// flush sends the commands of the batch of a connection and reads
// their replies.
func (d *ServerDst) flush(conn *resp.Client) error {
	batch := d.batches[conn]
	delete(d.batches, conn)
	if batch == 0 {
		return nil
	}
	for range batch {
		t, err := conn.ReadValue(d.ctx)
		if err != nil {
			return err
		}
		if t.Kind() == resp.KindError {
			return t.Err()
		}
		resp.SkipReply(conn.D, t)
	}
	return nil
}
