package chain

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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

// RDBDst writes the tokens of an RDB stream to a writer. The writes
// are buffered, the end of the stream flushes them. The dialect
// of the head is the dialect of the file it writes.
type RDBDst struct {
	out     *bufio.Writer
	dialect rdb.Dialect
	enc     *rdb.FileEncoder
}

// NewRDBDst constructs an RDBDst writing to w. The head of the chain
// names the dialect of the file the destination writes.
func NewRDBDst(w io.Writer) *RDBDst {
	buf, ok := w.(*bufio.Writer)
	if !ok {
		buf = bufio.NewWriter(w)
	}
	return &RDBDst{out: buf}
}

// Process writes one token of the RDB stream. The head names the
// dialect of the encoder it constructs.
func (d *RDBDst) Process(t Token) error {
	switch tt := t.(type) {
	default:
		return fmt.Errorf("rdbc transfer: invalid token %T", t)
	case Head:
		if tt.Kind != KindRDB {
			return fmt.Errorf("rdbc transfer: invalid kinds %v", tt.Kind)
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

func (d *RDBDst) Finish() error {
	return d.out.Flush()
}

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
		if t.Kind != KindJSON {
			return fmt.Errorf("rdbc transfer: invalid kinds %v", t.Kind)
		}
		return nil
	case jsontext.Token:
		if d.enc == nil {
			d.enc = jsontext.NewEncoder(d.out, jsontext.WithIndent("  "))
		}
		return d.enc.WriteToken(t)
	}
}

func (d *JSONDst) Finish() error {
	return nil
}

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
		if tt.Kind != KindAOF {
			return fmt.Errorf("rdbc transfer: invalid kinds %v", tt.Kind)
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

func (d *AOFDst) Finish() error {
	if d.enc == nil {
		return nil
	}
	return d.enc.Flush()
}

func (d *AOFDst) Abort(err error) {}

// ManifestDst writes a manifest archive on a tar writer. The tokens
// of the [KindRDB] kind are its base snapshot, and the commands of
// the [KindAOF] kind are its incremental file. The base snapshot is
// written in the dialect of its head. The archive of a head of both
// kinds leads with the manifest file and carries the base snapshot
// and one incremental file, the archive of a head of the RDB kind
// alone carries the base snapshot only. The manifest file is written
// with the head, the files of the transfer are written with the end
// of the stream, the size of an entry is the one datum a tar header
// needs ahead of its data. Finish ends the archive, the caller wraps
// the tar writer in a gzip writer when the file of the archive is
// compressed.
type ManifestDst struct {
	tw      *tar.Writer
	kind    Kind
	dialect rdb.Dialect
	base    bytes.Buffer
	incr    bytes.Buffer
	baseEnc *rdb.FileEncoder
	incrEnc *resp.RESP3Encoder
}

// NewManifestDst constructs a ManifestDst writing on the tar writer
// of the caller.
func NewManifestDst(w io.Writer) *ManifestDst {
	return &ManifestDst{tw: tar.NewWriter(w)}
}

// Process receives one token of the archive. The head binds the
// dialect of the base snapshot and writes the manifest file of an
// archive of both kinds.
func (d *ManifestDst) Process(t Token) error {
	switch tt := t.(type) {
	default:
		return fmt.Errorf("rdbc transfer: invalid token %T, want RDB or AOF", tt)
	case Head:
		if tt.Kind&KindRDB == 0 || tt.Kind&^(KindRDB|KindAOF) != 0 {
			return fmt.Errorf("rdbc transfer: invalid kinds %s", tt.Kind)
		}
		d.kind = tt.Kind
		d.dialect = tt.Dialect
		if d.kind&KindAOF != 0 {
			var manifest strings.Builder
			fmt.Fprintf(&manifest, "file %s seq 1 type b\n", "appendonly.aof.1.base.rdb")
			fmt.Fprintf(&manifest, "file %s seq 1 type i\n", fmt.Sprintf("appendonly.aof.%d.incr.aof", 1))
			if err := d.writeEntry(manifestEntry{"appendonly.aof.manifest", []byte(manifest.String())}); err != nil {
				return err
			}
		}
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
		if d.kind&KindAOF == 0 {
			return fmt.Errorf("rdbc transfer: invalid token %T, the manifest takes %s", t, d.kind)
		}
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

// writeEntry writes one file of the archive.
func (d *ManifestDst) writeEntry(e manifestEntry) error {
	if err := d.tw.WriteHeader(&tar.Header{
		Name:    e.name,
		Mode:    0o644,
		Size:    int64(len(e.data)),
		ModTime: time.Now(),
	}); err != nil {
		return err
	}
	_, err := d.tw.Write(e.data)
	return err
}

func (d *ManifestDst) Finish() error {
	defer d.tw.Close()
	if d.baseEnc == nil {
		return errors.New("rdbc transfer: premature end, missing RDB snapshot")
	}
	if err := d.writeEntry(manifestEntry{"appendonly.aof.1.base.rdb", d.base.Bytes()}); err != nil {
		return err
	}
	if d.kind&KindAOF != 0 {
		if d.incrEnc != nil {
			if err := d.incrEnc.Flush(); err != nil {
				return err
			}
		}
		if err := d.writeEntry(manifestEntry{fmt.Sprintf("appendonly.aof.%d.incr.aof", 1), d.incr.Bytes()}); err != nil {
			return err
		}
	}
	return nil
}

func (d *ManifestDst) Abort(err error) {
	d.tw.Close()
}

// ServerDst stores the keys in the nodes of a cluster. It can
// accept [KindDump] and [KindAOF] tokens. The supported token
// kind list can be reduced using kind.
type ServerDst struct {
	ctx  context.Context
	c    *clusters.Cluster
	kind Kind

	batchN  int
	batches map[*resp.Client]int

	readCh     map[*resp.Client]chan int
	readCancel context.CancelFunc
	stop       chan struct{}
	readErr    atomic.Pointer[error]
	wg         sync.WaitGroup
}

// NewServerDst starts the destination on the nodes of the cluster
// with the kinds it takes. The cluster owns the connections of its
// nodes, and the context bounds the reads of the replies.
func NewServerDst(ctx context.Context, c *clusters.Cluster, kind Kind) *ServerDst {
	return &ServerDst{
		ctx:     ctx,
		kind:    kind & (KindDump | KindAOF),
		batchN:  500,
		c:       c,
		batches: make(map[*resp.Client]int),
		readCh:  make(map[*resp.Client]chan int),
	}
}

// SetBatchN sets the number of commands a batch sends. It defaults
// to 500.
func (d *ServerDst) SetBatchN(n int) {
	d.batchN = n
}

func (d *ServerDst) startRead() {
	var readCtx context.Context
	readCtx, d.readCancel = context.WithCancel(d.ctx)
	d.stop = make(chan struct{})
	for _, n := range d.c.All() {
		ch := make(chan int, 128)
		d.readCh[n.Conn] = ch
		d.wg.Add(1)
		go func(conn *resp.Client) {
			defer d.wg.Done()
			for n := range ch {
				for range n {
					t, err := conn.ReadValue(readCtx)
					if err != nil {
						d.fail(err)
						return
					}
					if t.Kind() == resp.KindError {
						d.fail(t.Err())
						return
					}
					resp.SkipReply(conn.D, t)
				}
			}
		}(n.Conn)
	}
}

// fail records the first error of the replies and releases the sends
// of the counts that wait behind it, the transfer is lost beyond the
// first error.
func (d *ServerDst) fail(err error) {
	if d.readErr.CompareAndSwap(nil, &err) {
		close(d.stop)
	}
}

func (d *ServerDst) Process(t Token) error {
	if err := d.readErr.Load(); err != nil {
		return *err
	}
	switch tt := t.(type) {
	default:
		return fmt.Errorf("rdbc transfer: invalid token %T", t)
	case Head:
		if tt.Kind == 0 || tt.Kind&^d.kind != 0 {
			return fmt.Errorf("rdbc transfer: invalid kinds %s, the server takes %s", tt.Kind, d.kind)
		}
		if tt.Dialect.WithVersion(0) != d.c.Dialect().WithVersion(0) {
			return fmt.Errorf("rdbc transfer: invalid dialect %T", tt.Dialect)
		}
		d.startRead()
		return nil
	case dump.Token:
		if d.kind&KindDump == 0 {
			return fmt.Errorf("rdbc transfer: invalid token %T, the server takes %s", t, d.kind)
		}
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
		if d.kind&KindAOF == 0 {
			return fmt.Errorf("rdbc transfer: invalid token %T, the server takes %s", t, d.kind)
		}
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
		return nil
	}
}

func (d *ServerDst) Finish() error {
	for _, n := range d.c.All() {
		if err := d.flush(n.Conn); err != nil {
			for _, ch := range d.readCh {
				close(ch)
			}
			return err
		}
	}
	for _, ch := range d.readCh {
		close(ch)
	}
	d.wg.Wait()
	if err := d.readErr.Load(); err != nil {
		return *err
	}
	return nil
}

func (d *ServerDst) Abort(err error) {
	if d.readCancel != nil {
		d.readCancel()
	}
	for _, ch := range d.readCh {
		close(ch)
	}
}

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

// flush sends the commands of the batch of a connection and names
// the count of their replies to the reader of the connection, the
// send gives up when the reader died behind the first error.
func (d *ServerDst) flush(conn *resp.Client) error {
	batch := d.batches[conn]
	delete(d.batches, conn)
	if err := conn.Flush(); err != nil {
		return err
	}
	if batch == 0 {
		return nil
	}
	select {
	case d.readCh[conn] <- batch:
	case <-d.stop:
		return *d.readErr.Load()
	}
	return nil
}
