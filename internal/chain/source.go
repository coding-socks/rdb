package chain

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coding-socks/rdb"
	"github.com/coding-socks/rdb/aof"
	"github.com/coding-socks/rdb/clusters"
	"github.com/coding-socks/rdb/dump"
	"github.com/coding-socks/rdb/resp"
)

// ProgressReporter is implemented by sources that can report the
// progress of their work.
type ProgressReporter interface {
	// Progress returns one reading per step of the work of the
	// source, a step being a node of the cluster or a member of a
	// manifest archive. A step that has not started carries no
	// unit.
	Progress() []ProgressReport
}

// ProgressReport is one reading of a step of the work of a source,
// I of N units finished. N is 0 when the source cannot know the size
// of the step's work. Unit names the unit the counts are in, bytes or
// keys.
type ProgressReport struct {
	I    uint64
	N    uint64
	Unit string
}

// step is the progress of one step of the work of a source, a node
// of a cluster or a member of a manifest archive.
type step struct {
	n atomic.Uint64
	i atomic.Uint64
}

func (s *step) report(unit string) ProgressReport {
	return ProgressReport{I: s.i.Load(), N: s.n.Load(), Unit: unit}
}

// Msg pairs a token with the error of the read that produced it.
// The sources of the tests produce their tokens as a list of
// messages.
type Msg struct {
	T   Token
	Err error
}

// SliceSrc pushes the tokens of a list of [Msg] into the
// destination, the source of the tests: the head is pushed first, and
// the Err of a message fails the run when it is not nil.
type SliceSrc struct {
	head Head
	msgs []Msg
}

// NewSliceSrc constructs a SliceSrc pushing the head followed by the
// tokens of the messages.
func NewSliceSrc(h Head, msgs []Msg) *SliceSrc {
	return &SliceSrc{head: h, msgs: msgs}
}

func (s *SliceSrc) Run(ctx context.Context, dst Dst) error {
	if err := dst.Process(s.head); err != nil {
		return err
	}
	for _, msg := range s.msgs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if msg.Err != nil {
			return msg.Err
		}
		if err := dst.Process(msg.T); err != nil {
			return err
		}
	}
	return io.EOF
}

// RDBFileSrc pushes the tokens of an RDB file into the destination. The
// dialect of the file decoder forces the dispatch of the op codes;
// the head reads the header of the file and reports the dialect it
// names.
type RDBFileSrc struct {
	r       io.Reader
	dialect rdb.Dialect

	n atomic.Uint64
	i atomic.Uint64
}

// NewRDBFileSrc constructs an RDBFileSrc reading from r. The dialect forces
// the dispatch of the op codes instead of being detected from the
// header of the file.
func NewRDBFileSrc(r io.Reader, dialect rdb.Dialect) *RDBFileSrc {
	return &RDBFileSrc{r: r, dialect: dialect}
}

func (s *RDBFileSrc) Run(ctx context.Context, dst Dst) error {
	dec := rdb.NewFileDecoder(&countReader{r: s.r, n: &s.i}, s.dialect)
	// The head reads the header of the file, an unknown magic
	// string or a version the family does not support fails the
	// run before the tokens that follow it.
	dialect, err := dec.Dialect()
	if err != nil {
		return err
	}
	if err := dst.Process(Head{Kind: KindRDB, Dialect: dialect}); err != nil {
		return err
	}
	if f, ok := s.r.(*os.File); ok {
		// a pipe stats the size 0, the report stays indeterminate
		if fi, err := f.Stat(); err == nil {
			s.n.Store(uint64(fi.Size()))
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		t, err := dec.ReadToken()
		if errors.Is(err, io.EOF) {
			return io.EOF
		}
		if err != nil {
			return err
		}
		if err := dst.Process(t); err != nil {
			return err
		}
	}
}

// Progress reports the bytes the decoder read. The total is the size
// of the file, 0 when the source reads a stream of unknown size.
func (s *RDBFileSrc) Progress() []ProgressReport {
	return []ProgressReport{{N: s.n.Load(), I: s.i.Load(), Unit: "bytes"}}
}

// countReader counts the bytes its reader delivers.
type countReader struct {
	r io.Reader
	n *atomic.Uint64
}

func (c *countReader) Read(p []byte) (int, error) {
	nn, err := c.r.Read(p)
	c.n.Add(uint64(nn))
	return nn, err
}

// AOFFileSrc pushes the commands of an AOF file into the destination. The
// commands carry no dialect, so the dialect of the stream is set from
// outside.
type AOFFileSrc struct {
	r       io.Reader
	dialect rdb.Dialect // set from outside, the commands carry none

	n atomic.Uint64
	i atomic.Uint64
}

// NewAOFFileSrc constructs an AOFFileSrc reading from r.
func NewAOFFileSrc(r io.Reader, dialect rdb.Dialect) *AOFFileSrc {
	if dialect == nil {
		dialect = rdb.RedisDialect{}
	}
	return &AOFFileSrc{r: r, dialect: dialect}
}

func (s *AOFFileSrc) Run(ctx context.Context, dst Dst) error {
	dec := aof.NewReader(&countReader{r: s.r, n: &s.i})
	if err := dst.Process(Head{Kind: KindAOF, Dialect: s.dialect}); err != nil {
		return err
	}
	if f, ok := s.r.(*os.File); ok {
		// a pipe stats the size 0, the report stays indeterminate
		if fi, err := f.Stat(); err == nil {
			s.n.Store(uint64(fi.Size()))
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		t, err := dec.Next()
		if errors.Is(err, io.EOF) {
			return io.EOF
		}
		if err != nil {
			return err
		}
		if err := dst.Process(t); err != nil {
			return err
		}
	}
}

// Progress reports the bytes the reader consumed. The total is the size
// of the file, 0 when the source reads a stream of unknown size.
func (s *AOFFileSrc) Progress() []ProgressReport {
	return []ProgressReport{{N: s.n.Load(), I: s.i.Load(), Unit: "bytes"}}
}

// ManifestSrc pushes the tokens of a manifest archive into the
// destination. The tokens of its base RDB snapshot are pushed first,
// and the commands of its incremental files follow them; the head
// names the kinds of the whole archive.
type ManifestSrc struct {
	dialect rdb.Dialect
	arch    io.Reader

	mu    sync.Mutex // guards steps, Run publishes them once the archive is read
	steps []step
}

// NewManifestSrc constructs a ManifestSrc reading from arch. The
// caller of the chain opens the archive and closes it after [Run]
// returned.
func NewManifestSrc(arch io.Reader, dialect rdb.Dialect) *ManifestSrc {
	return &ManifestSrc{arch: arch, dialect: dialect}
}

func (s *ManifestSrc) Run(ctx context.Context, dst Dst) error {
	data, err := io.ReadAll(s.arch)
	if err != nil {
		return err
	}
	base, incrs, err := manifestRead(data)
	if err != nil {
		return err
	}
	steps := make([]step, 1+len(incrs))
	steps[0].n.Store(uint64(len(base)))
	for i, incr := range incrs {
		steps[1+i].n.Store(uint64(len(incr)))
	}
	s.mu.Lock()
	s.steps = steps
	s.mu.Unlock()

	dec := rdb.NewFileDecoder(&countReader{r: bytes.NewReader(base), n: &steps[0].i}, s.dialect)
	// The head reads the header of the base snapshot, a family or a
	// version the dialect does not take fails the run before the
	// tokens that follow it. It names the kinds of the whole
	// archive, the commands of the incremental files among them.
	dialect, err := dec.Dialect()
	if err != nil {
		return err
	}
	kinds := KindRDB
	if len(incrs) > 0 {
		kinds |= KindAOF
	}
	if err := dst.Process(Head{Kind: kinds, Dialect: dialect}); err != nil {
		return err
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		t, err := dec.ReadToken()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if err := dst.Process(t); err != nil {
			return err
		}
	}
	for i, incr := range incrs {
		r := aof.NewReader(&countReader{r: bytes.NewReader(incr), n: &steps[1+i].i})
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			t, err := r.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return err
			}
			if err := dst.Process(t); err != nil {
				return err
			}
		}
	}
	return io.EOF
}

// Progress reports the bytes the members decoded, one reading per
// member of the archive, the base snapshot first. The total of a
// reading is the size of the member. The readings are empty until
// the archive is read.
func (s *ManifestSrc) Progress() []ProgressReport {
	s.mu.Lock()
	defer s.mu.Unlock()
	reports := make([]ProgressReport, len(s.steps))
	for i := range s.steps {
		reports[i] = s.steps[i].report("bytes")
	}
	return reports
}

// DumpServerSrc scans the keyspace of every node of a cluster, one node
// after the other. The scan of a node walks the keyspace of the
// database its connection serves. Every key carries its payload, the
// type of its value makes no difference.
type DumpServerSrc struct {
	cluster *clusters.Cluster

	keyspace bool
	steps    []step
}

// ServerOption configures a server source, see [NewDumpServerSrc]
// and [NewAOFServerSrc].
type ServerOption func(*serverOptions)

type serverOptions struct {
	keyspace bool
}

// WithKeyspace makes the walk follow the keyspace event notifications
// of the server. A key a notification names is read again, a key a
// deletion notification names is emitted as a DEL command, and the
// head of a dump source names the commands of an AOF stream among the
// keys of the scan. The server must serve the notifications already,
// see [Bulk.KeyspaceNotifications].
func WithKeyspace() ServerOption {
	return func(o *serverOptions) { o.keyspace = true }
}

// NewDumpServerSrc starts a scan of every node of the cluster.
func NewDumpServerSrc(c *clusters.Cluster, opts ...ServerOption) *DumpServerSrc {
	var o serverOptions
	for _, opt := range opts {
		opt(&o)
	}
	return &DumpServerSrc{cluster: c, keyspace: o.keyspace, steps: make([]step, len(c.All()))}
}

func (s *DumpServerSrc) Run(ctx context.Context, dst Dst) error {
	kinds := KindDump
	if s.keyspace {
		kinds |= KindAOF
	}
	if err := dst.Process(Head{Kind: kinds, Dialect: s.cluster.Dialect()}); err != nil {
		return err
	}
	for i, n := range s.cluster.All() {
		tok, err := n.Conn.Exec(ctx, []byte("DBSIZE"))
		if err != nil {
			return err
		}
		if tok.Kind() == resp.KindError {
			return tok.Err()
		}
		s.steps[i].n.Store(uint64(tok.Int()))
		scanner := DumpServerScanner{
			bulk: NewBulk(n.Conn),
			emit: func(t Token) error {
				return dst.Process(t)
			},
			i:        &s.steps[i].i,
			keyspace: s.keyspace,
		}
		if err := scanner.Scan(ctx); err != nil {
			return err
		}
	}
	return io.EOF
}

// Progress reports the keys the walk of every node visited, one
// reading per node. The total of a reading is the size of the
// keyspace the node scans.
func (s *DumpServerSrc) Progress() []ProgressReport {
	reports := make([]ProgressReport, len(s.steps))
	for i := range s.steps {
		reports[i] = s.steps[i].report("keys")
	}
	return reports
}

// DumpServerScanner reads the payloads of the keyspace a [Bulk] scans.
// A key that is gone or expired between the SCAN and its read stores
// nothing, and a payload that cannot carry a record of a value stores
// nothing.
type DumpServerScanner struct {
	bulk *Bulk
	emit func(t Token) error
	i    *atomic.Uint64

	keyspace bool
}

func (s *DumpServerScanner) Scan(ctx context.Context) error {
	if s.keyspace {
		s.bulk.KeyspaceNotifications(s.notified)
	}
	return s.bulk.Scan(ctx, func(key []byte) {
		if s.i != nil {
			s.i.Add(1)
		}
		s.read(key)
	})
}

func (s *DumpServerScanner) notified(event, key []byte) error {
	switch string(event) {
	case "del", "expired", "evicted", "rename_from", "move_from":
		return s.emit(aof.Token{[]byte("DEL"), key})
	default:
		s.read(key)
		return nil
	}
}

func (s *DumpServerScanner) read(key []byte) {
	var pttl int64
	s.bulk.Send([][]byte{[]byte("PTTL"), key}, func(t resp.Token, _ resp.Decoder) error {
		if t.Kind() == resp.KindError {
			return t.Err()
		}
		pttl = t.Int()
		return nil
	})
	s.bulk.Send([][]byte{[]byte("DUMP"), key}, func(t resp.Token, _ resp.Decoder) error {
		if t.Kind() == resp.KindError {
			return t.Err()
		}
		if t.IsNil() {
			return nil
		}
		payload := t.Bytes()
		if pttl < -1 || pttl == 0 || len(payload) < 11 {
			return nil
		}
		return s.emit(dump.Token{Key: key, Payload: payload, TTL: pttl})
	})
}

// readStringMax is the size of the GETRANGE batches of a string
// key, the bytes a large string is read in.
const readStringMax = 100 << 20

// readBatchCount is the count of the batches of a large value: the
// members of a LRANGE batch, the count of a SCAN family command and
// the entries of a XRANGE batch.
const readBatchCount = 100

// readBatchCountBytes is the byte representation of [readBatchCount],
// the count argument of the SCAN family commands and of the XRANGE
// command.
var readBatchCountBytes = strconv.AppendInt(nil, readBatchCount, 10)

// AOFServerSrc reads the keyspace of every node of a cluster, one node
// after the other. It scans the keyspace with the SCAN command and
// reads every key with the read command of its type. The supported
// types are a string, a list, a set, a sorted set, a hash, a stream,
// a JSON key, a time series and a bloom and a cuckoo filter. The keys
// of the other types are skipped. The values arrive as AOF tokens, a
// large value in the batches its read delivers. The tokens are
// volatile, they stay valid until the next token is delivered. A key
// with a time to live carries its expiry with it.
type AOFServerSrc struct {
	cluster *clusters.Cluster

	keyspace bool
	steps    []step
}

// NewAOFServerSrc starts a read of the keyspace of every node of the
// cluster.
func NewAOFServerSrc(c *clusters.Cluster, opts ...ServerOption) *AOFServerSrc {
	var o serverOptions
	for _, opt := range opts {
		opt(&o)
	}
	return &AOFServerSrc{cluster: c, keyspace: o.keyspace, steps: make([]step, len(c.All()))}
}

func (s *AOFServerSrc) Run(ctx context.Context, dst Dst) error {
	if err := dst.Process(Head{Kind: KindAOF, Dialect: s.cluster.Dialect()}); err != nil {
		return err
	}
	_, keydb := s.cluster.Dialect().(rdb.KeyDBDialect)
	for i, n := range s.cluster.All() {
		tok, err := n.Conn.Exec(ctx, []byte("DBSIZE"))
		if err != nil {
			return err
		}
		if tok.Kind() == resp.KindError {
			return tok.Err()
		}
		s.steps[i].n.Store(uint64(tok.Int()))
		scanner := AOFServerScanner{
			bulk: NewBulk(n.Conn),
			emit: func(t aof.Token) error {
				return dst.Process(t)
			},
			i:               &s.steps[i].i,
			usePTTL:         keydb,
			useHPEXPIRETIME: !keydb,
			keyspace:        s.keyspace,
		}
		if err := scanner.Scan(ctx); err != nil {
			return err
		}
	}
	return io.EOF
}

// Progress reports the keys the walk of every node visited, one
// reading per node. The total of a reading is the size of the
// keyspace the node scans.
func (s *AOFServerSrc) Progress() []ProgressReport {
	reports := make([]ProgressReport, len(s.steps))
	for i := range s.steps {
		reports[i] = s.steps[i].report("keys")
	}
	return reports
}

// AOFServerScanner reads the values of the keyspace a [Bulk] scans. The
// expiry of a hash field is read with the HPEXPIRETIME command when
// it is enabled and becomes the HPEXPIREAT of its field. The keys of
// the types without a read command here are skipped.
type AOFServerScanner struct {
	bulk *Bulk
	emit func(aof.Token) error
	i    *atomic.Uint64

	usePTTL         bool
	useHPEXPIRETIME bool
	keyspace        bool
}

func (s AOFServerScanner) Scan(ctx context.Context) error {
	if s.keyspace {
		s.bulk.KeyspaceNotifications(s.notified)
	}
	return s.bulk.Scan(ctx, func(key []byte) {
		if s.i != nil {
			s.i.Add(1)
		}
		s.bulk.Send([][]byte{[]byte("TYPE"), key}, func(t resp.Token, _ resp.Decoder) error {
			if t.Kind() == resp.KindError {
				return t.Err()
			}
			s.read(key, t.String())
			return nil
		})
	})
}

func (s *AOFServerScanner) notified(event, key []byte) error {
	switch string(event) {
	case "del", "expired", "evicted", "rename_from", "move_from":
		return s.emit(aof.Token{[]byte("DEL"), key})
	default:
		s.bulk.Send([][]byte{[]byte("TYPE"), key}, func(t resp.Token, _ resp.Decoder) error {
			if t.Kind() == resp.KindError {
				return t.Err()
			}
			s.read(key, t.String())
			return nil
		})
		return nil
	}
}

func (s *AOFServerScanner) read(key []byte, typ string) {
	switch typ {
	case "string":
		s.readString(key, 0)
	case "list":
		s.readList(key, 0)
	case "set":
		s.readSet(key, nil)
	case "zset":
		s.readZSet(key, nil)
	case "hash":
		s.readHash(key, nil)
	case "stream":
		s.readStream(key, nil)
	case "ReJSON-RL":
		s.readJSON(key)
	case "MBbloom--":
		s.readBloom(key, filterBloom, 0)
	case "MBbloomCF":
		s.readBloom(key, filterCuckoo, 0)
	case "TSDB-TYPE":
		s.readTimeSeries(key)
	}
}

func (s AOFServerScanner) readString(key []byte, start int64) {
	end := start + readStringMax - 1
	s.bulk.Send([][]byte{
		[]byte("GETRANGE"), key,
		strconv.AppendInt(nil, start, 10), strconv.AppendInt(nil, end, 10),
	}, func(t resp.Token, _ resp.Decoder) error {
		if t.Kind() == resp.KindError {
			return t.Err()
		}
		if start == 0 {
			if err := s.emit(aof.Token{[]byte("UNLINK"), key}); err != nil {
				return err
			}
		}
		chunk := t.Bytes()
		if len(chunk) > 0 {
			if err := s.emit(aof.Token{[]byte("APPEND"), key, chunk}); err != nil {
				return err
			}
		}
		if int64(len(chunk)) == readStringMax {
			s.readString(key, end+1)
			return nil
		}
		s.expire(key)
		return nil
	})
}

func (s AOFServerScanner) readList(key []byte, start int64) {
	end := start + readBatchCount - 1
	s.bulk.Send([][]byte{
		[]byte("LRANGE"), key,
		strconv.AppendInt(nil, start, 10), strconv.AppendInt(nil, end, 10),
	}, func(t resp.Token, d resp.Decoder) error {
		if t.Kind() == resp.KindError {
			return t.Err()
		}
		if start == 0 {
			if err := s.emit(aof.Token{[]byte("UNLINK"), key}); err != nil {
				return err
			}
		}
		elems, elemsErr := t.Iter(d)
		n := 0
		for el := range elems {
			n++
			if err := s.emit(aof.Token{[]byte("RPUSH"), key, el.Bytes()}); err != nil {
				return err
			}
		}
		if err := elemsErr(); err != nil {
			return err
		}
		if int64(n) == readBatchCount {
			s.readList(key, end+1)
			return nil
		}
		s.expire(key)
		return nil
	})
}

func (s AOFServerScanner) readSet(key []byte, cur []byte) {
	if bytes.Equal(cur, []byte("0")) {
		s.expire(key)
		return
	}
	cursor := cur
	if cursor == nil {
		cursor = []byte("0")
	}
	s.bulk.Send([][]byte{[]byte("SSCAN"), key, cursor, []byte("COUNT"), readBatchCountBytes}, func(t resp.Token, d resp.Decoder) error {
		if t.Kind() == resp.KindError {
			return t.Err()
		}
		if cur == nil {
			if err := s.emit(aof.Token{[]byte("UNLINK"), key}); err != nil {
				return err
			}
		}
		pairs, pairsErr := t.Iter(d)
		i := 0
		var next []byte
		for el := range pairs {
			switch i {
			case 0:
				next = bytes.Clone(el.Bytes())
			case 1:
				members, membersErr := el.Iter(d)
				for m := range members {
					if err := s.emit(aof.Token{[]byte("SADD"), key, m.Bytes()}); err != nil {
						return err
					}
				}
				if err := membersErr(); err != nil {
					return err
				}
			}
			i++
		}
		if err := pairsErr(); err != nil {
			return err
		}
		if !bytes.Equal(next, []byte("0")) {
			s.readSet(key, next)
			return nil
		}
		s.expire(key)
		return nil
	})
}

func (s AOFServerScanner) readZSet(key []byte, cur []byte) {
	if bytes.Equal(cur, []byte("0")) {
		s.expire(key)
		return
	}
	cursor := cur
	if cursor == nil {
		cursor = []byte("0")
	}
	s.bulk.Send([][]byte{[]byte("ZSCAN"), key, cursor, []byte("COUNT"), readBatchCountBytes}, func(t resp.Token, d resp.Decoder) error {
		if t.Kind() == resp.KindError {
			return t.Err()
		}
		if cur == nil {
			if err := s.emit(aof.Token{[]byte("UNLINK"), key}); err != nil {
				return err
			}
		}
		pairs, pairsErr := t.Iter(d)
		i := 0
		var next, member []byte
		for el := range pairs {
			switch i {
			case 0:
				next = bytes.Clone(el.Bytes())
			case 1:
				members, membersErr := el.Iter(d)
				j := 0
				for m := range members {
					if j%2 == 0 {
						member = bytes.Clone(m.Bytes())
					} else {
						if err := s.emit(aof.Token{[]byte("ZADD"), key, m.Bytes(), member}); err != nil {
							return err
						}
					}
					j++
				}
				if err := membersErr(); err != nil {
					return err
				}
				if j%2 != 0 {
					return fmt.Errorf("rdbc transfer: unexpected zset reply")
				}
			}
			i++
		}
		if err := pairsErr(); err != nil {
			return err
		}
		if !bytes.Equal(next, []byte("0")) {
			s.readZSet(key, next)
			return nil
		}
		s.expire(key)
		return nil
	})
}

func (s AOFServerScanner) readHash(key []byte, cur []byte) {
	if bytes.Equal(cur, []byte("0")) {
		s.expire(key)
		return
	}
	cursor := cur
	if cursor == nil {
		cursor = []byte("0")
	}
	s.bulk.Send([][]byte{[]byte("HSCAN"), key, cursor, []byte("COUNT"), readBatchCountBytes}, func(t resp.Token, d resp.Decoder) error {
		if t.Kind() == resp.KindError {
			return t.Err()
		}
		if cur == nil {
			if err := s.emit(aof.Token{[]byte("UNLINK"), key}); err != nil {
				return err
			}
		}
		pairs, pairsErr := t.Iter(d)
		i := 0
		var next, field []byte
		var names [][]byte
		for el := range pairs {
			switch i {
			case 0:
				next = bytes.Clone(el.Bytes())
			case 1:
				fields, fieldsErr := el.Iter(d)
				j := 0
				for v := range fields {
					if j%2 == 0 {
						field = bytes.Clone(v.Bytes())
						names = append(names, field)
					} else {
						if err := s.emit(aof.Token{[]byte("HSET"), key, field, v.Bytes()}); err != nil {
							return err
						}
					}
					j++
				}
				if err := fieldsErr(); err != nil {
					return err
				}
				if j%2 != 0 {
					return fmt.Errorf("rdbc transfer: unexpected hash reply")
				}
			}
			i++
		}
		if err := pairsErr(); err != nil {
			return err
		}
		if s.useHPEXPIRETIME && len(names) > 0 {
			args := make([][]byte, 0, 4+len(names))
			args = append(args, []byte("HPEXPIRETIME"), key,
				[]byte("FIELDS"), strconv.AppendInt(nil, int64(len(names)), 10))
			args = append(args, names...)
			s.bulk.Send(args, func(t resp.Token, d resp.Decoder) error {
				if t.Kind() == resp.KindError {
					return t.Err()
				}
				times, timesErr := t.Iter(d)
				i := 0
				for ts := range times {
					if i >= len(names) {
						return fmt.Errorf("rdbc transfer: unexpected hash field expiry reply")
					}
					ms := ts.Int()
					if ms != -1 {
						if ms < 0 {
							ms = 0
						}
						if err := s.emit(aof.Token{
							[]byte("HPEXPIREAT"), key,
							strconv.AppendInt(nil, ms, 10),
							[]byte("FIELDS"), []byte("1"), names[i],
						}); err != nil {
							return err
						}
					}
					i++
				}
				return timesErr()
			})
		}
		if !bytes.Equal(next, []byte("0")) {
			s.readHash(key, next)
			return nil
		}
		s.expire(key)
		return nil
	})
}

func (s AOFServerScanner) readStream(key []byte, id []byte) {
	start := []byte("-")
	if id != nil {
		start = append([]byte("("), id...)
	}
	s.bulk.Send([][]byte{
		[]byte("XRANGE"), key, start, []byte("+"), []byte("COUNT"), readBatchCountBytes,
	}, func(t resp.Token, d resp.Decoder) error {
		if t.Kind() == resp.KindError {
			return t.Err()
		}
		if id == nil {
			if err := s.emit(aof.Token{[]byte("UNLINK"), key}); err != nil {
				return err
			}
		}
		var last []byte
		n := 0
		entries, entriesErr := t.Iter(d)
		for entry := range entries {
			n++
			args := aof.Token{[]byte("XADD"), key}
			fields, fieldsErr := entry.Iter(d)
			i := 0
			for el := range fields {
				if i == 0 {
					last = bytes.Clone(el.Bytes())
					args = append(args, last)
				} else {
					values, valuesErr := el.Iter(d)
					for v := range values {
						args = append(args, bytes.Clone(v.Bytes()))
					}
					if err := valuesErr(); err != nil {
						return err
					}
				}
				i++
			}
			if err := fieldsErr(); err != nil {
				return err
			}
			if err := s.emit(args); err != nil {
				return err
			}
		}
		if err := entriesErr(); err != nil {
			return err
		}
		if n == readBatchCount {
			s.readStream(key, last)
			return nil
		}
		s.readStreamGroups(key)
		s.expire(key)
		return nil
	})
}

func (s AOFServerScanner) readStreamGroups(key []byte) {
	s.bulk.Send([][]byte{[]byte("XINFO"), []byte("GROUPS"), key}, func(t resp.Token, d resp.Decoder) error {
		if t.Kind() == resp.KindError {
			return t.Err()
		}
		groups, groupsErr := t.Iter(d)
		for group := range groups {
			fields, fieldsErr := group.Iter(d)
			j := 0
			var field, name, last, entries []byte
			for el := range fields {
				if j%2 == 0 {
					field = bytes.Clone(el.Bytes())
				} else {
					switch string(field) {
					case "name":
						name = bytes.Clone(el.Bytes())
					case "last-delivered-id":
						last = bytes.Clone(el.Bytes())
					case "entries-read":
						if !el.IsNil() {
							entries = strconv.AppendInt(entries, el.Int(), 10)
						}
					}
				}
				j++
			}
			if err := fieldsErr(); err != nil {
				return err
			}
			if j%2 != 0 {
				return fmt.Errorf("rdbc transfer: unexpected stream groups reply")
			}
			args := aof.Token{[]byte("XGROUP"), []byte("CREATE"), key, name, last, []byte("MKSTREAM")}
			if entries != nil {
				args = append(args, []byte("ENTRIESREAD"), entries)
			}
			if err := s.emit(args); err != nil {
				return err
			}
		}
		return groupsErr()
	})
}

func (s AOFServerScanner) readJSON(key []byte) {
	s.bulk.Send([][]byte{[]byte("JSON.GET"), key}, func(t resp.Token, _ resp.Decoder) error {
		if t.Kind() == resp.KindError {
			return t.Err()
		}
		if t.IsNil() {
			return nil
		}
		if err := s.emit(aof.Token{[]byte("JSON.SET"), key, []byte("$"), t.Bytes()}); err != nil {
			return err
		}
		s.expire(key)
		return nil
	})
}

const (
	filterCuckoo = iota
	filterBloom
)

var dumpCmds = map[int][]byte{
	filterBloom:  []byte("BF.SCANDUMP"),
	filterCuckoo: []byte("CF.SCANDUMP"),
}

var loadCmds = map[int][]byte{
	filterBloom:  []byte("BF.LOADCHUNK"),
	filterCuckoo: []byte("CF.LOADCHUNK"),
}

func (s AOFServerScanner) readBloom(key []byte, filterType int, iter int64) {
	s.bulk.Send([][]byte{dumpCmds[filterType], key, strconv.AppendInt(nil, iter, 10)}, func(t resp.Token, d resp.Decoder) error {
		if t.Kind() == resp.KindError {
			return t.Err()
		}
		if t.IsNil() {
			return nil
		}
		parts, partsErr := t.Iter(d)
		i := 0
		var next int64
		var chunk []byte
		for el := range parts {
			switch i {
			case 0:
				next = el.Int()
			case 1:
				if !el.IsNil() {
					chunk = bytes.Clone(el.Bytes())
				}
			}
			i++
		}
		if err := partsErr(); err != nil {
			return err
		}
		if i != 2 {
			return fmt.Errorf("rdbc transfer: unexpected bloom reply")
		}
		if len(chunk) > 0 {
			if err := s.emit(aof.Token{bytes.Clone(loadCmds[filterType]), key,
				strconv.AppendInt(nil, next, 10), chunk}); err != nil {
				return err
			}
		}
		if next != 0 {
			s.readBloom(key, filterType, next)
			return nil
		}
		s.expire(key)
		return nil
	})
}

func (s AOFServerScanner) readTimeSeries(key []byte) {
	s.bulk.Send([][]byte{[]byte("TS.INFO"), key}, func(t resp.Token, d resp.Decoder) error {
		if t.Kind() == resp.KindError {
			return t.Err()
		}
		if t.IsNil() {
			return nil
		}
		pairs, pairsErr := t.Iter(d)
		j := 0
		var name []byte
		var retention, chunkSize int64
		var encoding, policy []byte
		var labels, rules [][]byte
		for el := range pairs {
			if j%2 == 0 {
				name = bytes.Clone(el.Bytes())
			} else {
				switch string(name) {
				case "retentionTime":
					retention = el.Int()
				case "chunkSize":
					chunkSize = el.Int()
				case "chunkType":
					if bytes.Equal(el.Bytes(), []byte("uncompressed")) {
						encoding = []byte("UNCOMPRESSED")
					}
				case "duplicatePolicy":
					if !el.IsNil() {
						policy = bytes.Clone(el.Bytes())
					}
				case "labels":
					// The map form of the reply carries the
					// labels as the pairs of a map, the
					// array form wraps every pair in an
					// array of two.
					lbls, lblsErr := el.Iter(d)
					for lbl := range lbls {
						switch lbl.Kind() {
						case resp.KindArray, resp.KindMap:
							kv, kvErr := lbl.Iter(d)
							k := 0
							for v := range kv {
								if v.IsNil() {
									labels = append(labels, []byte{})
								} else {
									labels = append(labels, bytes.Clone(v.Bytes()))
								}
								k++
							}
							if err := kvErr(); err != nil {
								return err
							}
							if k != 2 {
								return fmt.Errorf("rdbc transfer: unexpected time series labels reply")
							}
						default:
							if lbl.IsNil() {
								labels = append(labels, []byte{})
							} else {
								labels = append(labels, bytes.Clone(lbl.Bytes()))
							}
						}
					}
					if err := lblsErr(); err != nil {
						return err
					}
				case "rules":
					rrs, rrsErr := el.Iter(d)
					var dst, agg []byte
					var bucket int64
					for rule := range rrs {
						switch {
						case dst != nil:
							// The map form of the reply
							// flattens the destination keys
							// and the arrays of the durations,
							// the aggregators and the
							// alignments behind them.
							inner, innerErr := rule.Iter(d)
							j := 0
							for f := range inner {
								switch j {
								case 0:
									bucket = f.Int()
								case 1:
									agg = bytes.Clone(f.Bytes())
								}
								j++
							}
							if err := innerErr(); err != nil {
								return err
							}
						case rule.Kind() == resp.KindArray || rule.Kind() == resp.KindMap:
							// The array form of the reply
							// carries the destination key, the
							// duration, the aggregator and the
							// alignment of the rule in one
							// array.
							fields, fieldsErr := rule.Iter(d)
							j := 0
							for f := range fields {
								switch j {
								case 0:
									dst = bytes.Clone(f.Bytes())
								case 1:
									bucket = f.Int()
								case 2:
									agg = bytes.Clone(f.Bytes())
								}
								j++
							}
							if err := fieldsErr(); err != nil {
								return err
							}
						default:
							dst = bytes.Clone(rule.Bytes())
							continue
						}
						if dst == nil || agg == nil {
							return fmt.Errorf("rdbc transfer: unexpected time series rules reply")
						}
						rules = append(rules, dst, strconv.AppendInt(nil, bucket, 10), agg)
						dst, agg, bucket = nil, nil, 0
					}
					if err := rrsErr(); err != nil {
						return err
					}
				}
			}
			j++
		}
		if err := pairsErr(); err != nil {
			return err
		}
		args := aof.Token{[]byte("TS.CREATE"), key}
		if retention > 0 {
			args = append(args, []byte("RETENTION"), strconv.AppendInt(nil, retention, 10))
		}
		if encoding != nil {
			args = append(args, []byte("ENCODING"), encoding)
		}
		if chunkSize > 0 {
			args = append(args, []byte("CHUNK_SIZE"), strconv.AppendInt(nil, chunkSize, 10))
		}
		if policy != nil {
			args = append(args, []byte("DUPLICATE_POLICY"), policy)
		}
		if len(labels) > 0 {
			args = append(args, []byte("LABELS"))
			args = append(args, labels...)
		}
		if err := s.emit(args); err != nil {
			return err
		}
		s.readTimeSeriesSamples(key, nil, rules)
		return nil
	})
}

func (s AOFServerScanner) readTimeSeriesSamples(key, from []byte, rules [][]byte) {
	start := []byte("-")
	if from != nil {
		start = from
	}
	s.bulk.Send([][]byte{
		[]byte("TS.RANGE"), key, start, []byte("+"), []byte("COUNT"), readBatchCountBytes,
	}, func(t resp.Token, d resp.Decoder) error {
		if t.Kind() == resp.KindError {
			return t.Err()
		}
		samples, samplesErr := t.Iter(d)
		n := 0
		var last []byte
		for sample := range samples {
			n++
			fields, fieldsErr := sample.Iter(d)
			i := 0
			for el := range fields {
				switch i {
				case 0:
					last = strconv.AppendInt(nil, el.Int(), 10)
				case 1:
					v := strconv.AppendFloat(nil, el.Float(), 'g', -1, 64)
					if err := s.emit(aof.Token{[]byte("TS.ADD"), key, last, v}); err != nil {
						return err
					}
				}
				i++
			}
			if err := fieldsErr(); err != nil {
				return err
			}
			if i != 2 {
				return fmt.Errorf("rdbc transfer: unexpected time series reply")
			}
		}
		if err := samplesErr(); err != nil {
			return err
		}
		if n == readBatchCount {
			from, err := strconv.ParseInt(string(last), 10, 64)
			if err != nil {
				return err
			}
			s.readTimeSeriesSamples(key, strconv.AppendInt(nil, from+1, 10), rules)
			return nil
		}
		for i := 0; i < len(rules); i += 3 {
			if err := s.emit(aof.Token{
				[]byte("TS.CREATERULE"), key, rules[i],
				[]byte("AGGREGATION"), rules[i+2], rules[i+1],
			}); err != nil {
				return err
			}
		}
		s.expire(key)
		return nil
	})
}

func (s AOFServerScanner) expire(key []byte) {
	if s.usePTTL {
		s.expirePTTL(key)
	} else {
		s.expireTime(key)
	}
}

func (s AOFServerScanner) expirePTTL(key []byte) {
	s.bulk.Send([][]byte{[]byte("PTTL"), key}, func(t resp.Token, _ resp.Decoder) error {
		if t.Kind() == resp.KindError {
			return t.Err()
		}
		pttl := t.Int()
		if pttl == -1 {
			return nil
		}
		ms := strconv.AppendInt(nil, time.Now().UnixMilli()+pttl, 10)
		return s.emit(aof.Token{[]byte("PEXPIREAT"), key, ms})
	})
}

func (s AOFServerScanner) expireTime(key []byte) {
	s.bulk.Send([][]byte{[]byte("PEXPIRETIME"), key}, func(t resp.Token, _ resp.Decoder) error {
		if t.Kind() == resp.KindError {
			return t.Err()
		}
		ptime := t.Int()
		if ptime == -1 {
			return nil
		}
		ms := strconv.AppendInt(nil, ptime, 10)
		return s.emit(aof.Token{[]byte("PEXPIREAT"), key, ms})
	})
}

// SyncServerSrc pushes the RDB payloads the nodes of a cluster send their
// replicas into the destination, one node after the other. The
// payloads decode into one stream of RDB tokens, the dialect names
// the family of the payloads and the header of a payload binds its
// version.
type SyncServerSrc struct {
	cluster *clusters.Cluster

	steps []step
}

// NewSyncServerSrc starts the SYNC transfer of every node of the cluster.
// The dialect is the one the scheme of the url of the server names.
func NewSyncServerSrc(c *clusters.Cluster) *SyncServerSrc {
	return &SyncServerSrc{cluster: c, steps: make([]step, len(c.All()))}
}

func (s *SyncServerSrc) Run(ctx context.Context, dst Dst) error {
	if err := dst.Process(Head{Kind: KindRDB, Dialect: s.cluster.Dialect()}); err != nil {
		return err
	}
	for i, n := range s.cluster.All() {
		if err := s.node(ctx, &s.steps[i], n, dst); err != nil {
			return err
		}
	}
	return io.EOF
}

// Progress reports the bytes the payloads decoded, one reading per
// node. The total of a reading is the size the payload announced, 0
// when it announced none.
func (s *SyncServerSrc) Progress() []ProgressReport {
	reports := make([]ProgressReport, len(s.steps))
	for i := range s.steps {
		reports[i] = s.steps[i].report("bytes")
	}
	return reports
}

// node pushes the tokens of the payload of one node into dst.
func (s *SyncServerSrc) node(ctx context.Context, st *step, n clusters.Node, dst Dst) error {
	// The payload is read without the client, the watcher aborts
	// its reads when the context is done. The transfer reads the
	// connection of the node for the last time when its payload
	// ends.
	stop := resp.AbortReadOn(ctx, n.Conn.C)
	defer stop()
	r, err := s.payload(ctx, st, n)
	if err != nil {
		return err
	}
	dec := rdb.NewFileDecoder(&countReader{r: r, n: &st.i}, s.cluster.Dialect())
	for {
		t, err := dec.ReadToken()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := dst.Process(t); err != nil {
			return err
		}
	}
}

// payload sends the SYNC command to the node, reads the handshake and
// returns the reader of its RDB payload.
func (s *SyncServerSrc) payload(ctx context.Context, st *step, node clusters.Node) (io.Reader, error) {
	if err := node.Conn.WriteAndFlush([]byte("SYNC")); err != nil {
		return nil, err
	}
	r := node.Conn.D.Handoff()
	buf := bytes.NewBuffer(nil)
	for {
		buf.Reset()
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := r.Advance(resp.TermByte("\n")); err != nil {
			return nil, err
		}
		if _, err := buf.ReadFrom(r); err != nil {
			return nil, err
		}
		if buf.Len() <= 1 {
			continue // skip keepalive "\n"
		}
		b := buf.Bytes()
		switch resp.Kind(b[0]) {
		case resp.KindString:
			// likely a +FULLRESYNC reply
			fmt.Fprintf(os.Stderr, "%s\n", b[1:len(b)-2])
		case resp.KindError:
			return nil, errors.New(string(b[1 : len(b)-2]))
		case resp.KindBlobString:
			if len(b) > 7 && bytes.Equal(b[:5], []byte("$EOF:")) {
				// unknown size
				r.Advance(resp.TermByte(b[5 : len(b)-2]))
				return r, nil
			}
			l, err := strconv.ParseInt(string(b[1:len(b)-2]), 10, 64)
			if err != nil {
				return nil, err
			}
			if l < 0 {
				return nil, fmt.Errorf("rdbc transfer: invalid length %d in SYNC", l)
			}
			st.n.Store(uint64(l))
			r.Advance(resp.TermLen(int(l) + 2))
			return r, nil
		default:
			return nil, fmt.Errorf("rdbc transfer: invalid kind %s in SYNC", string(b))
		}
	}
}
