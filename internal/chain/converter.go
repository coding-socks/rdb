package chain

import (
	"encoding/base64"
	"encoding/json/jsontext"
	"fmt"
	"math"
	"time"
	"unicode/utf8"

	"github.com/coding-socks/rdb"
	"github.com/coding-socks/rdb/aof"
	"github.com/coding-socks/rdb/dump"
)

// ToRDB converts the keys of a scan to the records of an RDB
// file. The end of the scan becomes the end of the file, the dialect
// of the head decodes the payloads and names the head the middleware
// produces.
func ToRDB() Middleware {
	return func(next Dst) Dst {
		return &toRDB{Dst: next}
	}
}

// toRDB is the [Dst] the ToRDB middleware wraps: the keys of
// the scan become the records of an RDB file. Finish is overridden,
// the tail of the conversion writes the end of the file.
type toRDB struct {
	Dst

	dec *dump.Decoder // the decoder of the payloads, the head constructs it
}

func (d *toRDB) Process(t Token) error {
	switch tt := t.(type) {
	case Head:
		tt.Kinds &= ^KindDump
		tt.Kinds |= KindRDB
		d.dec = dump.NewDecoder(tt.Dialect)
		return d.Dst.Process(tt)
	case dump.Token:
		toks, err := d.dec.Decode(tt)
		if err != nil {
			return err
		}
		for _, tok := range toks {
			if err := d.Dst.Process(tok); err != nil {
				return err
			}
		}
		return nil
	default:
		return d.Dst.Process(t)
	}
}

// Finish writes the end of the file before the wrapped destination
// ends the stream; a scan carries no end-of-file opcode.
func (d *toRDB) Finish() error {
	if err := d.Dst.Process(rdb.Op(rdb.RedisOpCodeEndOfRDB)); err != nil {
		return err
	}
	return d.Dst.Finish()
}

// ToAOF converts the records of an RDB file and the keys of a scan to
// the commands of an AOF file that recreate them, the commands of an
// AOF stream pass through. The values that have no command
// representation are skipped, the payload of a scan key decodes like
// the RDB value it carries, the dialect of the head decodes it.
func ToAOF() Middleware {
	return func(next Dst) Dst {
		return &toAOF{Dst: next, conv: aof.NewConverter()}
	}
}

// toAOF is the [Dst] the ToAOF middleware wraps. Finish and Abort are
// promoted: the commands have no tail.
type toAOF struct {
	Dst

	conv *aof.Converter
	dec  *dump.Decoder // the decoder of the payloads of a scan, the head constructs it
}

func (e *toAOF) Process(t Token) error {
	switch tt := t.(type) {
	case Head:
		tt.Kinds &= ^(KindDump | KindRDB)
		tt.Kinds |= KindAOF
		e.dec = dump.NewDecoder(tt.Dialect)
		return e.Dst.Process(tt)
	case rdb.Token:
		return e.convert(tt)
	case dump.Token:
		toks, err := e.dec.Decode(tt)
		if err != nil {
			return err
		}
		for _, tok := range toks {
			if err := e.convert(tok); err != nil {
				return err
			}
		}
		return nil
	default:
		return e.Dst.Process(t)
	}
}

// convert converts one token of an RDB record to the commands that
// re-create it.
func (e *toAOF) convert(tt rdb.Token) error {
	toks, err := e.conv.Convert(tt)
	if err != nil {
		return err
	}
	for _, tok := range toks {
		if err := e.Dst.Process(tok); err != nil {
			return err
		}
	}
	return nil
}

// ToDump converts the records of an RDB file to the keys of a
// scan: every value becomes the payload of a DUMP command, the way
// the DUMP command returns a value, so the values that have no
// command representation, the packed encodings and the streams among
// them, are transferred too. The payloads are written with the
// version of the dialect the head names. The keys of a scan are all
// the conversion produces, the op codes that carry no key of a scan,
// the SELECT of a database among them, produce none.
func ToDump() Middleware {
	return func(next Dst) Dst {
		return &toDump{Dst: next}
	}
}

// toDump is the [Dst] the ToDump middleware wraps: the tokens of a
// record are collected until the op code behind it, the record
// becomes the key of a scan. Finish and Abort are promoted: a scan
// has no tail.
type toDump struct {
	Dst

	enc       *dump.Encoder // the encoder of the payloads, the head constructs it
	pending   uint8         // the metadata token the op code ahead of a record expects
	skipping  bool          // the tokens of an op code with no key of a scan are dropped until the next one
	record    []rdb.Token   // the tokens of the record being collected
	expiry    time.Time     // the expiry of the record being collected
	hasExpiry bool
}

// The metadata a pending op code expects ahead of a record.
const (
	pendingNone uint8 = iota
	pendingSelect
	pendingExpiry
)

func (e *toDump) Process(t Token) error {
	switch tt := t.(type) {
	case Head:
		tt.Kinds &= ^KindRDB
		tt.Kinds |= KindDump
		e.enc = dump.NewEncoder(tt.Dialect)
		return e.Dst.Process(tt)
	case rdb.Token:
		return e.convert(tt)
	default:
		return e.Dst.Process(t)
	}
}

// convert consumes one token of the stream: the tokens of a record
// are collected until the op code behind the record completes it.
func (e *toDump) convert(t rdb.Token) error {
	if t.Kind() != rdb.KindOpCode {
		return e.value(t)
	}
	if err := e.flush(); err != nil {
		return err
	}
	switch op := t.OpCode(); op {
	case rdb.RedisOpCodeEndOfRDB:
		// The records are done, the keys of the scan written so
		// far are the conversion.
	case rdb.RedisOpCodeSelectDB:
		e.pending = pendingSelect
	case rdb.RedisOpCodeExpirySeconds, rdb.RedisOpCodeExpiryMilli:
		e.pending = pendingExpiry
	case rdb.RedisOpCodeIdle, rdb.RedisOpCodeFreq:
		// The idle time and the frequency of a key are modifiers
		// of its RESTORE command, a key of a scan carries none.
		e.skipping = true
	case rdb.RedisOpCodeModule2, rdb.RedisOpCodeModuleAux, rdb.RedisOpCodeModulePreGa,
		rdb.RedisOpCodeFunc, rdb.RedisOpCodeFunc2,
		rdb.RedisOpCodeAux, rdb.RedisOpCodeResizeDB, rdb.RedisOpCodeSlotInfo,
		rdb.RedisOpCodeRamLru, rdb.RedisOpCodeSlotNum, rdb.RedisOpCodeKeyMeta,
		rdb.RedisOpCodeHashTemplate:
		// The modules and the metadata records of the file: they
		// carry no key of a scan, their tokens are dropped until
		// the next op code.
		e.skipping = true
	default:
		// The opcode of a value: its record becomes the payload
		// of a key of a scan.
		e.skipping = false
		e.record = append(e.record[:0], rdb.Op(op))
	}
	return nil
}

// value consumes one token of the record being collected.
func (e *toDump) value(t rdb.Token) error {
	switch e.pending {
	case pendingSelect:
		// The number of the database the following keys live in,
		// a key of a scan names none.
		e.pending = pendingNone
		return nil
	case pendingExpiry:
		e.expiry, e.hasExpiry = t.Time(), true
		e.pending = pendingNone
		return nil
	}
	if e.skipping {
		return nil
	}
	// The tokens of a record stay volatile until the encode, the
	// collection copies them.
	e.record = append(e.record, t.Clone())
	return nil
}

// flush encodes the record being collected and hands the key of a
// scan to the wrapped destination. A record without a value, a
// metadata record among them, produces no key.
func (e *toDump) flush() error {
	if len(e.record) == 0 {
		return nil
	}
	var toks []rdb.Token
	if e.hasExpiry {
		toks = append(toks,
			rdb.Op(rdb.RedisOpCodeExpiryMilli),
			rdb.Expiry(uint64(e.expiry.UnixMilli())),
		)
	}
	toks = append(toks, e.record...)
	e.record = e.record[:0]
	e.hasExpiry = false
	e.expiry = time.Time{}
	tok, err := e.enc.Encode(toks...)
	if err != nil {
		return err
	}
	if tok.TTL != -1 && tok.TTL <= 0 {
		// The key expired between the file and the key of the scan.
		return nil
	}
	return e.Dst.Process(tok)
}

// jsonState assembles the tokens of a JSON stream: an array of
// objects, one object per database slot, where each object maps the
// keys of the database to their values. [ToJSON] feeds the records
// it reads into it.
type jsonState struct {
	dialect rdb.Dialect

	started bool
	dbOpen  bool
	skip    bool
	key     *keyState
	jout    []Token
}

// start opens the array of the JSON stream. The first record of the
// stream calls it.
func (s *jsonState) start() {
	if !s.started {
		s.started = true
		s.jout = append(s.jout, jsontext.BeginArray)
	}
}

// record starts the record of the op code. The metadata op codes the
// JSON output does not represent are skipped, and a value op code
// opens a key.
func (s *jsonState) record(op rdb.OpCode) error {
	if skipOpcode(op) {
		return s.skipRecord()
	}
	s.start()
	if err := s.closeKey(); err != nil {
		return err
	}
	s.skip = false
	if !s.dbOpen {
		s.jout = append(s.jout, jsontext.BeginObject)
		s.dbOpen = true
	}
	s.key = &keyState{dialect: s.dialect, op: op}
	return nil
}

// skipRecord drops the tokens of the record it is called for until
// the next op code.
func (s *jsonState) skipRecord() error {
	s.start()
	if err := s.closeKey(); err != nil {
		return err
	}
	s.key, s.skip = nil, true
	return nil
}

// selectDB closes the open key and the object of the database. The
// keys of the following database land in a new object.
func (s *jsonState) selectDB() error {
	s.start()
	if err := s.closeKey(); err != nil {
		return err
	}
	if s.dbOpen {
		s.jout = append(s.jout, jsontext.EndObject)
	}
	s.jout = append(s.jout, jsontext.BeginObject)
	s.dbOpen = true
	s.key, s.skip = nil, false
	return nil
}

// value shapes the value token of the open key.
func (s *jsonState) value(t rdb.Token) error {
	if s.skip || s.key == nil {
		return nil
	}
	if err := s.key.value(&s.jout, t); err != nil {
		return err
	}
	if s.key.done {
		s.key = nil
	}
	return nil
}

// closeKey closes the open container of the key of the state.
func (s *jsonState) closeKey() error {
	if s.key == nil {
		return nil
	}
	k := s.key
	return closeContainer(&s.jout, k)
}

// finish closes the open key and the containers of the JSON stream.
func (s *jsonState) finish() error {
	if err := s.closeKey(); err != nil {
		return err
	}
	if s.dbOpen {
		s.jout = append(s.jout, jsontext.EndObject)
		s.dbOpen = false
	}
	if s.started {
		s.jout = append(s.jout, jsontext.EndArray)
		s.started = false
	}
	return nil
}

// emit returns the tokens the state produced and empties it.
func (s *jsonState) emit() []Token {
	out := s.jout
	s.jout = nil
	return out
}

// ToJSON converts the records of an RDB file or the keys of a scan
// to the tokens of a JSON stream: an array of objects, one object
// per database slot, where each object maps the keys of the database
// to their values. The value of a scan key decodes like the RDB
// value it carries, the dialect of the head decodes the payloads.
// A head keeps the kinds the conversion does not take, the
// destination rejects the kinds nobody takes.
func ToJSON() Middleware {
	return func(next Dst) Dst {
		return &toJSON{Dst: next}
	}
}

// toJSON is the [Dst] the ToJSON middleware wraps. Abort is
// promoted, and Finish closes the containers the stream left open.
type toJSON struct {
	Dst

	dec   *dump.Decoder // the decoder of the payloads of a scan, the head constructs it
	state jsonState
}

func (e *toJSON) Process(t Token) error {
	switch tt := t.(type) {
	case Head:
		tt.Kinds &= ^(KindRDB | KindDump)
		tt.Kinds |= KindJSON
		e.dec = dump.NewDecoder(tt.Dialect)
		e.state.dialect = tt.Dialect
		e.state.jout = append(e.state.jout, tt)
	case rdb.Token:
		if err := e.receive(tt); err != nil {
			return err
		}
	case dump.Token:
		toks, err := e.dec.Decode(tt)
		if err != nil {
			return err
		}
		for _, tok := range toks {
			if err := e.receive(tok); err != nil {
				return err
			}
		}
	default:
		return e.Dst.Process(t)
	}
	return e.emit()
}

// Finish closes the containers the stream left open before the
// wrapped destination ends the stream.
func (e *toJSON) Finish() error {
	if err := e.state.finish(); err != nil {
		return err
	}
	if err := e.emit(); err != nil {
		return err
	}
	return e.Dst.Finish()
}

// emit passes the tokens the state produced to the wrapped
// destination and empties it.
func (e *toJSON) emit() error {
	for _, tok := range e.state.emit() {
		if err := e.Dst.Process(tok); err != nil {
			return err
		}
	}
	return nil
}

// receive feeds one token of an RDB record into the state.
func (e *toJSON) receive(tt rdb.Token) error {
	if tt.Kind() == rdb.KindOpCode {
		switch op := tt.OpCode(); op {
		case rdb.RedisOpCodeEndOfRDB:
			// The tail tokens of the state stay in its output,
			// Process emits them.
			return e.state.finish()
		case rdb.RedisOpCodeSelectDB:
			return e.state.selectDB()
		default:
			return e.state.record(op)
		}
	}
	return e.state.value(tt)
}

// skipOpcode reports whether the op code carries data the JSON output
// does not represent. The tokens of its payload are dropped until the
// next op code.
func skipOpcode(op rdb.OpCode) bool {
	switch op {
	case rdb.RedisOpCodeAux,
		rdb.RedisOpCodeFunc,
		rdb.RedisOpCodeFunc2,
		rdb.RedisOpCodeExpirySeconds,
		rdb.RedisOpCodeExpiryMilli,
		rdb.RedisOpCodeIdle,
		rdb.RedisOpCodeResizeDB,
		rdb.RedisOpCodeSlotInfo,
		rdb.RedisOpCodeFreq,
		rdb.RedisOpCodeRamLru,
		rdb.RedisOpCodeSlotNum,
		rdb.RedisOpCodeKeyMeta,
		rdb.RedisOpCodeHashTemplate,
		rdb.RedisOpCodeModulePreGa,
		rdb.RedisOpCodeModule2,
		rdb.RedisOpCodeModuleAux:
		return true
	}
	return false
}

// keyState assembles the JSON value of a key. The first token is the
// key name, and the remaining tokens shape its value.
type keyState struct {
	dialect rdb.Dialect
	op      rdb.OpCode
	step    keyStep
	n, i    uint64
	open    jsontext.Kind // the open container, KindInvalid when none
	done    bool
}

// keyStep is the step of the record a keyState waits for.
type keyStep uint8

const (
	stepKey   keyStep = iota // the key name
	stepCount                // the count of the entries
	stepValue                // the entries themselves
)

func (k *keyState) value(out *[]any, t rdb.Token) error {
	switch k.step {
	case stepKey:
		if t.Kind() != rdb.KindString {
			return fmt.Errorf("rdbc: expected key name, got %v in %s", t.Kind(), k.dialect.OpCodeString(k.op))
		}
		*out = append(*out, jsonString(t.String()))
		switch shapeOf(k.op) {
		case shapeString:
			k.step = stepValue
		case shapeList, shapeHash, shapeZset:
			k.step = stepCount
		case shapeBlob:
			k.step = stepValue
		default:
			k.step = stepValue
			*out = append(*out, jsontext.BeginArray)
			k.open = jsontext.KindBeginArray
		}
	case stepCount:
		if t.Kind() != rdb.KindUint {
			return fmt.Errorf("rdbc: expected count, got %v in %s", t.Kind(), k.dialect.OpCodeString(k.op))
		}
		n := t.Uint64()
		if k.op == rdb.RedisOpCodeHash || k.op == rdb.RedisOpCodeZset || k.op == rdb.RedisOpCodeZset2 {
			n *= 2
		}
		k.n = n
		k.step = stepValue
		switch shapeOf(k.op) {
		case shapeList:
			*out = append(*out, jsontext.BeginArray)
			k.open = jsontext.KindBeginArray
		case shapeHash, shapeZset:
			*out = append(*out, jsontext.BeginObject)
			k.open = jsontext.KindBeginObject
		}
	case stepValue:
		return k.valueToken(out, t)
	}
	return nil
}

func (k *keyState) valueToken(out *[]any, t rdb.Token) error {
	shape := shapeOf(k.op)
	switch shape {
	case shapeString, shapeBlob:
		v, err := tokenValue(t)
		if err != nil {
			return err
		}
		*out = append(*out, v)
		k.done = true
		return nil

	case shapeList:
		v, err := tokenValue(t)
		if err != nil {
			return err
		}
		*out = append(*out, v)

	case shapeHash:
		if t.Kind() != rdb.KindString {
			return fmt.Errorf("rdbc: expected hash field, got %v in %s", t.Kind(), k.dialect.OpCodeString(k.op))
		}
		*out = append(*out, jsonString(t.String()))

	case shapeZset:
		if k.i&1 == 0 {
			if t.Kind() != rdb.KindString {
				return fmt.Errorf("rdbc: expected zset member, got %v in %s", t.Kind(), k.dialect.OpCodeString(k.op))
			}
			*out = append(*out, jsonString(t.String()))
		} else {
			if t.Kind() != rdb.KindFloat64 {
				return fmt.Errorf("rdbc: expected zset score, got %v in %s", t.Kind(), k.dialect.OpCodeString(k.op))
			}
			*out = append(*out, scoreToken(t.Float64()))
		}
	}
	if shape == shapeGeneric {
		v, err := tokenValue(t)
		if err != nil {
			return err
		}
		*out = append(*out, v)
		return nil
	}
	k.i++
	if k.i >= k.n {
		if err := closeContainer(out, k); err != nil {
			return err
		}
		k.done = true
	}
	return nil
}

// tokenValue converts a single token of a record to a JSON token.
func tokenValue(t rdb.Token) (jsontext.Token, error) {
	switch t.Kind() {
	case rdb.KindString:
		return jsonString(t.String()), nil
	case rdb.KindUint:
		return jsontext.Uint(t.Uint64()), nil
	case rdb.KindFloat64:
		return scoreToken(t.Float64()), nil
	case rdb.KindExpiry, rdb.KindTime:
		return jsontext.String(t.Time().UTC().Format(time.RFC3339Nano)), nil
	case rdb.KindStreamID:
		ms, seq := t.StreamID()
		return jsontext.String(fmt.Sprintf("%d-%d", ms, seq)), nil
	}
	return jsontext.String(base64.StdEncoding.EncodeToString(t.Bytes())), nil
}

// jsonString returns a JSON string of a raw string. Binary strings
// that are not valid UTF-8 are base64 encoded.
func jsonString(s string) jsontext.Token {
	if !utf8.ValidString(s) {
		s = base64.StdEncoding.EncodeToString([]byte(s))
	}
	return jsontext.String(s)
}

// scoreToken returns the score of a sorted set member. The special
// values infinity and not-a-number have no JSON number representation,
// so they are written as strings.
func scoreToken(f float64) jsontext.Token {
	switch {
	case math.IsInf(f, 1):
		return jsontext.String("+inf")
	case math.IsInf(f, -1):
		return jsontext.String("-inf")
	case math.IsNaN(f):
		return jsontext.String("nan")
	}
	return jsontext.Float(f)
}

// closeContainer closes the open container of the key. It is called
// when the count of the container is reached or when a new op code
// arrives.
func closeContainer(out *[]any, k *keyState) error {
	switch k.open {
	case jsontext.KindBeginArray:
		*out = append(*out, jsontext.EndArray)
	case jsontext.KindBeginObject:
		*out = append(*out, jsontext.EndObject)
	}
	k.open = jsontext.KindInvalid
	return nil
}

// valueShape classifies how the tokens of a value op code form a
// JSON value.
type valueShape uint8

const (
	shapeGeneric valueShape = iota // the tokens form an array as they decode
	shapeString                    // the value is a single string
	shapeList                      // a count followed by the entries of an array
	shapeHash                      // a count followed by the field and value pairs of an object
	shapeZset                      // a count followed by the member and score pairs of an object
	shapeBlob                      // a single value, the packed encodings decode to one
)

// shapeOf returns the shape of the JSON value of an op code.
func shapeOf(op rdb.OpCode) valueShape {
	switch op {
	case rdb.RedisOpCodeString:
		return shapeString
	case rdb.RedisOpCodeList, rdb.RedisOpCodeSet:
		return shapeList
	case rdb.RedisOpCodeHash:
		return shapeHash
	case rdb.RedisOpCodeZset, rdb.RedisOpCodeZset2:
		return shapeZset
	case rdb.RedisOpCodeHashZipmap,
		rdb.RedisOpCodeHashZiplist,
		rdb.RedisOpCodeHashListpack,
		rdb.RedisOpCodeListZiplist,
		rdb.RedisOpCodeHashListpackExPreGa,
		rdb.RedisOpCodeHashTmplLpRef,
		rdb.RedisOpCodeSetIntset,
		rdb.RedisOpCodeSetListpack,
		rdb.RedisOpCodeZsetZiplist,
		rdb.RedisOpCodeZsetListpack:
		return shapeBlob
	}
	return shapeGeneric
}
