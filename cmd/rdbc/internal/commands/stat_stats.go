package commands

import (
	"math"
	"slices"
	"strconv"
)

// humanBytes formats a byte count the way redis-cli does: plain bytes
// below 1K, then two decimals with a binary unit suffix.
func humanBytes(n uint64) string {
	units := []uint64{1 << 50, 1 << 40, 1 << 30, 1 << 20, 1 << 10}
	suffixes := []string{"P", "T", "G", "M", "K"}
	for i, unit := range units {
		if n >= unit {
			return strconv.FormatFloat(float64(n)/float64(unit), 'f', 2, 64) + suffixes[i]
		}
	}
	return strconv.FormatUint(n, 10) + "B"
}

// percentile returns the p-th percentile of the sorted values with the
// nearest-rank method. The values must be sorted ascending.
func percentile(sorted []uint64, p float64) uint64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(p * float64(len(sorted)) / 100))
	if rank < 1 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return sorted[rank-1]
}

// typeStat carries the statistics of one key type. The sizeCmd is the
// command measuring the number of elements, empty for types without
// one; the sizeUnit names what it counts.
type typeStat struct {
	name string

	count         int64
	totalMem      uint64
	totalLen      uint64
	biggestMem    uint64
	biggestMemKey string
	biggestLen    uint64
	biggestLenKey string
}

// record folds one key into the statistics. The length counts only
// when hasLen says a length command answered for the key.
func (t *typeStat) record(key string, mem uint64, length int64, hasLen bool) {
	t.count++
	t.totalMem += mem
	if mem > t.biggestMem {
		t.biggestMem = mem
		t.biggestMemKey = key
	}
	if hasLen {
		t.totalLen += uint64(length)
		if length > int64(t.biggestLen) {
			t.biggestLen = uint64(length)
			t.biggestLenKey = key
		}
	}
}

// typeStats is the registry of the key types seen so far. The known
// types come with their length command, an unknown type seen in a TYPE
// reply is created dynamically without one.
type typeStats struct {
	names  []string
	byName map[string]*typeStat
}

var (
	sizeCommands = map[string]string{
		"string":    "STRLEN",
		"list":      "LLEN",
		"set":       "SCARD",
		"hash":      "HLEN",
		"zset":      "ZCARD",
		"stream":    "XLEN",
		"vectorset": "VCARD",
	}
	sizeUnits = map[string]string{
		"string":    "bytes",
		"list":      "items",
		"set":       "members",
		"hash":      "fields",
		"zset":      "members",
		"stream":    "entries",
		"vectorset": "members",
	}
)

func newTypeStats() *typeStats {
	ts := &typeStats{byName: make(map[string]*typeStat)}
	for _, t := range []typeStat{
		{name: "string"},
		{name: "list"},
		{name: "set"},
		{name: "hash"},
		{name: "zset"},
		{name: "stream"},
		{name: "vectorset"},
	} {
		t := t
		ts.names = append(ts.names, t.name)
		ts.byName[t.name] = &t
	}
	return ts
}

func (ts *typeStats) get(name string) *typeStat {
	if t, ok := ts.byName[name]; ok {
		return t
	}
	t := &typeStat{name: name}
	ts.names = append(ts.names, name)
	ts.byName[name] = t
	return t
}

// keyInfo is an entry of the top key sizes list.
type keyInfo struct {
	key  string
	typ  string
	size uint64
}

// topKey is one node of the list [topKeys] keeps.
type topKey struct {
	keyInfo
	next *topKey
}

// topKeys keeps the biggest keys seen so far, sorted by size from high
// to low. A limit of zero is unbounded, like the top flag of
// redis-cli. Equals keep the order in which they arrived.
type topKeys struct {
	limit int
	head  *topKey
	n     int
}

func newTopKeys(limit int) *topKeys {
	return &topKeys{limit: limit}
}

func (t *topKeys) add(key string, typ string, size uint64) {
	var prev *topKey
	node := t.head
	for node != nil && size <= node.size {
		prev, node = node, node.next
	}
	link := &topKey{keyInfo: keyInfo{key: key, typ: typ, size: size}, next: node}
	if prev == nil {
		t.head = link
	} else {
		prev.next = link
	}
	t.n++
	if t.limit != 0 && t.n > t.limit {
		// The node just linked is beyond the limit, the walk cuts
		// the tail at the node the limit keeps.
		node := t.head
		for i := 1; i < t.limit; i++ {
			node = node.next
		}
		node.next = nil
		t.n = t.limit
	}
}

func (t *topKeys) names() []string {
	names := make([]string, 0, t.n)
	for node := t.head; node != nil; node = node.next {
		names = append(names, node.key)
	}
	return names
}

// list returns the entries of the list sorted by size from high to
// low.
func (t *topKeys) list() []keyInfo {
	infos := make([]keyInfo, 0, t.n)
	for node := t.head; node != nil; node = node.next {
		infos = append(infos, node.keyInfo)
	}
	return infos
}

// lenDistBounds holds the upper bounds of the key name length buckets,
// lengths beyond the last bound land in the infinite bucket.
var lenDistBounds = []uint64{8, 16, 32, 64, 128, 256, 512, 1024}

// lenDistRow is one line of the cumulative key name length
// distribution, a size of zero is the infinite bucket.
type lenDistRow struct {
	size    uint64
	percent float64
	keys    uint64
}

// lenDist is the cumulative distribution of the key name lengths.
type lenDist struct {
	counts   []uint64
	total    uint64
	totalLen uint64
	maxLen   uint64
}

func newLenDist() *lenDist {
	return &lenDist{counts: make([]uint64, len(lenDistBounds)+1)}
}

func (d *lenDist) add(n uint64) {
	d.total++
	d.totalLen += n
	if n > d.maxLen {
		d.maxLen = n
	}
	i := 0
	for i < len(lenDistBounds) && n > lenDistBounds[i] {
		i++
	}
	d.counts[i]++
}

// rows returns the cumulative rows of the non-empty buckets followed by
// the infinite row when keys exceeded the last bound. The displayed
// size of a bucket is capped at the observed maximum.
func (d *lenDist) rows() []lenDistRow {
	var rows []lenDistRow
	var cum uint64
	for i, bound := range lenDistBounds {
		if d.counts[i] == 0 {
			continue
		}
		cum += d.counts[i]
		size := bound
		if d.maxLen < size {
			size = d.maxLen
		}
		rows = append(rows, lenDistRow{
			size:    size,
			percent: 100 * float64(cum) / float64(d.total),
			keys:    cum,
		})
	}
	if cum < d.total {
		rows = append(rows, lenDistRow{percent: 100, keys: d.total})
	}
	return rows
}

// memDistRow is one line of the memory size percentile table, keys is
// the cumulative number of keys up to and including the percentile.
type memDistRow struct {
	p    float64
	size uint64
	keys int64
}

// memDist carries the memory usage distribution of the sampled keys.
type memDist struct {
	percentiles []memDistRow
	mean        uint64
}

// keyReport is the aggregated result of a scan, rendered by
// reportModel.
type keyReport struct {
	sampled  int64
	totalMem uint64
	types    []typeStat
	top      []keyInfo
	mem      memDist
	lens     struct {
		rows     []lenDistRow
		totalLen uint64
		avgLen   float64
	}
}

// keyStat folds the loaded keys into all statistics of the report.
type keyStat struct {
	sampled  int64
	totalMem uint64
	types    *typeStats
	top      *topKeys
	mem      []uint64
	lens     *lenDist
}

func newKeyStat(top int) *keyStat {
	return &keyStat{
		types: newTypeStats(),
		top:   newTopKeys(top),
		lens:  newLenDist(),
	}
}

// add folds one loaded key into the statistics. Keys that vanished
// between SCAN and TYPE have the type none and are skipped, like
// redis-cli does.
func (s *keyStat) add(msg loadMsg) {
	if msg.kind == "none" {
		return
	}
	key := string(msg.key)
	s.sampled++
	s.totalMem += msg.mem
	s.types.get(msg.kind).record(key, msg.mem, msg.len, msg.hasLen)
	s.top.add(key, msg.kind, msg.mem)
	s.mem = append(s.mem, msg.mem)
	s.lens.add(uint64(len(key)))
}

// report assembles the aggregated statistics into a report.
func (s *keyStat) report() keyReport {
	r := keyReport{
		sampled:  s.sampled,
		totalMem: s.totalMem,
		top:      s.top.list(),
	}
	for _, name := range s.types.names {
		if t := s.types.byName[name]; t.count > 0 {
			r.types = append(r.types, *t)
		}
	}

	r.mem.percentiles = make([]memDistRow, 0, 6)
	if s.sampled > 0 {
		sorted := slices.Clone(s.mem)
		slices.Sort(sorted)
		for _, p := range []float64{50, 75, 90, 95, 99, 100} {
			rank := int(math.Ceil(p * float64(len(sorted)) / 100))
			r.mem.percentiles = append(r.mem.percentiles, memDistRow{
				p:    p,
				size: percentile(sorted, p),
				keys: int64(rank),
			})
		}
		r.mem.mean = uint64(math.Round(float64(s.totalMem) / float64(s.sampled)))
	}

	r.lens.rows = s.lens.rows()
	r.lens.totalLen = s.lens.totalLen
	if s.sampled > 0 {
		r.lens.avgLen = float64(s.lens.totalLen) / float64(s.sampled)
	}
	return r
}
