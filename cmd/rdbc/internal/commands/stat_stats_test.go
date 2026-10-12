package commands

import (
	"slices"
	"testing"
)

func Test_humanBytes(t *testing.T) {
	tests := []struct {
		name string
		in   uint64
		want string
	}{
		{name: "bytes", in: 1023, want: "1023B"},
		{name: "kibibytes", in: 1024, want: "1.00K"},
		{name: "mebibytes", in: 3 << 20, want: "3.00M"},
		{name: "gibibytes", in: 5 << 30, want: "5.00G"},
		{name: "tebibytes", in: 2 << 40, want: "2.00T"},
		{name: "pebibytes", in: 1 << 50, want: "1.00P"},
		{name: "zero", in: 0, want: "0B"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := humanBytes(tt.in); got != tt.want {
				t.Errorf("humanBytes(%d) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func Test_percentile(t *testing.T) {
	tests := []struct {
		name   string
		values []uint64
		p      float64
		want   uint64
	}{
		{name: "empty"},
		{name: "single value", values: []uint64{7}, p: 99, want: 7},
		{name: "nearest rank 50", values: []uint64{10, 20, 30, 40}, p: 50, want: 20},
		{name: "nearest rank 75", values: []uint64{10, 20, 30, 40}, p: 75, want: 30},
		{name: "nearest rank 90", values: []uint64{10, 20, 30, 40}, p: 90, want: 40},
		{name: "nearest rank 95", values: []uint64{10, 20, 30, 40}, p: 95, want: 40},
		{name: "nearest rank 99", values: []uint64{10, 20, 30, 40}, p: 99, want: 40},
		{name: "nearest rank 100", values: []uint64{10, 20, 30, 40}, p: 100, want: 40},
		{name: "low percentile of two values", values: []uint64{1, 2}, p: 50, want: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := percentile(tt.values, tt.p); got != tt.want {
				t.Errorf("percentile(%v, %v) = %d, want %d", tt.values, tt.p, got, tt.want)
			}
		})
	}
}

func Test_typeStats_get(t *testing.T) {
	t.Run("names keep insertion order", func(t *testing.T) {
		ts := newTypeStats()
		ts.get("hash")
		ts.get("wobble")
		want := []string{"string", "list", "set", "hash", "zset", "stream", "vectorset", "wobble"}
		if !slices.Equal(ts.names, want) {
			t.Errorf("names = %q, want %q", ts.names, want)
		}
	})
}

func Test_typeStat_record(t *testing.T) {
	t.Run("accumulates count and memory", func(t *testing.T) {
		var st typeStat
		st.record("a", 100, 0, false)
		st.record("b", 50, 0, false)
		if st.count != 2 || st.totalMem != 150 {
			t.Errorf("count = %d, totalMem = %d, want 2 and 150", st.count, st.totalMem)
		}
	})
	t.Run("biggest memory keeps the first of equals", func(t *testing.T) {
		var st typeStat
		st.record("a", 100, 0, false)
		st.record("b", 300, 0, false)
		st.record("c", 300, 0, false)
		if st.biggestMem != 300 || st.biggestMemKey != "b" {
			t.Errorf("biggest = %d %q, want 300 %q", st.biggestMem, st.biggestMemKey, "b")
		}
	})
	t.Run("length is skipped without a measurement", func(t *testing.T) {
		var st typeStat
		st.record("a", 10, 42, false)
		if st.totalLen != 0 || st.biggestLenKey != "" {
			t.Errorf("totalLen = %d, biggestLenKey = %q, want both unset", st.totalLen, st.biggestLenKey)
		}
	})
	t.Run("accumulates length", func(t *testing.T) {
		var st typeStat
		st.record("a", 10, 4, true)
		st.record("b", 10, 9, true)
		st.record("c", 10, 9, true)
		if st.totalLen != 22 || st.biggestLen != 9 || st.biggestLenKey != "b" {
			t.Errorf("totalLen = %d, biggest = %d %q, want 22, 9, %q",
				st.totalLen, st.biggestLen, st.biggestLenKey, "b")
		}
	})
}

func Test_topKeys_add(t *testing.T) {
	t.Run("sorted by size descending", func(t *testing.T) {
		top := newTopKeys(10)
		top.add("a", "string", 100)
		top.add("b", "hash", 300)
		top.add("c", "set", 200)
		got := top.names()
		want := []string{"b", "c", "a"}
		if !slices.Equal(got, want) {
			t.Errorf("names = %q, want %q", got, want)
		}
	})
	t.Run("bounded to the limit", func(t *testing.T) {
		top := newTopKeys(3)
		top.add("a", "string", 100)
		top.add("b", "hash", 300)
		top.add("c", "set", 200)
		top.add("d", "list", 400)
		got := top.names()
		want := []string{"d", "b", "c"}
		if !slices.Equal(got, want) {
			t.Errorf("names = %q, want %q", got, want)
		}
	})
	t.Run("equals keep first in order", func(t *testing.T) {
		top := newTopKeys(10)
		top.add("a", "string", 300)
		top.add("b", "hash", 300)
		got := top.names()
		want := []string{"a", "b"}
		if !slices.Equal(got, want) {
			t.Errorf("names = %q, want %q", got, want)
		}
	})
	t.Run("rejects sizes at or below the smallest when full", func(t *testing.T) {
		top := newTopKeys(2)
		top.add("a", "string", 300)
		top.add("b", "hash", 100)
		top.add("c", "set", 200)
		if got := top.names(); !slices.Equal(got, []string{"a", "c"}) {
			t.Fatalf("names = %q, want [a c]", got)
		}
		top.add("d", "list", 100)
		// A size equal to the smallest kept one is rejected too.
		top.add("e", "stream", 200)
		got := top.names()
		want := []string{"a", "c"}
		if !slices.Equal(got, want) {
			t.Errorf("names = %q, want %q", got, want)
		}
	})
	t.Run("zero limit is unbounded", func(t *testing.T) {
		top := newTopKeys(0)
		for i := range 20 {
			top.add(string(rune('a'+i)), "string", uint64(i))
		}
		if top.n != 20 {
			t.Errorf("len = %d, want 20", top.n)
		}
	})
}

func Test_lenDist_add(t *testing.T) {
	t.Run("buckets and cumulative rows", func(t *testing.T) {
		d := newLenDist()
		d.add(5)
		d.add(8)
		d.add(9)
		d.add(100)
		d.add(2000)
		rows := d.rows()
		want := []lenDistRow{
			{size: 8, percent: 40, keys: 2},
			{size: 16, percent: 60, keys: 3},
			{size: 128, percent: 80, keys: 4},
			{size: 0, percent: 100, keys: 5},
		}
		if !slices.Equal(rows, want) {
			t.Errorf("rows = %+v, want %+v", rows, want)
		}
		if d.total != 5 || d.totalLen != 2122 || d.maxLen != 2000 {
			t.Errorf("total = %d, totalLen = %d, maxLen = %d, want 5, 2122, 2000",
				d.total, d.totalLen, d.maxLen)
		}
	})
	t.Run("row size is capped at the observed maximum", func(t *testing.T) {
		d := newLenDist()
		d.add(3)
		rows := d.rows()
		if len(rows) != 1 || rows[0].size != 3 || rows[0].keys != 1 || rows[0].percent != 100 {
			t.Errorf("rows = %+v, want a single capped row", rows)
		}
	})
	t.Run("empty distribution has no rows", func(t *testing.T) {
		if rows := newLenDist().rows(); len(rows) != 0 {
			t.Errorf("rows = %+v, want none", rows)
		}
	})
}

func Test_keyStat_add(t *testing.T) {
	newStat := func() *keyStat {
		s := newKeyStat(2)
		s.add(loadMsg{key: []byte("aaaa"), kind: "string", mem: 100, len: 4, hasLen: true})
		s.add(loadMsg{key: []byte("bb"), kind: "hash", mem: 300, len: 2, hasLen: true})
		s.add(loadMsg{key: []byte("cc"), kind: "string", mem: 200, len: 2, hasLen: true})
		s.add(loadMsg{key: []byte("dd"), kind: "wobble", mem: 50})
		return s
	}
	t.Run("counts sampled keys and memory", func(t *testing.T) {
		if s := newStat(); s.sampled != 4 || s.totalMem != 650 {
			t.Errorf("sampled = %d, totalMem = %d, want 4 and 650", s.sampled, s.totalMem)
		}
	})
	t.Run("none keys are skipped", func(t *testing.T) {
		s := newKeyStat(2)
		s.add(loadMsg{key: []byte("gone"), kind: "none", mem: 1})
		if s.sampled != 0 || s.totalMem != 0 {
			t.Errorf("sampled = %d, totalMem = %d, want both 0", s.sampled, s.totalMem)
		}
		if s.types.get("none").count != 0 {
			t.Error("none got counted as a type")
		}
	})
	t.Run("top keys span types", func(t *testing.T) {
		if got := newStat().top.names(); !slices.Equal(got, []string{"bb", "cc"}) {
			t.Errorf("top = %q, want [bb cc]", got)
		}
	})
	t.Run("per type statistics", func(t *testing.T) {
		s := newStat()
		str := s.types.get("string")
		if str.count != 2 || str.totalMem != 300 || str.totalLen != 6 ||
			str.biggestMemKey != "cc" || str.biggestLenKey != "aaaa" {
			t.Errorf("string = %+v, want 2 keys, 300 mem, 6 len, biggest cc/aaaa", str)
		}
		if st := s.types.get("wobble"); st.totalLen != 0 {
			t.Errorf("wobble = %+v, want no length data", st)
		}
	})
}

func Test_keyStat_report(t *testing.T) {
	s := newKeyStat(2)
	s.add(loadMsg{key: []byte("aaaa"), kind: "string", mem: 100, len: 4, hasLen: true})
	s.add(loadMsg{key: []byte("bb"), kind: "hash", mem: 300, len: 2, hasLen: true})
	s.add(loadMsg{key: []byte("cc"), kind: "string", mem: 200, len: 2, hasLen: true})

	t.Run("only types with keys are reported", func(t *testing.T) {
		r := s.report()
		if len(r.types) != 2 {
			t.Fatalf("types = %+v, want string and hash", r.types)
		}
		if r.types[0].name != "string" || r.types[1].name != "hash" {
			t.Errorf("types = %q %q, want [string hash]", r.types[0].name, r.types[1].name)
		}
	})
	t.Run("totals", func(t *testing.T) {
		r := s.report()
		if r.sampled != 3 || r.totalMem != 600 {
			t.Errorf("sampled = %d, totalMem = %d, want 3 and 600", r.sampled, r.totalMem)
		}
	})
	t.Run("memory percentiles", func(t *testing.T) {
		r := s.report()
		want := []memDistRow{
			{p: 50, size: 200, keys: 2},
			{p: 75, size: 300, keys: 3},
			{p: 90, size: 300, keys: 3},
			{p: 95, size: 300, keys: 3},
			{p: 99, size: 300, keys: 3},
			{p: 100, size: 300, keys: 3},
		}
		if !slices.Equal(r.mem.percentiles, want) {
			t.Errorf("percentiles = %+v, want %+v", r.mem.percentiles, want)
		}
		if r.mem.mean != 200 {
			t.Errorf("mean = %d, want 200", r.mem.mean)
		}
	})
	t.Run("length distribution", func(t *testing.T) {
		r := s.report()
		want := []lenDistRow{
			{size: 4, percent: 100, keys: 3},
		}
		if !slices.Equal(r.lens.rows, want) {
			t.Errorf("rows = %+v, want %+v", r.lens.rows, want)
		}
		if r.lens.totalLen != 8 || r.lens.avgLen != 8.0/3 {
			t.Errorf("totalLen = %d, avgLen = %v, want 8 and %v",
				r.lens.totalLen, r.lens.avgLen, 8.0/3)
		}
	})
	t.Run("empty report has empty distributions", func(t *testing.T) {
		r := newKeyStat(10).report()
		if len(r.mem.percentiles) != 0 || len(r.lens.rows) != 0 || r.mem.mean != 0 {
			t.Errorf("report of no samples = %+v, want empty", r)
		}
	})
}
