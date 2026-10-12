package commands

import (
	"bytes"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/exp/teatest/v2"
	"github.com/coding-socks/rdb"
	"github.com/coding-socks/rdb/internal/chain"
)

func Test_parseSrcCmd(t *testing.T) {
	tests := []struct {
		name    string
		arg     string
		args    []string
		want    srcCmd
		wantErr bool
	}{
		{
			name: "rdb", arg: "rdb", args: []string{"dst"},
			want: srcCmd{kinds: chain.KindRDB, srcMaker: rdbFileSrc{}, rest: []string{"dst"}},
		},
		{
			name: "aof", arg: "aof", args: []string{"dst"},
			want: srcCmd{kinds: chain.KindAOF, srcMaker: aofFileSrc{}, rest: []string{"dst"}},
		},
		{
			name: "manifest", arg: "manifest", args: []string{"dst"},
			want: srcCmd{kinds: chain.KindRDB | chain.KindAOF, srcMaker: manifestFileSrc{}, rest: []string{"dst"}},
		},
		{
			name: "/tmp/in.rdb", arg: "/tmp/in.rdb", args: []string{"dst"},
			want: srcCmd{kinds: chain.KindRDB, srcMaker: rdbFileSrc{fileSrc{file: "/tmp/in.rdb"}}, rest: []string{"dst"}},
		},
		{
			name: "/tmp/in.aof", arg: "/tmp/in.aof", args: []string{"dst"},
			want: srcCmd{kinds: chain.KindAOF, srcMaker: aofFileSrc{fileSrc{file: "/tmp/in.aof"}}, rest: []string{"dst"}},
		},
		{
			name: "/tmp/in.manifest.tar", arg: "/tmp/in.manifest.tar", args: []string{"dst"},
			want: srcCmd{kinds: chain.KindRDB | chain.KindAOF, srcMaker: manifestFileSrc{fileSrc{file: "/tmp/in.manifest.tar"}}, rest: []string{"dst"}},
		},
		{
			name: "/tmp/in.manifest.tar.gz", arg: "/tmp/in.manifest.tar.gz", args: []string{"dst"},
			want: srcCmd{kinds: chain.KindRDB | chain.KindAOF, srcMaker: manifestFileSrc{fileSrc{file: "/tmp/in.manifest.tar.gz"}}, rest: []string{"dst"}},
		},
		{
			name: "/tmp/in.manifest.tgz", arg: "/tmp/in.manifest.tgz", args: []string{"dst"},
			want: srcCmd{kinds: chain.KindRDB | chain.KindAOF, srcMaker: manifestFileSrc{fileSrc{file: "/tmp/in.manifest.tgz"}}, rest: []string{"dst"}},
		},
		{
			name: "the aof source names its dialect", arg: "/tmp/in.aof", args: []string{"-dialect", "valkey", "dst"},
			want: srcCmd{kinds: chain.KindAOF, srcMaker: aofFileSrc{fileSrc{dialect: rdb.ValkeyDialect{}, file: "/tmp/in.aof"}}, rest: []string{"dst"}},
		},
		{
			name: "a server source takes no dialect flag", arg: "redis://host:6379", args: []string{"-dialect", "valkey"},
			wantErr: true,
		},
		{
			name: "the sync strategy", arg: "redis://host:6379", args: []string{"-strategy", "sync", "dst"},
			want: srcCmd{kinds: chain.KindRDB, srcMaker: serverSyncSrc{serverSrc{url: &url.URL{Scheme: "redis", Host: "host:6379"}, dialer: dialerSingle{}}}, rest: []string{"dst"}},
		},
		{
			name: "the dump strategy", arg: "redis://host:6379", args: []string{"-strategy", "dump", "dst"},
			want: srcCmd{kinds: chain.KindDump, srcMaker: serverDumpSrc{serverSrc: serverSrc{url: &url.URL{Scheme: "redis", Host: "host:6379"}, dialer: dialerSingle{}}}, rest: []string{"dst"}},
		},
		{
			name: "the aof strategy", arg: "redis://host:6379", args: []string{"-strategy", "aof", "dst"},
			want: srcCmd{kinds: chain.KindAOF, srcMaker: serverAOFSrc{serverSrc: serverSrc{url: &url.URL{Scheme: "redis", Host: "host:6379"}, dialer: dialerSingle{}}}, rest: []string{"dst"}},
		},
		{
			name: "an unknown strategy", arg: "redis://host:6379", args: []string{"-strategy", "bogus"},
			wantErr: true,
		},
		{
			name: "a server source takes the dialect of the scheme", arg: "valkeys://host:6379", args: []string{"-strategy", "dump", "dst"},
			want: srcCmd{kinds: chain.KindDump, srcMaker: serverDumpSrc{serverSrc: serverSrc{url: &url.URL{Scheme: "valkeys", Host: "host:6379"}, dialer: dialerSingle{}}}, rest: []string{"dst"}},
		},
		{
			name: "a server source takes the -cluster flag", arg: "redis://host:6379", args: []string{"-strategy", "dump", "-cluster", "dst"},
			want: srcCmd{kinds: chain.KindDump, srcMaker: serverDumpSrc{serverSrc: serverSrc{url: &url.URL{Scheme: "redis", Host: "host:6379"}, dialer: dialerCluster{}}}, rest: []string{"dst"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src, err := parseSrcCmd(tt.arg, tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseSrcCmd(%q, %v) error = %v, wantErr %v", tt.arg, tt.args, err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}
			if !reflect.DeepEqual(src, tt.want) {
				t.Errorf("parseSrcCmd(%q, %v) = %+v, want %+v", tt.arg, tt.args, src, tt.want)
			}
		})
	}
}

func Test_srcCmd_Make(t *testing.T) {
	t.Run("a file source is returned with its file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "in.rdb")
		if err := os.WriteFile(path, nil, 0o644); err != nil {
			t.Fatal(err)
		}
		src, err := parseSrcCmd(path, nil)
		if err != nil {
			t.Fatal(err)
		}
		s, closer, err := src.Make(t.Context())
		if err != nil {
			t.Fatalf("Make() error = %v", err)
		}
		if _, ok := s.(*chain.RDBFileSrc); !ok {
			t.Errorf("Make() = %T, want an rdb source", s)
		}
		f, ok := closer.(*os.File)
		if !ok || f == nil {
			t.Fatalf("Make() closer = %T, want the file of the source", closer)
		}
		if err := closer.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	t.Run("a bare name reads stdin and closes nothing", func(t *testing.T) {
		src, err := parseSrcCmd("aof", nil)
		if err != nil {
			t.Fatal(err)
		}
		s, closer, err := src.Make(t.Context())
		if err != nil {
			t.Fatalf("Make() error = %v", err)
		}
		if _, ok := s.(*chain.AOFFileSrc); !ok {
			t.Errorf("Make() = %T, want an aof source", s)
		}
		if closer != nil {
			t.Errorf("Make() closer = %v, want none", closer)
		}
	})
}

func Test_parseDstCmd(t *testing.T) {
	tests := []struct {
		name    string
		arg     string
		args    []string
		want    dstCmd
		wantErr bool
	}{
		{
			name: "unknown arg", arg: "unknown",
			wantErr: true,
		},
		{
			name: "json", arg: "json", args: []string{"-x"},
			want: dstCmd{kinds: chain.KindJSON, dstMaker: jsonFileDst{}, rest: []string{"-x"}},
		},
		{
			name: "rdb", arg: "rdb", args: []string{"-x"},
			want: dstCmd{kinds: chain.KindRDB, dstMaker: rdbFileDst{}, rest: []string{"-x"}},
		},
		{
			name: "aof", arg: "aof", args: []string{"-x"},
			want: dstCmd{kinds: chain.KindAOF, dstMaker: aofFileDst{}, rest: []string{"-x"}},
		},
		{
			name: "manifest", arg: "manifest", args: []string{"-x"},
			want: dstCmd{kinds: chain.KindRDB | chain.KindAOF, dstMaker: manifestFileDst{}, rest: []string{"-x"}},
		},
		{
			name: "/tmp/out.json", arg: "/tmp/out.json", args: []string{"-x"},
			want: dstCmd{kinds: chain.KindJSON, dstMaker: jsonFileDst{fileDst{file: "/tmp/out.json"}}, rest: []string{"-x"}},
		},
		{
			name: "/tmp/out.rdb", arg: "/tmp/out.rdb", args: []string{"-x"},
			want: dstCmd{kinds: chain.KindRDB, dstMaker: rdbFileDst{fileDst{file: "/tmp/out.rdb"}}, rest: []string{"-x"}},
		},
		{
			name: "/tmp/out.aof", arg: "/tmp/out.aof", args: []string{"-x"},
			want: dstCmd{kinds: chain.KindAOF, dstMaker: aofFileDst{fileDst{file: "/tmp/out.aof"}}, rest: []string{"-x"}},
		},
		{
			name: "/tmp/out.manifest.tgz", arg: "/tmp/out.manifest.tgz", args: []string{"-x"},
			want: dstCmd{kinds: chain.KindRDB | chain.KindAOF, dstMaker: manifestFileDst{fileDst{file: "/tmp/out.manifest.tgz"}}, rest: []string{"-x"}},
		},
		{
			name: "a server destination takes the batch size ", arg: "keydb://host:6379", args: []string{"-batch", "20"},
			want: dstCmd{kinds: chain.KindDump, dstMaker: serverDst{url: &url.URL{Scheme: "keydb", Host: "host:6379"}, dialer: dialerSingle{}, batch: 20}, rest: []string{}},
		},
		{
			name: "unknown strategy", arg: "redis://host:6379", args: []string{"-strategy", "bogus"},
			wantErr: true,
		},
		{
			name: "the dump strategy", arg: "redis://host:6379", args: []string{"-strategy", "dump"},
			want: dstCmd{kinds: chain.KindDump, dstMaker: serverDst{url: &url.URL{Scheme: "redis", Host: "host:6379"}, dialer: dialerSingle{}, batch: 500}, rest: []string{}},
		},
		{
			name: "the aof strategy", arg: "redis://host:6379", args: []string{"-strategy", "aof"},
			want: dstCmd{kinds: chain.KindAOF, dstMaker: serverDst{url: &url.URL{Scheme: "redis", Host: "host:6379"}, dialer: dialerSingle{}, batch: 500}, rest: []string{}},
		},
		{
			name: "the dump+aof strategy", arg: "redis://host:6379", args: []string{"-strategy", "dump+aof"},
			want: dstCmd{kinds: chain.KindDump | chain.KindAOF, dstMaker: serverDst{url: &url.URL{Scheme: "redis", Host: "host:6379"}, dialer: dialerSingle{}, batch: 500}, rest: []string{}},
		},
		{
			name: "a server destination takes the -cluster flag", arg: "redis://host:6379", args: []string{"-cluster", "-batch", "20"},
			want: dstCmd{kinds: chain.KindDump, dstMaker: serverDst{url: &url.URL{Scheme: "redis", Host: "host:6379"}, dialer: dialerCluster{}, batch: 20}, rest: []string{}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dst, err := parseDstCmd(tt.arg, tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseDstCmd(%q, %v) error = %v, wantErr %v", tt.arg, tt.args, err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}
			if !reflect.DeepEqual(dst, tt.want) {
				t.Errorf("parseDstCmd(%q, %v) = %+v, want %+v", tt.arg, tt.args, dst, tt.want)
			}
		})
	}
}

func Test_dstCmd_Make(t *testing.T) {
	t.Run("a file destination is created and returned with its file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "out.rdb")
		dst, err := parseDstCmd(path, nil)
		if err != nil {
			t.Fatal(err)
		}
		d, closer, err := dst.Make(t.Context())
		if err != nil {
			t.Fatalf("Make() error = %v", err)
		}
		if _, ok := d.(*chain.RDBDst); !ok {
			t.Errorf("Make() = %T, want an rdb destination", d)
		}
		f, ok := closer.(*os.File)
		if !ok || f == nil {
			t.Fatalf("Make() closer = %T, want the file of the destination", closer)
		}
		if err := closer.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	t.Run("a bare name writes stdout and closes nothing", func(t *testing.T) {
		dst, err := parseDstCmd("json", nil)
		if err != nil {
			t.Fatal(err)
		}
		d, closer, err := dst.Make(t.Context())
		if err != nil {
			t.Fatalf("Make() error = %v", err)
		}
		if _, ok := d.(*chain.JSONDst); !ok {
			t.Errorf("Make() = %T, want a json destination", d)
		}
		if closer != nil {
			t.Errorf("Make() closer = %v, want none", closer)
		}
	})
}

func Test_converterFor(t *testing.T) {
	tests := []struct {
		name string
		src  srcCmd
		dst  dstCmd
		want Converter
	}{
		// The payloads of the SYNC command and of an RDB file.
		{
			name: "sync to rdb",
			src:  srcCmd{kinds: chain.KindRDB}, dst: dstCmd{kinds: chain.KindRDB},
		},
		{
			name: "sync to manifest",
			src:  srcCmd{kinds: chain.KindRDB}, dst: dstCmd{kinds: chain.KindRDB | chain.KindAOF},
		},
		{
			name: "sync to json",
			src:  srcCmd{kinds: chain.KindRDB}, dst: dstCmd{kinds: chain.KindJSON},
			want: ConverterJSON,
		},
		{
			name: "sync to aof",
			src:  srcCmd{kinds: chain.KindRDB}, dst: dstCmd{kinds: chain.KindAOF},
			want: ConverterAOF,
		},
		{
			name: "sync to a dump server",
			src:  srcCmd{kinds: chain.KindRDB}, dst: dstCmd{kinds: chain.KindDump},
			want: ConverterDump,
		},
		{
			name: "sync to an aof server",
			src:  srcCmd{kinds: chain.KindRDB}, dst: dstCmd{kinds: chain.KindAOF},
			want: ConverterAOF,
		},
		// The dump conversion keeps the values that have no command
		// representation in the payloads of their keys.
		{
			name: "sync to a dump+aof server",
			src:  srcCmd{kinds: chain.KindRDB}, dst: dstCmd{kinds: chain.KindDump | chain.KindAOF},
			want: ConverterDump,
		},
		{
			name: "rdb to rdb",
			src:  srcCmd{kinds: chain.KindRDB}, dst: dstCmd{kinds: chain.KindRDB},
		},
		{
			name: "rdb to json",
			src:  srcCmd{kinds: chain.KindRDB}, dst: dstCmd{kinds: chain.KindJSON},
			want: ConverterJSON,
		},
		// The keys of a SCAN and a DUMP scan.
		{
			name: "scan to rdb",
			src:  srcCmd{kinds: chain.KindDump}, dst: dstCmd{kinds: chain.KindRDB},
			want: ConverterRDB,
		},
		{
			name: "scan to manifest",
			src:  srcCmd{kinds: chain.KindDump}, dst: dstCmd{kinds: chain.KindRDB | chain.KindAOF},
			want: ConverterRDB,
		},
		{
			name: "scan to json",
			src:  srcCmd{kinds: chain.KindDump}, dst: dstCmd{kinds: chain.KindJSON},
			want: ConverterJSON,
		},
		{
			name: "scan to aof",
			src:  srcCmd{kinds: chain.KindDump}, dst: dstCmd{kinds: chain.KindAOF},
			want: ConverterAOF,
		},
		{
			name: "scan to a dump server",
			src:  srcCmd{kinds: chain.KindDump}, dst: dstCmd{kinds: chain.KindDump},
		},
		{
			name: "scan to an aof server",
			src:  srcCmd{kinds: chain.KindDump}, dst: dstCmd{kinds: chain.KindAOF},
			want: ConverterAOF,
		},
		{
			name: "scan to a dump+aof server",
			src:  srcCmd{kinds: chain.KindDump}, dst: dstCmd{kinds: chain.KindDump | chain.KindAOF},
		},
		// The store commands of a scan+get and of an AOF file.
		{
			name: "aof to an aof file",
			src:  srcCmd{kinds: chain.KindAOF}, dst: dstCmd{kinds: chain.KindAOF},
		},
		{
			name: "aof to an aof server",
			src:  srcCmd{kinds: chain.KindAOF}, dst: dstCmd{kinds: chain.KindAOF},
		},
		{
			name: "aof to a dump+aof server",
			src:  srcCmd{kinds: chain.KindAOF}, dst: dstCmd{kinds: chain.KindDump | chain.KindAOF},
		},
		// The converter of the kind the destination wants rejects the
		// commands it cannot take when the head arrives.
		{
			name: "aof to a dump server",
			src:  srcCmd{kinds: chain.KindAOF}, dst: dstCmd{kinds: chain.KindDump},
			want: ConverterDump,
		},
		{
			name: "aof to rdb",
			src:  srcCmd{kinds: chain.KindAOF}, dst: dstCmd{kinds: chain.KindRDB},
			want: ConverterRDB,
		},
		// The manifest destination decides at Finish on the base
		// snapshot the commands do not carry.
		{
			name: "aof to manifest",
			src:  srcCmd{kinds: chain.KindAOF}, dst: dstCmd{kinds: chain.KindRDB | chain.KindAOF},
		},
		{
			name: "aof to json",
			src:  srcCmd{kinds: chain.KindAOF}, dst: dstCmd{kinds: chain.KindJSON},
			want: ConverterJSON,
		},
		// The base snapshot and the incremental commands of a
		// manifest, the dump conversion keeps the values that have no
		// command representation and passes the commands.
		{
			name: "manifest to manifest",
			src:  srcCmd{kinds: chain.KindRDB | chain.KindAOF}, dst: dstCmd{kinds: chain.KindRDB | chain.KindAOF},
		},
		{
			name: "manifest to a dump server",
			src:  srcCmd{kinds: chain.KindRDB | chain.KindAOF}, dst: dstCmd{kinds: chain.KindDump},
			want: ConverterDump,
		},
		{
			name: "manifest to an aof server",
			src:  srcCmd{kinds: chain.KindRDB | chain.KindAOF}, dst: dstCmd{kinds: chain.KindAOF},
			want: ConverterAOF,
		},
		{
			name: "manifest to a dump+aof server",
			src:  srcCmd{kinds: chain.KindRDB | chain.KindAOF}, dst: dstCmd{kinds: chain.KindDump | chain.KindAOF},
			want: ConverterDump,
		},
		{
			name: "manifest to rdb",
			src:  srcCmd{kinds: chain.KindRDB | chain.KindAOF}, dst: dstCmd{kinds: chain.KindRDB},
			want: ConverterRDB,
		},
		{
			name: "manifest to json",
			src:  srcCmd{kinds: chain.KindRDB | chain.KindAOF}, dst: dstCmd{kinds: chain.KindJSON},
			want: ConverterJSON,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := converterFor(tt.src, tt.dst)
			if got != tt.want {
				t.Errorf("converterFor() got = %s, want %s", got, tt.want)
			}
		})
	}
}

// fakeProgressReporter is a chain.ProgressReporter with one fixed
// reading, the reporter of the model tests.
type fakeProgressReporter struct {
	reports []chain.ProgressReport
}

func (f fakeProgressReporter) Progress() []chain.ProgressReport { return f.reports }

func Test_transferModel_Update(t *testing.T) {
	newTestModel := func(reports []chain.ProgressReport) *teatest.TestModel {
		t.Helper()
		return teatest.NewTestModel(t, newTransferModel(fakeProgressReporter{reports}), teatest.WithInitialTermSize(80, 24))
	}

	t.Run("a report with a total moves the bar", func(t *testing.T) {
		reports := []chain.ProgressReport{{I: 5, N: 10, Unit: "keys"}}
		tm := newTestModel(reports)
		tm.Send(progressMsg{reports: reports})
		var out []byte
		teatest.WaitFor(t, tm.Output(), func(b []byte) bool {
			out = append(out, b...)
			return bytes.Contains(out, []byte("5/10 keys"))
		}, teatest.WithDuration(2*time.Second))
		tm.Send(tea.KeyPressMsg{Code: 'c', Mod: tea.ModCtrl})
		tm.WaitFinished(t, teatest.WithFinalTimeout(2*time.Second))
		fm := tm.FinalModel(t).(transferModel)
		if len(fm.reports) != 1 || fm.reports[0].I != 5 || fm.reports[0].N != 10 || fm.reports[0].Unit != "keys" {
			t.Errorf("the report = %v, want 5/10 keys", fm.reports)
		}
		if len(fm.progress) != 1 || fm.progress[0].Percent() != 0.5 {
			t.Errorf("the percent = %v, want 0.5", fm.progress[0].Percent())
		}
	})

	t.Run("a report without a total keeps the spinner", func(t *testing.T) {
		reports := []chain.ProgressReport{{I: 3, Unit: "keys"}}
		tm := newTestModel(reports)
		tm.Send(progressMsg{reports: reports})
		var out []byte
		teatest.WaitFor(t, tm.Output(), func(b []byte) bool {
			out = append(out, b...)
			return bytes.Contains(out, []byte("3 keys"))
		}, teatest.WithDuration(2*time.Second))
		tm.Send(tea.KeyPressMsg{Code: 'c', Mod: tea.ModCtrl})
		tm.WaitFinished(t, teatest.WithFinalTimeout(2*time.Second))
		fm := tm.FinalModel(t).(transferModel)
		if len(fm.reports) != 1 || fm.reports[0].N != 0 || fm.reports[0].I != 3 || fm.reports[0].Unit != "keys" {
			t.Errorf("the report = %v, want 3 keys of unknown total", fm.reports)
		}
	})

	t.Run("every step gets its own bar", func(t *testing.T) {
		reports := []chain.ProgressReport{
			{I: 5, N: 10, Unit: "keys"},
			{I: 2, Unit: "keys"},
			{},
		}
		tm := newTestModel(reports)
		tm.Send(progressMsg{reports: reports})
		tm.Send(tea.KeyPressMsg{Code: 'c', Mod: tea.ModCtrl})
		tm.WaitFinished(t, teatest.WithFinalTimeout(2*time.Second))
		fm := tm.FinalModel(t).(transferModel)
		if len(fm.progress) != 3 {
			t.Errorf("the model built %d bars, want 3", len(fm.progress))
		}
		if len(fm.progress) > 0 && fm.progress[0].Percent() != 0.5 {
			t.Errorf("the percent of the first step = %v, want 0.5", fm.progress[0].Percent())
		}
	})

	t.Run("a source without a reporter spins alone", func(t *testing.T) {
		tm := teatest.NewTestModel(t, newTransferModel(nil), teatest.WithInitialTermSize(80, 24))
		var out []byte
		teatest.WaitFor(t, tm.Output(), func(b []byte) bool {
			out = append(out, b...)
			return bytes.Contains(out, []byte("▁▂▃▄▅▆"))
		}, teatest.WithDuration(2*time.Second))
		tm.Send(tea.KeyPressMsg{Code: 'c', Mod: tea.ModCtrl})
		tm.WaitFinished(t, teatest.WithFinalTimeout(2*time.Second))
		fm := tm.FinalModel(t).(transferModel)
		if fm.pr != nil {
			t.Errorf("the reporter = %v, want none", fm.pr)
		}
		if bytes.Contains(out, []byte("keys")) {
			t.Errorf("the view rendered %q, want the spinner alone", out)
		}
	})
}
