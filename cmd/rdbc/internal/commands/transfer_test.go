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
		want    srcMaker
		wantErr bool
	}{
		{
			name: "rdb reads stdin", arg: "rdb", args: []string{"-", "nonflag"},
			want: srcFile{m: srcRDBFile{}, file: "-", args: []string{"nonflag"}},
		},
		{
			name: "aof reads stdin", arg: "aof", args: []string{"-", "nonflag"},
			want: srcFile{m: srcAOFFile{}, file: "-", args: []string{"nonflag"}},
		},
		{
			name: "manifest reads stdin", arg: "manifest", args: []string{"-", "nonflag"},
			want: srcFile{m: srcManifestFile{}, file: "-", args: []string{"nonflag"}},
		},
		{
			name: "/tmp/in.rdb", arg: "rdb", args: []string{"/tmp/in.rdb", "nonflag"},
			want: srcFile{m: srcRDBFile{}, file: "/tmp/in.rdb", args: []string{"nonflag"}},
		},
		{
			name: "/tmp/in.aof", arg: "aof", args: []string{"/tmp/in.aof", "nonflag"},
			want: srcFile{m: srcAOFFile{}, file: "/tmp/in.aof", args: []string{"nonflag"}},
		},
		{
			name: "/tmp/in.manifest.tar", arg: "manifest", args: []string{"/tmp/in.manifest.tar", "nonflag"},
			want: srcFile{m: srcManifestFile{}, file: "/tmp/in.manifest.tar", args: []string{"nonflag"}},
		},
		{
			name: "/tmp/in.manifest.tar.gz", arg: "manifest", args: []string{"/tmp/in.manifest.tar.gz", "nonflag"},
			want: srcFile{m: srcManifestFile{}, file: "/tmp/in.manifest.tar.gz", args: []string{"nonflag"}},
		},
		{
			name: "/tmp/in.manifest.tgz", arg: "manifest", args: []string{"/tmp/in.manifest.tgz", "nonflag"},
			want: srcFile{m: srcManifestFile{}, file: "/tmp/in.manifest.tgz", args: []string{"nonflag"}},
		},
		{
			name: "the aof source names its dialect", arg: "aof", args: []string{"-dialect", "valkey", "/tmp/in.aof", "nonflag"},
			want: srcFile{m: srcAOFFile{}, dialect: rdb.ValkeyDialect{}, file: "/tmp/in.aof", args: []string{"nonflag"}},
		},
		{
			name: "a missing source file", arg: "rdb",
			wantErr: true,
		},
		{
			name: "server+sync", arg: "server+sync", args: []string{"redis://host:6379", "nonflag"},
			want: srcServer{m: srcSyncServer{}, dialer: dialerSingle{u: &url.URL{Scheme: "redis", Host: "host:6379"}}, args: []string{"nonflag"}},
		},
		{
			name: "server+dump", arg: "server+dump", args: []string{"redis://host:6379", "nonflag"},
			want: srcServer{m: srcDumpServer{}, dialer: dialerSingle{u: &url.URL{Scheme: "redis", Host: "host:6379"}}, args: []string{"nonflag"}},
		},
		{
			name: "server+dump takes the keyspace notifications", arg: "server+dump", args: []string{"-keyspace", "redis://host:6379"},
			want: srcServer{m: srcDumpServer{keyspace: true}, dialer: dialerSingle{u: &url.URL{Scheme: "redis", Host: "host:6379"}}, args: []string{}},
		},
		{
			name: "server+aof", arg: "server+aof", args: []string{"redis://host:6379", "nonflag"},
			want: srcServer{m: srcAOFServer{}, dialer: dialerSingle{u: &url.URL{Scheme: "redis", Host: "host:6379"}}, args: []string{"nonflag"}},
		},
		{
			name: "server+aof takes the keyspace notifications", arg: "server+aof", args: []string{"-keyspace", "redis://host:6379"},
			want: srcServer{m: srcAOFServer{keyspace: true}, dialer: dialerSingle{u: &url.URL{Scheme: "redis", Host: "host:6379"}}, args: []string{}},
		},
		{
			name: "an unknown strategy", arg: "unknown",
			wantErr: true,
		},
		{
			name: "a server source takes no dialect flag", arg: "server+sync", args: []string{"-dialect", "valkey", "redis://host:6379"},
			wantErr: true,
		},
		{
			name: "a server source takes the dialect of the scheme", arg: "server+dump", args: []string{"valkeys://host:6379", "nonflag"},
			want: srcServer{m: srcDumpServer{}, dialer: dialerSingle{u: &url.URL{Scheme: "valkeys", Host: "host:6379"}}, args: []string{"nonflag"}},
		},
		{
			name: "a server source takes the -cluster flag", arg: "server+dump", args: []string{"-cluster", "redis://host:6379", "nonflag"},
			want: srcServer{m: srcDumpServer{}, dialer: dialerCluster{u: &url.URL{Scheme: "redis", Host: "host:6379"}}, args: []string{"nonflag"}},
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
		src, err := parseSrcCmd("rdb", []string{path})
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

	t.Run("a dash reads stdin and closes nothing", func(t *testing.T) {
		src, err := parseSrcCmd("aof", []string{"-"})
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
		if err := closer.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
}

func Test_parseDstCmd(t *testing.T) {
	tests := []struct {
		name    string
		arg     string
		args    []string
		want    dstMaker
		wantErr bool
	}{
		{
			name: "unknown arg", arg: "unknown",
			wantErr: true,
		},
		{
			name: "json writes stdout", arg: "json", args: []string{"-", "nonflag"},
			want: dstFile{m: dstJSONFile{}, file: "-", args: []string{"nonflag"}},
		},
		{
			name: "rdb writes stdout", arg: "rdb", args: []string{"-", "nonflag"},
			want: dstFile{m: dstRDBFile{}, file: "-", args: []string{"nonflag"}},
		},
		{
			name: "aof writes stdout", arg: "aof", args: []string{"-", "nonflag"},
			want: dstFile{m: dstAOFFile{}, file: "-", args: []string{"nonflag"}},
		},
		{
			name: "manifest writes stdout", arg: "manifest", args: []string{"-", "nonflag"},
			want: dstFile{m: dstManifestFile{}, file: "-", args: []string{"nonflag"}},
		},
		{
			name: "/tmp/out.json", arg: "json", args: []string{"/tmp/out.json", "nonflag"},
			want: dstFile{file: "/tmp/out.json", m: dstJSONFile{}, args: []string{"nonflag"}},
		},
		{
			name: "/tmp/out.rdb", arg: "rdb", args: []string{"/tmp/out.rdb", "nonflag"},
			want: dstFile{file: "/tmp/out.rdb", m: dstRDBFile{}, args: []string{"nonflag"}},
		},
		{
			name: "/tmp/out.aof", arg: "aof", args: []string{"/tmp/out.aof", "nonflag"},
			want: dstFile{file: "/tmp/out.aof", m: dstAOFFile{}, args: []string{"nonflag"}},
		},
		{
			name: "/tmp/out.manifest.tgz", arg: "manifest", args: []string{"/tmp/out.manifest.tgz", "nonflag"},
			want: dstFile{file: "/tmp/out.manifest.tgz", m: dstManifestFile{}, args: []string{"nonflag"}},
		},
		{
			name: "a missing destination file", arg: "json",
			wantErr: true,
		},
		{
			name: "a server destination takes the batch size ", arg: "server+restore", args: []string{"-batch", "20", "keydb://host:6379"},
			want: dstServer{kind: chain.KindDump, dialer: dialerSingle{u: &url.URL{Scheme: "keydb", Host: "host:6379"}}, batch: 20, args: []string{}},
		},
		{
			name: "a missing destination url", arg: "server+restore",
			wantErr: true,
		},
		{
			name: "an invalid destination url", arg: "server+restore", args: []string{"::"},
			wantErr: true,
		},
		{
			name: "server+restore", arg: "server+restore", args: []string{"redis://host:6379"},
			want: dstServer{kind: chain.KindDump, dialer: dialerSingle{u: &url.URL{Scheme: "redis", Host: "host:6379"}}, batch: 500, args: []string{}},
		},
		{
			name: "server+aof", arg: "server+aof", args: []string{"redis://host:6379"},
			want: dstServer{kind: chain.KindAOF, dialer: dialerSingle{u: &url.URL{Scheme: "redis", Host: "host:6379"}}, batch: 500, args: []string{}},
		},
		{
			name: "server", arg: "server", args: []string{"redis://host:6379"},
			want: dstServer{kind: chain.KindDump | chain.KindAOF, dialer: dialerSingle{u: &url.URL{Scheme: "redis", Host: "host:6379"}}, batch: 500, args: []string{}},
		},
		{
			name: "a server destination takes the -cluster flag", arg: "server+restore", args: []string{"-cluster", "-batch", "20", "redis://host:6379"},
			want: dstServer{kind: chain.KindDump, dialer: dialerCluster{u: &url.URL{Scheme: "redis", Host: "host:6379"}}, batch: 20, args: []string{}},
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
		dst, err := parseDstCmd("rdb", []string{path})
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

	t.Run("a dash writes stdout and closes nothing", func(t *testing.T) {
		dst, err := parseDstCmd("json", []string{"-"})
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
		if err := closer.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
}

func Test_converterFor(t *testing.T) {
	tests := []struct {
		name string
		src  srcMaker
		dst  dstMaker
		want Converter
	}{
		// The payloads of the SYNC command and of an RDB file.
		{
			name: "sync to rdb",
			src:  srcServer{m: srcSyncServer{}}, dst: dstFile{m: dstRDBFile{}},
		},
		{
			name: "sync to manifest",
			src:  srcServer{m: srcSyncServer{}}, dst: dstFile{m: dstManifestFile{}},
		},
		{
			name: "sync to json",
			src:  srcServer{m: srcSyncServer{}}, dst: dstFile{m: dstJSONFile{}},
			want: ConverterJSON,
		},
		{
			name: "sync to aof",
			src:  srcServer{m: srcSyncServer{}}, dst: dstFile{m: dstAOFFile{}},
			want: ConverterAOF,
		},
		{
			name: "sync to a dump server",
			src:  srcServer{m: srcSyncServer{}}, dst: dstServer{kind: chain.KindDump},
			want: ConverterDump,
		},
		{
			name: "sync to an aof server",
			src:  srcServer{m: srcSyncServer{}}, dst: dstFile{m: dstAOFFile{}},
			want: ConverterAOF,
		},
		{
			name: "sync to a dump+aof server",
			src:  srcServer{m: srcSyncServer{}}, dst: dstServer{kind: chain.KindDump | chain.KindAOF},
			want: ConverterDump,
		},
		// RDB
		{
			name: "rdb to rdb",
			src:  srcFile{m: srcRDBFile{}}, dst: dstFile{m: dstRDBFile{}},
		},
		{
			name: "rdb to json",
			src:  srcFile{m: srcRDBFile{}}, dst: dstFile{m: dstJSONFile{}},
			want: ConverterJSON,
		},
		{
			name: "rdb to dump",
			src:  srcFile{m: srcRDBFile{}}, dst: dstServer{kind: chain.KindDump},
			want: ConverterDump,
		},
		// SCAN and DUMP
		{
			name: "scan to rdb",
			src:  srcServer{m: srcDumpServer{}}, dst: dstFile{m: dstRDBFile{}},
			want: ConverterRDB,
		},
		{
			name: "scan to manifest",
			src:  srcServer{m: srcDumpServer{}}, dst: dstFile{m: dstManifestFile{}},
			want: ConverterRDB,
		},
		{
			name: "scan to json",
			src:  srcServer{m: srcDumpServer{}}, dst: dstFile{m: dstJSONFile{}},
			want: ConverterJSON,
		},
		{
			name: "scan to aof",
			src:  srcServer{m: srcDumpServer{}}, dst: dstFile{m: dstAOFFile{}},
			want: ConverterAOF,
		},
		{
			name: "scan to a dump server",
			src:  srcServer{m: srcDumpServer{}}, dst: dstServer{kind: chain.KindDump},
		},
		{
			name: "scan to an aof server",
			src:  srcServer{m: srcDumpServer{}}, dst: dstServer{kind: chain.KindAOF},
			want: ConverterAOF,
		},
		{
			name: "scan to a dump+aof server",
			src:  srcServer{m: srcDumpServer{}}, dst: dstServer{kind: chain.KindDump | chain.KindAOF},
		},
		// AOF
		{
			name: "aof to an aof file",
			src:  srcFile{m: srcAOFFile{}}, dst: dstFile{m: dstAOFFile{}},
		},
		{
			name: "aof to an aof server",
			src:  srcFile{m: srcAOFFile{}}, dst: dstServer{kind: chain.KindAOF},
		},
		{
			name: "aof to a dump+aof server",
			src:  srcFile{m: srcAOFFile{}}, dst: dstServer{kind: chain.KindDump | chain.KindAOF},
		},
		{
			name: "aof to a dump server",
			src:  srcFile{m: srcAOFFile{}}, dst: dstServer{kind: chain.KindDump},
			want: ConverterDump,
		},
		{
			name: "aof to rdb",
			src:  srcFile{m: srcAOFFile{}}, dst: dstFile{m: dstRDBFile{}},
			want: ConverterRDB,
		},
		{
			name: "aof to manifest",
			src:  srcFile{m: srcAOFFile{}}, dst: dstFile{m: dstManifestFile{}},
		},
		{
			name: "aof to json",
			src:  srcFile{m: srcAOFFile{}}, dst: dstFile{m: dstJSONFile{}},
			want: ConverterJSON,
		},
		// Manifest
		{
			name: "manifest to manifest",
			src:  srcFile{m: srcManifestFile{}}, dst: dstFile{m: dstManifestFile{}},
		},
		{
			name: "manifest to a dump server",
			src:  srcFile{m: srcManifestFile{}}, dst: dstServer{kind: chain.KindDump},
			want: ConverterDump,
		},
		{
			name: "manifest to an aof server",
			src:  srcFile{m: srcManifestFile{}}, dst: dstFile{m: dstAOFFile{}},
			want: ConverterAOF,
		},
		{
			name: "manifest to a dump+aof server",
			src:  srcFile{m: srcManifestFile{}}, dst: dstServer{kind: chain.KindDump | chain.KindAOF},
			want: ConverterDump,
		},
		{
			name: "manifest to rdb",
			src:  srcFile{m: srcManifestFile{}}, dst: dstFile{m: dstRDBFile{}},
			want: ConverterRDB,
		},
		{
			name: "manifest to json",
			src:  srcFile{m: srcManifestFile{}}, dst: dstFile{m: dstJSONFile{}},
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
