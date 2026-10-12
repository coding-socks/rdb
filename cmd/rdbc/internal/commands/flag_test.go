package commands

import (
	"flag"
	"testing"
)

func TestTargetFlag_UnmarshalText(t *testing.T) {
	tests := []struct {
		name    string
		text    string
		format  string
		file    string
		wantErr bool
	}{
		{name: "json", text: "json", format: "json"},
		{name: "rdb", text: "rdb", format: "rdb"},
		{name: "aof", text: "aof", format: "aof"},
		{name: "manifest", text: "manifest", format: "manifest"},
		{name: "/tmp/out.json", text: "/tmp/out.json", format: "json", file: "/tmp/out.json"},
		{name: "/tmp/out.rdb", text: "/tmp/out.rdb", format: "rdb", file: "/tmp/out.rdb"},
		{name: "/tmp/out.aof", text: "/tmp/out.aof", format: "aof", file: "/tmp/out.aof"},
		{name: "/tmp/out.manifest.tgz", text: "/tmp/out.manifest.tgz", format: "manifest", file: "/tmp/out.manifest.tgz"},
		{name: "/tmp/out.manifest.tar.tgz", text: "/tmp/out.manifest.tar.tgz", format: "manifest", file: "/tmp/out.manifest.tar.tgz"},
		{name: "/tmp/out.txt", text: "/tmp/out.txt", wantErr: true},
		{name: "empty", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var target targetFlag
			// The flag package copies the default value into target
			// before UnmarshalText runs.
			target = targetFlag{format: "json"}
			err := target.UnmarshalText([]byte(tt.text))
			if (err != nil) != tt.wantErr {
				t.Fatalf("UnmarshalText() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			if target.format != tt.format || target.file != tt.file {
				t.Fatalf("got format %q file %q, want %q %q", target.format, target.file, tt.format, tt.file)
			}
		})
	}
}

func TestTargetFlag_TextVar(t *testing.T) {
	var target targetFlag
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.TextVar(&target, "target", targetFlag{format: "json"}, "")
	if err := fs.Parse([]string{"-target", "/tmp/out.rdb"}); err != nil {
		t.Fatal(err)
	}
	if target.format != "rdb" || target.file != "/tmp/out.rdb" {
		t.Fatalf("got format %q file %q", target.format, target.file)
	}
}

func Test_includeFlag(t *testing.T) {
	t.Run("the glob compiles while the flag is read", func(t *testing.T) {
		var f includeFlag
		if err := f.Set("user:*"); err != nil {
			t.Fatal(err)
		}
		if !f.pattern.Match("user:1") || f.pattern.Match("other") {
			t.Error("the glob does not match")
		}
		if f.String() != "user:*" {
			t.Errorf("String() = %q, want user:*", f.String())
		}
	})

	t.Run("a malformed glob fails", func(t *testing.T) {
		var f excludeFlag
		if err := f.Set("[a"); err == nil {
			t.Error("Set() with a malformed glob did not fail")
		}
	})
}
