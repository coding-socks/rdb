package commands

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/coding-socks/rdb"
	"github.com/gobwas/glob"
)

type dialectFlag struct {
	dialect rdb.Dialect
}

func (t dialectFlag) MarshalText() (text []byte, err error) {
	switch t.dialect.(type) {
	default:
		return nil, fmt.Errorf("unknown dialect %T", t.dialect)
	case rdb.RedisDialect:
		return []byte("redis"), nil
	case rdb.ValkeyDialect:
		return []byte("valkey"), nil
	case rdb.KeyDBDialect:
		return []byte("keydb"), nil
	}
}

func (t *dialectFlag) UnmarshalText(text []byte) error {
	name := string(text)
	switch strings.ToLower(name) {
	default:
		return fmt.Errorf("unknown dialect %q (supported: redis, valkey, keydb)", name)
	case "redis":
		t.dialect = rdb.RedisDialect{}
		return nil
	case "valkey":
		t.dialect = rdb.ValkeyDialect{}
		return nil
	case "keydb":
		t.dialect = rdb.KeyDBDialect{}
		return nil
	}
}

// targetFlag describes where the output of a command goes and in which
// format.
type targetFlag struct {
	format string
	file   string
}

func (t targetFlag) MarshalText() (text []byte, err error) {
	if t.file == "" {
		return []byte(t.format), nil
	}
	return []byte(t.file), nil
}

func (t *targetFlag) UnmarshalText(text []byte) error {
	*t = targetFlag{}
	name := string(text)
	switch {
	case slices.Contains([]string{"json", "rdb", "aof", "manifest"}, name):
		t.format = name
	case strings.HasSuffix(name, ".manifest.tar.tgz"), strings.HasSuffix(name, ".manifest.tgz"):
		t.format, t.file = "manifest", name
	case strings.HasSuffix(name, ".json"):
		t.format, t.file = "json", name
	case strings.HasSuffix(name, ".rdb"):
		t.format, t.file = "rdb", name
	case strings.HasSuffix(name, ".aof"):
		t.format, t.file = "aof", name
	}
	if t.format == "" {
		return fmt.Errorf("rdbc: invalid target %q (supported: json, rdb, aof, manifest, or a file path ending in .json, .rdb, .aof, .manifest.tgz)", name)
	}
	return nil
}

// writer returns the writer of a file targetFlag, stdout for a bare
// format. The file is written to a temporary file next to the
// destination, the returned function closes it: with a nil error the
// temporary file replaces the destination, with an error the partial
// file is removed and the error is returned.
func (t targetFlag) writer() (io.Writer, func(error) error, error) {
	if t.file == "" {
		return os.Stdout, func(err error) error { return err }, nil
	}
	f, err := os.CreateTemp(filepath.Dir(t.file), filepath.Base(t.file)+".rdbc-tmp-*")
	if err != nil {
		return nil, nil, err
	}
	return f, func(err error) error {
		f.Close()
		if err != nil {
			os.Remove(f.Name())
			return err
		}
		return os.Rename(f.Name(), t.file)
	}, nil
}

// includeFlag compiles its glob while the flag is read: the keys that
// match it are transferred.
type includeFlag struct {
	pattern *glob.Pattern
}

func (f *includeFlag) String() string {
	if f.pattern == nil {
		return ""
	}
	return f.pattern.String()
}

func (f *includeFlag) Set(v string) error {
	p, err := glob.Compile(v)
	if err != nil {
		return fmt.Errorf("rdbc transfer: invalid glob %q", v)
	}
	f.pattern = p
	return nil
}

// excludeFlag compiles its glob while the flag is read: the keys that
// match it are skipped.
type excludeFlag struct {
	pattern *glob.Pattern
}

func (f *excludeFlag) String() string {
	if f.pattern == nil {
		return ""
	}
	return f.pattern.String()
}

func (f *excludeFlag) Set(v string) error {
	p, err := glob.Compile(v)
	if err != nil {
		return fmt.Errorf("rdbc transfer: invalid glob %q", v)
	}
	f.pattern = p
	return nil
}
