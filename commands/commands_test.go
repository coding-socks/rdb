package commands

import (
	"reflect"
	"testing"
)

func TestSplitCmd(t *testing.T) {
	tests := []struct {
		name string
		arg  []byte
		want [][]byte
	}{
		{
			name: "empty",
			arg:  []byte{},
			want: nil,
		},
		{
			name: "single",
			arg:  []byte("COMMAND"),
			want: [][]byte{[]byte("COMMAND")},
		},
		{
			name: "multi",
			arg:  []byte("COMMAND ARG1 ARG2"),
			want: [][]byte{[]byte("COMMAND"), []byte("ARG1"), []byte("ARG2")},
		},
		{
			name: "extra white space",
			arg:  []byte("COMMAND  ARG1 ARG2   ARG3"),
			want: [][]byte{[]byte("COMMAND"), []byte("ARG1"), []byte("ARG2"), []byte("ARG3")},
		},
		{
			name: "escaped white space",
			arg:  []byte(`COMMAND ARG1 \  ARG2`),
			want: [][]byte{[]byte("COMMAND"), []byte("ARG1"), []byte(" "), []byte("ARG2")},
		},
		{
			name: "single quote",
			arg:  []byte(`COMMAND 'ARG 1\' ARG2`),
			want: [][]byte{[]byte("COMMAND"), []byte(`ARG 1\`), []byte("ARG2")},
		},
		{
			name: "double quote",
			arg:  []byte(`COMMAND "ARG 1" ARG2`),
			want: [][]byte{[]byte("COMMAND"), []byte("ARG 1"), []byte("ARG2")},
		},
		{
			name: "escape char",
			arg:  []byte(`COMMAND "ARG \" 1" ARG2`),
			want: [][]byte{[]byte("COMMAND"), []byte(`ARG " 1`), []byte("ARG2")},
		},
		{
			name: "escape char",
			arg:  []byte(`COMMAND "ARG \\ 1" ARG2`),
			want: [][]byte{[]byte("COMMAND"), []byte(`ARG \ 1`), []byte("ARG2")},
		},
		{
			name: "open double quote",
			arg:  []byte(`COMMAND "`),
			want: [][]byte{[]byte("COMMAND"), []byte("")},
		},
		{
			name: "open single quote",
			arg:  []byte(`COMMAND '`),
			want: [][]byte{[]byte("COMMAND"), []byte("")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SplitCmd(tt.arg)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SplitCmd() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestTable_Lookup(t *testing.T) {
	tests := []struct {
		name    string
		args    [][]byte
		want    string
		wantErr bool
	}{
		{
			name: "command",
			args: [][]byte{[]byte("GET"), []byte("foo")},
			want: "GET",
		},
		{
			name: "command case insensitive",
			args: [][]byte{[]byte("get"), []byte("foo")},
			want: "GET",
		},
		{
			name: "subcommand",
			args: [][]byte{[]byte("MEMORY"), []byte("USAGE"), []byte("mykey")},
			want: "USAGE",
		},
		{
			name:    "empty command",
			args:    [][]byte{},
			wantErr: true,
		},
		{
			name:    "unknown command",
			args:    [][]byte{[]byte("TOTALLY_UNKNOWN_COMMAND"), []byte("foo")},
			wantErr: true,
		},
		{
			name:    "unknown subcommand",
			args:    [][]byte{[]byte("MEMORY"), []byte("UNKNOWN_SUB"), []byte("foo")},
			wantErr: true,
		},
		{
			name: "container as bare name",
			args: [][]byte{[]byte("HIMPORT")},
			want: "HIMPORT",
		},
		{
			name: "variadic command without subcommands",
			args: [][]byte{[]byte("QUIT"), []byte("bye")},
			want: "QUIT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd, err := RedisCommands.Lookup(tt.args)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Lookup(%v) error = %v, wantErr %v", tt.args, err, tt.wantErr)
			}
			if err == nil && cmd.Name != tt.want {
				t.Errorf("Lookup(%v) = %q, want %q", tt.args, cmd.Name, tt.want)
			}
		})
	}
}

func TestCommand_ExtractKeys(t *testing.T) {
	tests := []struct {
		name    string
		args    [][]byte
		want    [][]byte
		wantErr bool
	}{
		{
			name: "GET",
			args: [][]byte{[]byte("GET"), []byte("foo")},
			want: [][]byte{[]byte("foo")},
		},
		{
			name: "SET with options",
			args: [][]byte{[]byte("SET"), []byte("foo"), []byte("bar"), []byte("EX"), []byte("10"), []byte("NX")},
			want: [][]byte{[]byte("foo")},
		},
		{
			name: "MGET",
			args: [][]byte{[]byte("MGET"), []byte("a"), []byte("b"), []byte("c")},
			want: [][]byte{[]byte("a"), []byte("b"), []byte("c")},
		},
		{
			name: "MSET",
			args: [][]byte{[]byte("MSET"), []byte("a"), []byte("1"), []byte("b"), []byte("2"), []byte("c"), []byte("3")},
			want: [][]byte{[]byte("a"), []byte("b"), []byte("c")},
		},
		{
			name: "DEL",
			args: [][]byte{[]byte("DEL"), []byte("a"), []byte("b"), []byte("c")},
			want: [][]byte{[]byte("a"), []byte("b"), []byte("c")},
		},
		{
			name: "EXISTS",
			args: [][]byte{[]byte("EXISTS"), []byte("k1"), []byte("k2")},
			want: [][]byte{[]byte("k1"), []byte("k2")},
		},
		{
			name: "MEMORY USAGE subcommand",
			args: [][]byte{[]byte("MEMORY"), []byte("USAGE"), []byte("mykey")},
			want: [][]byte{[]byte("mykey")},
		},
		{
			name: "OBJECT ENCODING subcommand",
			args: [][]byte{[]byte("OBJECT"), []byte("ENCODING"), []byte("mykey")},
			want: [][]byte{[]byte("mykey")},
		},
		{
			name: "XGROUP CREATE subcommand",
			args: [][]byte{[]byte("XGROUP"), []byte("CREATE"), []byte("mystream"), []byte("mygroup"), []byte("0")},
			want: [][]byte{[]byte("mystream")},
		},
		{
			name: "EVAL custom parser",
			args: [][]byte{[]byte("EVAL"), []byte("return 1"), []byte("2"), []byte("k1"), []byte("k2"), []byte("arg1")},
			want: [][]byte{[]byte("k1"), []byte("k2")},
		},
		{
			name: "EVAL with 0 keys",
			args: [][]byte{[]byte("EVAL"), []byte("return 1"), []byte("0")},
			want: nil,
		},
		{
			name: "FCALL custom parser",
			args: [][]byte{[]byte("FCALL"), []byte("myfunc"), []byte("3"), []byte("k1"), []byte("k2"), []byte("k3"), []byte("a1")},
			want: [][]byte{[]byte("k1"), []byte("k2"), []byte("k3")},
		},
		{
			name: "XREAD with options",
			args: [][]byte{[]byte("XREAD"), []byte("COUNT"), []byte("10"), []byte("STREAMS"), []byte("s1"), []byte("s2"), []byte("0"), []byte("0")},
			want: [][]byte{[]byte("s1"), []byte("s2")},
		},
		{
			name: "XREAD with block",
			args: [][]byte{[]byte("XREAD"), []byte("BLOCK"), []byte("1000"), []byte("STREAMS"), []byte("mystream"), []byte("$")},
			want: [][]byte{[]byte("mystream")},
		},
		{
			name: "XREADGROUP",
			args: [][]byte{[]byte("XREADGROUP"), []byte("GROUP"), []byte("g"), []byte("c"), []byte("COUNT"), []byte("5"), []byte("STREAMS"), []byte("s1"), []byte("0-0")},
			want: [][]byte{[]byte("s1")},
		},
		{
			name: "MIGRATE single key",
			args: [][]byte{[]byte("MIGRATE"), []byte("127.0.0.1"), []byte("6379"), []byte("mykey"), []byte("0"), []byte("5000")},
			want: [][]byte{[]byte("mykey")},
		},
		{
			name: "MIGRATE with KEYS",
			args: [][]byte{[]byte("MIGRATE"), []byte("127.0.0.1"), []byte("6379"), []byte(""), []byte("0"), []byte("5000"), []byte("KEYS"), []byte("k1"), []byte("k2"), []byte("k3")},
			want: [][]byte{[]byte("k1"), []byte("k2"), []byte("k3")},
		},
		{
			name: "SORT with STORE",
			args: [][]byte{[]byte("SORT"), []byte("mylist"), []byte("BY"), []byte("weight_*"), []byte("GET"), []byte("item_*"), []byte("STORE"), []byte("outlist")},
			want: [][]byte{[]byte("mylist"), []byte("outlist")},
		},
		{
			name: "SORT without STORE",
			args: [][]byte{[]byte("SORT"), []byte("mylist")},
			want: [][]byte{[]byte("mylist")},
		},
		{
			name: "SORT_RO",
			args: [][]byte{[]byte("SORT_RO"), []byte("mylist"), []byte("BY"), []byte("weight_*")},
			want: [][]byte{[]byte("mylist")},
		},
		{
			name: "BITFIELD",
			args: [][]byte{[]byte("BITFIELD"), []byte("mykey"), []byte("GET"), []byte("u8"), []byte("0")},
			want: [][]byte{[]byte("mykey")},
		},
		{
			name: "GEORADIUS with STORE",
			args: [][]byte{[]byte("GEORADIUS"), []byte("Sicily"), []byte("15"), []byte("37"), []byte("200"), []byte("km"), []byte("STORE"), []byte("out")},
			want: [][]byte{[]byte("Sicily"), []byte("out")},
		},
		{
			name: "GEORADIUS without STORE",
			args: [][]byte{[]byte("GEORADIUS"), []byte("Sicily"), []byte("15"), []byte("37"), []byte("200"), []byte("km")},
			want: [][]byte{[]byte("Sicily")},
		},
		{
			name: "ZUNIONSTORE",
			args: [][]byte{[]byte("ZUNIONSTORE"), []byte("dst"), []byte("2"), []byte("z1"), []byte("z2"), []byte("WEIGHTS"), []byte("1"), []byte("2")},
			want: [][]byte{[]byte("dst"), []byte("z1"), []byte("z2")},
		},
		{
			name: "ZUNION",
			args: [][]byte{[]byte("ZUNION"), []byte("2"), []byte("z1"), []byte("z2")},
			want: [][]byte{[]byte("z1"), []byte("z2")},
		},
		{
			name: "SINTERCARD",
			args: [][]byte{[]byte("SINTERCARD"), []byte("2"), []byte("s1"), []byte("s2")},
			want: [][]byte{[]byte("s1"), []byte("s2")},
		},
		{
			name: "module command with one key",
			args: [][]byte{[]byte("BF.LOADCHUNK"), []byte("bf:key"), []byte("1"), []byte("chunk")},
			want: [][]byte{[]byte("bf:key")},
		},
		{
			name: "module command with two keys",
			args: [][]byte{[]byte("TS.CREATERULE"), []byte("src"), []byte("dst"), []byte("AGGREGATION"), []byte("AVG"), []byte("1000")},
			want: [][]byte{[]byte("src"), []byte("dst")},
		},
		{
			name: "module command with a key multiple and a trailing argument",
			args: [][]byte{[]byte("JSON.MGET"), []byte("k1"), []byte("k2"), []byte("k3"), []byte("$")},
			want: [][]byte{[]byte("k1"), []byte("k2"), []byte("k3")},
		},
		{
			name: "module command with a key multiple behind a key count",
			args: [][]byte{[]byte("CMS.MERGE"), []byte("dst"), []byte("2"), []byte("s1"), []byte("s2"), []byte("WEIGHTS"), []byte("1"), []byte("2")},
			want: [][]byte{[]byte("dst"), []byte("s1"), []byte("s2")},
		},
		{
			name: "PFMERGE",
			args: [][]byte{[]byte("PFMERGE"), []byte("dest"), []byte("src1"), []byte("src2")},
			want: [][]byte{[]byte("dest"), []byte("src1"), []byte("src2")},
		},
		{
			name:    "arity violation GET missing arg",
			args:    [][]byte{[]byte("GET")},
			wantErr: true,
		},
		{
			name:    "arity violation GET too many args",
			args:    [][]byte{[]byte("GET"), []byte("k1"), []byte("k2")},
			wantErr: true,
		},
		{
			name:    "arity violation SET missing args",
			args:    [][]byte{[]byte("SET"), []byte("k1")},
			wantErr: true,
		},
		{
			name:    "EVAL negative numkeys",
			args:    [][]byte{[]byte("EVAL"), []byte("return 1"), []byte("-1"), []byte("k1")},
			wantErr: true,
		},
		{
			name:    "EVAL numkeys exceeding args",
			args:    [][]byte{[]byte("EVAL"), []byte("return 1"), []byte("5"), []byte("k1")},
			wantErr: true,
		},
		{
			name:    "XREAD missing STREAMS keyword",
			args:    [][]byte{[]byte("XREAD"), []byte("COUNT"), []byte("10"), []byte("k1"), []byte("0")},
			wantErr: true,
		},
		{
			name:    "XREAD uneven streams and IDs",
			args:    [][]byte{[]byte("XREAD"), []byte("STREAMS"), []byte("s1"), []byte("s2"), []byte("0")},
			wantErr: true,
		},
		{
			name:    "MIGRATE conflicting key and KEYS option",
			args:    [][]byte{[]byte("MIGRATE"), []byte("127.0.0.1"), []byte("6379"), []byte("mykey"), []byte("0"), []byte("5000"), []byte("KEYS"), []byte("k1"), []byte("k2")},
			wantErr: true,
		},
		{
			name: "PING",
			args: [][]byte{[]byte("PING")},
			want: [][]byte{},
		},
		{
			name: "CLUSTER KEYSLOT subcommand",
			args: [][]byte{[]byte("CLUSTER"), []byte("KEYSLOT"), []byte("mykey")},
			want: [][]byte{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd, err := RedisCommands.Lookup(tt.args)
			if err != nil {
				t.Fatalf("Lookup(%v) failed: %v", tt.args, err)
			}
			keys, err := cmd.ExtractKeys(tt.args)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ExtractKeys(%v) error = %v, wantErr %v", tt.args, err, tt.wantErr)
			}
			if !tt.wantErr {
				if len(keys) == 0 && len(tt.want) == 0 {
					return
				}
				if !reflect.DeepEqual(keys, tt.want) {
					t.Errorf("ExtractKeys(%v) = %v, want %v", tt.args, keys, tt.want)
				}
			}
		})
	}
}
