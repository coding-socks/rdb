package commands

import "testing"

func TestSuggester_Hint(t *testing.T) {
	s := NewSuggester(RedisCommands)
	tests := []struct {
		name string
		line string
		want string
	}{
		{
			name: "empty",
			line: "",
			want: "",
		},
		{
			name: "unknown command",
			line: "FOO",
			want: "",
		},
		{
			name: "prefix of a command name",
			line: "GE",
			want: "T key",
		},
		{
			name: "command name",
			line: "GET",
			want: " key",
		},
		{
			name: "command name",
			line: "HSET",
			want: " key field value [field value ...]",
		},
		{
			name: "command name with space",
			line: "HSET ",
			want: "key field value [field value ...]",
		},
		{
			name: "command name with key",
			line: "HSET k",
			want: " field value [field value ...]",
		},
		{
			name: "command name with key and space",
			line: "HSET k ",
			want: "field value [field value ...]",
		},
		{
			name: "command name with key and field",
			line: "HSET k f",
			want: " value [field value ...]",
		},
		{
			name: "command name with key, field and space",
			line: "HSET k f ",
			want: "value [field value ...]",
		},
		{
			name: "command name with key, field and value",
			line: "HSET k f v",
			want: " [field value ...]",
		},
		{
			name: "command name with key, field and value and space",
			line: "HSET k f v ",
			want: "[field value ...]",
		},
		{
			name: "command name with key, field and value and space",
			line: "HSET k f v f",
			want: " value [field value ...]",
		},
		{
			name: "command name with key, field and value and space",
			line: "HSET k f v f ",
			want: "value [field value ...]",
		},
		{
			name: "prefix of a subcommand name",
			line: "acl c",
			want: "AT [category]",
		},
		{
			name: "container suggests its first subcommand",
			line: "ACL ",
			want: "CAT [category]",
		},
		{
			name: "command name complete",
			line: "GET ",
			want: "key",
		},
		{
			name: "case insensitive",
			line: "sEt ",
			want: "key value [NX | XX | IFEQ ifeq-value | IFNE ifne-value | IFDEQ ifdeq-digest | IFDNE ifdne-digest] [GET] [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL]",
		},
		{
			name: "value being typed",
			line: "SET k",
			want: " value [NX | XX | IFEQ ifeq-value | IFNE ifne-value | IFDEQ ifdeq-digest | IFDNE ifdne-digest] [GET] [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL]",
		},
		{
			name: "value being typed",
			line: `SET "my key"`,
			want: " value [NX | XX | IFEQ ifeq-value | IFNE ifne-value | IFDEQ ifdeq-digest | IFDNE ifdne-digest] [GET] [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL]",
		},
		{
			name: "flag being typed",
			line: "SET k v N",
			want: "X [GET] [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL]",
		},
		{
			name: "flag prefix with its value",
			line: "SET k v E",
			want: "X seconds [NX | XX | IFEQ ifeq-value | IFNE ifne-value | IFDEQ ifdeq-digest | IFDNE ifdne-digest] [GET]",
		},
		{
			name: "flag prefix with its value",
			line: "SET k v IFEQ val",
			want: " [GET] [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL]",
		},
		{
			name: "flag complete",
			line: "SET k v EX",
			want: " seconds [NX | XX | IFEQ ifeq-value | IFNE ifne-value | IFDEQ ifdeq-digest | IFDNE ifdne-digest] [GET]",
		},
		{
			name: "value of a flag pending",
			line: "SET k v EX ",
			want: "seconds [NX | XX | IFEQ ifeq-value | IFNE ifne-value | IFDEQ ifdeq-digest | IFDNE ifdne-digest] [GET]",
		},
		{
			name: "value of a flag typed",
			line: "SET k v EX 10 ",
			want: "[NX | XX | IFEQ ifeq-value | IFNE ifne-value | IFDEQ ifdeq-digest | IFDNE ifdne-digest] [GET]",
		},
		{
			name: "value of a flag typed",
			line: "SET k v EX 10 GET ",
			want: "[NX | XX | IFEQ ifeq-value | IFNE ifne-value | IFDEQ ifdeq-digest | IFDNE ifdne-digest]",
		},
		{
			name: "word that fits nothing hides the hint",
			line: "SET k v ZZZ",
			want: "",
		},
		{
			name: "multiple argument",
			line: "DEL ",
			want: "key [key ...]",
		},
		{
			name: "open multiple argument",
			line: "DEL k ",
			want: "[key ...]",
		},
		{
			name: "container name being typed",
			line: "CONFIG",
			want: " GET parameter [parameter ...]",
		},
		{
			name: "multiple argument being typed",
			line: "CONFIG ",
			want: "GET parameter [parameter ...]",
		},
		{
			name: "multiple argument being typed",
			line: "CONFIG GET p",
			want: " [parameter ...]",
		},
		{
			name: "quoted value",
			line: `GET "my key"`,
			want: "",
		},
		{
			name: "ambiguous command",
			line: "HIMPORT",
			want: " SET key fieldset-name value [value ...]",
		},
		{
			name: "ambiguous command",
			line: "HIMPORT ",
			want: "SET key fieldset-name value [value ...]",
		},
		{
			name: "ambiguous command",
			line: "HSETEX k FIELDS 1 ",
			want: "field value [field value ...]",
		},
		{
			name: "container name prefix",
			line: "con",
			want: "FIG GET parameter [parameter ...]",
		},
		{
			name: "module command name being typed",
			line: "BF.A",
			want: "DD key item",
		},
		{
			name: "module command",
			line: "BF.ADD ",
			want: "key item",
		},
		{
			name: "module command",
			line: "BF.ADD k ",
			want: "item",
		},
		{
			name: "module command flag being typed",
			line: "TS.ADD k 1234 45 R",
			want: "ETENTION retentionPeriod [UNCOMPRESSED | COMPRESSED] [CHUNK_SIZE size] [BLOCK | FIRST | LAST | MIN | MAX | SUM] [label value] [label value ...]",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := s.Hint(tt.line); got != tt.want {
				t.Errorf("Hint(%q) = %q, want %q", tt.line, got, tt.want)
			}
		})
	}
}
