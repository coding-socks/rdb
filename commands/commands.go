//go:generate go run -tags codegen make_commands.go -- commands_redis.go RedisCommands ../lib/redis/src/commands/*.json ../lib/RediSearch/commands.json ../lib/RedisJSON/commands.json ../lib/RedisTimeSeries/commands.json ../lib/RedisBloom/commands.json ../lib/redis/modules/vector-sets/commands.json
//go:generate go run -tags codegen make_commands.go -- commands_valkey.go ValkeyCommands ../lib/valkey/src/commands/*.json
//go:generate go run -tags codegen make_commands.go -- commands_keydb.go KeyDBCommands keydb/*.json

package commands

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// SSplitCmd splits the string p around each instance of one or more
// consecutive white space characters, returning a slice of arguments of
// p or an empty slice if p contains only white space. Double and single
// quoted arguments keep their white space, a backslash escapes the next
// byte.
func SSplitCmd(p string) [][]byte {
	return SplitCmd([]byte(p))
}

// SplitCmd splits the command line p around each instance of one or more
// consecutive white space characters, returning a slice of arguments of
// p or an empty slice if p contains only white space. Double and single
// quoted arguments keep their white space, a backslash escapes the next
// byte.
func SplitCmd(p []byte) [][]byte {
	var (
		cmd    [][]byte
		buf    []byte
		escape bool
		quoted bool
		quote  byte
	)
	for i := range p {
		b := p[i]
		switch {
		case escape:
			buf = append(buf, b)
			escape = false
		case b == '\\' && (quote == 0 || quote == '"'):
			escape = true
		case quote == 0 && (b == '"' || b == '\''):
			quote, quoted = b, true
		case quote != 0 && b == quote:
			quote = 0
		case quote == 0 && (b == ' ' || b == '\t' || b == '\n' || b == '\r'):
			if len(buf) > 0 || quoted {
				cmd = append(cmd, bytes.Clone(buf))
				buf = buf[:0]
				quoted = false
			}
		default:
			buf = append(buf, b)
		}
	}
	if len(buf) > 0 || quoted {
		cmd = append(cmd, bytes.Clone(buf))
	}
	return cmd
}

// Table is a map of the command definitions of one dialect, keyed by the
// command name. The keys must be upper case.
type Table map[string]*Command

// Lookup resolves a command or container subcommand by name (case-insensitive).
func (t Table) Lookup(args [][]byte) (*Command, error) {
	if len(args) == 0 {
		return nil, errors.New("commands: empty command")
	}

	name := strings.ToUpper(string(args[0]))

	cmd, ok := t[name]
	if !ok {
		return nil, fmt.Errorf("commands: unknown command '%s'", string(args[0]))
	}

	if len(args) > 1 && len(cmd.Subcommands) > 0 {
		for _, sub := range cmd.Subcommands {
			if strings.EqualFold(sub.Name, string(args[1])) {
				return sub, nil
			}
		}
		return nil, fmt.Errorf("commands: unknown command '%s %s'", cmd.Name, string(args[1]))
	}

	return cmd, nil
}

// ArgSpec describes an argument of a command.
type ArgSpec struct {
	Name          string
	Type          string // key, string, integer, double, unix-time, pure-token, oneof, block
	Token         string
	Text          string
	Optional      bool
	Multiple      bool
	MultipleToken bool
	Args          []ArgSpec
}

// BeginSearch finds the start position of the key specification.
// ok reports that the search found it.
type BeginSearch interface {
	Begin(args [][]byte) (first int, ok bool)
}

// BeginSearchIndex defines a fix position of the key specification.
type BeginSearchIndex struct {
	Pos int
}

func (s BeginSearchIndex) Begin(args [][]byte) (int, bool) {
	return s.Pos, true
}

// BeginSearchKeyword defines a key specification starting after
// a keyword argument. The search starts at the Start position.
// A negative Start counts from the end.
type BeginSearchKeyword struct {
	Keyword string
	Start   int
}

func (s BeginSearchKeyword) Begin(args [][]byte) (int, bool) {
	start := s.Start
	if start < 0 {
		start += len(args)
	}
	end := 1
	if s.Start > 0 {
		end = len(args) - 1
	}
	step := 1
	if start > end {
		step = -1
	}
	for i := start; ; i += step {
		if i >= len(args) || i < 1 {
			break
		}
		if strings.EqualFold(string(args[i]), s.Keyword) {
			return i + 1, true
		}
		if i == end {
			break
		}
	}
	return 0, false
}

// FindKeys finds the key arguments of a key specification,
// starting from the provided first argument.
// ok reports that the specification applies to the arguments.
type FindKeys interface {
	Find(args [][]byte, first int) (last, step int, ok bool, err error)
}

// FindKeysRange spans the keys over a range of arguments. A negative
// Last counts from the end of the arguments, Limit bounds the range.
type FindKeysRange struct {
	Last  int
	Step  int
	Limit int
}

func (f FindKeysRange) Find(args [][]byte, first int) (int, int, bool, error) {
	step := f.Step
	if step <= 0 {
		step = 1
	}
	var last int
	if f.Last >= 0 {
		last = first + f.Last
	} else if f.Limit == 0 {
		last = len(args) + f.Last
	} else {
		last = first + ((len(args)-first)/f.Limit + f.Last)
	}
	return last, step, true, nil
}

// FindKeysKeynum spans the keys a numkeys argument counts. KeynumIdx
// locates the numkeys argument relative to the first key, First
// shifts the first key and Step is the distance between them.
type FindKeysKeynum struct {
	KeynumIdx int
	First     int
	Step      int
}

func (f FindKeysKeynum) Find(args [][]byte, first int) (int, int, bool, error) {
	step := f.Step
	if step <= 0 {
		step = 1
	}
	keynumIdx := first + f.KeynumIdx
	if keynumIdx >= len(args) || keynumIdx < 0 {
		return 0, 0, false, errors.New("commands: invalid keynum index")
	}
	numkeys, err := strconv.ParseInt(string(args[keynumIdx]), 10, 64)
	if err != nil || numkeys < 0 {
		return 0, 0, false, fmt.Errorf("commands: invalid number of keys %q", string(args[keynumIdx]))
	}
	first += f.First
	if first < 0 || numkeys-1 > int64((len(args)-1-first)/step) {
		if numkeys == 0 {
			return 0, 0, false, nil
		}
		return 0, 0, false, errors.New("commands: keynum out of range")
	}
	if numkeys == 0 {
		return 0, 0, false, nil
	}
	return first + int(numkeys-1)*step, step, true, nil
}

// KeySpec locates the keys of a command in its arguments.
type KeySpec struct {
	Flags       []string
	BeginSearch BeginSearch
	FindKeys    FindKeys
}

// Command represents a command definition and its key extraction rules.
type Command struct {
	Name        string
	Arity       int
	Flags       []string
	Subcommands []*Command
	KeySpecs    []KeySpec
	Args        []ArgSpec
}

// ValidateArity checks if the number of arguments satisfies the command's arity.
func (c *Command) ValidateArity(argc int) error {
	if c.Arity > 0 && argc != c.Arity {
		return fmt.Errorf("commands: wrong number of arguments for '%s' command", c.Name)
	}
	if c.Arity < 0 && argc < -c.Arity {
		return fmt.Errorf("commands: wrong number of arguments for '%s' command", c.Name)
	}
	return nil
}

// ExtractKeys extracts all key arguments from the command line arguments.
func (c *Command) ExtractKeys(args [][]byte) ([][]byte, error) {
	if err := c.ValidateArity(len(args)); err != nil {
		return nil, err
	}

	if parse, ok := customParsers[c.Name]; ok {
		return parse(c, args)
	}

	if len(c.KeySpecs) > 0 {
		return c.extractKeysUsingKeySpecs(args)
	}
	return c.extractKeysUsingArgs(args)
}

func (c *Command) extractKeysUsingKeySpecs(args [][]byte) ([][]byte, error) {
	var keys [][]byte

	for _, spec := range c.KeySpecs {
		// Skip non-key specifications
		if hasFlag(spec.Flags, "NOT_KEY") {
			continue
		}

		first, ok := spec.BeginSearch.Begin(args)
		if !ok {
			continue
		}

		last, step, ok, err := spec.FindKeys.Find(args, first)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}

		if last >= len(args) || last < first || first >= len(args) {
			return nil, errors.New("commands: syntax error")
		}

		for i := first; i <= last; i += step {
			if i < 0 || i >= len(args) {
				return nil, errors.New("commands: argument index out of bounds")
			}
			keys = append(keys, args[i])
		}
	}

	return keys, nil
}

func (c *Command) extractKeysUsingArgs(args [][]byte) ([][]byte, error) {
	var keys [][]byte
	for i, a := range c.Args {
		if a.Type != "key" || a.Optional {
			continue
		}
		if a.MultipleToken {
			return nil, nil
		}
		pos := i + 1
		switch {
		case a.Multiple && i > 0 && c.Args[i-1].Type == "integer":
			if pos >= len(args) {
				return nil, errors.New("commands: invalid keynum index")
			}
			numkeys, err := strconv.ParseInt(string(args[i]), 10, 64)
			if err != nil || numkeys < 0 {
				return nil, fmt.Errorf("commands: invalid number of keys %q", string(args[i]))
			}
			if numkeys == 0 {
				continue
			}
			if pos+int(numkeys) > len(args) {
				return nil, errors.New("commands: keynum out of range")
			}
			for j := 0; j < int(numkeys); j++ {
				keys = append(keys, args[pos+j])
			}
		case a.Multiple:
			clean := true
			for _, t := range c.Args[i+1:] {
				if t.Optional {
					clean = false
					break
				}
			}
			if !clean {
				continue
			}
			last := len(args) - (len(c.Args) - i - 1) - 1
			if pos > last {
				return nil, errors.New("commands: syntax error")
			}
			keys = append(keys, args[pos:last+1]...)
		default:
			if pos >= len(args) {
				return nil, errors.New("commands: syntax error")
			}
			keys = append(keys, args[pos])
		}
	}
	return keys, nil
}

func hasFlag(flags []string, flag string) bool {
	for _, f := range flags {
		if strings.EqualFold(f, flag) {
			return true
		}
	}
	return false
}
