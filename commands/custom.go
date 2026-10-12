package commands

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// customParserFunc extracts the keys of a command whose key positions no
// key spec describes.
type customParserFunc func(cmd *Command, args [][]byte) ([][]byte, error)

// customParsers maps the names of the commands whose keys the key
// specs cannot describe to the parsers implementing the logic of their C
// getkeys procedures.
var customParsers = map[string]customParserFunc{
	"BITFIELD":          bitfieldGetKeys,
	"BLMPOP":            func(c *Command, args [][]byte) ([][]byte, error) { return genericGetKeys(0, 2, 3, 1, args) },
	"BZMPOP":            func(c *Command, args [][]byte) ([][]byte, error) { return genericGetKeys(0, 2, 3, 1, args) },
	"DELEX":             delexGetKeys,
	"EVAL":              evalGetKeys,
	"EVALSHA":           evalGetKeys,
	"EVALSHA_RO":        evalGetKeys,
	"EVAL_RO":           evalGetKeys,
	"FCALL":             functionGetKeys,
	"FCALL_RO":          functionGetKeys,
	"GEORADIUS":         georadiusGetKeys,
	"GEORADIUSBYMEMBER": georadiusGetKeys,
	"LMPOP":             func(c *Command, args [][]byte) ([][]byte, error) { return genericGetKeys(0, 1, 2, 1, args) },
	"MIGRATE":           migrateGetKeys,
	"PFMERGE":           pfmergeGetKeys,
	"SDIFFCARD":         func(c *Command, args [][]byte) ([][]byte, error) { return genericGetKeys(0, 1, 2, 1, args) },
	"SINTERCARD":        func(c *Command, args [][]byte) ([][]byte, error) { return genericGetKeys(0, 1, 2, 1, args) },
	"SORT":              sortGetKeys,
	"SORT_RO":           sortROGetKeys,
	"SUNIONCARD":        func(c *Command, args [][]byte) ([][]byte, error) { return genericGetKeys(0, 1, 2, 1, args) },
	"XREAD":             xreadGetKeys,
	"XREADGROUP":        xreadGetKeys,
	"ZDIFF":             func(c *Command, args [][]byte) ([][]byte, error) { return genericGetKeys(0, 1, 2, 1, args) },
	"ZDIFFSTORE":        func(c *Command, args [][]byte) ([][]byte, error) { return genericGetKeys(1, 2, 3, 1, args) },
	"ZINTER":            func(c *Command, args [][]byte) ([][]byte, error) { return genericGetKeys(0, 1, 2, 1, args) },
	"ZINTERCARD":        func(c *Command, args [][]byte) ([][]byte, error) { return genericGetKeys(0, 1, 2, 1, args) },
	"ZINTERSTORE":       func(c *Command, args [][]byte) ([][]byte, error) { return genericGetKeys(1, 2, 3, 1, args) },
	"ZMPOP":             func(c *Command, args [][]byte) ([][]byte, error) { return genericGetKeys(0, 1, 2, 1, args) },
	"ZUNION":            func(c *Command, args [][]byte) ([][]byte, error) { return genericGetKeys(0, 1, 2, 1, args) },
	"ZUNIONSTORE":       func(c *Command, args [][]byte) ([][]byte, error) { return genericGetKeys(1, 2, 3, 1, args) },
}

// genericGetKeys returns the store key at storeKeyOfs and the numkeys
// arguments starting at firstKeyOfs, one every keyStep arguments. A
// storeKeyOfs of 0 names no store key.
func genericGetKeys(storeKeyOfs, keyCountOfs, firstKeyOfs, keyStep int, args [][]byte) ([][]byte, error) {
	argc := len(args)
	if keyCountOfs >= argc {
		return nil, errors.New("commands: missing numkeys argument")
	}
	numkeys, err := strconv.ParseInt(string(args[keyCountOfs]), 10, 64)
	if err != nil || numkeys < 0 {
		return nil, fmt.Errorf("commands: invalid number of keys %q", string(args[keyCountOfs]))
	}
	if keyStep <= 0 {
		keyStep = 1
	}
	if numkeys > 0 && int(numkeys-1) > (argc-1-firstKeyOfs)/keyStep {
		return nil, errors.New("commands: syntax error, not enough key arguments")
	}

	keys := make([][]byte, 0, numkeys+1)
	if storeKeyOfs > 0 && storeKeyOfs < argc {
		keys = append(keys, args[storeKeyOfs])
	}
	for i := 0; i < int(numkeys); i++ {
		idx := firstKeyOfs + i*keyStep
		if idx < argc {
			keys = append(keys, args[idx])
		}
	}
	return keys, nil
}

func evalGetKeys(cmd *Command, args [][]byte) ([][]byte, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("commands: wrong number of arguments for '%s' command", cmd.Name)
	}
	numkeys, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil || numkeys < 0 {
		return nil, fmt.Errorf("commands: invalid number of keys %q", string(args[2]))
	}
	if int(numkeys) > len(args)-3 {
		return nil, errors.New("commands: syntax error, numkeys exceeds available arguments")
	}
	keys := make([][]byte, numkeys)
	for i := 0; i < int(numkeys); i++ {
		keys[i] = args[3+i]
	}
	return keys, nil
}

func functionGetKeys(cmd *Command, args [][]byte) ([][]byte, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("commands: wrong number of arguments for '%s' command", cmd.Name)
	}
	numkeys, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil || numkeys < 0 {
		return nil, fmt.Errorf("commands: invalid number of keys %q", string(args[2]))
	}
	if int(numkeys) > len(args)-3 {
		return nil, errors.New("commands: syntax error, numkeys exceeds available arguments")
	}
	keys := make([][]byte, numkeys)
	for i := 0; i < int(numkeys); i++ {
		keys[i] = args[3+i]
	}
	return keys, nil
}

func xreadGetKeys(cmd *Command, args [][]byte) ([][]byte, error) {
	streamsPos := -1
	for i := 1; i < len(args); i++ {
		arg := string(args[i])
		if strings.EqualFold(arg, "block") || strings.EqualFold(arg, "count") ||
			strings.EqualFold(arg, "maxcount") || strings.EqualFold(arg, "maxsize") ||
			strings.EqualFold(arg, "claim") {
			i++
		} else if strings.EqualFold(arg, "group") {
			i += 2
		} else if strings.EqualFold(arg, "noack") {
			// No extra argument
		} else if strings.EqualFold(arg, "streams") {
			streamsPos = i
			break
		} else {
			break
		}
	}

	if streamsPos == -1 {
		return nil, errors.New("commands: syntax error, STREAMS keyword not found")
	}

	num := len(args) - streamsPos - 1
	if num == 0 || num%2 != 0 {
		return nil, errors.New("commands: syntax error, uneven number of streams and IDs")
	}
	numKeys := num / 2
	keys := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = args[streamsPos+1+i]
	}
	return keys, nil
}

func migrateGetKeys(cmd *Command, args [][]byte) ([][]byte, error) {
	if len(args) < 6 {
		return nil, fmt.Errorf("commands: wrong number of arguments for '%s' command", cmd.Name)
	}

	first := 3
	num := 1

	if len(args) > 6 {
		for i := 6; i < len(args); i++ {
			arg := string(args[i])
			if strings.EqualFold(arg, "keys") {
				if len(args[3]) > 0 {
					return nil, errors.New("commands: syntax error, MIGRATE with KEYS option requires empty key argument")
				}
				first = i + 1
				num = len(args) - first
				break
			} else if strings.EqualFold(arg, "copy") || strings.EqualFold(arg, "replace") {
				// No extra argument
			} else if strings.EqualFold(arg, "auth") {
				i++
			} else if strings.EqualFold(arg, "auth2") {
				i += 2
			}
		}
	}

	if num <= 0 || first+num > len(args) {
		return nil, errors.New("commands: syntax error in MIGRATE")
	}

	keys := make([][]byte, num)
	for i := 0; i < num; i++ {
		keys[i] = args[first+i]
	}
	return keys, nil
}

func sortGetKeys(cmd *Command, args [][]byte) ([][]byte, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("commands: wrong number of arguments for '%s' command", cmd.Name)
	}

	keys := [][]byte{args[1]}
	var storeKey []byte

	for i := 2; i < len(args); i++ {
		arg := string(args[i])
		if strings.EqualFold(arg, "limit") {
			i += 2
		} else if strings.EqualFold(arg, "get") || strings.EqualFold(arg, "by") {
			i++
		} else if strings.EqualFold(arg, "store") && i+1 < len(args) {
			storeKey = args[i+1]
			i++
		}
	}

	if storeKey != nil {
		keys = append(keys, storeKey)
	}
	return keys, nil
}

func sortROGetKeys(cmd *Command, args [][]byte) ([][]byte, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("commands: wrong number of arguments for '%s' command", cmd.Name)
	}
	return [][]byte{args[1]}, nil
}

func bitfieldGetKeys(cmd *Command, args [][]byte) ([][]byte, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("commands: wrong number of arguments for '%s' command", cmd.Name)
	}
	return [][]byte{args[1]}, nil
}

func georadiusGetKeys(cmd *Command, args [][]byte) ([][]byte, error) {
	if len(args) < 5 {
		return nil, fmt.Errorf("commands: wrong number of arguments for '%s' command", cmd.Name)
	}
	keys := [][]byte{args[1]}
	var storeKey []byte
	for i := 5; i < len(args); i++ {
		arg := string(args[i])
		if (strings.EqualFold(arg, "store") || strings.EqualFold(arg, "storedist")) && i+1 < len(args) {
			storeKey = args[i+1]
			i++
		}
	}
	if storeKey != nil {
		keys = append(keys, storeKey)
	}
	return keys, nil
}

func pfmergeGetKeys(cmd *Command, args [][]byte) ([][]byte, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("commands: wrong number of arguments for '%s' command", cmd.Name)
	}
	keys := make([][]byte, len(args)-1)
	for i := 1; i < len(args); i++ {
		keys[i-1] = args[i]
	}
	return keys, nil
}

func delexGetKeys(cmd *Command, args [][]byte) ([][]byte, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("commands: wrong number of arguments for '%s' command", cmd.Name)
	}
	return [][]byte{args[1]}, nil
}
