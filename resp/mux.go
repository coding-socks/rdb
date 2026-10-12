package resp

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// ServeMux dispatches the commands of a [Server] to the handlers
// registered for their patterns.
//
// # Patterns
//
// A pattern is one of:
//
//   - The empty string, the fallback for every command no other
//     pattern matched.
//   - The name of a command, e.g. "PING", matched case-insensitively
//     against the name of the command regardless of its arguments.
//   - The name of a command and its nested subcommands separated by
//     pipes, like the command rules of Redis ACLs, e.g. "CONFIG|GET",
//     matched case-insensitively against the name and the leading
//     arguments of the command, the remaining arguments are accepted
//     regardless of their value.
//
// A command is served by the most specific pattern that matches it:
// a pattern with more names beats the one with fewer, the fallback
// receives every command no other pattern matched.
type ServeMux struct {
	// entries are the registered patterns, an earlier registration
	// wins over an equally specific one.
	entries []muxEntry

	// fallback handles every command no other pattern matched.
	fallback Handler
}

// muxEntry is a handler for the commands whose name and leading
// arguments match the path.
type muxEntry struct {
	path    []string // the uppercased name and subcommands
	handler Handler
}

// matches reports whether the name and the leading arguments of the
// command match the path.
func (e *muxEntry) matches(cmd *Command) bool {
	if len(cmd.Args) < len(e.path) {
		return false
	}
	for i, seg := range e.path {
		if !strings.EqualFold(string(cmd.Args[i]), seg) {
			return false
		}
	}
	return true
}

// moreSpecific reports whether a is more specific than b: a pattern
// with more names wins. Equally specific patterns keep their
// registration order.
func moreSpecific(a, b *muxEntry) bool {
	return len(a.path) > len(b.path)
}

// NewServeMux constructs an empty mux.
func NewServeMux() *ServeMux {
	return &ServeMux{}
}

// Handle registers the handler for the pattern, see [ServeMux].
// It panics if the handler is nil, if the name of the pattern is
// registered twice, or if the pattern carries more than one command.
func (mux *ServeMux) Handle(pattern string, handler Handler) {
	if handler == nil {
		panic("resp: nil handler")
	}
	fields := strings.Fields(pattern)
	switch len(fields) {
	case 0:
		if mux.fallback != nil {
			panic("resp: multiple registrations for the empty pattern")
		}
		mux.fallback = handler
		return
	case 1:
	default:
		panic("resp: pattern " + strconv.Quote(pattern) + " carries more than one command")
	}

	path := strings.Split(fields[0], "|")
	for i, seg := range path {
		seg = strings.ToUpper(strings.TrimSpace(seg))
		if seg == "" {
			panic("resp: pattern " + strconv.Quote(pattern) + " has no command name")
		}
		path[i] = seg
	}
	for i := range mux.entries {
		if slices.Equal(mux.entries[i].path, path) {
			panic("resp: multiple registrations for " + fields[0])
		}
	}
	mux.entries = append(mux.entries, muxEntry{path: path, handler: handler})
}

// HandleFunc registers the handler function for the pattern, see
// [ServeMux.Handle].
func (mux *ServeMux) HandleFunc(pattern string, handler func(ResponseWriter, *Command)) {
	mux.Handle(pattern, HandlerFunc(handler))
}

// ServeRESP serves the command with the handler of the most specific
// pattern that matches it. It replies with the error of an unknown
// command when no pattern matches and no fallback is registered.
func (mux *ServeMux) ServeRESP(w ResponseWriter, cmd *Command) {
	best := -1
	for i := range mux.entries {
		if !mux.entries[i].matches(cmd) {
			continue
		}
		if best == -1 || moreSpecific(&mux.entries[i], &mux.entries[best]) {
			best = i
		}
	}
	if best != -1 {
		mux.entries[best].handler.ServeRESP(w, cmd)
		return
	}
	if mux.fallback != nil {
		mux.fallback.ServeRESP(w, cmd)
		return
	}
	w.Proto().WriteToken(Error(fmt.Sprintf("ERR unknown command '%s'", cmd.Args[0])))
}
