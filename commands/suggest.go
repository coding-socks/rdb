package commands

import (
	"slices"
	"strconv"
	"strings"
)

// Suggester computes the completion hint of an input line. The
// command names live in a trie of their words, the syntax of every
// command with argument specs is compiled into a graph of states the
// complete words of a line walk.
type Suggester struct {
	names  *nameNode
	graphs map[string]*argGraph
}

// NewSuggester returns a suggester for the commands of one dialect,
// the argument specs included where the dialect carries them.
func NewSuggester(cmds Table) *Suggester {
	s := &Suggester{
		names:  &nameNode{children: map[string]*nameNode{}},
		graphs: map[string]*argGraph{},
	}
	var insert func(c *Command, name string)
	insert = func(c *Command, name string) {
		if len(c.Subcommands) == 0 {
			s.insertName(name)
		}
		if len(c.Args) > 0 {
			s.graphs[name] = &argGraph{start: compileSpecs(c.Args, 0, &argState{}, false)}
		}
		for _, sub := range c.Subcommands {
			insert(sub, name+" "+sub.Name)
		}
	}
	for _, c := range cmds {
		insert(c, c.Name)
	}
	return s
}

// nameNode is one word of the command names, the words of a name
// chain from the root, the full name sits on the node of its last
// word.
type nameNode struct {
	children map[string]*nameNode
	name     string
}

func (s *Suggester) insertName(name string) {
	node := s.names
	for w := range strings.FieldsSeq(strings.ToLower(name)) {
		child := node.children[w]
		if child == nil {
			child = &nameNode{children: map[string]*nameNode{}}
			node.children[w] = child
		}
		node = child
	}
	node.name = name
}

// Hint returns the text the line continues with, a leading space
// included where the next input starts a new word. An empty string
// hides the hint.
func (s *Suggester) Hint(line string) string {
	typed, partial := splitWords(line)
	if len(typed) == 0 && partial == "" {
		return ""
	}
	node, consumed, named := walkNames(s.names, typed)
	if hint, ok := s.nameCompletion(node, consumed, partial); ok {
		return hint
	}
	return argsHint(s.graphs[named], typed[consumed:], partial)
}

// walkNames descends the trie along the complete words and returns
// the node they reach, how many words it consumed, and the full name
// of the deepest command on the path.
func walkNames(root *nameNode, typed []string) (*nameNode, int, string) {
	node, named, consumed := root, "", 0
	for _, w := range typed {
		child := node.children[w]
		if child == nil {
			break
		}
		node = child
		consumed++
		if child.name != "" {
			named = child.name
		}
	}
	return node, consumed, named
}

// nameCompletion completes the command name while it is typed, the
// word being typed extends the name of the shortest command the
// complete words already carry. ok reports that the line is still
// typing a command name.
func (s *Suggester) nameCompletion(node *nameNode, consumed int, partial string) (string, bool) {
	var names []string
	var visit func(n *nameNode)
	visit = func(n *nameNode) {
		if n.name != "" {
			names = append(names, n.name)
		}
		for _, child := range n.children {
			visit(child)
		}
	}
	for word, child := range node.children {
		if !strings.HasPrefix(word, partial) {
			continue
		}
		if word == partial && child.name != "" {
			// the word completes the name of a command, the hint
			// continues with its arguments behind a new word
			if r := argsHint(s.graphs[child.name], nil, ""); r != "" {
				return " " + r, true
			}
			return "", false
		}
		visit(child)
	}
	slices.SortFunc(names, shorterName)
	for _, name := range names {
		if rem := s.nameRemainder(name, consumed, partial); rem != "" {
			return rem, true
		}
	}
	if len(names) > 0 {
		return "", true
	}
	return "", false
}

// nameRemainder renders the completion of the command name, the
// remaining name words and the syntax of its arguments included.
func (s *Suggester) nameRemainder(name string, consumed int, partial string) string {
	words := strings.Fields(name)
	rem := words[consumed][len(partial):]
	for _, w := range words[consumed+1:] {
		rem += " " + w
	}
	if g := s.graphs[name]; g != nil {
		if r := renderState(g.start, nil, map[int]bool{}); r != "" {
			rem += " " + r
		}
	}
	return rem
}

// shorterName orders two command names by length, the
// lexicographic order of two equally long ones.
func shorterName(a, b string) int {
	if len(a) != len(b) {
		return len(a) - len(b)
	}
	return strings.Compare(a, b)
}

// splitWords splits the line into the complete words and the word
// being typed, all of them lower case. The word being typed is the
// text behind the last space, an empty string when the line ends
// with one.
func splitWords(line string) (typed []string, partial string) {
	words := SplitCmd([]byte(line))
	if len(words) > 0 && !endsWithSpace(line) {
		partial = strings.ToLower(string(words[len(words)-1]))
		words = words[:len(words)-1]
	}
	typed = make([]string, len(words))
	for i, w := range words {
		typed[i] = strings.ToLower(string(w))
	}
	return typed, partial
}

func endsWithSpace(line string) bool {
	if line == "" {
		return false
	}
	switch line[len(line)-1] {
	case ' ', '\t':
		return true
	}
	return false
}

// argGraph is the compiled syntax of one command, the complete words
// of a line walk it from the start state.
type argGraph struct {
	start *argState
}

// argState is a position in the syntax of a command, its edges are
// the words the line may carry next.
type argState struct {
	edges []argEdge
}

// argEdge is one word or value the syntax accepts at a state. A
// group edge enters the alternatives of a oneof or the items of a
// block, its next state is where their words enter.
type argEdge struct {
	word   string   // the literal word, empty for a value or a group
	kind   byte     // the value kind of a value edge
	group  bool     // the edge enters a oneof or a block
	reopen bool     // the edge belongs to a multiple argument
	slot   int      // the top level argument the edge belongs to
	arg    *ArgSpec // the argument the edge renders as
	unit   *ArgSpec // the argument the words of the edge consume
	next   *argState
}

// compileSpecs wires the specs as edges leading to next, the words
// enter the graph at the returned state. The members of a run of
// optional arguments share the state they start from, any of them
// may come in any order, a multiple loops back into it.
func compileSpecs(specs []ArgSpec, slot int, next *argState, reopen bool) *argState {
	start := next
	i := len(specs) - 1
	for i >= 0 {
		switch {
		case specs[i].Optional:
			j := i
			for j >= 0 && specs[j].Optional {
				j--
			}
			start = compileRun(specs[j+1:i+1], slot+j+1, start, reopen)
			i = j
		case specs[i].Multiple:
			st := &argState{}
			entry := compileArg(&specs[i], slot+i, st, true)
			st.edges = append(slices.Clone(entry.edges), start.edges...)
			start = entry
			i--
		default:
			start = compileArg(&specs[i], slot+i, start, reopen)
			i--
		}
	}
	return start
}

// compileRun wires a run of consecutive optional arguments onto one
// state, their words may come in any order and repeat.
func compileRun(run []ArgSpec, slot int, next *argState, reopen bool) *argState {
	st := &argState{}
	for k := range run {
		st.edges = append(st.edges, compileArg(&run[k], slot+k, st, reopen).edges...)
	}
	st.edges = append(st.edges, next.edges...)
	return st
}

// compileArg wires one argument as the edges leading to next, the
// words enter at the returned state.
func compileArg(a *ArgSpec, slot int, next *argState, reopen bool) *argState {
	var start *argState
	switch a.Type {
	case "oneof":
		inner := &argState{}
		for i := range a.Args {
			inner.edges = append(inner.edges, compileArg(&a.Args[i], slot, next, reopen).edges...)
		}
		start = &argState{edges: []argEdge{{group: true, reopen: reopen, slot: slot, unit: a, arg: a, next: inner}}}
	case "block":
		start = &argState{edges: []argEdge{{group: true, reopen: reopen, slot: slot, unit: a, arg: a, next: compileSpecs(a.Args, slot, next, reopen)}}}
	default:
		start = next
		if a.Type != "pure-token" {
			start = &argState{edges: []argEdge{{reopen: reopen, kind: valueKind(a.Type), slot: slot, unit: a, arg: a, next: next}}}
		}
	}
	if a.Token != "" {
		return &argState{edges: []argEdge{{reopen: reopen, word: strings.ToLower(a.Token), slot: slot, unit: a, arg: a, next: start}}}
	}
	return start
}

func valueKind(t string) byte {
	switch t {
	case "integer", "unix-time":
		return 'i'
	case "double":
		return 'd'
	}
	return 'a'
}

// argsHint walks the complete words through the graph of a command
// and renders what the line continues with. A word being typed
// completes the token of the next argument, a word that fits
// nothing hides the hint.
func argsHint(g *argGraph, words []string, partial string) string {
	if g == nil {
		return ""
	}
	cur := g.start
	seenSlot := map[int]bool{}
	var last *argEdge
	for _, w := range words {
		e := step(cur, w)
		if e == nil {
			return ""
		}
		if isValue(e) || e.word != "" && valuePart(e.arg) == "" {
			seenSlot[e.slot] = true
		}
		last, cur = e, e.next
	}
	if partial == "" {
		if last != nil && last.word != "" && valuePart(last.arg) != "" {
			seenSlot[last.slot] = true
		}
		return renderState(cur, last, seenSlot)
	}
	var (
		prefix string
		st     *argState
	)
	if e := literalEdge(cur, partial, seenSlot); e != nil {
		prefix = e.arg.Token[len(partial):]
		seenSlot[e.slot] = true
		if vp := valuePart(e.arg); vp != "" {
			prefix += " " + vp
			v := e.next.edges[0]
			st, last, seenSlot = v.next, &v, seenSlot
		} else {
			st, last, seenSlot = e.next, e, seenSlot
		}
	} else {
		if e := valueEdge(cur, seenSlot); e != nil {
			seenSlot[e.slot] = true
			st, last, seenSlot = e.next, e, seenSlot
		}
	}
	if st == nil {
		return prefix
	}
	r := renderState(st, last, seenSlot)
	if r == "" {
		return prefix
	}
	return prefix + " " + r
}

// step returns the edge of the state the word matches, the literal
// words of a state before its values.
func step(st *argState, word string) *argEdge {
	var value *argEdge
	for i := range st.edges {
		e := &st.edges[i]
		switch {
		case e.word != "":
			if e.word == word {
				return e
			}
		case e.group:
			if hit := step(e.next, word); hit != nil {
				return hit
			}
		case value == nil && valueOK(e, word):
			value = e
		}
	}
	return value
}

func valueOK(e *argEdge, word string) bool {
	switch e.kind {
	case 'i', 'u':
		_, err := strconv.ParseInt(word, 10, 64)
		return err == nil
	case 'd':
		_, err := strconv.ParseFloat(word, 64)
		return err == nil
	}
	return true
}

// literalEdge returns the edge of the state whose token the word
// being typed prefixes, the arguments the line consumed excluded.
func literalEdge(st *argState, partial string, taken map[int]bool) *argEdge {
	for i := range st.edges {
		e := &st.edges[i]
		if taken[e.slot] && !e.reopen {
			continue
		}
		if e.word != "" && strings.HasPrefix(e.word, partial) {
			return e
		}
		if e.group {
			if hit := literalEdge(e.next, partial, taken); hit != nil {
				return hit
			}
		}
	}
	return nil
}

// valueEdge returns the edge of the state whose value the word
// being typed fills, the arguments the line consumed excluded.
func valueEdge(st *argState, taken map[int]bool) *argEdge {
	for i := range st.edges {
		e := &st.edges[i]
		if taken[e.slot] && !e.reopen {
			continue
		}
		if isValue(e) {
			return e
		}
		if e.group {
			if hit := valueEdge(e.next, taken); hit != nil {
				return hit
			}
		}
	}
	return nil
}

func isValue(e *argEdge) bool {
	return e.word == "" && !e.group
}

// renderState renders the syntax the state still accepts. The
// rendering includes the value a required argument waits for and
// offers the repetition of an argument the line is in the middle of.
func renderState(st *argState, last *argEdge, taken map[int]bool) string {
	// the value of the token the line ends with is still missing
	if last != nil && last.word != "" && len(st.edges) == 1 {
		if e := &st.edges[0]; isValue(e) && e.unit == last.unit {
			r := valuePart(e.arg)
			if after := renderState(e.next, e, taken); after != "" {
				r += " " + after
			}
			return r
		}
	}
	rendered := map[int]bool{}
	var parts []string
	for {
		// a required value continues the syntax with the state after it
		if len(st.edges) == 1 && isValue(&st.edges[0]) && (last == nil || st.edges[0].unit != last.unit) {
			e := &st.edges[0]
			parts = append(parts, renderSpec(e.arg))
			rendered[e.slot] = true
			last, st = e, e.next
			continue
		}
		for i := range st.edges {
			e := &st.edges[i]
			switch {
			case last != nil && last.word == "" && isValue(e) && e.unit == last.unit && !rendered[e.slot]:
				// another repetition of the argument fits
				parts = append(parts, repeatPart(e.arg))
			case last != nil && last.word == "" && e.group && e.reopen && taken[e.slot]:
				// the line is inside the arguments of the multiple
				// group, its repetition is what fits next
				parts = append(parts, repeatPart(e.arg))
			case taken[e.slot] || rendered[e.slot]:
				// the line consumed this argument
			default:
				parts = append(parts, renderSpec(e.arg))
			}
		}
		return strings.Join(parts, " ")
	}
}

// renderSpec renders the syntax of one argument, the brackets of the
// optional arguments and the repetitions of a multiple included.
func renderSpec(a *ArgSpec) string {
	inner := unit(a)
	if a.Type == "oneof" || a.Type == "block" {
		inner = valuePart(a)
	}
	if a.Optional {
		inner = "[" + inner + "]"
	}
	if a.Multiple {
		inner += " " + repeatPart(a)
	}
	return inner
}

// unit renders one argument or alternative, the token with the name
// of its value.
func unit(a *ArgSpec) string {
	if a.Type == "oneof" || a.Type == "block" {
		return valuePart(a)
	}
	s := a.Token
	if n := displayName(a); n != "" {
		if s != "" {
			s += " "
		}
		s += n
	}
	return s
}

// valuePart renders the value of an argument without its token, the
// alternatives of a oneof and the items of a block included.
func valuePart(a *ArgSpec) string {
	if a.Type != "oneof" && a.Type != "block" {
		return displayName(a)
	}
	sep := " "
	if a.Type == "oneof" {
		sep = " | "
	}
	parts := make([]string, 0, len(a.Args))
	for i := range a.Args {
		parts = append(parts, unit(&a.Args[i]))
	}
	return strings.Join(parts, sep)
}

// repeatPart renders the repetition of a multiple argument.
func repeatPart(a *ArgSpec) string {
	var inner string
	if a.MultipleToken && a.Token != "" {
		inner = a.Token + " "
	}
	switch a.Type {
	case "oneof", "block":
		inner += valuePart(a)
	default:
		inner += displayName(a)
	}
	if inner == "" {
		return ""
	}
	return "[" + inner + " ...]"
}

func displayName(a *ArgSpec) string {
	if a.Type == "pure-token" {
		return ""
	}
	if a.Text != "" {
		return a.Text
	}
	return a.Name
}
