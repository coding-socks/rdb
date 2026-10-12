package commands

import (
	"context"
	"fmt"
	"strings"

	"charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"
	"github.com/coding-socks/rdb/commands"
)

var hintStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("8"))

// commander produces the commands of the session: the write of a
// submitted line, the read of a message of the connection and the
// note of a write that outlives its timeout.
type commander interface {
	writeCmd(sentID int, args [][]byte) tea.Cmd
}

// inputModel edits the command line of the session, its view is the
// prompt with the input. Enter runs the command, the line as typed
// stays on the screen. The hint of the suggester trails the input
// while the cursor sits at its end. Ctrl-C and Ctrl-D end the session
// whatever the input holds, and the up and down keys browse the
// history of the submitted lines. On an input that is not a terminal
// the lines are submitted as they arrive.
type inputModel struct {
	sess  commander
	ctx   context.Context
	input lineEdit
	hints *commands.Suggester
	err   error

	msgID    int  // incremented for every message the connection delivered
	quit     bool // a QUIT command is written
	frefresh bool

	history []string
	hist    int // the browsed entry of the history, -1 when none
	draft   string
}

func newInputModel(sess commander, ctx context.Context) inputModel {
	return inputModel{
		sess:  sess,
		ctx:   ctx,
		input: newLineEdit("> ", 80),
		hist:  -1,
	}
}

func (m inputModel) WithPrompt(prompt string) inputModel {
	m.input.Prompt = prompt
	return m
}

func (m inputModel) WithSuggester(s *commands.Suggester) inputModel {
	m.hints = s
	return m
}

func (m inputModel) Init() tea.Cmd {
	return nil
}

func (m inputModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.input.width = max(1, msg.Width)
		return m, nil
	case errorMsg:
		return m.printError(msg)
	case ackReplyMsg:
		return m.ackReply()
	case writtenMsg:
		if msg.err != nil {
			m.err = fmt.Errorf("rdbc client: %w", msg.err)
			return m, tea.Quit
		}
		return m, nil
	case timeoutMsg:
		if msg.latestID == m.msgID {
			// No message arrived since the command was written, the
			// session keeps waiting for its reply.
			return m, tea.Sequence(
				tea.Println("(reply is still pending, waiting)"),
				tea.Quit,
			)
		}
		return m, nil
	case tea.KeyPressMsg:
		switch msg.String() {
		case "ctrl+c", "ctrl+d":
			return m, tea.Sequence(
				tea.Println(m.input.Prompt+m.input.Value()),
				tea.Quit,
			)
		case "enter":
			return m.submit(m.input.Value())
		case "up":
			return m.historyPrev()
		case "down":
			return m.historyNext()
		}
	}
	m.input = m.input.Update(msg)
	return m, nil
}

// printError ends the session with the error the connection read.
func (m inputModel) printError(msg errorMsg) (tea.Model, tea.Cmd) {
	if m.ctx.Err() != nil {
		return m, nil
	}
	m.msgID++
	// A broken connection ends the session.
	m.err = fmt.Errorf("rdbc client: %w", msg)
	return m, tea.Quit
}

// ackReply acknowledges the message the connection read by cancelling
// the read timeout.
func (m inputModel) ackReply() (tea.Model, tea.Cmd) {
	if m.ctx.Err() != nil {
		return m, nil
	}
	m.msgID++
	var cmds []tea.Cmd
	if m.quit {
		cmds = append(cmds, tea.Quit)
	}
	m.frefresh = !m.frefresh
	return m, tea.Sequence(cmds...)
}

// View renders the prompt with the input, the text wraps over the rows
// of the terminal and the cursor is on the row and column of the input
// position. A blank cell trails the line when the view is out of sync
// with the terminal.
func (m inputModel) View() tea.View {
	s := m.inputView()
	if m.frefresh {
		s += "\u200B"
	}
	v := tea.NewView(s)
	if c := m.input.Cursor(); c != nil {
		v.Cursor = c
	}
	return v
}

// inputView renders the input, the hint of the suggester trails it in
// grey while the cursor sits at the end of the line.
func (m inputModel) inputView() string {
	s := m.input.View()
	if m.hints != nil && m.input.AtEnd() {
		s += hintStyle.Render(m.hints.Hint(m.input.Value()))
	}
	return s
}

// submit runs the line as a command of the session, a line typed on a
// terminal is echoed above the input and lands in the history. The
// lines submitted while a command runs are queued, and the input of a
// session without a terminal is read again after every line.
func (m inputModel) submit(line string) (tea.Model, tea.Cmd) {
	echoCmd := tea.Println(m.input.Prompt + line)
	m.input.Reset()
	if line != "" && (len(m.history) == 0 || m.history[len(m.history)-1] != line) {
		m.history = append(m.history, line)
	}
	m.hist, m.draft = -1, ""
	var cmds []tea.Cmd
	if args := commands.SSplitCmd(line); len(args) > 0 {
		var exec tea.Cmd
		m, exec = m.run(args)
		if echoCmd != nil {
			// The line as typed is on the screen before the
			// reply is printed.
			exec = tea.Sequence(echoCmd, exec)
		}
		cmds = append(cmds, exec)
	}
	return m, tea.Batch(cmds...)
}

// run writes the command of the line to the connection, it is batched
// with the note that is printed when no message of the connection
// arrives before the timeout of the session.
func (m inputModel) run(args [][]byte) (inputModel, tea.Cmd) {
	if strings.EqualFold(string(args[0]), "quit") {
		m.quit = true
	}
	return m, tea.Batch(m.sess.writeCmd(m.msgID, args))
}

func (m inputModel) historyPrev() (tea.Model, tea.Cmd) {
	if len(m.history) == 0 {
		return m, nil
	}
	if m.hist == -1 {
		m.hist = len(m.history)
		m.draft = m.input.Value()
	}
	if m.hist > 0 {
		m.hist--
	}
	m.input.SetValue(m.history[m.hist])
	m.input.CursorEnd()
	return m, nil
}

func (m inputModel) historyNext() (tea.Model, tea.Cmd) {
	if m.hist == -1 {
		return m, nil
	}
	m.hist++
	if m.hist >= len(m.history) {
		m.hist = -1
		m.input.SetValue(m.draft)
		m.draft = ""
	} else {
		m.input.SetValue(m.history[m.hist])
	}
	m.input.CursorEnd()
	return m, nil
}

// lineEdit edits one line of input, it borrows the editing keys of the
// textinput of the bubbles library and wraps the line over the rows of
// the terminal like the textarea does. The cursor of the view is on
// the row and the column of the input position.
type lineEdit struct {
	Prompt string
	value  []rune
	pos    int  // the cursor, in runes from the start of the input
	focus  bool // the cursor of an unfocused line is hidden
	width  int  // the columns of the terminal
}

func newLineEdit(prompt string, width int) lineEdit {
	return lineEdit{Prompt: prompt, focus: true, width: max(1, width)}
}

func (e *lineEdit) Focus() {
	e.focus = true
}

// Value returns the line as typed.
func (e lineEdit) Value() string {
	return string(e.value)
}

// SetValue replaces the line, the cursor moves to its end.
func (e *lineEdit) SetValue(s string) {
	e.value = []rune(s)
	e.pos = len(e.value)
}

// Reset empties the line.
func (e *lineEdit) Reset() {
	e.value = e.value[:0]
	e.pos = 0
}

// CursorEnd moves the cursor behind the last rune of the line.
func (e *lineEdit) CursorEnd() {
	e.pos = len(e.value)
}

// AtEnd reports that the cursor sits behind the last rune of the line.
func (e lineEdit) AtEnd() bool {
	return e.pos == len(e.value)
}

// Cursor returns the position of the cursor on the terminal, a nil
// cursor hides it.
func (e lineEdit) Cursor() *tea.Cursor {
	if !e.focus {
		return nil
	}
	lines := strings.Split(e.wrap(e.Prompt+string(e.value[:e.pos])), "\n")
	return tea.NewCursor(lipgloss.Width(lines[len(lines)-1]), len(lines)-1)
}

// Update applies the keys of the line, an insert of runes and the
// motion and kill keys of a line editor.
func (e lineEdit) Update(msg tea.Msg) lineEdit {
	switch msg := msg.(type) {
	default:
		return e
	case tea.PasteMsg:
		e.insert(msg.String())
		return e
	case tea.KeyPressMsg:
		switch msg.String() {
		case "left", "ctrl+b":
			if e.pos > 0 {
				e.pos--
			}
		case "right", "ctrl+f":
			if e.pos < len(e.value) {
				e.pos++
			}
		case "home", "ctrl+a":
			e.pos = 0
		case "end", "ctrl+e":
			e.CursorEnd()
		case "backspace", "ctrl+h":
			if e.pos > 0 {
				e.pos--
				e.value = append(e.value[:e.pos], e.value[e.pos+1:]...)
			}
		case "delete", "ctrl+d":
			if e.pos < len(e.value) {
				e.value = append(e.value[:e.pos], e.value[e.pos+1:]...)
			}
		case "ctrl+k": // the line from the cursor to its end
			e.value = e.value[:e.pos]
		case "ctrl+u": // the line from its start to the cursor
			e.value = append(e.value[:0], e.value[e.pos:]...)
			e.pos = 0
		default:
			if msg.Text != "" {
				e.insert(msg.Text)
			}
		}
		return e
	}
}

// insert adds the runes of s at the cursor, the line breaks in it are
// skipped.
func (e *lineEdit) insert(s string) {
	for _, r := range s {
		if r == '\r' || r == '\n' {
			continue
		}
		e.value = append(e.value, r)
		copy(e.value[e.pos+1:], e.value[e.pos:])
		e.value[e.pos] = r
		e.pos++
	}
}

// View renders the prompt with the line, a text wider than the width
// of the terminal wraps onto the next row.
func (e lineEdit) View() string {
	return e.wrap(e.Prompt + string(e.value))
}

// wrap fills the rows of the terminal up to their last column, a rune
// that does not fit moves to the next row whole.
func (e lineEdit) wrap(s string) string {
	return ansi.Hardwrap(s, e.width, true)
}
