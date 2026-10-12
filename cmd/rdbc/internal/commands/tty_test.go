package commands

import (
	"bytes"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/exp/teatest/v2"
)

// stubSession records the commands it is asked to write and answers
// the read of the session with a plain reply.
type stubSession struct {
	mu        sync.Mutex
	ran       [][]byte
	err       error
	noteAfter time.Duration // the timeout of a write
}

func (e *stubSession) writeCmd(sentID int, args [][]byte) tea.Cmd {
	return tea.Batch(func() tea.Msg {
		if e.noteAfter <= 0 {
			return nil
		}
		<-time.NewTimer(e.noteAfter).C
		return timeoutMsg{latestID: sentID}
	}, func() tea.Msg {
		e.mu.Lock()
		e.ran = append(e.ran, slices.Clone(args[0]))
		err := e.err
		e.mu.Unlock()
		return writtenMsg{err: err}
	})
}

func (e *stubSession) commands() [][]byte {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Clone(e.ran)
}

// waitFor polls until the commands of the stub satisfy cond.
func waitForCommands(t *testing.T, e *stubSession, cond func([][]byte) bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond(e.commands()) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("commands not run, got %q", e.commands())
}

func newTestInputModel(t *testing.T, e *stubSession) *teatest.TestModel {
	t.Helper()
	m := newInputModel(e, t.Context()).WithPrompt("test> ")
	tm := teatest.NewTestModel(t, m, teatest.WithInitialTermSize(40, 10))
	return tm
}

func Test_inputModel_Update(t *testing.T) {
	t.Run("submit runs the command", func(t *testing.T) {
		e := &stubSession{}
		tm := newTestInputModel(t, e)
		tm.Type("PING")
		tm.Send(tea.KeyPressMsg{Code: tea.KeyEnter})
		waitForCommands(t, e, func(cmds [][]byte) bool {
			return len(cmds) == 1 && string(cmds[0]) == "PING"
		})
		// The submitted line stays on the screen, the program prints it
		// above the input line before it runs the command.
		teatest.WaitFor(t, tm.Output(), func(b []byte) bool {
			return bytes.Contains(b, []byte("test> PING"))
		}, teatest.WithDuration(2*time.Second))
		tm.Quit()
		tm.WaitFinished(t, teatest.WithFinalTimeout(2*time.Second))
		fm := tm.FinalModel(t).(inputModel)
		if fm.input.Value() != "" {
			t.Errorf("input = %q after the submit, want it empty", fm.input.Value())
		}
	})

	t.Run("enter of an empty input runs nothing", func(t *testing.T) {
		e := &stubSession{}
		tm := newTestInputModel(t, e)
		tm.Send(tea.KeyPressMsg{Code: tea.KeyEnter})
		time.Sleep(50 * time.Millisecond)
		if cmds := e.commands(); len(cmds) != 0 {
			t.Errorf("commands = %q, want none", cmds)
		}
		tm.Quit()
		tm.WaitFinished(t, teatest.WithFinalTimeout(2*time.Second))
	})

	// The up key browses the history of the submitted lines, the
	// first line stays at the top of the history.
	t.Run("history up", func(t *testing.T) {
		e := &stubSession{}
		tm := newTestInputModel(t, e)
		tm.Type("first")
		tm.Send(tea.KeyPressMsg{Code: tea.KeyEnter})
		waitForCommands(t, e, func(cmds [][]byte) bool { return len(cmds) == 1 })
		tm.Type("second")
		tm.Send(tea.KeyPressMsg{Code: tea.KeyEnter})
		waitForCommands(t, e, func(cmds [][]byte) bool { return len(cmds) == 2 })
		tm.Type("sec")
		tm.Send(tea.KeyPressMsg{Code: tea.KeyUp})
		tm.Send(tea.KeyPressMsg{Code: tea.KeyUp})
		tm.Send(tea.KeyPressMsg{Code: tea.KeyUp})
		tm.Quit()
		tm.WaitFinished(t, teatest.WithFinalTimeout(2*time.Second))
		fm := tm.FinalModel(t).(inputModel)
		if fm.input.Value() != "first" {
			t.Errorf("input = %q, want the first line of the history", fm.input.Value())
		}
	})

	// The down key returns to the browsed entries and behind the last
	// one to the draft.
	t.Run("history down", func(t *testing.T) {
		e := &stubSession{}
		tm := newTestInputModel(t, e)
		tm.Type("first")
		tm.Send(tea.KeyPressMsg{Code: tea.KeyEnter})
		waitForCommands(t, e, func(cmds [][]byte) bool { return len(cmds) == 1 })
		tm.Type("second")
		tm.Send(tea.KeyPressMsg{Code: tea.KeyEnter})
		waitForCommands(t, e, func(cmds [][]byte) bool { return len(cmds) == 2 })
		tm.Type("sec")
		tm.Send(tea.KeyPressMsg{Code: tea.KeyUp})
		tm.Send(tea.KeyPressMsg{Code: tea.KeyDown})
		tm.Send(tea.KeyPressMsg{Code: tea.KeyDown})
		tm.Quit()
		tm.WaitFinished(t, teatest.WithFinalTimeout(2*time.Second))
		fm := tm.FinalModel(t).(inputModel)
		if fm.input.Value() != "sec" {
			t.Errorf("draft = %q, want %q", fm.input.Value(), "sec")
		}
	})

	// Ctrl-C ends the session whatever the input holds, a typed
	// command is not run.
	t.Run("ctrl-c ends the session", func(t *testing.T) {
		e := &stubSession{}
		tm := newTestInputModel(t, e)
		tm.Type("SET key value")
		tm.Send(tea.KeyPressMsg{Code: 'c', Mod: tea.ModCtrl})
		tm.WaitFinished(t, teatest.WithFinalTimeout(2*time.Second))
		if cmds := e.commands(); len(cmds) != 0 {
			t.Errorf("commands = %q, want none", cmds)
		}
	})

	t.Run("ctrl-d of an empty input ends the session", func(t *testing.T) {
		e := &stubSession{}
		tm := newTestInputModel(t, e)
		tm.Send(tea.KeyPressMsg{Code: 'd', Mod: tea.ModCtrl}) // Ctrl-D
		tm.WaitFinished(t, teatest.WithFinalTimeout(2*time.Second))
	})

	// Ctrl-D ends the session whatever the input holds, like Ctrl-C,
	// the line as typed stays on the screen.
	t.Run("ctrl-d of a text ends the session", func(t *testing.T) {
		e := &stubSession{}
		tm := newTestInputModel(t, e)
		tm.Type("SET key value")
		tm.Send(tea.KeyPressMsg{Code: 'd', Mod: tea.ModCtrl})
		tm.WaitFinished(t, teatest.WithFinalTimeout(2*time.Second))
		if cmds := e.commands(); len(cmds) != 0 {
			t.Errorf("commands = %q, want none", cmds)
		}
		teatest.WaitFor(t, tm.Output(), func(b []byte) bool {
			return bytes.Contains(b, []byte("SET key value"))
		}, teatest.WithDuration(2*time.Second))
	})

	// A QUIT command that received its reply ends the session.
	t.Run("quit ends the session", func(t *testing.T) {
		e := &stubSession{}
		tm := newTestInputModel(t, e)
		tm.Type("QUIT")
		tm.Send(tea.KeyPressMsg{Code: tea.KeyEnter})
		waitForCommands(t, e, func(cmds [][]byte) bool {
			return len(cmds) == 1 && string(cmds[0]) == "QUIT"
		})
		tm.Send(ackReplyMsg{})
		tm.WaitFinished(t, teatest.WithFinalTimeout(2*time.Second))
	})

	// An error of the session ends it with the error.
	t.Run("session error ends the session", func(t *testing.T) {
		e := &stubSession{err: fmt.Errorf("broken connection")}
		tm := newTestInputModel(t, e)
		tm.Type("PING")
		tm.Send(tea.KeyPressMsg{Code: tea.KeyEnter})
		tm.WaitFinished(t, teatest.WithFinalTimeout(2*time.Second))
		fm := tm.FinalModel(t).(inputModel)
		if fm.err == nil {
			t.Error("the session error was not reported")
		}
	})

	// A write that outlives the timeout of the session is noted while
	// the session keeps waiting for a message.
	t.Run("notes a slow reply", func(t *testing.T) {
		e := &stubSession{noteAfter: 50 * time.Millisecond}
		tm := newTestInputModel(t, e)
		tm.Type("PING")
		tm.Send(tea.KeyPressMsg{Code: tea.KeyEnter})
		go func() {
			select {
			case <-time.After(200 * time.Millisecond):
				tm.Send(ackReplyMsg{})
			case <-t.Context().Done():
				return
			}
		}()
		teatest.WaitFor(t, tm.Output(), func(b []byte) bool {
			return bytes.Contains(b, []byte("(reply is still pending, waiting)"))
		}, teatest.WithDuration(2*time.Second))
		tm.WaitFinished(t, teatest.WithFinalTimeout(2*time.Second))
	})

	// An empty line runs nothing, the input is read again.
	t.Run("empty input line runs nothing", func(t *testing.T) {
		e := &stubSession{}
		tm := newTestInputModel(t, e)
		tm.Type("  ")
		tm.Send(tea.KeyPressMsg{Code: tea.KeyEnter})
		time.Sleep(50 * time.Millisecond)
		if cmds := e.commands(); len(cmds) != 0 {
			t.Errorf("commands = %q, want none", cmds)
		}
		tm.Quit()
		tm.WaitFinished(t, teatest.WithFinalTimeout(2*time.Second))
	})
}

func Test_inputModel_View(t *testing.T) {
	// The input line wraps over the rows of the terminal, the cursor
	// sits on the row and column it landed on.
	t.Run("cursor wraps", func(t *testing.T) {
		e := &stubSession{}
		m := newInputModel(e, t.Context()).WithPrompt("test> ")
		nm, _ := m.Update(tea.WindowSizeMsg{Width: 10, Height: 5})
		m = nm.(inputModel)
		m.input.SetValue("abcdefghijkl")
		m.input.CursorEnd()
		if got, want := m.input.View(), "test> abcd\nefghijkl"; got != want {
			t.Errorf("view = %q, want %q", got, want)
		}
		c := m.input.Cursor()
		if c == nil {
			t.Fatal("the view has no cursor")
		}
		// The prompt is 6 columns and the input 12 more, over a
		// terminal of 10 columns the cursor sits on the second row.
		if c.Position.X != 8 || c.Position.Y != 1 {
			t.Errorf("cursor = %d,%d, want 8,1", c.Position.X, c.Position.Y)
		}
	})

	// A wide rune wraps as a whole onto the next row when it does not
	// fit, the cursor counts it as two columns.
	t.Run("wide rune wraps and counts twice", func(t *testing.T) {
		m := newInputModel(&stubSession{}, t.Context()).WithPrompt("")
		nm, _ := m.Update(tea.WindowSizeMsg{Width: 10, Height: 5})
		m = nm.(inputModel)
		m.input.SetValue("abcdefghi漢")
		m.input.CursorEnd()
		if got, want := m.input.View(), "abcdefghi\n漢"; got != want {
			t.Errorf("view = %q, want %q", got, want)
		}
		c := m.input.Cursor()
		if c == nil {
			t.Fatal("the view has no cursor")
		}
		if c.Position.X != 2 || c.Position.Y != 1 {
			t.Errorf("cursor = %d,%d, want 2,1", c.Position.X, c.Position.Y)
		}
	})
}
