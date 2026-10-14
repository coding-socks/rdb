package commands

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"charm.land/bubbletea/v2"
	"github.com/coding-socks/rdb/clusters"
	"github.com/coding-socks/rdb/commands"
	"github.com/coding-socks/rdb/resp"
	"golang.org/x/term"
)

func Client(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("client", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), `usage: rdbc client [-timeout DURATION] CONNECTION_STRING [COMMAND [ARG...]]

Connects to a Redis compatible server, KeyDB and Valkey included. The
connection string is of the form redis://user:pass@host:port, the s
variants rediss://, keydbs:// and valkeys:// connect over TLS. The user
parameters execute the HELLO command. With a COMMAND the response is
written to stdout and rdbc exits, without one an interactive session
starts.
`)
		fs.PrintDefaults()
	}
	var (
		cluster bool
		timeout time.Duration
		proto   int
	)
	fs.BoolVar(&cluster, "cluster", false, "connect to a cluster")
	fs.DurationVar(&timeout, "timeout", 10*time.Second, "time to wait for a reply, zero waits forever")
	fs.IntVar(&proto, "resp", 2, "protocol version to start the session with, it issues HELLO")

	fs.Parse(args)
	rest := fs.Args()
	if len(rest) == 0 {
		fs.Usage()
		return errors.New("rdbc client: missing connection string")
	}

	u, err := url.Parse(rest[0])
	if err != nil {
		return err
	}
	seed, err := resp.Dial(ctx, u, proto)
	if err != nil {
		return err
	}
	nodes := clusters.Single(seed)
	if cluster {
		nodes, err = clusters.Upgrade(ctx, seed)
		if err != nil {
			seed.Close()
			return fmt.Errorf("rdbc: unable to upgrade to cluster: %w", err)
		}
	}
	defer nodes.Close()

	if len(rest) > 1 {
		cmdArgs := make([][]byte, len(rest)-1)
		for i, arg := range rest[1:] {
			cmdArgs[i] = []byte(arg)
		}
		return runCommand(ctx, os.Stdout, nodes, cmdArgs, timeout)
	}
	return clientSession(ctx, u, nodes, timeout)
}

// runCommand executes a single command and writes the formatted
// response to w. An error reply is reported as an error, everything
// else the server sent is written.
func runCommand(ctx context.Context, w io.Writer, cluster *clusters.Cluster, args [][]byte, timeout time.Duration) error {
	var (
		keys, _ = cluster.Keys(args...)
		node    clusters.Node
		err     error
	)
	if len(keys) == 0 {
		nodes := cluster.All()
		node = nodes[rand.IntN(len(nodes))]
	} else {
		node, err = cluster.Target(keys[0])
		if err != nil {
			return fmt.Errorf("rdbc: unable to target node: %w", err)
		}
	}
	conn := node.Conn
	replyCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	t, err := conn.Exec(replyCtx, args...)
	if err != nil {
		return err
	}
	if t.Kind() == resp.KindError {
		return t.Err()
	}
	_, err = io.Copy(w, NewTokenReader(conn, t))
	return err
}

// clientSession runs an interactive session.
func clientSession(ctx context.Context, u *url.URL, cluster *clusters.Cluster, timeout time.Duration) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	s := newSession(cluster, timeout)
	m := newInputModel(s, ctx).
		WithPrompt(u.Hostname() + "> ").
		WithSuggester(commands.NewSuggester(cluster.Dialect().Commands()))
	opts := []tea.ProgramOption{tea.WithContext(ctx)}
	if !term.IsTerminal(int(os.Stdin.Fd())) {
		opts = append(opts, tea.WithInput(os.Stdin), tea.WithoutRenderer())
	}
	p := tea.NewProgram(m, opts...)

	for _, n := range cluster.All() {
		go func() {
			for {
				if err := ctx.Err(); err != nil {
					return
				}
				t, err := n.Conn.ReadValue(ctx)
				if err != nil {
					return
				}
				s := bufio.NewScanner(NewTokenReader(n.Conn, t))
				for s.Scan() {
					p.Println(s.Text())
				}
				p.Send(ackReplyMsg{})
			}
		}()
	}

	final, err := p.Run()
	if f, ok := final.(inputModel); ok && f.err != nil {
		return f.err
	}
	if ctx.Err() != nil || errors.Is(err, tea.ErrInterrupted) || errors.Is(err, tea.ErrProgramKilled) {
		return nil
	}
	return err
}

// session is the connection of an interactive client. One goroutine
// per node of the cluster reads its messages, every message read
// travels to the model of the session and the model decides where it
// is printed. Commands are written as they are submitted, their
// replies are just the messages that follow on the connection.
type session struct {
	cluster *clusters.Cluster
	timeout time.Duration
}

func newSession(cluster *clusters.Cluster, timeout time.Duration) *session {
	return &session{cluster: cluster, timeout: timeout}
}

// ackReplyMsg is a message read from the connection.
type ackReplyMsg struct{}

// errorMsg is an error message read from the connection.
type errorMsg error

// writtenMsg is the outcome of writing a command to the connection.
type writtenMsg struct {
	err error
}

// timeoutMsg is the timeout of a write elapsed, latestID is the message
// ID of the model at the time the command was written.
type timeoutMsg struct {
	latestID int
}

// writeCmd writes a command to the connection.
func (s *session) writeCmd(latestID int, args [][]byte) tea.Cmd {
	return tea.Batch(func() tea.Msg {
		if s.timeout <= 0 {
			return nil
		}
		<-time.NewTimer(s.timeout).C
		return timeoutMsg{latestID: latestID}
	}, func() tea.Msg {
		keys, _ := s.cluster.Keys(args...)
		var node clusters.Node
		if len(keys) == 0 {
			nodes := s.cluster.All()
			node = nodes[rand.IntN(len(nodes))]
		} else {
			node, _ = s.cluster.Target(keys[0])
		}
		conn := node.Conn
		return writtenMsg{err: conn.WriteAndFlush(args...)}
	})
}

// NewTokenReader returns a reader of the elements of a message: the
// elements of an aggregate are read from the decoder of the connection
// and written to the reader one by one, the bytes of a blob are its
// value. The reader has to be read to the end, or the messages that
// follow on the connection desync.
func NewTokenReader(conn *resp.Client, t resp.Token) io.Reader {
	r, w := io.Pipe()
	go func() {
		defer w.Close()
		if t.Kind() == resp.KindPush {
			// A push message cannot be unbound, its elements are read
			// directly.
			writeElems3(conn.D, t, &pushWriter{w: w, bol: true}, "")
			return
		}
		writeReplyValue(conn.D, t, w)
	}()
	return r
}

// writeReply reads one message and writes it in the redis-cli like
// format of the RESP3 protocol into buf. A push message is formatted
// like a reply, its lines are marked to keep them apart from the
// replies of commands.
func writeReply(d resp.Decoder, w io.Writer) error {
	t, err := d.ReadToken()
	if err != nil {
		return err
	}
	if t.Kind() == resp.KindPush {
		// A push message cannot be unbound, its elements are read
		// directly.
		return writeElems3(d, t, &pushWriter{w: w, bol: true}, "")
	}
	return writeReplyValue(d, t, w)
}

// pushWriter prefixes every line of a push message with a marker, the
// lines of the message are kept apart from the replies of commands.
type pushWriter struct {
	w   io.Writer
	bol bool // at the beginning of a line
}

func (p *pushWriter) Write(data []byte) (int, error) {
	b, n := data, 0
	for {
		if len(b) == 0 {
			return 0, nil
		}
		if p.bol {
			m, err := io.WriteString(p.w, "> ")
			n += m
			if err != nil {
				return n, err
			}
			p.bol = false
		}
		i := bytes.IndexByte(b, '\n')
		if i < 0 {
			m, err := p.w.Write(b)
			n += m
			return n, err
		}
		m, err := p.w.Write(b[:i+1])
		n += m
		if err != nil {
			return n, err
		}
		p.bol = true
		b = b[i+1:]
	}
}

func writeReplyValue(d resp.Decoder, t resp.Token, w io.Writer) error {
	switch t.Kind() {
	case resp.KindError, resp.KindBlobError:
		_, err := io.WriteString(w, t.Err().Error())
		return err
	case resp.KindBlobString:
		// The nil check of a blob string is true for the unbound form
		// too, it has to run first.
		if t.IsUnbound() {
			parts, partsErr := t.Iter(d)
			for e := range parts {
				if _, err := w.Write(e.Bytes()); err != nil {
					return err
				}
			}
			if err := partsErr(); err != nil {
				return err
			}
			_, err := io.WriteString(w, "\n")
			return err
		}
		if t.IsNil() {
			_, err := io.WriteString(w, "(nil)\n")
			return err
		}
		if _, err := w.Write(t.Bytes()); err != nil {
			return err
		}
		_, err := io.WriteString(w, "\n")
		return err
	case resp.KindAttribute:
		// Attributes carry metadata about the reply that follows them,
		// they are read and skipped.
		pairs, pairsErr := t.Iter(d)
		for range pairs {
		}
		if err := pairsErr(); err != nil {
			return err
		}
		return writeReply(d, w)
	}
	return writeNested(d, t, w, "")
}

// writeNested writes an element of an RESP3 aggregate reply. Elements
// of an aggregate are quoted, a nested aggregate indents its entries
// with prefix.
func writeNested(d resp.Decoder, t resp.Token, w io.Writer, prefix string) error {
	switch t.Kind() {
	case resp.KindString:
		_, err := fmt.Fprintf(w, "%s\n", t.String())
		return err
	case resp.KindBlobString:
		if t.IsUnbound() {
			var b bytes.Buffer
			parts, partsErr := t.Iter(d)
			for e := range parts {
				b.Write(e.Bytes())
			}
			if err := partsErr(); err != nil {
				return err
			}
			_, err := fmt.Fprintf(w, "%q\n", b.Bytes())
			return err
		}
		if t.IsNil() {
			_, err := io.WriteString(w, "(nil)\n")
			return err
		}
		_, err := fmt.Fprintf(w, "%q\n", t.Bytes())
		return err
	case resp.KindVerbatimString:
		v := t.VerbatimString()
		_, err := fmt.Fprintf(w, "(verbatim:%s) %q\n", v.Format, v.String)
		return err
	case resp.KindError, resp.KindBlobError:
		_, err := fmt.Fprintf(w, "(error) %s\n", t.Err())
		return err
	case resp.KindNumber:
		_, err := fmt.Fprintf(w, "(integer) %d\n", t.Int())
		return err
	case resp.KindBigNumber:
		_, err := fmt.Fprintf(w, "(big number) %s\n", t.BigInt())
		return err
	case resp.KindDouble:
		_, err := fmt.Fprintf(w, "(dbl) %s\n", strconv.FormatFloat(t.Float(), 'g', -1, 64))
		return err
	case resp.KindBoolean:
		_, err := fmt.Fprintf(w, "(boolean) %t\n", t.Bool())
		return err
	case resp.KindNull, resp.KindEnd:
		_, err := io.WriteString(w, "(nil)\n")
		return err
	case resp.KindArray, resp.KindSet, resp.KindPush:
		return writeElems3(d, t, w, prefix)
	case resp.KindMap:
		return writePairs(d, t, w, prefix)
	}
	return fmt.Errorf("client: unexpected reply kind %s", t.Kind())
}

// writeElems3 writes an array, set or push reply, one element per
// numbered line. A nested aggregate starts on the line of its index,
// its following entries indent under it.
func writeElems3(d resp.Decoder, t resp.Token, w io.Writer, prefix string) error {
	sep, empty := ")", "(empty array)"
	switch t.Kind() {
	case resp.KindSet:
		sep, empty = "~", "(empty set)"
	case resp.KindPush:
		sep, empty = ")", "(empty push)"
	}
	if t.IsNil() {
		_, err := fmt.Fprintf(w, "(nil)\n")
		return err
	}
	if t.IsUnbound() {
		child := prefix + "   "
		for i := 0; ; i++ {
			e, err := d.ReadToken()
			if err != nil {
				return err
			}
			if e.Kind() == resp.KindEnd {
				return nil
			}
			if i > 0 {
				fmt.Fprint(w, prefix)
			}
			fmt.Fprintf(w, "%d%s ", i+1, sep)
			if err := writeNested(d, e, w, child); err != nil {
				return err
			}
		}
	}
	n := int(t.Int())
	if n == 0 {
		_, err := fmt.Fprintf(w, "%s\n", empty)
		return err
	}
	child := prefix + strings.Repeat(" ", numDigits(n)+2)
	for i := range n {
		e, err := d.ReadToken()
		if err != nil {
			return err
		}
		if i > 0 {
			fmt.Fprint(w, prefix)
		}
		fmt.Fprintf(w, "%*d%s ", numDigits(n), i+1, sep)
		if err := writeNested(d, e, w, child); err != nil {
			return err
		}
	}
	return nil
}

// writePairs writes a map reply, one key => value pair per numbered
// line. A multiline value breaks to the next line and indents under
// the key.
func writePairs(d resp.Decoder, t resp.Token, w io.Writer, prefix string) error {
	if t.IsUnbound() {
		child := prefix + "   "
		for i := 0; ; i++ {
			key, err := d.ReadToken()
			if err != nil {
				return err
			}
			if key.Kind() == resp.KindEnd {
				return nil
			}
			key = key.Clone()
			value, err := d.ReadToken()
			if err != nil {
				return err
			}
			if i > 0 {
				fmt.Fprint(w, prefix)
			}
			fmt.Fprintf(w, "%d# ", i+1)
			if err := writePair(d, key, value, w, child); err != nil {
				return err
			}
		}
	}
	n := int(t.Int())
	if n == 0 {
		_, err := io.WriteString(w, "(empty hash)\n")
		return err
	}
	child := prefix + strings.Repeat(" ", numDigits(n)+2)
	for i := range n {
		key, err := d.ReadToken()
		if err != nil {
			return err
		}
		key = key.Clone()
		value, err := d.ReadToken()
		if err != nil {
			return err
		}
		if i > 0 {
			fmt.Fprint(w, prefix)
		}
		fmt.Fprintf(w, "%*d# ", numDigits(n), i+1)
		if err := writePair(d, key, value, w, child); err != nil {
			return err
		}
	}
	return nil
}

// writePair writes one key => value pair of a map reply. The value is
// buffered to decide whether it renders over multiple lines.
func writePair(d resp.Decoder, key, value resp.Token, w io.Writer, child string) error {
	var b bytes.Buffer
	if err := writeNested(d, key, &b, child); err != nil {
		return err
	}
	k := b.Bytes()
	if _, err := w.Write(k[:len(k)-1]); err != nil {
		return err
	}
	if _, err := io.WriteString(w, " => "); err != nil {
		return err
	}
	b.Reset()
	if err := writeNested(d, value, &b, child); err != nil {
		return err
	}
	v := b.Bytes()
	if bytes.IndexByte(v[:len(v)-1], '\n') >= 0 {
		if _, err := io.WriteString(w, "\n"+child); err != nil {
			return err
		}
	}
	_, err := w.Write(v)
	return err
}

func numDigits(n int) int {
	return len(strconv.Itoa(n))
}
