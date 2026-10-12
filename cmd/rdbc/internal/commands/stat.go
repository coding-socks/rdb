package commands

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"charm.land/bubbles/v2/progress"
	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"
	"charm.land/glamour/v2"
	"charm.land/glamour/v2/styles"
	"charm.land/lipgloss/v2"
	"charm.land/lipgloss/v2/table"
	"github.com/coding-socks/rdb/internal/chain"
	"github.com/coding-socks/rdb/resp"
	"golang.org/x/term"
)

func Stat(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("stat", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), `usage: rdbc stat [-samples SAMPLES] [-top TOP] [-match PATTERN] [-count COUNT] [-sleep SLEEP] [-glamour] CONNECTION_STRING [CURSOR]

Connects to a Redis compatible server, KeyDB and Valkey included. The
connection string is of the form redis://user:pass@host:port, the s
variants rediss://, keydbs:// and valkeys:// connect over TLS. The
keyspace of the target server is scanned and the keys matching the
given glob-style pattern are processed to provide statistics of the
memory usage and number of elements.
`)
		fs.PrintDefaults()
	}
	var (
		samples  uint64
		top      uint64
		match    string
		count    uint64
		sleep    uint64
		glamourF bool
	)
	fs.Uint64Var(&samples, "samples", 5, "Number of samples to collect")
	fs.Uint64Var(&top, "top", 10, "Number of top commands to show")
	fs.StringVar(&match, "match", "", "Only collect stats from keys matching the given glob-style pattern.")
	fs.Uint64Var(&count, "count", 10, "The number of keys to process per iteration.")
	fs.Uint64Var(&sleep, "sleep", 0, "Sleep time in milliseconds between samples.")
	fs.BoolVar(&glamourF, "glamour", false, "Render the report with the dark glamour theme.")

	fs.Parse(args)
	rest := fs.Args()
	if len(rest) < 1 || 2 < len(rest) {
		fs.Usage()
		return errors.New("missing connection string")
	}
	u, err := url.Parse(rest[0])
	if err != nil {
		return errors.New("invalid connection string")
	}
	var cursor uint64
	if len(rest) == 2 {
		i, err := strconv.ParseUint(rest[1], 10, 64)
		if err != nil {
			fs.Usage()
			return errors.New("invalid cursor")
		}
		cursor = i
	}

	style := styles.AsciiStyle
	if glamourF {
		style = styles.DarkStyle
	}
	renderer, err := glamour.NewTermRenderer(
		glamour.WithStylePath(style),
	)
	if err != nil {
		renderer, _ = glamour.NewTermRenderer()
	}

	conn, err := resp.Dial(ctx, u, 3)
	if err != nil {
		return fmt.Errorf("failed to initialize connection: %w", err)
	}
	defer conn.Close()

	// The program cancels the context it derives from this one, the
	// external context is only cancelled here, when the run ended.
	// The cancellation releases the scanner behind a read.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var lastMsg reportMsg
	s := &statScanner{
		conn: conn,
		send: func(msg reportMsg) {
			lastMsg = msg
		},

		bulk: chain.NewBulk(conn,
			chain.WithCount(int64(count)),
			chain.WithMatch([]byte(match)),
			chain.WithCursor(cursor),
		),
		samples: samples,

		stats: *newKeyStat(int(top)),
		types: newTypeStats(),
	}
	if !term.IsTerminal(int(os.Stdout.Fd())) {
		if err := s.Scan(ctx); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
		fmt.Fprint(os.Stdout, renderReport(renderer, lastMsg.reportView(reportSectionAll)))
		return nil
	}

	p := tea.NewProgram(statModel{
		progress: progress.New(progress.WithDefaultBlend(), func(m *progress.Model) { m.PercentFormat = " %6.2f%%" }),
		renderer: renderer,
	}, tea.WithContext(ctx))
	s.send = func(msg reportMsg) {
		lastMsg = msg
		p.Send(msg)
	}
	go func() {
		if err := s.Scan(ctx); err != nil {
			p.Send(errorMsg(err))
		}
	}()
	_, err = p.Run()
	if err != nil {
		return fmt.Errorf("failed to run program: %w", err)
	}
	fmt.Fprint(os.Stdout, renderReport(renderer, lastMsg.reportView(reportSectionAll)))
	return nil
}

// reportMsg is one beat of the walk: the percent of the keyspace
// folded so far, and, when the walk ended, the report of the
// statistics with the cursor to resume from.
type reportMsg struct {
	percent float64
	report  keyReport
	cursor  uint64
}

// statTable renders one table of the report, the cells styled the
// way their column asks for, the headers bold above a rule.
func statTable(headers []string, rows [][]string) string {
	return table.New().
		Headers(headers...).
		Rows(rows...).
		BorderTop(false).BorderBottom(false).
		Border(lipgloss.MarkdownBorder()).
		String()
}

// renderReport renders the markdown of the report with the given
// glamour style. A failed render falls back to the markdown.
func renderReport(renderer *glamour.TermRenderer, md string) string {
	if renderer == nil {
		return md
	}
	out, err := renderer.Render(md)
	if err != nil {
		return md
	}
	return out
}

const (
	reportSectionTopSizes reportSection = 1 << iota
	reportSectionTypeSizes
	reportSectionTypeLengths
	reportSectionSizeDist
	reportSectionLengthDist
	reportSectionKeyTypes
	reportSectionCursor

	// reportSectionLive is the display of a running report.
	reportSectionLive = reportSectionTopSizes | reportSectionTypeSizes | reportSectionTypeLengths
	reportSectionAll  = math.MaxUint
)

// reportSection is a bit mask of the sections of a report.
type reportSection uint

// reportView returns a report the sections the mask names.
func (m reportMsg) reportView(sections reportSection) string {
	r := m.report
	var sb strings.Builder
	var sep string

	if sections&reportSectionTopSizes != 0 {
		fmt.Fprintf(&sb, "%s# Top %d key sizes\n\n", sep, len(r.top))
		rows := make([][]string, 0, len(r.top))
		for i, k := range r.top {
			rows = append(rows, []string{
				strconv.Itoa(i + 1),
				humanBytes(k.size),
				k.typ,
				quoteKey(k.key),
			})
		}
		io.WriteString(&sb, statTable([]string{"#", "Size", "Type", "Key"}, rows))
		sep = "\n\n"
	}

	if sections&reportSectionTypeSizes != 0 {
		fmt.Fprintf(&sb, "%s# Top size per type\n\n", sep)
		rows := make([][]string, 0, len(r.types))
		for _, t := range r.types {
			if t.biggestMemKey != "" {
				rows = append(rows, []string{t.name, quoteKey(t.biggestMemKey), humanBytes(t.biggestMem)})
			}
		}
		io.WriteString(&sb, statTable([]string{"Type", "Key", "Size"}, rows))
		sep = "\n\n"
	}

	if sections&reportSectionTypeLengths != 0 {
		fmt.Fprintf(&sb, "%s# Top length and cardinality per type\n\n", sep)
		rows := make([][]string, 0, len(r.types))
		for _, t := range r.types {
			if t.biggestLenKey == "" {
				continue
			}
			su := sizeUnits[t.name]
			length := fmt.Sprintf("%d %s", t.biggestLen, su)
			if su == "bytes" {
				length = humanBytes(t.biggestLen)
			}
			rows = append(rows, []string{t.name, quoteKey(t.biggestLenKey), length})
		}
		io.WriteString(&sb, statTable([]string{"Type", "Key", "Length"}, rows))
		sep = "\n\n"
	}

	if sections&reportSectionSizeDist != 0 {
		fmt.Fprintf(&sb, "%s# Key size distribution\n\n", sep)
		rows := make([][]string, 0, len(r.mem.percentiles))
		for _, row := range r.mem.percentiles {
			rows = append(rows, []string{
				humanBytes(row.size),
				fmt.Sprintf("%.2f%%", row.p),
				strconv.FormatInt(row.keys, 10),
			})
		}
		io.WriteString(&sb, statTable([]string{"Key size", "Percentile", "Total keys"}, rows))
		fmt.Fprintf(&sb, "\n\nMean: %s\n", humanBytes(r.mem.mean))
		sep = "\n\n"
	}

	if sections&reportSectionLengthDist != 0 {
		fmt.Fprintf(&sb, "%s# Key name length distribution\n\n", sep)
		rows := make([][]string, 0, len(r.lens.rows))
		for _, row := range r.lens.rows {
			size := "inf"
			if row.size > 0 {
				size = humanBytes(row.size)
			}
			rows = append(rows, []string{
				size,
				fmt.Sprintf("%.2f%%", row.percent),
				strconv.FormatUint(row.keys, 10),
			})
		}
		io.WriteString(&sb, statTable([]string{"Key name length", "Percentile", "Total keys"}, rows))
		if r.lens.totalLen > 0 {
			fmt.Fprintf(&sb, "\n\nTotal key length is %s (%s avg)\n",
				humanBytes(r.lens.totalLen),
				humanBytes(uint64(r.lens.avgLen)))
		}
		sep = "\n\n"
	}

	if sections&reportSectionKeyTypes != 0 {
		fmt.Fprintf(&sb, "%s# Key types\n\n", sep)
		rows := make([][]string, 0, len(r.types))
		for _, t := range r.types {
			percent := 0.0
			if r.sampled > 0 {
				percent = 100 * float64(t.count) / float64(r.sampled)
			}
			totalLen, avgLen := " - ", " - "
			if _, ok := sizeCommands[t.name]; ok && t.count > 0 {
				avg := float64(t.totalLen) / float64(t.count)
				su := sizeUnits[t.name]
				if su == "bytes" {
					totalLen = humanBytes(t.totalLen)
					avgLen = humanBytes(uint64(avg))
				} else {
					totalLen = fmt.Sprintf("%d %s", t.totalLen, su)
					avgLen = fmt.Sprintf("%.2f", avg)
				}
			}
			rows = append(rows, []string{
				t.name,
				strconv.FormatInt(t.count, 10),
				fmt.Sprintf("%.2f%%", percent),
				humanBytes(t.totalMem),
				humanBytes(uint64(float64(t.totalMem) / float64(t.count))),
				totalLen,
				avgLen,
			})
		}
		io.WriteString(&sb, statTable([]string{"Type", "Total keys", "Keys %", "Tot size", "Avg size", "Total length/card", "Avg ln/card"}, rows))
		sep = "\n\n"
	}

	if sections&reportSectionCursor != 0 {
		fmt.Fprintf(&sb, "%sScan ended at cursor %d.", sep, m.cursor)
		if m.cursor != 0 {
			fmt.Fprintf(&sb, "\n\nUse 'rdbc stat CONNECTION_STRING %d' to scan the keyspace again from there.", m.cursor)
		}
	}
	return sb.String()
}

// loadMsg is one key of the walk with the replies of its read.
type loadMsg struct {
	key    []byte
	kind   string
	mem    uint64
	len    int64
	hasLen bool // a length command answered for the key
}

// statScanner scans the keyspace of a connection and folds every key
// into its statistics. Scan runs on its own goroutine, everything it
// reports leaves through send: a reportMsg per percent of the walk and
// the reportMsg of the finished walk with the resume cursor. The
// values it delivers are copies, the buffers they were read into are
// reused by the connection.
type statScanner struct {
	conn *resp.Client
	send func(msg reportMsg)

	bulk    *chain.Bulk
	samples uint64 // the samples of the MEMORY USAGE command

	stats keyStat
	types *typeStats // the size commands of the known types

	dbSize   int64
	loadN    int64
	lastFold time.Time
}

// Scan walks the keyspace and folds every key into the statistics. It
// delivers a reportMsg per percent of the walk and the reportMsg of
// the finished walk through send, and returns the first error of the
// connection, of a reply, or of the context.
func (s *statScanner) Scan(ctx context.Context) error {
	// The percent of the walk needs the size of the keyspace before
	// the first page folds, the connection is idle and serves it
	// before the commands of the walk.
	if err := s.conn.Write([]byte("DBSIZE")); err != nil {
		return err
	}
	t, err := s.conn.ReadValue(ctx)
	if err != nil {
		return err
	}
	if t.Kind() == resp.KindError {
		return t.Err()
	}
	if t.Kind() != resp.KindNumber {
		return fmt.Errorf("expected number, got %s", t.Kind())
	}
	s.dbSize = t.Int()

	if err := s.bulk.Scan(ctx, s.visit()); err != nil {
		return err
	}
	// The resumability hint of the report names the cursor of the
	// page that follows the walk, the cursor 0 ends it.
	s.send(reportMsg{percent: 1, report: s.stats.report(), cursor: s.bulk.Cursor()})
	return nil
}

func (s *statScanner) visit() func(key []byte) {
	return func(key []byte) {
		var typ string
		s.bulk.Send([][]byte{[]byte("TYPE"), key}, func(t resp.Token, _ resp.Decoder) error {
			if t.Kind() == resp.KindError {
				return t.Err()
			}
			typ = t.String()
			return nil
		})
		s.bulk.Send([][]byte{
			[]byte("MEMORY"), []byte("USAGE"), key,
			[]byte("SAMPLES"), strconv.AppendUint(nil, s.samples, 10),
		}, func(t resp.Token, _ resp.Decoder) error {
			if t.Kind() == resp.KindError {
				return t.Err()
			}
			var mem uint64
			switch t.Kind() {
			case resp.KindNumber:
				mem = uint64(t.Int())
			case resp.KindNull:
				mem = 0
			default:
				return fmt.Errorf("expected number, got %s", t.Kind())
			}
			load := loadMsg{key: key, kind: typ, mem: mem}
			if cmd, ok := sizeCommands[typ]; ok {
				s.read(load, cmd)
			} else {
				s.fold(load)
			}
			return nil
		})
	}
}

func (s *statScanner) read(load loadMsg, cmd string) {
	s.bulk.Send([][]byte{[]byte(cmd), load.key}, func(t resp.Token, _ resp.Decoder) error {
		if t.Kind() == resp.KindError {
			return t.Err()
		}
		if t.Kind() != resp.KindNumber {
			return fmt.Errorf("expected number, got %s", t.Kind())
		}
		load.len, load.hasLen = t.Int(), true
		s.fold(load)
		return nil
	})
}

func (s *statScanner) fold(load loadMsg) {
	s.stats.add(load)
	s.loadN++
	percent := 1.0
	if s.loadN < s.dbSize {
		percent = float64(s.loadN) / float64(s.dbSize)
	}
	if time.Now().Add(-time.Second).After(s.lastFold) {
		s.lastFold = time.Now()
		s.send(reportMsg{percent: percent, report: s.stats.report(), cursor: s.bulk.Cursor()})
	}
}

// statModel renders the progress of a stat walk and the report of the
// finished one. The keyspace is read by a [statScanner] on its own
// goroutine, the model holds no connection and only displays what the
// program delivers.
type statModel struct {
	progress progress.Model
	viewport viewport.Model

	renderer *glamour.TermRenderer

	ready bool
}

func (m statModel) Init() tea.Cmd {
	return nil
}

func (m statModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.progress.SetWidth(msg.Width)
		if !m.ready {
			m.ready = true
			m.viewport = viewport.New(
				viewport.WithWidth(msg.Width),
				viewport.WithHeight(msg.Height-1),
			)
			m.viewport.YPosition = 1
			m.viewport.SetContent(renderReport(m.renderer, reportMsg{}.reportView(reportSectionLive)))
		} else {
			m.viewport.SetWidth(msg.Width)
			m.viewport.SetHeight(msg.Height - 1)
		}
	case errorMsg:
		return m, tea.Sequence(
			tea.Println(msg),
			tea.Quit,
		)
	case reportMsg:
		m.viewport.SetContent(renderReport(m.renderer, msg.reportView(reportSectionLive)))
		return m, m.progress.SetPercent(msg.percent)
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "ctrl+d":
			return m, tea.Quit
		}
	case progress.FrameMsg:
		var cmd tea.Cmd
		m.progress, cmd = m.progress.Update(msg)
		return m, cmd
	}

	var cmd tea.Cmd
	m.viewport, cmd = m.viewport.Update(msg)
	return m, cmd
}

func (m statModel) View() tea.View {
	var sb strings.Builder
	sb.WriteString(m.progress.View())
	sb.WriteString("\n")
	sb.WriteString(m.viewport.View())
	v := tea.NewView(sb.String())
	v.AltScreen = true
	v.MouseMode = tea.MouseModeCellMotion
	return v
}

// quoteKey renders a key the way redis-cli does, with C style escapes.
func quoteKey(key string) string {
	return strconv.Quote(key)
}
