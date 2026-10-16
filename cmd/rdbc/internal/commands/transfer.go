package commands

import (
	"compress/gzip"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"charm.land/bubbles/v2/progress"
	"charm.land/bubbles/v2/spinner"
	tea "charm.land/bubbletea/v2"
	"github.com/coding-socks/rdb"
	"github.com/coding-socks/rdb/clusters"
	"github.com/coding-socks/rdb/internal/chain"
	"github.com/coding-socks/rdb/resp"
	"golang.org/x/term"
)

func Transfer(ctx context.Context, args []string) error {
	const transferUsage = `usage: rdbc transfer [-include GLOB] [-exclude GLOB] SRC [SRC_FLAGS] {<SRC_FILE | SRC_URL>} DST [DST_FLAGS] {DST_FILE | DST_URL}

Transfers the keys between files and Redis compatible servers, KeyDB
and Valkey included.

source: SRC [SRC_FLAGS] {<SRC_FILE | SRC_URL>}
  rdb [-dialect DIALECT] SRC_FILE
  aof [-dialect DIALECT] SRC_FILE
  manifest [-dialect DIALECT] SRC_FILE
  server+sync [-cluster] SRC_URL
  server+dump [-cluster] [-keyspace] SRC_URL
  server+aof [-cluster] [-keyspace] SRC_URL

destination: DST [DST_FLAGS] {DST_FILE | DST_URL}
  rdb DST_FILE
  aof DST_FILE
  json DST_FILE
  manifest DST_FILE
  server [-cluster] [-batch BATCH] DST_URL
  server+restore [-cluster] [-batch BATCH] DST_URL
  server+aof [-cluster] [-batch BATCH] DST_URL

The keys of the transfer can be filtered. The added filters use
github.com/gobwas/glob to parse GLOB. By default all keys are included.

SRC_FILE and DST_FILE accept a file path or ` + "`-`" + ` for stdin/stdout.

SRC_URL and DST_URL accept a url in redis://user:pass@host:port form.
The s variants rediss://, keydbs:// and valkeys:// connect over TLS,
the query parameter insecure=true skips the verification of the TLS
certificate of the server, the user parameters execute the AUTH
command.

For compatibility, some types can be converted to other types.
╔═══════════════╦══════╤═══════╤══════╤══════════╤════════════════╤════════════╤════════╗
║ SRC ↓ / DST → ║ rdb  │ json  │ aof  │ manifest │ server+restore │ server+aof │ server ║
╠═══════════════╬══════╪═══════╪══════╪══════════╪════════════════╪════════════╪════════╣
║ rdb           ║ ✓    │ ⟶json │ ⟶aof │ ✓        │ ⟶dump          │ ⟶aof       │ ⟶dump  ║
║ aof           ║ ✗    │ ✗     │ ✓    │ ✗        │ ✗              │ ✓          │ ✓      ║
║ manifest      ║ ✗    │ ✗     │ ⟶aof │ ✓        │ ✗              │ ⟶aof       │ ⟶dump  ║
║ server+sync   ║ ✓    │ ⟶json │ ⟶aof │ ✓        │ ⟶dump          │ ⟶aof       │ ⟶dump  ║
║ server+dump   ║ ⟶rdb │ ⟶json │ ⟶aof │ ⟶rdb     │ ✓              │ ⟶aof       │ ✓      ║
║ └ keyspace    ║ ✗    │ ✗     │ ⟶aof │ ✓        │ ✗              │ ⟶aof       │ ✓.     ║
║ server+aof    ║ ✗    │ ✗     │ ✓    │ ✗        │ ✗              │ ✓          │ ✓.     ║
║ └ keyspace    ║ ✗    │ ✗     │ ✓    │ ✗        │ ✗              │ ✓          │ ✓.     ║
╚═══════════════╩══════╧═══════╧══════╧══════════╧════════════════╧════════════╧════════╝

example:
  rdbc transfer server+sync -cluster redis://127.0.0.1:6379 rdb /path/to/dump.rdb
  rdbc transfer -exclude 'user:*' rdb /path/to/dump.rdb json -
`
	// TODO: offer functionality for SNI, a trusted cert store,
	//  and client certificate authentication

	fs := flag.NewFlagSet("transfer", flag.ExitOnError)
	var include includeFlag
	var exclude excludeFlag
	fs.Var(&include, "include", "glob of the keys to transfer")
	fs.Var(&exclude, "exclude", "glob of the keys to skip")
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), transferUsage)
		fs.PrintDefaults()
	}
	fs.Parse(args)
	args = fs.Args()
	if len(args) < 2 {
		fs.Usage()
		return errors.New("rdbc transfer: missing source or destination")
	}

	src, err := parseSrcCmd(args[0], args[1:])
	if err != nil {
		return err
	}
	rest := src.Args()
	if len(rest) == 0 {
		fs.Usage()
		return errors.New("rdbc transfer: missing destination")
	}

	dst, err := parseDstCmd(rest[0], rest[1:])
	if err != nil {
		return err
	}
	if rest := dst.Args(); len(rest) > 0 {
		return fmt.Errorf("rdbc transfer: unexpected argument %q", rest[0])
	}

	conv := converterFor(src, dst)

	s, closeSrc, err := src.Make(ctx)
	if err != nil {
		return err
	}
	defer closeSrc.Close()
	d, closeDst, err := dst.Make(ctx)
	if err != nil {
		return err
	}
	defer closeDst.Close()

	if conv != ConverterNone {
		d = converters[conv](d)
	}
	if include.pattern != nil {
		d = chain.Filter(func(s string) bool {
			return include.pattern.Match(s)
		})(d)
	}
	if exclude.pattern != nil {
		d = chain.Filter(func(s string) bool {
			return !exclude.pattern.Match(s)
		})(d)
	}
	pr, _ := s.(chain.ProgressReporter)
	if !term.IsTerminal(int(os.Stderr.Fd())) {
		return chain.Run(ctx, s, d)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errs := make(chan error, 1)
	p := tea.NewProgram(newTransferModel(pr), tea.WithOutput(os.Stderr))
	go func() {
		errs <- chain.Run(ctx, s, d)
		// https://github.com/charmbracelet/bubbletea/issues/1590
		// When exiting too quickly, bubbletea produces garbage data.
		<-time.After(100 * time.Millisecond)
		p.Send(tea.Quit())
	}()
	_, err = p.Run()
	cancel()
	runErr := <-errs
	if err != nil && !errors.Is(err, tea.ErrProgramKilled) {
		return err
	}
	return runErr
}

// progressPeriod is the period the transfer model polls the progress
// of its source with.
const progressPeriod = 100 * time.Millisecond

// wave is the spinner of the transfer model, a window of the heights
// ▁▂▃▄▅▆▇█ that slides over their cycle.
var wave = spinner.Spinner{
	FPS: time.Second / 10,
	Frames: []string{
		"▁▂▃▄▅▆", "▂▃▄▅▆▇", "▃▄▅▆▇█", "▄▅▆▇█▇", "▅▆▇█▇▆",
		"▆▇█▇▆▅", "▇█▇▆▅▄", "█▇▆▅▄▃", "▇▆▅▄▃▂", "▆▅▄▃▂▁",
		"▅▄▃▂▁▂", "▄▃▂▁▂▃", "▃▂▁▂▃▄", "▂▁▂▃▄▅",
	},
}

// newTransferModel constructs the model of a transfer, the spinner
// of the unknown progress included.
func newTransferModel(pr chain.ProgressReporter) transferModel {
	return transferModel{
		spinner: spinner.New(spinner.WithSpinner(wave)),
		pr:      pr,
	}
}

// transferModel renders the progress of a transfer. The source it
// polls reports one reading per step of its work, a source without a
// reporter spins alone, the transfer runs behind the program and
// quits it when it is done.
type transferModel struct {
	progress []progress.Model
	spinner  spinner.Model
	pr       chain.ProgressReporter
	start    time.Time

	reports []chain.ProgressReport
}

type progressMsg struct {
	reports []chain.ProgressReport
}

func (m transferModel) Init() tea.Cmd {
	if m.pr == nil {
		return m.spinner.Tick
	}
	return pollProgress(m.pr)
}

func pollProgress(pr chain.ProgressReporter) tea.Cmd {
	return tea.Tick(progressPeriod, func(time.Time) tea.Msg {
		return progressMsg{reports: pr.Progress()}
	})
}

func (m transferModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.start = time.Now()
	case progressMsg:
		m.reports = msg.reports
		for len(m.progress) < len(m.reports) {
			m.progress = append(m.progress, progress.New(progress.WithDefaultBlend(),
				func(m *progress.Model) { m.PercentFormat = " %5.1f%%" }))
		}
		cmds := []tea.Cmd{pollProgress(m.pr), m.spinner.Tick}
		for i := range m.reports {
			if m.reports[i].N == 0 {
				continue
			}
			if p := float64(m.reports[i].I) / float64(m.reports[i].N); m.progress[i].Percent() != p {
				cmds = append(cmds, m.progress[i].SetPercent(p))
			}
		}
		return m, tea.Batch(cmds...)
	case progress.FrameMsg:
		var cmds []tea.Cmd
		for i := range m.progress {
			var cmd tea.Cmd
			m.progress[i], cmd = m.progress[i].Update(msg)
			cmds = append(cmds, cmd)
		}
		return m, tea.Batch(cmds...)
	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "ctrl+d":
			return m, tea.Quit
		}
	}
	return m, nil
}

func (m transferModel) View() tea.View {
	if m.pr == nil || len(m.reports) == 0 {
		return tea.NewView(m.spinner.View())
	}
	var b strings.Builder
	for i := range m.reports {
		if i != 0 {
			b.WriteByte('\n')
		}
		b.WriteString(m.stepView(i))
	}
	return tea.NewView(b.String())
}

// stepView renders one step of the progress, a step whose total is
// unknown spins, the spinner of a step carries its I units, the bar
// of a step carries its I of N units.
func (m transferModel) stepView(i int) string {
	r := m.reports[i]
	if r.N != 0 {
		return fmt.Sprintf("%s %d/%d %s, elapsed %.fs", m.progress[i].View(), r.I, r.N, r.Unit, time.Since(m.start).Seconds())
	}
	if r.I != 0 {
		return fmt.Sprintf("%s %d %s, elapsed %.fs", m.spinner.View(), r.I, r.Unit, time.Since(m.start).Seconds())
	}
	return fmt.Sprintf("%s elapsed %.fs", m.spinner.View(), time.Since(m.start).Seconds())
}

type dialer interface {
	Dial(ctx context.Context, p int) (*clusters.Cluster, error)
}

type dialerSingle struct{ u *url.URL }

func (s dialerSingle) Dial(ctx context.Context, p int) (*clusters.Cluster, error) {
	c, err := resp.Dial(ctx, s.u, 3)
	if err != nil {
		return nil, err
	}
	return clusters.Single(c), nil
}

type dialerCluster struct{ u *url.URL }

func (c dialerCluster) Dial(ctx context.Context, p int) (*clusters.Cluster, error) {
	return clusters.Dial(ctx, c.u, p)
}

// srcMaker opens the source the argument of the command line
// selects.
type srcMaker interface {
	Kind() chain.Kind
	Args() []string
	Make(ctx context.Context) (chain.Src, io.Closer, error)
}

type srcServerMaker interface {
	Kind() chain.Kind
	Make(c *clusters.Cluster) chain.Src
}

type srcFileMaker interface {
	Kind() chain.Kind
	Make(r io.Reader, d rdb.Dialect) chain.Src
}

// srcServer dials the cluster of a server url, the dialect is the one
// the scheme of the url names.
type srcServer struct {
	dialer dialer
	args   []string
	m      srcServerMaker
}

func (s srcServer) Kind() chain.Kind { return s.m.Kind() }

func (s srcServer) Args() []string { return s.args }

func (s srcServer) Make(ctx context.Context) (chain.Src, io.Closer, error) {
	c, err := s.dialer.Dial(ctx, 3)
	if err != nil {
		return nil, nil, err
	}
	return s.m.Make(c), c, nil
}

// srcSyncServer reads the RDB payload of the SYNC command of every
// node of the cluster.
type srcSyncServer struct{}

func (s srcSyncServer) Kind() chain.Kind { return chain.KindRDB }

func (s srcSyncServer) Make(c *clusters.Cluster) chain.Src {
	return chain.NewSyncServerSrc(c)
}

// srcDumpServer scans the keyspace of every node of the cluster, one
// key with the PTTL and DUMP commands.
type srcDumpServer struct{ keyspace bool }

func (s srcDumpServer) Kind() chain.Kind {
	if s.keyspace {
		return chain.KindDump | chain.KindAOF
	}
	return chain.KindDump
}

func (s srcDumpServer) Make(c *clusters.Cluster) chain.Src {
	if s.keyspace {
		return chain.NewDumpServerSrc(c, chain.WithKeyspace())
	}
	return chain.NewDumpServerSrc(c)
}

// srcAOFServer reads every key of the keyspace with the read command
// of its type.
type srcAOFServer struct{ keyspace bool }

func (s srcAOFServer) Kind() chain.Kind { return chain.KindAOF }

func (s srcAOFServer) Make(c *clusters.Cluster) chain.Src {
	if s.keyspace {
		return chain.NewAOFServerSrc(c, chain.WithKeyspace())
	}
	return chain.NewAOFServerSrc(c)
}

// srcFile opens the file of a source, the bare names read stdin.
type srcFile struct {
	file    string
	args    []string
	dialect rdb.Dialect
	m       srcFileMaker
}

func (s srcFile) Kind() chain.Kind { return s.m.Kind() }

func (s srcFile) Args() []string { return s.args }

func (s srcFile) Make(ctx context.Context) (chain.Src, io.Closer, error) {
	if s.file == "-" {
		return s.m.Make(os.Stdin, s.dialect), io.NopCloser(os.Stdin), nil
	}
	f, err := os.Open(s.file)
	if err != nil {
		return nil, nil, err
	}
	return s.m.Make(f, s.dialect), f, nil
}

// srcRDBFile decodes the records of an RDB file.
type srcRDBFile struct{}

func (s srcRDBFile) Kind() chain.Kind { return chain.KindRDB }

func (s srcRDBFile) Make(r io.Reader, d rdb.Dialect) chain.Src {
	return chain.NewRDBFileSrc(r, d)
}

// srcAOFFile decodes the commands of an AOF file.
type srcAOFFile struct{}

func (s srcAOFFile) Kind() chain.Kind { return chain.KindAOF }

func (s srcAOFFile) Make(r io.Reader, d rdb.Dialect) chain.Src {
	return chain.NewAOFFileSrc(r, d)
}

// srcManifestFile decodes the base snapshot and the incremental
// commands of a manifest archive.
type srcManifestFile struct{}

func (s srcManifestFile) Kind() chain.Kind { return chain.KindRDB | chain.KindAOF }

func (s srcManifestFile) Make(r io.Reader, d rdb.Dialect) chain.Src {
	return chain.NewManifestSrc(r, d)
}

// parseSrcCmd parses the provided command and arguments. It returns
// a source maker, a builder of some sort, that initializes the source
// in a later step. It accepts rdb, aof, and manifest commands for
// a file source and server+sync, server+dump, and server+aof for
// server sources. Each command has their own args and flags.
func parseSrcCmd(cmd string, args []string) (srcMaker, error) {
	fs := flag.NewFlagSet("rdbc transfer source", flag.ContinueOnError)
	switch cmd {
	case "server+sync", "server+dump", "server+aof":
		var (
			clusterFlag bool
			keyspace    bool
		)
		fs.BoolVar(&clusterFlag, "cluster", false, "read the keyspace of every primary shard of the cluster of the server")
		switch cmd {
		case "server+dump", "server+aof":
			fs.BoolVar(&keyspace, "keyspace", false, "enable keyspace notifications during scan")
		}
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		if len(fs.Args()) == 0 {
			return nil, fmt.Errorf("rdbc transfer: missing source url")
		}
		u, err := url.Parse(fs.Arg(0))
		if err != nil {
			return nil, fmt.Errorf("rdbc transfer: invalid source url: %w", err)
		}
		s := srcServer{args: fs.Args()[1:], dialer: dialerSingle{u: u}}
		if clusterFlag {
			s.dialer = dialerCluster{u: u}
		}
		switch cmd {
		case "server+sync":
			s.m = srcSyncServer{}
		case "server+dump":
			s.m = srcDumpServer{keyspace: keyspace}
		case "server+aof":
			s.m = srcAOFServer{keyspace: keyspace}
		}
		return s, nil
	case "rdb", "aof", "manifest":
		var own dialectFlag
		fs.TextVar(&own, "dialect", dialectFlag{}, "the dialect of the file (redis, valkey, keydb)")
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		if len(fs.Args()) == 0 {
			return nil, fmt.Errorf("rdbc transfer: missing source file")
		}
		s := srcFile{args: fs.Args()[1:], file: fs.Arg(0), dialect: own.dialect}
		switch cmd {
		case "rdb":
			s.m = srcRDBFile{}
		case "aof":
			s.m = srcAOFFile{}
		case "manifest":
			s.m = srcManifestFile{}
		}
		return s, nil
	default:
		return nil, fmt.Errorf("rdbc transfer: invalid source %q", cmd)
	}

}

// dstMaker opens the destination the argument of the command line
// selects.
type dstMaker interface {
	Kind() chain.Kind
	Args() []string
	Make(ctx context.Context) (chain.Dst, io.Closer, error)
}

type dstFileMaker interface {
	Kind() chain.Kind
	Make(w io.Writer) chain.Dst
}

// dstServer stores the keys of the stream on the nodes of the cluster
// of the server.
type dstServer struct {
	dialer dialer
	kind   chain.Kind
	args   []string
	batch  uint
}

func (d dstServer) Kind() chain.Kind { return d.kind }

func (d dstServer) Args() []string { return d.args }

func (d dstServer) Make(ctx context.Context) (chain.Dst, io.Closer, error) {
	c, err := d.dialer.Dial(ctx, 3)
	if err != nil {
		return nil, nil, err
	}
	dst := chain.NewServerDst(ctx, c, d.kind)
	dst.SetBatchN(int(d.batch))
	return dst, c, nil
}

// dstFile creates the file of a destination, the bare names write
// stdout.
type dstFile struct {
	file string
	args []string
	m    dstFileMaker
}

func (d dstFile) Kind() chain.Kind { return d.m.Kind() }

func (d dstFile) Args() []string { return d.args }

type multiCloser struct{ a, b io.Closer }

func (m multiCloser) Close() error {
	return errors.Join(m.a.Close(), m.b.Close())
}

func (d dstFile) Make(ctx context.Context) (chain.Dst, io.Closer, error) {
	if d.file == "-" {
		return d.m.Make(os.Stdout), io.NopCloser(os.Stdout), nil
	}
	f, err := os.Create(d.file)
	if err != nil {
		return nil, nil, err
	}
	ext := filepath.Ext(d.file)
	if ext == ".gz" || ext == ".tgz" {
		w := gzip.NewWriter(f)
		return d.m.Make(w), multiCloser{w, f}, nil
	}
	return d.m.Make(f), f, nil
}

// dstJSONFile writes the stream as json.
type dstJSONFile struct{}

func (d dstJSONFile) Kind() chain.Kind { return chain.KindJSON }

func (d dstJSONFile) Make(w io.Writer) chain.Dst {
	return chain.NewJSONDst(w)
}

// dstRDBFile encodes the records of an RDB file, the dialect is the
// one the head of the stream names.
type dstRDBFile struct{}

func (d dstRDBFile) Kind() chain.Kind { return chain.KindRDB }

func (d dstRDBFile) Make(w io.Writer) chain.Dst {
	return chain.NewRDBDst(w)
}

// dstAOFFile writes the commands of the stream.
type dstAOFFile struct{}

func (d dstAOFFile) Kind() chain.Kind { return chain.KindAOF }

func (d dstAOFFile) Make(w io.Writer) chain.Dst {
	return chain.NewAOFDst(w)
}

// dstManifestFile writes the base snapshot and the incremental
// commands of the stream as a manifest archive.
type dstManifestFile struct{}

func (d dstManifestFile) Kind() chain.Kind { return chain.KindRDB | chain.KindAOF }

func (d dstManifestFile) Make(w io.Writer) chain.Dst {
	return chain.NewManifestDst(w)
}

// parseDstCmd parses the provided command and arguments. It returns
// a source maker, a builder of some sort, that initializes the source
// in a later step. It accepts rdb, aof, json, and manifest commands for
// a file source and server+restore, server+aof, and server for server
// sources. Each command has their own args and flags.
func parseDstCmd(cmd string, args []string) (dstMaker, error) {
	fs := flag.NewFlagSet("rdbc transfer destination", flag.ContinueOnError)
	switch cmd {
	case "server+restore", "server+aof", "server":
		var (
			batch       uint
			clusterFlag bool
		)
		fs.UintVar(&batch, "batch", 500, "commands to send to a server before reading the replies")
		fs.BoolVar(&clusterFlag, "cluster", false, "store every key on the primary shard of the cluster that serves its slot")
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		if len(fs.Args()) == 0 {
			return nil, fmt.Errorf("rdbc transfer: missing destination url")
		}
		u, err := url.Parse(fs.Arg(0))
		if err != nil {
			return nil, fmt.Errorf("rdbc transfer: invalid destination url: %w", err)
		}
		d := dstServer{args: fs.Args()[1:], dialer: dialerSingle{u: u}}
		if clusterFlag {
			d.dialer = dialerCluster{u: u}
		}

		d.batch = batch
		if d.batch == 0 {
			d.batch = 500
		}

		switch cmd {
		case "server+restore":
			d.kind = chain.KindDump
		case "server+aof":
			d.kind = chain.KindAOF
		case "server":
			d.kind = chain.KindDump | chain.KindAOF
		}
		return d, nil

	case "json", "aof", "rdb", "manifest":
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		if len(fs.Args()) == 0 {
			return nil, fmt.Errorf("rdbc transfer: missing destination file")
		}
		d := dstFile{file: fs.Arg(0), args: fs.Args()[1:]}
		switch cmd {
		case "json":
			d.m = dstJSONFile{}
		case "aof":
			d.m = dstAOFFile{}
		case "rdb":
			d.m = dstRDBFile{}
		case "manifest":
			d.m = dstManifestFile{}
		}
		return d, nil
	default:
		return nil, fmt.Errorf("rdbc transfer: invalid destination %q", cmd)
	}
}

const (
	ConverterNone Converter = iota
	ConverterRDB
	ConverterAOF
	ConverterJSON
	ConverterDump
)

type Converter uint8

func (c Converter) String() string {
	switch c {
	default:
		return "unknown"
	case ConverterNone:
		return "none"
	case ConverterRDB:
		return "rdb"
	case ConverterAOF:
		return "aof"
	case ConverterJSON:
		return "json"
	case ConverterDump:
		return "dump"
	}
}

var converters = map[Converter]chain.Middleware{
	ConverterRDB:  chain.ToRDB(),
	ConverterAOF:  chain.ToAOF(),
	ConverterJSON: chain.ToJSON(),
	ConverterDump: chain.ToDump(),
}

// converterFor returns the converter a transfer needs, none when the
// destination takes every kind the source carries and the converter
// of the kind the destination wants otherwise. The destination
// rejects the kinds nobody takes when the head arrives.
func converterFor(src srcMaker, dst dstMaker) Converter {
	dkind := dst.Kind()
	if dkind&chain.KindJSON > 0 {
		return ConverterJSON
	}
	if (src.Kind() &^ dkind) == 0 {
		return ConverterNone
	}
	switch {
	case dkind&chain.KindDump > 0:
		return ConverterDump
	case dkind&chain.KindRDB > 0:
		return ConverterRDB
	case dkind&chain.KindAOF > 0:
		return ConverterAOF
	}
	return ConverterNone
}
