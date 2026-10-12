package commands

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
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

// transferSchemes are the url schemes of a Redis compatible server,
// their s variants connect over TLS.
var transferSchemes = map[string]bool{
	"redis":   true,
	"rediss":  true,
	"valkey":  true,
	"valkeys": true,
	"keydb":   true,
	"keydbs":  true,
}

func Transfer(ctx context.Context, args []string) error {
	const transferUsage = `usage: rdbc transfer [-include GLOB] [-exclude GLOB] SRC [SRC_FLAGS] DST [DST_FLAGS]

Transfers the keys between files and Redis compatible servers, KeyDB
and Valkey included.

SRC and DST are a bare name, which reads stdin or writes stdout, a
file path whose extension selects the strategy (.rdb, .aof, .json,
.manifest.tar, .manifest.tar.gz or .manifest.tgz), or the url of a
server of the form redis://user:pass@host:port. The s variants
rediss://, keydbs:// and valkeys:// connect over TLS, the query
parameter insecure=true skips the verification of the TLS
certificate of the server, the user parameters execute the AUTH
command.

The keys of the transfer pass the filters: a key is transferred when
it matches the include glob and no exclude glob, a command without a
key is always transferred. A transfer of mixed dialects is rejected.
`
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
	if closeSrc != nil {
		defer closeSrc.Close()
	}
	d, closeDst, err := dst.Make(ctx)
	if err != nil {
		return err
	}
	if closeDst != nil {
		defer closeDst.Close()
	}

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
		return fmt.Sprintf("%s %d/%d %s", m.progress[i].View(), r.I, r.N, r.Unit)
	}
	if r.I != 0 {
		return fmt.Sprintf("%s %d %s", m.spinner.View(), r.I, r.Unit)
	}
	return m.spinner.View()
}

// srcMaker opens the source the argument of the command line
// selects.
type srcMaker interface {
	Make(ctx context.Context) (chain.Src, io.Closer, error)
}

// srcCmd is the source of a transfer: the maker the argument of the
// command line selects, the kinds of the chain the source carries and
// the flags that follow it. Make opens the source and returns it with
// the resource it opened, nil when the source reads stdin. The caller
// closes the resource when the transfer ended.
type srcCmd struct {
	srcMaker
	kinds chain.Kind
	rest  []string // the arguments of the destination
}

// Kinds returns the kinds of the chain the source carries.
func (s srcCmd) Kinds() chain.Kind { return s.kinds }

// Args returns the arguments that follow the source: the argument and
// the flags of the destination.
func (s srcCmd) Args() []string { return s.rest }

// serverSrc dials the cluster of a server url, the dialect is the one
// the scheme of the url names.
type serverSrc struct {
	url    *url.URL
	dialer dialer
}

// serverSyncSrc reads the RDB payload of the SYNC command of every
// node of the cluster.
type serverSyncSrc struct{ serverSrc }

func (s serverSyncSrc) Make(ctx context.Context) (chain.Src, io.Closer, error) {
	c, err := s.dialer.Dial(ctx, s.url, 3)
	if err != nil {
		return nil, nil, err
	}
	return chain.NewSyncServerSrc(c), c, nil
}

// serverDumpSrc scans the keyspace of every node of the cluster, one
// key with the TYPE, PTTL and DUMP commands.
type serverDumpSrc struct {
	serverSrc
	keyspace bool
}

func (s serverDumpSrc) Make(ctx context.Context) (chain.Src, io.Closer, error) {
	c, err := s.dialer.Dial(ctx, s.url, 3)
	if err != nil {
		return nil, nil, err
	}
	if s.keyspace {
		return chain.NewDumpServerSrc(c, chain.WithKeyspace()), c, nil
	}
	return chain.NewDumpServerSrc(c), c, nil
}

// serverAOFSrc reads every key of the keyspace with the read command
// of its type.
type serverAOFSrc struct {
	serverSrc
	keyspace bool
}

func (s serverAOFSrc) Make(ctx context.Context) (chain.Src, io.Closer, error) {
	c, err := s.dialer.Dial(ctx, s.url, 3)
	if err != nil {
		return nil, nil, err
	}
	if s.keyspace {
		return chain.NewAOFServerSrc(c, chain.WithKeyspace()), c, nil
	}
	return chain.NewAOFServerSrc(c), c, nil
}

// fileSrc opens the file of a source, the bare names read stdin.
type fileSrc struct {
	file    string // empty reads stdin
	dialect rdb.Dialect
}

// open reads the file of the source, os.Stdin when the name is bare.
func (s fileSrc) open() (io.Reader, io.Closer, error) {
	if s.file == "" {
		return os.Stdin, nil, nil
	}
	f, err := os.Open(s.file)
	if err != nil {
		return nil, nil, err
	}
	return f, f, nil
}

// rdbFileSrc decodes the records of an RDB file.
type rdbFileSrc struct{ fileSrc }

func (s rdbFileSrc) Make(context.Context) (chain.Src, io.Closer, error) {
	r, closer, err := s.open()
	if err != nil {
		return nil, nil, err
	}
	return chain.NewRDBFileSrc(r, s.dialect), closer, nil
}

// aofFileSrc decodes the commands of an AOF file.
type aofFileSrc struct{ fileSrc }

func (s aofFileSrc) Make(context.Context) (chain.Src, io.Closer, error) {
	r, closer, err := s.open()
	if err != nil {
		return nil, nil, err
	}
	return chain.NewAOFFileSrc(r, s.dialect), closer, nil
}

// manifestFileSrc decodes the base snapshot and the incremental
// commands of a manifest archive.
type manifestFileSrc struct{ fileSrc }

func (s manifestFileSrc) Make(context.Context) (chain.Src, io.Closer, error) {
	r, closer, err := s.open()
	if err != nil {
		return nil, nil, err
	}
	return chain.NewManifestSrc(r, s.dialect), closer, nil
}

type dialer interface {
	Dial(ctx context.Context, u *url.URL, p int) (*clusters.Cluster, error)
}

type dialerSingle struct{}

func (s dialerSingle) Dial(ctx context.Context, u *url.URL, p int) (*clusters.Cluster, error) {
	c, err := resp.Dial(ctx, u, 3)
	if err != nil {
		return nil, err
	}
	return clusters.Single(c), nil
}

type dialerCluster struct{}

func (c dialerCluster) Dial(ctx context.Context, u *url.URL, p int) (*clusters.Cluster, error) {
	return clusters.Dial(ctx, u, 3)
}

// parseSrcCmd sets the maker the argument selects and reads the flags
// that follow it: a server url reads its keys the way its -strategy
// flag names (sync, dump or aof, the scan strategies with +keyspace
// follow the keyspace event notifications of the server), its -cluster
// flag reads
// the keyspace of every primary shard of the cluster of the server,
// the dialect of a server source is the one the scheme of the url
// names, the rdb, aof and manifest strategies take their own
// -dialect flag. The bare names read stdin, the extension of a path
// selects the strategy of a file.
func parseSrcCmd(arg string, args []string) (srcCmd, error) {
	var s srcCmd
	if u, err := url.Parse(arg); err == nil && transferSchemes[u.Scheme] {
		fs := flag.NewFlagSet("rdbc transfer source", flag.ContinueOnError)
		var (
			strategy    string
			clusterFlag bool
		)
		fs.StringVar(&strategy, "strategy", "sync", "how the keys of the server are read: sync (the RDB payload of the SYNC command), dump (the SCAN and the DUMP commands) or aof (the read command of the type of every key); +keyspace follows the keyspace event notifications of the server while the scan runs")
		fs.BoolVar(&clusterFlag, "cluster", false, "read the keyspace of every primary shard of the cluster of the server")
		if err := fs.Parse(args); err != nil {
			return s, err
		}
		s.rest = fs.Args()
		v := serverSrc{url: u, dialer: dialerSingle{}}
		if clusterFlag {
			v.dialer = dialerCluster{}
		}

		switch strategy {
		case "sync":
			s.srcMaker, s.kinds = serverSyncSrc{v}, chain.KindRDB
		case "dump":
			s.srcMaker, s.kinds = serverDumpSrc{serverSrc: v}, chain.KindDump
		case "dump+keyspace":
			s.srcMaker, s.kinds = serverDumpSrc{serverSrc: v, keyspace: true}, chain.KindDump|chain.KindAOF
		case "aof":
			s.srcMaker, s.kinds = serverAOFSrc{serverSrc: v}, chain.KindAOF
		case "aof+keyspace":
			s.srcMaker, s.kinds = serverAOFSrc{serverSrc: v, keyspace: true}, chain.KindAOF
		default:
			return s, fmt.Errorf("rdbc transfer: invalid strategy %q of the source", strategy)
		}
		return s, nil
	}
	fs := flag.NewFlagSet("rdbc transfer source", flag.ContinueOnError)
	var own dialectFlag
	switch {
	case arg == "rdb", strings.HasSuffix(arg, ".rdb"):
		s.kinds = chain.KindRDB
		fs.TextVar(&own, "dialect", dialectFlag{}, "the dialect of the file (redis, valkey, keydb)")
	case arg == "aof", strings.HasSuffix(arg, ".aof"):
		s.kinds = chain.KindAOF
		fs.TextVar(&own, "dialect", dialectFlag{}, "the dialect of the AOF file (redis, valkey, keydb)")
	case arg == "manifest", strings.HasSuffix(arg, ".manifest.tar"),
		strings.HasSuffix(arg, ".manifest.tar.gz"),
		strings.HasSuffix(arg, ".manifest.tgz"):
		s.kinds = chain.KindRDB | chain.KindAOF
		fs.TextVar(&own, "dialect", dialectFlag{}, "the dialect of the base snapshot (redis, valkey, keydb)")
	default:
		return s, fmt.Errorf("rdbc transfer: invalid source %q", arg)
	}
	if err := fs.Parse(args); err != nil {
		return s, err
	}
	f := fileSrc{dialect: own.dialect}
	switch arg {
	case "rdb", "aof", "manifest":
		// The bare name reads stdin.
	default:
		f.file = arg
	}
	switch {
	case arg == "rdb", strings.HasSuffix(arg, ".rdb"):
		s.srcMaker = rdbFileSrc{f}
	case arg == "aof", strings.HasSuffix(arg, ".aof"):
		s.srcMaker = aofFileSrc{f}
	default:
		s.srcMaker = manifestFileSrc{f}
	}
	s.rest = fs.Args()
	return s, nil
}

// dstMaker opens the destination the argument of the command line
// selects.
type dstMaker interface {
	Make(ctx context.Context) (chain.Dst, io.Closer, error)
}

// dstCmd is the destination of a transfer: the maker the argument of
// the command line selects, the kinds of the chain the destination
// takes and the flags that follow it. Make opens the destination and
// returns it with the resource it opened, nil when the destination
// writes stdout. The caller closes the resource when the transfer
// ended.
type dstCmd struct {
	dstMaker
	kinds chain.Kind
	rest  []string // the arguments left unparsed
}

// Kinds returns the kinds of the chain the destination takes.
func (d dstCmd) Kinds() chain.Kind { return d.kinds }

// Args returns the arguments the destination left unparsed.
func (d dstCmd) Args() []string { return d.rest }

// serverDst stores the keys of the stream on the nodes of the cluster
// of the server.
type serverDst struct {
	url    *url.URL
	dialer dialer
	batch  uint
}

func (d serverDst) Make(ctx context.Context) (chain.Dst, io.Closer, error) {
	c, err := d.dialer.Dial(ctx, d.url, 3)
	if err != nil {
		return nil, nil, err
	}
	dst := chain.NewServerDst(ctx, c)
	dst.SetBatchN(int(d.batch))
	return dst, c, nil
}

// fileDst creates the file of a destination, the bare names write
// stdout.
type fileDst struct {
	file string // empty writes stdout
}

// create writes the file of the destination, os.Stdout when the name
// is bare.
func (d fileDst) create() (io.Writer, io.Closer, error) {
	if d.file == "" {
		return os.Stdout, nil, nil
	}
	f, err := os.Create(d.file)
	if err != nil {
		return nil, nil, err
	}
	return f, f, nil
}

// jsonFileDst writes the stream as json.
type jsonFileDst struct{ fileDst }

func (d jsonFileDst) Make(context.Context) (chain.Dst, io.Closer, error) {
	out, closer, err := d.create()
	if err != nil {
		return nil, nil, err
	}
	return chain.NewJSONDst(out), closer, nil
}

// rdbFileDst encodes the records of an RDB file, the dialect is the
// one the head of the stream names.
type rdbFileDst struct{ fileDst }

func (d rdbFileDst) Make(context.Context) (chain.Dst, io.Closer, error) {
	out, closer, err := d.create()
	if err != nil {
		return nil, nil, err
	}
	return chain.NewRDBDst(out), closer, nil
}

// aofFileDst writes the commands of the stream.
type aofFileDst struct{ fileDst }

func (d aofFileDst) Make(context.Context) (chain.Dst, io.Closer, error) {
	out, closer, err := d.create()
	if err != nil {
		return nil, nil, err
	}
	return chain.NewAOFDst(out), closer, nil
}

// manifestFileDst writes the base snapshot and the incremental
// commands of the stream as a manifest archive.
type manifestFileDst struct{ fileDst }

func (d manifestFileDst) Make(context.Context) (chain.Dst, io.Closer, error) {
	out, closer, err := d.create()
	if err != nil {
		return nil, nil, err
	}
	return chain.NewManifestDst(out), closer, nil
}

// parseDstCmd sets the maker the argument selects and reads the flags
// that follow it: a server url stores the keys the way its -strategy
// flag names (dump, aof or dump+aof), its -batch flag sets the size of
// the batches, its -cluster flag stores every key on the primary shard
// that serves its slot, the dialect of a server destination is the one
// the scheme of the url names. The bare names write stdout, the
// extension of a path selects the strategy of a file, a file
// destination takes no flags of its own and leaves the flags that
// follow it unparsed.
func parseDstCmd(arg string, args []string) (dstCmd, error) {
	var d dstCmd
	if u, err := url.Parse(arg); err == nil && transferSchemes[u.Scheme] {
		fs := flag.NewFlagSet("rdbc transfer destination", flag.ContinueOnError)
		var (
			strategy    string
			batch       uint
			clusterFlag bool
		)
		fs.StringVar(&strategy, "strategy", "dump", "how the keys are stored: dump (the RESTORE of the DUMP payloads), aof (the store commands of an AOF stream) or dump+aof (both)")
		fs.UintVar(&batch, "batch", 500, "commands to send to a server before reading the replies")
		fs.BoolVar(&clusterFlag, "cluster", false, "store every key on the primary shard of the cluster that serves its slot")
		if err := fs.Parse(args); err != nil {
			return d, err
		}
		switch strategy {
		case "dump":
			d.kinds = chain.KindDump
		case "aof":
			d.kinds = chain.KindAOF
		case "dump+aof":
			d.kinds = chain.KindDump | chain.KindAOF
		default:
			return d, fmt.Errorf("rdbc transfer: invalid strategy %q", strategy)
		}
		if batch == 0 {
			batch = 500
		}
		v := serverDst{url: u, batch: batch, dialer: dialerSingle{}}
		if clusterFlag {
			v.dialer = dialerCluster{}
		}
		d.dstMaker = v
		d.rest = fs.Args()
		return d, nil
	}
	f := fileDst{}
	switch {
	case arg == "json", strings.HasSuffix(arg, ".json"):
		d.kinds = chain.KindJSON
	case arg == "aof", strings.HasSuffix(arg, ".aof"):
		d.kinds = chain.KindAOF
	case arg == "rdb", strings.HasSuffix(arg, ".rdb"):
		d.kinds = chain.KindRDB
	case arg == "manifest", strings.HasSuffix(arg, ".manifest.tar"),
		strings.HasSuffix(arg, ".manifest.tar.gz"),
		strings.HasSuffix(arg, ".manifest.tgz"):
		d.kinds = chain.KindRDB | chain.KindAOF
	default:
		return d, fmt.Errorf("rdbc transfer: invalid destination %q", arg)
	}
	switch arg {
	case "json", "aof", "rdb", "manifest":
		// The bare name writes stdout.
	default:
		f.file = arg
	}
	switch {
	case arg == "json", strings.HasSuffix(arg, ".json"):
		d.dstMaker = jsonFileDst{f}
	case arg == "aof", strings.HasSuffix(arg, ".aof"):
		d.dstMaker = aofFileDst{f}
	case arg == "rdb", strings.HasSuffix(arg, ".rdb"):
		d.dstMaker = rdbFileDst{f}
	default:
		d.dstMaker = manifestFileDst{f}
	}
	d.rest = args
	return d, nil
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
func converterFor(src srcCmd, dst dstCmd) Converter {
	dkind := dst.Kinds()
	if dkind&chain.KindJSON > 0 {
		return ConverterJSON
	}
	if (src.Kinds() &^ dkind) == 0 {
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
