package main

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coding-socks/rdb"
	"github.com/coding-socks/rdb/resp"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// cliRunTimeout is the lifetime of one CLI run, the process is killed
// when it outlives it.
const cliRunTimeout = 30 * time.Second

var (
	cliPath  string // the rdbc binary the tests execute
	workDir  string
	coverDir string // set when the test run is coverage instrumented
)

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		fmt.Fprintln(os.Stderr, "skipping the CLI integration tests in short mode")
		os.Exit(0)
	}

	var err error
	workDir, err = os.MkdirTemp("", "rdbc-cli")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	cliPath = filepath.Join(workDir, "rdbc")
	buildArgs := []string{"build", "-o", cliPath}
	if testing.CoverMode() != "" {
		// The runs of an instrumented binary write their counters
		// to the cover directory.
		buildArgs = append(buildArgs, "-cover")
		coverDir = filepath.Join(workDir, "cover")
		if err := os.MkdirAll(coverDir, 0o755); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	build := exec.Command("go", buildArgs...)
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "building the rdbc binary: %v\n%s", err, out)
		os.Exit(1)
	}

	code := m.Run()

	if coverDir != "" {
		profile := filepath.Join(workDir, "rdbc.cover.out")
		cov := exec.Command("go", "tool", "covdata", "textfmt", "-i", coverDir, "-o", profile)
		if out, err := cov.CombinedOutput(); err != nil {
			fmt.Fprintf(os.Stderr, "converting the CLI coverage: %v\n%s", err, out)
		} else {
			fmt.Fprintf(os.Stderr, "rdbc CLI coverage profile: %s\n", profile)
		}
	}
	os.RemoveAll(workDir)
	os.Exit(code)
}

// runCLI executes the rdbc binary with the arguments and the stdin
// input, it returns its stdout and stderr output. The process is
// killed when it outlives the run timeout.
func runCLI(t *testing.T, stdin string, args ...string) (string, string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), cliRunTimeout)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, cliPath, args...)
	cmd.WaitDelay = time.Second
	if coverDir != "" {
		cmd.Env = append(os.Environ(), "GOCOVERDIR="+coverDir)
	}
	cmd.Stdin = strings.NewReader(stdin)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	if ctx.Err() != nil {
		t.Fatalf("rdbc %s did not finish within %s", strings.Join(args, " "), cliRunTimeout)
	}
	return stdout.String(), stderr.String(), err
}

// fixtureRDB returns a minimal RDB file with the string key foo and
// the value bar, the dialect stamps the magic and the newest version
// it supports into the header, a nil dialect selects the newest
// redis one.
func fixtureRDB(t *testing.T, dialect rdb.Dialect) []byte {
	t.Helper()
	var buf bytes.Buffer
	e := rdb.NewFileEncoder(&buf, dialect)
	for _, tok := range []rdb.Token{
		rdb.Op(rdb.RedisOpCodeSelectDB),
		rdb.Uint(0),
		rdb.Op(rdb.RedisOpCodeString),
		rdb.Str("foo"),
		rdb.Str("bar"),
		rdb.Op(rdb.RedisOpCodeEndOfRDB),
	} {
		if err := e.WriteToken(tok); err != nil {
			t.Fatal(err)
		}
	}
	return buf.Bytes()
}

// fixtureAOF returns a minimal AOF file that sets foo to bar.
func fixtureAOF(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	e := resp.NewRESP3Encoder(&buf)
	write := func(args ...string) {
		if err := e.WriteToken(resp.ArrayLen(len(args))); err != nil {
			t.Fatal(err)
		}
		for _, arg := range args {
			if err := e.WriteToken(resp.Bytes([]byte(arg))); err != nil {
				t.Fatal(err)
			}
		}
	}
	write("SELECT", "0")
	write("SET", "foo", "bar")
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// writeFixture writes a fixture to a file of a directory of the test,
// it returns the path of the file.
func writeFixture(t *testing.T, name string, content []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// commandRecorder records the names of the commands the server read.
type commandRecorder struct {
	address string
	mu      sync.Mutex
	names   []string
}

// addr returns the network address of the server.
func (r *commandRecorder) addr() string {
	return r.address
}

// add records the name of one command.
func (r *commandRecorder) add(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.names = append(r.names, name)
}

// commands returns the name of every command the server read.
func (r *commandRecorder) commands() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.names)
}

// startMockServer starts a RESP server that replies +OK to every
// command of the handler, the reserved HELLO is served by the
// connection itself. It is closed at the end of the test.
func startMockServer(t *testing.T) *commandRecorder {
	t.Helper()
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	rec := &commandRecorder{address: ln.Addr().String()}
	srv := &resp.Server{
		Handler: resp.HandlerFunc(func(w resp.ResponseWriter, cmd *resp.Command) {
			rec.add(string(cmd.Args[0]))
			w.Proto().WriteToken(resp.String("OK"))
		}),
	}
	go srv.Serve(ln)
	t.Cleanup(func() { ln.Close() })
	return rec
}

// compareRoundTrip compares a re-encoded RDB file with its source.
// The re-encode writes the newest version into the header and carries
// a checksum, the source of a version 5 or newer file carries its own
// checksum, an older source carries none.
func compareRoundTrip(t *testing.T, got, want []byte) {
	t.Helper()
	if len(got) < 17 || len(want) < 9 {
		t.Fatalf("short RDB file: got %d bytes, want %d bytes", len(got), len(want))
	}
	got = got[9 : len(got)-8] // the header and the checksum of the re-encode
	src := want[9:]
	if v, err := strconv.Atoi(string(want[5:9])); err != nil || v >= 5 {
		src = src[:len(src)-8]
	}
	if !bytes.Equal(got, src) {
		t.Fatalf("round trip mismatch:\ngot  %x\nwant %x", got, src)
	}
}

func TestRDBC_client(t *testing.T) {
	srv := startMockServer(t)
	stdout, stderr, err := runCLI(t, "", "client", "redis://"+srv.addr(), "PING")
	if err != nil {
		t.Fatalf("client error = %v\nstdout: %s\nstderr: %s", err, stdout, stderr)
	}
	if want := "PONG\n"; stdout != want {
		t.Errorf("client wrote %q, want %q", stdout, want)
	}
}

// TestCLI_client_hangIsKilled verifies that a CLI run is terminated
// when it outlives the timeout of the test: an interactive session
// with an input that never ends is killed by the context of the run.
func TestRDBC_client_hangIsKilled(t *testing.T) {
	srv := startMockServer(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, cliPath, "client", "redis://"+srv.addr())
	cmd.WaitDelay = time.Second
	if coverDir != "" {
		cmd.Env = append(os.Environ(), "GOCOVERDIR="+coverDir)
	}
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		cmd.Wait()
		stdin.Close()
	})
	// The session idles until the context of the run kills it.
	if err := cmd.Wait(); ctx.Err() == nil {
		t.Fatalf("the idle session exited on its own: %v", err)
	}
}

func TestRDBC_transfer(t *testing.T) {
	t.Run("upload rdb", func(t *testing.T) {
		srv := startMockServer(t)
		path := writeFixture(t, "fixture.rdb", fixtureRDB(t, rdb.RedisDialect{}))
		_, stderr, err := runCLI(t, "", "transfer", path, "redis://"+srv.addr())
		if err != nil {
			t.Fatalf("transfer error = %v\nstderr: %s", err, stderr)
		}
		got := srv.commands()
		// The conversion to the keys of a scan carries no SELECT,
		// the keys of a scan live in the database the connection
		// serves.
		if !slices.Contains(got, "RESTORE") {
			t.Errorf("the server read %v, want it to contain RESTORE", got)
		}
	})

	// A server destination takes the keys of a scan only, the
	// commands of an AOF file have no transfer to a server.
	t.Run("upload aof", func(t *testing.T) {
		srv := startMockServer(t)
		path := writeFixture(t, "fixture.aof", fixtureAOF(t))
		if _, _, err := runCLI(t, "", "transfer", path, "redis://"+srv.addr()); err == nil {
			t.Error("the transfer of an AOF file to a server did not fail")
		}
	})

	// The download reads the RDB file from a serve run of the CLI
	// itself: the replica handshake of the transfer is answered by
	// serve.
	t.Run("download serve", func(t *testing.T) {
		path := writeFixture(t, "fixture.rdb", fixtureRDB(t, rdb.RedisDialect{}))
		ctx, cancel := context.WithTimeout(t.Context(), cliRunTimeout)
		defer cancel()
		serve := exec.CommandContext(ctx, cliPath, "serve", path, ":0")
		if coverDir != "" {
			serve.Env = append(os.Environ(), "GOCOVERDIR="+coverDir)
		}
		stdoutPipe, err := serve.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		if err := serve.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			cancel()
			serve.Wait()
		})

		// The address of the listener is on the stderr of the serve run.
		lines := make(chan string, 8)
		go func() {
			sc := bufio.NewScanner(stdoutPipe)
			for sc.Scan() {
				lines <- sc.Text()
			}
		}()
		var addr string
		for addr == "" {
			select {
			case line := <-lines:
				if i := strings.LastIndex(line, "addr="); i >= 0 {
					addr = strings.TrimSpace(line[i+5:])
				}
			case <-time.After(5 * time.Second):
				t.Fatal("serve did not report its address")
			}
		}

		stdout, stderr, err := runCLI(t, "", "transfer", "redis://"+addr, "rdb")
		if err != nil {
			t.Fatalf("transfer error = %v\nstderr: %s", err, stderr)
		}
		compareRoundTrip(t, []byte(stdout), fixtureRDB(t, rdb.RedisDialect{}))
	})
}

// serverImages are the Redis compatible servers the CLI is tested
// against, they run in Docker containers.
var serverImages = []struct {
	name    string
	image   string
	command []string
	scheme  string      // the url scheme names the dialect of the server
	magic   string      // the magic of the RDB files the dialect writes
	dialect rdb.Dialect // the dialect the fixtures are written in
}{
	{
		name:    "redis",
		image:   "redis:8",
		scheme:  "redis",
		magic:   "REDIS",
		dialect: rdb.RedisDialect{},
	},
	{
		name:    "valkey",
		image:   "valkey/valkey:latest",
		scheme:  "valkey",
		magic:   "VALKEY",
		dialect: rdb.ValkeyDialect{},
	},
	{
		name:    "keydb",
		image:   "eqalpha/keydb:latest",
		scheme:  "keydb",
		magic:   "REDIS",
		dialect: rdb.KeyDBDialect{},
	},
}

// startServerContainer runs a server image in a Docker container on a
// free local port, the container is removed at the end of the test.
func startServerContainer(t *testing.T, name, image string) int {
	t.Helper()
	server, err := testcontainers.Run(t.Context(), image,
		testcontainers.WithLogger(log.New(io.Discard, "", 0)),
		testcontainers.WithName(fmt.Sprintf("rdbc-cli-%s-%d", name, time.Now().UnixNano())),
		testcontainers.WithExposedPorts("6379/tcp"),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("6379/tcp")),
	)
	if err != nil {
		t.Fatalf("starting the %s container: %v", name, err)
	}
	testcontainers.CleanupContainer(t, server)
	port, err := server.MappedPort(t.Context(), "6379/tcp")
	if err != nil {
		t.Fatal(err)
	}
	return int(port.Num())
}

// waitReady retries the PING of the client command until the server at
// the url answers or the deadline passes.
func waitReady(t *testing.T, name, url string) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		_, _, err := runCLI(t, "", "client", url, "PING")
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s at %s did not become ready: %v", name, url, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// TestRDBC_server runs the CLI against a real server in a Docker
// container, the keys of a fixture are stored with a transfer to the
// server url and the replica transfer from the server reads them back.
// A real server serializes the keys in its own encoding, the download
// is compared as json, not with the bytes of the fixture. The
// containers need Docker, the test is skipped when it is not usable.
func TestRDBC_server(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)

	for _, srv := range serverImages {
		t.Run(srv.name, func(t *testing.T) {
			path := writeFixture(t, "fixture.rdb", fixtureRDB(t, srv.dialect))
			port := startServerContainer(t, srv.name, srv.image)
			url := fmt.Sprintf("%s://127.0.0.1:%d", srv.scheme, port)
			waitReady(t, srv.name, url)

			stdout, stderr, err := runCLI(t, "", "client", url, "PING")
			if err != nil {
				t.Fatalf("client error = %v\nstdout: %s\nstderr: %s", err, stdout, stderr)
			}
			if want := "PONG\n"; stdout != want {
				t.Errorf("client wrote %q, want %q", stdout, want)
			}

			if _, stderr, err := runCLI(t, "", "transfer", "-include", "nomatch:*", path, "-dialect", srv.scheme, url); err != nil {
				t.Fatalf("transfer filtered upload error = %v\nstderr: %s", err, stderr)
			}
			stdout, stderr, err = runCLI(t, "", "transfer", url, "json")
			if err != nil {
				t.Fatalf("filtered download error = %v\nstderr: %s", err, stderr)
			}
			if !strings.Contains(stdout, "[]") {
				t.Errorf("the filtered transfer stored keys: %s", stdout)
			}

			if _, stderr, err := runCLI(t, "", "transfer", path, "-dialect", srv.scheme, url); err != nil {
				t.Fatalf("transfer upload error = %v\nstderr: %s", err, stderr)
			}
			stdout, stderr, err = runCLI(t, "", "transfer", url, "rdb")
			if err != nil {
				t.Fatalf("transfer download error = %v\nstderr: %s", err, stderr)
			}

			// The download is an RDB file with the keys of the
			// server, the json of the file carries the key and the
			// value of the fixture.
			stdout, stderr, err = runCLI(t, stdout, "transfer", "rdb", "-dialect", srv.scheme, "json")
			if err != nil {
				t.Fatalf("open error = %v\nstderr: %s", err, stderr)
			}
			for _, want := range []string{"foo", "bar"} {
				if !strings.Contains(stdout, want) {
					t.Errorf("open wrote %q, want it to contain %q", stdout, want)
				}
			}

			// The dump strategy reads every key with the DUMP command, it
			// synthesizes an RDB file in the dialect of the server.
			stdout, stderr, err = runCLI(t, "", "transfer", url, "-strategy", "dump", "rdb")
			if err != nil {
				t.Fatalf("transfer dump error = %v\nstderr: %s", err, stderr)
			}
			if !strings.HasPrefix(stdout, srv.magic) || len(stdout) <= len(srv.magic)+4 {
				t.Errorf("transfer dump wrote %d bytes, want an RDB file of the %s dialect", len(stdout), srv.scheme)
			}

			// The aof strategy reads the value with the GET command
			// of the type of the key, the commands of the aof stream
			// store it again.
			stdout, stderr, err = runCLI(t, "", "transfer", url, "-strategy", "aof", "aof")
			if err != nil {
				t.Fatalf("transfer aof error = %v\nstderr: %s", err, stderr)
			}
			if !strings.Contains(stdout, "APPEND") || !strings.Contains(stdout, "foo") {
				t.Errorf("transfer aof wrote %q, want it to store foo", stdout)
			}
		})
	}
}
