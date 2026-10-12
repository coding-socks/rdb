// Command rdbc converts, serves and inspects RDB files and talks to
// Redis compatible servers.
//
// The commands are transfer, client, serve and stat, see their usage
// output for the flags they take.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/coding-socks/rdb/cmd/rdbc/internal/commands"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	flag.Usage = func() {
		fmt.Fprint(flag.CommandLine.Output(), `usage: rdbc <command> [flags] [args...]

commands:
  client    connect to a Redis compatible server and read/write to it
  transfer  transfer the keys between files and Redis compatible servers
  serve     serve an RDB file to connecting replicas
  stat      show size statistics of the keyspace of a server
`)
	}
	flag.Parse()

	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(2)
	}
	switch cmd := flag.Arg(0); cmd {
	case "client":
		if err := commands.Client(ctx, flag.Args()[1:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "transfer":
		if err := commands.Transfer(ctx, flag.Args()[1:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "serve":
		if err := commands.Serve(ctx, flag.Args()[1:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "stat":
		if err := commands.Stat(ctx, flag.Args()[1:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(flag.CommandLine.Output(), "unknown command %q\n", cmd)
		flag.Usage()
		os.Exit(2)
	}
}
