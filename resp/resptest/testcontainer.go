package resptest

import (
	"fmt"
	"io"
	"log"
	"net/url"
	"sync"
	"testing"

	"github.com/coding-socks/rdb/resp"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func RunTestcontainer(t *testing.T) *Container {
	server, err := testcontainers.Run(t.Context(), "redis:latest",
		testcontainers.WithLogger(log.New(io.Discard, "", 0)),
		testcontainers.WithExposedPorts("6379/tcp"),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("6379/tcp")),
	)
	if err != nil {
		t.Fatalf("starting the redis container: %v", err)
	}
	testcontainers.CleanupContainer(t, server)

	c := &Container{server: server, t: t}
	t.Cleanup(func() { c.Close() })
	return c
}

type Container struct {
	server *testcontainers.DockerContainer

	t       testing.TB     // the test the server serves for
	mu      sync.Mutex     // guards closed and clients
	closed  bool           // the server was closed
	clients []*resp.Client // the connections the clients of the test dialed
}

func (c *Container) Client() *resp.Client {
	port, _ := c.server.MappedPort(c.t.Context(), "6379/tcp")

	u, err := url.Parse(fmt.Sprintf("redis://127.0.0.1:%s", port))
	if err != nil {
		c.t.Fatal(err)
	}
	conn, err := resp.Dial(c.t.Context(), u, 3)
	if err != nil {
		c.t.Fatal(err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.clients = append(c.clients, conn)
	return conn
}

func (c *Container) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	clients := c.clients
	c.clients = nil
	c.mu.Unlock()
	for _, c := range clients {
		c.Close()
	}
	return nil
}
