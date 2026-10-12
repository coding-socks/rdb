package resptest

import (
	"net"
	"strconv"
	"strings"
	"testing"

	"github.com/coding-socks/rdb"
	"github.com/coding-socks/rdb/resp"
)

// Shard describes one primary shard of a test cluster: the handler
// that serves the commands of its node and the slots the node serves.
type Shard struct {
	// ID is the name of the node the CLUSTER command reports.
	ID string
	// Slots are the ranges of the hash slots the node serves.
	Slots []rdb.SlotRange
	// Handler serves the commands of the node, a nil one replies to
	// every command with the error of an unknown command.
	Handler resp.Handler
}

// Cluster is a cluster of test servers, one per shard of
// [NewCluster]: every node answers the CLUSTER command with its own
// name and the topology of the cluster, the connection of any node
// answers the topology discovery of the clusters package.
type Cluster struct {
	shards []Shard
	nodes  []*Server
}

// NewCluster starts the cluster of the shards on local addresses of
// the test and returns it. The cleanup of the test closes the
// cluster.
func NewCluster(t testing.TB, shards ...Shard) *Cluster {
	t.Helper()
	c := &Cluster{shards: shards}
	for i := range shards {
		c.nodes = append(c.nodes, NewServer(t, c.nodeHandler(i)))
	}
	t.Cleanup(c.Close)
	return c
}

// Node returns the test server of the node with the id. It returns
// nil when the cluster carries no node of the name.
func (c *Cluster) Node(id string) *Server {
	for i, s := range c.shards {
		if s.ID == id {
			return c.nodes[i]
		}
	}
	return nil
}

// Nodes returns the test servers of the shards in the order the
// topology of the cluster names them.
func (c *Cluster) Nodes() []*Server {
	return append([]*Server(nil), c.nodes...)
}

// Close closes every node of the cluster.
func (c *Cluster) Close() {
	for _, n := range c.nodes {
		n.Close()
	}
}

// nodeHandler returns the handler of the node of the shard at i: the
// CLUSTER commands that name the topology of the cluster and the
// commands of the shard behind them.
func (c *Cluster) nodeHandler(i int) resp.Handler {
	shard := c.shards[i].Handler
	if shard == nil {
		shard = resp.NewServeMux()
	}
	return resp.HandlerFunc(func(w resp.ResponseWriter, cmd *resp.Command) {
		var tokens []resp.Token
		if len(cmd.Args) > 1 && strings.EqualFold(string(cmd.Args[0]), "CLUSTER") {
			switch strings.ToUpper(string(cmd.Args[1])) {
			case "MYID":
				tokens = []resp.Token{resp.Bytes([]byte(c.shards[i].ID))}
			case "SHARDS":
				tokens = c.shardsReply()
			case "SLOTS":
				tokens = c.slotsReply()
			}
		}
		if tokens == nil {
			shard.ServeRESP(w, cmd)
			return
		}
		for _, tok := range tokens {
			if err := w.Proto().WriteToken(tok); err != nil {
				return
			}
		}
	})
}

// shardsReply is the tokens of the CLUSTER SHARDS reply of the
// cluster: every shard names the ranges of its slots and the node
// that serves them.
func (c *Cluster) shardsReply() []resp.Token {
	tokens := []resp.Token{resp.ArrayLen(len(c.shards))}
	for i, s := range c.shards {
		tokens = append(tokens,
			resp.ArrayLen(4),
			resp.Bytes([]byte("slots")),
			resp.ArrayLen(2*len(s.Slots)),
		)
		for _, r := range s.Slots {
			tokens = append(tokens, resp.Int(int64(r.Start)), resp.Int(int64(r.End)))
		}
		tokens = append(tokens,
			resp.Bytes([]byte("nodes")),
			resp.ArrayLen(1),
		)
		tokens = append(tokens, c.nodeTokens(i)...)
	}
	return tokens
}

// nodeTokens is the tokens of the node of the shard at i of the
// CLUSTER SHARDS reply: its name, its address and its role.
func (c *Cluster) nodeTokens(i int) []resp.Token {
	host, port := hostPort(c.nodes[i].Addr)
	return []resp.Token{
		resp.ArrayLen(12),
		resp.Bytes([]byte("id")), resp.Bytes([]byte(c.shards[i].ID)),
		resp.Bytes([]byte("endpoint")), resp.Bytes([]byte(host)),
		resp.Bytes([]byte("ip")), resp.Bytes([]byte(host)),
		resp.Bytes([]byte("port")), resp.Int(port),
		resp.Bytes([]byte("role")), resp.Bytes([]byte("master")),
		resp.Bytes([]byte("health")), resp.Bytes([]byte("online")),
	}
}

// slotsReply is the tokens of the CLUSTER SLOTS reply of the
// cluster: an entry names the first and the last slot of a range and
// the node that serves it, the ranges of a node arrive in an entry
// each.
func (c *Cluster) slotsReply() []resp.Token {
	var n int
	for _, s := range c.shards {
		n += len(s.Slots)
	}
	tokens := []resp.Token{resp.ArrayLen(n)}
	for i, s := range c.shards {
		host, port := hostPort(c.nodes[i].Addr)
		for _, r := range s.Slots {
			tokens = append(tokens,
				resp.ArrayLen(3),
				resp.Int(int64(r.Start)),
				resp.Int(int64(r.End)),
				resp.ArrayLen(3),
				resp.Bytes([]byte(host)),
				resp.Int(port),
				resp.Bytes([]byte(c.shards[i].ID)),
			)
		}
	}
	return tokens
}

// hostPort returns the host and the port of the address a node
// announces.
func hostPort(addr string) (string, int64) {
	host, port, _ := net.SplitHostPort(addr)
	n, _ := strconv.ParseInt(port, 10, 64)
	return host, n
}
