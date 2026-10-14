package clusters

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/netip"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coding-socks/rdb"
	"github.com/coding-socks/rdb/resp"
	"github.com/coding-socks/rdb/resp/resptest"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func Test_KeySlot(t *testing.T) {
	// The slots of the keys are the ones the servers report, the
	// CLUSTER KEYSLOT of the spec examples among them.
	tests := []struct {
		name string
		key  string
		slot uint16
	}{
		{name: "foo", key: "foo", slot: 12182},
		{name: "bar", key: "bar", slot: 5061},
		{name: "hash tag", key: "{user1000}.following", slot: 3443},
		{name: "the same hash tag", key: "{user1000}.tofollow", slot: 3443},
		{name: "empty hash tag", key: "foo{}{bar}", slot: 8363},
		{name: "escaped hash tag", key: "foo{{bar}}zap", slot: 4015},
		{name: "empty key hash tag", key: "{}", slot: 15257},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if slot := KeySlot([]byte(tt.key)); slot != tt.slot {
				t.Errorf("KeySlot(%q) = %d, want %d", tt.key, slot, tt.slot)
			}
		})
	}
}

func TestNode_Contains(t *testing.T) {
	// A node serves the slots of every range it carries.
	n := Node{Slots: []rdb.SlotRange{{Start: 0, End: 5460}, {Start: 12000, End: 13000}}}
	if !n.Contains(0) || !n.Contains(5460) || !n.Contains(12182) {
		t.Error("Contains() named not a slot of the node")
	}
	if n.Contains(5461) || n.Contains(11999) || n.Contains(16383) {
		t.Error("Contains() named a slot of no range")
	}
}

func Test_crc16(t *testing.T) {
	// The check value of the CCITT variant the servers hash with.
	if crc := crc16([]byte("123456789")); crc != 0x31c3 {
		t.Errorf("crc16() = %#04x, want 0x31c3", crc)
	}
}

func Test_Single(t *testing.T) {
	// A single instance is the only node of its cluster, it serves
	// every slot.
	c1, c2 := net.Pipe()
	defer c2.Close()
	conn := &resp.Client{C: c1}
	c := Single(conn)
	nodes := c.All()
	if len(nodes) != 1 {
		t.Fatalf("Single() = %d nodes, want one", len(nodes))
	}
	if nodes[0].Conn != conn {
		t.Error("Single() names not the connection of the server")
	}
	if !slices.Equal(nodes[0].Slots, []rdb.SlotRange{{Start: 0, End: slotCount - 1}}) {
		t.Errorf("Single() slots = %v, want every slot", nodes[0].Slots)
	}
	if _, err := c.Target([]byte("any")); err != nil {
		t.Errorf("Target() error = %v", err)
	}
	if err := c.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

// reply returns the handler that writes the tokens of a static reply,
// the header of an aggregate and its elements arrive one after the
// other.
func reply(tokens ...resp.Token) resp.HandlerFunc {
	return func(w resp.ResponseWriter, cmd *resp.Command) {
		for _, tok := range tokens {
			if err := w.Proto().WriteToken(tok); err != nil {
				return
			}
		}
	}
}

// nodeOfShard is the tokens of one node of a shard of the CLUSTER
// SHARDS reply, it names its name, its address and its role.
func nodeOfShard(id, role string, port int) []resp.Token {
	return []resp.Token{
		resp.ArrayLen(12),
		resp.Bytes([]byte("id")), resp.Bytes([]byte(id)),
		resp.Bytes([]byte("endpoint")), resp.Bytes([]byte("127.0.0.1")),
		resp.Bytes([]byte("ip")), resp.Bytes([]byte("127.0.0.1")),
		resp.Bytes([]byte("port")), resp.Int(int64(port)),
		resp.Bytes([]byte("role")), resp.Bytes([]byte(role)),
		resp.Bytes([]byte("health")), resp.Bytes([]byte("online")),
	}
}

// shardOfReply is the tokens of one shard of the CLUSTER SHARDS reply,
// the range of the slots is served by its first node and the replicas
// behind it.
func shardOfReply(start, end int64, nodes ...[]resp.Token) []resp.Token {
	tokens := []resp.Token{
		resp.ArrayLen(4),
		resp.Bytes([]byte("slots")),
		resp.ArrayLen(2),
		resp.Int(start),
		resp.Int(end),
		resp.Bytes([]byte("nodes")),
		resp.ArrayLen(len(nodes)),
	}
	for _, node := range nodes {
		tokens = append(tokens, node...)
	}
	return tokens
}

// shardsReply is the tokens of the CLUSTER SHARDS reply of the shards.
func shardsReply(shards ...[]resp.Token) []resp.Token {
	tokens := []resp.Token{resp.ArrayLen(len(shards))}
	for _, shard := range shards {
		tokens = append(tokens, shard...)
	}
	return tokens
}

func TestCluster_Target(t *testing.T) {
	// The node of a key is the one that serves its slot, the keys
	// of the test route to the two nodes of the cluster. The slot
	// of the key zap stays unassigned.
	cl := resptest.NewCluster(t,
		resptest.Shard{ID: "node-0", Slots: []rdb.SlotRange{{Start: 0, End: 5460}}},
		resptest.Shard{ID: "node-1", Slots: []rdb.SlotRange{{Start: 12000, End: 13000}}},
	)
	c, err := Upgrade(t.Context(), cl.Node("node-0").Client())
	if err != nil {
		t.Fatalf("Upgrade() error = %v", err)
	}
	defer c.Close()
	tests := []struct {
		name   string
		key    string
		node   int
		wantEr string
	}{
		{name: "bar", key: "bar", node: 0}, // the slot of the key is 5061
		{name: "foo", key: "foo", node: 1}, // the slot of the key is 12182
		{name: "zap", key: "zap", wantEr: "no node serves the slot 6469"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node, err := c.Target([]byte(tt.key))
			if tt.wantEr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantEr) {
					t.Errorf("Target(%q) error = %v, want it to contain %q", tt.key, err, tt.wantEr)
				}
				return
			}
			if err != nil {
				t.Errorf("Target(%q) error = %v", tt.key, err)
				return
			}
			if want := c.All()[tt.node]; !slices.Equal(node.Slots, want.Slots) {
				t.Errorf("Target(%q) = %+v, want the node %d", tt.key, node, tt.node)
			}
		})
	}
}

func Test_Upgrade(t *testing.T) {
	t.Run("the seed is pinned to its node", func(t *testing.T) {
		cl := resptest.NewCluster(t,
			resptest.Shard{ID: "node-0", Slots: []rdb.SlotRange{{Start: 0, End: 8191}}},
			resptest.Shard{ID: "node-1", Slots: []rdb.SlotRange{{Start: 8192, End: 16383}}},
		)
		seed := cl.Node("node-0").Client()
		c, err := Upgrade(t.Context(), seed)
		if err != nil {
			t.Fatalf("Upgrade() error = %v", err)
		}
		defer c.Close()
		nodes := c.All()
		if len(nodes) != 2 {
			t.Fatalf("Upgrade() = %d nodes, want the primary shards of the cluster", len(nodes))
		}
		// The MYID of the seed names the first primary shard, the
		// connection of the seed serves it and the second primary shard
		// is dialed alone.
		if nodes[0].Conn != seed || nodes[0].Addr != cl.Node("node-0").Addr {
			t.Errorf("Upgrade()[0] = %+v, want the seed connection of %s", nodes[0], cl.Node("node-0").Addr)
		}
		if nodes[1].Conn == nil || nodes[1].Conn == seed || nodes[1].Addr != cl.Node("node-1").Addr {
			t.Errorf("Upgrade()[1] = %+v, want the dial of %s", nodes[1], cl.Node("node-1").Addr)
		}
	})

	t.Run("a seed that names no primary shard is closed", func(t *testing.T) {
		m1 := resptest.NewServer(t, nil).Addr
		m2 := resptest.NewServer(t, nil).Addr
		seedNode := resptest.NewServer(t, func() resp.Handler {
			mux := resp.NewServeMux()
			mux.HandleFunc("CLUSTER|SHARDS", reply(shardsOfPrimaries(m1, m2)...))
			mux.HandleFunc("CLUSTER|MYID", reply(resp.Bytes([]byte("other"))))
			return mux
		}()).Addr
		u, err := url.Parse("redis://" + seedNode)
		if err != nil {
			t.Fatal(err)
		}
		seed, err := resp.Dial(t.Context(), u, 2)
		if err != nil {
			t.Fatal(err)
		}
		c, err := Upgrade(t.Context(), seed)
		if err != nil {
			t.Fatalf("Upgrade() error = %v", err)
		}
		defer c.Close()
		// The seed names no primary shard of the shards, every primary shard is
		// dialed anew.
		nodes := c.All()
		if len(nodes) != 2 {
			t.Fatalf("Upgrade() = %d nodes, want the primary shards of the shards", len(nodes))
		}
		for i, want := range []string{m1, m2} {
			if nodes[i].Addr != want {
				t.Errorf("Upgrade()[%d].Addr = %s, want %s", i, nodes[i].Addr, want)
			}
			if nodes[i].Conn == nil || nodes[i].Conn == seed {
				t.Errorf("Upgrade()[%d] carries not the dial of a new connection", i)
			}
		}
	})
}

// shardsOfPrimaries is the CLUSTER SHARDS reply of a cluster of the
// given primary shards, a primary shard of an address serves the half of the slots.
func shardsOfPrimaries(addrs ...string) []resp.Token {
	var shards [][]resp.Token
	for i, addr := range addrs {
		_, port, _ := net.SplitHostPort(addr)
		n, _ := strconv.Atoi(port)
		start := int64(i * (slotCount / len(addrs)))
		end := start + int64(slotCount/len(addrs)) - 1
		shards = append(shards, shardOfReply(start, end, nodeOfShard(fmt.Sprintf("node-%d", i), "master", n)))
	}
	return shardsReply(shards...)
}

func Test_topology(t *testing.T) {
	// The wire of a CLUSTER SHARDS reply: a shard names the pairs
	// of the first and the last slot of its ranges and the nodes
	// that serve them, a node names its name, its address and its
	// role, the flat arrays of the names and the values arrive as
	// they do.
	const (
		disabled = "ERR This instance has cluster support disabled"
		noAuth   = "ERR no auth"
	)
	tests := []struct {
		name    string
		script  func(mux *resp.ServeMux)
		want    []Node
		wantErr bool
	}{
		{
			name: "the primary shards of the shards",
			script: func(mux *resp.ServeMux) {
				mux.HandleFunc("CLUSTER|SHARDS", reply(shardsReply(
					shardOfReply(0, 5460, nodeOfShard("node-1", "master", 7000), nodeOfShard("replica-1", "replica", 7001)),
					shardOfReply(5461, 16383, nodeOfShard("replica-2", "replica", 7002), nodeOfShard("node-2", "master", 7003)),
				)...))
			},
			want: []Node{
				{ID: "node-1", Addr: "127.0.0.1:7000", Slots: []rdb.SlotRange{{Start: 0, End: 5460}}},
				{ID: "node-2", Addr: "127.0.0.1:7003", Slots: []rdb.SlotRange{{Start: 5461, End: 16383}}},
			},
		},
		{
			name: "the primary shards of the slots",
			script: func(mux *resp.ServeMux) {
				// The SLOTS of the servers that reject the SHARDS
				// form: an entry names the first and the last slot
				// of a range and the primary shard that serves it, the
				// ranges of a primary shard arrive in an entry each.
				mux.HandleFunc("CLUSTER|SHARDS", reply(resp.Error("ERR unknown command 'CLUSTER'")))
				mux.HandleFunc("CLUSTER|SLOTS", reply(
					resp.ArrayLen(2),
					resp.ArrayLen(3), resp.Int(0), resp.Int(100), resp.ArrayLen(3), resp.Bytes([]byte("127.0.0.1")), resp.Int(7000), resp.Bytes([]byte("node-1")),
					resp.ArrayLen(3), resp.Int(200), resp.Int(300), resp.ArrayLen(3), resp.Bytes([]byte("127.0.0.1")), resp.Int(7000), resp.Bytes([]byte("node-1")),
				))
			},
			want: []Node{
				{ID: "node-1", Addr: "127.0.0.1:7000", Slots: []rdb.SlotRange{{Start: 0, End: 100}, {Start: 200, End: 300}}},
			},
		},
		{
			name: "a server that runs no cluster",
			script: func(mux *resp.ServeMux) {
				mux.HandleFunc("CLUSTER|SHARDS", reply(resp.Error(disabled)))
				mux.HandleFunc("CLUSTER|SLOTS", reply(resp.Error(disabled)))
			},
			wantErr: true,
		},
		{
			name: "the CLUSTER command of a server fails",
			script: func(mux *resp.ServeMux) {
				mux.HandleFunc("CLUSTER|SHARDS", reply(resp.Error(noAuth)))
				mux.HandleFunc("CLUSTER|SLOTS", reply(resp.Error(noAuth)))
			},
			wantErr: true,
		},
		{
			name: "a cluster in construction serves no slots",
			script: func(mux *resp.ServeMux) {
				mux.HandleFunc("CLUSTER|SHARDS", reply(resp.ArrayLen(0)))
			},
			wantErr: true,
		},
		{
			// The reply a server routed to another node of the
			// cluster carries the answer of that node, a +PONG of a
			// PING for example.
			name: "the SHARDS reply is a string",
			script: func(mux *resp.ServeMux) {
				mux.HandleFunc("CLUSTER|SHARDS", reply(resp.String("PONG")))
			},
			wantErr: true,
		},
		{
			name: "the shard of the SHARDS reply is a string",
			script: func(mux *resp.ServeMux) {
				mux.HandleFunc("CLUSTER|SHARDS", reply(resp.ArrayLen(1), resp.String("shard")))
			},
			wantErr: true,
		},
		{
			name: "the slots of a shard are a string",
			script: func(mux *resp.ServeMux) {
				mux.HandleFunc("CLUSTER|SHARDS", reply(
					resp.ArrayLen(1),
					resp.ArrayLen(2),
					resp.Bytes([]byte("slots")), resp.String("oops"),
				))
			},
			wantErr: true,
		},
		{
			name: "the nodes of a shard are a string",
			script: func(mux *resp.ServeMux) {
				mux.HandleFunc("CLUSTER|SHARDS", reply(
					resp.ArrayLen(1),
					resp.ArrayLen(2),
					resp.Bytes([]byte("nodes")), resp.String("oops"),
				))
			},
			wantErr: true,
		},
		{
			name: "the node of a shard is a string",
			script: func(mux *resp.ServeMux) {
				mux.HandleFunc("CLUSTER|SHARDS", reply(
					resp.ArrayLen(1),
					resp.ArrayLen(4),
					resp.Bytes([]byte("slots")), resp.ArrayLen(2), resp.Int(0), resp.Int(100),
					resp.Bytes([]byte("nodes")), resp.ArrayLen(1), resp.String("node"),
				))
			},
			wantErr: true,
		},
		{
			name: "the SLOTS reply is a string",
			script: func(mux *resp.ServeMux) {
				mux.HandleFunc("CLUSTER|SHARDS", reply(resp.Error("ERR unknown command 'CLUSTER'")))
				mux.HandleFunc("CLUSTER|SLOTS", reply(resp.String("PONG")))
			},
			wantErr: true,
		},
		{
			name: "the entry of the SLOTS reply is a string",
			script: func(mux *resp.ServeMux) {
				mux.HandleFunc("CLUSTER|SHARDS", reply(resp.Error("ERR unknown command 'CLUSTER'")))
				mux.HandleFunc("CLUSTER|SLOTS", reply(resp.ArrayLen(1), resp.String("entry")))
			},
			wantErr: true,
		},
		{
			name: "the primary shard of an entry is a string",
			script: func(mux *resp.ServeMux) {
				mux.HandleFunc("CLUSTER|SHARDS", reply(resp.Error("ERR unknown command 'CLUSTER'")))
				mux.HandleFunc("CLUSTER|SLOTS", reply(
					resp.ArrayLen(1),
					resp.ArrayLen(3), resp.Int(0), resp.Int(100), resp.String("oops"),
				))
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr := resptest.NewServer(t, func() resp.Handler {
				mux := resp.NewServeMux()
				tt.script(mux)
				return mux
			}()).Addr
			u, err := url.Parse("redis://" + addr)
			if err != nil {
				t.Fatal(err)
			}
			conn, err := resp.Dial(t.Context(), u, 2)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { conn.Close() })
			nodes, err := topology(t.Context(), conn)
			if (err != nil) != tt.wantErr {
				t.Fatalf("topology() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			if len(nodes) != len(tt.want) {
				t.Fatalf("topology() = %v, want %v", nodes, tt.want)
			}
			for i, want := range tt.want {
				if nodes[i].ID != want.ID || nodes[i].Addr != want.Addr || !slices.Equal(nodes[i].Slots, want.Slots) {
					t.Errorf("topology()[%d] = %+v, want %+v", i, nodes[i], want)
				}
			}
		})
	}
}

func Test_topology_context(t *testing.T) {
	// The read of the reply is aborted when the context is done,
	// the server of the test answers nothing.
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		io.Copy(io.Discard, c)
	}()
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	// The codecs are wired without the handshake of the dial, the
	// peer of the test answers nothing.
	c := &resp.Client{C: conn, D: resp.NewRESP3Decoder(conn), E: resp.NewRESP3Encoder(conn)}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if _, err := topology(ctx, c); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("topology() error = %v, want %v", err, context.DeadlineExceeded)
	}
}

func TestCluster_Refresh(t *testing.T) {
	// dial dials a scripted server of the test the way the
	// transfer dials a node.
	dial := func(addr string) *resp.Client {
		t.Helper()
		u, err := url.Parse("redis://" + addr)
		if err != nil {
			t.Fatal(err)
		}
		conn, err := resp.Dial(t.Context(), u, 2)
		if err != nil {
			t.Fatal(err)
		}
		return conn
	}

	// shardOf is the CLUSTER SHARDS reply of one primary shard, it serves
	// the range of the slots.
	shardOf := func(id, addr string, start, end int64) []resp.Token {
		_, port, _ := net.SplitHostPort(addr)
		n, _ := strconv.Atoi(port)
		return shardOfReply(start, end, nodeOfShard(id, "master", n))
	}

	t.Run("the nodes the cluster keeps are kept", func(t *testing.T) {
		// The discovery of the refresh is asked on the connection of
		// one of the kept nodes, both serve the topology it names:
		// the node it keeps and a node it gained, the range of the
		// kept node moved with the resharding, the node it dropped
		// is named no more.
		var a0, a1, a2 string
		// The tokens of the reply name the addresses of the
		// servers of the test, they are built when the command
		// arrives, the addresses do not exist before.
		shards := func() resp.Handler {
			mux := resp.NewServeMux()
			mux.HandleFunc("CLUSTER|SHARDS", func(w resp.ResponseWriter, cmd *resp.Command) {
				for _, tok := range shardsReply(
					shardOf("node-0", a0, 0, 4000),
					shardOf("node-2", a2, 4001, 16383),
				) {
					if err := w.Proto().WriteToken(tok); err != nil {
						return
					}
				}
			})
			return mux
		}
		a0 = resptest.NewServer(t, shards()).Addr
		a1 = resptest.NewServer(t, shards()).Addr
		a2 = resptest.NewServer(t, nil).Addr
		old0 := dial(a0)
		old1 := dial(a1)
		c := &Cluster{nodes: []Node{
			{ID: "node-0", Addr: a0, Conn: old0, Slots: []rdb.SlotRange{{Start: 0, End: 8191}}},
			{ID: "node-1", Addr: a1, Conn: old1, Slots: []rdb.SlotRange{{Start: 8192, End: 16383}}},
		}}
		fresh, err := c.Refresh(t.Context())
		if err != nil {
			t.Fatalf("Refresh() error = %v", err)
		}
		// The kept node carries the connection of the transfer, the
		// gained node carries the dial of its address and the
		// connection of the dropped node is closed.
		nodes := fresh.All()
		if len(nodes) != 2 {
			t.Fatalf("Refresh() = %d nodes, want the kept and the gained one", len(nodes))
		}
		if nodes[0].ID != "node-0" || nodes[0].Addr != a0 || nodes[0].Conn != old0 {
			t.Errorf("Refresh()[0] = %+v, want the kept connection of %s", nodes[0], a0)
		}
		if nodes[1].ID != "node-2" || nodes[1].Addr != a2 || nodes[1].Conn == nil {
			t.Errorf("Refresh()[1] = %+v, want the dial of %s", nodes[1], a2)
		}
		if !slices.Equal(nodes[0].Slots, []rdb.SlotRange{{Start: 0, End: 4000}}) {
			t.Errorf("Refresh()[0] slots = %v, want the slots of the new topology", nodes[0].Slots)
		}
		if !slices.Equal(nodes[1].Slots, []rdb.SlotRange{{Start: 4001, End: 16383}}) {
			t.Errorf("Refresh()[1] slots = %v, want the slots of the shard", nodes[1].Slots)
		}
		// The routing follows the slots of the new topology, the key
		// foo lands on the gained node with its range and the key of
		// the moved range stays on the kept one.
		if n, err := fresh.Target([]byte("foo")); err != nil || n.Addr != a2 {
			t.Errorf("Target(foo) = %+v, want the gained node of %s", n, a2)
		}
		if n, err := fresh.Target([]byte("{user1000}.following")); err != nil || n.Addr != a0 {
			t.Errorf("Target({user1000}.following) = %+v, want the kept node of %s", n, a0)
		}
		if _, err := old1.Exec(t.Context(), []byte("PING")); err == nil {
			t.Error("the connection of the dropped node is open")
		}
		if err := fresh.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	t.Run("a cluster that fails the discovery is left alone", func(t *testing.T) {
		// The discovery is asked on the connection of the only node
		// of the cluster, its server rejects it.
		a0 := resptest.NewServer(t, func() resp.Handler {
			mux := resp.NewServeMux()
			mux.HandleFunc("CLUSTER|SHARDS", reply(resp.Error("ERR boom")))
			mux.HandleFunc("CLUSTER|SLOTS", reply(resp.Error("ERR boom")))
			return mux
		}()).Addr
		old0 := dial(a0)
		c := &Cluster{nodes: []Node{
			{ID: "node-0", Addr: a0, Conn: old0, Slots: []rdb.SlotRange{{Start: 0, End: 16383}}},
		}}
		defer c.Close()
		var err error
		if c, err = c.Refresh(t.Context()); err == nil {
			t.Fatal("Refresh() did not fail")
		}
	})
}

func TestCluster_Close(t *testing.T) {
	// The connections of every node are closed.
	cl := resptest.NewCluster(t,
		resptest.Shard{ID: "node-0", Slots: []rdb.SlotRange{{Start: 0, End: 8191}}},
		resptest.Shard{ID: "node-1", Slots: []rdb.SlotRange{{Start: 8192, End: 16383}}},
	)
	c, err := Upgrade(t.Context(), cl.Node("node-0").Client())
	if err != nil {
		t.Fatalf("Upgrade() error = %v", err)
	}
	nodes := c.All()
	if len(nodes) != 2 {
		t.Fatalf("Upgrade() = %d nodes, want the primary shards of the cluster", len(nodes))
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	for _, n := range nodes {
		if _, err := n.Conn.Exec(t.Context(), []byte("PING")); err == nil {
			t.Errorf("the connection of the node %s is open", n.Addr)
		}
	}
}

// clusterURL is the url of the seed of the live cluster, the port
// mapping of the container maps it to the ports the nodes announce.
var clusterURL = "redis://127.0.0.1:7001"

// startClusterContainer starts the cluster of the live test and
// removes it at the end of the test.
func startClusterContainer(t *testing.T) {
	t.Helper()
	redis := func(port int, config string) string {
		return fmt.Sprintf("redis-server --port %d --cluster-enabled yes --cluster-config-file %s --cluster-announce-ip 127.0.0.1 --protected-mode no & ", port, config)
	}
	script := redis(7001, "nodes1.conf") + redis(7002, "nodes2.conf") + redis(7003, "nodes3.conf") +
		"sleep 1 && redis-cli --cluster create 127.0.0.1:7001 127.0.0.1:7002 127.0.0.1:7003 --cluster-yes && wait"

	// The nodes announce 127.0.0.1 with their own port, the binding
	// maps every node to the port it announces so the addresses the
	// discovery reports are host reachable.
	bindings := network.PortMap{}
	clusterPorts := []string{"7001", "7002", "7003"}
	for _, port := range clusterPorts {
		key, err := network.ParsePort(port + "/tcp")
		if err != nil {
			t.Fatal(err)
		}
		bindings[key] = []network.PortBinding{{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: port}}
	}
	clusterC, err := testcontainers.Run(t.Context(), "redis:latest",
		testcontainers.WithLogger(log.New(io.Discard, "", 0)),
		testcontainers.WithExposedPorts("7001/tcp", "7002/tcp", "7003/tcp"),
		testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
			hc.PortBindings = bindings
		}),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("7001/tcp")),
		testcontainers.WithCmd("sh", "-c", script),
	)
	if err != nil {
		t.Fatalf("starting the cluster container: %v", err)
	}
	testcontainers.CleanupContainer(t, clusterC)
	waitClusterReady(t, clusterPorts)
}

// waitClusterReady retries the CLUSTER INFO of every node until the
// cluster serves its slots or the deadline passes, a node that still
// waits for the gossip of the others answers a SET with CLUSTERDOWN.
func waitClusterReady(t *testing.T, clusterPorts []string) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for {
		ready := true
		for _, port := range clusterPorts {
			u, err := url.Parse("redis://127.0.0.1:" + port)
			if err != nil {
				t.Fatal(err)
			}
			conn, err := resp.Dial(t.Context(), u, 2)
			if err != nil {
				ready = false
				break
			}
			// The payload of a reply streams behind its header, it
			// is read before the connection closes.
			ok := false
			if reply, err := conn.Exec(t.Context(), []byte("CLUSTER"), []byte("INFO")); err == nil && reply.Kind() != resp.KindError {
				ok = strings.Contains(reply.String(), "cluster_state:ok")
			}
			conn.Close()
			if !ok {
				ready = false
				break
			}
		}
		if ready {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the cluster of %s did not become ready", clusterURL)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// liveReply runs a command on the connection of a node and returns
// its reply, an error reply becomes the error of the call.
func liveReply(ctx context.Context, conn *resp.Client, args ...string) (resp.Token, error) {
	argsb := make([][]byte, len(args))
	for i, arg := range args {
		argsb[i] = []byte(arg)
	}
	t, err := conn.Exec(ctx, argsb...)
	if err != nil {
		return resp.Token{}, err
	}
	if t.Kind() == resp.KindError {
		return resp.Token{}, t.Err()
	}
	return t, nil
}

// TestCluster speaks to a real cluster. It starts a Docker container
// with testcontainers, three servers on the ports 7001-7003 that form
// one cluster, and removes it at the end of the test:
//
//	redis-server --port 7001 --cluster-enabled yes --cluster-config-file nodes1.conf --cluster-announce-ip 127.0.0.1 --protected-mode no &
//	redis-server --port 7002 --cluster-enabled yes --cluster-config-file nodes2.conf --cluster-announce-ip 127.0.0.1 --protected-mode no &
//	redis-server --port 7003 --cluster-enabled yes --cluster-config-file nodes3.conf --cluster-announce-ip 127.0.0.1 --protected-mode no &
//	sleep 1 && redis-cli --cluster create 127.0.0.1:7001 127.0.0.1:7002 127.0.0.1:7003 --cluster-yes && wait
func TestCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("the test runs a server in a Docker container")
	}

	startClusterContainer(t)

	u, err := url.Parse(clusterURL)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := resp.Dial(t.Context(), u, 2)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	c, err := Upgrade(t.Context(), conn)
	if err != nil {
		t.Fatalf("Upgrade() error = %v", err)
	}
	defer c.Close()

	t.Run("the primary shards of the cluster", func(t *testing.T) {
		nodes := c.All()
		if len(nodes) != 3 {
			t.Fatalf("All() = %d nodes, want the three primary shards of the cluster", len(nodes))
		}
		for i, n := range nodes {
			if n.Conn == nil {
				t.Errorf("All()[%d] carries no connection", i)
			}
			if len(n.Slots) == 0 {
				t.Errorf("All()[%d] serves no slots", i)
			}
		}
	})

	t.Run("the target of a key serves its slot", func(t *testing.T) {
		for _, key := range []string{"foo", "bar", "{user1000}.following"} {
			n, err := c.Target([]byte(key))
			if err != nil {
				t.Fatalf("Target(%q) error = %v", key, err)
			}
			// The slot the cluster reports matches the one the
			// keys are hashed with, the node of the key serves
			// it.
			reply, err := liveReply(t.Context(), n.Conn, "CLUSTER", "KEYSLOT", key)
			if err != nil {
				t.Fatalf("CLUSTER KEYSLOT(%q) error = %v", key, err)
			}
			if slot := reply.Int(); slot != int64(KeySlot([]byte(key))) {
				t.Errorf("CLUSTER KEYSLOT(%q) = %d, want %d", key, slot, KeySlot([]byte(key)))
			}
			if !n.Contains(uint16(reply.Int())) {
				t.Errorf("Target(%q) serves no slot %d", key, reply.Int())
			}
			// The key is stored on the node its slot names and
			// read back from it.
			if _, err := liveReply(t.Context(), n.Conn, "SET", key, "value"); err != nil {
				t.Fatalf("SET(%q) error = %v", key, err)
			}
			reply, err = liveReply(t.Context(), n.Conn, "GET", key)
			if err != nil {
				t.Fatalf("GET(%q) error = %v", key, err)
			}
			if reply.String() != "value" {
				t.Errorf("GET(%q) = %q, want value", key, reply.String())
			}
			// The key leaves the keyspace empty for the walk of
			// the scan.
			if _, err := liveReply(t.Context(), n.Conn, "DEL", key); err != nil {
				t.Fatalf("DEL(%q) error = %v", key, err)
			}
		}
	})

	t.Run("the topology of a stable cluster is kept", func(t *testing.T) {
		before := map[string]Node{}
		for _, n := range c.All() {
			before[n.ID] = n
		}
		fresh, err := c.Refresh(t.Context())
		if err != nil {
			t.Fatalf("Refresh() error = %v", err)
		}
		// The discovery is asked on a node of the cluster, the
		// servers name the same primary shards in an order of their own,
		// the nodes are compared as the set of the stable cluster.
		after := fresh.All()
		if len(after) != len(before) {
			t.Fatalf("Refresh() left %d nodes, want the %d of the stable cluster", len(after), len(before))
		}
		for _, n := range after {
			old, ok := before[n.ID]
			if !ok {
				t.Errorf("Refresh() gained the node %s of a stable cluster", n.ID)
				continue
			}
			if n.Addr != old.Addr {
				t.Errorf("Refresh()[%s] addr = %s, want %s", n.ID, n.Addr, old.Addr)
			}
			if n.Conn != old.Conn {
				t.Errorf("Refresh() dialed the kept node %s anew", n.ID)
			}
			if !slices.Equal(n.Slots, old.Slots) {
				t.Errorf("Refresh()[%s] slots = %v, want %v", n.ID, n.Slots, old.Slots)
			}
		}
	})
}
