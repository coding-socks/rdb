package resptest

import (
	"testing"

	"github.com/coding-socks/rdb"
	"github.com/coding-socks/rdb/clusters"
	"github.com/coding-socks/rdb/resp"
)

func TestNewCluster(t *testing.T) {
	// The topology of the cluster is the one its shards describe,
	// the ranges of the slots of a node arrive in a shard each or
	// behind the first one.
	shards := []Shard{
		{ID: "node-0", Slots: []rdb.SlotRange{{Start: 0, End: 100}, {Start: 200, End: 300}}},
		{ID: "node-1", Slots: []rdb.SlotRange{{Start: 5461, End: 16383}}},
	}

	t.Run("every node names itself", func(t *testing.T) {
		cl := NewCluster(t, shards...)
		for _, s := range shards {
			tok, err := cl.Node(s.ID).Client().Exec(t.Context(), []byte("CLUSTER"), []byte("MYID"))
			if err != nil {
				t.Fatalf("Exec() error = %v", err)
			}
			if tok.Kind() != resp.KindBlobString || tok.String() != s.ID {
				t.Errorf("CLUSTER MYID = %s %v, want %q", tok.Kind(), tok.Val(), s.ID)
			}
		}
	})

	t.Run("the discovery adopts the topology", func(t *testing.T) {
		cl := NewCluster(t, shards...)
		c, err := clusters.Upgrade(t.Context(), cl.Node("node-0").Client())
		if err != nil {
			t.Fatalf("Upgrade() error = %v", err)
		}
		defer c.Close()
		nodes := c.All()
		if len(nodes) != len(shards) {
			t.Fatalf("Upgrade() = %d nodes, want %d", len(nodes), len(shards))
		}
		for i, s := range shards {
			if nodes[i].ID != s.ID || nodes[i].Addr != cl.Node(s.ID).Addr {
				t.Errorf("Upgrade()[%d] = %+v, want %s on %s", i, nodes[i], s.ID, cl.Node(s.ID).Addr)
			}
			if len(nodes[i].Slots) != len(s.Slots) {
				t.Fatalf("Upgrade()[%d] slots = %v, want %v", i, nodes[i].Slots, s.Slots)
			}
			for j, r := range s.Slots {
				if got := nodes[i].Slots[j]; got.Start != r.Start || got.End != r.End {
					t.Errorf("Upgrade()[%d] slots[%d] = %+v, want %+v", i, j, got, r)
				}
			}
		}
	})

	t.Run("the SLOTS reply names the topology", func(t *testing.T) {
		// An entry of the reply names the first and the last slot
		// of a range and the node that serves it, the ranges of a
		// node arrive in an entry each.
		cl := NewCluster(t, shards...)
		c := cl.Node("node-0").Client()
		tok, err := c.Exec(t.Context(), []byte("CLUSTER"), []byte("SLOTS"))
		if err != nil {
			t.Fatalf("Exec() error = %v", err)
		}
		want := []struct {
			start, end int64
			id         string
		}{
			{0, 100, "node-0"},
			{200, 300, "node-0"},
			{5461, 16383, "node-1"},
		}
		if tok.Kind() != resp.KindArray || tok.Int() != int64(len(want)) {
			t.Fatalf("CLUSTER SLOTS = %s %v, want %d entries", tok.Kind(), tok.Val(), len(want))
		}
		next := func() resp.Token {
			t.Helper()
			tok, err := c.D.ReadToken()
			if err != nil {
				t.Fatalf("ReadToken() error = %v", err)
			}
			return tok
		}
		for _, w := range want {
			if tok := next(); tok.Kind() != resp.KindArray || tok.Int() != 3 {
				t.Fatalf("an entry = %s %v, want the range and its node", tok.Kind(), tok.Val())
			}
			if tok := next(); tok.Kind() != resp.KindNumber || tok.Int() != w.start {
				t.Errorf("the first slot of an entry = %s %v, want %d", tok.Kind(), tok.Val(), w.start)
			}
			if tok := next(); tok.Kind() != resp.KindNumber || tok.Int() != w.end {
				t.Errorf("the last slot of an entry = %s %v, want %d", tok.Kind(), tok.Val(), w.end)
			}
			if tok := next(); tok.Kind() != resp.KindArray || tok.Int() != 3 {
				t.Fatalf("the node of an entry = %s %v, want its address and its name", tok.Kind(), tok.Val())
			}
			next() // the address of the node, the name carries the check
			next() // the port of the node
			if tok := next(); tok.Kind() != resp.KindBlobString || tok.String() != w.id {
				t.Errorf("the node of an entry = %s %v, want %q", tok.Kind(), tok.Val(), w.id)
			}
		}
	})
}

func TestCluster_Close(t *testing.T) {
	// The close of the cluster closes every node of it.
	cl := NewCluster(t,
		Shard{ID: "node-0", Slots: []rdb.SlotRange{{Start: 0, End: 8191}}},
		Shard{ID: "node-1", Slots: []rdb.SlotRange{{Start: 8192, End: 16383}}},
	)
	nodes := cl.Nodes()
	clients := make([]*resp.Client, len(nodes))
	for i, n := range nodes {
		clients[i] = n.Client()
	}
	cl.Close()
	for i, c := range clients {
		if _, err := c.Exec(t.Context(), []byte("PING")); err == nil {
			t.Errorf("the node %d serves commands after Close()", i)
		}
	}
}
