// Package clusters establishes the connections of the nodes of a
// Redis compatible cluster and routes the keys to them: the CLUSTER
// command of a server names the primary shards of the cluster and the slots
// they serve, [Dial] dials them and [Cluster.Target] names the node
// that serves the slot of a key. A single instance takes part in a
// transfer as the only node of its cluster, the node that serves
// every slot.
package clusters

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/coding-socks/rdb"
	"github.com/coding-socks/rdb/resp"
)

// slotCount is the number of the hash slots of a cluster.
const slotCount = 16384

// Node is one primary shard of a cluster: the address it serves its slots
// on, the slots it serves, and the connection of the transfer.
type Node struct {
	// ID is the name of the node the server reports.
	ID string
	// Addr is the host:port address the node announces, the
	// address the transfer dials it on.
	Addr string
	// Slots are the ranges of the hash slots the node serves.
	Slots []rdb.SlotRange
	// Conn is the connection the transfer speaks to the node on.
	// The constructors of the package set it and [Cluster.Close]
	// closes it, the sources and the destinations of the transfer
	// read it.
	Conn *resp.Client
}

// Contains reports whether the node serves the slot.
func (n Node) Contains(slot uint16) bool {
	for _, r := range n.Slots {
		if r.Contains(slot) {
			return true
		}
	}
	return false
}

// Cluster is the set of the primary shards of a Redis compatible cluster
// with the connections of the transfer: [Cluster.Target] routes a key
// to the node that serves its slot and [Cluster.Refresh] adopts the
// topology a failover or a resharding left behind.
type Cluster struct {
	nodes []Node
}

// Single returns the cluster of a single instance: the server of the
// connection is the only node of its cluster and serves every slot.
// The connection belongs to the cluster, [Cluster.Close] closes it.
func Single(conn *resp.Client) *Cluster {
	return &Cluster{
		nodes: []Node{{Conn: conn, Slots: []rdb.SlotRange{{Start: 0, End: slotCount - 1}}}},
	}
}

func Dial(ctx context.Context, u *url.URL, p int) (*Cluster, error) {
	seed, err := resp.Dial(ctx, u, p)
	if err != nil {
		return nil, err
	}
	return Upgrade(ctx, seed)
}

// Upgrade discovers the primary shards of the cluster the seed connection is
// dialed on and returns their cluster. The CLUSTER MYID command names
// the node the seed itself speaks to, the CLUSTER command names the
// primary shards and the connection of the seed becomes the one of its node,
// the primary shards it does not serve are dialed on the addresses they
// announce with the url and the protocol of the seed. The upgrade
// fails when the MYID of the seed fails, the seed is closed. A seed
// whose MYID names no primary shard is closed and every primary shard
// is dialed anew. A failed upgrade closes the connections it opened,
// the seed included.
func Upgrade(ctx context.Context, seed *resp.Client) (*Cluster, error) {
	var old []Node
	id, err := ReadID(ctx, seed)
	if err != nil {
		seed.Close()
		return nil, fmt.Errorf("failed to read cluster id: %w", err)
	}
	old = []Node{{ID: id, Conn: seed}}
	nodes, err := refreshNodes(ctx, old)
	if err != nil {
		for _, n := range old {
			n.Conn.Close()
		}
		return nil, err
	}
	return &Cluster{nodes: nodes}, nil
}

// Dialect returns the dialect of the server the cluster is dialed on,
// the dialect the scheme of the url of its connections names.
func (c *Cluster) Dialect() rdb.Dialect {
	if len(c.nodes) == 0 {
		return nil
	}
	return c.nodes[0].Conn.Dialect()
}

func (c *Cluster) SetDialect(dialect rdb.Dialect) {
	for _, n := range c.nodes {
		n.Conn.SetDialect(dialect)
	}
}

// Keys returns the keys a command touches, the command table of the
// dialect of the cluster decides which arguments of the command name
// them. It fails when the table names no command for the arguments or
// the arguments violate the arity of the command.
func (c *Cluster) Keys(command ...[]byte) ([][]byte, error) {
	cmd, err := c.Dialect().Commands().Lookup(command)
	if err != nil {
		return nil, err
	}
	return cmd.ExtractKeys(command)
}

// Target returns the node of the cluster that serves the slot of a
// key, the transfer executes the commands of the key on its
// connection. It fails when no node of the cluster serves the slot of
// the key. It does not guard the connection of a node against a
// concurrent use of its own.
func (c *Cluster) Target(key []byte) (Node, error) {
	i, err := route(c.nodes, key)
	if err != nil {
		return Node{}, err
	}
	return c.nodes[i], nil
}

// All returns the primary shards of the cluster in the order the server
// names them, the replicas are left out: the transfer reads the keys
// of the primary shards and stores them on the primary shards alone.
func (c *Cluster) All() []Node {
	if len(c.nodes) == 0 {
		return nil
	}
	return append([]Node(nil), c.nodes...)
}

// Refresh rediscovers the primary shards of the cluster.
// It returns a cluster with the new topology.
func (c *Cluster) Refresh(ctx context.Context) (*Cluster, error) {
	nodes, err := refreshNodes(ctx, c.nodes)
	if err != nil {
		return nil, err
	}
	c.nodes = nodes
	return &Cluster{nodes: nodes}, nil
}

// Close closes the connections of the nodes of the cluster, the
// errors of the nodes are joined.
func (c *Cluster) Close() error {
	var errs []error
	for _, n := range c.nodes {
		if n.Conn != nil {
			errs = append(errs, n.Conn.Close())
		}
	}
	return errors.Join(errs...)
}

// ReadID asks the server of the connection for the name of its own
// node with the CLUSTER MYID command.
func ReadID(ctx context.Context, conn *resp.Client) (string, error) {
	reply, err := conn.Exec(ctx, []byte("CLUSTER"), []byte("MYID"))
	if err != nil {
		return "", err
	}
	if reply.Kind() == resp.KindError {
		return "", reply.Err()
	}
	return reply.String(), nil
}

// refreshNodes rediscovers the primary shards of the cluster on the
// connection of one of the nodes of the caller and adopts the topology
// it names: the connections of the nodes the cluster keeps are kept,
// the nodes it dropped are closed and the nodes it gained are dialed
// with the url of the seed. It fails when the caller names no node.
// The topology of the caller is left unchanged when the discovery
// fails, the connection the discovery ran on and the connections it
// dialed for the new nodes are closed.
func refreshNodes(ctx context.Context, old []Node) ([]Node, error) {
	if len(old) == 0 {
		return nil, errors.New("no nodes were provided")
	}
	seed := old[rand.IntN(len(old))].Conn
	nodes, err := topology(ctx, seed)
	if err != nil {
		seed.Close()
		return nil, err
	}
	keep := make(map[string]*resp.Client, len(old))
	for _, n := range old {
		keep[n.ID] = n.Conn
	}
	var fresh []*resp.Client
	for i := range nodes {
		if conn, ok := keep[nodes[i].ID]; ok {
			nodes[i].Conn = conn
			delete(keep, nodes[i].ID)
			continue
		}
		nu := *seed.URL()
		nu.Host = nodes[i].Addr
		conn, err := resp.Dial(ctx, &nu, seed.Proto())
		if err != nil {
			for _, c := range fresh {
				c.Close()
			}
			seed.Close()
			return nil, fmt.Errorf("node %s: %w", nodes[i].Addr, err)
		}
		nodes[i].Conn = conn
		fresh = append(fresh, conn)
	}
	for _, conn := range keep {
		conn.Close()
	}
	return nodes, nil
}

// route returns the index of the node of the topology that serves the
// slot of a key. It fails when no node of the topology serves the
// slot of the key.
func route(nodes []Node, key []byte) (int, error) {
	slot := KeySlot(key)
	for i, n := range nodes {
		if n.Contains(slot) {
			return i, nil
		}
	}
	return 0, fmt.Errorf("clusters: no node serves the slot %d of the key %q", slot, key)
}

// topology asks the server of the connection for the primary shards of its
// cluster with the CLUSTER command, the SHARDS form of a Redis 7
// server first, the SLOTS form of the servers that reject it second.
// The primary shards of the cluster are returned in the order the server
// names them, the replicas are left out: the transfer reads the keys
// of the primary shards and stores them on the primary shards alone. A server that
// runs no cluster rejects the CLUSTER command, it fails the call. The
// context ends the read of the reply. The cluster of a server in
// construction serves no slots yet, it fails the call.
func topology(ctx context.Context, conn *resp.Client) ([]Node, error) {
	reply, err := conn.Exec(ctx, []byte("CLUSTER"), []byte("SHARDS"))
	if err != nil {
		return nil, err
	}
	if reply.Kind() != resp.KindError {
		nodes, err := clusterShards(conn.D, reply)
		if err != nil {
			return nil, fmt.Errorf("unable to read CLUSTER SHARDS: %w", err)
		}
		if len(nodes) == 0 {
			return nil, errors.New("clusters: the cluster carries no slots")
		}
		return nodes, nil
	}
	// A server that runs no cluster rejects the SHARDS form and so
	// do the older servers that take the SLOTS form, the second
	// reply tells the two apart.
	shardsErr := reply.Err()
	reply, err = conn.Exec(ctx, []byte("CLUSTER"), []byte("SLOTS"))
	if err != nil {
		return nil, err
	}
	if reply.Kind() == resp.KindError {
		if isClusterDisabled(shardsErr) {
			return nil, errors.New("clusters: the server runs no cluster")
		}
		return nil, reply.Err()
	}
	nodes, err := clusterSlots(conn.D, reply)
	if err != nil {
		return nil, fmt.Errorf("unable to read CLUSTER SLOTS: %w", err)
	}
	if len(nodes) == 0 {
		return nil, errors.New("clusters: the cluster carries no slots")
	}
	return nodes, nil
}

// clusterShards reads the primary shards of the CLUSTER SHARDS reply. A
// shard names the ranges of the slots it serves and the nodes that
// serve them, the primary shard of the nodes carries the ranges of the
// shard. It fails when the reply or one of its aggregates is not the
// array the form names.
func clusterShards(d resp.Decoder, reply resp.Token) ([]Node, error) {
	if reply.Kind() != resp.KindArray {
		return nil, fmt.Errorf("unexpected token kind %s", reply.Kind())
	}
	var nodes []Node
	shards, shardsErr := reply.Iter(d)
	for shard := range shards {
		if shard.Kind() != resp.KindArray && shard.Kind() != resp.KindMap {
			return nil, fmt.Errorf("unexpected token kind %s", shard.Kind())
		}
		var (
			slots   []rdb.SlotRange
			primary Node
			found   bool
		)
		err := pairsOf(d, shard, func(name string, value resp.Token) error {
			switch name {
			case "slots":
				rs, err := slotRanges(d, value)
				if err != nil {
					return err
				}
				slots = rs
			case "nodes":
				if value.Kind() != resp.KindArray {
					return fmt.Errorf("unexpected token kind %s", value.Kind())
				}
				nodeToks, nodeErr := value.Iter(d)
				for nodeTok := range nodeToks {
					n, isPrimary, err := nodeOf(d, nodeTok)
					if err != nil {
						return err
					}
					if isPrimary {
						primary, found = n, true
					}
				}
				if err := nodeErr(); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		for _, r := range slots {
			if found {
				nodes = nodeAppendSlot(nodes, primary, r)
			}
		}
	}
	if err := shardsErr(); err != nil {
		return nil, err
	}
	return nodes, nil
}

// clusterSlots reads the primary shards of the CLUSTER SLOTS reply, the form
// of the servers that reject the SHARDS one. An entry of the reply
// names the first and the last slot of a range and the primary shard that
// serves it, the replicas of the primary shard follow it and are left out.
// The ranges of a primary shard arrive in an entry each, the primary shard carries
// them all. It fails when the reply or one of its entries is not the
// array the form names.
func clusterSlots(d resp.Decoder, reply resp.Token) ([]Node, error) {
	if reply.Kind() != resp.KindArray {
		return nil, fmt.Errorf("unexpected token kind %s", reply.Kind())
	}
	var nodes []Node
	entries, entriesErr := reply.Iter(d)
	for entry := range entries {
		if entry.Kind() != resp.KindArray {
			return nil, fmt.Errorf("unexpected token kind %s", entry.Kind())
		}
		var start, end uint16
		i := 0
		elems, elemsErr := entry.Iter(d)
		for e := range elems {
			switch i {
			case 0:
				start = uint16(e.Int())
			case 1:
				end = uint16(e.Int())
			case 2:
				n, err := slotPrimary(d, e)
				if err != nil {
					return nil, err
				}
				nodes = nodeAppendSlot(nodes, n, rdb.SlotRange{Start: start, End: end})
			}
			i++
		}
		if err := elemsErr(); err != nil {
			return nil, err
		}
	}
	if err := entriesErr(); err != nil {
		return nil, err
	}
	return nodes, nil
}

// slotPrimary reads the primary shard of an entry of the CLUSTER SLOTS reply:
// the address it announces, its port and its name.
func slotPrimary(d resp.Decoder, t resp.Token) (Node, error) {
	if t.Kind() != resp.KindArray {
		return Node{}, fmt.Errorf("unexpected token kind %s", t.Kind())
	}
	var (
		n        Node
		endpoint string
		port     int64
	)
	next, errf := t.Iter(d)
	i := 0
	for e := range next {
		switch i {
		case 0:
			endpoint = e.String()
		case 1:
			port = e.Int()
		case 2:
			n.ID = e.String()
		}
		i++
	}
	if err := errf(); err != nil {
		return Node{}, err
	}
	n.Addr = net.JoinHostPort(endpoint, strconv.FormatInt(port, 10))
	return n, nil
}

// pairsOf walks the name/value pairs of a map token. A RESP2 map
// arrives as the flat array of its names and values, the elements of
// the iterator alternate in the same way.
func pairsOf(d resp.Decoder, t resp.Token, fn func(name string, value resp.Token) error) error {
	next, errf := t.Iter(d)
	i := 0
	var name string
	for e := range next {
		if i%2 == 0 {
			name = e.String()
		} else if err := fn(name, e); err != nil {
			return err
		}
		i++
	}
	return errf()
}

// slotRanges reads the ranges of the slots of a shard, the integers
// of the reply arrive as the pairs of the first and the last slot of
// a range.
func slotRanges(d resp.Decoder, t resp.Token) ([]rdb.SlotRange, error) {
	if t.Kind() != resp.KindArray {
		return nil, fmt.Errorf("unexpected token kind %s", t.Kind())
	}
	var ranges []rdb.SlotRange
	next, errf := t.Iter(d)
	i := 0
	var start uint16
	for e := range next {
		if i%2 == 0 {
			start = uint16(e.Int())
		} else {
			ranges = append(ranges, rdb.SlotRange{Start: start, End: uint16(e.Int())})
		}
		i++
	}
	if err := errf(); err != nil {
		return nil, err
	}
	if i%2 != 0 {
		return nil, errors.New("clusters: odd slot count in the CLUSTER reply")
	}
	return ranges, nil
}

// nodeOf reads a node of a shard: its name, the address it announces
// and its role in the replication. The second result reports a
// primary shard.
func nodeOf(d resp.Decoder, t resp.Token) (Node, bool, error) {
	if t.Kind() != resp.KindArray && t.Kind() != resp.KindMap {
		return Node{}, false, fmt.Errorf("unexpected token kind %s", t.Kind())
	}
	var (
		n        Node
		endpoint string
		port     int64
		primary  bool
	)
	err := pairsOf(d, t, func(name string, value resp.Token) error {
		switch name {
		case "id":
			n.ID = value.String()
		case "endpoint", "ip":
			// The endpoint names the address of the node,
			// the ip of the older servers is its fallback.
			if endpoint == "" {
				endpoint = value.String()
			}
		case "port":
			port = value.Int()
		case "role":
			primary = value.String() == "master"
		}
		return nil
	})
	if err != nil {
		return Node{}, false, err
	}
	n.Addr = net.JoinHostPort(endpoint, strconv.FormatInt(port, 10))
	return n, primary, nil
}

// nodeAppendSlot gives the range of the slots to the node of its name
// in the topology, a primary shard the reply names again carries the range
// of its next entry.
func nodeAppendSlot(nodes []Node, n Node, r rdb.SlotRange) []Node {
	for i := range nodes {
		if nodes[i].ID == n.ID {
			nodes[i].Slots = append(nodes[i].Slots, r)
			return nodes
		}
	}
	n.Slots = []rdb.SlotRange{r}
	return append(nodes, n)
}

// isClusterDisabled reports whether an error reply of the CLUSTER
// command reports the server that runs no cluster.
func isClusterDisabled(err error) bool {
	return strings.Contains(err.Error(), "cluster support disabled")
}

// crc16Table is the table of the CRC16 the slots of a cluster hash
// with, the CCITT variant of the polynomial 0x1021.
var crc16Table = func() (tab [256]uint16) {
	for i := range tab {
		crc := uint16(i) << 8
		for range 8 {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ 0x1021
			} else {
				crc <<= 1
			}
		}
		tab[i] = crc
	}
	return tab
}()

// crc16 hashes the bytes the way the servers hash the slots of a
// cluster.
func crc16(b []byte) uint16 {
	var crc uint16
	for _, c := range b {
		crc = crc<<8 ^ crc16Table[byte(crc>>8)^c]
	}
	return crc
}

// KeySlot returns the hash slot of a key of a cluster: the CRC16 of
// the key modulo the number of the slots. The hash tag of a key, the
// bytes between its first braces, replaces the key in the hash when
// it is not empty.
func KeySlot(key []byte) uint16 {
	if start := bytes.IndexByte(key, '{'); start >= 0 {
		// An empty tag hashes the key as a whole, the braces
		// of the tag carry no bytes to hash.
		if end := bytes.IndexByte(key[start+1:], '}'); end > 0 {
			return crc16(key[start+1:start+1+end]) & (slotCount - 1)
		}
	}
	return crc16(key) & (slotCount - 1)
}
