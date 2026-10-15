package chain

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/coding-socks/rdb/resp"
)

var zeroByte = []byte("0")

const (
	// keyeventChannel is the pattern of the keyevent channels of
	// the database 0.
	keyeventChannel = "__keyevent@0__:*"
	// keyeventPrefix separates the event name from the channel of
	// a keyevent notification.
	keyeventPrefix = "__keyevent@0__:"
)

// Reply receives the reply of one command of a [Bulk]. The elements
// of an aggregate are read with [resp.Token.Iter] or skipped with
// [resp.SkipReply] before it returns, a reply that leaves elements
// unread desyncs the connection. The error a Reply returns fails the
// run of the bulk.
type Reply func(t resp.Token, d resp.Decoder) error

// Send writes one command of a [Bulk] to the connection and queues
// its reply. The arguments are serialized when Send is called, they
// may be the volatile bytes of a reply that is still being read. A
// nil Reply is never called.
type Send func(args [][]byte, reply Reply)

// BulkOption configures a [Bulk], see [NewBulk].
type BulkOption func(*Bulk)

// WithCount sets the count of the SCAN commands of the run, the
// number of keys the server returns per page.
func WithCount(n int64) BulkOption {
	return func(b *Bulk) {
		b.count = strconv.AppendInt(nil, n, 10)
	}
}

// WithMatch sets the pattern of the MATCH option of the SCAN
// commands, the keys of a page all match it. An empty pattern leaves
// the SCAN commands without one.
func WithMatch(pattern []byte) BulkOption {
	return func(b *Bulk) {
		b.match = pattern
	}
}

// WithCursor starts the walk at the cursor of a previous run, the
// first page continues where that run stopped. The walk ends when a
// page reports the cursor 0.
func WithCursor(cursor uint64) BulkOption {
	return func(b *Bulk) {
		b.cursor = strconv.AppendUint(nil, cursor, 10)
	}
}

// Bulk walks the keyspace of the server a [resp.Client] serves. It
// runs one SCAN after the other and hands the keys of every page to
// the visitor of [Bulk.Scan]. A Bulk is not safe for concurrent use,
// its methods are called by the visitor and the replies of its run
// alone.
type Bulk struct {
	c     *resp.Client
	visit func(key []byte)
	err   error // the first write error of the run

	queue []Reply // the replies of the commands written since the last flush

	count  []byte // the count of the SCAN commands, the keys of a page
	match  []byte // the MATCH pattern of the SCAN commands, empty matches every key
	cursor []byte
	done   bool

	notify     func(event, key []byte) error // the callback of the keyspace event notifications
	subscribed bool                          // the run follows the notifications, the callbacks may queue commands
}

// NewBulk constructs a Bulk that walks the keyspace of the server the
// connection serves. The options configure it, the count of the SCAN
// commands defaults to 100.
func NewBulk(c *resp.Client, opts ...BulkOption) *Bulk {
	b := &Bulk{c: c, count: []byte("100")}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

func (b *Bulk) Cursor() uint64 {
	i, _ := strconv.ParseUint(string(b.cursor), 10, 64)
	return i
}

// Send writes one command of the run and queues its reply, see
// [Send]. A write error is stored, the run fails behind it.
func (b *Bulk) Send(args [][]byte, reply Reply) {
	if b.err == nil {
		b.err = b.c.Write(args...)
	}
	b.queue = append(b.queue, reply)
}

// KeyspaceNotifications registers notify as the callback of the
// keyspace event notifications of the run. Scan delivers every
// notification of a key to the callback as the event name and the
// key, copies both, and ends the subscription when the walk ends. The
// server must serve the notifications already, Scan fails when the
// notify-keyspace-events configuration misses their letters. An error
// the callback returns fails the walk. The connection must speak
// RESP3 and the registration must happen before Scan.
func (b *Bulk) KeyspaceNotifications(notify func(event, key []byte) error) {
	b.notify = notify
}

// Scan walks the keyspace and calls the visitor once per key of every
// page, in the order of the page. The key is a copy, it stays valid
// after the visitor returned. The notifications of a
// [Bulk.KeyspaceNotifications] registration reach its callback while
// the walk runs. It returns the first error of the connection, of a
// write, of a reply, or of the context.
func (b *Bulk) Scan(ctx context.Context, visit func(key []byte)) error {
	b.visit = visit
	if b.notify != nil {
		if err := b.subscribe(ctx); err != nil {
			return err
		}
	}
	var spare []Reply // Used for recycling the queue slice.
	for b.err == nil {
		// The subscription ends when the walk is done, the replies
		// of the commands it queued are read behind the end of it.
		if b.done && b.subscribed {
			if err := b.unsubscribe(ctx); err != nil {
				return err
			}
			b.subscribed = false
		}
		if b.done && len(b.queue) == 0 {
			return nil
		}
		// One SCAN of the walk is queued per round, the cursor of
		// the next page is only known behind the reply of this one.
		if !b.done {
			b.scan()
		}
		if err := b.c.Flush(); err != nil {
			return err
		}
		replies := b.queue
		b.queue = spare
		for i := 0; i < len(replies); {
			t, err := b.c.ReadValue(ctx)
			if err != nil {
				return err
			}
			if t.Kind() == resp.KindPush {
				// Intermediate push reply calls notify, not a reply.
				if err := b.deliver(t, b.c.D); err != nil {
					return err
				}
				continue
			}
			reply := replies[i]
			i++
			if reply != nil {
				if err := reply(t, b.c.D); err != nil {
					return err
				}
			}
		}
		spare = replies[:0]
	}
	return b.err
}

// command writes one command of the run and returns its first reply,
// a push between the write and the reply reaches the callback of the
// notifications.
func (b *Bulk) command(ctx context.Context, args ...[]byte) (resp.Token, error) {
	if err := b.c.WriteAndFlush(args...); err != nil {
		return resp.Token{}, err
	}
	for {
		t, err := b.c.ReadValue(ctx)
		if err != nil {
			return resp.Token{}, err
		}
		if t.Kind() != resp.KindPush {
			return t, nil
		}
		if err := b.deliver(t, b.c.D); err != nil {
			return resp.Token{}, err
		}
	}
}

func (b *Bulk) subscribe(ctx context.Context) error {
	if b.c.Proto() != 3 {
		return errors.New("chain: keyspace notifications need a RESP3 connection")
	}
	t, err := b.command(ctx, []byte("CONFIG"), []byte("GET"), []byte("notify-keyspace-events"))
	if err != nil {
		return err
	}
	if t.Kind() == resp.KindError {
		return t.Err()
	}
	var cur string
	iter, errf := t.Iter(b.c.D)
	i := 0
	for el := range iter {
		if i == 1 {
			cur = el.String()
		}
		i++
	}
	if err := errf(); err != nil {
		return err
	}
	if err := keyspaceClasses(cur); err != nil {
		return err
	}
	t, err = b.c.Exec(ctx, []byte("PSUBSCRIBE"), []byte(keyeventChannel))
	if err != nil {
		return err
	}
	if t.Kind() == resp.KindPush {
		// No message can precede the acknowledgment of the
		// subscription.
		if err := b.deliver(t, b.c.D); err != nil {
			return err
		}
		b.subscribed = true
		return nil
	}
	if t.Kind() == resp.KindError {
		return t.Err()
	}
	return errors.New("chain: unexpected reply to the keyspace subscription")
}

func (b *Bulk) unsubscribe(ctx context.Context) error {
	t, err := b.command(ctx, []byte("RESET"))
	if err != nil {
		return err
	}
	if t.Kind() == resp.KindError {
		return t.Err()
	}
	return nil
}

func (b *Bulk) deliver(t resp.Token, d resp.Decoder) error {
	if b.notify == nil || !b.subscribed {
		return resp.SkipReply(d, t)
	}
	iter, errf := t.Iter(d)
	var channel, payload []byte
	i := 0
	for el := range iter {
		// The tokens are volatile, the callback keeps the bytes.
		switch i {
		case 2:
			channel = bytes.Clone(el.Bytes())
		case 3:
			payload = bytes.Clone(el.Bytes())
		}
		i++
	}
	if err := errf(); err != nil {
		return err
	}
	// A pmessage of the keyevent pattern, the other pushes are the
	// acknowledgments of the subscription.
	if i != 4 || !bytes.HasPrefix(channel, []byte(keyeventPrefix)) {
		return nil
	}
	return b.notify(channel[len(keyeventPrefix):], payload)
}

func keyspaceClasses(cur string) error {
	var miss []byte
	for _, c := range []byte{'E', 'A'} {
		if strings.IndexByte(cur, c) < 0 {
			miss = append(miss, c)
		}
	}
	if miss != nil {
		return fmt.Errorf("chain: notify-keyspace-events misses the class letters %s, the keyspace event notifications stay disabled without them", miss)
	}
	return nil
}

func (b *Bulk) scan() {
	cur := b.cursor
	if len(cur) == 0 {
		cur = zeroByte
	}
	args := [][]byte{[]byte("SCAN"), cur, []byte("COUNT"), b.count}
	if len(b.match) > 0 {
		args = append(args, []byte("MATCH"), b.match)
	}
	b.Send(
		args,
		func(t resp.Token, d resp.Decoder) error {
			if t.Kind() == resp.KindError {
				return t.Err()
			}
			if t.IsNil() {
				return errors.New("chain: the SCAN reply carries no cursor")
			}
			iter, errf := t.Iter(d)
			i := 0
			for el := range iter {
				switch i {
				case 0:
					// The tokens are volatile, the element
					// reads overwrite the cursor.
					b.cursor = bytes.Clone(el.Bytes())
					b.done = bytes.Equal(b.cursor, zeroByte)
				case 1:
					if k := el.Kind(); k != resp.KindArray && k != resp.KindSet {
						return fmt.Errorf("chain: unexpected %s in the SCAN reply", k)
					}
					// The elements of the keys are drained even
					// when no visitor is set, the unread input of
					// an aggregate desyncs the connection.
					keys, kf := el.Iter(d)
					for key := range keys {
						if b.visit != nil {
							b.visit(bytes.Clone(key.Bytes()))
						}
					}
					if err := kf(); err != nil {
						return err
					}
				}
				i++
			}
			return errf()
		},
	)
}
