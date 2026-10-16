package chain

import (
	"errors"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/coding-socks/rdb/resp"
	"github.com/coding-socks/rdb/resp/resptest"
)

func TestBulk_Scan(t *testing.T) {
	errorsBoom := errors.New("boom")
	// RESET is served by the connection of the server itself, it
	// never reaches the handler.
	tests := []struct {
		name       string
		cur        string // the notification classes the server serves
		cbErr      error  // the error the callback returns
		wantErr    string // a substring of the error, empty wants none
		wantServed [][]string
	}{
		{
			name:    "the classes of the notifications are missing",
			cur:     "",
			wantErr: "notify-keyspace-events",
			wantServed: [][]string{
				{"CONFIG", "GET", "notify-keyspace-events"},
			},
		},
		{
			name: "the notifications of the walk",
			cur:  "KEA",
			wantServed: [][]string{
				{"CONFIG", "GET", "notify-keyspace-events"},
				{"PSUBSCRIBE", "__keyevent@0__:*"},
				{"SCAN", "0", "COUNT", "1000"},
			},
		},
		{
			name:    "an error of the callback fails the walk",
			cur:     "EA",
			cbErr:   errorsBoom,
			wantErr: "boom",
			wantServed: [][]string{
				{"CONFIG", "GET", "notify-keyspace-events"},
				{"PSUBSCRIBE", "__keyevent@0__:*"},
				{"SCAN", "0", "COUNT", "1000"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				mu     sync.Mutex
				served [][]string
			)
			record := func(cmd *resp.Command) {
				mu.Lock()
				defer mu.Unlock()
				served = append(served, argsOf(cmd))
			}
			mux := resp.NewServeMux()
			mux.HandleFunc("CONFIG|GET", func(w resp.ResponseWriter, cmd *resp.Command) {
				record(cmd)
				p := w.Proto()
				p.WriteToken(resp.ArrayLen(2))
				p.WriteToken(resp.Bytes([]byte("notify-keyspace-events")))
				p.WriteToken(resp.Bytes([]byte(tt.cur)))
			})
			mux.HandleFunc("CONFIG|SET", func(w resp.ResponseWriter, cmd *resp.Command) {
				record(cmd)
				w.Proto().WriteToken(resp.String("OK"))
			})
			mux.HandleFunc("PSUBSCRIBE", func(w resp.ResponseWriter, cmd *resp.Command) {
				record(cmd)
				p := w.Proto()
				p.WriteToken(resp.Push(3))
				p.WriteToken(resp.Bytes([]byte("psubscribe")))
				p.WriteToken(resp.Bytes([]byte("__keyevent@0__:*")))
				p.WriteToken(resp.Int(1))
			})
			// The notification of a key of the page precedes the
			// reply of the page.
			mux.HandleFunc("SCAN", func(w resp.ResponseWriter, cmd *resp.Command) {
				record(cmd)
				p := w.Proto()
				p.WriteToken(resp.Push(4))
				p.WriteToken(resp.Bytes([]byte("pmessage")))
				p.WriteToken(resp.Bytes([]byte("__keyevent@0__:*")))
				p.WriteToken(resp.Bytes([]byte("__keyevent@0__:set")))
				p.WriteToken(resp.Bytes([]byte("k1")))
				p.WriteToken(resp.ArrayLen(2))
				p.WriteToken(resp.Bytes([]byte("0")))
				p.WriteToken(resp.ArrayLen(1))
				p.WriteToken(resp.Bytes([]byte("k1")))
			})
			srv := resptest.NewServer(t, mux)
			u, err := url.Parse("redis://" + srv.Addr)
			if err != nil {
				t.Fatal(err)
			}
			cl, err := resp.Dial(t.Context(), u, 3)
			if err != nil {
				t.Fatal(err)
			}
			defer cl.Close()

			var events [][2]string
			b := NewBulk(cl)
			b.KeyspaceNotifications(func(event, key []byte) error {
				events = append(events, [2]string{string(event), string(key)})
				return tt.cbErr
			})
			var keys []string
			err = b.Scan(t.Context(), func(key []byte) {
				keys = append(keys, string(key))
			})
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("Scan() error = %v, want none", err)
				}
				if !slices.Equal(events, [][2]string{{"set", "k1"}}) {
					t.Errorf("the callback received %v, want [set k1]", events)
				}
				if !slices.Equal(keys, []string{"k1"}) {
					t.Errorf("the visitor received %v, want [k1]", keys)
				}
			} else if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("Scan() error = %v, want it to carry %q", err, tt.wantErr)
			}
			mu.Lock()
			defer mu.Unlock()
			if !reflect.DeepEqual(served, tt.wantServed) {
				t.Errorf("the server served %v, want %v", served, tt.wantServed)
			}
		})
	}
}
