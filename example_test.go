package rdb_test

import (
	"fmt"
	"log"
	"strings"

	"github.com/coding-socks/rdb"
)

func ExampleNewFileDecoder() {
	d := rdb.NewFileDecoder(strings.NewReader(data), nil)
	dial, err := d.Dialect()
	if err != nil {
		log.Fatalf("could not read the header: %v", err)
	}
	magic, ver := dial.Header()
	fmt.Printf("%s%d\n", magic, ver)

	// Output: REDIS11
}

var data = "REDIS0011"
