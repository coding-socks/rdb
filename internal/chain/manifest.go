package chain

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"sort"
	"strconv"
	"strings"
)

// manifestEntry is one file of a manifest archive.
type manifestEntry struct {
	name string
	data []byte
}

// manifestRead splits a multi part AOF manifest archive into its base
// RDB snapshot and its incremental AOF files. The archive may be
// gzipped, the incremental files are returned in the order the
// manifest lists them.
func manifestRead(data []byte) (base []byte, incr [][]byte, err error) {
	r := io.Reader(bytes.NewReader(data))
	if bytes.HasPrefix(data, []byte{0x1f, 0x8b}) {
		z, err := gzip.NewReader(r)
		if err != nil {
			return nil, nil, err
		}
		defer z.Close()
		r = z
	}
	files := map[string][]byte{}
	var manifest []byte
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, nil, err
		}
		content, err := io.ReadAll(tr)
		if err != nil {
			return nil, nil, err
		}
		if hdr.Name == ".manifest" {
			manifest = content
			continue
		}
		files[hdr.Name] = content
	}

	// The manifest lists one file per line, the base snapshot has
	// type b and the incremental files type i with a sequence number:
	//
	//   file appendonly.aof.1.base.rdb seq 1 type b
	//   file appendonly.aof.1.incr.aof seq 1 type i
	var baseName string
	var incrNames []string
	seq := map[string]int{}
	for _, line := range strings.Split(string(manifest), "\n") {
		f := strings.Fields(line)
		if len(f) != 6 || f[0] != "file" {
			continue
		}
		switch f[5] {
		case "b":
			if baseName == "" {
				baseName = f[1]
			}
		case "i":
			n, err := strconv.Atoi(f[3])
			if err != nil {
				return nil, nil, fmt.Errorf("rdbc: invalid manifest line %q", line)
			}
			incrNames = append(incrNames, f[1])
			seq[f[1]] = n
		}
	}
	if manifest == nil {
		// The archive carries no manifest, the first RDB file is the
		// base snapshot and the AOF files are incremental.
		for _, name := range slices.Sorted(maps.Keys(files)) {
			switch {
			case baseName == "" && strings.HasSuffix(name, ".rdb"):
				baseName = name
			case strings.HasSuffix(name, ".aof"):
				incrNames = append(incrNames, name)
			}
		}
	}
	base, ok := files[baseName]
	if !ok {
		return nil, nil, errors.New("rdbc: the manifest archive carries no base RDB file")
	}
	sort.Slice(incrNames, func(i, j int) bool {
		return seq[incrNames[i]] < seq[incrNames[j]]
	})
	incrs := make([][]byte, 0, len(incrNames))
	for _, name := range incrNames {
		content, ok := files[name]
		if !ok {
			return nil, nil, fmt.Errorf("rdbc: the manifest references the missing file %q", name)
		}
		incrs = append(incrs, content)
	}
	return base, incrs, nil
}
