# AGENTS.md

Guidance for coding agents working in this repository.

## Project overview

`github.com/coding-socks/rdb` is a Go library (Go 1.27) that reads **and writes** Redis RDB dump files, plus the `rdbc` CLI (`cmd/rdbc`) that converts, serves, and inspects RDB files and transfers keys to/from Redis-compatible servers (Redis, Valkey, KeyDB). The library is pre-1.0: the public API can change at any time (see README).

## Commands

```sh
go test ./...          # full suite (CI runs exactly this)
go test -short ./...   # skips the Docker-based CLI/chain integration tests
go build ./...
go vet ./...
```

- **`go test -short ./...` skips all Docker-based tests** (`cmd/rdbc` CLI integration, `internal/chain` server tests, `clusters.TestCluster`) and passes without Docker.
- Docker-dependent tests (`cmd/rdbc/rdbc_test.go`, `internal/chain/source_test.go`, `clusters/clusters_test.go`) gate on `testing.Short()` and skip with a message; they need Docker when run in full mode.
- Releases are handled by GoReleaser on `v*` tags (`.goreleaser.yaml`); no manual build steps. `CGO_ENABLED=0`.

### Code generation

Generated files are committed; regeneration is only needed when command JSON specs or the LZF tables change:

- `commands/commands_*.go` are generated from JSON command specs in the `lib/` git submodules (see the `go:generate` directives at the top of `commands/commands.go`). Requires `git submodule update --init` (submodules use SSH URLs and are **not initialized by default**; CI never initializes them) and the `codegen` build tag: `go generate ./commands` or `go run -tags codegen make_commands.go -- ...` from `commands/`.
- `internal/lzf/compress.go` is generated: `go generate ./internal/lzf` (its `//go:generate` names `make_compress.go` directly, so the file's `codegen` build tag does not block it).
- `commands/keydb/*.json` are hand-maintained local copies of KeyDB specs (KeyDB has no upstream JSON).

Never hand-edit generated files (`commands_redis.go`, `commands_valkey.go`, `commands_keydb.go`, `internal/lzf/compress.go`).

## Architecture

The whole codebase is built around **streaming token codecs**, in the style of `encoding/json/jsontext`. There is no DOM/tree: every format is read and written one `Token` at a time.

### Package map

| Package | Role |
|---|---|
| `rdb` (root: `rdb.go`, `dec.go`, `enc.go`) | RDB file codec. `FileDecoder`/`FileEncoder` handle file framing (header, dialect detection, CRC64 checksum); `Decoder`/`Encoder` handle records. |
| `resp` | RESP2/RESP3 client and a `net/http`-style server (`Server`, `Handler`, `ResponseWriter`, `ServeMux` with patterns like `"CONFIG|GET"`). |
| `commands` | Generated command tables (Redis/Valkey/KeyDB), key extraction (the "getkeys" logic incl. `customParsers` for commands the JSON specs can't describe), and a completion `Suggester` (trie + argument-state-graph) used by the rdbc TUI. |
| `aof` | Converts RDB keys into AOF commands that recreate them; also reads AOF streams as RESP arrays. |
| `dump` | `DUMP`/`RESTORE` payload codec: wraps a single RDB record + version + CRC64 into a key payload. |
| `clusters` | Discovers cluster topology via CLUSTER commands, dials nodes, routes keys to the node serving their slot (16384 slots). |
| `internal/chain` | Transfer pipeline: `Src` pushes tokens through `Middleware`s into a `Dst`. `Token = any` — carriers are `rdb.Token`, `aof.Token`, `dump.Token`, or `jsontext.Token`; the first token is a `Head` naming the kinds and dialect of the stream. `Run` owns the `Finish`/`Abort` commit protocol. |
| `internal/lzf`, `internal/crc64jones` | LZF compression and Jones CRC-64 (the RDB checksum). |
| `cmd/rdbc` | CLI: `client`, `transfer`, `serve`, `stat` subcommands, hand-rolled on `flag` (no cobra). TUI parts use charm.land bubbletea/lipgloss. Implementation lives in `cmd/rdbc/internal/commands`. |

### RDB codec internals (root package)

- A record is dispatched by its `OpCode`; each shape has a `XXXDecoder` handler type implementing `TokenDecoder`: `Handle` returns the handler for the *next* token, `nil` when the record is complete. Handlers spanning a whole key chain to a value handler via a `Next` field. See the design note at the top of `dec.go` before touching it.
- Tokens read from a stream hold **volatile raw bytes** (aliased into the decoder's buffer); they are only safe until the next `ReadToken` unless `Token.Clone` is called. Constructor-built tokens own their values.
- Dialects abstract forks: `RedisDialect` (magic `REDIS`, max version 15), `ValkeyDialect` (magic `VALKEY`, version numbering **restarts at 80**; 12–79 are reserved), `KeyDBDialect` (reuses the `REDIS` magic, so it **cannot be auto-detected** — it must be set explicitly). `RegisterDialect` adds third-party forks. Dialects are version-aware: op codes introduced after the bound version are rejected (`ErrOpCodeTooNew`).
- Special op codes count *down* from 0xff (`OpCode = 0xff - iota`); value-type op codes count up from 0. Every dialect shares the core values; forks only diverge at the top of the ranges.

### Data flow for a transfer (`rdbc transfer`)

Source (RDB file / AOF manifest archive / server scan or sync) → `chain.Src` → filters/converters (`chain.Middleware`) → `chain.Dst` (RDB file / AOF manifest archive / server writes). `rdbc serve` replays an RDB file as a master to connecting replicas via the `resp` server. `rdbc stat` computes keyspace size statistics. Mixed-dialect transfers are rejected.

## Testing patterns

- **Round-trip testing is the core technique**: decode a fixture with `FileDecoder`, re-encode every token with `FileEncoder` using the detected dialect, and compare the output byte-for-byte with the fixture (including the checksum). See `rdb_test.go`.
- Fixtures live in `testdata/rdb/`, `testdata/valkey/`, `testdata/keydb/`. Filenames encode dialect feature and RDB version, e.g. `hash_template_v15.rdb`, `stream_v13_idmp.rdb`, `future_v19.rdb` (an intentionally unsupported future version). Corrupt-input fixtures also exist (`corrupt_*.rdb`, `invalid_chksum_v8.rdb`).
- Tests use plain stdlib `testing` (no testify assertions). Table-driven tests throughout.
- In-process end-to-end tests use `resp/resptest.NewServer` (fake RESP server, no Docker) — prefer this over testcontainers when possible. `resptest/testcontainer.go` starts a real Redis in Docker.
- The `cmd/rdbc` tests are true CLI integration tests: `TestMain` builds the binary, runs it as a subprocess with a 30s timeout, and exits the whole test binary early in `-short` mode.

## Conventions

- **Doc comments are literary prose**, not tag lists: "A [Token] is the unit of data a Decoder reads...". Use godoc links (`[Type]`, `[Type.Method]`), and start exported var/const docs with the identifier name (`ErrMissingMagicString indicates that ...`). Every package has a substantial `doc.go` or package comment — maintain that quality when adding exported API.
- Sentinel errors are package-scoped with the package name prefix (`rdb:`, `resp:`, `dump:`), checked with `errors.Is`.
- Comments explain *why* and cross-reference design notes (`// See [FileDecoder]`, "The file framing lives in [FileDecoder]"); deprecated identifiers get a `// Deprecated:` paragraph with a replacement.
- No Makefile, no linter config, no CI beyond `go test ./...`. Don't introduce tooling.
- `go vet` currently reports a few pre-existing warnings (`commands/suggest.go` self-assignments) — don't treat vet output as clean-baseline; don't fix unrelated warnings in passing.

## Gotchas

- RDB format reference docs live in-repo: `rdb_file_format.md` (the project's own spec, includes the dialect/version table) and `format.md` (third-party format description). Consult these before changing decoder/encoder logic.
- An all-zero checksum is *valid* — that is how Redis disables checksums. Don't "fix" this.
- `commands.Table` keys must be upper case; `Lookup` is case-insensitive and resolves container subcommands (e.g. `CONFIG GET`).
- RESP protocol switches (HELLO) happen mid-session: the decoder's unread input must survive via `Handoff`. Preserve this when touching `resp`.
- URL schemes `redis|rediss|valkey|valkeys|keydb|keydbs` are accepted for servers; `insecure=true` query param skips TLS verification.
- The `lib/` submodules are reference sources only (for command JSON specs); nothing imports them at build time.
