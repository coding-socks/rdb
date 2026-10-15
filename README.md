[![Go Reference](https://pkg.go.dev/badge/github.com/coding-socks/rdb.svg)](https://pkg.go.dev/github.com/coding-socks/rdb)

# RDB

An RDB (Redis Database) reader written in Go.

## Introduction

> RDB is a very compact single-file point-in-time representation of your Redis data.

Source: https://redis.io/docs/management/persistence/#rdb-advantages

The goal of this project is to create an implementation based on the documents and during the implementation, learn.

## Production readiness

**This project is still in development phase.** In this stage the public API can change any time.

Beta version will be considered when the feature set covers the documents the implementation is based on, and the public
API is reached a mature state.

Stable version will be considered only if enough positive feedback is gathered to lock the public API.

## Documents

### Official sites

- [librdb](https://github.com/redis/librdb) (Draft)
- [Official RDB implementation](https://github.com/redis/redis/blob/unstable/src/rdb.c)
- [Official RESP2 specifications](https://github.com/redis/redis-specifications/blob/master/protocol/RESP2.md)
- [Official RESP3 specifications](https://github.com/redis/redis-specifications/blob/master/protocol/RESP3.md)

### Unofficial sites

- [RDB File Format](https://github.com/sripathikrishnan/redis-rdb-tools/wiki/Redis-RDB-Dump-File-Format) (Version 4)
- [RDB File Format](https://rdb.fnordig.de/file_format.html) (Version 7)

## Inspiration

Inspiration for the implementation comes from the following places:

- https://pkg.go.dev/golang.org/x/image/riff
- https://pkg.go.dev/database/sql#Rows
- https://github.com/go-json-experiment/json/jsontext

## Similar libraries

Last updated: 2026-10-12

| URL                                | Status                      |
|------------------------------------|-----------------------------|
| https://github.com/HDT3213/rdb     | In active development       |
| https://github.com/siddontang/rdb  | Last updated on 17 Mar 2015 |
| https://github.com/tent/rdb        | Archived                    |
| https://github.com/tommy351/rdb-go | Last updated on 31 Oct 2023 |
