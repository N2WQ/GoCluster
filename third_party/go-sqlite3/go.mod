module github.com/ncruces/go-sqlite3

go 1.26.0

require (
	github.com/ncruces/go-sqlite3-wasm/v6 v6.3.35304
	github.com/ncruces/julianday v1.0.0
	golang.org/x/sys v0.38.0
)

replace github.com/ncruces/go-sqlite3-wasm/v6 => ./engine
