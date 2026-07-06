<p align="center"><img src="https://raw.githubusercontent.com/go-ruby-connection-pool/brand/main/social/go-ruby-connection-pool-connection-pool.png" alt="go-ruby-connection-pool/connection-pool" width="720"></p>

# connection-pool — go-ruby-connection-pool

[![Docs](https://img.shields.io/badge/docs-mkdocs--material-DC2626)](https://go-ruby-connection-pool.github.io/docs/)
[![License](https://img.shields.io/badge/license-BSD--3--Clause-blue)](LICENSE)
[![Go](https://img.shields.io/badge/go-1.26.4%2B-00ADD8)](https://go.dev/dl/)
[![Coverage](https://img.shields.io/badge/coverage-100%25-1a7f37)](#tests--coverage)

**A pure-Go (no cgo) reimplementation of Ruby's [`connection_pool`](https://github.com/mperham/connection_pool) gem**
— the generic, thread-safe pool of reusable connections — faithful to the
observable behaviour of `connection_pool` on MRI 4.0.5. It mirrors the gem's
lazy creation up to a fixed size, its timeout-bounded checkout that raises
`ConnectionPool::TimeoutError` on exhaustion, its per-thread reentrant checkout,
`shutdown` / `reload` disposal, and the method-delegating `ConnectionPool::Wrapper`
— **without any Ruby runtime**.

It is the `connection_pool` backend for
[go-embedded-ruby](https://github.com/go-embedded-ruby/ruby), but is a
**standalone, reusable** module with no dependency on the Ruby runtime — a sibling
of [go-ruby-set](https://github.com/go-ruby-set/set) and
[go-ruby-bigdecimal](https://github.com/go-ruby-bigdecimal/bigdecimal).

> **MRI-faithful, not Composition-Oriented.** This is the *gem*'s pool — every
> semantics decision matches what MRI 4.0.5 + `connection_pool` does, verified by a
> differential oracle that runs the real gem side by side with this package.

## Connections are opaque

A "connection" is **any value** the factory returns; the pool never inspects it.
Redis clients, database handles, sockets, or plain Go structs all work. The pool
only tracks ownership and lifetime — how many exist, who holds one, and when each
returns.

```go
type Factory func() any
```

The factory is called **lazily**, at most `size` times over the pool's life, the
first time a caller needs a connection that is not already idle.

## Reentrancy — the caller-key seam

In MRI the checked-out connection lives in **thread-local** storage, so a nested
`with` on the same thread reuses the same connection (a depth counter decides when
it truly returns to the pool). Go has no thread-locals, so the host identifies the
logical caller with a **`CallerKey`**:

```go
type CallerKey any
```

Two `Checkout`/`With` calls sharing a key **nest** — the inner one reuses the
outer's connection and the connection returns to the pool only when the outermost
`Checkin` runs — exactly as two nested `with` blocks on one Ruby thread do. The
[go-embedded-ruby](https://github.com/go-embedded-ruby/ruby) binding plugs the
current Ruby thread/fiber in here, so `require "connection_pool"` behaves
identically to MRI.

## Install

```sh
go get github.com/go-ruby-connection-pool/connection-pool
```

## Usage

```go
package main

import (
	"fmt"
	"time"

	connectionpool "github.com/go-ruby-connection-pool/connection-pool"
)

func main() {
	// ConnectionPool.new(size: 2, timeout: 5) { Conn.new }
	pool := connectionpool.New(2, 5*time.Second, func() any { return newConn() })

	// pool.with { |conn| conn.query(...) }  — the CallerKey stands in for the thread.
	res, err := pool.With(currentCaller(), func(conn any) (any, error) {
		return conn.(*Conn).Query("SELECT 1"), nil
	})
	fmt.Println(res, err)

	// A Wrapper delegates any method straight through the pool, like the gem's:
	//   ConnectionPool::Wrapper.new(pool: pool)
	w := connectionpool.NewWrapper(pool, currentCaller, dispatch)
	_, _ = w.Call("query", "SELECT 1") // borrows, calls query, returns the conn

	pool.Shutdown(func(conn any) { conn.(*Conn).Close() }) // dispose every idle conn
}
```

## API

```go
type Factory func() any        // creates a connection (any value)
type CallerKey any             // identifies the logical caller (thread/fiber)

// ConnectionPool — models ConnectionPool
func New(size int, timeout time.Duration, factory Factory) *ConnectionPool
func (p *ConnectionPool) With(key CallerKey, fn func(conn any) (any, error)) (any, error) // with
func (p *ConnectionPool) Checkout(key CallerKey, timeout time.Duration) (any, error)      // checkout
func (p *ConnectionPool) Checkin(key CallerKey) error                                     // checkin
func (p *ConnectionPool) Shutdown(block func(conn any))                                    // shutdown
func (p *ConnectionPool) Reload(block func(conn any))                                      // reload
func (p *ConnectionPool) Size() int                                                       // size
func (p *ConnectionPool) Available() int                                                  // available

// Wrapper — models ConnectionPool::Wrapper (method delegation through the pool)
type Dispatch func(conn any, name string, args ...any) any // the host's `send`
type KeyFunc func() CallerKey
func NewWrapper(pool *ConnectionPool, keyFn KeyFunc, dispatch Dispatch) *Wrapper
func Wrap(size int, timeout time.Duration, factory Factory, keyFn KeyFunc, dispatch Dispatch) *Wrapper
func (w *Wrapper) Call(name string, args ...any) (any, error)              // method_missing
func (w *Wrapper) With(fn func(conn any) (any, error)) (any, error)        // with
func (w *Wrapper) RespondTo(name string, respond func(conn any) bool) (bool, error) // respond_to?
func (w *Wrapper) WrappedPool() *ConnectionPool                            // wrapped_pool
func (w *Wrapper) PoolShutdown(block func(conn any))                       // pool_shutdown
func (w *Wrapper) PoolSize() int                                          // pool_size
func (w *Wrapper) PoolAvailable() int                                    // pool_available

// TimedStack — models ConnectionPool::TimedStack (the bounded, timed backing store)
type TimedStack struct{ /* ... */ }
func NewTimedStack(size int, factory Factory) *TimedStack
func (s *TimedStack) Pop(timeout time.Duration) (any, error)
func (s *TimedStack) Push(obj any)
func (s *TimedStack) Shutdown(block func(conn any), reload bool)
func (s *TimedStack) Length() int
func (s *TimedStack) Max() int

// Errors — model the gem's hierarchy
type Error struct{ Msg string }                 // ConnectionPool::Error
type PoolShuttingDownError struct{}             // ConnectionPool::PoolShuttingDownError
type TimeoutError struct{ Msg string }          // ConnectionPool::TimeoutError
```

## Semantics

- **Lazy, bounded creation.** The factory runs on demand, never more than `size`
  times; `Available()` counts idle connections plus those not yet created.
- **Timeout.** `Checkout` blocks up to the timeout for a free connection and
  returns a `*TimeoutError` once the deadline passes. A zero timeout fails
  immediately when the pool is exhausted.
- **Reentrancy.** A nested `Checkout`/`With` on the same `CallerKey` reuses the
  same connection and only returns it to the pool on the outermost `Checkin`.
- **Shutdown / reload.** `Shutdown` disposes every idle connection (LIFO) through
  the block and makes later checkouts fail with `*PoolShuttingDownError`; a
  connection still checked out is disposed when it is returned. `Reload` disposes
  the idle connections and resets the pool for reuse.
- **Thread-safe.** A `sync.Mutex` + condition variable back the pool; the
  timeout machinery never leaks goroutines, and the suite runs under `-race`.

## Tests & coverage

```sh
go test -race ./...
```

The suite holds **100% line coverage** — including the timeout, exhaustion,
shutdown, reload, and reentrancy paths — and cross-compiles on all six supported
64-bit targets (`amd64`, `arm64`, `riscv64`, `loong64`, `ppc64le`, `s390x`,
including the big-endian `s390x`). A differential **MRI oracle** runs the real
`connection_pool` gem side by side with this package on the lanes where Ruby and
the gem are present; it skips itself elsewhere, so the deterministic, Ruby-free
tests alone drive the coverage gate.

## License

BSD-3-Clause — see [LICENSE](LICENSE). Copyright the go-ruby-connection-pool/connection-pool authors.
