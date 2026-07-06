// Copyright (c) the go-ruby-connection-pool/connection-pool authors
//
// SPDX-License-Identifier: BSD-3-Clause

// Package connectionpool is a pure-Go (no cgo) reimplementation of Ruby's
// connection_pool gem (v2, MRI 4.0.5) — a generic, thread-safe pool of reusable
// connections. It mirrors the gem's observable behaviour — lazy creation up to a
// fixed size, a timeout-bounded checkout that raises ConnectionPool::TimeoutError
// on exhaustion, per-caller reentrant checkout, Shutdown/Reload disposal, and the
// method-delegating Wrapper — without any Ruby runtime. It is the connection_pool
// backend for go-embedded-ruby but is a standalone, reusable module.
//
// # Connections are opaque
//
// A "connection" is any value the Factory returns; the pool never inspects it.
// Redis clients, database handles, or plain Go structs all work. The pool only
// tracks ownership and lifetime.
//
// # Reentrancy — the caller-key seam
//
// The gem stores the checked-out connection in thread-local storage, so a nested
// `with` on the same thread reuses the same connection (and a depth counter
// decides when it truly returns to the pool). Go has no thread-locals, so the
// host identifies the logical caller with a CallerKey: two checkouts sharing a
// key nest exactly as two nested `with` blocks on one Ruby thread do. The
// go-embedded-ruby binding supplies the current Ruby thread/fiber as the key.
package connectionpool

import (
	"sync"
	"time"
)

// CallerKey identifies the logical caller (a Ruby thread or fiber) for reentrant
// checkout. Any comparable value works. Two Checkout calls with an equal key
// nest — the second reuses the first's connection — and the connection returns to
// the pool only when the outermost Checkin runs. It stands in for the gem's
// per-thread storage.
type CallerKey any

// ConnectionPool is a thread-safe pool of reusable connections. The zero value is
// not usable; construct it with New.
type ConnectionPool struct {
	stack   *TimedStack
	timeout time.Duration

	mu      sync.Mutex
	checked map[CallerKey]*checkoutState
}

// checkoutState is a caller's in-flight checkout: the connection it holds and how
// deeply it has re-checked it out (the reentrancy depth counter from the gem).
type checkoutState struct {
	conn  any
	count int
}

// New builds a pool that lazily creates at most size connections through factory
// and blocks up to timeout in Checkout waiting for a free one. It mirrors
// ConnectionPool.new(size:, timeout:, &factory).
func New(size int, timeout time.Duration, factory Factory) *ConnectionPool {
	return &ConnectionPool{
		stack:   NewTimedStack(size, factory),
		timeout: timeout,
		checked: make(map[CallerKey]*checkoutState),
	}
}

// With checks out a connection for key, runs fn with it, and checks it back in
// even if fn panics — the faithful analogue of ConnectionPool#with's ensure. It
// returns fn's result, or the *TimeoutError / *PoolShuttingDownError from an
// unsuccessful checkout (in which case fn does not run).
func (p *ConnectionPool) With(key CallerKey, fn func(conn any) (any, error)) (any, error) {
	conn, err := p.Checkout(key, p.timeout)
	if err != nil {
		return nil, err
	}
	defer p.Checkin(key)
	return fn(conn)
}

// Checkout returns a connection for key, blocking up to timeout. If key already
// holds a connection (a nested checkout on the same caller), it returns that same
// connection and deepens the reentrancy counter instead of taking another from
// the pool. Mirrors ConnectionPool#checkout.
func (p *ConnectionPool) Checkout(key CallerKey, timeout time.Duration) (any, error) {
	p.mu.Lock()
	if st := p.checked[key]; st != nil {
		st.count++
		conn := st.conn
		p.mu.Unlock()
		return conn, nil
	}
	p.mu.Unlock()

	conn, err := p.stack.Pop(timeout)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	p.checked[key] = &checkoutState{conn: conn, count: 1}
	p.mu.Unlock()
	return conn, nil
}

// Checkin returns key's connection to the pool. On a nested checkout it only
// unwinds one level of the reentrancy counter; the connection returns to the pool
// when the outermost Checkin runs. It returns an *Error if key holds no
// connection. Mirrors ConnectionPool#checkin.
func (p *ConnectionPool) Checkin(key CallerKey) error {
	p.mu.Lock()
	st := p.checked[key]
	if st == nil {
		p.mu.Unlock()
		return &Error{Msg: "no connections are checked out"}
	}
	if st.count > 1 {
		st.count--
		p.mu.Unlock()
		return nil
	}
	delete(p.checked, key)
	p.mu.Unlock()
	p.stack.Push(st.conn)
	return nil
}

// Shutdown disposes every idle connection through block and marks the pool shut
// down, so later checkouts fail and connections still checked out are disposed
// when returned. block must not be nil. Mirrors ConnectionPool#shutdown.
func (p *ConnectionPool) Shutdown(block func(conn any)) {
	p.stack.Shutdown(block, false)
}

// Reload disposes every idle connection through block and resets the pool for
// reuse, so it will create fresh connections on demand again. Mirrors
// ConnectionPool#reload.
func (p *ConnectionPool) Reload(block func(conn any)) {
	p.stack.Shutdown(block, true)
}

// Size reports the maximum number of connections the pool will create. Mirrors
// ConnectionPool#size.
func (p *ConnectionPool) Size() int { return p.stack.Max() }

// Available reports how many connections can be checked out right now without
// waiting (idle plus not-yet-created). Mirrors ConnectionPool#available.
func (p *ConnectionPool) Available() int { return p.stack.Length() }
