// Copyright (c) the go-ruby-connection-pool/connection-pool authors
//
// SPDX-License-Identifier: BSD-3-Clause

package connectionpool

import "time"

// Dispatch invokes method name with args on a connection and returns the result.
// It is the seam through which Wrapper delegates arbitrary calls to a pooled
// connection: the go-embedded-ruby binding plugs in Ruby's send, so a Wrapper
// behaves like the connection itself. Mirrors the connection.send(...) inside
// ConnectionPool::Wrapper#method_missing.
type Dispatch func(conn any, name string, args ...any) any

// KeyFunc yields the CallerKey of the current logical caller. Wrapper calls it
// per delegation, exactly as the gem reads Thread.current inside pool.with.
type KeyFunc func() CallerKey

// Wrapper delegates method calls to a connection borrowed from a pool for the
// duration of each call. It is a faithful port of ConnectionPool::Wrapper: every
// delegated call checks a connection out, invokes the method, and checks it back
// in. Because delegation runs through With, nested delegated calls on the same
// caller reuse one connection, so a Wrapper is safe to treat as if it were a
// single connection.
type Wrapper struct {
	pool     *ConnectionPool
	keyFn    KeyFunc
	dispatch Dispatch
}

// NewWrapper wraps pool so that Call delegates to a pooled connection. keyFn
// supplies the current caller key (for reentrancy) and dispatch performs the
// method invocation. Mirrors ConnectionPool::Wrapper.new(pool:).
func NewWrapper(pool *ConnectionPool, keyFn KeyFunc, dispatch Dispatch) *Wrapper {
	return &Wrapper{pool: pool, keyFn: keyFn, dispatch: dispatch}
}

// Wrap builds a fresh pool and wraps it in one call, mirroring
// ConnectionPool.wrap(size:, timeout:) { factory }.
func Wrap(size int, timeout time.Duration, factory Factory, keyFn KeyFunc, dispatch Dispatch) *Wrapper {
	return NewWrapper(New(size, timeout, factory), keyFn, dispatch)
}

// Call borrows a connection and invokes method name with args on it, returning
// the method's result. It is the analogue of ConnectionPool::Wrapper#method_missing:
// any call the connection understands can be made straight on the Wrapper. It
// returns the checkout error (a *TimeoutError / *PoolShuttingDownError) if no
// connection is available.
func (w *Wrapper) Call(name string, args ...any) (any, error) {
	return w.pool.With(w.keyFn(), func(conn any) (any, error) {
		return w.dispatch(conn, name, args...), nil
	})
}

// With runs fn with a borrowed connection, exactly like the pool's With but using
// the Wrapper's caller-key source. Mirrors ConnectionPool::Wrapper#with.
func (w *Wrapper) With(fn func(conn any) (any, error)) (any, error) {
	return w.pool.With(w.keyFn(), fn)
}

// RespondTo reports whether a delegated call to name would resolve: either it is
// one of the Wrapper's own methods, or a borrowed connection responds to it
// (probed through respond). Mirrors ConnectionPool::Wrapper#respond_to?.
func (w *Wrapper) RespondTo(name string, respond func(conn any) bool) (bool, error) {
	if wrapperMethods[name] {
		return true, nil
	}
	res, err := w.pool.With(w.keyFn(), func(conn any) (any, error) {
		return respond(conn), nil
	})
	if err != nil {
		return false, err
	}
	return res.(bool), nil
}

// wrapperMethods are the names the Wrapper answers itself rather than delegating,
// matching ConnectionPool::Wrapper::METHODS.
var wrapperMethods = map[string]bool{
	"with":          true,
	"pool_shutdown": true,
	"wrapped_pool":  true,
}

// WrappedPool returns the underlying pool. Mirrors ConnectionPool::Wrapper#wrapped_pool.
func (w *Wrapper) WrappedPool() *ConnectionPool { return w.pool }

// PoolShutdown disposes the underlying pool through block. Mirrors
// ConnectionPool::Wrapper#pool_shutdown.
func (w *Wrapper) PoolShutdown(block func(conn any)) { w.pool.Shutdown(block) }

// PoolSize reports the underlying pool's size. Mirrors ConnectionPool::Wrapper#pool_size.
func (w *Wrapper) PoolSize() int { return w.pool.Size() }

// PoolAvailable reports the underlying pool's availability. Mirrors
// ConnectionPool::Wrapper#pool_available.
func (w *Wrapper) PoolAvailable() int { return w.pool.Available() }
