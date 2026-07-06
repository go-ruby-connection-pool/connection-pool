// Copyright (c) the go-ruby-connection-pool/connection-pool authors
//
// SPDX-License-Identifier: BSD-3-Clause

package connectionpool

import (
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// waitUntil spins until cond holds or the deadline passes, yielding the scheduler
// each turn. It lets a test block deterministically on another goroutine reaching
// a state (e.g. a caller parking in Pop) without sleeping for a fixed duration.
func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met before deadline")
		}
		runtime.Gosched()
	}
}

// counter is a trivial connection: a stateful value the pool hands out, so tests
// can assert identity and that a delegated method observes state.
type counter struct{ n int }

func (c *counter) inc() int { c.n++; return c.n }

// newSeq returns a factory that produces distinct, monotonically-numbered
// connections, so a test can tell which connection it received.
func newSeq() Factory {
	var id int64
	return func() any {
		return &counter{n: int(atomic.AddInt64(&id, 1)) * 100}
	}
}

func TestNewAndSizeAvailable(t *testing.T) {
	p := New(2, time.Second, newSeq())
	if p.Size() != 2 {
		t.Fatalf("Size = %d, want 2", p.Size())
	}
	if p.Available() != 2 {
		t.Fatalf("Available = %d, want 2 (nothing created yet)", p.Available())
	}
}

func TestWithBorrowsAndReturns(t *testing.T) {
	p := New(1, time.Second, newSeq())
	var got any
	res, err := p.With(1, func(conn any) (any, error) {
		got = conn
		if p.Available() != 0 {
			t.Fatalf("Available during checkout = %d, want 0", p.Available())
		}
		return "done", nil
	})
	if err != nil || res != "done" {
		t.Fatalf("With = (%v, %v), want (done, nil)", res, err)
	}
	if got == nil {
		t.Fatal("block did not receive a connection")
	}
	if p.Available() != 1 {
		t.Fatalf("Available after checkin = %d, want 1", p.Available())
	}
}

func TestWithPropagatesBlockError(t *testing.T) {
	p := New(1, time.Second, newSeq())
	sentinel := errors.New("boom")
	_, err := p.With(1, func(conn any) (any, error) { return nil, sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatalf("With err = %v, want %v", err, sentinel)
	}
	if p.Available() != 1 {
		t.Fatalf("connection not returned after block error: Available = %d", p.Available())
	}
}

func TestWithCheckinRunsOnPanic(t *testing.T) {
	p := New(1, time.Second, newSeq())
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic to propagate")
			}
		}()
		_, _ = p.With(1, func(conn any) (any, error) { panic("kaboom") })
	}()
	if p.Available() != 1 {
		t.Fatalf("connection not returned after panic: Available = %d", p.Available())
	}
}

func TestReentrantCheckoutReusesConnection(t *testing.T) {
	p := New(1, time.Second, newSeq())
	outer, err := p.Checkout(1, time.Second)
	if err != nil {
		t.Fatalf("outer checkout: %v", err)
	}
	if p.Available() != 0 {
		t.Fatalf("Available after first checkout = %d, want 0", p.Available())
	}
	inner, err := p.Checkout(1, time.Second) // same caller key -> nested
	if err != nil {
		t.Fatalf("inner checkout: %v", err)
	}
	if inner != outer {
		t.Fatal("nested checkout returned a different connection")
	}
	if p.Available() != 0 {
		t.Fatalf("nested checkout consumed another slot: Available = %d", p.Available())
	}
	if err := p.Checkin(1); err != nil { // unwind inner: still held
		t.Fatalf("inner checkin: %v", err)
	}
	if p.Available() != 0 {
		t.Fatalf("connection returned too early: Available = %d", p.Available())
	}
	if err := p.Checkin(1); err != nil { // unwind outer: returned now
		t.Fatalf("outer checkin: %v", err)
	}
	if p.Available() != 1 {
		t.Fatalf("connection not returned after outermost checkin: Available = %d", p.Available())
	}
}

func TestReentrantWithNested(t *testing.T) {
	p := New(1, time.Second, newSeq())
	_, err := p.With(7, func(outer any) (any, error) {
		return p.With(7, func(inner any) (any, error) {
			if inner != outer {
				t.Fatal("nested With got a different connection")
			}
			return nil, nil
		})
	})
	if err != nil {
		t.Fatalf("nested With: %v", err)
	}
	if p.Available() != 1 {
		t.Fatalf("Available after nested With = %d, want 1", p.Available())
	}
}

func TestCheckinWithoutCheckout(t *testing.T) {
	p := New(1, time.Second, newSeq())
	err := p.Checkin(1)
	var perr *Error
	if !errors.As(err, &perr) {
		t.Fatalf("Checkin err = %v, want *Error", err)
	}
	if perr.Error() != "no connections are checked out" {
		t.Fatalf("message = %q", perr.Error())
	}
}

func TestCheckoutTimesOutWhenExhausted(t *testing.T) {
	p := New(1, 0, newSeq()) // zero timeout -> immediate failure when empty
	if _, err := p.Checkout(1, 0); err != nil {
		t.Fatalf("first checkout should succeed: %v", err)
	}
	_, err := p.Checkout(2, 0) // different caller, pool exhausted
	var terr *TimeoutError
	if !errors.As(err, &terr) {
		t.Fatalf("Checkout err = %v, want *TimeoutError", err)
	}
	if terr.Error() == "" {
		t.Fatal("TimeoutError message is empty")
	}
}

func TestCheckoutBlocksThenWakesOnCheckin(t *testing.T) {
	p := New(1, 2*time.Second, newSeq())
	held, err := p.Checkout(1, 2*time.Second)
	if err != nil {
		t.Fatalf("hold checkout: %v", err)
	}

	type result struct {
		conn any
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		conn, err := p.Checkout(2, 2*time.Second)
		ch <- result{conn, err}
	}()

	// Wait until the second caller has actually parked in Pop, then release.
	waitUntil(t, func() bool { return p.stack.waiting() == 1 })
	if err := p.Checkin(1); err != nil {
		t.Fatalf("checkin: %v", err)
	}

	got := <-ch
	if got.err != nil {
		t.Fatalf("woken checkout err = %v", got.err)
	}
	if got.conn != held {
		t.Fatal("woken caller did not receive the released connection")
	}
	if err := p.Checkin(2); err != nil {
		t.Fatalf("final checkin: %v", err)
	}
	assertNoLeak(t)
}

func TestCheckoutTimesOutWhileWaiting(t *testing.T) {
	p := New(1, time.Second, newSeq())
	if _, err := p.Checkout(1, time.Second); err != nil {
		t.Fatalf("hold checkout: %v", err)
	}
	// Positive timeout with no checkin: the caller parks, the timer fires, and
	// Pop returns a TimeoutError.
	_, err := p.Checkout(2, 25*time.Millisecond)
	var terr *TimeoutError
	if !errors.As(err, &terr) {
		t.Fatalf("Checkout err = %v, want *TimeoutError", err)
	}
	assertNoLeak(t)
}

func TestShutdownDisposesIdleAndBlocksCheckout(t *testing.T) {
	p := New(2, time.Second, newSeq())
	a, _ := p.Checkout(1, time.Second)
	b, _ := p.Checkout(2, time.Second)
	_ = p.Checkin(1)
	_ = p.Checkin(2) // que now holds both

	var disposed []any
	p.Shutdown(func(conn any) { disposed = append(disposed, conn) })
	if len(disposed) != 2 {
		t.Fatalf("shutdown disposed %d connections, want 2", len(disposed))
	}
	// Disposed in reverse (stack) order: b was pushed last.
	if disposed[0] != b || disposed[1] != a {
		t.Fatal("shutdown did not dispose in stack order")
	}

	_, err := p.Checkout(3, time.Second)
	var derr *PoolShuttingDownError
	if !errors.As(err, &derr) {
		t.Fatalf("Checkout after shutdown = %v, want *PoolShuttingDownError", err)
	}
	if derr.Error() == "" {
		t.Fatal("PoolShuttingDownError message is empty")
	}
}

func TestShutdownDisposesConnectionReturnedAfterShutdown(t *testing.T) {
	p := New(1, time.Second, newSeq())
	held, _ := p.Checkout(1, time.Second) // out of the pool during shutdown

	var disposed []any
	p.Shutdown(func(conn any) { disposed = append(disposed, conn) })
	if len(disposed) != 0 {
		t.Fatalf("nothing idle to dispose yet, got %d", len(disposed))
	}
	// Returning the still-held connection must route it to the shutdown block.
	if err := p.Checkin(1); err != nil {
		t.Fatalf("checkin after shutdown: %v", err)
	}
	if len(disposed) != 1 || disposed[0] != held {
		t.Fatalf("late-returned connection not disposed: %v", disposed)
	}
}

func TestShutdownRequiresBlock(t *testing.T) {
	p := New(1, time.Second, newSeq())
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("Shutdown(nil) should panic")
		}
	}()
	p.Shutdown(nil)
}

func TestReloadResetsPool(t *testing.T) {
	p := New(1, time.Second, newSeq())
	c, _ := p.Checkout(1, time.Second)
	_ = p.Checkin(1) // que holds c

	var disposed []any
	p.Reload(func(conn any) { disposed = append(disposed, conn) })
	if len(disposed) != 1 || disposed[0] != c {
		t.Fatalf("reload did not dispose idle connection: %v", disposed)
	}
	if p.Available() != 1 {
		t.Fatalf("Available after reload = %d, want 1", p.Available())
	}
	// The pool works again and mints a fresh connection.
	fresh, err := p.Checkout(1, time.Second)
	if err != nil {
		t.Fatalf("checkout after reload: %v", err)
	}
	if fresh == c {
		t.Fatal("reload should have created a fresh connection")
	}
	_ = p.Checkin(1)
}

// assertNoLeak checks that background waiters/timer goroutines have all exited,
// so the timeout machinery does not leak goroutines.
func assertNoLeak(t *testing.T) {
	t.Helper()
	base := runtime.NumGoroutine()
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > base && time.Now().Before(deadline) {
		runtime.Gosched()
	}
}
