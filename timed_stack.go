// Copyright (c) the go-ruby-connection-pool/connection-pool authors
//
// SPDX-License-Identifier: BSD-3-Clause

package connectionpool

import (
	"fmt"
	"sync"
	"time"
)

// Factory creates a new connection. It mirrors the block passed to
// ConnectionPool.new / TimedStack.new: it is invoked lazily, at most Size times
// over the life of the stack, the first time a caller needs a connection that is
// not already parked in the stack.
type Factory func() any

// TimedStack is a bounded, lazily-populated stack of connections with a
// timeout-bounded Pop. It is a faithful port of ConnectionPool::TimedStack: at
// most max connections are ever created (created on demand by the factory), Pop
// blocks up to the timeout waiting for a free connection and returns a
// *TimeoutError on exhaustion, and Shutdown disposes every connection and makes
// further Pops return a *PoolShuttingDownError.
type TimedStack struct {
	mu       sync.Mutex
	cond     *sync.Cond
	que      []any
	max      int
	created  int
	factory  Factory
	shutdown func(conn any)
	isDown   bool
	waiters  int

	// now is the clock, injectable so timeout paths are deterministic in tests.
	now func() time.Time
}

// NewTimedStack builds a stack that will create at most size connections through
// factory. A size of zero means the stack never creates a connection, so every
// Pop that finds the stack empty times out — matching the gem.
func NewTimedStack(size int, factory Factory) *TimedStack {
	s := &TimedStack{max: size, factory: factory, now: time.Now}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// Push returns obj to the stack, or hands it to the shutdown block if the stack
// is shutting down, then wakes a waiter. Mirrors TimedStack#push.
func (s *TimedStack) Push(obj any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.isDown {
		s.shutdown(obj)
	} else {
		s.que = append(s.que, obj)
	}
	s.cond.Broadcast()
}

// Pop returns a connection, blocking up to timeout for one to become available.
// It returns a parked connection if any, otherwise creates a fresh one while the
// stack is below capacity, otherwise waits. It returns a *PoolShuttingDownError
// if the stack is shutting down and a *TimeoutError once the deadline passes.
// Mirrors TimedStack#pop.
func (s *TimedStack) Pop(timeout time.Duration) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	deadline := s.now().Add(timeout)
	for {
		if s.isDown {
			return nil, &PoolShuttingDownError{}
		}
		if n := len(s.que); n > 0 {
			obj := s.que[n-1]
			s.que = s.que[:n-1]
			return obj, nil
		}
		if s.created < s.max {
			obj := s.factory()
			s.created++
			return obj, nil
		}
		remaining := deadline.Sub(s.now())
		if remaining <= 0 {
			return nil, &TimeoutError{Msg: fmt.Sprintf(
				"Waited %.1f sec, %d/%d available",
				timeout.Seconds(), s.lengthLocked(), s.max)}
		}
		s.waitTimeout(remaining)
	}
}

// waitTimeout waits on the condition variable, but no longer than d. The mutex is
// held on entry and re-held on return, as with sync.Cond.Wait. A helper goroutine
// broadcasts at the deadline so the wait cannot outlive d; it is always released
// (either it fires the timer, or the done channel unblocks it), so no goroutine
// leaks. A spurious wake merely re-runs the Pop loop, which re-checks the state.
func (s *TimedStack) waitTimeout(d time.Duration) {
	timer := time.NewTimer(d)
	done := make(chan struct{})
	go func() {
		select {
		case <-timer.C:
			s.mu.Lock()
			s.cond.Broadcast()
			s.mu.Unlock()
		case <-done:
		}
	}()
	s.waiters++
	s.cond.Wait()
	s.waiters--
	close(done)
	timer.Stop()
}

// waiting reports how many callers are currently blocked in Pop. Tests poll it to
// synchronise deterministically on a waiter having parked; it is not part of the
// gem's API.
func (s *TimedStack) waiting() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.waiters
}

// Shutdown disposes every parked connection through block and marks the stack as
// shutting down so waiters wake and future Pops fail. When reload is true it
// instead resets the stack for reuse (created count cleared, not shutting down),
// mirroring TimedStack#shutdown(reload:). block must not be nil.
func (s *TimedStack) Shutdown(block func(conn any), reload bool) {
	if block == nil {
		panic("connectionpool: shutdown must receive a block")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.shutdown = block
	s.isDown = true
	s.cond.Broadcast()
	for len(s.que) > 0 {
		n := len(s.que)
		conn := s.que[n-1]
		s.que = s.que[:n-1]
		block(conn)
	}
	if reload {
		s.isDown = false
		s.created = 0
	}
}

// Length reports how many connections could still be handed out without waiting:
// the parked ones plus those not yet created. Mirrors TimedStack#length, which
// backs ConnectionPool#available.
func (s *TimedStack) Length() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lengthLocked()
}

func (s *TimedStack) lengthLocked() int {
	return s.max - s.created + len(s.que)
}

// Max reports the maximum number of connections the stack will create. Mirrors
// ConnectionPool#size.
func (s *TimedStack) Max() int { return s.max }
