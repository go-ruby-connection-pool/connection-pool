// Copyright (c) the go-ruby-connection-pool/connection-pool authors
//
// SPDX-License-Identifier: BSD-3-Clause

package connectionpool

import (
	"os/exec"
	"strings"
	"testing"
	"time"
)

// rubyBin locates a usable `ruby` that has the connection_pool gem, once. The
// oracle tests skip themselves when ruby or the gem is absent (the Windows lane
// and the qemu cross-arch lanes), so the deterministic, ruby-free suite alone
// drives the 100% coverage gate; the oracle is a faithfulness check that runs on
// developer machines and the ubuntu/macos lanes when the gem is installed.
func rubyBin(t *testing.T) string {
	t.Helper()
	path, err := exec.LookPath("ruby")
	if err != nil {
		t.Skip("ruby not on PATH; skipping MRI oracle")
	}
	if err := exec.Command(path, "-e", `require "connection_pool"`).Run(); err != nil {
		t.Skip("connection_pool gem not installed; skipping MRI oracle")
	}
	return path
}

// mri runs a Ruby snippet against MRI and returns its trimmed stdout.
func mri(t *testing.T, ruby, src string) string {
	t.Helper()
	out, err := exec.Command(ruby, "-e", src).CombinedOutput()
	if err != nil {
		t.Fatalf("ruby failed: %v\n%s", err, out)
	}
	return strings.TrimSpace(string(out))
}

// TestOracleReentrancy: a nested checkout on the same caller reuses the same
// connection and returns exactly one to the pool, matching MRI's per-thread
// storage. Our CallerKey stands in for Thread.current.
func TestOracleReentrancy(t *testing.T) {
	ruby := rubyBin(t)
	want := mri(t, ruby, `
require "connection_pool"
p = ConnectionPool.new(size: 1, timeout: 1) { Object.new }
same = p.with { |a| p.with { |b| a.equal?(b) } }
print "#{same} #{p.available}"`)

	p := New(1, time.Second, newSeq())
	var same bool
	_, _ = p.With(1, func(a any) (any, error) {
		_, _ = p.With(1, func(b any) (any, error) { same = (a == b); return nil, nil })
		return nil, nil
	})
	got := boolStr(same) + " " + itoa(p.Available())
	if got != want {
		t.Fatalf("reentrancy: go=%q mri=%q", got, want)
	}
}

// TestOracleAvailable: available drops by one for the duration of a checkout and
// is restored on checkin.
func TestOracleAvailable(t *testing.T) {
	ruby := rubyBin(t)
	want := mri(t, ruby, `
require "connection_pool"
p = ConnectionPool.new(size: 2, timeout: 1) { Object.new }
print "#{p.available} #{p.with { p.available }} #{p.available}"`)

	p := New(2, time.Second, newSeq())
	before := p.Available()
	inside, _ := p.With(1, func(any) (any, error) { return p.Available(), nil })
	got := itoa(before) + " " + itoa(inside.(int)) + " " + itoa(p.Available())
	if got != want {
		t.Fatalf("available: go=%q mri=%q", got, want)
	}
}

// TestOracleCheckinError: checking in with nothing checked out raises
// ConnectionPool::Error with a specific message; ours matches byte-for-byte.
func TestOracleCheckinError(t *testing.T) {
	ruby := rubyBin(t)
	want := mri(t, ruby, `
require "connection_pool"
p = ConnectionPool.new(size: 1) { Object.new }
begin; p.checkin; rescue => e; print e.message; end`)

	p := New(1, time.Second, newSeq())
	got := p.Checkin(1).Error()
	if got != want {
		t.Fatalf("checkin error message: go=%q mri=%q", got, want)
	}
}

// TestOracleShutdownOrder: shutdown disposes idle connections in last-in-first-out
// (stack) order, matching TimedStack.
func TestOracleShutdownOrder(t *testing.T) {
	ruby := rubyBin(t)
	// MRI: two threads each hold a distinct connection (same-thread checkout is
	// reentrant and would collapse to one), released in a controlled order so the
	// idle stack is deterministically [1, 2]; shutdown then pops LIFO -> "2,1".
	want := mri(t, ruby, `
require "connection_pool"
seq = 0
p = ConnectionPool.new(size: 2, timeout: 2) { seq += 1 }
c1 = Queue.new; c2 = Queue.new
t1 = Thread.new { p.checkout; c1 << 1; c2.pop; p.checkin }
c1.pop                      # conn 1 is out (seq == 1)
t2 = Thread.new { p.checkout; c1 << 1; c2.pop; p.checkin }
c1.pop                      # conn 2 is out (seq == 2)
c2 << 1; t1.join            # return conn 1 first  -> stack [1]
c2 << 1; t2.join            # return conn 2 second -> stack [1, 2]
order = []
p.shutdown { |c| order << c }
print order.join(",")`)

	seq := 0
	p := New(2, time.Second, func() any { seq++; return seq })
	_, _ = p.Checkout(1, time.Second)
	_, _ = p.Checkout(2, time.Second)
	_ = p.Checkin(1)
	_ = p.Checkin(2)
	var order []string
	p.Shutdown(func(c any) { order = append(order, itoa(c.(int))) })
	got := strings.Join(order, ",")
	if got != want {
		t.Fatalf("shutdown order: go=%q mri=%q", got, want)
	}
}

// TestOracleWrapperDelegates: a Wrapper delegates arbitrary calls to a borrowed
// connection, so operating on the wrapper mutates the pooled object, as in MRI.
func TestOracleWrapperDelegates(t *testing.T) {
	ruby := rubyBin(t)
	want := mri(t, ruby, `
require "connection_pool"
p = ConnectionPool.new(size: 1, timeout: 1) { [] }
w = ConnectionPool::Wrapper.new(pool: p)
w.push(1); w.push(2)
print "#{w.length} #{w.respond_to?(:push)} #{w.respond_to?(:with)}"`)

	// A []-backed connection with a Dispatch that models push/length.
	p := New(1, time.Second, func() any { s := []int{}; return &s })
	dispatch := func(conn any, name string, args ...any) any {
		s := conn.(*[]int)
		switch name {
		case "push":
			*s = append(*s, args[0].(int))
			return nil
		case "length":
			return len(*s)
		default:
			return nil
		}
	}
	w := NewWrapper(p, func() CallerKey { return "t" }, dispatch)
	_, _ = w.Call("push", 1)
	_, _ = w.Call("push", 2)
	length, _ := w.Call("length")
	respondPush, _ := w.RespondTo("push", func(any) bool { return true })
	respondWith, _ := w.RespondTo("with", func(any) bool { return false })
	got := itoa(length.(int)) + " " + boolStr(respondPush) + " " + boolStr(respondWith)
	if got != want {
		t.Fatalf("wrapper: go=%q mri=%q", got, want)
	}
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
