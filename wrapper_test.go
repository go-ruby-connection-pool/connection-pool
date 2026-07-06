// Copyright (c) the go-ruby-connection-pool/connection-pool authors
//
// SPDX-License-Identifier: BSD-3-Clause

package connectionpool

import (
	"errors"
	"testing"
	"time"
)

// dispatchCounter is the Dispatch seam a host would supply: it routes a method
// name to a behaviour on a *counter connection.
func dispatchCounter(conn any, name string, args ...any) any {
	c := conn.(*counter)
	switch name {
	case "inc":
		return c.inc()
	case "value":
		return c.n
	default:
		return nil
	}
}

func constKey() CallerKey { return "caller" }

func TestWrapperDelegatesCall(t *testing.T) {
	w := NewWrapper(New(1, time.Second, newSeq()), constKey, dispatchCounter)

	res, err := w.Call("inc")
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if res.(int) != 101 { // seq starts connections at 100, inc -> 101
		t.Fatalf("Call(inc) = %v, want 101", res)
	}
	// Second call borrows the (only) connection again and observes its state.
	res, _ = w.Call("inc")
	if res.(int) != 102 {
		t.Fatalf("second Call(inc) = %v, want 102", res)
	}
	if w.PoolAvailable() != 1 {
		t.Fatalf("PoolAvailable = %d, want 1 (connection returned)", w.PoolAvailable())
	}
	if w.PoolSize() != 1 {
		t.Fatalf("PoolSize = %d, want 1", w.PoolSize())
	}
	if w.WrappedPool().Size() != 1 {
		t.Fatal("WrappedPool did not expose the underlying pool")
	}
}

func TestWrapperWith(t *testing.T) {
	w := NewWrapper(New(1, time.Second, newSeq()), constKey, dispatchCounter)
	res, err := w.With(func(conn any) (any, error) {
		return conn.(*counter).inc(), nil
	})
	if err != nil || res.(int) != 101 {
		t.Fatalf("Wrapper.With = (%v, %v)", res, err)
	}
}

func TestWrapperRespondTo(t *testing.T) {
	w := NewWrapper(New(1, time.Second, newSeq()), constKey, dispatchCounter)

	// Own method: answered without touching the pool.
	ok, err := w.RespondTo("with", func(any) bool {
		t.Fatal("respond probe should not run for a Wrapper method")
		return false
	})
	if err != nil || !ok {
		t.Fatalf("RespondTo(with) = (%v, %v), want (true, nil)", ok, err)
	}

	// Delegated: probes a borrowed connection.
	ok, err = w.RespondTo("inc", func(conn any) bool {
		_, is := conn.(*counter)
		return is
	})
	if err != nil || !ok {
		t.Fatalf("RespondTo(inc) = (%v, %v), want (true, nil)", ok, err)
	}
}

func TestWrapperPoolShutdown(t *testing.T) {
	w := NewWrapper(New(1, time.Second, newSeq()), constKey, dispatchCounter)
	if _, err := w.Call("inc"); err != nil { // create + return one connection
		t.Fatalf("prime: %v", err)
	}
	var disposed int
	w.PoolShutdown(func(any) { disposed++ })
	if disposed != 1 {
		t.Fatalf("PoolShutdown disposed %d, want 1", disposed)
	}
}

func TestWrapperCallErrorAfterShutdown(t *testing.T) {
	w := NewWrapper(New(1, 0, newSeq()), constKey, dispatchCounter)
	w.PoolShutdown(func(any) {})

	if _, err := w.Call("inc"); !isShuttingDown(err) {
		t.Fatalf("Call after shutdown err = %v, want *PoolShuttingDownError", err)
	}
	if _, err := w.RespondTo("inc", func(any) bool { return true }); !isShuttingDown(err) {
		t.Fatalf("RespondTo after shutdown err = %v, want *PoolShuttingDownError", err)
	}
}

func TestWrapConstructor(t *testing.T) {
	w := Wrap(2, time.Second, newSeq(), constKey, dispatchCounter)
	if w.PoolSize() != 2 {
		t.Fatalf("Wrap PoolSize = %d, want 2", w.PoolSize())
	}
	if _, err := w.Call("inc"); err != nil {
		t.Fatalf("Wrap Call: %v", err)
	}
}

func isShuttingDown(err error) bool {
	var d *PoolShuttingDownError
	return errors.As(err, &d)
}
