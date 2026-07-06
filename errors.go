// Copyright (c) the go-ruby-connection-pool/connection-pool authors
//
// SPDX-License-Identifier: BSD-3-Clause

package connectionpool

// Error is the base error type for the pool, mirroring ConnectionPool::Error
// (a RuntimeError subclass in the gem). Checkin on a caller that holds no
// connection raises it.
type Error struct{ Msg string }

func (e *Error) Error() string { return e.Msg }

// PoolShuttingDownError mirrors ConnectionPool::PoolShuttingDownError. It is
// returned by Checkout when the pool has been shut down (a ConnectionPool::Error
// subclass in the gem).
type PoolShuttingDownError struct{}

func (e *PoolShuttingDownError) Error() string { return "connection pool shutting down" }

// TimeoutError mirrors ConnectionPool::TimeoutError (a Timeout::Error subclass in
// the gem). It is returned by Checkout when no connection becomes available
// within the timeout.
type TimeoutError struct{ Msg string }

func (e *TimeoutError) Error() string { return e.Msg }
