package alarm

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

// Dispatch status values. dispatching, retry, and delivered are stored in
// alarm_state.dispatch_status and todo_alarm_state.dispatch_status.
// unclaimed describes a trigger with no state row. It is a report value,
// not a stored status.
//
//	dispatching - a checker owns the claim and the notification is in flight
//	retry       - every channel failed; the row waits for retry_at
//	delivered   - a notification backend (or its fallback) completed
//	unclaimed   - no state row exists for the trigger
const (
	DispatchUnclaimed   = "unclaimed"
	DispatchDispatching = "dispatching"
	DispatchRetry       = "retry"
	DispatchDelivered   = "delivered"
)

// DispatchLease bounds one dispatch attempt. A "dispatching" row whose
// claim is older than the lease is treated as orphaned. The owner process
// exited or hung, so another checker may take the row over. The lease is
// longer than every backend run time. Desktop and audio backends return
// within their short command timeouts. An SMTP dial to a black-holed host
// fails inside the operating-system connect timeout. A lease shorter than
// a still-running backend could let two checkers deliver the same alarm.
const DispatchLease = 5 * time.Minute

// maxDispatchErrorLen bounds the failure message stored in last_error.
const maxDispatchErrorLen = 500

// ClaimKind selects the atomic statement a checker uses to own a trigger.
type ClaimKind int

const (
	// ClaimFresh inserts the state row. The (alarm_id, trigger_at) unique
	// index makes the insert the claim.
	ClaimFresh ClaimKind = iota
	// ClaimRefire clears an expired snooze. A gated UPDATE is the claim.
	ClaimRefire
	// ClaimRecover takes over a retry-due row or an orphaned dispatch.
	ClaimRecover
)

// retrySchedule is the deterministic backoff after a failed dispatch
// attempt. The wait for attempt N is retrySchedule[N-1], capped at
// maxRetryDelay. A fixed schedule keeps the takeover boundary predictable
// and the tests exact.
var retrySchedule = []time.Duration{
	30 * time.Second,
	1 * time.Minute,
	2 * time.Minute,
	5 * time.Minute,
}

// maxRetryDelay caps the backoff for attempts past retrySchedule.
const maxRetryDelay = 15 * time.Minute

// RetryDelay returns the wait after attempts failed dispatches. Attempts
// starts at 1 for the first failure.
func RetryDelay(attempts int) time.Duration {
	if attempts <= 0 {
		attempts = 1
	}
	if attempts <= len(retrySchedule) {
		return retrySchedule[attempts-1]
	}
	return maxRetryDelay
}

// newClaimToken returns a random, opaque owner identity for one service
// instance. Two overlapping checkers (daemon, alarm check, snooze refire)
// get different tokens, so the token-gated UPDATEs keep one owner per
// trigger.
func newClaimToken() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand fails only when the system has no entropy source.
		// Falling back to a time-based value would collide too easily.
		panic("alarm: cannot read random bytes for dispatch claim token: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}

// dispatchErrorText renders a backend error for last_error. It keeps the
// stored value short, because a long SMTP handshake error must not grow the
// state table without a bound.
func dispatchErrorText(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	if len(msg) > maxDispatchErrorLen {
		msg = msg[:maxDispatchErrorLen]
	}
	return msg
}

// rfc3339Ptr formats t as a nullable UTC RFC 3339 string.
func rfc3339Ptr(t time.Time) *string {
	s := t.UTC().Format(time.RFC3339)
	return &s
}

// recoverableDispatch reports whether a state row may be dispatched again
// by the current checker. Delivered, acknowledged, and snoozed rows never
// qualify. A retry row qualifies at or after retry_at. A dispatching row
// qualifies only when its lease expired. The arguments are the nullable
// state columns, read at check time.
func recoverableDispatch(status string, acked, snoozed bool, claimedAt, retryAt *string, nowStr, leaseBoundaryStr string) bool {
	if acked || snoozed {
		return false
	}
	switch status {
	case DispatchRetry:
		return retryAt != nil && *retryAt <= nowStr
	case DispatchDispatching:
		// A NULL claimed_at is a defect. Recover it at once instead of
		// leaving the reminder stuck.
		return claimedAt == nil || *claimedAt <= leaseBoundaryStr
	default:
		return false
	}
}
