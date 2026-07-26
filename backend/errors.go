package backend

import (
	"errors"
	"fmt"
)

var (
	// ErrStaleAttempt reports that an operation named an execution that is no
	// longer the current one, because the job was reclaimed, rescued, or
	// already finalized. Match it with [errors.Is]; a backend reports which
	// jobs were affected by returning a [*StaleError].
	ErrStaleAttempt = errors.New("goque: stale attempt")
	// ErrNotSupported reports that a backend does not implement an optional
	// feature. It is reserved: no shipped code returns it yet.
	ErrNotSupported = errors.New("goque: not supported by backend")
	// ErrDuplicate reports that an insert was rejected as a duplicate of a job
	// already stored. It is reserved for deduplicated enqueues, which are not
	// implemented yet.
	ErrDuplicate = errors.New("goque: duplicate job")
	// ErrQueuePaused reports that a queue is not accepting work. It is reserved
	// for queue pausing, which is not implemented yet.
	ErrQueuePaused = errors.New("goque: queue paused")
	// ErrInvalidTx reports that a caller passed a transaction handle of a type
	// the backend does not understand. It is reserved for transactional
	// enqueues, which are not implemented yet.
	ErrInvalidTx = errors.New("goque: invalid transaction type")
	// ErrAlreadyFinalized reports an operation on a job that has already
	// reached a terminal state. It is reserved: backends report that case as a
	// stale attempt today.
	ErrAlreadyFinalized = errors.New("goque: job already finalized")
	// ErrVersionMismatch reports that a job's stored args version is not one
	// the worker accepts. It is reserved: the version stamped on a row is never
	// validated yet.
	ErrVersionMismatch = errors.New("goque: job args version mismatch")
	// ErrDuplicateKind reports that a job kind was registered twice in one
	// worker registry. Registering a worker returns an error wrapping it.
	ErrDuplicateKind = errors.New("goque: duplicate kind registration")
	// ErrConflictingOptions reports enqueue options that cannot be combined,
	// such as asking for both a delay and an absolute schedule. Enqueuing
	// returns an error wrapping it.
	ErrConflictingOptions = errors.New("goque: conflicting job options")
	// ErrInvalidMetadata reports metadata that is not a JSON object, which is
	// the only shape a job row can carry. Enqueuing returns an error wrapping
	// it.
	ErrInvalidMetadata = errors.New("goque: metadata must be a JSON object")
	// ErrTimeOutOfRange reports an instant a backend cannot store. Backends
	// order jobs by an int64 nanosecond count, so every instant they are given
	// must fall between MinInstant and MaxInstant inclusive.
	ErrTimeOutOfRange = errors.New("goque: time outside the representable range")
)

// StaleError reports the jobs in a batch that a fenced operation could not
// apply to, because each had been reclaimed, rescued, or already finalized. The
// entries it does not name were applied normally, so a caller that only wants
// to know whether anything went wrong can test the error with [errors.Is]
// against [ErrStaleAttempt], which StaleError unwraps to.
type StaleError struct {
	// IDs are the jobs whose entries were rejected as stale.
	IDs []int64
}

// Error lists the job IDs that were rejected as stale.
func (e *StaleError) Error() string {
	return fmt.Sprintf("stale attempt for job ids %v", e.IDs)
}

// Unwrap returns [ErrStaleAttempt], so [errors.Is] matches a StaleError against
// the sentinel.
func (e *StaleError) Unwrap() error {
	return ErrStaleAttempt
}
