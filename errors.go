package pulse

import "errors"

var (
	ErrAgentStopped = errors.New("pulse: agent has been stopped and cannot be started again")
)

// nonRetryableError marks an error as not retryable by export loops.
type nonRetryableError struct {
	err error
}

func (e nonRetryableError) Error() string {
	if e.err == nil {
		return "pulse: non-retryable export error"
	}
	return e.err.Error()
}

func (e nonRetryableError) Unwrap() error {
	return e.err
}

// NonRetryable wraps an error as non-retryable.
func NonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return nonRetryableError{err: err}
}

// IsNonRetryable reports whether err is marked as non-retryable.
func IsNonRetryable(err error) bool {
	var nre nonRetryableError
	return errors.As(err, &nre)
}
