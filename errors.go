package pulse

import "errors"

var (
	ErrAgentStopped = errors.New("pulse: agent has been stopped and cannot be started again")
)
