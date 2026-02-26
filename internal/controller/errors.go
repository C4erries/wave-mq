package controller

import "errors"

var (
	// ErrNotLeader indicates that a command was sent to a follower.
	ErrNotLeader = errors.New("not leader")
	// ErrLeaderNotElected indicates that the cluster has no known leader yet.
	ErrLeaderNotElected = errors.New("leader not elected")
)

// NotLeaderError provides leader routing hint for callers.
type NotLeaderError struct {
	Leader string
}

func (e NotLeaderError) Error() string {
	if e.Leader == "" {
		return ErrNotLeader.Error()
	}

	return "not leader: leader=" + e.Leader
}

func (e NotLeaderError) Is(target error) bool {
	return target == ErrNotLeader
}
