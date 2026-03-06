package group

import "errors"

// GroupCoordinator manages consumer group state: join, heartbeat, leave,
// offset commit, and offset fetch.
type GroupCoordinator struct{}

// ErrGroupNotFound is returned when the group does not exist.
var ErrGroupNotFound = errors.New("group not found")
