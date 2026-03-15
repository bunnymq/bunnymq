package management

import "errors"

var (
	ErrTopicNotFound      = errors.New("topic not found")
	ErrTopicAlreadyExists = errors.New("topic already exists")
	ErrInvalidArgument    = errors.New("invalid argument")
	ErrUnavailable        = errors.New("unavailable")
)
