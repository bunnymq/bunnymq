package data

import "errors"

var ErrOffsetNotFound = errors.New("offset not found")
var ErrOffsetOutOfRange = errors.New("offset out of range")
