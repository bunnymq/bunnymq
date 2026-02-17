package log

import "errors"

type Log struct {
	Offset  uint
	Version uint8
}

func Marshal() ([]byte, error) {
	return nil, errors.New("TODO: not implemented")
}
