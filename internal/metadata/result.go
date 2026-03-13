package metadata

import sm "github.com/lni/dragonboat/v4/statemachine"

const (
	ResultOK              uint64 = 0
	ResultErrAlreadyExists uint64 = 1
	ResultErrNotFound      uint64 = 2
	ResultErrInvalidArg    uint64 = 3
)

func OKResult() sm.Result {
	return sm.Result{Value: ResultOK}
}

func ErrorResult(code uint64, msg string) sm.Result {
	return sm.Result{Value: code, Data: []byte(msg)}
}
