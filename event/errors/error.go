package errors

import "errors"

type Error struct {
	Err           error
	msg, streamID string
	extra         any
}

func (e Error) Error() string {
	str := e.msg
	if e.Err != nil {
		str += ": " + e.Err.Error()
	}
	return str
}

func (e Error) StreamID() string {
	return e.streamID
}

func (e Error) Extra() any {
	return e.extra
}

func (e Error) Unwrap() error { return e.Err }

func (e Error) Is(err error) bool {
	if er, ok := err.(Error); ok {
		return e.msg == er.msg
	}
	return false
}

func New(msg string) Error {
	return Error{msg: msg}
}

func WithStream(e Error, stmID string) Error {
	e.streamID = stmID
	return e
}

func ErrIs(err error, errs ...error) bool {
	for _, e := range errs {
		if errors.Is(err, e) {
			return true
		}
	}
	return false
}

func ErrAs[T error](err error) (ok bool, target T) {
	ok = errors.As(err, &target)
	return
}

func Err(err Error, stmID string, extra any) Error {
	err = WithStream(err, stmID)
	if e, ok := extra.(error); ok {
		err.Err = e
	} else {
		err.extra = extra
	}
	return err
}
