//go:build task_template

package main

import "errors"

// possible errors
var (
	ErrNextFailed        = errors.New("next failed")
	ErrProcessFailed     = errors.New("process failed")
	ErrCommitFailed      = errors.New("commit failed")
	ErrCommitSeqViolated = errors.New("failed get next cookie from sequence")
)

type Producer interface {
	// Next returns:
	// - batch of items to be processed
	// - cookie to be commited when processing is done
	// - error
	Next() (items []any, cookie int, err error)
	// Commit is used to mark data batch as processed
	Commit(cookie int) error
}

type Consumer interface {
	Process(items []any) error
}

func Pipe(p Producer, c Consumer, MaxItems int) error {
	// TODO
}
