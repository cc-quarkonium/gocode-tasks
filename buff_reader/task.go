//go:build task_template

package main

import "errors"

/*
Предопределенные константы вынесены в отдельную структуру конфига
type Config struct {
	maxItems       uint16
	processWorkers uint8
}
*/

var (
	ErrEofCommitCookie   = errors.New("no more data")
	ErrNextFailed        = errors.New("next failed")
	ErrProcessFailed     = errors.New("process failed")
	ErrCommitFailed      = errors.New("commit failed")
	ErrCommitSeqViolated = errors.New("failed get next cookie from sequence")
)

type Producer interface {
	// Next возвращает:
	// itmes - батч элементов, которые необходимо обратотать
	// cookie - значение, которым впоследствии необходимо подтвердить обработку вызовом Commit
	// error - ошибка, в том числе ошибку, сигнализирущая об отсутствии следующий данных
	Next() (items []any, cookie int, err error)
	// Commit - ф-я для подтверждения обработки данных потребителем
	Commit(cookie int) error
}

type Consumer interface {
	Process(items []any) error
}

func Pipe(config *Congif, producer Producer, consumer Consumer) error {
	// TODO
}
