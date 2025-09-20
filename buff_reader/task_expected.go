// проверка работоспособности: $ go test -race -v

package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"slices"

	"golang.org/x/sync/errgroup"
)

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
	Next() (items []any, cookie int, err error)
	Commit(cookie int) error
}

type Consumer interface {
	Process(items []any) error
}

// batch - единица передачи в пайплайне (последовательно обрабатываемая пачка)
type batch struct {
	seq     int   // порядковый номер batch, нужен для Commit в правильном порядке
	items   []any // элементы данных
	cookies []int // список cookie, которые нужно подтвердить после Commit
}

// основная идея - распознать, что задача решается Pipeline-паттерном
// но есть два усложнения:
// для Process нужно накапливать batch
// Commit() нужно вызывать в последовательности получения Cookie из Next()
func Pipe(config *Config, producer Producer, consumer Consumer) error {
	gr, ctx := errgroup.WithContext(context.Background())

	// для тестовых запусков хорошо бы проверить как себя ведет программа с небуф. каналами
	// с небуф. каналами в случае потенциальных ошибок runtime go может сразу обнаружить некоторые из них (e.g., deadlock)
	// для оптимизации пропускной способности затем можно добавить буферы с размером 2*кол-во воркеров,
	// чтобы у буфера было дополнительно окно размером в кол-во воркеров, чтобы воркеры могли продолжать брать новые задачи,
	// пока Producer подготавливает следующие
	batchCh := make(chan batch)
	procCh := make(chan batch)

	gr.Go(func() error {
		return runNext(ctx, producer, int(config.maxItems), batchCh)
	})

	gr.Go(func() error {
		return runProcess(ctx, config, consumer, batchCh, procCh)
	})

	gr.Go(func() error {
		return runCommit(ctx, producer, procCh)
	})

	return gr.Wait()
}

func runNext(ctx context.Context, p Producer, maxItems int, batchCh chan<- batch) error {
	defer close(batchCh)
	// seqCounter - атомарный счётчик порядковых номеров вызовов Next
	var seqCounter int64
	// локальный буфер для накопления элементов в batch
	buf := make([]any, 0, maxItems)
	// список cookie, соответствующих элементам в buf
	var cookies []int

	for {
		items, cookie, err := p.Next()
		if errors.Is(err, ErrEofCommitCookie) {
			if len(buf) > 0 {
				// копируем buf и cookies, чтобы избежать гонок
				err := writeChanWithContext(ctx,
					batchCh,
					batch{
						seq:     int(atomic.AddInt64(&seqCounter, 1) - 1),
						items:   slices.Clone(buf),
						cookies: slices.Clone(cookies),
					},
				)
				if err != nil {
					return fmt.Errorf("write to batch channel: %w", err)
				}
			}
			break
		}
		if err != nil {
			return fmt.Errorf("%w: %v", ErrNextFailed, err)
		}

		// если items не помещаются в buf -> сбрасываем его как batch
		if len(buf)+len(items) > maxItems {
			err := writeChanWithContext(ctx,
				batchCh,
				batch{
					seq:     int(atomic.AddInt64(&seqCounter, 1) - 1),
					items:   slices.Clone(buf),
					cookies: slices.Clone(cookies),
				},
			)
			if err != nil {
				return fmt.Errorf("write to batch channel: %w", err)
			}
			// обнуляем буфер и список cookies
			buf = make([]any, 0, maxItems)
			cookies = []int{}
		}
		// добавляем новые элементы и cookie в текущий batch
		buf = append(buf, items...)
		cookies = append(cookies, cookie)
	}

	return nil
}

func runProcess(ctx context.Context, config *Config, c Consumer, batchCh <-chan batch, procCh chan<- batch) error {
	defer close(procCh)

	gr, ctx := errgroup.WithContext(ctx)

	for range config.processWorkers {
		gr.Go(func() error {
			for {
				b, ok, err := readChanWithContext(ctx, batchCh)
				if err != nil {
					return fmt.Errorf("read from batch channel: %w", err)
				}
				if !ok {
					return nil
				}
				if err := c.Process(b.items); err != nil {
					return fmt.Errorf("%w: %v", ErrProcessFailed, err)
				}
				if err := writeChanWithContext(ctx, procCh, b); err != nil {
					return fmt.Errorf("write to commit channel: %w", err)
				}
			}
		})
	}

	return gr.Wait()
}

func runCommit(ctx context.Context, p Producer, procCh <-chan batch) error {
	// nextSeq - ожидаемый номер следующего батча для коммита
	nextSeq := 0
	// buffer - хранит батчи, которые пришли раньше времени
	buffer := make(map[int]batch)

	for {
		batch, ok, err := readChanWithContext(ctx, procCh)
		if err != nil {
			return fmt.Errorf("read from commit channel: %w", err)
		}

		if !ok { // канал закрыт
			// если канал закрыт, а в буфере остались данные, то значит не смогли восстановить порядок коммитов
			if len(buffer) > 0 {
				return fmt.Errorf("missing sequence %d, %w", nextSeq, ErrCommitSeqViolated)
			}
			return nil
		}

		// кладём batch в буфер
		buffer[batch.seq] = batch

		// коммитим в нужном порядке (как данные отдавались Next())
		for {
			b, exists := buffer[nextSeq]
			if !exists { // ожидаем пока не поступит батч с нужным порядком коммита
				break
			}
			for _, cookie := range b.cookies {
				if err := p.Commit(cookie); err != nil {
					return fmt.Errorf("%w: %v", ErrCommitFailed, err)
				}
			}
			delete(buffer, nextSeq)
			nextSeq++
		}
	}
}

// вспомогательные переиспользуемые функции
func readChanWithContext[T any](ctx context.Context, ch <-chan T) (T, bool, error) {
	var res T
	select {
	case <-ctx.Done():
		return res, false, ctx.Err()
	case v, ok := <-ch:
		if !ok {
			return res, false, nil
		}
		return v, true, nil
	}
}

func writeChanWithContext[T any](ctx context.Context, ch chan<- T, v T) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case ch <- v:
		return nil
	}
}
