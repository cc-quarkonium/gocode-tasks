package main

import (
	"context"
	"fmt"
	"io"
	"sync"
)

type Row []interface{}

type Database interface {
	io.Closer

	// Возвращает максимальный id в таблице
	GetMaxID(ctx context.Context) (uint64, error)

	// Загружает строки из диапазона [minID, maxID)
	LoadRows(ctx context.Context, minID, maxID uint64) ([]Row, error)

	// Сохраняет строки, вызов идемпотентен
	SaveRows(ctx context.Context, rows []Row) error
}

// также внутри пакета дана функция подключения:
// func Connect(ctx context.Context, dbname string) (Database, error)

// CopyTable копирует таблицу profiles с одного сервера на другой.
// Если full=false, то переливка продолжается с места прошлой ошибки.
// Если full=true, то переливка выполняется "с нуля".
func CopyTable(fromName string, toName string, full bool) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	prodDB, err := withRetry(ctx, 3, func() (Database, error) {
		return Connect(ctx, fromName)
	})
	if err != nil {
		return fmt.Errorf("connect to PROD: %w", err)
	}
	defer prodDB.Close()

	statsDB, err := withRetry(ctx, 3, func() (Database, error) {
		return Connect(ctx, toName)
	})
	if err != nil {
		return fmt.Errorf("connect to STATS: %w", err)
	}
	defer statsDB.Close()

	var startID uint64
	if full {
		startID = 0
	} else {
		startID, err = withRetry(ctx, 3, func() (uint64, error) {
			return statsDB.GetMaxID(ctx)
		})
		if err != nil {
			return fmt.Errorf("get STATS max ID: %w", err)
		}
	}

	endID, err := withRetry(ctx, 3, func() (uint64, error) {
		return prodDB.GetMaxID(ctx)
	})
	if err != nil {
		return fmt.Errorf("get PROD max ID: %w", err)
	}

	const batchSize = 10_000
	const workers = 10

	jobs := make(chan []Row, workers)
	errCh := make(chan error, 1)

	var wg sync.WaitGroup
	wg.Add(workers)

	// запуск воркеров
	for range workers {
		go func() {
			defer wg.Done()
			for rows := range jobs {
				if e := withRetryVoid(ctx, 3, func() error {
					return statsDB.SaveRows(ctx, rows)
				}); e != nil {
					select {
					case errCh <- e:
					default:
					}
					cancel()
					return
				}
			}
		}()
	}

	// формируем батчи
	curID := startID
	for curID < endID {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return <-errCh
		default:
		}

		batchRows := make([]Row, 0, batchSize)

		for len(batchRows) < batchSize && curID < endID {
			nextID := curID + uint64(batchSize-len(batchRows))
			rows, e := withRetry(ctx, 3, func() ([]Row, error) {
				return prodDB.LoadRows(ctx, curID, nextID)
			})
			if e != nil {
				close(jobs)
				wg.Wait()
				return fmt.Errorf("load rows: %w", e)
			}

			if len(rows) == 0 {
				curID = nextID
				continue
			}
			batchRows = append(batchRows, rows...)
			curID = nextID
		}

		jobs <- batchRows
	}

	close(jobs)
	wg.Wait()

	select {
	case e := <-errCh:
		return fmt.Errorf("copy failed: %w", e)
	default:
		return nil
	}
}

// retry-хелперы
func withRetry[T any](ctx context.Context, attempts int, fn func() (T, error)) (T, error) {
	var zero T
	var err error
	for i := 0; i < attempts; i++ {
		var v T
		v, err = fn()
		if err == nil {
			return v, nil
		}
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		default:
		}
	}
	return zero, err
}

func withRetryVoid(ctx context.Context, attempts int, fn func() error) error {
	_, err := withRetry(ctx, attempts, func() (struct{}, error) {
		return struct{}{}, fn()
	})
	return err
}
