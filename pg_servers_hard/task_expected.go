package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"time"

	"golang.org/x/sync/errgroup"
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

// Также внутри пакета дана функция подключения:
// func Connect(ctx context.Context, dbname string) (Database, error)

// Подразумеваем, что в результатах методов Database и Connect
// временные ошибки обернуты кастомной ошибкой ErrDBTemporal
var ErrDBTemporal = errors.New("temporary db error")

// CopyTable копирует таблицу profiles с одного сервера на другой.
// Если full=false, то переливка продолжается с места прошлой ошибки.
// Если full=true, то переливка выполняется "с нуля".
func CopyTable(fromName string, toName string, full bool) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// подключение с ретраями
	prodDB, err := withRetry(ctx, func() (Database, error) {
		return Connect(ctx, fromName)
	})
	if err != nil {
		return fmt.Errorf("connect to PROD: %w", err)
	}
	defer prodDB.Close()

	statsDB, err := withRetry(ctx, func() (Database, error) {
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
		startID, err = withRetry(ctx, func() (uint64, error) {
			return statsDB.GetMaxID(ctx)
		})
		if err != nil {
			return fmt.Errorf("get STATS max ID: %w", err)
		}
	}

	endID, err := withRetry(ctx, func() (uint64, error) {
		return prodDB.GetMaxID(ctx)
	})
	if err != nil {
		return fmt.Errorf("get PROD max ID: %w", err)
	}

	const batchSize = 10_000
	const workers = 10

	rowsCh := make(chan []Row, workers)

	g, gctx := errgroup.WithContext(ctx)

	// Горутина собирает батчи из PROD
	g.Go(func() error {
		defer close(rowsCh)

		curID := startID
		for curID < endID {
			batchRows := make([]Row, 0, batchSize)

			for len(batchRows) < batchSize && curID < endID {
				nextID := curID + uint64(batchSize-len(batchRows))

				rows, err := withRetry(gctx, func() ([]Row, error) {
					return prodDB.LoadRows(gctx, curID, nextID)
				})
				if err != nil {
					return fmt.Errorf("load rows: %w", err)
				}

				batchRows = append(batchRows, rows...)
				curID = nextID
			}

			if len(batchRows) > 0 {
				select {
				case <-gctx.Done():
					return gctx.Err()
				case rowsCh <- batchRows:
				}
			}
		}
		return nil
	})

	// Воркеры сохраняют данные
	for range workers {
		g.Go(func() error {
			for {
				select {
				case <-gctx.Done():
					return gctx.Err()
				case rows, ok := <-rowsCh:
					if !ok {
						return nil
					}
					_, err := withRetry(gctx, func() ([]Row, error) {
						return nil, statsDB.SaveRows(gctx, rows)
					})
					if err != nil {
						return fmt.Errorf("save rows: %w", err)
					}
				}
			}
		})
	}

	// ждём завершения
	if err := g.Wait(); err != nil {
		return fmt.Errorf("copy failed: %w", err)
	}
	return nil
}

// пропишем константы тут; вслух можно сказать, что по-хорошему храним это где-нибудь в конфиге
const maxRetries = 3
const backoffBaseForRetries = time.Millisecond * 100

// ф-я обертка операций с ретраями при временных ошибках
func withRetry[T any](ctx context.Context, fn func() (T, error)) (T, error) {
	var result T
	backoff := backoffBaseForRetries // на основе базовой константы заводим переменную, которая будет расти с кол-вом попыток

	// + 1 т.к. первая попытка это не повтор
	for range maxRetries + 1 {
		val, err := fn()
		if err == nil {
			return val, nil
		}
		// если ошибка не является постоянной, то есть смысл повторить
		if errors.Is(err, ErrDBTemporal) {
			// добавляем джиттер
			jitter := time.Duration(rand.Int63n(int64(backoff)))
			sleep := backoff + jitter

			// помним, что у нас есть context и если он отменем, то нет смысла в след. попытках
			select {
			case <-ctx.Done():
				return result, ctx.Err()
			case <-time.After(sleep):
			}

			backoff *= 2
			continue
		}
		return result, err
	}

	return result, fmt.Errorf("too many retries: %w", ErrDBTemporal)
}
