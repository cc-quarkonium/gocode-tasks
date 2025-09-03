package main

import (
	"context"
	"errors"
	"fmt"
	"io"
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
func CopyTable(fromName string, toName string, full bool) error {
	ctx := context.Background()

	// retry для подключения к PROD
	prodDB, err := withRetry(func() (Database, error) {
		return Connect(ctx, fromName)
	})
	if err != nil {
		return fmt.Errorf("connect to PROD: %w", err)
	}
	defer prodDB.Close()

	// retry для подключения к STATS
	statsDB, err := withRetry(func() (Database, error) {
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
		startID, err = statsDB.GetMaxID(ctx)
		if err != nil {
			return fmt.Errorf("get STATS max ID: %w", err)
		}
	}

	endID, err := prodDB.GetMaxID(ctx)
	if err != nil {
		return fmt.Errorf("get PROD max ID: %w", err)
	}

	batchSize := uint64(10_000)
	batches := splitOnBatches(startID, endID, batchSize)

	for interval := range batches {
		rows, err := withRetry(func() ([]Row, error) {
			return prodDB.LoadRows(ctx, interval[0], interval[1]+1)
		})
		if err != nil {
			return fmt.Errorf("cant get rows from db: %w", err)
		}

		_, err = withRetry(func() ([]Row, error) {
			return nil, statsDB.SaveRows(ctx, rows)
		})
		if err != nil {
			return fmt.Errorf("cant save rows to db: %w", err)
		}
	}

	return nil
}

// splitOnBatches возвращает канал батчей [start, end]
func splitOnBatches(start, end, batchSize uint64) <-chan [2]uint64 {
	ch := make(chan [2]uint64)

	go func() {
		defer close(ch)
		for i := start; i <= end; i += batchSize {
			if i+batchSize > end {
				ch <- [2]uint64{i, end}
			} else {
				ch <- [2]uint64{i, i + batchSize - 1}
			}
		}
	}()

	return ch
}

// пропишем константы тут; вслух можно сказать, что по-хорошему храним это где-нибудь в конфиге
const maxRetries = 3

// ф-я обертка для операций с ретраями при временных ошибках
func withRetry[T any](fn func() (T, error)) (T, error) {
	var result T

	// + 1 т.к. первая попытка это не повтор
	for range maxRetries + 1 {
		val, err := fn()
		if err == nil {
			return val, nil
		}
		// если ошибка не является временной, то нет смысла повторять
		if !errors.Is(err, ErrDBTemporal) {
			return result, err
		}

	}

	return result, fmt.Errorf("too many retries: %w", ErrDBTemporal)
}
