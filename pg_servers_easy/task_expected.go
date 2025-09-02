package main

import (
	"context"
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

// также внутри пакета дана функция подключения:
// func Connect(ctx context.Context, dbname string) (Database, error)

// CopyTable копирует таблицу profiles с одного сервера на другой.
// Если full=false, то переливка продолжается с места прошлой ошибки.
// Если full=true, то переливка выполняется "с нуля".
func CopyTable(fromName string, toName string, full bool) error {
	ctx := context.Background()

	prodDB, err := Connect(ctx, fromName)
	if err != nil {
		return fmt.Errorf("connect to PROD: %w", err)
	}
	defer prodDB.Close()

	statsDB, err := Connect(ctx, toName)
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

	batches := splitOnBatches(startID, endID, uint64(10_000))

	for _, interval := range batches {
		rows, err := prodDB.LoadRows(ctx, interval[0], interval[1]+1)

		if err != nil {
			return fmt.Errorf("cant get row from db: %w", err)
		}

		err = statsDB.SaveRows(ctx, rows)
		if err != nil {
			return fmt.Errorf("cant save rows to db: %w", err)
		}
	}

	return nil
}

// [start, end)
func splitOnBatches(start, end, batchSize uint64) [][]uint64 {
	var batches [][]uint64

	for i := start; i <= end+1; i += batchSize {
		if i+batchSize >= end {
			batches = append(batches, []uint64{i, end})
			continue
		}
		batches = append(batches, []uint64{i, i + batchSize - 1})
	}

	return batches
}
