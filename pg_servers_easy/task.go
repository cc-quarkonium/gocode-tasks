//go:build task_template

package main

import (
	"context"
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
	// TODO
}
