//go:build task_template

package main

import "os"

type Logger interface {
	Log(message string) error
	Close() error
}

type FileLogger struct{ file *os.File }

func NewFileLogger(fileName string) (*FileLogger, error) {
	f, err := os.Create(fileName)
	if err != nil {
		return nil, err
	}
	return &FileLogger{f}, nil
}

func (f FileLogger) Log(message string) error {
	_, err := f.file.WriteString(message + "\n")
	return err
}

func (f FileLogger) Close() error {
	return f.file.Close()
}

// ---

type SequentialLogger struct {
	wrappedLogger Logger
}

func NewSequentialLogger(wrappedLogger Logger) SequentialLogger {
	// TODO
}

func (sl SequentialLogger) Log(message string) error {
	// TODO
}

func (sl SequentialLogger) Close() error {
	// TODO
}
