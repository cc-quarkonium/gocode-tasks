package main

import (
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

// mockLogger - мок для тестов SequentialLogger
type mockLogger struct {
	mu       sync.Mutex
	logCalls []string
	closed   bool
	logCh    chan string
	closeCh  chan struct{}
}

func newMockLogger() *mockLogger {
	return &mockLogger{
		logCh:   make(chan string, 10),
		closeCh: make(chan struct{}),
	}
}

func (m *mockLogger) Log(msg string) error {
	m.mu.Lock()
	m.logCalls = append(m.logCalls, msg)
	m.mu.Unlock()
	m.logCh <- msg // сигнал в тест: Log вызван
	return nil
}

func (m *mockLogger) Close() error {
	m.mu.Lock()
	m.closed = true
	m.mu.Unlock()
	close(m.closeCh) // сигнал в тест: Close вызван
	return nil
}

func (m *mockLogger) LastCall() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.logCalls) == 0 {
		return ""
	}
	return m.logCalls[len(m.logCalls)-1]
}

func (m *mockLogger) Calls() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string{}, m.logCalls...)
}

func (m *mockLogger) IsClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

func TestSequentialLogger_BatchBySize(t *testing.T) {
	mock := newMockLogger()
	sl := NewSequentialLogger(mock)

	// Отправляем batchSize сообщений
	for i := 0; i < batchSize; i++ {
		assert.NoError(t, sl.Log("msg"))
	}

	// ждём первый flush (по размеру)
	got := <-mock.logCh
	assert.Contains(t, got, "msg")

	sl.Close()
	assert.True(t, mock.IsClosed())
}

func TestSequentialLogger_BatchByTime(t *testing.T) {
	mock := newMockLogger()
	sl := NewSequentialLogger(mock)

	assert.NoError(t, sl.Log("msg1"))

	// ждём flush по таймеру
	got := <-mock.logCh
	assert.Contains(t, got, "msg1")

	sl.Close()
	assert.True(t, mock.IsClosed())
}

func TestSequentialLogger_FlushOnClose(t *testing.T) {
	mock := newMockLogger()
	sl := NewSequentialLogger(mock)

	assert.NoError(t, sl.Log("final-msg"))

	// закрываем — ожидаем flush остатка
	sl.Close()

	// ждём сигнал Log при закрытии
	got := <-mock.logCh
	assert.Contains(t, got, "final-msg")
	assert.True(t, mock.IsClosed())
}

func TestSequentialLogger_LogIsAsync(t *testing.T) {
	mock := newMockLogger()
	sl := NewSequentialLogger(mock)

	// проверяем, что Log() не вызывает mock.Log синхронно
	select {
	case <-mock.logCh:
		t.Fatal("wrapped Log was called synchronously inside Log()")
	default:
		// всё ок, Log ещё не вызван
	}

	assert.NoError(t, sl.Log("async-msg"))

	// ждём сигнал от воркера
	got := <-mock.logCh
	assert.Contains(t, got, "async-msg")

	sl.Close()
}

func TestSequentialLogger_BatchCombineMessages(t *testing.T) {
	mock := newMockLogger()
	sl := NewSequentialLogger(mock)

	assert.NoError(t, sl.Log("msg1"))
	assert.NoError(t, sl.Log("msg2"))
	assert.NoError(t, sl.Log("msg3"))

	// закрываем — вызовет flush
	sl.Close()

	// получаем батч
	got := <-mock.logCh
	assert.True(t, strings.Contains(got, "msg1"))
	assert.True(t, strings.Contains(got, "msg2"))
	assert.True(t, strings.Contains(got, "msg3"))
	// сообщения должны быть в одном батче
	assert.Equal(t, 1, len(mock.Calls()))
}
