package main

import (
	"os"
	"strings"
	"time"
)

type Logger interface {
	Log(message string) error
	Close() error
}

// Заметим, что в шаблоне FileLogger содержит указатели на *os.File
// Но далее интерфейс логгера реализуется FileLogger через value-receiver,
// что копирует структуру при каждом вызове методов с одинаковой ссылкой на os.File,
// что приведет к гонке данных при конкурентных вызовах.
// Поэотму для структур, которые владеют ресурсами (файл, сокет, mutex, канал и т.п.),
// нужно всегда делать pointer receivers, чтобы избежать "копий-зомби".
type FileLogger struct{ file *os.File }

func NewFileLogger(fileName string) (*FileLogger, error) {
	f, err := os.Create(fileName)
	if err != nil {
		return nil, err
	}
	return &FileLogger{f}, nil
}

func (f *FileLogger) Log(message string) error {
	_, err := f.file.WriteString(message)
	return err
}

func (f *FileLogger) Close() error {
	return f.file.Close()
}

// Заметим, что в реализации FileLogger.Log() сообщения просто пишутся с новой строки.
// Таким образом мы можем отправлять в Log() наборы сообщений, разделив их символом новой строки,
// тем самым снизив кол-во системных вызовов. Поэтому в SequentialLogger реализуем накопление батча.
// При этом важный нюанс - т.к. main-горутина может быть каким-нибудь бесконенчным процессом,
// то готовность батча к записи на диск должна определяться не по размеру, а по времени.
// В противном случае мы рискуем не видеть часть последних актуальных логов из-за ожидания кол-ва
// сообщений для формирования полного батча для записи.
// Но также стоит ограничивать и размер батча, чтобы не переполниться по памяти.

type SequentialLogger struct {
	wrappedLogger Logger
	done          chan struct{}
	batchCh       chan string
	batchSize     int
	flushInterval time.Duration
}

const (
	bufSize       = 1
	batchSize     = 100
	flushInterval = 100 * time.Millisecond
)

func NewSequentialLogger(wrappedLogger Logger) *SequentialLogger {
	sl := &SequentialLogger{
		wrappedLogger: wrappedLogger,
		batchCh:       make(chan string, bufSize),
		done:          make(chan struct{}),
		batchSize:     batchSize,
		flushInterval: flushInterval,
	}

	go sl.worker()
	return sl
}

func (sl *SequentialLogger) worker() {
	defer close(sl.done)

	batch := make([]string, 0, sl.batchSize)
	ticker := time.NewTicker(sl.flushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		data := strings.Join(batch, "\n") + "\n"
		// запись идёт в отдельном воркере асинхронно, напрямую пробросить пользователю API логгера
		// мы не можем; обычно в таких сценариях ошибку логируют, но мы сами же тут пишем лог)) ?
		_ = sl.wrappedLogger.Log(data)
		batch = batch[:0] // сбрасываем слайс без переаллокации памяти (len=0, cap=batchSize)
	}

	for {
		select {
		case msg, ok := <-sl.batchCh:
			if !ok {
				// канал закрыт - пишем всё, что осталось
				flush()
				return
			}
			batch = append(batch, msg)
			// флашим по достижении лимита
			if len(batch) >= sl.batchSize {
				flush()
			}
		case <-ticker.C:
			// флашим по времени
			flush()
		}
	}
}

func (sl *SequentialLogger) Log(message string) error {
	sl.batchCh <- message
	return nil
}

func (sl *SequentialLogger) Close() error {
	close(sl.batchCh)
	<-sl.done
	return sl.wrappedLogger.Close()
}
