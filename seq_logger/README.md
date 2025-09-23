# Логгер

Дан интерфейс `Logger` и его реализация `FileLogger`. В этом коде есть какие-то проблемы. Нужно найти эти проблемы. \
**Одна** из такой проблемы - у нас запись в файл очень медленная, которая блокирует main-горутину. Ничего с `FileLogger` мы сделать не можем, это готовая реализация. Требуется реализовать декоратор `SequentialLogger`, который решит проблемы с медленной записью.
package main

```go
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
```

Требования, ограничения и допущения:
- Мы не можем менять внутреннюю реализацию `FileLogger`.
- Вызов записи в лог не должен блокировать выполнение другой любой логики программы.
- Сообщения в файл должны быть записаны последовательно в порядке вызова `Log()`

Архитектурные вопросы:
- Если пожертвовать последовательной записью в файл, то можно ли ещё как-то оптимизировать код?
- Сейчас логгер пишет напрямую на диск, а как можно изменить архитектуру, чтобы не писать напрямую на диск и сделать решение более масштабируемым?
