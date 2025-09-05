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

// Проанализировав требования, приходим к выводу, что нам потребуется
// определить какой-то размер батча, кол-во воркеров, а также какую-то политику повторов.
// Для чего заведём константы; вслух можно сказать, что по-хорошему храним это где-нибудь в конфиге,
// Дополнительно стоит проговорить про контроль в подборе констант так, чтобы рост
// кол-ва повторов и рост длительности backoff-пауз не приводил к неконтролируемому потреблению ресурсов.
const batchSize = 10_000
const workers = 10
const maxRetries = 3
const backoffBaseForRetries = time.Millisecond * 100

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

	// Создадим канал, в который будут передаваться батчи, собранные из рез-тов LoadRows()
	// Есть два пути - использовать буфер или нет.
	//
	// С одной стороны по условию требуется равномерная загрузка воркеров, которая кроме батчей
	// примерно равного размера может подразумевать минимизацию воркеров, которые ожидают данные
	// из канала. Добавляя буфер размером в кол-во воркеров, он довольно быстро заполнится батчами,
	// которые воркеры практически равномерно будут разбирать на обработку (тем более чтение из
	// базы PROD наверняка быстрее записи в базу STATS). Также буф. канал может сгладить
	// потенциальные поочередные подвисания между базами PROD и STATS. Например, если
	// сначала подвисала запись SaveRows() в STATS, но за счёт буфера она не сразу блокировала
	// чтение LoadRows() из PROD, а затем наоборот затормозило чтение PROD, а запись пошла быстрее,
	// то за счёт наличия буфера можно частично компенсировать такие взаимные опаздывания.
	// Но, как правило, на практике в таких ситуациях уже смотрят в сторону брокеров/очередей
	// как отдельного инфраструктурного компонента в архитектуре сервисов.
	//
	// С другой стороны, опять же обратимся к тому, что скорее всего LoadRows() из PROD будет
	// работать пошустрее чем запись в STATS. Тогда не имеет большого смысла считывать данные,
	// которые не готов принять ещё ни один из воркеров, разве что для сглаживания поочередных
	// задержек, увеличивая пропускную способность копирования. К тому же, подобные задачи,
	// как правило, фоновые, где  можно жертвовать временем в пользу большего контроля.
	// Небуф. канал как раз даёт больший контроль в том смысле, что меньше нужно заботиться
	// об утечке данных и горутин. Отчего, небуф. канал является предпочтильнее, если нет
	// основательных причин использовать буфер.
	rowsCh := make(chan []Row)

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

	// Синхронизируем завершение
	if err := g.Wait(); err != nil {
		return fmt.Errorf("copy failed: %w", err)
	}
	return nil
}

// Ф-я обертка для операций с ретраями при временных ошибках
func withRetry[T any](ctx context.Context, fn func() (T, error)) (T, error) {
	var result T

	// На основе базовой константы заводим переменную, которая будет расти с кол-вом попыток
	backoff := backoffBaseForRetries

	// + 1 т.к. первая попытка это не повтор
	for range maxRetries + 1 {
		val, err := fn()
		if err == nil {
			return val, nil
		}
		// Если ошибка не является постоянной, то есть смысл повторить
		if errors.Is(err, ErrDBTemporal) {
			// Добавляем джиттер
			jitter := time.Duration(rand.Int63n(int64(backoff)))
			sleep := backoff + jitter

			// Можно было использовать time.After, но *Timer даёт больше контроля, см. комменты ниже
			t := time.NewTimer(sleep)

			// Помним, что у нас есть context и если он отменем, то нет смысла в след. попытках
			select {
			case <-ctx.Done():
				if !t.Stop() { // Также необходимо остановить текущий таймер
					// Если таймер уже сработал, в буферизированный канал t.C положилось значение,
					// его нужно вычитать, иначе канал останется "грязным" и рантайм удержит таймер в памяти
					<-t.C
				}
				return result, ctx.Err()
			case <-t.C:
			}

			backoff *= 2
			continue
		}
		return result, err
	}

	return result, fmt.Errorf("too many retries: %w", ErrDBTemporal)
}
