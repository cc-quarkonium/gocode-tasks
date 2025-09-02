package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type mockRow struct {
	id uint64
}

type mockDatabase interface {
	Database

	// Вспомогательные методы для проверок в тестах
	GetDataLen() int
	GetParallel() int32
	GetLoadСallNums() []int
	GetSaveСallNums() []int
}

// mockDB имитирует базу данных (в памяти)
type mockDB struct {
	mu    *sync.Mutex
	name  string
	data  map[uint64]Row
	maxID uint64

	maxIDErr     bool
	saveRowsErr  bool
	loadСallNums []int // вызовы LoadRows() и кол-во отданных Rows
	saveСallNums []int // вызовы SaveRows() и кол-во сохраненных Rows

	concurrencyCheck chan struct{}
	current          int32
	max              int32
	once             sync.Once
}

// Глобальное хранилище "подключений"
var mockDatabases = map[string]*mockDB{}

func NewMockDatabase(dbname string, ids []uint64, raiseMaxIDErr, raiseSaveRowsErr bool) *mockDB {
	db := &mockDB{
		mu:               &sync.Mutex{},
		name:             dbname,
		data:             make(map[uint64]Row, len(ids)),
		maxID:            uint64(0),
		maxIDErr:         raiseMaxIDErr,
		saveRowsErr:      raiseSaveRowsErr,
		concurrencyCheck: make(chan struct{}),
	}

	slices.Sort(ids)

	for _, id := range ids {
		db.data[id] = []interface{}{mockRow{id: id}}
		if id > db.maxID {
			db.maxID = id
		}
	}

	mockDatabases[dbname] = db

	return db
}

type mockConnections struct {
	Prod  mockDatabase
	Stats mockDatabase
}

func getMockDatabases() (*mockConnections, error) {
	ctx := context.Background()

	prodDB, err := Connect(ctx, "PROD")
	if err != nil {
		return nil, fmt.Errorf("cant connect to mocked PROD: %w", err)
	}
	defer prodDB.Close()

	statsDB, err := Connect(ctx, "STATS")
	if err != nil {
		return nil, fmt.Errorf("cant connect to mocked STATS: %w", err)
	}
	defer statsDB.Close()

	return &mockConnections{
		Prod:  prodDB,
		Stats: statsDB,
	}, nil
}

// --- Реализация интерфейса Database ---

func (db *mockDB) Close() error {
	// Ничего не делаем
	return nil
}

func (db *mockDB) GetMaxID(ctx context.Context) (uint64, error) {
	if db.maxIDErr {
		return 0, errGetMaxID
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	return db.maxID, nil
}

func (db *mockDB) LoadRows(ctx context.Context, minID, maxID uint64) ([]Row, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	rows := []Row{}

	for id := minID; id < maxID; id++ {
		if r, ok := db.data[id]; ok {
			rows = append(rows, r)
		}
	}

	db.loadСallNums = append(db.loadСallNums, len(rows))

	// обеспечиванием последовательное возрастание ID
	sort.SliceStable(rows, func(i, j int) bool {
		r1, _ := rows[i][0].(mockRow)
		r2, _ := rows[j][0].(mockRow)
		return r1.id < r2.id
	})

	return rows, nil
}

func (db *mockDB) SaveRows(ctx context.Context, rows []Row) error {
	// подсчитываем максимум SaveRows в моменте для теста многопоточки
	// через атомики и CAS-loop, иначе если навешать на всё тело ф-ии Lock/defer Unlock,
	// то словим Lock-contention и, несмотря на распараллеливание в решении, тут в моке
	// сведется практически к поочередному выполнению
	cur := atomic.AddInt32(&db.current, 1)

	// обновляем максимум
	for {
		max := atomic.LoadInt32(&db.max)
		if cur > max && atomic.CompareAndSwapInt32(&db.max, max, cur) {
			break
		}
		if cur <= max {
			break
		}
	}

	// как только достигли 2+ одновременных вызовов - закрываем concurrencyCheck
	if cur >= 2 {
		db.once.Do(func() { close(db.concurrencyCheck) })
	}

	// ждём пока не придёт сообщение из другой горутины
	select {
	case <-db.concurrencyCheck:
		// дождались чтения из concurrencyCheck
	case <-time.After(10 * time.Millisecond):
		// одиночный вызов — не ждём вечно
	}

	db.mu.Lock()
	for _, r := range rows {
		if len(r) < 1 {
			return fmt.Errorf("invalid row: %v", r)
		}
		mockRow, ok := r[0].(mockRow)
		if !ok {
			return fmt.Errorf("first column must be uint64, got %T", r[0])
		}
		id := mockRow.id

		db.data[id] = r
		if id > db.maxID {
			db.maxID = id
		}
	}

	db.saveСallNums = append(db.saveСallNums, len(rows))
	db.mu.Unlock()

	atomic.AddInt32(&db.current, -1)

	return nil
}

// Вспомогательные методы для проверок в тестах
func (db *mockDB) GetDataLen() int {
	db.mu.Lock()
	defer db.mu.Unlock()
	return len(db.data)
}

func (db *mockDB) GetLoadСallNums() []int {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.loadСallNums
}

func (db *mockDB) GetSaveСallNums() []int {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.saveСallNums
}

func (db *mockDB) GetParallel() int32 {
	return atomic.LoadInt32(&db.max)
}

// Connect возвращает подключение к "базе"
func Connect(ctx context.Context, dbname string) (mockDatabase, error) {
	if db, ok := mockDatabases[dbname]; ok {
		return db, nil
	}

	return nil, errors.New("no database found")
}
