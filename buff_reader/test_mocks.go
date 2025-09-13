package main

import (
	"sync/atomic"
)

type mockProducer struct {
	maxItems int   // сколько всего элементов в источнике
	batch    int   // желаемый размер выдаваемой пачки
	counter  int32 // сколько уже выдано (атомик)
}

func (m *mockProducer) Next() ([]any, int, error) {
	for {
		cur := int(atomic.LoadInt32(&m.counter))
		if cur >= m.maxItems {
			// конец данных
			return nil, eofCommitCookie, nil
		}

		remaining := m.maxItems - cur
		n := min(m.batch, remaining)
		new := cur + n

		// Попытка атомарно зарезервировать [cur, new)
		if atomic.CompareAndSwapInt32(&m.counter, int32(cur), int32(new)) {
			// Успешно зарезервировали n элементов
			items := make([]any, n) // значений нам не важно
			// cookie = индекс последнего элемента в этой пачке
			cookie := new - 1
			return items, cookie, nil
		}
		// иначе — кто-то другой ушёл вперед, повторяем попытку
	}
}

func (m *mockProducer) Commit(cookie int) error {
	// В тесте просто принимаем вызов
	return nil
}

type mockConsumer struct {
	processed atomic.Int32
}

func (m *mockConsumer) Process(items []any) error {
	m.processed.Add(int32(len(items)))
	return nil
}
