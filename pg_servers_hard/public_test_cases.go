package main

import (
	"context"
	"errors"
)

var errGetMaxID = errors.New("error get max ID")

type TestCase struct {
	name string
	full bool
	//prepare func(prodMaxID, statsMaxID uint64) struct{}
	prepare func() struct{}
	check   func(full bool) bool
}

var testCases = []TestCase{
	// Публичные тесткейсы
	{
		name: "Максимальные ID из двух баз совпадают при полном копировании (full=true)",
		full: true,
		prepare: func() struct{} {
			const prodRowNum = 100
			prodIds := make([]uint64, prodRowNum)
			for i := range prodRowNum {
				prodIds[i] = uint64(i + 1)
			}

			NewMockDatabase("PROD", prodIds, false, false)
			NewMockDatabase("STATS", []uint64{}, false, false)
			return struct{}{}
		},
		check: func(full bool) bool {
			CopyTable("PROD", "STATS", full)

			dbs, err := getMockDatabases()
			if err != nil {
				return false
			}

			ctx := context.Background()
			prodMaxID, err := dbs.Prod.GetMaxID(ctx)
			if err != nil {
				return false
			}

			statsMaxID, err := dbs.Stats.GetMaxID(ctx)
			if err != nil {
				return false
			}

			return prodMaxID == statsMaxID
		},
	},
	{
		name: "Максимальные ID из двух баз совпадают при возобновлении (full=false)",
		full: false,
		prepare: func() struct{} {
			const prodRowNum = 100
			prodIds := make([]uint64, prodRowNum)
			for i := range prodRowNum {
				prodIds[i] = uint64(i + 1)
			}

			NewMockDatabase("PROD", prodIds, false, false)
			NewMockDatabase("STATS", []uint64{1, 2}, false, false)
			return struct{}{}
		},
		check: func(full bool) bool {
			CopyTable("PROD", "STATS", full)
			dbs, err := getMockDatabases()
			if err != nil {
				return false
			}

			ctx := context.Background()
			prodMaxID, err := dbs.Prod.GetMaxID(ctx)
			if err != nil {
				return false
			}

			statsMaxID, err := dbs.Stats.GetMaxID(ctx)
			if err != nil {
				return false
			}

			return prodMaxID == statsMaxID
		},
	},
	{
		name: "Не переносим данные, если база PROD пустая",
		full: true,
		prepare: func() struct{} {
			NewMockDatabase("PROD", []uint64{}, false, false)
			NewMockDatabase("STATS", []uint64{}, false, false)
			return struct{}{}
		},
		check: func(full bool) bool {
			CopyTable("PROD", "STATS", full)
			dbs, err := getMockDatabases()
			if err != nil {
				return false
			}

			return len(dbs.Stats.GetSaveСallNums()) <= 1 && dbs.Stats.GetDataLen() == 0
		},
	},
	{
		name: "Данные корректно переливаются при наличии дырок в значениях ID",
		full: true,
		prepare: func() struct{} {
			const prodRowNum = 100
			prodIds := make([]uint64, prodRowNum)
			for i := range prodRowNum {
				prodIds[i] = uint64(i + 1)
			}

			// создадим "дырку" на 5-ом ID
			for j := 5; j < prodRowNum; j++ {
				prodIds[j] = prodIds[j] + 1
			}

			// создадим "дырку" на последнем ID
			prodIds[prodRowNum-1] = prodIds[prodRowNum-1] + 1

			NewMockDatabase("PROD", prodIds, false, false)
			NewMockDatabase("STATS", []uint64{1, 2}, false, false)
			return struct{}{}
		},
		check: func(full bool) bool {
			CopyTable("PROD", "STATS", full)
			dbs, err := getMockDatabases()
			if err != nil {
				return false
			}

			ctx := context.Background()
			prodMaxID, err := dbs.Prod.GetMaxID(ctx)
			if err != nil {
				return false
			}

			statsMaxID, err := dbs.Stats.GetMaxID(ctx)
			if err != nil {
				return false
			}

			return prodMaxID == statsMaxID && dbs.Prod.GetDataLen() == dbs.Stats.GetDataLen()
		},
	},
	{
		name: "Данные корректно переливаются при наличии больших разниц в значениях ID",
		full: true,
		prepare: func() struct{} {
			NewMockDatabase("PROD", []uint64{1, 2, 4, 1_998_193, 102_123_453}, false, false)
			NewMockDatabase("STATS", []uint64{}, false, false)
			return struct{}{}
		},
		check: func(full bool) bool {
			CopyTable("PROD", "STATS", full)
			dbs, err := getMockDatabases()
			if err != nil {
				return false
			}

			ctx := context.Background()
			prodMaxID, err := dbs.Prod.GetMaxID(ctx)
			if err != nil {
				return false
			}

			statsMaxID, err := dbs.Stats.GetMaxID(ctx)
			if err != nil {
				return false
			}

			return prodMaxID == statsMaxID && dbs.Prod.GetDataLen() == dbs.Stats.GetDataLen()
		},
	},
	{
		name: "Ожидается корректная обертка ошибок",
		full: false,
		prepare: func() struct{} {
			NewMockDatabase("PROD", []uint64{1}, true, false)
			NewMockDatabase("STATS", []uint64{}, false, false)

			return struct{}{}
		},
		check: func(full bool) bool {
			err := CopyTable("PROD", "STATS", full)
			return errors.Is(err, errGetMaxID)
		},
	},
	{
		name: "Ожидается перелив данных небольшими частями",
		full: true,
		prepare: func() struct{} {
			const prodRowNum = 1_000_100 // соточка сверху, если кандидат решил что и мильон это ок для размера батча
			prodIds := make([]uint64, prodRowNum)
			for i := range prodRowNum {
				prodIds[i] = uint64(i + 1)
			}

			NewMockDatabase("PROD", prodIds, false, false)
			NewMockDatabase("STATS", []uint64{}, false, false)
			return struct{}{}
		},
		check: func(full bool) bool {
			CopyTable("PROD", "STATS", full)
			dbs, err := getMockDatabases()
			if err != nil {
				return false
			}

			return len(dbs.Prod.GetLoadСallNums()) > 1 && len(dbs.Stats.GetSaveСallNums()) > 1
		},
	},
	// тесты hard части
	{
		name: "Ожидаются батчи примерно одинакового размера (для равномерной загрузки воркеров)",
		full: true,
		prepare: func() struct{} {
			const prodRowNum = 1_000_100
			prodIds := make([]uint64, prodRowNum)
			// первые 100 последовательных id
			for i := range prodRowNum - 1_000_000 {
				prodIds[i] = uint64(i + 1)
			}

			// создадим "дырку" после 100 на 100_000
			for j := prodRowNum - 1_000_000; j < prodRowNum; j++ {
				prodIds[j] = uint64(j + 100_000 + 1)
			}

			NewMockDatabase("PROD", prodIds, false, false)
			NewMockDatabase("STATS", []uint64{}, false, false)
			return struct{}{}
		},
		check: func(full bool) bool {
			CopyTable("PROD", "STATS", full)
			dbs, err := getMockDatabases()
			if err != nil {
				return false
			}

			nums := dbs.Stats.GetSaveСallNums()
			if len(nums) < 2 {
				return false // нет батчей
			}

			// смотрим разницу в кол-ве данных между вызовами SaveRows()
			// за исключением одного из батчей, по сути последнего,
			// но из-за конкурентного выполнения в nums он может попасть не в конец,
			// поэтому флаг once
			once := false
			for i := 0; i+1 < len(nums); i++ {
				// при миллионе записей (prodRowNum) допускаем разброс не более тысячи
				if nums[i]-nums[i+1] > 1000 {
					if once {
						return false
					}
					once = true
				}
			}

			return true
		},
	},
	{
		name: "Ожидается параллельная/конкурентная работа воркеров",
		full: true,
		prepare: func() struct{} {
			const prodRowNum = 1_000_100
			prodIds := make([]uint64, prodRowNum)
			for i := range prodRowNum {
				prodIds[i] = uint64(i + 1)
			}

			NewMockDatabase("PROD", prodIds, false, false)
			NewMockDatabase("STATS", []uint64{}, false, false)
			return struct{}{}
		},
		check: func(full bool) bool {
			CopyTable("PROD", "STATS", full)
			dbs, err := getMockDatabases()
			if err != nil {
				return false
			}

			return dbs.Stats.GetParallel() > 1
		},
	},
}
