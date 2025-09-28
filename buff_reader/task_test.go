package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPipe_NoData(t *testing.T) {
	conf := &Config{
		maxItems:       0,
		processWorkers: 1,
	}
	p := &mockProducer{maxItems: int(conf.maxItems), batch: 10}
	c := &mockConsumer{}

	err := Pipe(conf, p, c)
	require.NoError(t, err)
	require.Equal(t, int32(0), c.processed.Load(), "ничего не должно быть обработано")
}

func TestPipe_SingleBatch10(t *testing.T) {
	conf := &Config{
		maxItems:       10,
		processWorkers: 1,
	}
	p := &mockProducer{maxItems: int(conf.maxItems), batch: 10}
	c := &mockConsumer{}

	err := Pipe(conf, p, c)
	require.NoError(t, err)
	require.Equal(t, int32(10), c.processed.Load(), "ровно один батч на 10 элементов")
}

func TestPipe_1000Items_By10(t *testing.T) {
	conf := &Config{
		maxItems:       1000,
		processWorkers: 5,
	}
	p := &mockProducer{maxItems: int(conf.maxItems), batch: 10}
	c := &mockConsumer{}

	err := Pipe(conf, p, c)
	require.NoError(t, err)
	require.Equal(t, int32(1000), c.processed.Load(), "все 1000 элементов должны быть обработаны")
}

func TestPipe_999Items_By10(t *testing.T) {
	conf := &Config{
		maxItems:       999,
		processWorkers: 5,
	}
	p := &mockProducer{maxItems: int(conf.maxItems), batch: 10}
	c := &mockConsumer{}

	err := Pipe(conf, p, c)
	require.NoError(t, err)
	require.Equal(t, int32(999), c.processed.Load(), "ровно 999 элементов должны быть обработаны")
}

func TestPipe_1002Items_By10(t *testing.T) {
	conf := &Config{
		maxItems:       1002,
		processWorkers: 5,
	}
	p := &mockProducer{maxItems: int(conf.maxItems), batch: 10}
	c := &mockConsumer{}

	err := Pipe(conf, p, c)
	require.NoError(t, err)
	require.Equal(t, int32(1002), c.processed.Load(), "все 1002 элемента должны быть обработаны")
}

func TestPipe_101Items_ByOne(t *testing.T) {
	conf := &Config{
		maxItems:       101,
		processWorkers: 5,
	}
	p := &mockProducer{maxItems: int(conf.maxItems), batch: 1}
	c := &mockConsumer{}

	err := Pipe(conf, p, c)
	require.NoError(t, err)
	require.Equal(t, int32(101), c.processed.Load(), "все 101 элемента должны быть обработаны")
}
