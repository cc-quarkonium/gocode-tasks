package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPipe_NoData(t *testing.T) {
	p := &mockProducer{maxItems: 0, batch: 10}
	c := &mockConsumer{}

	err := Pipe(p, c, 0)
	require.NoError(t, err)
	require.Equal(t, int32(0), c.processed.Load(), "ничего не должно быть обработано")
}

func TestPipe_SingleBatch10(t *testing.T) {
	p := &mockProducer{maxItems: 10, batch: 10}
	c := &mockConsumer{}

	err := Pipe(p, c, 10)
	require.NoError(t, err)
	require.Equal(t, int32(10), c.processed.Load(), "ровно один батч на 10 элементов")
}

func TestPipe_1000Items_By10(t *testing.T) {
	p := &mockProducer{maxItems: 1000, batch: 10}
	c := &mockConsumer{}

	err := Pipe(p, c, 1000)
	require.NoError(t, err)
	require.Equal(t, int32(1000), c.processed.Load(), "все 1000 элементов должны быть обработаны")
}

func TestPipe_999Items_By10(t *testing.T) {
	p := &mockProducer{maxItems: 999, batch: 10}
	c := &mockConsumer{}

	err := Pipe(p, c, 999)
	require.NoError(t, err)
	require.Equal(t, int32(999), c.processed.Load(), "ровно 999 элементов должны быть обработаны")
}

func TestPipe_1002Items_By10(t *testing.T) {
	p := &mockProducer{maxItems: 1002, batch: 10}
	c := &mockConsumer{}

	err := Pipe(p, c, 1002)
	require.NoError(t, err)
	require.Equal(t, int32(1002), c.processed.Load(), "все 1002 элемента должны быть обработаны")
}

func TestPipe_101Items_ByOne(t *testing.T) {
	p := &mockProducer{maxItems: 101, batch: 1}
	c := &mockConsumer{}

	err := Pipe(p, c, 101)
	require.NoError(t, err)
	require.Equal(t, int32(101), c.processed.Load(), "все 101 элемента должны быть обработаны")
}
