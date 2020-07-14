package parquet

import (
	"io"

	kpq "github.com/kostya-sh/parquet-go/parquet"
)

type columnReader struct {
	file     *kpq.File
	col      kpq.Column
	rowGroup int

	chunk   *kpq.ColumnChunkReader
	values  []interface{}
	dLevels []uint16
	rLevels []uint16

	n, i, vi int // chunk stats
}

func newColumnReader(file *kpq.File, col kpq.Column, batchSize int) *columnReader {
	return &columnReader{
		file:    file,
		col:     col,
		values:  make([]interface{}, batchSize),
		dLevels: make([]uint16, batchSize),
		rLevels: make([]uint16, batchSize),
	}
}

func (c *columnReader) Name() string {
	return c.col.String()
}

func (c *columnReader) Next() (interface{}, error) {
	if err := c.ensureChunk(); err != nil {
		return nil, err
	}

	if err := c.ensureValues(); err == kpq.EndOfChunk {
		c.chunk = nil
		return c.Next()
	} else if err != nil {
		return nil, err
	}

	dLevel := c.dLevels[c.i]
	c.i++

	if notNull := dLevel == c.col.MaxD(); notNull {
		val := c.values[c.vi]
		c.vi++
		return val, nil
	}

	return nil, nil
}

func (c *columnReader) ensureChunk() error {
	if c.chunk != nil {
		return nil
	}
	if c.rowGroup >= len(c.file.MetaData.RowGroups) {
		return io.EOF
	}

	rd, err := c.file.NewReader(c.col, c.rowGroup)
	if err != nil {
		return err
	}
	c.chunk = rd
	c.rowGroup++
	return nil
}

func (c *columnReader) ensureValues() error {
	if c.n != 0 && c.i < c.n {
		return nil
	}

	n, err := c.chunk.Read(c.values, c.dLevels, c.rLevels)
	if err != nil {
		return err
	}

	c.n, c.i, c.vi = n, 0, 0
	return nil
}
