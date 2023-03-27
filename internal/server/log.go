package server

import (
	"fmt"
	"sync"
)

var ErrOffsetNotFound = fmt.Errorf("offet not found")

type Log struct {
	mu      sync.Mutex
	records []Record
}

func NewLog() *Log {
	return &Log{}
}

// input 추가할 레코드 값
// ouput 추가된 레코드의 인덱스
func (c *Log) Append(record Record) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	record.Offest = uint64(len(c.records))
	c.records = append(c.records, record)
	return record.Offest, nil
}

func (c *Log) Read(offset uint64) (Record, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if offset >= uint64(len(c.records)) {
		return Record{}, ErrOffsetNotFound
	}

	return c.records[offset], nil
}

type Record struct {
	Value  []byte `json:"value"`
	Offest uint64 `json:"offset"`
}
