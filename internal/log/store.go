package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

// encrypt >>
var (
	enc = binary.BigEndian
)

// record byte length
const (
	lenWidth = 8
)

// file wrapper
type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pos = s.size
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	// 버퍼에 임시 저장, 시스템 호출 횟수를 줄여 성능을 개선해주는 효과가 있음.
	w, err := s.buf.Write(p) // w=11, "hello world"
	if err != nil {
		return 0, 0, err
	}

	w += lenWidth
	s.size += uint64(w)
	return uint64(w), pos, nil // 19, 0, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 아직 버퍼에 있는 경우를 대비해 쓰기 버퍼의 내용을 플러시.
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	// 레코드의 바이트 크기를 알아내고 (0,0,0,0,0,0,0,11)
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	// 그만큼의 바이트를 읽어 리턴
	b := make([]byte, enc.Uint64(size)) // size, cap = 11
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return b, nil
}

func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.File.ReadAt(p, off)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return err
	}

	return s.File.Close()
}
