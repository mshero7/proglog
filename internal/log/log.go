package log

import (
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/mshero7/proglog/api/v1"
)

// segment 관리, 활성 세그먼트, 세그먼트 포인터 슬라이스
type Log struct {
	mu     sync.RWMutex
	Dir    string // location
	Config Config

	activeSegment *segment
	segments      []*segment
}

func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}

	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}

	l := &Log{
		Dir:    dir,
		Config: c,
	}

	return l, l.setup()
}

// 로그 인스턴스 설정
func (l *Log) setup() error {
	files, err := ioutil.ReadDir(l.Dir)

	if err != nil {
		return err
	}

	var baseOffsets []uint64

	for _, file := range files {
		offStr := strings.TrimSuffix(
			file.Name(),
			path.Ext(file.Name()),
		)
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}

	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	for i := 0; i < len(baseOffsets); i++ {
		if err = l.newSegment(baseOffsets[i]); err != nil {
			return err
		}
		// 베이스 오프셋은 index와 store 두 파일을 중복해서 담고 있어서 중복값을 담고 있어
		// 한번 건너 뛰기 위함.
		i++
	}

	if l.segments == nil {
		if err = l.newSegment(
			l.Config.Segment.InitialOffset,
		); err != nil {
			return err
		}
	}

	return nil
}

func (l *Log) newSegment(off uint64) error {
	s, err := newSegment(l.Dir, off, l.Config)

	if err != nil {
		return err
	}

	// 기존 로그의 세그먼트 슬라이스에 추가 + 활성 세그먼트로 만듬
	l.segments = append(l.segments, s)
	l.activeSegment = s

	return nil
}

// mutex로 조율
func (l *Log) Append(record *api.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.activeSegment.IsMaxed() {
		off := l.activeSegment.nextOffset
		if err := l.newSegment(off); err != nil {
			return 0, err
		}
	}

	return l.activeSegment.Append(record)
}

func (l *Log) Read(off uint64) (*api.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var s *segment
	for _, segment := range l.segments {
		if segment.baseOffset <= off && off < segment.nextOffset {
			s = segment
			break
		}
	}
	if s == nil || s.nextOffset <= off {
		// return nil, fmt.Errorf("offset out of range: %d", off) 기존 에러
		return nil, api.ErrOffsetOutOfRange{Offset: off} // GRPC error
	}

	return s.Read(off)
}

// 로그의 모든 세그먼트를 닫는다.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}

	return nil
}

// 로그를 닫고 데이터를 무도 지운다.
func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}

	return os.RemoveAll(l.Dir)
}

// 로그를 제거하고 이를 대체할 새로운 로그를 생성한다.
func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}

	return l.setup()
}

// 오프셋들에 대한 범위
func (l *Log) LowestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.segments[0].baseOffset, nil
}

func (l *Log) HighestOffset() (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	off := l.segments[len(l.segments)-1].nextOffset
	if off == 0 {
		return 0, nil
	}

	return off - 1, nil
}

// 가장 큰 오프셋이 가장 작은 오프셋보다 작은 세그먼트를 찾아 모두 제거 = 특정 시점보다 오래된 세그먼트들을 지움
func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// 새로 교체할 semgent 배열
	var segments []*segment
	for _, s := range l.segments {
		if s.nextOffset <= lowest+1 {
			if err := s.Remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, s)
	}

	l.segments = segments

	return nil
}

// 전체 로그 읽기 위한 io.Reader
// 스냅숏, 복원 기능
func (l *Log) Reader() io.Reader {
	l.mu.Lock()
	defer l.mu.Unlock()

	readers := make([]io.Reader, len(l.segments))
	for i, segment := range l.segments {
		readers[i] = &originReader{segment.store, 0}
	}

	return io.MultiReader(readers...)
}

// io.Reader 인터페이스 구현하기 위해 세그먼트 스토어를 감싸준다.
type originReader struct {
	*store
	off int64
}

func (o *originReader) Read(p []byte) (int, error) {
	n, err := o.ReadAt(p, o.off)
	o.off += int64(n)

	return n, err
}
