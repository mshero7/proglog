package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

// 인덱스 항목 내의 바이트 수를 정의.
// 레코드 오프셋과 스토어 파일에서의 위치를 나타냄.
var (
	offWidth uint64 = 4 // '레코드의 오프셋' 오프셋은 uin32 라 4바이트
	posWidth uint64 = 8 // '스토어 파일에서의 위치' uint64 라 8바이트
	endWidth        = offWidth + posWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64 // 인덱스를 어디에 쓸지 나타냄
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}

	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	// 인덱스 파일의 데이터양을 추적하기 위한 파일의 현재 크기 저장.
	idx.size = uint64(fi.Size())
	if err = os.Truncate(
		f.Name(), int64(c.Segment.MaxIndexBytes),
	); err != nil {
		return nil, err
	}

	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}

	return idx, nil
}

// 메모리 맵 파일과 실제 파일의 데이터가 확실하게 동기화 되고, 실제 데이터가 있는 만큼 자르고 파일을 닫는다.
func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}

	return i.file.Close()
}

func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	if in == -1 {
		out = uint32((i.size / endWidth) - 1)
	} else {
		out = uint32(in)
	}

	pos = uint64(out) * endWidth
	if i.size < pos+endWidth {
		return 0, 0, io.EOF
	}
	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+endWidth])

	return out, pos, nil
}

func (i *index) Write(off uint32, pos uint64) error {
	if uint64(len(i.mmap)) < i.size+endWidth {
		return io.EOF
	}

	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+endWidth], pos)
	i.size += uint64(endWidth)

	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}
