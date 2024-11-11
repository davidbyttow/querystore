package querystore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"strings"
	"sync"
	"time"
)

const (
	extension         = "dat"
	indexFileName     = "__index" + "." + extension
	timestampFileName = "__timestamp" + "." + extension
	filePerm          = 0644
)

type ColumnType int

const (
	ColumnTypeBool ColumnType = iota
	ColumnTypeInt64
	ColumnTypeFloat64
	ColumnTypeString
)

type ColumnFile struct {
	typ  ColumnType
	fp   *os.File
	size int64
}

func OpenColumnFile(path string, typ ColumnType, writer bool) (*ColumnFile, error) {
	var size int64
	stat, err := os.Stat(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	println(path, err)
	if os.IsExist(err) {
		size = stat.Size()
	}
	mode := os.O_RDONLY
	if writer {
		mode = os.O_WRONLY | os.O_APPEND | os.O_CREATE
	}
	indexFile, err := os.OpenFile(path, mode, filePerm)
	if err != nil {
		return nil, err
	}
	return &ColumnFile{typ: typ, fp: indexFile, size: size}, nil
}

func (cf *ColumnFile) IndexedWrite(index int64, v any) error {
	var data []byte
	var err error
	switch cf.typ {
	case ColumnTypeBool:
		var buf [9]byte
		binary.LittleEndian.PutUint64(buf[:8], uint64(index))
		if v.(bool) {
			buf[8] = 1
		} else {
			buf[8] = 0
		}
		data = buf[:]
	case ColumnTypeInt64:
		var buf [16]byte
		binary.LittleEndian.PutUint64(buf[:8], uint64(index))
		binary.LittleEndian.PutUint64(buf[8:16], toUint64(v))
		data = buf[:]
	case ColumnTypeFloat64:
		var buf [16]byte
		binary.LittleEndian.PutUint64(buf[:8], uint64(index))
		binary.LittleEndian.PutUint64(buf[8:16], math.Float64bits(toFloat64(v)))
		data = buf[:]
	case ColumnTypeString:
		str := v.(string)
		len := len(str)
		buf := make([]byte, 16+len)
		binary.LittleEndian.PutUint64(buf[:8], uint64(index))
		binary.LittleEndian.PutUint64(buf[8:16], uint64(len))
		data = buf[:]
	}
	_, err = cf.fp.Write(data[:])
	return err
}

func (cf *ColumnFile) Close() error {
	return cf.fp.Close()
}

type ColumnWriter struct {
	lock      sync.Mutex
	dir       string
	next      int64
	indexFile *ColumnFile
	tsFile    *ColumnFile
	files     map[string]*ColumnFile
}

func OpenColumnWriter(dir string) (*ColumnWriter, error) {
	exists, err := FileExists(dir)
	if err != nil {
		return nil, err
	}
	if !exists {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, err
		}
	}
	indexFile, err := OpenColumnFile(path.Join(dir, indexFileName), ColumnTypeInt64, true)
	if err != nil {
		return nil, err
	}
	tsFile, err := OpenColumnFile(path.Join(dir, timestampFileName), ColumnTypeInt64, true)
	if err != nil {
		return nil, err
	}
	if indexFile.size != tsFile.size {
		return nil, fmt.Errorf("mismatch index and timestamp file sizes: %d != %d", indexFile.size, tsFile.size)
	}
	files := map[string]*ColumnFile{
		indexFileName: indexFile,

		timestampFileName: tsFile,
	}
	next := int64(indexFile.size << 3)
	return &ColumnWriter{dir: dir, indexFile: indexFile, tsFile: tsFile, files: files, next: next}, nil
}

func (cw *ColumnWriter) WriteColumns(fields map[string]any) error {
	cw.lock.Lock()
	defer cw.lock.Unlock()

	for name, v := range fields {
		if strings.HasPrefix(name, "__") {
			return fmt.Errorf("column name cannot start with '__': %s", name)
		}
		var ct ColumnType
		switch v.(type) {
		case bool:
			ct = ColumnTypeBool
		case string:
			ct = ColumnTypeString
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			ct = ColumnTypeInt64
		case float32, float64:
			ct = ColumnTypeFloat64
		default:
			return fmt.Errorf("unsupported type: %T", v)
		}
		_, ok := cw.files[name]
		if !ok {
			cf, err := OpenColumnFile(path.Join(cw.dir, name+"."+extension), ct, true)
			if err != nil {
				return err
			}
			cw.files[name] = cf
		}
	}

	// TODO: handle partial failure somehow
	index := cw.next
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(index))
	_, err := cw.indexFile.fp.Write(buf[:])
	if err != nil {
		return err
	}
	cw.next += 1
	ts := time.Now().UnixNano()
	if err = cw.tsFile.IndexedWrite(index, ts); err != nil {
		return err
	}

	for name, v := range fields {
		cf := cw.files[name]
		if err := cf.IndexedWrite(index, v); err != nil {
			return err
		}
	}
	return nil
}

func (fs *ColumnWriter) Close() error {
	var errs []error
	for _, f := range fs.files {
		if err := f.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

type ColumnarStore struct {
	fs *ColumnWriter
}

func (s *ColumnarStore) Append(fields map[string]any) error {
	return s.fs.WriteColumns(fields)
}

func NewColumnarStore(fs *ColumnWriter) *ColumnarStore {
	return &ColumnarStore{fs: fs}
}
