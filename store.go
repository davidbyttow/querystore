package querystore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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

var columnTypeToSuffix = map[ColumnType]string{
	ColumnTypeBool:    "bool",
	ColumnTypeInt64:   "int64",
	ColumnTypeFloat64: "float64",
	ColumnTypeString:  "str",
}

var columnSuffixToType = biMap(columnTypeToSuffix)

type ColumnHandle struct {
	path    string
	typ     ColumnType
	writeFp *os.File
}

func (ch *ColumnHandle) Write(b []byte) error {
	if ch.writeFp == nil {
		fp, err := os.OpenFile(ch.path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, filePerm)
		if err != nil {
			return err
		}
		ch.writeFp = fp
	}
	_, err := ch.writeFp.Write(b)
	return err
}

func (cf *ColumnHandle) IndexedWrite(index int64, v any) error {
	// TODO: handle conversions where `v` does not match the expected type

	var data []byte
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
		buf := make([]byte, 8+2+len)
		binary.LittleEndian.PutUint64(buf[:8], uint64(index))
		binary.LittleEndian.PutUint16(buf[8:10], uint16(len))
		copy(buf[10:], str)
		data = buf[:]
	}
	return cf.Write(data[:])
}

type ColumnReader struct {
	fp       *os.File
	typ      ColumnType
	curIndex int64
	curVal   any
}

func (cr *ColumnReader) SeekToIndex(targetIndex int64) (any, error) {
	if targetIndex < cr.curIndex {
		panic("cannot seek backwards")
	}

	if targetIndex == cr.curIndex {
		return cr.curVal, nil
	}

	// TODO: read in chunks, of overreading then save the last index and value
	var index int64
	var val any
	var err error
	if cr.typ == ColumnTypeString {
		var buf [10]byte
		_, err = cr.fp.Read(buf[:])
		index = int64(binary.LittleEndian.Uint64(buf[:8]))
		len := int16(binary.LittleEndian.Uint16(buf[8:10]))
		strBuf := make([]byte, len)
		cr.fp.Read(strBuf[:])
		val = string(strBuf)
	} else {
		switch cr.typ {
		case ColumnTypeBool:
			var buf [9]byte
			_, err = cr.fp.Read(buf[:])
			index = int64(binary.LittleEndian.Uint64(buf[:8]))
			val = buf[8] == 1
		case ColumnTypeInt64:
			var buf [16]byte
			_, err = cr.fp.Read(buf[:])
			index = int64(binary.LittleEndian.Uint64(buf[:8]))
			val = int64(binary.LittleEndian.Uint64(buf[8:16]))
		case ColumnTypeFloat64:
			var buf [16]byte
			_, err = cr.fp.Read(buf[:])
			index = int64(binary.LittleEndian.Uint64(buf[:8]))
			val = math.Float64frombits(binary.LittleEndian.Uint64(buf[8:16]))
		}
		if err == io.EOF {
			return nil, nil
		}
	}
	if err != nil {
		return nil, err
	}
	if index == targetIndex {
		return val, nil
	}
	if index > targetIndex {
		cr.curIndex = index
		cr.curVal = val
	}
	return nil, nil
}

func (cr *ColumnReader) Close() error {
	if cr.fp != nil {
		err := cr.fp.Close()
		cr.fp = nil
		return err
	}
	return nil
}

func (ch *ColumnHandle) createReader() (*ColumnReader, error) {
	fp, err := os.OpenFile(ch.path, os.O_RDONLY, filePerm)
	if err != nil {
		return nil, err
	}
	return &ColumnReader{fp: fp, typ: ch.typ, curIndex: -1}, nil
}

func (cf *ColumnHandle) Close() error {
	if cf.writeFp != nil {
		err := cf.writeFp.Close()
		cf.writeFp = nil
		return err
	}
	return nil
}

type ColumnFS struct {
	lock          sync.Mutex
	dir           string
	nextID        int64
	indexHandle   *ColumnHandle
	columnHandles map[string]*ColumnHandle
}

func OpenColumnFS(dir string) (*ColumnFS, error) {
	exists, err := fileExists(dir)
	if err != nil {
		return nil, err
	}
	if !exists {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, err
		}
	}

	indexPath := path.Join(dir, indexFileName)
	indexHandle := &ColumnHandle{path: indexPath, typ: ColumnTypeInt64}
	handles := map[string]*ColumnHandle{
		indexFileName: indexHandle,
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var indexSize int64
	for _, de := range entries {
		if !strings.HasSuffix(de.Name(), extension) {
			continue
		}
		if de.Name() == indexFileName {
			fi, err := de.Info()
			if err != nil {
				return nil, err
			}
			indexSize = fi.Size()
		}
		colNameAndType := strings.TrimSuffix(de.Name(), "."+extension)
		parts := strings.Split(colNameAndType, ".")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid column file name: %s", de.Name())
		}
		colName := parts[0]
		colType, ok := columnSuffixToType[parts[1]]
		if !ok {
			panic(fmt.Sprintf("unknown column type: %s", parts[1]))
		}
		ch := &ColumnHandle{path: path.Join(dir, de.Name()), typ: colType}
		handles[colName] = ch
	}

	if indexSize%16 != 0 {
		panic("index file size is not a multiple of 16")
	}

	nextID := int64(indexSize / 16)
	return &ColumnFS{dir: dir, indexHandle: indexHandle, columnHandles: handles, nextID: nextID}, nil
}

func (fs *ColumnFS) WriteColumns(fields map[string]any) error {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	for name, v := range fields {
		if strings.HasPrefix(name, "__") {
			return fmt.Errorf("column name cannot start with '__': %s", name)
		}
		ch := fs.columnHandles[name]
		if ch == nil {
			typ := valueColumnType(v)
			fn := makeColumnFileName(name, typ)
			ch := &ColumnHandle{path: path.Join(fs.dir, fn), typ: typ}
			fs.columnHandles[name] = ch
		}
	}

	index := fs.nextID
	ts := time.Now().UnixNano()

	var buf [16]byte
	binary.LittleEndian.PutUint64(buf[:8], uint64(index))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(ts))
	err := fs.indexHandle.Write(buf[:])
	if err != nil {
		return err
	}
	for name, v := range fields {
		cf := fs.columnHandles[name]
		if err := cf.IndexedWrite(index, v); err != nil {
			return err
		}
	}
	fs.nextID += 1
	return nil
}

func (fs *ColumnFS) Close() error {
	var errs []error
	for _, f := range fs.columnHandles {
		if err := f.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

type ColumnarStore struct {
	fs *ColumnFS
}

func (s *ColumnarStore) Append(fields map[string]any) error {
	return s.fs.WriteColumns(fields)
}

func (s *ColumnarStore) Query(q *Query) ([]map[string]any, error) {
	lastID := s.fs.nextID

	cols := map[string]bool{}
	rows := []map[string]any{}

	for _, f := range q.Filters {
		cols[f.Attribute] = true
	}
	if q.AggregatorAttribute != "" {
		cols[q.AggregatorAttribute] = true
	}
	cf := map[string]*ColumnReader{}
	for col := range cols {
		ch := s.fs.columnHandles[col]
		if ch == nil {
			continue
		}
		cr, err := ch.createReader()
		if err != nil {
			return nil, err
		}
		cf[col] = cr
	}

	for i := range lastID {
		pass := true
		row := map[string]any{
			"__index":     i,
			"__timestamp": 0,
		}
		for _, f := range q.Filters {
			cr := cf[f.Attribute]
			rowValue, err := cr.SeekToIndex(i)
			if err != nil {
				return nil, err
			}
			if rowValue == nil {
				pass = false
				break
			}
			filterValue := castValueToColumnType(f.Value, cr.typ)
			if !conditionals[f.Condition][cr.typ](rowValue, filterValue) {
				pass = false
				break
			}
			row[f.Attribute] = rowValue
		}
		if pass {
			rows = append(rows, row)
		}
	}

	return rows, nil
}

func NewColumnarStore(fs *ColumnFS) *ColumnarStore {
	return &ColumnarStore{fs: fs}
}
