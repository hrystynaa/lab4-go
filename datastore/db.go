package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	outFileName = "current-data"
	bufSize     = 8192
)

var ErrNotFound = fmt.Errorf("record does not exist")

type hashIndex map[string]int64

type Db struct {
	segments     []*Segment
	out          *os.File
	outOffset    int64
	dir          string
	segmentSize  int64
	segmentIndex int
}

type Segment struct {
	index    hashIndex
	filePath string
}

func (db *Db) addSegment() error {
	filePath := filepath.Join(db.dir, fmt.Sprintf("%s%d", outFileName, db.segmentIndex))
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return err
	}
	db.out = f
	db.outOffset = 0

	segment := &Segment{
		filePath: filePath,
		index:    make(hashIndex),
	}
	db.segments = append(db.segments, segment)
	if len(db.segments) >= 3 {
		db.compact()
	}
	db.segmentIndex++
	return err
}

func NewDb(dir string, segmentSize int64) (*Db, error) {
	db := &Db{
		segments:    make([]*Segment, 0),
		dir:         dir,
		segmentSize: segmentSize,
	}

	if err := db.addSegment(); err != nil {
		return nil, err
	}

	if err := db.recover(); err != nil && err != io.EOF {
		return nil, err
	}
	return db, nil
}

func (db *Db) compact() {
	go func() {
		var offset int64
		filePath := filepath.Join(db.dir, fmt.Sprintf("%s%d", outFileName, db.segmentIndex))
		db.segmentIndex++
		segment := &Segment{
			filePath: filePath,
			index:    make(hashIndex),
		}
		f, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
		if err != nil {
			return
		}
		segmentIndex := len(db.segments) - 2
		for i := 0; i <= segmentIndex; i++ {
			s := db.segments[i]
			for key := range s.index {
				if i < segmentIndex && db.checkKey(key, db.segments[i+1:segmentIndex+1]) {
					continue
				}
				value, _ := db.Get(key)
				e := entry{
					key:   key,
					value: value,
				}
				n, err := f.Write(e.Encode())
				if err == nil {
					segment.index[key] = offset
					offset += int64(n)
				}
			}
		}
		db.segments = append([]*Segment{segment}, db.segments[len(db.segments)-1])
	}()
}

func (db *Db) checkKey(key string, segments []*Segment) bool {
	for _, s := range segments {
		if _, ok := s.index[key]; ok {
			return true
		}
	}
	return false
}

func (db *Db) recover() error {
	var err error
	var buf [bufSize]byte

	in := bufio.NewReaderSize(db.out, bufSize)
	for err == nil {
		var (
			header, data []byte
			n            int
		)
		header, err = in.Peek(bufSize)
		if err == io.EOF {
			if len(header) == 0 {
				return err
			}
		} else if err != nil {
			return err
		}
		size := binary.LittleEndian.Uint32(header)

		if size < bufSize {
			data = buf[:size]
		} else {
			data = make([]byte, size)
		}
		n, err = io.ReadFull(in, data)

		if err == nil {
			if n != int(size) {
				return fmt.Errorf("corrupted file")
			}

			var e entry
			e.Decode(data)
			db.segments[len(db.segments)-1].index[e.key] = db.outOffset
			db.outOffset += int64(n)
		}
	}
	return err
}

func (db *Db) Close() error {
	return db.out.Close()
}

func (db *Db) Get(key string) (string, error) {
	for i := len(db.segments) - 1; i >= 0; i-- {
		segment := db.segments[i]
		position, ok := segment.index[key]
		if ok {
			file, err := os.Open(segment.filePath)
			if err != nil {
				return "", err
			}
			defer file.Close()

			_, err = file.Seek(position, io.SeekStart)
			if err != nil {
				return "", err
			}

			reader := bufio.NewReader(file)
			value, err := readValue(reader)
			if err != nil {
				return "", err
			}
			return value, nil
		}
	}

	return "", ErrNotFound
}

func (db *Db) Put(key, value string) error {
	e := entry{
		key:   key,
		value: value,
	}
	length := e.length()
	if db.shouldCreateNewSegment(length) {
		if err := db.addSegment(); err != nil {
			return err
		}
	}
	n, err := db.writeEntry(e)
	if err == nil {
		db.updateIndex(key, db.outOffset)
		db.outOffset += n
	}
	return err
}

func (db *Db) shouldCreateNewSegment(entryLength int64) bool {
	stat, err := db.out.Stat()
	if err != nil {
		return false
	}
	return stat.Size()+int64(entryLength) > db.segmentSize
}

func (db *Db) writeEntry(e entry) (int64, error) {
	n, err := db.out.Write(e.Encode())
	if err != nil {
		return 0, err
	}
	return int64(n), nil
}

func (db *Db) updateIndex(key string, position int64) {
	db.segments[len(db.segments)-1].index[key] = position
}
