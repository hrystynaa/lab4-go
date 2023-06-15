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

type IndexOp struct {
	isWrite bool
	key     string
	index   int64
}

type KeyPosition struct {
	segment  *Segment
	position int64
}

type Db struct {
	segments      []*Segment
	out           *os.File
	outPath       string
	outOffset     int64
	dir           string
	segmentSize   int64
	segmentIndex  int
	indexOps      chan IndexOp
	keyPositions  chan *KeyPosition
	putOps        chan entry
	putDone       chan error
	workerRequest chan WorkerRequest
	workerResult  chan WorkerResult
}

type Segment struct {
	index    hashIndex
	filePath string
}

type WorkerResult struct {
	Value string
	Err   error
}

type WorkerRequest struct {
	Key        string
	ResultChan chan WorkerResult
}

func (db *Db) addSegment() error {
	filePath := filepath.Join(db.dir, fmt.Sprintf("%s%d", outFileName, db.segmentIndex))
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return err
	}
	db.out = f
	db.outOffset = 0
	db.outPath = filePath
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
		segments:      make([]*Segment, 0),
		dir:           dir,
		segmentSize:   segmentSize,
		indexOps:      make(chan IndexOp),
		keyPositions:  make(chan *KeyPosition),
		putOps:        make(chan entry),
		putDone:       make(chan error),
		workerRequest: make(chan WorkerRequest),
		workerResult:  make(chan WorkerResult),
	}

	numWorkers := 10 // Кількість виконавців в пулі
	for i := 0; i < numWorkers; i++ {
		go db.worker()
	}

	if err := db.addSegment(); err != nil {
		return nil, err
	}

	if err := db.recover(); err != nil && err != io.EOF {
		return nil, err
	}
	db.startIndexRoutine()
	db.startPutRoutine()

	return db, nil
}

func (db *Db) worker() {
	for key := range db.workerRequest {
		op := IndexOp{
			isWrite: false,
			key:     key.Key,
		}
		db.indexOps <- op
		keyPos := <-db.keyPositions

		if keyPos == nil {
			continue
		}

		file, err := os.Open(keyPos.segment.filePath)
		if err != nil {
			db.workerResult <- WorkerResult{"", err}
			continue
		}
		defer file.Close()

		_, err = file.Seek(keyPos.position, 0)
		if err != nil {
			db.workerResult <- WorkerResult{"", err}
			continue
		}

		reader := bufio.NewReader(file)
		value, err := readValue(reader)
		if err != nil {
			db.workerResult <- WorkerResult{"", err}
			continue
		}

		db.workerResult <- WorkerResult{value, nil}
	}
}

func (db *Db) startIndexRoutine() {
	go func() {
		for {
			op := <-db.indexOps
			if op.isWrite {
				db.segments[len(db.segments)-1].index[op.key] = db.outOffset
				db.outOffset += op.index
			} else {
				var err error
				for i := len(db.segments) - 1; i >= 0; i-- {
					segment := db.segments[i]
					position, ok := segment.index[op.key]
					if ok {
						db.keyPositions <- &KeyPosition{
							segment,
							position,
						}
						err = nil
						break
					} else {
						err = ErrNotFound
					}
				}
				if err != nil {
					db.keyPositions <- nil
				}
			}
		}
	}()
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

	for _, segment := range db.segments {
		file, err := os.Open(segment.filePath)
		if err != nil {
			return err
		}
		defer file.Close()

		reader := bufio.NewReaderSize(file, bufSize)
		for err == nil {
			var (
				header, data []byte
				n            int
			)
			header, err = reader.Peek(bufSize)
			if err == io.EOF {
				if len(header) == 0 {
					break
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
			n, err = io.ReadFull(reader, data)

			if err == nil {
				if n != int(size) {
					return fmt.Errorf("corrupted file")
				}

				var e entry
				e.Decode(data)
				segment.index[e.key] = db.outOffset
				db.outOffset += int64(n)
			}
		}
	}

	return err
}

func (db *Db) Close() error {
	return db.out.Close()
}

func (db *Db) Get(key string) (string, error) {
	resultChan := make(chan WorkerResult)
	db.workerRequest <- WorkerRequest{Key: key, ResultChan: resultChan}

	go func() {
		result := <-db.workerResult
		resultChan <- result
	}()

	result := <-resultChan

	return result.Value, result.Err
}

func (db *Db) startPutRoutine() {
	go func() {
		for {
			e := <-db.putOps
			length := e.length()
			stat, err := db.out.Stat()
			if err != nil {
				db.putDone <- err
				continue
			}
			if stat.Size()+length > db.segmentSize {
				err := db.addSegment()
				if err != nil {
					db.putDone <- err
					continue
				}
			}
			n, err := db.out.Write(e.Encode())
			if err == nil {
				db.indexOps <- IndexOp{
					isWrite: true,
					key:     e.key,
					index:   int64(n),
				}
			}
			db.putDone <- nil
		}
	}()
}

func (db *Db) Put(key, value string) error {
	e := entry{
		key:   key,
		value: value,
	}
	db.putOps <- e
	return <-db.putDone
}
