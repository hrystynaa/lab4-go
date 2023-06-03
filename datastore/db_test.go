package datastore

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDb_Put(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDb(dir, 500)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	pairs := [][]string{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}

	t.Run("put/get", func(t *testing.T) {
		for _, pair := range pairs {
			t.Run(pair[0], func(t *testing.T) {
				err := db.Put(pair[0], pair[1])
				if err != nil {
					t.Errorf("Cannot put %s: %s", pair[0], err)
				}
				value, err := db.Get(pair[0])
				if err != nil {
					t.Errorf("Cannot get %s: %s", pair[0], err)
				}
				if value != pair[1] {
					t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
				}
			})
		}
	})

	outFile, err := os.Open(filepath.Join(dir, outFileName+"0"))
	if err != nil {
		t.Fatal(err)
	}

	outInfo, err := outFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	size1 := outInfo.Size()

	t.Run("file growth", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pair[0], err)
			}
		}
		outInfo, err := outFile.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if size1*2 != outInfo.Size() {
			t.Errorf("Unexpected size (%d vs %d)", size1, outInfo.Size())
		}
	})

	t.Run("new db process", func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = NewDb(dir, 300)
		if err != nil {
			t.Fatal(err)
		}

		for _, pair := range pairs {
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pair[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}
	})
}

func TestDb_Segmentation(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDb(dir, 90)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	t.Run("new file", func(t *testing.T) {

		err = db.Put("key1", "value11")
		if err != nil {
			t.Fatal(err)
		}
		err = db.Put("key2", "value21")
		if err != nil {
			t.Fatal(err)
		}
		err = db.Put("key1", "value12")
		if err != nil {
			t.Fatal(err)
		}
		err = db.Put("key2", "value22")
		if err != nil {
			t.Fatal(err)
		}
		err = db.Put("key3", "value31")
		if err != nil {
			t.Fatal(err)
		}

		if len(db.segments) != 2 {
			t.Errorf("Expected 2 segments, got %d", len(db.segments))
		}
	})

	t.Run("segmentation", func(t *testing.T) {
		t.Run("add_values", func(t *testing.T) {
			err = db.Put("key1", "value13")
			if err != nil {
				t.Fatal(err)
			}
			err = db.Put("key3", "value32")
			if err != nil {
				t.Fatal(err)
			}

		})

		t.Run("check_segments_before_segmentation", func(t *testing.T) {
			if len(db.segments) != 3 {
				t.Errorf("Очікувалося 3 сегмента, отримано %d", len(db.segments))
			}
		})

		time.Sleep(3 * time.Second)

		t.Run("check_segments_after_segmentation", func(t *testing.T) {
			if len(db.segments) != 2 {
				t.Errorf("Очікувалося 2 сегмента, отримано %d", len(db.segments))
			}
		})

		t.Run("delete old values", func(t *testing.T) {
			outFile, err := os.Open(db.segments[0].filePath)
			if err != nil {
				t.Fatal(err)
			}
			defer outFile.Close()
			outInfo, err := outFile.Stat()
			if err != nil {
				t.Fatal(err)
			}

			expectedSize := int64(69)
			if outInfo.Size() != expectedSize {
				t.Errorf("Unexpected size (%d vs %d)", expectedSize, outInfo.Size())
			}

			value, err := db.Get("key1")
			if err != nil {
				t.Fatal(err)
			}
			if value != "value13" {
				t.Errorf("Bad value returned expected value13, got %s", value)
			}
			value1, err := db.Get("key2")
			if err != nil {
				t.Fatal(err)
			}
			if value1 != "value22" {
				t.Errorf("Bad value returned expected value22, got %s", value1)
			}
		})
	})
}
