package sst

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"sync"
	"testing"

	"github.com/Pallinder/go-randomdata"
)

var once sync.Once
var data map[string]string
var sortedKeys []string

func create() (*bytes.Buffer, error) {
	buf := new(bytes.Buffer)

	w := NewWriter(buf)

	once.Do(func() {
		data = make(map[string]string)
		sortedKeys = make([]string, 1000)

		for i := 0; i < 10; i++ {
			sortedKeys[i] = randomdata.Email()
			data[sortedKeys[i]] = randomdata.FirstName(randomdata.RandomGender)
		}

		sort.Strings(sortedKeys)
	})

	for _, key := range sortedKeys {
		w.Set([]byte(key), []byte(data[key]))
	}

	err := w.Close()

	return buf, err
}

func read(buf *bytes.Buffer) error {
	r, _ := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))

	for key, val := range data {
		if found, err := r.Get([]byte(key)); string(found) != string(val) || err != nil {
			return fmt.Errorf("Key %q: wanted: %q, found: %q", key, val, found)
		}

		iter, err := r.Find([]byte(key))

		if err != nil {
			return err
		}

		if !iter.Next() {
			return fmt.Errorf("Iterating for %q: not found", key)
		}

		if string(iter.Key()) != string(key) {
			return fmt.Errorf("Iterating for %q: wrong key: %q", key, iter.Key())
		}

		if string(iter.Value()) != string(val) {
			return fmt.Errorf("Iterating for %q: wrong val: %q", key, iter.Value())
		}
	}

	return nil
}

func TestWriter(t *testing.T) {
	buf, err := create()
	if err != nil {
		t.Fatal(err)
	}

	err = read(buf)
	if err != nil {
		t.Fatal(err)
	}

	os.MkdirAll("tmp", 0755)
	os.Remove("tmp/test.sst")

	file, err := os.Create("tmp/test.sst")
	if err != nil {
		t.Fatal(err)
	}

	_, err = buf.WriteTo(file)
	if err != nil {
		t.Fatal(err)
	}
	file.Close()

	file, err = os.Open("tmp/test.sst")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := ioutil.ReadAll(file)
	err = read(bytes.NewBuffer(b))
	if err != nil {
		t.Fatal(err)
	}
}
