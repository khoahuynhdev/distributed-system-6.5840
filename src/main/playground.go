//go:build ignore

package main

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
	"time"
	"unicode"

	"6.5840/mr"
)

const (
	RReducer = 5
)

func GetHash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func ReadContent(fileName string) (string, error) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal("cannot open %v", fileName)
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return "", err
	}

	return string(content), nil
}

type SyncWriter struct {
	FilePath string
	KVA      []struct {
		Key   string
		Value string
	}
	Mut sync.RWMutex
	Ch  chan string
	Q   chan string
}

func (w *SyncWriter) Dispatch() {
	c, q := w.Ch, w.Q
	for {
		select {
		case v := <-c:
			// fmt.Println("Adding to dict, value: ", v)
			w.KVA = append(w.KVA, struct {
				Key   string
				Value string
			}{Key: v, Value: "1"})
		case <-q:
			fmt.Printf("%+v\n", w.KVA)
			return
		default:
			time.Sleep(time.Second)
		}
	}
}

// PROBLEM

func (w *SyncWriter) Write(content string) {
	w.Mut.Lock()
	defer w.Mut.Unlock()
	file, err := os.OpenFile(w.FilePath, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		fmt.Printf("Get Error opening file %v", err)
		return
	}
	_, err = fmt.Fprintf(file, "%s%s", content, "\n")
	if err != nil {
		fmt.Printf("failed to write file %v", err)
	}
	file.Close()
}

// 3 Map Workers

// (k1, v1) => [](k2,v2)
func MapF(filename, content string, writers []*SyncWriter) []mr.KeyValue {
	kva := []mr.KeyValue{}
	ff := func(r rune) bool { return !unicode.IsLetter(r) }
	words := strings.FieldsFunc(content, ff)
	var wg sync.WaitGroup
	for _, w := range words {
		wg.Add(1)
		tw := w
		go func() {
			// kva = append(kva, mr.KeyValue{Key: tw, Value: "1"})
			// add the Keva to the right partition by hashing % RReducer
			hashIndex := GetHash(tw) % RReducer
			writers[hashIndex].Ch <- tw
			// fmt.Printf("Append key \"%s\" with hash %d to the table\n", tw, hashIndex)
			wg.Done()
		}()
		// spawn many go routine to write to mr-out-var
	}
	wg.Wait()
	for _, wr := range writers {
		wr.Q <- ""
	}
	return kva
}

func main() {
	fileName := "pg-grimm.txt"
	writers := make([]*SyncWriter, RReducer)
	for idx := range writers {
		// file, _ := os.OpenFile(fmt.Sprintf("mr-out-%d", idx), os.O_CREATE|os.O_APPEND, 0644)
		dir, _ := os.Getwd()
		writers[idx] = &SyncWriter{
			Mut:      sync.RWMutex{},
			FilePath: fmt.Sprintf(dir+"/mr-out-%d", idx),
			Ch:       make(chan string),
			Q:        make(chan string),
		}

		go writers[idx].Dispatch()
	}
	content, err := ReadContent(fileName)
	if err != nil {
		log.Fatalf("cannot open file %v", fileName)
	}
	MapF(fileName, content, writers)
}
