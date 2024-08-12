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

type Worker struct {
	filePath string
	mut      sync.RWMutex
}

func (w *Worker) Write(content string) {
	w.mut.Lock()
	defer w.mut.Unlock()
	file, err := os.OpenFile(w.filePath, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0666)
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
func MapF(filename, content string, workers []*Worker) []mr.KeyValue {
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
			workers[hashIndex].Write(fmt.Sprintf("%s, %s", tw, "1"))
			// fmt.Printf("Append key \"%s\" with hash %d to the table\n", tw, hashIndex)
			wg.Done()
		}()
		// spawn many go routine to write to mr-out-var
	}
	wg.Wait()
	return kva
}

func main() {
	fileName := "pg-grimm.txt"
	workers := make([]*Worker, RReducer)
	for idx := range workers {
		// file, _ := os.OpenFile(fmt.Sprintf("mr-out-%d", idx), os.O_CREATE|os.O_APPEND, 0644)
		dir, _ := os.Getwd()
		workers[idx] = &Worker{
			mut:      sync.RWMutex{},
			filePath: fmt.Sprintf(dir+"/mr-out-%d", idx),
		}
	}
	content, err := ReadContent(fileName)
	if err != nil {
		log.Fatalf("cannot open file %v", fileName)
	}
	MapF(fileName, content, workers)
}
