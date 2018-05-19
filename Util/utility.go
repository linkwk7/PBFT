package Util

import (
	"bytes"
	"crypto/sha1"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"sync"
)

const (
	debug = 1
)

// Struct's all member must be exported
func Digest(message interface{}) string {
	var buf bytes.Buffer
	gob.Register(message)
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(message)
	if err != nil {
		panic("Gob encode error" + err.Error())
	}

	return fmt.Sprintf("%x", sha1.Sum(buf.Bytes()))
}

// Dprintf is use to print debug function, print or not is control by a constant Debug in package Util
func Dprintf(fmt string, v ...interface{}) {
	if debug > 0 {
		log.Printf(fmt, v...)
	}
}

// PQElem is the element of priority queue
type PQElem struct {
	Pri int
	C   interface{}
}

type PriorityQueue struct {
	lock    sync.Mutex
	content []PQElem
}

// ExtractMax is use to extract the element with highest priority from priority queue
func (pq *PriorityQueue) ExtractMax() (PQElem, error) {
	// pq.lock.Lock()
	// defer pq.lock.Unlock()

	if len(pq.content) != 0 {
		elem := pq.content[len(pq.content)-1]
		pq.content = pq.content[:len(pq.content)-1]
		return elem, nil
	}
	return PQElem{}, errors.New("PriorityQueue length is 0")
}

// ExtractMin is use to extract the element with lowest priority from priority queue
func (pq *PriorityQueue) ExtractMin() (PQElem, error) {
	// pq.lock.Lock()
	// defer pq.lock.Unlock()

	if len(pq.content) != 0 {
		elem := pq.content[0]
		pq.content = pq.content[1:]
		return elem, nil
	}
	return PQElem{}, errors.New("PriorityQueue length is 0")
}

// GetMax is use to get the element with highest priority from priority queue
func (pq *PriorityQueue) GetMax() (PQElem, error) {
	// pq.lock.Lock()
	// defer pq.lock.Unlock()

	if len(pq.content) != 0 {
		elem := pq.content[len(pq.content)-1]
		return elem, nil
	}
	return PQElem{}, errors.New("PriorityQueue length is 0")
}

// GetMin is use to get the element with lowest priority from priority queue
func (pq *PriorityQueue) GetMin() (PQElem, error) {
	// pq.lock.Lock()
	// defer pq.lock.Unlock()

	if len(pq.content) != 0 {
		elem := pq.content[0]
		return elem, nil
	}
	return PQElem{}, errors.New("PriorityQueue length is 0")
}

// Insert is use to insert an element into a priority queue
func (pq *PriorityQueue) Insert(e PQElem) bool {
	// pq.lock.Lock()
	// defer pq.lock.Unlock()

	if len(pq.content) == 0 {
		pq.content = append(pq.content, e)
	} else {
		for i, sz := 0, len(pq.content); i < sz; i++ {
			if pq.content[i].Pri == e.Pri {
				return false
			} else if pq.content[i].Pri > e.Pri {
				pq.content = append(pq.content, PQElem{})
				for j := len(pq.content) - 1; j > i; j-- {
					pq.content[j] = pq.content[j-1]
				}
				pq.content[i] = e
				return true
			}
		}
		pq.content = append(pq.content, e)
	}

	return true
}

func (pq *PriorityQueue) Length() int {
	return len(pq.content)
}

// NewPriorityQueue is use to generate a new priority queue
func NewPriorityQueue() *PriorityQueue {
	return &PriorityQueue{
		content: make([]PQElem, 0),
	}
}

func DifferentElemInSlice(lst []int) int {
	count := make(map[int]bool)

	for i, sz := 0, len(lst); i < sz; i++ {
		count[lst[i]] = true
	}
	return len(count)
}
