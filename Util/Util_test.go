package Util

import (
	"fmt"
	"testing"
)

func TestPriorityQueue(t *testing.T) {
	pq := NewPriorityQueue()

	for i := 0; i < 10; i++ {
		pq.Insert(PQElem{i, i})
	}

	for i := 0; i < 5; i++ {
		max, _ := pq.GetMax()
		min, _ := pq.GetMin()
		fmt.Printf("Max:%d Min:%d\n", max, min)

		pq.ExtractMax()
		pq.ExtractMin()
	}
}
