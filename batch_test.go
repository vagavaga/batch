package batch_test

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vagavaga/batch"
)

// what is a rest API

// what is a resource ??? we need definition of a resource for working rest api implementation
// also what is the path in rest ???

type processor struct {
	ch   chan interface{}
	wg   sync.WaitGroup
	idx  int
	time time.Time
}
type processors map[interface{}]*processor

func (p *processors) Process(key, value interface{}) {}
func (p *processors) Wait()                          {}

type pq []*processor

func (q pq) Len() int           { return len(q) }
func (q pq) Less(i, j int) bool { return q[i].time.Before(q[j].time) }
func (q pq) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].idx, q[j].idx = i, j
}
func (q *pq) Push(x interface{}) {
	n := len(*q)
	i := x.(*processor)
	i.idx = n
	*q = append(*q, i)
}
func (q *pq) Pop() interface{} {
	old := *q
	n := len(old)
	item := old[n-1]
	item.idx = -1 // for safety
	*q = old[0 : n-1]
	return item
}

type tuple struct {
	x, y int
}

func randomInts(m, n int) <-chan int {
	ch := make(chan int)
	go func() {
		for i := 0; i < n; i++ {
			ch <- rand.Intn(m)
		}
	}()
	return ch
}

func randomTuples(count int) (<-chan tuple, <-chan map[int]int) {
	tuples := make(chan tuple, 100)
	totals := make(chan map[int]int)
	go func() {
		defer fmt.Println("generator done!")
		total := make(map[int]int)
		for i := 0; i < count; i++ {
			x, y := rand.Intn(100), rand.Intn(10)
			total[x] += y
			tuples <- tuple{x, y}
			fmt.Print(".")
		}
		close(tuples)
		totals <- total
		close(totals)
	}()
	return tuples, totals
}

func TestProcessingInSingleThread(t *testing.T) {
	var commits int64
	mapMu := &sync.Mutex{}
	actualMap := make(map[int]int)

	processingFn := func(batch []interface{}) {
		fmt.Println(len(batch), batch)
		sum := 0
		key := -1
		for _, el := range batch {
			t := el.(tuple)
			if key == -1 || key == t.x {
				key = t.x
				sum += t.y
				continue
			}
			mapMu.Lock()
			actualMap[key] += sum
			time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
			mapMu.Unlock()
			atomic.AddInt64(&commits, 1)
			key = t.x
			sum = t.y
		}
		if key != -1 {
			mapMu.Lock()
			actualMap[key] += sum
			time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
			mapMu.Unlock()
			atomic.AddInt64(&commits, 1)
		}
	}
	keyFn := func(in interface{}) interface{} {
		if t, ok := in.(tuple); ok {
			return t.x
		}
		return nil
	}
	var p batch.Processor

	_ = keyFn

	p = batch.NewKeyProcessor(keyFn, processingFn, 25)
	// p = batch.NewBatchProcessor(processingFn)
	tuples, totals := randomTuples(1000)

	for tuple := range tuples {
		p.Process(tuple)
	}
	p.Close()
	tot := <-totals
	fmt.Println(tot)
	fmt.Println("commits:", commits)
	assert.Equal(t, tot, actualMap)
}
