package batch

import (
	"sync"
)

type Processor interface {
	Process(value interface{}) bool
	Close()
}

func NewBatchProcessor(fn func([]interface{})) Processor {
	bp := &batchProcessor{msgs: make(chan interface{}), fn: fn}
	bp.wg.Add(1)
	go bp.run()
	return bp
}

type batchProcessor struct {
	msgs chan interface{}
	fn   func([]interface{})
	wg   sync.WaitGroup
}

func (p *batchProcessor) Start() { p.run() }
func (p *batchProcessor) Process(value interface{}) bool {
	p.msgs <- value
	return true
}
func (p *batchProcessor) Close() {
	close(p.msgs)
	p.wg.Wait()
}
func (p *batchProcessor) run() {
	defer p.wg.Done()
	for i := range p.msgs {
		// fmt.Println("got msg", i)
		batch := make([]interface{}, 0, 20)
		batch = append(batch, i)
		// fmt.Println(batch)
	loop:
		for len(batch) < 20 {
			select {
			case j, ok := <-p.msgs:
				if !ok {
					break loop
				}
				batch = append(batch, j)
				// fmt.Println(batch)
			default:
				break loop
			}
		}
		// fmt.Println(batch)
		p.fn(batch)
	}
}
