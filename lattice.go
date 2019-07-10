package batch

import (
	"sync"
	"sync/atomic"
)

func NewKeyProcessor(keyFn func(interface{}) interface{}, fn func([]interface{}), max int) Processor {
	if max <= 0 {
		panic("max must be >1")
	}
	p := &lattice{keyFn: keyFn, input: make(chan interface{}, 10), max: max}
	p.wg.Add(1)
	go p.run(fn)
	return p
}

type lattice struct {
	keyFn func(interface{}) interface{}
	max   int

	input chan interface{}
	wg    sync.WaitGroup
}

func (p *lattice) Process(m interface{}) bool { p.input <- m; return true }
func (p *lattice) Close()                     { close(p.input); p.wg.Wait() }
func (p *lattice) run(fn func([]interface{})) {
	defer p.wg.Done()
	allDone := &sync.WaitGroup{}
	syncState := &sync.Map{}

	var inputCh <-chan interface{}
loop:
	for {
		inputCh = p.input

		select {
		case m, ok := <-inputCh:
			if !ok {
				break loop
			}
			k := p.keyFn(m)

		processLoop:
			for {
				meta, _ := syncState.LoadOrStore(k, &atomicProcessor{
					wg:      allDone,
					fn:      fn,
					onClose: func() { syncState.Delete(k) /*fmt.Println("<-", k)*/ },
					in:      make(chan interface{}, p.max),
					size:    p.max,
				})

				if meta.(*atomicProcessor).Process(m) {
					break processLoop
				}
			}
		}
	}
	allDone.Wait()
}

type atomicProcessor struct {
	in      chan interface{}
	fn      func([]interface{})
	onClose func()
	wg      *sync.WaitGroup
	size    int

	inFlight int64
	once     sync.Once
}

func (p *atomicProcessor) Process(v interface{}) bool {
cs:
	for {
		x := atomic.LoadInt64(&p.inFlight)
		if x < 0 {
			return false
		}
		if atomic.CompareAndSwapInt64(&p.inFlight, x, x+1) {
			break cs
		}
	}

	p.once.Do(func() {
		p.wg.Add(1)
		go p.run()
	})
	p.in <- v
	return true
}

func (p *atomicProcessor) run() {
	defer p.wg.Done()

	for i := range p.in {
		batch := make([]interface{}, 0, p.size)
		batch = append(batch, i)

	loop:
		for len(batch) < p.size {
			select {
			case j, ok := <-p.in:
				if !ok {
					break loop
				}
				batch = append(batch, j)

			default:
				break loop
			}
		}

		p.fn(batch)
		if atomic.AddInt64(&p.inFlight, -int64(len(batch))) == 0 {
			// express intent to shut down
			if atomic.CompareAndSwapInt64(&p.inFlight, 0, -1) {
				p.onClose()
				return
			}
		}
	}
}
