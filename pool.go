package buckets

import (
	"sync"
	"time"
)

// Callback is a function called when flushing a bucket, giving access to the
// items pulled from it.
type Callback func(items []interface{})

// Pool processes an input stream of items and puts every item in the firstly available
// bucket. When a bucket is full or the flushing interval is reached it gets flushed and
// the callback function is called with the items that were stored in the bucket.
type Pool struct {
	buckets       []*bucket
	q             *queue
	wg            *sync.WaitGroup
	flushInterval time.Duration
	stop          chan struct{}
}

// NewPool creates a new Pool.
func NewPool(size, cap int, flushInterval time.Duration, cb Callback) *Pool {
	buckets := make([]*bucket, size)
	q := newQueue()
	for i := range buckets {
		buckets[i] = newBucket(cap, q, cb)
		buckets[i].id = i + 1
	}
	return &Pool{
		buckets:       buckets,
		wg:            &sync.WaitGroup{},
		flushInterval: flushInterval,
		q:             q,
		stop:          make(chan struct{}),
	}
}

// Add adds a new item to the pool, appending it to the first available bucket.
func (p *Pool) Add(item interface{}) bool {
	return p.q.push(item)
}

// Snapshot returns a map[int]int where the key is the ID of a bucket
// and the value is the number of items currently stored in it.
func (p *Pool) Snapshot() map[int]int {
	data := make(map[int]int)
	for _, b := range p.buckets {
		b.rLock(func() {
			data[b.id] = len(b.buffer)
		})
	}
	return data
}

// Run starts the Pool, accepting incoming items and storing them.
// It also starts the timed flushes goroutine, which will force-flush non-full
// buckets every time the related interval is reached.
func (p *Pool) Run() {
	for _, b := range p.buckets {
		p.wg.Add(1)
		go b.run(p.wg)
	}
	p.wg.Add(1)
	go p.timedFlushes()
}

// StopAndWait gracefully stops the Pool, waiting for pending buckets to be flushed.
func (p *Pool) StopAndWait() {
	p.Stop()
	p.wg.Wait()
}

// Stop stops immediately the Pool, without waiting for the pending buckets to be flushed.
func (p *Pool) Stop() {
	p.q.close()
	close(p.stop)
}

func (p *Pool) timedFlushes() {
	defer p.wg.Done()
	ticker := time.NewTicker(p.flushInterval)
	for {
		select {
		case <-ticker.C:
			for _, b := range p.buckets {
				go b.lock(b.flush)
			}
		case <-p.stop:
			ticker.Stop()
			var wg sync.WaitGroup
			for _, b := range p.buckets {
				wg.Add(1)
				go func() {
					defer wg.Done()
					b.lock(b.flush)
				}()
				wg.Wait()
			}
			return
		}
	}
}
