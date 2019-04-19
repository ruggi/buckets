package buckets

import (
	"sync"
)

type bucket struct {
	q        *queue
	buffer   []interface{}
	callback Callback

	id       int
	rmu      *sync.RWMutex
	stopChan chan struct{}
}

func newBucket(cap int, q *queue, callback Callback) *bucket {
	return &bucket{
		buffer:   make([]interface{}, 0, cap),
		q:        q,
		callback: callback,
		rmu:      &sync.RWMutex{},
		stopChan: make(chan struct{}),
	}
}

func (b *bucket) run(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		item, ok := b.q.pop()
		if !ok {
			return
		}
		b.lock(func() {
			b.buffer = append(b.buffer, item)
			if len(b.buffer) == cap(b.buffer) {
				b.flush()
			}
		})
	}
}

func (b *bucket) lock(fn func()) {
	b.rmu.Lock()
	defer b.rmu.Unlock()
	fn()
}

func (b *bucket) rLock(fn func()) {
	b.rmu.RLock()
	defer b.rmu.RUnlock()
	fn()
}

func (b *bucket) flush() {
	b.callback(b.buffer[:len(b.buffer)])
	b.buffer = make([]interface{}, 0, cap(b.buffer))
}
