package buckets

type queue struct {
	closed bool
	in     chan interface{}
	stop   chan struct{}
}

func newQueue() *queue {
	return &queue{
		in:   make(chan interface{}),
		stop: make(chan struct{}),
	}
}

func (q *queue) push(item interface{}) bool {
	select {
	case <-q.stop:
		return false
	default:
		q.in <- item
		return true
	}
}

func (q *queue) pop() (interface{}, bool) {
	select {
	case <-q.stop:
		select {
		case v, ok := <-q.in:
			return v, ok
		default:
			return nil, false
		}
	case v, ok := <-q.in:
		return v, ok
	}
}

func (q *queue) close() {
	close(q.stop)
}
