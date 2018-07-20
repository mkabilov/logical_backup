/*
A GoRoutine safe queue.

based on github.com/damnever/goqueue
*/

package queue

import (
	"container/list"
	"context"
	"errors"
	"sync"
)

var (
	// ErrEmptyQueue is returned when queue is empty.
	ErrEmptyQueue = errors.New("queue is empty")
)

type waiter chan interface{}

func newWaiter() waiter {
	w := make(chan interface{}, 1)
	return w
}

// Queue is data structure, which has much similar behavior with channel.
type Queue struct {
	ctx context.Context

	mutex   sync.Mutex
	items   *list.List // store items
	putters *list.List // store blocked Put operators
	getters *list.List // store blocked Get operators
}

// New create a new Queue
func New(ctx context.Context) *Queue {
	return &Queue{
		ctx:     ctx,
		mutex:   sync.Mutex{},
		items:   list.New(),
		putters: list.New(),
		getters: list.New(),
	}
}

func (q *Queue) newPutter() *list.Element {
	w := newWaiter()
	return q.putters.PushBack(w)
}

func (q *Queue) newGetter() *list.Element {
	w := newWaiter()
	return q.getters.PushBack(w)
}

func (q *Queue) notifyPutter(getter *list.Element) bool {
	if getter != nil {
		q.getters.Remove(getter)
	}
	if q.putters.Len() == 0 {
		return false
	}
	e := q.putters.Front()
	q.putters.Remove(e)
	w := e.Value.(waiter)
	w <- true
	return true
}

func (q *Queue) notifyGetter(putter *list.Element, val interface{}) bool {
	if putter != nil {
		q.putters.Remove(putter)
	}
	if q.getters.Len() == 0 {
		return false
	}
	e := q.getters.Front()
	q.getters.Remove(e)
	w := e.Value.(waiter)
	w <- val

	return true
}

func (q *Queue) clearPending() {
	for q.putters.Len() != 0 {
		q.notifyPutter(nil)
	}

	for !q.isempty() && q.getters.Len() != 0 {
		v := q.get()
		q.notifyGetter(nil, v)
	}
}

func (q *Queue) get() interface{} {
	e := q.items.Front()
	q.items.Remove(e)

	return e.Value
}

func (q *Queue) put(val interface{}) {
	q.items.PushBack(val)
}

// Get gets an element from Queue.
func (q *Queue) Get() (interface{}, error) {
	q.mutex.Lock()
	q.clearPending()
	if !q.isempty() {
		v := q.get()
		q.notifyPutter(nil)
		q.mutex.Unlock()

		return v, nil
	}

	e := q.newGetter()
	q.mutex.Unlock()
	w := e.Value.(waiter)

	var v interface{}
	select {
	case v = <-w:
	case <-q.ctx.Done():
		return nil, q.ctx.Err()
	}

	q.mutex.Lock()
	q.notifyPutter(e)
	q.mutex.Unlock()

	return v, nil
}

// Put puts an element into Queue.
func (q *Queue) Put(val interface{}) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	q.clearPending()
	if !q.notifyGetter(nil, val) {
		q.put(val)
	}
}

func (q *Queue) size() int {
	return q.items.Len()
}

// Size returns the size of Queue.
func (q *Queue) Size() int {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	return q.size()
}

func (q *Queue) isempty() bool {
	return q.size() == 0
}

// IsEmpty returns true if Queue is empty.
func (q *Queue) IsEmpty() bool {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	return q.isempty()
}
