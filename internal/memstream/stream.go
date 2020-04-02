package memstream

import (
	"container/list"
	"errors"
)

var (
	errNotFound    = errors.New("not found")
	errBadLimit    = errors.New("bad limit")
	errBadSequence = errors.New("bad sequence")
)

// Item to be kept inside stream.
type Item struct {
	Seq   uint64
	Value interface{}
}

// Stream is a non-thread safe simple in-memory data structure
// that maintains a stream of values limited by size and provides
// methods to access a range of values from provided position.
type Stream struct {
	top   uint64
	list  *list.List
	index map[uint64]*list.Element
}

// New creates new Stream.
func New() *Stream {
	return &Stream{
		list:  list.New(),
		index: make(map[uint64]*list.Element),
	}
}

// Add item to stream.
func (s *Stream) Add(v interface{}, size int) (uint64, error) {
	s.top++
	item := Item{
		Seq:   s.top,
		Value: v,
	}
	el := s.list.PushBack(item)
	s.index[item.Seq] = el
	for s.list.Len() > size {
		el := s.list.Front()
		item := el.Value.(Item)
		s.list.Remove(el)
		delete(s.index, item.Seq)
	}
	return s.top, nil
}

// Top returns top of stream.
func (s *Stream) Top() uint64 {
	return s.top
}

// Reset stream.
func (s *Stream) Reset() {
	s.top = 0
	s.list = list.New()
	s.index = make(map[uint64]*list.Element)
}

// Expire stream.
func (s *Stream) Expire() {
	s.list = list.New()
	s.index = make(map[uint64]*list.Element)
}

// Get items since provided position.
// If seq is zero then elements since current first element in stream will be returned.
func (s *Stream) Get(seq uint64, limit int) ([]Item, uint64, error) {

	if seq == s.top+1 {
		return nil, s.top, nil
	}

	if seq > s.top+1 {
		return nil, s.top, errBadSequence
	}

	var cap int
	if limit > 0 {
		cap = limit
	} else {
		cap = int(s.top - seq + 1)
	}

	result := make([]Item, 0, cap)
	var el *list.Element
	if seq > 0 {
		var ok bool
		el, ok = s.index[seq]
		if !ok {
			el = s.list.Front()
		}
	} else {
		el = s.list.Front()
	}

	if el == nil {
		return nil, s.top, nil
	}

	item := el.Value.(Item)
	result = append(result, item)
	var i int = 1
	for e := el.Next(); e != nil; e = e.Next() {
		if limit >= 0 && i >= limit {
			break
		}
		i++
		item := e.Value.(Item)
		result = append(result, item)
	}
	return result, s.top, nil
}
