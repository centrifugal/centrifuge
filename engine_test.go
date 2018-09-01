package centrifuge

import (
	"testing"
	"time"

	"github.com/centrifugal/centrifuge/internal/uuid"
	"github.com/stretchr/testify/assert"
)

type testMessage struct {
	Sequence   int
	Generation string
}

type testStore struct {
	Sequence   int
	Generation string
	messages   []testMessage
	lifetime   time.Duration
	size       int
	expireAt   time.Time
}

func newTestStore() *testStore {
	return &testStore{
		Sequence:   0,
		Generation: uuid.Must(uuid.NewV4()).String(),
		messages:   make([]testMessage, 0),
		lifetime:   500 * time.Millisecond,
		size:       5,
	}
}

func (s *testStore) add(m testMessage) (int, string) {
	s.Sequence++
	m.Sequence = s.Sequence
	m.Generation = s.Generation
	s.messages = append(s.messages, m)
	if len(s.messages) > s.size {
		s.messages = s.messages[1:]
	}
	s.expireAt = time.Now().Add(s.lifetime)
	return s.Sequence, s.Generation
}

func (s *testStore) get() []testMessage {
	if time.Now().Sub(s.expireAt) < 0 {
		return s.messages
	}
	return nil
}

func (s *testStore) last() (int, string) {
	return s.Sequence, s.Generation
}

func (s *testStore) recover(seq int, gen string) ([]testMessage, bool) {
	lastSeq, currentGen := s.last()
	messages := s.get()

	broken := false
	position := -1

	if seq == 0 {
		if len(messages) > 0 {
			if messages[0].Sequence == seq+1 {
				return messages, gen == currentGen
			}
			return messages, false
		}
		return nil, gen == currentGen && lastSeq == seq
	}

	for i := 0; i < len(messages); i++ {
		msg := messages[i]
		if msg.Sequence == seq {
			if msg.Generation != gen {
				broken = true
			}
			position = i + 1
			break
		}
	}
	if position > -1 {
		return messages[position:], !broken
	}

	return messages, false
}

func TestCase01(t *testing.T) {
	s := newTestStore()
	seq, gen := s.last()
	assert.Equal(t, seq, 0)
	assert.NotEmpty(t, gen)

	messages, recovered := s.recover(seq, gen)
	assert.Equal(t, 0, len(messages))
	assert.True(t, recovered)

	s.add(testMessage{Generation: "1"})
	messages, recovered = s.recover(seq, gen)
	assert.Equal(t, 1, len(messages))
	assert.True(t, recovered)
}

func TestCase02(t *testing.T) {
	s := newTestStore()

	s.add(testMessage{})
	messages, recovered := s.recover(0, "1212")
	assert.Equal(t, 1, len(messages))
	assert.False(t, recovered)
}

func TestCase03(t *testing.T) {
	s := newTestStore()

	seq1, gen := s.add(testMessage{})
	_, gen = s.add(testMessage{})
	messages, recovered := s.recover(seq1, gen)
	assert.Equal(t, 1, len(messages))
	assert.True(t, recovered)
}

func TestCase04(t *testing.T) {
	s := newTestStore()

	seq1, gen := s.add(testMessage{})
	_, gen = s.add(testMessage{})
	_, gen = s.add(testMessage{})
	_, gen = s.add(testMessage{})
	_, gen = s.add(testMessage{})
	_, gen = s.add(testMessage{})
	_, gen = s.add(testMessage{})
	messages, recovered := s.recover(seq1, gen)
	assert.Equal(t, 5, len(messages))
	assert.False(t, recovered)
}

func TestCase05(t *testing.T) {
	s := newTestStore()

	seq1, gen := s.add(testMessage{})
	_, gen = s.add(testMessage{})
	_, gen = s.add(testMessage{})
	_, gen = s.add(testMessage{})
	_, gen = s.add(testMessage{})
	messages, recovered := s.recover(seq1, gen)
	assert.Equal(t, 4, len(messages))
	assert.True(t, recovered)
}

func TestCase06(t *testing.T) {
	s := newTestStore()

	seq, gen := s.add(testMessage{})
	messages, recovered := s.recover(seq, gen)
	assert.Equal(t, 0, len(messages))
	assert.True(t, recovered)
}

func TestCase07(t *testing.T) {
	s := newTestStore()

	seq, gen := s.add(testMessage{})
	time.Sleep(time.Second)
	messages, recovered := s.recover(seq, gen)
	assert.Equal(t, 0, len(messages))
	assert.False(t, recovered)
}

func TestCase08(t *testing.T) {
	s := newTestStore()

	seq, gen := s.add(testMessage{})

	s = newTestStore()
	s.add(testMessage{})

	messages, recovered := s.recover(seq, gen)
	assert.Equal(t, 0, len(messages))
	assert.False(t, recovered)
}

func TestCase09(t *testing.T) {
	// What if we use new Store but with data saved from old one.
	s := newTestStore()
	seq, gen := s.add(testMessage{})
	oldMessages := s.messages
	oldExpireAt := s.expireAt

	s = newTestStore()
	s.Sequence = seq
	s.Generation = gen
	s.messages = oldMessages
	s.expireAt = oldExpireAt
	messages, recovered := s.recover(seq, gen)
	assert.Equal(t, 0, len(messages))
	assert.True(t, recovered)
}

func TestCase10(t *testing.T) {
	s := newTestStore()
	seq, gen := s.last()
	s.add(testMessage{})
	time.Sleep(time.Second)
	messages, recovered := s.recover(seq, gen)
	assert.Equal(t, 0, len(messages))
	assert.False(t, recovered)
}
