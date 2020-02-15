package centrifuge

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/centrifugal/centrifuge/internal/uuid"
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
	if time.Since(s.expireAt) < 0 {
		return s.messages
	}
	return nil
}

func (s *testStore) last() (int, string) {
	return s.Sequence, s.Generation
}

func (s *testStore) recover(seq int, gen string) ([]testMessage, bool) {
	// Following 2 lines must be atomic.
	lastSeq, currentGen := s.last()
	messages := s.get()

	broken := false
	position := -1

	if lastSeq == seq && gen == currentGen {
		return nil, true
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
		if msg.Sequence == seq+1 {
			if msg.Generation != gen {
				broken = true
			}
			position = i
			break
		}
	}
	if position > -1 {
		return messages[position:], !broken
	}

	return messages, false
}

func TestCaseAbstractRecover01(t *testing.T) {
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

func TestCaseAbstractRecover02(t *testing.T) {
	s := newTestStore()
	s.add(testMessage{})
	messages, recovered := s.recover(0, "1212")
	assert.Equal(t, 1, len(messages))
	assert.False(t, recovered)
}

func TestCaseAbstractRecover03(t *testing.T) {
	s := newTestStore()
	seq1, _ := s.add(testMessage{})
	_, gen := s.add(testMessage{})
	messages, recovered := s.recover(seq1, gen)
	assert.Equal(t, 1, len(messages))
	assert.True(t, recovered)
}

func TestCaseAbstractRecover04(t *testing.T) {
	s := newTestStore()
	seq1, gen := s.add(testMessage{})
	for i := 0; i < 6; i++ {
		_, gen = s.add(testMessage{})
	}
	messages, recovered := s.recover(seq1, gen)
	assert.Equal(t, 5, len(messages))
	assert.False(t, recovered)
}

func TestCaseAbstractRecover05(t *testing.T) {
	s := newTestStore()
	seq1, gen := s.add(testMessage{})
	for i := 0; i < 4; i++ {
		_, gen = s.add(testMessage{})
	}
	messages, recovered := s.recover(seq1, gen)
	assert.Equal(t, 4, len(messages))
	assert.True(t, recovered)
}

func TestCaseAbstractRecover06(t *testing.T) {
	s := newTestStore()
	seq, gen := s.add(testMessage{})
	messages, recovered := s.recover(seq, gen)
	assert.Equal(t, 0, len(messages))
	assert.True(t, recovered)
}

func TestCaseAbstractRecover07(t *testing.T) {
	s := newTestStore()
	seq, gen := s.add(testMessage{})
	time.Sleep(time.Second)
	messages, recovered := s.recover(seq, gen)
	assert.Equal(t, 0, len(messages))
	assert.True(t, recovered)
}

func TestCaseAbstractRecover08(t *testing.T) {
	s := newTestStore()
	seq, gen := s.add(testMessage{})

	s = newTestStore()
	s.add(testMessage{})

	messages, recovered := s.recover(seq, gen)
	assert.Equal(t, 0, len(messages))
	assert.False(t, recovered)
}

func TestCaseAbstractRecover09(t *testing.T) {
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

func TestCaseAbstractRecover10(t *testing.T) {
	s := newTestStore()
	seq, gen := s.last()
	s.add(testMessage{})
	time.Sleep(time.Second)
	messages, recovered := s.recover(seq, gen)
	assert.Equal(t, 0, len(messages))
	assert.False(t, recovered)
}

func TestCaseAbstractRecover11(t *testing.T) {
	s := newTestStore()
	seq, gen := s.last()
	for i := 0; i < 7; i++ {
		s.add(testMessage{})
	}
	messages, recovered := s.recover(seq, gen)
	assert.Equal(t, 5, len(messages))
	assert.False(t, recovered)
}

func TestCaseAbstractRecover12(t *testing.T) {
	s := newTestStore()
	seq, gen := s.last()
	for i := 0; i < 3; i++ {
		s.add(testMessage{})
	}
	time.Sleep(time.Second)
	s.messages = nil
	for i := 0; i < 3; i++ {
		s.add(testMessage{})
	}
	messages, recovered := s.recover(seq, gen)
	assert.Equal(t, 3, len(messages))
	assert.False(t, recovered)
}
