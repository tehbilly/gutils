package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"
)

// MemoryStore is an implementation of Storage that only stores objects in memory
type MemoryStore struct {
	strings     map[string]string
	ints        map[string]int
	lists       map[string][]string
	maps        map[string]map[string]string
	objs        map[string][]byte
	expirations map[string]time.Time

	objSer ObjectSerializer
	objDes ObjectDeserializer
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		strings:     map[string]string{},
		ints:        map[string]int{},
		lists:       map[string][]string{},
		maps:        map[string]map[string]string{},
		objs:        map[string][]byte{},
		expirations: map[string]time.Time{},
		objSer:      json.Marshal,
		objDes:      json.Unmarshal,
	}
}

func (m *MemoryStore) handleExpirations() {
	for id, exp := range m.expirations {
		if time.Now().Equal(exp) || time.Now().After(exp) {
			_ = m.Delete(context.Background(), id)
			delete(m.expirations, id)
		}
	}
}

func (m *MemoryStore) SetMap(_ context.Context, id string, value map[string]string) error {
	m.maps[id] = value
	return nil
}

func (m *MemoryStore) GetMap(_ context.Context, id string) (map[string]string, error) {
	m.handleExpirations()

	v, ok := m.maps[id]
	if !ok {
		return nil, NoSuchItem
	}

	rv := make(map[string]string)

	for k, v := range v {
		rv[k] = fmt.Sprint(v)
	}

	return rv, nil
}

func (m *MemoryStore) SetMapValue(_ context.Context, id string, key string, value string) error {
	vm, ok := m.maps[id]
	if !ok {
		return NoSuchItem
	}

	vm[key] = value
	return nil
}

func (m *MemoryStore) GetMapValue(_ context.Context, id string, key string) (string, error) {
	m.handleExpirations()

	vm, ok := m.maps[id]
	if !ok {
		return "", NoSuchItem
	}

	kv, ok := vm[key]
	if !ok {
		return "", NoSuchItem
	}

	return fmt.Sprint(kv), nil
}

func (m *MemoryStore) SetObject(_ context.Context, id string, obj interface{}) error {
	bytes, err := m.objSer(obj)
	if err != nil {
		return err
	}

	m.objs[id] = bytes
	return nil
}

func (m *MemoryStore) GetObject(_ context.Context, id string, dest interface{}) error {
	m.handleExpirations()

	bytes, ok := m.objs[id]
	if !ok {
		return NoSuchItem
	}

	return m.objDes(bytes, dest)
}

func (m *MemoryStore) ListSize(ctx context.Context, id string) (int, error) {
	return m.QueueSize(ctx, id)
}

func (m *MemoryStore) ListSet(_ context.Context, id string, value []string) error {
	m.lists[id] = value
	return nil
}

func (m *MemoryStore) ListGet(_ context.Context, id string) ([]string, error) {
	m.handleExpirations()

	v, ok := m.lists[id]
	if !ok {
		return nil, NoSuchItem
	}
	return v, nil
}

func (m MemoryStore) ListGetAt(_ context.Context, id string, pos int) (string, error) {
	m.handleExpirations()

	l, ok := m.lists[id]
	if !ok {
		return "", NoSuchItem
	}

	if pos >= len(l) {
		return "", NoSuchItem
	}

	return l[pos], nil
}

func (m MemoryStore) ListSetAt(_ context.Context, id string, pos int, value interface{}) error {
	m.handleExpirations()

	l, ok := m.lists[id]
	if !ok {
		return NoSuchItem
	}

	if pos >= len(l) {
		return errors.New("index out of range")
	}

	l[pos] = fmt.Sprint(value)
	return nil
}

func (m *MemoryStore) ListInsert(_ context.Context, id string, pos int, value interface{}) error {
	m.handleExpirations()

	l, ok := m.lists[id]
	if !ok {
		return NoSuchItem
	}

	tmp := append(l[:pos+1], l[pos:]...)
	tmp[pos] = fmt.Sprint(value)
	m.lists[id] = tmp

	return nil
}

func (m *MemoryStore) ListDeleteAt(_ context.Context, id string, pos int) error {
	m.handleExpirations()

	if pos < 0 {
		return errors.New("index out of bounds")
	}

	l, ok := m.lists[id]
	if !ok {
		return NoSuchItem
	}

	if pos >= len(l) {
		return errors.New("index out of bounds")
	}

	m.lists[id] = append(l[:pos], l[pos+1:]...)

	return nil
}

func (m *MemoryStore) ListDeleteElem(_ context.Context, id string, elem interface{}) error {
	l, ok := m.lists[id]
	if !ok {
		return fmt.Errorf("list with id '%s' not found", id)
	}

	for i := 0; i < len(l); i++ {
		if l[i] == elem {
			l = append(l[:i], l[i+1:]...)
			i--
		}
	}

	m.lists[id] = l
	return nil
}

func (m *MemoryStore) QueueSize(_ context.Context, id string) (int, error) {
	q, ok := m.lists[id]
	if !ok {
		return 0, nil
	}
	return len(q), nil
}

func (m *MemoryStore) QueuePush(ctx context.Context, id string, value string) error {
	return m.QueuePushHead(ctx, id, value)
}

func (m *MemoryStore) QueuePushHead(_ context.Context, id string, value string) error {
	m.handleExpirations()

	_, ok := m.lists[id]
	if !ok {
		m.lists[id] = make([]string, 0)
	}

	m.lists[id] = append(m.lists[id], fmt.Sprint(value))
	return nil
}

func (m *MemoryStore) QueuePushTail(_ context.Context, id string, value string) error {
	m.handleExpirations()

	_, ok := m.lists[id]
	if !ok {
		m.lists[id] = make([]string, 0)
	}

	m.lists[id] = append([]string{value}, m.lists[id]...)
	return nil
}

func (m *MemoryStore) QueuePop(ctx context.Context, id string) (string, error) {
	return m.QueuePopTail(ctx, id)
}

func (m *MemoryStore) QueuePopTail(_ context.Context, id string) (string, error) {
	m.handleExpirations()

	q, ok := m.lists[id]
	if !ok {
		return "", NoSuchItem
	}

	rv, q := q[0], q[1:]
	m.lists[id] = q

	return rv, nil
}

func (m *MemoryStore) QueuePopHead(_ context.Context, id string) (string, error) {
	m.handleExpirations()

	q, ok := m.lists[id]
	if !ok {
		return "", NoSuchItem
	}

	rv, q := q[len(q)-1], q[:len(q)-1]
	m.lists[id] = q

	return rv, nil
}

// QueueBlockingTransfer is a memory implementation of Redis' RPopLPush command.  This particular command
// only checks at intervals of every 10ms.
func (m *MemoryStore) QueueBlockingTransfer(ctx context.Context, srcID, dstID string) (string, error) {
	m.handleExpirations()

	for {
		select {
		case <-time.After(10 * time.Millisecond):
			l, ok := m.lists[srcID]
			if !ok {
				continue
			}

			if len(l) < 1 {
				continue
			}

			elem, err := m.QueuePopTail(ctx, srcID)
			if err != nil {
				return "", err
			}

			return elem, m.QueuePushHead(ctx, dstID, elem)
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
}

func (m *MemoryStore) Exists(_ context.Context, id string) bool {
	m.handleExpirations()

	for _, k := range m.allKeys() {
		if id == k {
			return true
		}
	}
	return false
}

func (m *MemoryStore) Delete(_ context.Context, id string) error {
	delete(m.strings, id)
	delete(m.ints, id)
	delete(m.lists, id)
	delete(m.maps, id)
	delete(m.objs, id)
	delete(m.expirations, id)
	return nil
}

func (m *MemoryStore) DeleteAll(ctx context.Context) error {
	return m.DeleteMatching(ctx, ".*")
}

func (m *MemoryStore) allKeys() []string {
	set := map[string]struct{}{}

	for k := range m.strings {
		set[k] = struct{}{}
	}

	for k := range m.ints {
		set[k] = struct{}{}
	}

	for k := range m.lists {
		set[k] = struct{}{}
	}

	for k := range m.expirations {
		set[k] = struct{}{}
	}

	for k := range m.objs {
		set[k] = struct{}{}
	}

	for k := range m.maps {
		set[k] = struct{}{}
	}

	var keys []string

	for k := range set {
		keys = append(keys, k)
	}

	return keys
}

func (m *MemoryStore) DeleteMatching(ctx context.Context, pattern string) error {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return err
	}

	for _, key := range m.allKeys() {
		if re.MatchString(key) {
			_ = m.Delete(ctx, key)
		}
	}

	return nil
}

func (m *MemoryStore) SetExpires(_ context.Context, id string, duration time.Duration) error {
	m.expirations[id] = time.Now().Add(duration)
	return nil
}

func (m *MemoryStore) SetExpiresAt(_ context.Context, id string, when time.Time) error {
	m.expirations[id] = when
	return nil
}

func (m *MemoryStore) RemoveExpiration(_ context.Context, id string) error {
	delete(m.expirations, id)
	return nil
}

func (m *MemoryStore) GetTTL(_ context.Context, id string) (time.Duration, error) {
	m.handleExpirations()

	t, ok := m.expirations[id]
	if !ok {
		return 0 * time.Second, nil
	}

	return t.Sub(time.Now()), nil
}

func (m *MemoryStore) SetString(_ context.Context, id string, value string) error {
	m.strings[id] = value
	return nil
}

func (m *MemoryStore) GetString(_ context.Context, id string) (string, error) {
	m.handleExpirations()

	v, ok := m.strings[id]
	if !ok {
		return "", NoSuchItem
	}
	return v, nil
}

func (m *MemoryStore) SetInt(_ context.Context, id string, value int) error {
	m.ints[id] = value
	return nil
}

func (m *MemoryStore) GetInt(_ context.Context, id string) (int, error) {
	m.handleExpirations()

	v, ok := m.ints[id]
	if !ok {
		return 0, NoSuchItem
	}
	return v, nil
}

// Increment will adjust an int by +1.
func (m *MemoryStore) Increment(_ context.Context, id string) error {
	m.ints[id] = m.ints[id] + 1
	return nil
}

// Decrement will adjust an int by -1.
func (m *MemoryStore) Decrement(_ context.Context, id string) error {
	m.ints[id] = m.ints[id] - 1
	return nil
}

// IncrementBy will adjust an int by +value.
func (m *MemoryStore) IncrementBy(_ context.Context, id string, value int) error {
	m.ints[id] = m.ints[id] + value
	return nil
}

// DecrementBy will adjust an int by -value.
func (m *MemoryStore) DecrementBy(_ context.Context, id string, value int) error {
	m.ints[id] = m.ints[id] - value
	return nil
}
