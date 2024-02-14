package synch

import (
	"maps"
	"sync"
)

// S is a synchronized wrapper around any type.
//
// Should be something that survives copying, like a pointer or map.
type S[V any] struct {
	v    V
	lock sync.RWMutex
}

// New returns a new S wrappingv.
func New[V any](v V) *S[V] {
	return &S[V]{v: v}
}

// Get returns the contained value in a synchronized way.
func (s *S[V]) Get() V {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.v
}

// Set sets the contained value in a synchronized way.
func (s *S[V]) Set(v V) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.v = v
}

// Read executes f while synchronized.
// f must not modify the synchronized value.
func (s *S[V]) Read(f func(v V)) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	f(s.v)
}

// Write executes f while synchronized.
func (s *S[V]) Write(f func(v V)) {
	s.lock.Lock()
	defer s.lock.Unlock()
	f(s.v)
}

// Lock is a simple mutex that only provides a synchronizer.
type Lock S[struct{}]

// Sync executes f synchronized.
func (l *Lock) Sync(f func() error) error {
	var err error
	(*S[struct{}])(l).Write(func(_ struct{}) {
		err = f()
	})
	return err
}

// SMap is a synchronized wrapper around a map.
type SMap[K comparable, V any] S[map[K]V]

// NewSMap returns a new synchronized map.
func NewSMap[K comparable, V any]() *SMap[K, V] {
	return &SMap[K, V]{
		v: map[K]V{},
	}
}

// Len returns the size of the synchronized map.
func (s *SMap[K, V]) Len() int {
	return len((*S[map[K]V])(s).Get())
}

// Clone returns an unsynchronized copy of the synchronized map.
func (s *SMap[K, V]) Clone() map[K]V {
	var result map[K]V
	(*S[map[K]V])(s).Read(func(m map[K]V) {
		result = maps.Clone(m)
	})
	return result
}

// Del deletes the key in the map, and returns the previously held value (if any) and whether there was a previous value.
func (s *SMap[K, V]) Del(k K) (V, bool) {
	var result V
	found := false
	(*S[map[K]V])(s).Write(func(m map[K]V) {
		if result, found = m[k]; found {
			delete(m, k)
		}
	})
	return result, found
}

// Get returns the held value (if any) and whether there was a value.
func (s *SMap[K, V]) Get(k K) (V, bool) {
	var result V
	found := false
	(*S[map[K]V])(s).Read(func(m map[K]V) {
		result, found = m[k]
	})
	return result, found
}

// Each executes f on each value in the synchronized map.
func (s *SMap[K, V]) Each(f func(k K, v V)) {
	(*S[map[K]V])(s).Read(func(m map[K]V) {
		for k, v := range m {
			f(k, v)
		}
	})
}

// Set sets the value for a key in the map, and returns the previously held value (if any) and whether there was a previous value.
func (s *SMap[K, V]) Set(k K, v V) (V, bool) {
	var result V
	found := false
	(*S[map[K]V])(s).Write(func(m map[K]V) {
		result, found = m[k]
		m[k] = v
	})
	return result, found
}

// SetIfMissing sets the value for a key in the map, if there was previously no value, and returns the value already there (if any) and if there was already a value.
func (s *SMap[K, V]) SetIfMissing(k K, v V) (V, bool) {
	var result V
	found := false
	(*S[map[K]V])(s).Write(func(m map[K]V) {
		if result, found = m[k]; !found {
			m[k] = v
			result = v
		}
	})
	return result, found
}
