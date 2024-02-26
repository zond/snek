package snek

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"

	"github.com/minio/highwayhash"
	"github.com/zond/snek/synch"
)

var (
	highwayHashKey = []byte("01234567801234567899012345678901")
)

// Subscriber handles data from subscriptions.
// Create subscribers by calling TypedSubscriber or AnySubscriber.
type Subscriber interface {
	handleResults(structSlicePointer any, err error) error
	prepareResult() (structSlicePointer any)
	getType() (structType reflect.Type)
}

type typedSubscriber[T any] struct {
	handler    func([]T, error) error
	structType reflect.Type
}

func (s *typedSubscriber[T]) handleResults(structSlicePointer any, err error) error {
	return s.handler(*(structSlicePointer.(*[]T)), err)
}

func (s *typedSubscriber[T]) prepareResult() any {
	res := []T{}
	return &res
}

func (s *typedSubscriber[T]) getType() reflect.Type {
	return s.structType
}

type anySubscriber struct {
	handler    func(structSlice any, err error) error
	structType reflect.Type
	sliceType  reflect.Type
}

func (a *anySubscriber) handleResults(structSlicePointer any, err error) error {
	return a.handler(reflect.ValueOf(structSlicePointer).Elem().Interface(), err)
}

func (a *anySubscriber) prepareResult() any {
	slicePointer := reflect.New(a.sliceType)
	slicePointer.Elem().Set(reflect.MakeSlice(a.sliceType, 0, 0))
	return slicePointer.Interface()
}

func (a *anySubscriber) getType() reflect.Type {
	return a.structType
}

// AnySubscriber returns a subscriber handling untyped results. The results are still slices of structs.
func AnySubscriber(structType reflect.Type, handler func(structSlice any, err error) error) Subscriber {
	return &anySubscriber{
		handler:    handler,
		structType: structType,
		sliceType:  reflect.SliceOf(structType),
	}
}

// TypedSubscriber returns a subscriber handling typed results, which might be more convenient.
func TypedSubscriber[T any](handler func([]T, error) error) Subscriber {
	return &typedSubscriber[T]{
		handler:    handler,
		structType: reflect.TypeOf(*new(T)),
	}
}

type subscription struct {
	id           ID
	query        *Query
	snek         *Snek
	subscriber   Subscriber
	caller       Caller
	lastPushHash [highwayhash.Size]byte
	lock         synch.Lock
}

func (s *subscription) Close() error {
	_, found := s.snek.getSubscriptions(s.subscriber.getType()).Del(string(s.id))
	if !found {
		return fmt.Errorf("not open")
	}
	return nil
}

func (s *subscription) matches(val reflect.Value) bool {
	if s.subscriber.getType() != val.Type() {
		return false
	}
	matches, err := s.query.Set.matches(val)
	if err != nil {
		query, _ := s.query.Set.toWhereCondition(s.subscriber.getType().Name())
		log.Printf("while matching %+v to %q: %v", val.Interface(), query, err)
		return false
	}
	return matches
}

func (s *subscription) load() (any, [highwayhash.Size]byte, error) {
	results := s.subscriber.prepareResult()
	err := s.snek.View(s.caller, func(v *View) error {
		return v.Select(results, s.query)
	})
	var emptyHash [highwayhash.Size]byte
	if err != nil {
		return nil, emptyHash, err
	}
	b, err := json.Marshal(results)
	if err != nil {
		return nil, emptyHash, err
	}
	hash := highwayhash.Sum(b, highwayHashKey)
	return results, hash, nil
}

func (s *subscription) push() {
	// It might seem crazy to hold a lock through not one but _two_ I/O operations (load from DB and send to a likely WebSocket),
	// but since this is unique per subscription it's fine - no client is really interested in multiple parallel deliveries of
	// data from the same subscription anyway.
	s.lock.Sync(func() error {
		results, hash, loadErr := s.load()
		if hash != s.lastPushHash {
			pushErr := s.subscriber.handleResults(results, loadErr)
			if pushErr != nil {
				subs := s.snek.getSubscriptions(s.subscriber.getType())
				subs.Del(string(s.id))
			} else {
				s.lastPushHash = hash
			}
		}
		return nil
	})
}

// Subscribe creates a subscription of the data in the store matching
// the query, and asynchronously sends the current content and the
// content post any update of the store to the subscriber.
// If the subscriber returns an error it will be cleaned up and removed.
func Subscribe(s *Snek, caller Caller, query *Query, subscriber Subscriber) (Subscription, error) {
	if len(query.Joins) > 0 {
		return nil, fmt.Errorf("join queries can't be subscribed - notifying on updates in joins not implemented")
	}
	if query.Set == nil {
		query.Set = All{}
	}
	sub := &subscription{
		id:         s.NewID(),
		snek:       s,
		query:      query,
		subscriber: subscriber,
		caller:     caller,
	}
	subs := s.getSubscriptions(sub.subscriber.getType())
	subs.Set(string(sub.id), sub)
	go func() {
		sub.push()
	}()
	return sub, nil
}
