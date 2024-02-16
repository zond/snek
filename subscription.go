package snek

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"

	"github.com/minio/highwayhash"
)

var (
	highwayHashKey = []byte("01234567801234567899012345678901")
)

// Subscriber handles results from subscriptions.
type Subscriber[T any] func([]T, error) error

type typedSubscription[T any] struct {
	typ          reflect.Type
	id           ID
	query        Query
	snek         *Snek
	subscriber   Subscriber[T]
	caller       Caller
	lastPushHash [highwayhash.Size]byte
}

func (s *typedSubscription[T]) Close() error {
	_, found := s.snek.getSubscriptions(s.typ).Del(string(s.id))
	if !found {
		return fmt.Errorf("not open")
	}
	return nil
}

func (s *typedSubscription[T]) matches(val reflect.Value) bool {
	if s.typ != val.Type() {
		return false
	}
	matches, err := s.query.Set.matches(val)
	if err != nil {
		query, _ := s.query.Set.toWhereCondition(s.typ.Name())
		log.Printf("while matching %+v to %q: %v", val.Interface(), query, err)
		return false
	}
	return matches
}

func (s *typedSubscription[T]) load() ([]T, bool, error) {
	results := []T{}
	err := s.snek.View(s.caller, func(v *View) error {
		return v.Select(&results, s.query)
	})
	if err != nil {
		return nil, true, err
	}
	b, err := json.Marshal(results)
	if err != nil {
		return nil, true, err
	}
	hash := highwayhash.Sum(b, highwayHashKey)
	changed := hash != s.lastPushHash
	s.lastPushHash = hash
	return results, changed, nil
}

func (s *typedSubscription[T]) push() {
	results, changed, loadErr := s.load()
	if changed {
		pushErr := s.subscriber(results, loadErr)
		if pushErr != nil {
			subs := s.snek.getSubscriptions(s.typ)
			subs.Del(string(s.id))
		}
	}
}

// Subscribe creates a subscription of the data in the store matching
// the query, and asynchronously sends the current content and the
// content post any update of the store to the subscriber.
// Once the subscriber returns an error it will be cleaned up and removed.
func Subscribe[T any](s *Snek, caller Caller, query Query, subscriber Subscriber[T]) (Subscription, error) {
	if len(query.Joins) > 0 {
		return nil, fmt.Errorf("join queries can't be subscribed - notifying on updates in joins not implemented")
	}
	if query.Set == nil {
		query.Set = All{}
	}
	sub := &typedSubscription[T]{
		typ:        reflect.TypeOf(*new(T)),
		id:         s.NewID(),
		snek:       s,
		query:      query,
		subscriber: subscriber,
		caller:     caller,
	}
	subs := s.getSubscriptions(sub.typ)
	if _, found := subs.Set(string(sub.id), sub); found {
		return nil, fmt.Errorf("found previous subscription with new subscription ID %+v. This should never happen.", sub.id)
	}
	go func() {
		sub.push()
	}()
	return sub, nil
}
