package snek

import (
	"fmt"
	"log"
	"reflect"
)

// Subscriber handles results from subscriptions.
type Subscriber[T any] func([]T, error) error

type typedSubscription[T any] struct {
	typ        reflect.Type
	id         ID
	query      Query
	snek       *Snek
	subscriber Subscriber[T]
	caller     Caller
}

func (s *typedSubscription[T]) getID() ID {
	return s.id
}

func (s *typedSubscription[T]) matches(val reflect.Value) bool {
	if s.typ != val.Type() {
		return false
	}
	matches, err := s.query.Set.matches(val)
	if err != nil {
		query, _ := s.query.Set.toWhereCondition()
		log.Printf("While matching %+v to %q: %v", val.Interface(), query, err)
		return false
	}
	return matches
}

func (s *typedSubscription[T]) push() {
	results := []T{}
	subscriberErr := s.snek.View(s.caller, func(v *View) error {
		return v.Select(&results, s.query)
	})
	pushErr := s.subscriber(results, subscriberErr)
	if pushErr != nil {
		subs := s.snek.getSubscriptions(s.typ)
		subs.Del(string(s.id))
	}
}

// Subscribe creates a subscription of the data in the store matching
// the query, and asynchronously sends the current content and the
// content post any update of the store to the subscriber.
// Once the subscriber returns an error it will be cleaned up and removed.
func Subscribe[T any](s *Snek, caller Caller, query Query, subscriber Subscriber[T]) error {
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
		return fmt.Errorf("found previous subscription with new subscription ID %+v. This should never happen.", sub.id)
	}
	go func() {
		sub.push()
	}()
	return nil
}
