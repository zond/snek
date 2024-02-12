package snek

import (
	"context"
	"encoding/hex"
	"math/rand"
	"reflect"
	"time"
	"unsafe"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/zond/snek/synch"
)

// ID is the identifier of anything.
type ID []byte

func (i ID) String() string {
	return hex.EncodeToString(i)
}

var (
	idType = reflect.TypeOf(ID{})
)

type subscription interface {
	push()
	matches(reflect.Value) bool
	getID() ID
}

type subscriptionSet map[string]subscription

func (s subscriptionSet) push() {
	for _, sub := range s {
		go func() {
			sub.push()
		}()
	}
}

func (s subscriptionSet) merge(other subscriptionSet) subscriptionSet {
	for id, sub := range other {
		s[id] = sub
	}
	return s
}

// Snek maintains a persistent, subscribable, and access controlled data store.
type Snek struct {
	ctx           context.Context
	db            *sqlx.DB
	options       Options
	rng           *rand.Rand
	subscriptions *synch.SMap[string, *synch.SMap[string, subscription]]
}

func (s *Snek) getSubscriptionsFor(val reflect.Value) subscriptionSet {
	result := subscriptionSet{}
	s.getSubscriptions(val.Type()).Each(func(id string, sub subscription) {
		if sub.matches(val) {
			result[id] = sub
		}
	})
	return result
}

func (s *Snek) getSubscriptions(typ reflect.Type) *synch.SMap[string, subscription] {
	result, _ := s.subscriptions.SetIfMissing(typ.Name(), synch.NewSMap[string, subscription]())
	return result
}

// AssertTable asserts that there is a table to support persisting data like exampleStructPointer.
func (s *Snek) AssertTable(exampleStructPointer any) error {
	return s.Update(func(u *Update) error {
		info, err := s.getValueInfo(reflect.ValueOf(exampleStructPointer))
		if err != nil {
			return err
		}
		return u.exec(info.toCreateStatement())
	})
}

// NewID returns a pseudo unique ID based on current time + 3 random uint64s.
func (s *Snek) NewID() ID {
	result := make(ID, 32)
	*(*[4]uint64)(unsafe.Pointer(&result[0])) = [4]uint64{uint64(time.Now().UnixNano()), s.rng.Uint64(), s.rng.Uint64(), s.rng.Uint64()}
	return result
}

func (s *Snek) logIf(condition bool, format string, params ...any) {
	if condition && s.options.Logger != nil {
		s.options.Logger.Printf(format, params...)
	}
}
