package snek

import (
	"context"
	"encoding/hex"
	"math/rand"
	"reflect"
	"time"
	"unsafe"

	"github.com/jmoiron/sqlx"
	"github.com/zond/snek/synch"

	_ "github.com/mattn/go-sqlite3"
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
	for _, loopSub := range s {
		go func(s subscription) {
			s.push()
		}(loopSub)
	}
}

func (s subscriptionSet) merge(other subscriptionSet) subscriptionSet {
	for id, sub := range other {
		s[id] = sub
	}
	return s
}

type permissions struct {
	queryControl  func(*View, Set) error
	updateControl func(*Update, any, any) error
}

// Snek maintains a persistent, subscribable, and access controlled data store.
type Snek struct {
	ctx           context.Context
	db            *sqlx.DB
	options       Options
	rng           *rand.Rand
	subscriptions *synch.SMap[string, *synch.SMap[string, subscription]]
	permissions   map[string]permissions
}

type systemCaller struct{}

func (s systemCaller) UserID() ID {
	return nil
}

func (s systemCaller) IsAdmin() bool {
	return false
}

func (s systemCaller) IsSystem() bool {
	return true
}

// UncontrolledQueries is a QueryControl that doesn't block any queries.
func UncontrolledQueries(*View, Set) error {
	return nil
}

// UncontrolledUpdates returns an UpdateControl that doesn't block any updates.
func UncontrolledUpdates[T any](t *T) UpdateControl[T] {
	return func(*Update, *T, *T) error {
		return nil
	}
}

// QueryControl returns nil if reading from the set is allowed in this view.
// Use View#Caller to examine the caller identity.
type QueryControl func(*View, Set) error

// UpdateControl returns nil if the update from prev (nil if Insert) to next (nil if Remove) is allowed in this update.
// Use Update#Caller to examine the caller identity.
type UpdateControl[T any] func(u *Update, prev *T, next *T) error

func (u UpdateControl[T]) call(update *Update, prev, next any) error {
	return u(update, prev.(*T), next.(*T))
}

// Register registers the type of the example structPointer in the store and ensures there is a table for the type.
func Register[T any](s *Snek, structPointer *T, queryControl QueryControl, updateControl UpdateControl[T]) error {
	info, err := s.getValueInfo(reflect.ValueOf(structPointer))
	if err != nil {
		return err
	}
	s.permissions[info.typ.Name()] = permissions{
		queryControl: queryControl,
		updateControl: func(update *Update, prev, next any) error {
			var realPrev, realNext *T
			switch v := prev.(type) {
			case *T:
				realPrev = v
			}
			switch v := next.(type) {
			case *T:
				realNext = v
			}
			return updateControl(update, realPrev, realNext)
		},
	}
	return s.Update(systemCaller{}, func(u *Update) error {
		return u.exec(info.toCreateStatement())
	})
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
