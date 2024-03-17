package snek

import (
	"bytes"
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

const (
	sqliteTimeFormat = "2006-01-02 15:04:05.999"
)

// TimeText represents timestamps in SQLite.
type TimeText string

func (t TimeText) Time() time.Time {
	res, err := time.Parse(sqliteTimeFormat, string(t))
	if err != nil {
		return time.Time{}
	}
	return res
}

func ToText(t time.Time) TimeText {
	return TimeText(t.Format(sqliteTimeFormat))
}

// ID is the identifier of anything.
type ID []byte

func (i ID) String() string {
	return hex.EncodeToString(i)
}

// Equal returns if this ID is equal to another ID.
func (i ID) Equal(other ID) bool {
	return bytes.Compare(i, other) == 0
}

var (
	idType = reflect.TypeOf(ID{})
)

type Subscription interface {
	push()
	matches(reflect.Value) bool
	Close() error
}

type subscriptionSet map[string]Subscription

func (s subscriptionSet) push() {
	for _, loopSub := range s {
		go func(s Subscription) {
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
	queryControl  func(*View, *Query) error
	updateControl func(*Update, any, any) error
}

// Snek maintains a persistent, subscribable, and access controlled data store.
type Snek struct {
	ctx           context.Context
	db            *sqlx.DB
	options       Options
	rng           *rand.Rand
	subscriptions *synch.SMap[string, *synch.SMap[string, Subscription]]
	permissions   map[string]permissions
}

type SystemCaller struct{}

func (s SystemCaller) UserID() ID {
	return nil
}

func (s SystemCaller) IsAdmin() bool {
	return false
}

func (s SystemCaller) IsSystem() bool {
	return true
}

// AnonCaller is a caller without identity.
type AnonCaller struct{}

func (a AnonCaller) UserID() ID {
	return nil
}

func (a AnonCaller) IsAdmin() bool {
	return false
}

func (a AnonCaller) IsSystem() bool {
	return false
}

// UncontrolledQueries is a QueryControl that doesn't block any queries.
func UncontrolledQueries(*View, *Query) error {
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
// It is permissible for QueryControl to modify the query if necessary.
type QueryControl func(*View, *Query) error

// UpdateControl returns nil if the update from prev (nil if Insert) to next (nil if Remove) is allowed in this update.
// Use Update#Caller to examine the caller identity.
// It is permissible for UpdateControl to modify the next value if necessary.
type UpdateControl[T any] func(u *Update, prev *T, next *T) error

func (u UpdateControl[T]) call(update *Update, prev, next any) error {
	return u(update, prev.(*T), next.(*T))
}

// Register registers the type of the example structPointer in the store and ensures there is a table for the type.
func Register[T any](s *Snek, structPointer *T, queryControl QueryControl, updateControl UpdateControl[T]) error {
	info, err := getValueInfo(reflect.ValueOf(structPointer))
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
	return s.Update(SystemCaller{}, func(u *Update) error {
		return u.exec(info.toCreateStatement())
	})
}

func (s *Snek) getSubscriptionsFor(val reflect.Value) subscriptionSet {
	result := subscriptionSet{}
	s.getSubscriptions(val.Type()).Each(func(id string, sub Subscription) {
		if sub.matches(val) {
			result[id] = sub
		}
	})
	return result
}

func (s *Snek) getSubscriptions(typ reflect.Type) *synch.SMap[string, Subscription] {
	result, _ := s.subscriptions.SetIfMissing(typ.Name(), synch.NewSMap[string, Subscription]())
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
